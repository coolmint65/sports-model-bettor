"""Performance drift detector (A3).

Watches the settled-decision stream and asks: "is the model still hitting
at the rate calibration predicts?" When realized hit rate diverges from
calibrated probability for a (sport, bet_type) cell beyond threshold,
emit a ``drift_signal`` event. The worker reads drift_signal events and
fires an out-of-cycle calibration refit + validation gate.

Different from ``engine.distribution_drift`` (which watches per-prop
distribution-family drift, e.g. Poisson → NegBin). This module watches
*model performance* drift: are picks honoring their calibrated
probabilities? Different question; complementary signal.

Two metrics computed per (sport, bet_type) over a rolling window:

  - Brier delta — Brier score on the last N picks vs the same Brier
    score on the prior N picks. Sustained increase = the model is
    losing predictive accuracy.
  - Hit-rate gap — realized hit rate - average calibrated probability.
    Big positive = model overconfident on losses; big negative =
    model underconfident on wins (still bad, just from the other
    direction).

A signal fires when EITHER metric exceeds its threshold. The worker
de-duplicates: it doesn't kick a refit twice for the same cell within
a cooldown window.

Public API:
  scan(sport=None, *, window=50, prior_window=50,
       brier_delta_threshold=0.04, hitrate_gap_threshold=0.10)
      Returns the list of cells that crossed threshold + writes
      drift_signal events for each.
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Iterable

from . import events as _events
from . import events_materialize

logger = logging.getLogger(__name__)


def _settled_pairs(sport: str | None = None) -> list[dict]:
    """Pull settled (decision, settle) pairs from the events log.
    Returns list of dicts with sport / bet_type / direction / prob /
    outcome (1 W, 0 L) / ts. Pushes/voids excluded — they don't move
    Brier or hit rate."""
    conn = _events._get_conn()
    where = "WHERE event_type = 'decision'"
    params: list = []
    if sport:
        where += " AND sport = ?"
        params.append(sport)
    decisions = conn.execute(
        f"SELECT id, sport, ts, pick_id, bet_type, pick_text, payload "
        f"FROM events {where} AND pick_id IS NOT NULL "
        f"ORDER BY ts ASC, id ASC",
        params,
    ).fetchall()

    settle_where = "WHERE event_type = 'settle'"
    settle_params: list = []
    if sport:
        settle_where += " AND sport = ?"
        settle_params.append(sport)
    settles = conn.execute(
        f"SELECT pick_id, sport, payload FROM events {settle_where}",
        settle_params,
    ).fetchall()
    settle_by = {(s["sport"], int(s["pick_id"])): s["payload"]
                  for s in settles if s["pick_id"] is not None}

    out = []
    for d in decisions:
        try:
            payload = json.loads(d["payload"]) if d["payload"] else {}
        except json.JSONDecodeError:
            continue
        if not payload.get("accepted"):
            continue
        s_payload_raw = settle_by.get((d["sport"], int(d["pick_id"])))
        if not s_payload_raw:
            continue
        try:
            s_payload = json.loads(s_payload_raw)
        except json.JSONDecodeError:
            continue
        result = s_payload.get("result")
        if result not in ("W", "L"):
            continue
        prob = payload.get("calibrated_prob") or payload.get("raw_prob")
        if prob is None:
            continue
        bt = (d["bet_type"] or "").upper()
        direction = events_materialize._direction_label(
            bt, d["pick_text"] or "")
        out.append({
            "sport": d["sport"],
            "ts": d["ts"],
            "bet_type": bt,
            "direction": direction,
            "prob": float(prob),
            "outcome": 1 if result == "W" else 0,
        })
    return out


def scan(
    sport: str | None = None,
    *,
    window: int = 50,
    prior_window: int = 50,
    brier_delta_threshold: float = 0.04,
    hitrate_gap_threshold: float = 0.10,
    cooldown_hours: int = 6,
) -> list[dict]:
    """Walk the event stream per (sport, bet_type[+direction]) and
    detect cells whose recent performance has drifted from prior.

    Args:
        sport:                  filter to one sport (None = all)
        window:                 size of the recent settled window
        prior_window:           size of the comparison window before that
        brier_delta_threshold:  Brier increase that triggers a signal
        hitrate_gap_threshold:  |realized - avg_predicted| trigger
        cooldown_hours:         no duplicate signal for the same cell
                                within this window

    Emits ``drift_signal`` events for each cell that crosses threshold.
    Returns the list of detection records."""
    pairs = _settled_pairs(sport=sport)
    # Group by (sport, bet_type, direction)
    cells: dict[tuple, list[dict]] = defaultdict(list)
    for p in pairs:
        key = (p["sport"], p["bet_type"], p["direction"])
        cells[key].append(p)

    detections: list[dict] = []
    cooldown_cutoff = (datetime.now(timezone.utc)
                        - timedelta(hours=cooldown_hours)).isoformat()
    for key, items in cells.items():
        sp, bt, direction = key
        if len(items) < (window + prior_window):
            continue  # not enough history

        # Last `window` and the `prior_window` before that
        recent = items[-window:]
        prior = items[-(window + prior_window):-window]

        recent_brier = _brier(recent)
        prior_brier = _brier(prior)
        brier_delta = (recent_brier - prior_brier
                        if recent_brier is not None and prior_brier is not None
                        else None)

        recent_hr = _hit_rate(recent)
        recent_avg_pred = _avg_pred(recent)
        hitrate_gap = (recent_hr - recent_avg_pred
                        if recent_hr is not None and recent_avg_pred is not None
                        else None)

        triggers: list[str] = []
        if brier_delta is not None and brier_delta > brier_delta_threshold:
            triggers.append(f"brier_increase={brier_delta:.4f}")
        if hitrate_gap is not None and abs(hitrate_gap) > hitrate_gap_threshold:
            triggers.append(f"hitrate_gap={hitrate_gap:+.3f}")
        if not triggers:
            continue

        # Cooldown — skip if we've signalled this cell recently
        if _has_recent_signal(sp, bt, direction, cooldown_cutoff):
            continue

        # Emit drift_signal events — one per metric that triggered
        scope = {"bet_type": bt, "direction": direction}
        for trig in triggers:
            metric, _, value_str = trig.partition("=")
            try:
                value = float(value_str)
            except ValueError:
                value = 0.0
            threshold = (brier_delta_threshold if metric == "brier_increase"
                          else hitrate_gap_threshold)
            _events.write_drift_signal(
                sport=sp, metric=metric, value=value,
                threshold=threshold, scope=scope,
            )

        detections.append({
            "sport": sp,
            "bet_type": bt,
            "direction": direction,
            "n_recent": len(recent),
            "n_prior": len(prior),
            "recent_brier": round(recent_brier, 4) if recent_brier else None,
            "prior_brier": round(prior_brier, 4) if prior_brier else None,
            "brier_delta": round(brier_delta, 4) if brier_delta else None,
            "recent_hit_rate": round(recent_hr, 4) if recent_hr else None,
            "recent_avg_pred": round(recent_avg_pred, 4) if recent_avg_pred else None,
            "hitrate_gap": round(hitrate_gap, 4) if hitrate_gap else None,
            "triggers": triggers,
        })
    return detections


def _brier(pairs: list[dict]) -> float | None:
    if not pairs:
        return None
    return sum((p["prob"] - p["outcome"]) ** 2 for p in pairs) / len(pairs)


def _hit_rate(pairs: list[dict]) -> float | None:
    if not pairs:
        return None
    return sum(p["outcome"] for p in pairs) / len(pairs)


def _avg_pred(pairs: list[dict]) -> float | None:
    if not pairs:
        return None
    return sum(p["prob"] for p in pairs) / len(pairs)


def _has_recent_signal(sport: str, bet_type: str, direction: str | None,
                        cutoff: str) -> bool:
    """Cooldown gate: did we already emit a drift_signal for this cell
    after the cutoff timestamp?"""
    conn = _events._get_conn()
    rows = conn.execute(
        "SELECT id, payload FROM events WHERE event_type = 'drift_signal' "
        "AND sport = ? AND bet_type = ? AND ts >= ?",
        (sport, bet_type, cutoff),
    ).fetchall()
    for r in rows:
        try:
            p = json.loads(r["payload"])
            if p.get("scope", {}).get("direction") == direction:
                return True
        except json.JSONDecodeError:
            continue
    return False


# ── Worker action loop ───────────────────────────────────────

def consume_unactioned_signals(
    *,
    handler=None,
    cooldown_hours: int = 6,
) -> dict:
    """Scan recent unactioned drift_signal events and call ``handler``
    for each. Defaults to a refit-trigger handler that calls
    empirical_calibration.refresh_calibration for the affected sport.

    "Unactioned" = no ``refit`` event recorded with the same scope
    after the drift_signal's timestamp. Once handler returns success,
    we emit a ``refit`` event so the cooldown applies.
    """
    if handler is None:
        handler = _default_drift_handler
    conn = _events._get_conn()
    cutoff = (datetime.now(timezone.utc)
                - timedelta(hours=cooldown_hours)).isoformat()
    signals = conn.execute(
        "SELECT id, ts, sport, bet_type, payload FROM events "
        "WHERE event_type = 'drift_signal' AND ts >= ?",
        (cutoff,),
    ).fetchall()
    actioned = 0
    skipped = 0
    for sig in signals:
        try:
            payload = json.loads(sig["payload"])
        except json.JSONDecodeError:
            continue
        # Has a refit event already followed this signal for this sport?
        recent_refit = conn.execute(
            "SELECT id FROM events WHERE event_type = 'refit' "
            "AND sport = ? AND ts > ? LIMIT 1",
            (sig["sport"], sig["ts"]),
        ).fetchone()
        if recent_refit:
            skipped += 1
            continue
        try:
            handler(sig["sport"], sig["bet_type"], payload)
            _events.write_refit(
                sport=sig["sport"],
                component="empirical_calibration",
                summary={"triggered_by_signal_id": sig["id"],
                          "metric": payload.get("metric"),
                          "value": payload.get("value")},
                triggered_by="drift",
            )
            actioned += 1
        except Exception as e:
            logger.warning("drift handler failed for %s/%s: %s",
                           sig["sport"], sig["bet_type"], e)
    return {"signals_seen": len(signals),
            "actioned": actioned, "skipped": skipped}


def _default_drift_handler(sport: str, bet_type: str | None,
                            payload: dict) -> None:
    """Default: refresh the empirical calibration table for the sport.
    Lightweight; the per-bet-type granularity is implicit since the
    refresh recomputes every bucket."""
    from . import empirical_calibration
    empirical_calibration.refresh_calibration(sport)


# ── CLI ──────────────────────────────────────────────────────

def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.drift_detector")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_scan = sub.add_parser("scan")
    p_scan.add_argument("--sport", default=None)
    p_scan.add_argument("--window", type=int, default=50)
    p_scan.add_argument("--prior-window", type=int, default=50)
    p_scan.add_argument("--brier-threshold", type=float, default=0.04)
    p_scan.add_argument("--hitrate-threshold", type=float, default=0.10)

    p_act = sub.add_parser("consume")
    p_act.add_argument("--cooldown-hours", type=int, default=6)

    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    if args.cmd == "scan":
        out = scan(
            sport=args.sport,
            window=args.window, prior_window=args.prior_window,
            brier_delta_threshold=args.brier_threshold,
            hitrate_gap_threshold=args.hitrate_threshold,
        )
        print(json.dumps(out, indent=2))
    elif args.cmd == "consume":
        out = consume_unactioned_signals(cooldown_hours=args.cooldown_hours)
        print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
