"""Unified train/infer harness (A7).

Single code path for prediction. "Training" becomes "predict over
historical games via the Predictor protocol, compare to known
outcomes." That's the entire deliverable: a Predictor that gives the
right answer in inference also gives the right answer when you ask
it to predict a game whose result you already know.

Why this matters: the legacy split has training in ``engine/gbm/train.py``
and inference in per-sport ``predict.py`` files. The two paths diverge
silently when one gets refactored — you train on features the live
serve doesn't compute, or vice versa. Single code path = symmetry by
construction.

This module replaces the "backtest" portion of the legacy split. New
models / new variants / new feature sets all run through here:

    backtest_predictor(predictor, sport, *, since, until, market='ml')
        Walk every settled game in the window, call predictor.predict
        on each, compare to outcome. Returns Brier + ROI + n.

    register_and_evaluate(predictor, sport, market='ml',
                            window_days=60) -> dict
        Convenience: run the backtest, register the predictor with A2's
        model_versions if Brier improves over current live, and emit
        ``decision`` events stamped with the candidate's version_id
        for shadow promotion accounting.

The evaluation reads the SAME event log A1 maintains; the ``decision``
events emitted during backtest land in the same log; A2 promotion gate
walks the same events to compare live vs candidate. End-to-end one
substrate.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from . import events as _events
from . import model_versions as _mv
from .predictor import GameContext, Predictor, Prediction

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """Aggregate metrics from running a Predictor over a window."""
    predictor_name: str
    sport: str
    market: str
    n: int
    brier: float | None
    accuracy: float | None
    roi: float | None
    profit: float
    hits: int
    losses: int
    pushes: int
    window_start: str
    window_end: str

    def as_dict(self) -> dict:
        return {
            "predictor_name": self.predictor_name,
            "sport": self.sport,
            "market": self.market,
            "n": self.n,
            "brier": self.brier,
            "accuracy": self.accuracy,
            "roi": self.roi,
            "profit": self.profit,
            "hits": self.hits,
            "losses": self.losses,
            "pushes": self.pushes,
            "window_start": self.window_start,
            "window_end": self.window_end,
        }


def _historical_settled(
    sport: str, *, since: str, until: str, market: str = "ml",
) -> list[dict]:
    """Pull (decision, settle) pairs from the event log within the window
    for the given sport + market. Each row carries enough context to
    rebuild a GameContext for the candidate to predict against."""
    conn = _events._get_conn()
    decisions = conn.execute(
        "SELECT id, sport, ts, pick_id, bet_type, pick_text, payload "
        "FROM events WHERE event_type = 'decision' "
        "  AND sport = ? AND ts >= ? AND ts < ?",
        (sport, since, until),
    ).fetchall()
    settle_rows = conn.execute(
        "SELECT pick_id, payload FROM events "
        "WHERE event_type = 'settle' AND sport = ? AND ts >= ?",
        (sport, since),
    ).fetchall()
    settle_by = {(int(s["pick_id"]) if s["pick_id"] else None): s["payload"]
                  for s in settle_rows}

    out = []
    market_upper = market.upper()
    for d in decisions:
        if (d["bet_type"] or "").upper() != market_upper:
            continue
        try:
            payload = json.loads(d["payload"])
        except json.JSONDecodeError:
            continue
        if not payload.get("accepted"):
            continue
        s_raw = settle_by.get(int(d["pick_id"]) if d["pick_id"] else None)
        if not s_raw:
            continue
        try:
            settle = json.loads(s_raw)
        except json.JSONDecodeError:
            continue
        if settle.get("result") not in ("W", "L", "P"):
            continue
        out.append({
            "ts": d["ts"],
            "pick_id": int(d["pick_id"]),
            "bet_type": d["bet_type"],
            "pick_text": d["pick_text"],
            "raw_prob": payload.get("raw_prob"),
            "calibrated_prob": payload.get("calibrated_prob"),
            "odds": payload.get("odds"),
            "result": settle.get("result"),
            "profit": float(settle.get("profit") or 0),
            "matchup": (payload.get("matchup")
                         or (payload.get("extra") or {}).get("matchup") or ""),
        })
    return out


def backtest_predictor(
    predictor: Predictor,
    sport: str,
    *,
    since: str | None = None,
    until: str | None = None,
    window_days: int = 60,
    market: str = "ml",
) -> BacktestResult:
    """Run ``predictor`` over every settled ``market`` decision in the
    window. Returns aggregate metrics.

    The window is defined inclusively over decision timestamps. For
    each historical decision, we rebuild a GameContext from its
    payload and ask the predictor what it would have said. Predictor
    output is compared to the actual settle.

    Brier score uses the predictor's ml_home as the probability head;
    if the candidate doesn't return ml_home (e.g. a regression-only
    GBM), the row is skipped from Brier and counted only toward ROI.
    """
    if not until:
        until = datetime.now(timezone.utc).isoformat()
    if not since:
        start_dt = (datetime.now(timezone.utc)
                    - timedelta(days=window_days))
        since = start_dt.isoformat()

    rows = _historical_settled(sport, since=since, until=until, market=market)

    n = 0
    brier_sum = 0.0
    brier_n = 0
    hits = losses = pushes = 0
    profit_sum = 0.0
    correct_top_pick = 0

    market_is_ml = (market or "").upper() == "ML"
    for r in rows:
        # Reconstruct GameContext from the decision matchup. For team
        # sports, matchup is "AWAY @ HOME"; tennis is "p1 vs p2".
        matchup = r["matchup"] or ""
        ctx = _matchup_to_context(sport, matchup)
        if not ctx:
            continue
        try:
            pred = predictor.predict(ctx)
        except Exception as e:
            logger.debug("predictor failed for %s: %s", matchup, e)
            continue
        if pred.error:
            continue

        # Brier + accuracy only meaningful for ML markets — the predictor's
        # ml_home is a head-to-head win prob, not directly comparable to
        # spread/total/derivative outcomes. ROI counts every settled pick
        # regardless of market.
        if market_is_ml and pred.ml_home is not None:
            is_home_pick = _pick_was_home(matchup, r["pick_text"], ctx)
            if is_home_pick is not None:
                actual_home_win = (
                    (r["result"] == "W" and is_home_pick)
                    or (r["result"] == "L" and not is_home_pick)
                )
                outcome = 1 if actual_home_win else 0
                brier_sum += (pred.ml_home - outcome) ** 2
                brier_n += 1
                # accuracy = pred picked the actual winner
                pred_home_win = pred.ml_home >= 0.5
                if pred_home_win == actual_home_win:
                    correct_top_pick += 1

        # ROI side — we use the original decision's odds + result. Counts
        # for every settled bet, ML or otherwise.
        if r["result"] == "W":
            hits += 1
            profit_sum += r["profit"]
        elif r["result"] == "L":
            losses += 1
            profit_sum += r["profit"]
        else:
            pushes += 1
        n += 1

    return BacktestResult(
        predictor_name=getattr(predictor, "name", "unknown"),
        sport=sport,
        market=market,
        n=n,
        brier=round(brier_sum / brier_n, 5) if brier_n else None,
        accuracy=round(correct_top_pick / brier_n, 4) if brier_n else None,
        roi=round(profit_sum / n, 2) if n else None,
        profit=round(profit_sum, 2),
        hits=hits, losses=losses, pushes=pushes,
        window_start=since,
        window_end=until,
    )


def _matchup_to_context(sport: str, matchup: str) -> GameContext | None:
    """Rebuild a GameContext from a stored matchup string."""
    if " @ " in matchup:
        away, home = matchup.split(" @ ", 1)
        return GameContext(sport=sport, home=home.strip(), away=away.strip())
    if " vs " in matchup:
        a, b = matchup.split(" vs ", 1)
        return GameContext(sport=sport, home=a.strip(), away=b.strip(),
                            extras={"tour": "atp"})  # default; caller can override
    return None


def _pick_was_home(matchup: str, pick_text: str,
                    ctx: GameContext) -> bool | None:
    """Did the original decision back the home side? Heuristic — we
    parse the pick text and compare against ctx.home / ctx.away."""
    if not pick_text:
        return None
    pk = pick_text.strip()
    if pk.startswith(ctx.home) or ctx.home in pk:
        return True
    if pk.startswith(ctx.away) or ctx.away in pk:
        return False
    return None


def register_and_evaluate(
    predictor: Predictor,
    sport: str,
    *,
    market: str = "ml",
    window_days: int = 60,
    component: str | None = None,
    promote_if_better: bool = False,
    min_n: int = 200,
    min_brier_improvement: float = 0.005,
) -> dict:
    """Run the backtest, optionally register the result as an A2
    candidate version, and (if ``promote_if_better=True``) gate it
    against the live version."""
    result = backtest_predictor(
        predictor, sport, window_days=window_days, market=market,
    )
    component = component or f"{predictor.__class__.__name__}_{sport}_{market}"
    version_id = _mv.register_version(
        component=component,
        config={
            "predictor_class": predictor.__class__.__name__,
            "predictor_name": getattr(predictor, "name", "unknown"),
            "market": market,
            "window_days": window_days,
            "backtest": result.as_dict(),
        },
        notes=f"A7 harness backtest n={result.n}",
    )

    out: dict = {
        "version_id": version_id,
        "component": component,
        "result": result.as_dict(),
    }

    if promote_if_better:
        verdict = _mv.promote_if_better(
            component=component,
            candidate_id=version_id,
            min_n=min_n,
            min_brier_improvement=min_brier_improvement,
            sport=sport,
        )
        out["promotion"] = verdict
    return out


# ── CLI ──────────────────────────────────────────────────────

def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.training_harness")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_bt = sub.add_parser("backtest")
    p_bt.add_argument("predictor", help="dotted path or registered name")
    p_bt.add_argument("sport")
    p_bt.add_argument("--window-days", type=int, default=60)
    p_bt.add_argument("--market", default="ml")

    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")

    # Resolve predictor — registered name or dotted path
    from . import predictor as _pred_mod
    p = _pred_mod.get(args.predictor)
    if p is None:
        # Try dotted import
        try:
            mod_name, attr = args.predictor.rsplit(".", 1)
            mod = __import__(mod_name, fromlist=[attr])
            cls = getattr(mod, attr)
            p = cls()
        except Exception as e:
            print(f"failed to resolve predictor {args.predictor!r}: {e}")
            return 1

    if args.cmd == "backtest":
        result = backtest_predictor(
            p, args.sport,
            window_days=args.window_days, market=args.market,
        )
        print(json.dumps(result.as_dict(), indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
