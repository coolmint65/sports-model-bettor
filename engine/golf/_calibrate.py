"""Golf walk-forward calibration.

For each historical tournament we have, run ``predict_field`` using only
data with end_date strictly before the tournament's start_date (the
predictor's ``fit_skill`` already enforces this), then compare per-player
predicted probabilities to realized outcomes pulled from ``field_entries``.

Different markets live in wildly different prob ranges (WINNER picks
fire on probs as low as 0.8%, MAKE_CUT on 55%+), so the shared
``engine.calibration_buckets`` grid is the wrong shape — it would dump
every WINNER prediction into the [0.30, 0.40) catch-all and learn
nothing. This calibrator owns market-specific bucket grids, persists
them per-tour, and is read by ``engine.golf._calibration_apply``.

Output: ``data/golf/<tour>_calibration.json``
    {
      "tour": "pga",
      "method": "tournament_walkforward",
      "holdout_start": "2024-08-01",
      "n_predicted": 7800,
      "buckets": {
        "WINNER":   [{"bucket": [lo, hi], "n", "avg_pred", "realized_wr"}, ...],
        "TOP_5":    [...],
        "TOP_10":   [...],
        "TOP_20":   [...],
        "MAKE_CUT": [...]
      }
    }

Caveats:
  - OWGR snapshot is a single point-in-time row per player. We can't
    walk it back, so OWGR features carry mild leakage for older
    holdout tournaments. The leakage is small in practice: a player
    who is OWGR #20 today was usually within ±20 of that rank a year
    ago. Accepted.
  - DataGolf SG snapshot: same story — single fetch.
  - Score history is fully leakage-clean (fit_skill filters by
    ``before_date=tournament.start_date``).
"""
from __future__ import annotations

import json
import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from ._db import get_conn
from ._predict import predict_field

logger = logging.getLogger(__name__)

_OUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "golf"


# Per-market bucket grids. WINNER probs live near 0.5–15%; MAKE_CUT
# near 55–95%; TOP_5/10/20 in between. Hand-tuned so each bucket has
# enough samples to be statistically useful after aggregation across
# ~50 holdout tournaments × ~150 players each.
_BUCKETS: dict[str, list[tuple[float, float]]] = {
    "WINNER": [
        (0.005, 0.010),
        (0.010, 0.015),
        (0.015, 0.025),
        (0.025, 0.040),
        (0.040, 0.060),
        (0.060, 0.090),
        (0.090, 0.150),
        # The previous top bucket [0.15, 1.01] collected only n=4
        # samples averaging 0.5 — after the 2026-05-18 small-field
        # softmax-temp fix, a chalk favorite (Scheffler @ +180) lands
        # with raw 16% prob and got mapped to calibrated 0.5 because
        # the lone thin bucket averaged 0.5 from a few past sessions.
        # Split into finer high-prob buckets so the realized win-rate
        # is bucket-local; thin top buckets fall back to raw-pass
        # behaviour in framework_calibration when sample is too small.
        (0.150, 0.220),
        (0.220, 1.01),
    ],
    "TOP_5": [
        (0.030, 0.050),
        (0.050, 0.080),
        (0.080, 0.120),
        (0.120, 0.170),
        (0.170, 0.230),
        (0.230, 0.300),
        (0.300, 0.400),
        (0.400, 1.01),
    ],
    "TOP_10": [
        (0.050, 0.100),
        (0.100, 0.150),
        (0.150, 0.200),
        (0.200, 0.270),
        (0.270, 0.350),
        (0.350, 0.450),
        (0.450, 0.600),
        (0.600, 1.01),
    ],
    "TOP_20": [
        (0.100, 0.170),
        (0.170, 0.240),
        (0.240, 0.320),
        (0.320, 0.420),
        (0.420, 0.520),
        (0.520, 0.620),
        (0.620, 0.750),
        (0.750, 1.01),
    ],
    "MAKE_CUT": [
        (0.500, 0.600),
        (0.600, 0.680),
        (0.680, 0.750),
        (0.750, 0.820),
        (0.820, 0.880),
        (0.880, 0.930),
        (0.930, 0.970),
        (0.970, 1.01),
    ],
}


def _bucket_for(market: str, p: float) -> tuple[float, float] | None:
    for lo, hi in _BUCKETS.get(market, []):
        if lo <= p < hi:
            return lo, hi
    return None


_PRED_KEY = {
    "WINNER":   "p_winner",
    "TOP_5":    "p_top_5",
    "TOP_10":   "p_top_10",
    "TOP_20":   "p_top_20",
    "MAKE_CUT": "p_made_cut",
}


def _holdout_tournaments(conn: sqlite3.Connection,
                          months: int) -> list[dict]:
    """Return tournament rows in the last ``months`` of finalized
    events, oldest first. Each row carries id + start_date + end_date
    so the walk-forward can replay in chronological order."""
    cutoff = (datetime.now() - timedelta(days=months * 30)
              ).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT id, start_date, end_date "
        "FROM tournaments "
        "WHERE status = 'final' "
        "  AND start_date IS NOT NULL "
        "  AND start_date >= ? "
        "ORDER BY start_date",
        (cutoff,),
    ).fetchall()
    return [dict(r) for r in rows]


def _realized(conn: sqlite3.Connection, tournament_id: str) -> dict[int, dict]:
    """Pull the realized outcome per player from field_entries.

    Returns ``{player_id: {made_cut, top_5, top_10, top_20, won}}``.
    """
    out: dict[int, dict] = {}
    rows = conn.execute(
        "SELECT player_id, final_position, made_cut, withdrew, disqualified "
        "FROM field_entries WHERE tournament_id = ?",
        (tournament_id,),
    ).fetchall()
    for r in rows:
        if r["withdrew"] or r["disqualified"]:
            # WD/DQ: not eligible for any market. Skip — picks shouldn't
            # have fired on these (the picker pulls from the live odds
            # board which already excludes withdrawn entries).
            continue
        pos_raw = r["final_position"]
        rank = _parse_position(pos_raw)
        out[int(r["player_id"])] = {
            "made_cut": bool(r["made_cut"]) if r["made_cut"] is not None else False,
            "won":      1 if rank == 1 else 0,
            "top_5":    1 if (rank is not None and rank <= 5) else 0,
            "top_10":   1 if (rank is not None and rank <= 10) else 0,
            "top_20":   1 if (rank is not None and rank <= 20) else 0,
        }
    return out


def _parse_position(pos) -> int | None:
    """Parse '1', 'T1', '2', 'T-2', 'CUT', None → int or None."""
    if pos is None:
        return None
    s = str(pos).strip().upper()
    if not s or s in ("CUT", "WD", "DQ"):
        return None
    if s.startswith("T"):
        s = s[1:].strip().lstrip("-")
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def seed(tour: str, *, holdout_months: int = 18) -> dict:
    """Walk holdout tournaments, predict each via predict_field (leakage-
    clean from scores; mild leakage from current OWGR/SG snapshots),
    bucket predicted prob → realized outcome per market.

    Args:
        tour: 'pga' | 'lpga' | 'kornferry'
        holdout_months: window of recent finalized events to score on.
                        Default 18mo = ~50 PGA events, ~30 LPGA, ~25 KF.

    Returns: ``{summary, bucket_view}``. Persists
    ``data/golf/<tour>_calibration.json``.
    """
    conn = get_conn(tour)
    holdout = _holdout_tournaments(conn, holdout_months)
    if not holdout:
        logger.warning("[golf:%s] no holdout tournaments — skipping", tour)
        return {"error": "no_holdout"}
    logger.info("[golf:%s] holdout: %d tournaments from %s onward",
                tour, len(holdout), holdout[0]["start_date"])

    accum: dict[str, dict[tuple[float, float], list[tuple[float, int]]]] = {
        market: defaultdict(list) for market in _PRED_KEY
    }
    n_predicted = 0
    n_skipped = 0

    for i, t in enumerate(holdout):
        if i and i % 10 == 0:
            logger.info("[golf:%s] %d/%d (predictions=%d)",
                        tour, i, len(holdout), n_predicted)
        try:
            pred = predict_field(tour, t["id"])
        except Exception as exc:
            logger.debug("[golf:%s] predict failed for %s: %s",
                         tour, t["id"], exc)
            n_skipped += 1
            continue
        if not pred:
            n_skipped += 1
            continue
        outcomes = _realized(conn, t["id"])
        if not outcomes:
            n_skipped += 1
            continue
        for pid, prow in pred.items():
            actual = outcomes.get(int(pid))
            if actual is None:
                continue
            for market, key in _PRED_KEY.items():
                p = float(prow.get(key, 0.0))
                if p <= 0.0:
                    continue
                if market == "WINNER":
                    won = actual["won"]
                elif market == "TOP_5":
                    won = actual["top_5"]
                elif market == "TOP_10":
                    won = actual["top_10"]
                elif market == "TOP_20":
                    won = actual["top_20"]
                elif market == "MAKE_CUT":
                    won = 1 if actual["made_cut"] else 0
                else:
                    continue
                bk = _bucket_for(market, p)
                if bk:
                    accum[market][bk].append((p, won))
        n_predicted += 1

    # Aggregate → realized hit rate per (market, bucket)
    cal_table: dict[str, list[dict]] = {}
    for market, buckets in accum.items():
        rows_out = []
        for lo, hi in _BUCKETS[market]:
            samples = buckets.get((lo, hi), [])
            n = len(samples)
            wins = sum(s[1] for s in samples)
            avg_pred = (sum(s[0] for s in samples) / n) if n else None
            realized = (wins / n) if n else None
            rows_out.append({
                "bucket": [lo, hi],
                "n": n,
                "avg_pred": round(avg_pred, 4) if avg_pred is not None else None,
                "realized_wr": round(realized, 4) if realized is not None else None,
            })
        cal_table[market] = rows_out

    out = {
        "tour": tour,
        "method": "tournament_walkforward",
        "holdout_months": holdout_months,
        "holdout_start": holdout[0]["start_date"],
        "n_tournaments": len(holdout),
        "n_tournaments_scored": n_predicted,
        "n_tournaments_skipped": n_skipped,
        "buckets": cal_table,
        "deferred": [],
        "fitted_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _OUT_DIR / f"{tour}_calibration.json"
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    logger.info("[golf:%s] calibration seeded → %s "
                "(n_scored=%d, n_skipped=%d)",
                tour, out_path.name, n_predicted, n_skipped)
    return out


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.golf._calibrate")
    ap.add_argument("tour", choices=("pga", "lpga", "kornferry"))
    ap.add_argument("--holdout-months", type=int, default=18)
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    res = seed(args.tour, holdout_months=args.holdout_months)
    print(json.dumps(res, indent=2, default=str))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
