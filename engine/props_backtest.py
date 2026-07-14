"""
One-shot prop backtest CLI — answers "are we running blind on player props?"

Three signals are reported per sport:

  1. CALIBRATION — for every (player, game) in the recent log window,
     replay the picker's probability with ONLY pre-game data, bucket the
     prediction, and compare the bucket's mean predicted prob to the
     observed hit rate. A gap > 5pp at any well-populated bucket flags
     a stat the model is over- or under-confident on.

     This is leak-free walk-forward — same code path as
     `engine.player_props_calibration.calibrate_sport`.

  2. SHRINKAGE RECOMMENDATIONS — derived from calibration. Suggests a
     multiplier on `(prob - 0.5)` that would close the average over-
     confidence gap per stat. Underconfidence is left alone (expanding
     probability could push picks above the edge floor that we wanted
     filtered).

     Already hardcoded entries live in `engine.distribution_fit.
     _PROB_SHRINK`; this script tells you which ones to revisit.

  3. LIVE ROI — actual P/L on settled prop picks per bet_type, segmented
     so a category losing money in production stands out even when its
     calibration looks fine. (Calibration is "honest probabilities";
     live ROI is what HR actually paid out — the two diverge when the
     market has information the model doesn't.)

Use:
    python -m engine.props_backtest             # all three sports
    python -m engine.props_backtest --sport nba
    python -m engine.props_backtest --days 60   # wider calibration window
"""

from __future__ import annotations

import argparse
import sys

from .player_props_db import _conn_for
from .player_props_calibration import (
    calibrate_sport, shrinkage_recommendation,
)
from .distribution_fit import get_prob_shrink, save_learned_shrinkage


_SPORTS = ("mlb", "nba", "nhl")


def _calibration_report(sport: str, days: int, n_sims: int) -> None:
    print(f"\n--- CALIBRATION ({sport.upper()}, last {days}d) ---")
    print(f"  {'stat':14s}  {'n':>6s}  {'pred':>6s}  {'obs':>6s}  {'gap':>7s}  flag")
    cal = calibrate_sport(sport, days=days, n_sims=n_sims)
    for stat, summary in cal.items():
        # Aggregate across buckets, ignore the warning marker.
        buckets = {k: v for k, v in summary.items()
                   if isinstance(v, dict) and "n" in v}
        total_n = sum(b["n"] for b in buckets.values())
        if total_n < 30:
            continue
        weighted_pred = sum(b["mean_predicted"] * b["n"]
                            for b in buckets.values()) / total_n
        weighted_obs = sum(b["observed_rate"] * b["n"]
                           for b in buckets.values()) / total_n
        gap = weighted_obs - weighted_pred
        flag = "OVERCONF" if gap < -0.05 else ("LOOSE" if gap > 0.05 else "ok")
        print(f"  {stat:14s}  {total_n:>6d}  {weighted_pred:>5.3f}  "
              f"{weighted_obs:>5.3f}  {gap:>+6.3f}  {flag}")
    return cal


def _shrinkage_report(sport: str, cal: dict) -> None:
    print(f"\n--- SHRINKAGE RECS ({sport.upper()}) ---")
    print(f"  {'stat':14s}  {'current':>8s}  {'recommend':>10s}  note")
    recs = shrinkage_recommendation(cal)
    for stat, info in recs.items():
        current = get_prob_shrink(sport, stat)
        rec = info.get("shrink", 1.0)
        note = info.get("note", "")
        flag = " <-- adjust" if abs(rec - current) > 0.02 else ""
        print(f"  {stat:14s}  {current:>8.3f}  {rec:>10.3f}  {note}{flag}")


def _live_roi_report(sport: str) -> None:
    conn = _conn_for(sport)
    rows = conn.execute(
        "SELECT bet_type, COUNT(*) AS n, "
        "       SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) AS w, "
        "       SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) AS l, "
        "       SUM(CASE WHEN result='P' THEN 1 ELSE 0 END) AS p, "
        "       COALESCE(SUM(profit), 0) AS profit "
        "FROM player_props_picks "
        "WHERE result IS NOT NULL "
        "GROUP BY bet_type ORDER BY n DESC"
    ).fetchall()
    if not rows:
        print(f"\n--- LIVE ROI ({sport.upper()}) ---  no settled picks yet")
        return
    print(f"\n--- LIVE ROI ({sport.upper()}) ---")
    print(f"  {'bet_type':22s}  {'n':>4s}  {'W-L-P':>10s}  "
          f"{'WR':>6s}  {'profit':>9s}  {'ROI':>7s}  flag")
    total_n = total_w = total_l = total_p = 0
    total_profit = 0.0
    for r in rows:
        n = r["n"] or 0
        w = r["w"] or 0
        l = r["l"] or 0
        p = r["p"] or 0
        profit = float(r["profit"] or 0)
        wr = (w / max(1, w + l)) * 100
        roi = profit / max(1, n)
        flag = "PAUSE?" if (n >= 20 and roi < -5) else (
            "GOOD" if roi > 5 else "ok")
        print(f"  {r['bet_type']:22s}  {n:>4d}  {w:>2d}-{l:>2d}-{p:>2d}  "
              f"{wr:>5.1f}%  {profit:>+8.2f}  {roi:>+6.2f}%  {flag}")
        total_n += n
        total_w += w
        total_l += l
        total_p += p
        total_profit += profit
    if total_n:
        wr = (total_w / max(1, total_w + total_l)) * 100
        roi = total_profit / total_n
        print(f"  {'TOTAL':22s}  {total_n:>4d}  "
              f"{total_w:>2d}-{total_l:>2d}-{total_p:>2d}  "
              f"{wr:>5.1f}%  {total_profit:>+8.2f}  {roi:>+6.2f}%")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", choices=_SPORTS,
                    help="Limit to one sport. Default: all three.")
    ap.add_argument("--days", type=int, default=30,
                    help="Calibration window in days (default 30).")
    ap.add_argument("--n-sims", type=int, default=3000,
                    help="MC simulations per (player, game) replay.")
    ap.add_argument("--persist", action="store_true",
                    help="Write the recommended shrinkage values to "
                         "data/prop_shrinkage.json so the live picker "
                         "uses them. Default off — the report is "
                         "diagnostic-only unless explicitly persisted.")
    args = ap.parse_args(argv)
    sports = (args.sport,) if args.sport else _SPORTS

    print("=" * 78)
    print("  PROP BACKTEST — calibration + shrinkage recs + live ROI")
    print("=" * 78)
    print("  Calibration is leak-free walk-forward over recent player_game_logs.")
    print("  Live ROI is actual P/L on placed picks at HR prices (settled only).")
    print("  A bet_type that calibrates well but loses money live = HR's prices")
    print("  carry information the model lacks — pause until the picker improves.")

    for sport in sports:
        cal = _calibration_report(sport, args.days, args.n_sims)
        _shrinkage_report(sport, cal)
        if args.persist:
            recs = shrinkage_recommendation(cal)
            path = save_learned_shrinkage(sport, recs)
            print(f"\n  [persisted shrinkage for {sport.upper()} → {path}]")
        _live_roi_report(sport)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
