"""CLI entry point for the NBA tracker package."""

from __future__ import annotations
import logging
import sys

from . import (
    record_picks, settle_picks, get_pick_summary, capture_closing_odds,
)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = set(sys.argv[1:])

    if "--record" in args:
        force = "--force" in args
        print(f"Recording today's NBA Q1 picks{' (force refresh)' if force else ''}...",
              flush=True)
        picks = record_picks(force=force)
        print(f"Recorded {len(picks)} NBA picks:")
        for p in picks:
            print(f"  {p['matchup']} | {p['type']:12s} | {p['pick']:20s} | "
                  f"{p['prob']:.1%} | edge: {p['edge']:+.1f}%")

    elif "--capture-closing" in args:
        print("Capturing NBA Q1 closing odds...", flush=True)
        n = capture_closing_odds()
        print(f"Updated {n} pending picks with closing odds.")

    elif "--settle" in args:
        print("Settling completed NBA Q1 picks...", flush=True)
        result = settle_picks()
        print(f"Settled: {result.get('settled', 0)} "
              f"({result.get('wins', 0)}W-{result.get('losses', 0)}L)")
        print(f"Pending: {result.get('pending_remaining', 0)}")
        if result.get("message"):
            print(f"  {result['message']}")

    elif "--summary" in args:
        summary = get_pick_summary()
        overall = summary["overall"]
        print(f"\n{'='*55}")
        print(f"  NBA PICK TRACKER -- Running Totals")
        print(f"{'='*55}")
        print(f"  Total picks: {overall['total']}")
        print(f"  Record: {overall['wins']}-{overall['losses']} ({overall['win_pct']}%)")
        print(f"  Profit: ${overall['profit']:+.2f}")
        print(f"  Pending: {overall['pending']}")
        if overall.get("avg_clv") is not None:
            print(f"  Avg CLV: {overall['avg_clv']:+.2f}% ({overall['clv_sample']} picks)")

    else:
        print("Usage: python -m engine.nba_tracker --record | "
              "--capture-closing | --settle | --summary")


if __name__ == "__main__":
    main()
