"""CLI entry point for the NHL tracker package."""

from __future__ import annotations
import logging
import sys

from . import record_picks, settle_picks, get_pick_summary


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = set(sys.argv[1:])

    if "--record" in args:
        force = "--force" in args
        print(f"Recording today's NHL picks{' (force reset)' if force else ''}...", flush=True)
        picks = record_picks(force=force)
        print(f"Recorded {len(picks)} NHL picks:")
        for p in picks:
            print(f"  {p['matchup']} | {p['type']:4s} | {p['pick']:15s} | {p['prob']:.1%} | edge: {p['edge']:+.1f}%")

    elif "--settle" in args:
        print("Settling completed NHL picks...", flush=True)
        result = settle_picks()
        print(f"Settled: {result.get('settled', 0)} ({result.get('wins', 0)}W-{result.get('losses', 0)}L)")
        print(f"Pending: {result.get('pending_remaining', '?')}")

    elif "--summary" in args:
        summary = get_pick_summary()
        overall = summary["overall"]
        print(f"\n{'='*50}")
        print(f"  NHL PICK TRACKER - Running Totals")
        print(f"{'='*50}")
        print(f"  Total picks: {overall['total']}")
        print(f"  Record: {overall['wins']}-{overall['losses']} ({overall['win_pct']}%)")
        print(f"  Profit: ${overall['profit']:+.2f}")
        if overall.get("avg_clv") is not None:
            print(f"  Avg CLV: {overall['avg_clv']:+.2f}% ({overall['clv_sample']} picks)")
        print(f"{'='*50}")

    else:
        print("Usage: python -m engine.nhl_tracker --record | --settle | --summary")


if __name__ == "__main__":
    main()
