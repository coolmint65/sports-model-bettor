"""CLI entry point for the tracker package.

Invoked via `python -m engine.tracker --record / --settle / --summary`.
Lives in __main__.py so the CLI behaviour the pre-split module had
under `if __name__ == "__main__"` keeps working as a package too.
"""

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
        print(f"Recording today's picks{' (force reset)' if force else ''}...", flush=True)
        picks = record_picks(force=force)
        print(f"Recorded {len(picks)} picks:")
        for p in picks:
            print(f"  {p['matchup']} | {p['type']:5s} | {p['pick']:15s} | {p['prob']:.1%} | edge: {p['edge']:+.1f}%")

    elif "--settle" in args:
        print("Settling completed picks...", flush=True)
        result = settle_picks()
        # settle_picks short-circuits when no pending rows exist and
        # returns a no-wins/losses dict — use .get() so the empty case
        # doesn't KeyError.
        print(f"Settled: {result.get('settled', 0)} "
              f"({result.get('wins', 0)}W-{result.get('losses', 0)}L)")
        print(f"Pending: {result.get('pending_remaining', '?')}")

    elif "--summary" in args:
        summary = get_pick_summary()
        overall = summary["overall"]
        print(f"\n{'='*50}")
        print(f"  PICK TRACKER - Running Totals")
        print(f"{'='*50}")
        print(f"  Total picks: {overall['total']}")
        print(f"  Record: {overall['wins']}-{overall['losses']} ({overall['win_pct']}%)")
        print(f"  Profit: ${overall['profit']:+.2f}")
        print(f"  Pending: {overall['pending']}")
        if overall.get("avg_clv") is not None:
            print(f"  Avg CLV: {overall['avg_clv']:+.2f}% ({overall['clv_sample']} picks)")
        print()
        for bt, label in [("ML", "Moneyline"), ("O/U", "Over/Under"),
                          ("1st INN", "1st Inning"), ("RL", "Run Line")]:
            s = summary["by_type"][bt]
            if s["total"] == 0:
                continue
            print(f"  {label}: {s['wins']}-{s['losses']} ({s['win_pct']}%) ${s['profit']:+.2f}")
        print(f"{'='*50}")

    else:
        print("Usage: python -m engine.tracker --record | --settle | --summary")


if __name__ == "__main__":
    main()
