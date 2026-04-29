"""CLI entry point for the NHL backtest package."""

from __future__ import annotations
import logging
import sys

from . import run_nhl_backtest, analyze_edge_thresholds, print_backtest


def main() -> None:
    logging.basicConfig(level=logging.WARNING)

    args = sys.argv[1:]
    season = None
    min_edge = 3.0
    days_val = 30
    pit = True
    run_thresholds = False

    i = 0
    while i < len(args):
        if args[i] == "--season" and i + 1 < len(args):
            season = int(args[i + 1])
            i += 2
        elif args[i] == "--min-edge" and i + 1 < len(args):
            min_edge = float(args[i + 1])
            i += 2
        elif args[i] == "--days" and i + 1 < len(args):
            days_val = int(args[i + 1])
            i += 2
        elif args[i] == "--no-pit":
            pit = False
            i += 1
        elif args[i] == "--thresholds":
            run_thresholds = True
            i += 1
        else:
            i += 1

    if run_thresholds:
        print(f"Running edge threshold analysis (season={season})...",
              flush=True)
        th_results = analyze_edge_thresholds(days=days_val, season=season,
                                             pit_mode=pit)
        print(f"\n{'='*70}")
        print(f"  EDGE THRESHOLD ANALYSIS")
        print(f"{'='*70}")
        for r in th_results:
            bb = r.get("best_bet", {})
            print(f"  {r['threshold']:>2}% min edge: "
                  f"{bb.get('bets', 0):>4} bets | "
                  f"{bb.get('win_pct', 0):>5.1f}% win | "
                  f"ROI {bb.get('roi', 0):>+6.1f}% | "
                  f"P/L ${bb.get('profit', 0):>+8.2f}")
        print(f"{'='*70}")
    else:
        print(f"Running NHL backtest (days={days_val}, min_edge={min_edge}%, "
              f"pit={'on' if pit else 'off'})...", flush=True)
        results = run_nhl_backtest(days=days_val, min_edge=min_edge,
                                   season=season, pit_mode=pit)
        print_backtest(results)


if __name__ == "__main__":
    main()
