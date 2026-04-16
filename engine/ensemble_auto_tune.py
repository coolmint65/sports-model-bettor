"""
Automatic ensemble weight tuner.

Runs nightly via sync_*.bat. For each (sport, market) pair:

  1. Count how many prediction_signals rows have matured into settled
     games (see engine.prediction_log.count_settled_per_market).
  2. If that count is below MIN_SAMPLES, skip -- weights default to the
     engine.ensemble defaults.
  3. Above the threshold, build the tuning dataset and brute-force the
     Brier-minimizing blend across the 5% weight grid (same math the
     manual ensemble_tune CLI runs).
  4. Write the winning weights to model_overrides under key
     ENSEMBLE_<market> with a 14-day expiry so stale weights roll off
     if we stop logging for any reason.
  5. Log a summary line per market so the nightly JSON log captures
     what happened.

Idempotent and safe to call on an empty database -- below-threshold
markets simply don't touch model_overrides.

Usage:
    python -m engine.ensemble_auto_tune mlb
    python -m engine.ensemble_auto_tune mlb --min-samples 300 --days 365
    python -m engine.ensemble_auto_tune all -v        # every sport
"""

from __future__ import annotations

import argparse
import json
import logging

logger = logging.getLogger(__name__)


SUPPORTED_SPORTS = ("mlb", "nhl", "nba")


def auto_tune_sport(sport: str, min_samples: int | None = None,
                     days: int = 365, dry_run: bool = False) -> dict:
    """Run the auto-tuner for one sport. Returns a summary dict."""
    from .prediction_log import (
        DEFAULT_MIN_SAMPLES, MARKETS,
        count_settled_per_market, build_tuning_dataset,
    )
    from .ensemble_tune import best_weights
    from .model_overrides import apply_override

    floor = min_samples if min_samples is not None else DEFAULT_MIN_SAMPLES

    counts = count_settled_per_market(sport, days=days)
    summary: dict = {
        "sport": sport,
        "min_samples": floor,
        "days": days,
        "counts": counts,
        "tuned": {},
        "skipped": {},
    }

    tunable = [m for m, c in counts.items() if c >= floor]
    if not tunable:
        summary["skipped"] = {
            m: f"only {c} settled (need {floor})" for m, c in counts.items()
        }
        return summary

    ds = build_tuning_dataset(sport, days=days, markets=tunable)

    for market in tunable:
        m = ds.get(market)
        if not m:
            summary["skipped"][market] = "dataset empty"
            continue
        actuals = m["actuals"]
        comps = m["components"]
        # Preserve a deterministic component order so the written
        # weights dict is readable in logs.
        names = sorted(comps.keys())
        arrays = [comps[n] for n in names]
        is_class = all(a in (0, 1) for a in actuals)
        weights, score = best_weights(arrays, actuals, is_classification=is_class)
        summary["tuned"][market] = {
            "n_samples": len(actuals),
            "weights": dict(zip(names, weights)),
            "score": score,
            "components": names,
        }
        if not dry_run:
            payload = dict(zip(names, weights))
            try:
                apply_override(
                    sport,
                    f"ENSEMBLE_{market}",
                    json.dumps(payload),
                    reason=f"auto-tune: n={len(actuals)}, score={score}",
                    n_samples=len(actuals),
                    p_value=None,
                    expiry_days=14,
                )
            except Exception as e:
                summary["tuned"][market]["write_error"] = str(e)

    return summary


def auto_tune_all(min_samples: int | None = None, days: int = 365,
                   dry_run: bool = False) -> dict:
    """Tune every supported sport. Returns a dict of per-sport summaries."""
    out = {}
    for sport in SUPPORTED_SPORTS:
        try:
            out[sport] = auto_tune_sport(
                sport, min_samples=min_samples, days=days, dry_run=dry_run,
            )
        except Exception as e:
            logger.warning("auto_tune_sport %s failed: %s", sport, e)
            out[sport] = {"error": str(e)}
    return out


def _print_summary(sport: str, summary: dict) -> None:
    print(f"\n── {sport.upper()} ensemble auto-tune ──")
    if "error" in summary:
        print(f"  ERROR: {summary['error']}")
        return
    counts = summary.get("counts") or {}
    if counts:
        print("  Settled samples per market:")
        for m, c in counts.items():
            marker = "✓" if c >= summary["min_samples"] else " "
            print(f"    [{marker}] {m:<16} n={c}")
    tuned = summary.get("tuned") or {}
    if tuned:
        print("  Tuned:")
        for m, info in tuned.items():
            w_str = ", ".join(f"{k}={v:.2f}" for k, v in info["weights"].items())
            print(f"    {m:<16} -> {w_str}  (score={info['score']}, "
                  f"n={info['n_samples']})")
    skipped = summary.get("skipped") or {}
    if skipped:
        print("  Skipped:")
        for m, reason in skipped.items():
            print(f"    {m:<16} {reason}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sport", choices=list(SUPPORTED_SPORTS) + ["all"])
    parser.add_argument("--min-samples", type=int, default=None,
                        help="Override DEFAULT_MIN_SAMPLES (200)")
    parser.add_argument("--days", type=int, default=365,
                        help="Only count signals logged in the last N days")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute tuned weights but DON'T write overrides")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(levelname)-7s %(message)s")

    if args.sport == "all":
        result = auto_tune_all(min_samples=args.min_samples, days=args.days,
                                dry_run=args.dry_run)
        for sport, summary in result.items():
            _print_summary(sport, summary)
    else:
        summary = auto_tune_sport(args.sport, min_samples=args.min_samples,
                                    days=args.days, dry_run=args.dry_run)
        _print_summary(args.sport, summary)

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
