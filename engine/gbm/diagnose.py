"""
Diagnose GBM feature extraction -- check for stuck-at-default features.

Trains on historical games, but the point-in-time feature helpers may
fall back to league-average defaults when the underlying stat tables
don't have rows for that date. When that happens the GBM has nothing
to learn and you see near-coin-flip metrics.

Usage:
    python -m engine.gbm.diagnose mlb --sample 500
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from datetime import datetime, timedelta
from statistics import mean, stdev

logger = logging.getLogger(__name__)


def diagnose_mlb(sample_size: int = 500) -> dict:
    """Check feature variance across a sample of historical games."""
    from engine.db import get_conn
    from engine.gbm.features import extract_mlb_features, extract_target, FEATURE_NAMES, _DEFAULTS

    conn = get_conn()
    rows = conn.execute(
        "SELECT mlb_game_id, date, home_team_id, away_team_id, "
        "       home_pitcher_id, away_pitcher_id, venue, "
        "       home_score, away_score, home_linescore, away_linescore, "
        "       weather_temp, weather_wind, umpire "
        "FROM games WHERE status = 'final' "
        "ORDER BY RANDOM() LIMIT ?",
        (sample_size,),
    ).fetchall()
    if not rows:
        return {"error": "no games"}

    n = 0
    # Count how many games have each feature stuck at the default value
    stuck_counts: Counter = Counter()
    feature_values: dict[str, list[float]] = {f: [] for f in FEATURE_NAMES}
    targets_seen = Counter()
    dates_seen: list[str] = []

    for r in rows:
        g = dict(r)
        f = extract_mlb_features(conn, g)
        if not f:
            continue
        t = extract_target(g)
        n += 1
        dates_seen.append(g["date"][:7])  # year-month
        for name in FEATURE_NAMES:
            v = f.get(name)
            feature_values[name].append(v)
            # Stuck-at-default check (with tiny tolerance for float drift)
            default = _DEFAULTS.get(name)
            if default is not None and v is not None and abs(v - default) < 1e-6:
                stuck_counts[name] += 1
        for k, v in (t or {}).items():
            if k in ("home_win", "nrfi_hit", "f5_home_win"):
                targets_seen[f"{k}_{int(v)}"] += 1

    out = {
        "n_games_sampled": n,
        "date_month_histogram": dict(Counter(dates_seen).most_common(10)),
        "target_class_balance": dict(targets_seen),
        "features": {},
    }
    for name in FEATURE_NAMES:
        vals = [v for v in feature_values[name] if v is not None]
        if not vals:
            out["features"][name] = {"all_none": True}
            continue
        out["features"][name] = {
            "stuck_at_default_pct": round(stuck_counts[name] / n * 100, 1),
            "min": round(min(vals), 3),
            "max": round(max(vals), 3),
            "mean": round(mean(vals), 3),
            "std": round(stdev(vals) if len(vals) > 1 else 0, 3),
        }
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sport", choices=["mlb"])
    parser.add_argument("--sample", type=int, default=500)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(levelname)-7s %(message)s")

    r = diagnose_mlb(sample_size=args.sample)
    print(json.dumps(r, indent=2, default=str))

    # Summary flags
    if "error" not in r:
        print("\n── Red flags ──")
        for name, stats in r["features"].items():
            if stats.get("all_none"):
                print(f"  {name}: ALL None (not extracted at all)")
            elif stats.get("stuck_at_default_pct", 0) > 70:
                print(f"  {name}: {stats['stuck_at_default_pct']}% at default "
                      f"(std={stats.get('std')})")
        tb = r.get("target_class_balance", {})
        total = sum(tb.values())
        if total:
            print(f"\n── Class balance ──")
            print(f"  home_win=1:     {tb.get('home_win_1', 0)}  "
                  f"({tb.get('home_win_1', 0) / max(1, tb.get('home_win_1', 0) + tb.get('home_win_0', 0)) * 100:.1f}%)")
            print(f"  nrfi_hit=1:     {tb.get('nrfi_hit_1', 0)}")
            print(f"  f5_home_win=1:  {tb.get('f5_home_win_1', 0)}")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
