"""Parity check: events-derived projections vs legacy tables.

A1's validation gate. Before any consumer cuts over to read from the
materialized views, we have to prove the projections produce
substantially equivalent output to the existing tables. This module
runs the comparison and reports deltas.

Differences to expect:
  - Direction sub-buckets — the legacy table has multiple key shapes
    (granular, fav/dog, direction, coarse). We compare the direction-
    aware shape.
  - Sampling cutoffs — legacy applied per-bucket Bayesian shrinkage
    that the projection here doesn't (yet). Expect projection's
    realized_wr to differ slightly per bucket; bucket *n* should match.

Goal: bucket-level n matches; realized_wr within 0.02 of legacy.

CLI::

    python -m engine.events_parity --sport mlb
    python -m engine.events_parity                    # all sports
"""
from __future__ import annotations

import logging
import json
from typing import Iterable

from . import events_materialize

logger = logging.getLogger(__name__)


def parity_for_sport(sport: str) -> dict:
    """Materialize calibration from events, compare to the legacy
    table's per-bucket counts. Returns a delta report."""
    proj = events_materialize.materialize_calibration(
        sport=sport, direction_aware=True,
    )
    sport_proj = proj.get(sport, {})

    # Pull legacy table snapshot
    from . import empirical_calibration
    empirical_calibration.refresh_calibration(sport)
    legacy = empirical_calibration._TABLE.get(sport, {})

    # Compare per (bet_type[|direction], bucket)
    out = {
        "sport": sport,
        "projection_keys": len(sport_proj),
        "legacy_keys": len(legacy),
        "matched_keys": 0,
        "projection_only": [],
        "legacy_only": [],
        "bucket_deltas": [],
    }

    # Legacy table key shape is (bt_lowercase, (lo, hi)) tuples whose
    # value is {n, w, alpha, beta, mean}. Build a normalized dict
    # keyed by uppercase bt for comparison against the projection's
    # bt|direction string keys.
    legacy_norm: dict[str, dict[tuple[float, float], dict]] = {}
    for k, v in legacy.items():
        if isinstance(k, tuple) and len(k) == 2:
            bt, bucket = k
            if isinstance(bt, str) and isinstance(bucket, tuple) \
                    and len(bucket) == 2 \
                    and isinstance(v, dict) and "mean" in v:
                key_upper = bt.upper()
                legacy_norm.setdefault(key_upper, {})[bucket] = v

    out["projection_keys"] = len(sport_proj)
    out["legacy_keys"] = len(legacy_norm)

    proj_key_set = set(sport_proj.keys())
    legacy_key_set = set(legacy_norm.keys())
    out["projection_only"] = sorted(proj_key_set - legacy_key_set)
    out["legacy_only"] = sorted(legacy_key_set - proj_key_set)

    for key in sorted(proj_key_set & legacy_key_set):
        out["matched_keys"] += 1
        proj_buckets = {tuple(b["bucket"]): b for b in sport_proj[key]}
        legacy_buckets = legacy_norm[key]
        for bucket, proj_cell in proj_buckets.items():
            legacy_cell = legacy_buckets.get(bucket)
            if not legacy_cell:
                continue
            proj_wr = proj_cell.get("realized_wr")
            legacy_wr = legacy_cell.get("mean")
            if proj_wr is None or legacy_wr is None:
                continue
            delta = round(proj_wr - legacy_wr, 4)
            out["bucket_deltas"].append({
                "key": key,
                "bucket": list(bucket),
                "proj_n": proj_cell.get("n"),
                "legacy_n": legacy_cell.get("n"),
                "projection_wr": proj_wr,
                "legacy_wr": round(legacy_wr, 4),
                "delta": delta,
            })
    out["bucket_deltas"].sort(key=lambda d: -abs(d["delta"]))
    out["max_abs_delta"] = max(
        (abs(d["delta"]) for d in out["bucket_deltas"]), default=0.0,
    )
    return out


def parity_all() -> dict:
    out = {}
    for sport in ("mlb", "nhl", "nba", "tennis"):
        try:
            out[sport] = parity_for_sport(sport)
        except Exception as e:
            out[sport] = {"error": str(e)}
    return out


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.events_parity")
    ap.add_argument("--sport", default=None)
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.WARNING)
    if args.sport:
        out = parity_for_sport(args.sport)
    else:
        out = parity_all()
    # Compact summary
    if isinstance(out, dict) and "sport" in out:
        s = out
        print(f"== {s['sport']} ==")
        print(f"  projection keys: {s['projection_keys']}, "
              f"legacy keys: {s['legacy_keys']}, matched: {s['matched_keys']}")
        print(f"  max abs delta: {s['max_abs_delta']}")
        print(f"  projection-only: {s['projection_only'][:6]}")
        print(f"  legacy-only:     {s['legacy_only'][:6]}")
        print(f"  bucket deltas (top 10):")
        for d in s["bucket_deltas"][:10]:
            print(f"    {d['key']:18s} {d['bucket']} "
                  f"proj_n={d['proj_n']:>4d} legacy_n={d['legacy_n']:>5d}  "
                  f"proj={d['projection_wr']:.3f}  legacy={d['legacy_wr']:.3f}  delta={d['delta']:+.3f}")
    else:
        for sport, s in out.items():
            if "error" in s:
                print(f"== {sport} ==  ERROR: {s['error']}")
                continue
            print(f"== {sport} ==  matched={s['matched_keys']}  "
                  f"max delta={s['max_abs_delta']}  "
                  f"proj-only={len(s['projection_only'])} "
                  f"legacy-only={len(s['legacy_only'])}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
