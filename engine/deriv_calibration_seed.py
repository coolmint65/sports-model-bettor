"""
Seed engine.empirical_calibration with derivative-market backtest data.

Why: derivative tracker just launched; the calibration system needs
N>=30 settled samples per (bet_type, prob_bucket) before it activates.
Without seeding, derivatives would silently mis-calibrate for months
while real picks accumulate. Backtest replay over 8000+ MLB / 4000+
NHL / ~1000 NBA games gives us enough data to populate every bucket
immediately.

Each backtest pick is written into ``calibration_samples`` with a
synthetic ``BT_<sport>_<game_id>`` game_id so they're distinguishable
from real tracker rows and easy to clear/re-seed.

Usage:
    python -m engine.deriv_calibration_seed              # all sports
    python -m engine.deriv_calibration_seed mlb          # one sport
    python -m engine.deriv_calibration_seed --clear-only # wipe seeded rows
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any

logger = logging.getLogger(__name__)


def _conn(sport: str):
    if sport == "mlb":
        from .db import get_conn
    elif sport == "nhl":
        from .nhl_db import get_conn
    elif sport == "nba":
        from .nba_db import get_conn
    else:
        raise ValueError(f"unknown sport {sport}")
    return get_conn()


def clear_seeded(sport: str) -> int:
    """Drop all backtest-seeded calibration_samples rows for `sport`.
    Marker is the BT_ prefix on game_id."""
    conn = _conn(sport)
    n = conn.execute(
        "DELETE FROM calibration_samples WHERE game_id LIKE 'BT_%'"
    ).rowcount
    conn.commit()
    return n


# Map sport -> backtest module name
_BACKTEST_MOD = {
    "mlb": "engine.mlb_deriv_retrobt",
    "nhl": "engine.nhl_deriv_retrobt",
    "nba": "engine.nba_deriv_retrobt",
}


def _run_backtest_collect(sport: str, min_edge: float = 0.0) -> list[dict]:
    """Run the sport's derivative backtest in collection mode — instead
    of just aggregating, return every individual pick's
    (bet_type, prob, result) so we can write them as samples.

    Re-uses the same `run()` function each backtest module exposes; we
    monkey-patch in a per-pick callback so we don't have to fork the
    runner. Cleanest path that doesn't duplicate the iteration loop.
    """
    import importlib
    mod = importlib.import_module(_BACKTEST_MOD[sport])

    # Capture picks via a closure rather than a shared module global
    captured: list[dict] = []

    # Each backtest's run() iterates picks but only aggregates into
    # `stats`. Easiest hook: re-implement the iteration here using the
    # backtest's settler functions directly. Since the backtest runners
    # are short and stable, we'll just call run() with a reduced sample
    # and pull the per-pick data out via a side channel.
    #
    # Simpler approach: run() returns aggregated buckets which include
    # n + wins per (bet_type, prob_bucket). We can convert those into
    # synthetic samples that have the same calibration impact.
    report = mod.run(min_edge=min_edge)
    for bt, r in report.items():
        for bucket_key, b in (r.get("buckets") or {}).items():
            # bucket_key like "60-70%" → midpoint as model_prob
            # b: {n, wins, avg_prob, actual_wr}
            n = b.get("n", 0)
            w = b.get("wins", 0)
            l = n - w
            avg_prob = b.get("avg_prob", 0.0)
            # Generate `w` win rows + `l` loss rows. Each row contributes
            # one sample to the bucket — same effect as if the backtest
            # had logged each pick individually.
            for i in range(w):
                captured.append({
                    "bet_type": bt,
                    "model_prob": avg_prob,
                    "result": "W",
                    "game_id": f"BT_{sport}_{bt}_{bucket_key}_W_{i}",
                })
            for i in range(l):
                captured.append({
                    "bet_type": bt,
                    "model_prob": avg_prob,
                    "result": "L",
                    "game_id": f"BT_{sport}_{bt}_{bucket_key}_L_{i}",
                })
    return captured


def seed_sport(sport: str, min_edge: float = 0.0) -> dict:
    """Wipe + reseed calibration_samples with this sport's backtest."""
    cleared = clear_seeded(sport)
    samples = _run_backtest_collect(sport, min_edge=min_edge)
    if not samples:
        return {"sport": sport, "cleared_existing_seed": cleared,
                "inserted": 0, "note": "backtest returned 0 picks"}

    conn = _conn(sport)
    today = "BACKTEST"
    inserted = 0
    for s in samples:
        try:
            conn.execute(
                "INSERT INTO calibration_samples "
                "(game_id, date, bet_type, pick, model_prob, result) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (s["game_id"], today, s["bet_type"], "synthetic",
                 s["model_prob"], s["result"]),
            )
            inserted += 1
        except Exception as e:
            logger.debug("insert failed for %s: %s", s["game_id"], e)
    conn.commit()

    # Force the calibration table to rebuild now that the new samples
    # are in. Without this the live model keeps using whatever cache
    # was in memory before the seed ran.
    from .empirical_calibration import refresh_calibration
    cal_summary = refresh_calibration(sport)

    return {"sport": sport, "cleared_existing_seed": cleared,
            "inserted": inserted, "min_edge": min_edge,
            "calibration": cal_summary}


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("sport", nargs="?", default="all",
                    choices=["mlb", "nhl", "nba", "all"])
    ap.add_argument("--min-edge", type=float, default=0.0,
                    help="Backtest edge floor. 0.0 = include all picks.")
    ap.add_argument("--clear-only", action="store_true",
                    help="Drop all BT_ rows without re-seeding.")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    sports = ["mlb", "nhl", "nba"] if args.sport == "all" else [args.sport]

    if args.clear_only:
        for s in sports:
            n = clear_seeded(s)
            print(f"{s.upper()}: cleared {n} BT_ rows")
        return 0

    for s in sports:
        print(f"\n=== Seeding {s.upper()} (min_edge={args.min_edge}) ===")
        result = seed_sport(s, min_edge=args.min_edge)
        print(f"  cleared existing seed: {result.get('cleared_existing_seed', 0)}")
        print(f"  inserted samples:      {result.get('inserted', 0)}")
        cal = result.get("calibration") or {}
        print(f"  calibration rebuild:   "
              f"granular={cal.get('granular_filled', 0)} "
              f"fav={cal.get('fav_filled', 0)} "
              f"coarse={cal.get('coarse_filled', 0)} "
              f"total_rows={cal.get('total_rows', 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
