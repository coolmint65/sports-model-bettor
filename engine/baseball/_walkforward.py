"""Walk-forward calibration for baseball framework leagues.

For each (bet_type, prob_bucket) pair, replay history chronologically,
predict every game using only PRE-game Elo state (no leakage), and
record how often the model's prob bucket actually realized. Persists
``data/baseball/{league}_calibration.json`` in the same shape the
framework_calibration loader expects, so picks_core's calibration
shrinkage runs automatically.

Bucket grid matches the basketball / soccer walk-forward so the on-
disk format is interchangeable.
"""
from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from . import LEAGUE_REGISTRY, get_league_config
from ._db import get_conn
from ._elo import update as elo_update, expected_score, INIT_ELO

logger = logging.getLogger(__name__)

_OUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "baseball"


_BUCKETS = [
    (0.30, 0.40),
    (0.40, 0.50),
    (0.50, 0.55),
    (0.55, 0.60),
    (0.60, 0.65),
    (0.65, 0.70),
    (0.70, 0.80),
    (0.80, 1.01),
]


def _bucket_for(p: float) -> tuple[float, float] | None:
    for lo, hi in _BUCKETS:
        if lo <= p < hi:
            return lo, hi
    return None


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _favored(p: float, won: int) -> tuple[float, int]:
    """Always bucket the favored side so calibration mirrors the
    picker's selection bias (it only fires the favored leg of a binary
    market)."""
    if p >= 0.5:
        return p, won
    return 1.0 - p, 1 - won


def seed_calibration(league: str, *, holdout_days: int = 90) -> dict:
    """Walk every finalized game chronologically, predict the markets
    with the live ensemble (factor + MC where cheap), bucket by raw
    prob, persist realized hit-rate per bucket.

    For NCAA we use a shorter holdout (90 days) than soccer's 240 since
    the season is only ~3 months long — using the standard 240 would
    eat the whole sample.
    """
    if league not in LEAGUE_REGISTRY:
        raise KeyError(f"unknown baseball league {league!r}")
    cfg = get_league_config(league)
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT date, home_team_id, away_team_id, home_score, away_score "
        "FROM games WHERE status='final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date ASC, game_id ASC"
    ).fetchall()
    if not rows:
        raise ValueError(f"no finalized games for {league!r}")

    # Replay everything — calibration is per-game, not split into a
    # train/test slab. For each game we predict with the Elo state
    # BEFORE the game, then update Elo with the realized result.
    # Buckets only accept games where the model's predicted prob lies
    # in [0.30, 1.01]. (We don't need to mirror selection bias here
    # because we record both sides via `_favored` below.)
    hfa = cfg.get("home_advantage_elo") or 30.0
    margin_sigma = cfg.get("margin_sigma") or 4.5
    total_sigma = cfg.get("total_sigma") or 4.8
    avg_total = cfg.get("league_avg_total") or 12.5
    elo_per_run = 100.0

    ratings: dict[int, float] = defaultdict(lambda: INIT_ELO)
    # Per-bucket: [n_samples, sum_pred, n_wins] so we can compute both
    # avg_pred (mean predicted prob in the bucket) and realized_wr
    # (mean realized outcome). framework_calibration's loader expects
    # both fields — without avg_pred it falls back to raw passthrough.
    buckets_ml = defaultdict(lambda: [0, 0.0, 0])
    buckets_total = defaultdict(lambda: [0, 0.0, 0])
    buckets_spread = defaultdict(lambda: [0, 0.0, 0])
    n_predicted = 0

    for r in rows:
        h_id = int(r["home_team_id"]); a_id = int(r["away_team_id"])
        r_h = ratings.get(h_id, INIT_ELO); r_a = ratings.get(a_id, INIT_ELO)
        h_score = int(r["home_score"]); a_score = int(r["away_score"])

        # ML prob (factor leg only — closed-form is the most sample-
        # efficient predictor, and we want a calibration map that
        # describes the factor signal regardless of GBM availability).
        p_home = expected_score(r_h, r_a, hfa)
        won_h = 1 if h_score > a_score else 0
        sp, sw = _favored(p_home, won_h)
        bk = _bucket_for(sp)
        if bk:
            buckets_ml[bk][0] += 1
            buckets_ml[bk][1] += sp
            buckets_ml[bk][2] += sw

        # Total prob — probe a line ±1 run around the average total
        # and bucket whichever direction the model leaned past 50%.
        expected_margin = (r_h - r_a + hfa) / elo_per_run
        expected_total = avg_total + 0  # symmetric around avg
        actual_total = h_score + a_score
        for line in (avg_total - 1, avg_total, avg_total + 1):
            z = (line - expected_total) / total_sigma
            p_under = _norm_cdf(z)
            p_over = 1.0 - p_under
            # Pick whichever side the model thinks is more likely so
            # the bucket reflects the picker's selection direction.
            if p_over >= 0.5:
                sp, sw = p_over, (1 if actual_total > line else 0)
            else:
                sp, sw = p_under, (1 if actual_total < line else 0)
            bk = _bucket_for(sp)
            if bk:
                buckets_total[bk][0] += 1
                buckets_total[bk][1] += sp
                buckets_total[bk][2] += sw

        # Spread cover — probe ±1.5 runs around the predicted margin.
        margin = h_score - a_score
        for spread in (-1.5, -0.5, 0.5, 1.5):
            z = (-spread - expected_margin) / margin_sigma
            p_home_cover = 1.0 - _norm_cdf(z)
            won_cover = 1 if (margin + spread) > 0 else 0
            sp, sw = _favored(p_home_cover, won_cover)
            bk = _bucket_for(sp)
            if bk:
                buckets_spread[bk][0] += 1
                buckets_spread[bk][1] += sp
                buckets_spread[bk][2] += sw

        # Roll Elo forward
        nh, na = elo_update(r_h, r_a,
                              home_score=h_score, away_score=a_score,
                              hfa=hfa)
        ratings[h_id] = nh; ratings[a_id] = na
        n_predicted += 1

    def _bucket_table(buckets: dict) -> list[dict]:
        out = []
        for lo, hi in _BUCKETS:
            n, sum_pred, wins = buckets.get((lo, hi), (0, 0.0, 0))
            avg_pred = (sum_pred / n) if n else None
            realized = (wins / n) if n else None
            out.append({
                "bucket":      [lo, hi],
                "n":           n,
                # Names match what framework_calibration's loader looks
                # for (avg_pred + realized_wr) so picks_core's
                # calibration shrinkage picks them up automatically.
                "avg_pred":    round(avg_pred, 4) if avg_pred is not None else None,
                "realized_wr": round(realized, 4) if realized is not None else None,
            })
        return out

    out = {
        "league":     league,
        "fitted_at":  datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n_games":    n_predicted,
        "buckets":    {
            "ML":     _bucket_table(buckets_ml),
            "TOTAL":  _bucket_table(buckets_total),
            "SPREAD": _bucket_table(buckets_spread),
        },
    }
    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = _OUT_DIR / f"{league}_calibration.json"
    path.write_text(json.dumps(out, indent=2))
    logger.info("[baseball:%s] calibration seeded -> %s (n=%d)",
                league, path.name, n_predicted)
    return out


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.baseball._walkforward")
    ap.add_argument("league")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    res = seed_calibration(args.league)
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
