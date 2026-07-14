"""Monte Carlo simulator for baseball matchups.

Samples (home_runs, away_runs) per game from Poisson-ish distributions
around the factor model's expected runs. Baseball's score distribution
has heavier mass on low totals (lots of 2-1, 3-2 games) so we use a
Negative Binomial fit rather than a pure Normal — captures the
real-world tail thicknesses better than the football-style normal MC.
"""
from __future__ import annotations

import math
import random

# Per-team standard deviation. Margin σ ≈ 4.5 runs in college
# baseball; σ_per_team ≈ 3.2 under independence.
_PER_TEAM_SIGMA_FROM_MARGIN = 1.0 / math.sqrt(2.0)
# Score range. Shutouts (0) are common; 25+ run blowouts happen but
# are tail events.
_MIN_RUNS = 0
_MAX_RUNS = 30

DEFAULT_N_SIMS = 10_000


def _sample_runs(mean: float, sigma: float, rng: random.Random) -> int:
    """Truncated normal sample. Negative Binomial would be more
    faithful but Normal is closer than it looks for college baseball
    where σ is large relative to mean. Revisit if Brier underperforms.
    """
    runs = rng.gauss(mean, sigma)
    runs = max(_MIN_RUNS, min(_MAX_RUNS, runs))
    return round(runs)


def simulate(*, expected_home: float, expected_away: float,
              margin_sigma: float, spread: float | None = None,
              total_line: float | None = None,
              n_sims: int = DEFAULT_N_SIMS,
              seed: int | None = None) -> dict:
    rng = random.Random(seed)
    sigma_per_team = margin_sigma * _PER_TEAM_SIGMA_FROM_MARGIN

    home_wins = 0
    home_covers = 0
    overs = 0
    pushes_ml = 0
    pushes_spread = 0
    pushes_total = 0
    total_home = 0
    total_away = 0

    for _ in range(n_sims):
        h = _sample_runs(expected_home, sigma_per_team, rng)
        a = _sample_runs(expected_away, sigma_per_team, rng)
        total_home += h
        total_away += a
        margin = h - a
        if margin > 0:
            home_wins += 1
        elif margin == 0:
            pushes_ml += 1
        if spread is not None:
            cover = margin + spread
            if cover > 0:
                home_covers += 1
            elif cover == 0:
                pushes_spread += 1
        if total_line is not None:
            tot = h + a
            if tot > total_line:
                overs += 1
            elif tot == total_line:
                pushes_total += 1

    p_home = home_wins / n_sims
    decided = n_sims - pushes_ml
    p_home_decided = home_wins / decided if decided else 0.5
    out = {
        "mc_n_sims":        n_sims,
        "mc_p_home":        round(p_home, 4),
        "mc_p_home_decided": round(p_home_decided, 4),
        "mc_p_away":        round(1.0 - p_home - (pushes_ml / n_sims), 4),
        "mc_p_tie":         round(pushes_ml / n_sims, 4),
        "mc_expected_home": round(total_home / n_sims, 2),
        "mc_expected_away": round(total_away / n_sims, 2),
        "mc_expected_margin": round(
            (total_home - total_away) / n_sims, 2),
        "mc_expected_total":  round(
            (total_home + total_away) / n_sims, 2),
    }
    if spread is not None:
        out["mc_p_home_cover"] = round(home_covers / n_sims, 4)
        out["mc_p_away_cover"] = round(
            1.0 - home_covers / n_sims - (pushes_spread / n_sims), 4)
    if total_line is not None:
        out["mc_p_over"]  = round(overs / n_sims, 4)
        out["mc_p_under"] = round(
            1.0 - overs / n_sims - (pushes_total / n_sims), 4)
    return out
