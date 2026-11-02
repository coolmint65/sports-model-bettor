"""Monte Carlo simulator for football matchups.

Samples (home_pts, away_pts) from independent normals around the
factor-model's expected scores, clipped to a realistic football
range. Used alongside the closed-form Normal in ``predict_match`` so
V3.2 has a second signal leg to surface and the ensemble blend has
something to weight.

Why a second signal at all when the closed-form solution gives
similar numbers analytically? Two practical reasons:

  1. Joint outcomes — cover + total + ML in one sim run gives
     internally-consistent probabilities. Closed-form math gives
     each independently and they can drift apart by 1-2% on edge
     cases (e.g. high-total games with big spreads).
  2. Tail probabilities — Normal underweights big upsets. Sampling
     gives realistic tail mass without re-deriving moment-matching
     transforms per market.

When UFL accumulates 4+ seasons of data we can swap the Normal
sampler for a discrete-score model that captures the 3s/7s
clustering football has; for n=110 the Normal is the right
sample-efficiency trade-off.
"""
from __future__ import annotations

import math
import random

import numpy as np

# Per-team standard deviation. The margin σ from `_calibrate` (~13.2
# for UFL) splits into σ_per_team = σ_margin / sqrt(2) ≈ 9.4 under the
# independence assumption. Refit when the σ_margin constant changes.
_PER_TEAM_SIGMA_FROM_MARGIN = 1.0 / math.sqrt(2.0)
# Score floor/ceiling. Football shutouts happen ~5% of the time
# (genuinely 0 is rare even in blowouts); 70+ is rarer than a moon
# landing. Clipping keeps wild tail samples from blowing up averages.
_MIN_PTS = 0
_MAX_PTS = 70

DEFAULT_N_SIMS = 10_000


def _sample_score(mean: float, sigma: float, rng: random.Random) -> int:
    """One independent normal sample, clipped + rounded to integer."""
    pts = rng.gauss(mean, sigma)
    pts = max(_MIN_PTS, min(_MAX_PTS, pts))
    return round(pts)


def simulate(*, expected_home: float, expected_away: float,
              margin_sigma: float, spread: float | None = None,
              total_line: float | None = None,
              margin_samples=None, margin_mean: float = 0.0,
              n_sims: int = DEFAULT_N_SIMS,
              return_samples: bool = False,
              seed: int | None = None) -> dict:
    """Run ``n_sims`` independent samples. Returns realized
    probabilities for the markets the picker cares about."""
    rng = np.random.default_rng(seed)
    sigma_per_team = margin_sigma * _PER_TEAM_SIGMA_FROM_MARGIN

    expected_margin = expected_home - expected_away
    if margin_samples:
        # Key-number-aware: MARGIN from the empirical dist (shifted to expected_margin),
        # TOTAL from the Normal, scores derived so the total bet stays realistic.
        margin = rng.choice(np.asarray(margin_samples, dtype=float),
                            size=n_sims) + (expected_margin - margin_mean)
        tot_s = rng.normal(expected_home + expected_away, margin_sigma, n_sims)
        h = (tot_s + margin) / 2.0
        a = (tot_s - margin) / 2.0
    else:
        h = np.round(np.clip(
            rng.normal(expected_home, sigma_per_team, n_sims),
            _MIN_PTS, _MAX_PTS))
        a = np.round(np.clip(
            rng.normal(expected_away, sigma_per_team, n_sims),
            _MIN_PTS, _MAX_PTS))
        margin = h - a

    total_home_pts = float(h.sum())
    total_away_pts = float(a.sum())
    home_wins = int((margin > 0).sum())
    pushes_ml = int((margin == 0).sum())

    home_covers = 0
    pushes_spread = 0
    if spread is not None:
        # Home covers ``spread`` iff (home - away + spread) > 0.
        # Football lines are typically half-points so a literal
        # push is rare, but record it when the line is whole.
        cover = margin + spread
        home_covers = int((cover > 0).sum())
        pushes_spread = int((cover == 0).sum())

    overs = 0
    pushes_total = 0
    if total_line is not None:
        tot = h + a
        overs = int((tot > total_line).sum())
        pushes_total = int((tot == total_line).sum())

    p_home = home_wins / n_sims
    # Excluding ties from ML denominator so the prob is "home wins
    # given a decision," matching how the books quote NFL ML.
    decided = n_sims - pushes_ml
    p_home_decided = home_wins / decided if decided else 0.5

    out = {
        "mc_n_sims":       n_sims,
        "mc_p_home":       round(p_home, 4),
        "mc_p_home_decided": round(p_home_decided, 4),
        "mc_p_away":       round(1.0 - p_home - (pushes_ml / n_sims), 4),
        "mc_p_tie":        round(pushes_ml / n_sims, 4),
        "mc_expected_home": round(total_home_pts / n_sims, 2),
        "mc_expected_away": round(total_away_pts / n_sims, 2),
        "mc_expected_margin": round(
            (total_home_pts - total_away_pts) / n_sims, 2),
        "mc_expected_total":  round(
            (total_home_pts + total_away_pts) / n_sims, 2),
    }
    if spread is not None:
        out["mc_p_home_cover"] = round(home_covers / n_sims, 4)
        out["mc_p_away_cover"] = round(
            1.0 - home_covers / n_sims - (pushes_spread / n_sims), 4)
    if total_line is not None:
        out["mc_p_over"]  = round(overs / n_sims, 4)
        out["mc_p_under"] = round(
            1.0 - overs / n_sims - (pushes_total / n_sims), 4)
    if return_samples:
        # raw per-sim scores so the derivative pricer can count any line
        # (alt ladders / team totals / margin / OT) off the SAME sim run.
        out["_h"] = h
        out["_a"] = a
    return out
