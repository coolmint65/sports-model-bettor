"""Soccer halftime live predictor.

Fires once per match at halftime: observed H1 scores become facts, the
remaining 45 minutes get re-projected using a Bayesian update of the
prematch 2H lambdas.

Math
----
Prematch model gives full-game Poisson rates (``lambda_home``,
``lambda_away``). Split into per-half rates using ``h1_share`` from
the league's fitted constants (≈ 0.42-0.48). The 2H expectation is::

    lambda_2H_x = lambda_full_x * (1 - h1_share)

At halftime we know what actually happened in H1. That information
shifts our belief about scoring pace in the 2H — if a team is
shooting 1.5x expected and pacing 1.5x expected, the 2H rate is
probably higher than the prematch number. We apply a *capped*
multiplicative pull::

    ratio   = observed_H1_x / expected_H1_x  (clamp [0.5, 2.0])
    new_lambda_2H_x = lambda_2H_x * (1 - PULL + PULL * ratio)

``PULL=0.3`` reflects "H1 carries some signal but it's only 45 min of
play and the prematch model already has the long-run profile". Cap
prevents one freak H1 from blowing up the 2H projection.

Final-game distribution is a sum of constants + Poissons:
``final_x = observed_H1_x + Poisson(adjusted_2H_x)``. We build a
joint score grid the same way ``_predict.predict_match`` does, just
with each cell offset by the observed H1 score.

Output matches ``predict_match`` so downstream pickers consume it
unchanged.
"""
from __future__ import annotations

import logging

from ._config import get_league_config
from ._predict import (
    DEFAULT_DC_RHO, _dixon_coles_tau, _pmf_poisson, predict_match,
)

logger = logging.getLogger(__name__)


# Pull strength: 0.0 = ignore H1 entirely, 1.0 = use observed pace as
# the full 2H expectation. 0.3 keeps the prematch profile dominant
# (it's been fit on hundreds of matches; H1 is one 45-min sample).
_H1_PULL = 0.3
_RATIO_LO = 0.5
_RATIO_HI = 2.0


def _adjusted_2h_lambda(prior_2h: float, observed_h1: float,
                          expected_h1: float) -> float:
    """Multiplicative Bayesian pull toward observed H1 pace. Clamped
    so an extreme 4-0 H1 against a 0.5-expected scoreline doesn't
    push the 2H lambda past 2x the prior."""
    if expected_h1 <= 0.05:
        return prior_2h
    ratio = observed_h1 / expected_h1
    ratio = max(_RATIO_LO, min(_RATIO_HI, ratio))
    return prior_2h * (1 - _H1_PULL + _H1_PULL * ratio)


def _final_grid(observed_h: int, observed_a: int,
                  lam_2h_h: float, lam_2h_a: float, rho: float,
                  k_max: int = 8) -> list[list[float]]:
    """Final-score distribution = observed H1 + Poisson(2H). Returns a
    grid indexed by FINAL scores starting at (observed_h, observed_a).
    Each cell carries the joint probability."""
    pmf_h = _pmf_poisson(lam_2h_h, k_max)
    pmf_a = _pmf_poisson(lam_2h_a, k_max)
    # Build a grid sized to fit observed + max 2H goals.
    grid = [[0.0] * (observed_a + k_max + 1)
             for _ in range(observed_h + k_max + 1)]
    for dh in range(k_max + 1):
        for da in range(k_max + 1):
            fh = observed_h + dh
            fa = observed_a + da
            joint = pmf_h[dh] * pmf_a[da]
            # Dixon-Coles low-score tau correction only re-shapes the
            # joint at (0,0)/(0,1)/(1,0)/(1,1) — those points map to
            # offsets (dh, da) in the 2H delta space. The DC adjustment
            # is meant for the full-game low-score cluster, which 2H-
            # only doesn't have the same prior on. Skip the tau here.
            grid[fh][fa] = joint
    return grid


def predict_at_halftime(league: str, home_team_id: int, away_team_id: int,
                          *, h1_home: int, h1_away: int,
                          neutral_site: bool = False,
                          home_side: str | None = None) -> dict:
    """Return a halftime-adjusted prediction dict with the same shape
    as ``predict_match``. Caller supplies observed H1 scores; we
    re-derive the final-game distribution under the assumption that H1
    is locked and 2H follows a Bayesian-updated Poisson.

    Picks engine downstream consumes the same fields it always has
    (``p_home``, ``p_over_25``, ``p_btts_yes``, ``p_dc_home``, etc.),
    so live picks reuse the prematch generator unchanged.
    """
    base = predict_match(
        league, home_team_id, away_team_id, neutral_site=neutral_site,
        home_side=home_side,
    )
    cfg = get_league_config(league)
    rho = cfg.get("dc_rho") if cfg.get("dc_rho") is not None else DEFAULT_DC_RHO
    h1_share = cfg.get("h1_share") or 0.45

    lam_h_full = base["lambda_home"]
    lam_a_full = base["lambda_away"]

    # Prior 2H lambdas + expected H1 scoring.
    prior_2h_h = lam_h_full * (1 - h1_share)
    prior_2h_a = lam_a_full * (1 - h1_share)
    exp_h1_h = lam_h_full * h1_share
    exp_h1_a = lam_a_full * h1_share

    # Updated 2H lambdas after observing H1.
    lam_2h_h = _adjusted_2h_lambda(prior_2h_h, h1_home, exp_h1_h)
    lam_2h_a = _adjusted_2h_lambda(prior_2h_a, h1_away, exp_h1_a)
    lam_2h_h = min(max(lam_2h_h, 0.05), 5.0)
    lam_2h_a = min(max(lam_2h_a, 0.05), 5.0)

    # Build the final-score distribution starting from observed.
    grid = _final_grid(int(h1_home), int(h1_away), lam_2h_h, lam_2h_a, rho)

    p_home = p_draw = p_away = p_btts_yes = 0.0
    p_over = {1.5: 0.0, 2.5: 0.0, 3.5: 0.0}
    top = []
    for h in range(len(grid)):
        for a in range(len(grid[h])):
            p = grid[h][a]
            if p <= 0.0:
                continue
            if   h > a: p_home += p
            elif h < a: p_away += p
            else:        p_draw += p
            total = h + a
            for line in p_over:
                if total > line:
                    p_over[line] += p
            if h > 0 and a > 0:
                p_btts_yes += p
            top.append((h, a, p))
    top.sort(key=lambda t: -t[2])

    # Normalize defensively — Poisson tail truncated at k_max can leave
    # a tiny mass unallocated. Re-scale so legs sum to 1.
    total_mass = p_home + p_draw + p_away
    if total_mass > 0:
        p_home /= total_mass
        p_draw /= total_mass
        p_away /= total_mass

    # Final lambdas for downstream display.
    final_lam_h = float(h1_home) + lam_2h_h
    final_lam_a = float(h1_away) + lam_2h_a

    out = dict(base)
    out.update({
        "lambda_home":    final_lam_h,
        "lambda_away":    final_lam_a,
        "lambda_2h_home": lam_2h_h,
        "lambda_2h_away": lam_2h_a,
        "h1_home":        int(h1_home),
        "h1_away":        int(h1_away),
        "is_halftime":    True,

        "p_home":   p_home,
        "p_draw":   p_draw,
        "p_away":   p_away,

        "p_over_15":  p_over[1.5],
        "p_under_15": 1.0 - p_over[1.5],
        "p_over_25":  p_over[2.5],
        "p_under_25": 1.0 - p_over[2.5],
        "p_over_35":  p_over[3.5],
        "p_under_35": 1.0 - p_over[3.5],

        "p_btts_yes": p_btts_yes,
        "p_btts_no":  1.0 - p_btts_yes,

        "p_dc_home":  p_home + p_draw,
        "p_dc_away":  p_away + p_draw,
        "p_dc_draw":  p_home + p_away,

        "p_dnb_home": p_home / (p_home + p_away) if (p_home + p_away) > 0 else 0.5,
        "p_dnb_away": p_away / (p_home + p_away) if (p_home + p_away) > 0 else 0.5,

        "top_scores": [{"home": h, "away": a, "p": round(p, 4)}
                         for h, a, p in top[:6]],
    })
    return out


__all__ = ["predict_at_halftime"]
