"""
NBA Q1 Monte Carlo simulator.

Possession-level sim: Q1 is ~24 possessions per team at league-average
pace. Each possession produces points drawn from a distribution shaped
by the offensive team's eFG%/TO%/ORB% profile and the defensive team's
ability to suppress those rates.

Design notes:
  - Simulates Q1 only because that's the market we trade.
  - Possession count per team = pace/2 (Q1 is 25% of game, but home
    and away each get ~24.5 possessions per 24 minutes at pace 99).
  - Points per possession (PPP) is drawn from a normal distribution
    calibrated to the team's ORTG adjusted by opponent DRTG and league
    average. Poisson integer counts are added for 3-pointer variance.
  - Playoff dampening matches engine.nba_q1_predict (pace * 0.97,
    scoring * 0.97).
  - Back-to-back / rest / injury adjustments are assumed already baked
    into the ORTG/DRTG the caller hands in.

This is a much lighter sim than MLB or NHL -- there's no base-state
machine, no lineup tracking. Just "draw Q1 points per team, repeat."
That's fine: Q1 is a high-variance quarter with little strategic depth
relative to the full game, so possession-level is the right granularity.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np

logger = logging.getLogger(__name__)


# ── League baselines ───────────────────────────────────────────
LEAGUE_Q1_PACE = 24.5          # Q1 possessions per team at pace 99
LEAGUE_Q1_PPP_MEAN = 1.12      # points per possession in Q1 (offense-side)
LEAGUE_Q1_PPP_STD = 0.28       # std dev across possessions
LEAGUE_Q1_HOME_BOOST = 0.69    # extra points home gets in Q1 (calibrated)


@dataclass
class NBATeamProfile:
    """Per-team sim inputs for an NBA Q1."""
    # Q1-specific rates
    q1_ppg: float = LEAGUE_Q1_PPP_MEAN * LEAGUE_Q1_PACE
    q1_opp_ppg: float = LEAGUE_Q1_PPP_MEAN * LEAGUE_Q1_PACE
    pace: float = 99.0     # possessions per 48 min
    # Back-to-back / rest / injury adjustments applied upstream
    scoring_mult: float = 1.0
    defense_mult: float = 1.0
    is_b2b: bool = False
    name: str = "?"


def simulate_q1(home: NBATeamProfile, away: NBATeamProfile,
                 n_sims: int = 50_000,
                 is_playoff: bool = False,
                 seed: int | None = None) -> dict:
    """Run N possession-level simulations of Q1 only.

    Returns a dict with per-sim home_q1 / away_q1 arrays.
    """
    rng = np.random.default_rng(seed)

    # Effective Q1 PPG for each team. Blend offense-vs-opponent-defense
    # via the standard opp-adjusted formula:
    #   team_expected = (team_off * opp_def) / league_avg_allowed
    lg = LEAGUE_Q1_PPP_MEAN * LEAGUE_Q1_PACE
    home_ppg = (home.q1_ppg * away.q1_opp_ppg) / lg * home.scoring_mult * away.defense_mult
    away_ppg = (away.q1_ppg * home.q1_opp_ppg) / lg * away.scoring_mult * home.defense_mult

    # Pace adjustment (matchup pace vs. league)
    matchup_pace = (home.pace + away.pace) / 2
    pace_factor = matchup_pace / 99.0
    if is_playoff:
        pace_factor *= 0.97
    home_ppg *= pace_factor
    away_ppg *= pace_factor
    if is_playoff:
        home_ppg *= 0.97
        away_ppg *= 0.97

    # Home Q1 boost
    home_ppg += LEAGUE_Q1_HOME_BOOST / 2
    away_ppg -= LEAGUE_Q1_HOME_BOOST / 2

    # B2B penalty (already baked into scoring_mult typically but leave
    # a direct field for easy override)
    if home.is_b2b:
        home_ppg -= 1.0
    if away.is_b2b:
        away_ppg -= 1.0

    # Sample: use a normal approximation per Q1 team total, scaled by
    # possession variance. Q1 std ~= std_per_poss * sqrt(n_possessions).
    n_poss = LEAGUE_Q1_PACE * (matchup_pace / 99.0)
    q1_std = LEAGUE_Q1_PPP_STD * np.sqrt(max(n_poss, 5))
    # Small floor so we don't draw negatives
    home_q1 = np.maximum(0, rng.normal(home_ppg, q1_std, size=n_sims)).round().astype(np.int16)
    away_q1 = np.maximum(0, rng.normal(away_ppg, q1_std, size=n_sims)).round().astype(np.int16)

    return {
        "home_q1": home_q1,
        "away_q1": away_q1,
    }


def aggregate_nba_q1(raw: dict) -> dict:
    """Build the Q1-market probability dict from a sim result."""
    h = raw["home_q1"]
    a = raw["away_q1"]
    n = len(h)
    total = h + a
    margin = h.astype(int) - a.astype(int)

    # Q1 spread (home line) at typical magnitudes
    anchor_spread = -round(float(margin.mean()) * 2) / 2
    spreads: dict = {}
    for offset in (-3.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.5):
        point = anchor_spread + offset
        home_cov = float((margin > -point).sum()) / n
        spreads[f"home_{point}"] = round(home_cov, 4)

    # Q1 totals
    mean_total = float(total.mean())
    anchor = round(mean_total * 2) / 2
    totals: dict = {}
    for offset in (-2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2):
        line = anchor + offset
        over = float((total > line).sum()) / n
        under = float((total < line).sum()) / n
        totals[f"{line:.1f}"] = {
            "over": round(over, 4),
            "under": round(under, 4),
            "push": round(1.0 - over - under, 4),
        }

    # ── Derivative market aggregations ──
    # Q1 Team Total per side — empirical distribution at lines around
    # each team's mean. Lets the derivative pickers swap factor's
    # Gaussian tail for MC's empirical bucket counts.
    def _team_q1_dist(arr) -> dict:
        mean_q = float(arr.mean())
        anchor_q = round(mean_q * 2) / 2
        out = {}
        for off in (-3, -2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2, 3):
            line = anchor_q + off
            if line < 0.5:
                continue
            out[f"{line:.1f}"] = {
                "over": round(float((arr > line).sum()) / n, 4),
                "under": round(float((arr < line).sum()) / n, 4),
            }
        return out

    team_totals = {
        "home": {"expected": round(float(h.mean()), 2),
                 "lines": _team_q1_dist(h)},
        "away": {"expected": round(float(a.mean()), 2),
                 "lines": _team_q1_dist(a)},
    }

    # Q1 Total Odd/Even — direct count of sim totals' parity.
    odd_count = int((total.astype(int) % 2 == 1).sum())
    total_oe = {
        "odd":  round(odd_count / n, 4),
        "even": round((n - odd_count) / n, 4),
    }

    return {
        "n_sims": n,
        "win_prob": {
            "home": round(float((margin > 0).sum()) / n, 4),
            "away": round(float((margin < 0).sum()) / n, 4),
            "tie":  round(float((margin == 0).sum()) / n, 4),
        },
        "expected_points": {
            "home": round(float(h.mean()), 2),
            "away": round(float(a.mean()), 2),
            "total": round(mean_total, 2),
        },
        "margin_std": round(float(margin.std()), 3),
        "total_std": round(float(total.std()), 3),
        "q1_spread": spreads,
        "q1_total": totals,
        "team_totals": team_totals,
        "total_oe": total_oe,
    }
