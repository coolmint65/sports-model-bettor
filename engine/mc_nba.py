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
from dataclasses import dataclass, field

import numpy as np

from . import mc_constants as _mc

logger = logging.getLogger(__name__)


# ── League baselines ───────────────────────────────────────────
# Backed by engine.mc_constants — calibration writes fitted values to
# nba_model_config and these accessors pick them up. Module-level
# constants used to be hardcoded literals; they're kept as thin wrappers
# (LEAGUE_*) so external callers (mc_nba_run, etc.) can still reference
# the names without each having to import mc_constants directly.
LEAGUE_Q1_PACE = _mc.nba_q1_pace
LEAGUE_Q1_PPP_MEAN = _mc.nba_q1_ppp_mean
LEAGUE_Q1_PPP_STD = _mc.nba_q1_ppp_std
LEAGUE_Q1_HOME_BOOST = _mc.nba_q1_home_boost


def _q1_default_ppg() -> float:
    return _mc.nba_q1_ppp_mean() * _mc.nba_q1_pace()


@dataclass
class NBATeamProfile:
    """Per-team sim inputs for an NBA Q1."""
    # Q1-specific rates (default reads fitted values at instance creation).
    q1_ppg: float = field(default_factory=_q1_default_ppg)
    q1_opp_ppg: float = field(default_factory=_q1_default_ppg)
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
    q1_ppp_mean = _mc.nba_q1_ppp_mean()
    q1_pace = _mc.nba_q1_pace()
    q1_ppp_std = _mc.nba_q1_ppp_std()
    q1_home_boost = _mc.nba_q1_home_boost()
    lg = q1_ppp_mean * q1_pace
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
    home_ppg += q1_home_boost / 2
    away_ppg -= q1_home_boost / 2

    # B2B penalty (already baked into scoring_mult typically but leave
    # a direct field for easy override)
    if home.is_b2b:
        home_ppg -= 1.0
    if away.is_b2b:
        away_ppg -= 1.0

    # Sample: use a normal approximation per Q1 team total, scaled by
    # possession variance. Q1 std ~= std_per_poss * sqrt(n_possessions).
    n_poss = q1_pace * (matchup_pace / 99.0)
    q1_std = q1_ppp_std * np.sqrt(max(n_poss, 5))
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


# ── Full-game (48-min) simulator ─────────────────────────────────
#
# Mirrors simulate_q1 but for a full 48 minutes. Possession count per
# team scales to (full_pace / 2), points-per-possession variance
# √n_possessions wider so the shape is consistent. Calibrated to
# n=4130 backfilled games:
#   Avg total       227.89
#   Home edge       +2.14
#   Margin std dev  16.06
#   Total std dev   21.25
#   Per-team PPG    113.95
#
# Used by ensemble_nba_full to blend with the factor and GBM models
# for full-game ML / spread / total picks.

LEAGUE_FULL_PACE = _mc.nba_full_pace
LEAGUE_FULL_PPP_MEAN = _mc.nba_full_ppp_mean
# Per-team scoring std calibrated from holdout games. Per-team std =
# sqrt(margin_std^2 / 2) under independence; reality is wider because
# possessions aren't iid. The fitted constant reproduces both the
# margin_std and total_std on the calibration set.
LEAGUE_FULL_TEAM_STD = _mc.nba_full_team_std
# Shared per-game pace/environment shock — added equally to both teams'
# scores so it cancels out of the margin but compounds in the total.
# Decomposed from total_std^2 = 4*shared^2 + 2*team_std^2 once
# (margin_std, total_std, team_std) are fitted.
LEAGUE_FULL_GAME_SHOCK_STD = _mc.nba_full_game_shock_std
LEAGUE_FULL_HOME_BOOST = _mc.nba_full_home_boost


def _full_default_ppg() -> float:
    return _mc.nba_full_ppp_mean() * _mc.nba_full_pace()


@dataclass
class NBAFullProfile:
    """Per-team sim inputs for a full-game NBA matchup."""
    ppg: float = field(default_factory=_full_default_ppg)
    opp_ppg: float = field(default_factory=_full_default_ppg)
    pace: float = 99.0
    scoring_mult: float = 1.0
    defense_mult: float = 1.0
    is_b2b: bool = False
    name: str = "?"


def simulate_full(home: NBAFullProfile, away: NBAFullProfile,
                  n_sims: int = 50_000,
                  is_playoff: bool = False,
                  seed: int | None = None) -> dict:
    """Run N possession-level simulations of a full 48-minute game.

    Returns ``{"home_score": np.ndarray, "away_score": np.ndarray}``.
    """
    rng = np.random.default_rng(seed)

    full_ppp_mean = _mc.nba_full_ppp_mean()
    full_pace = _mc.nba_full_pace()
    full_home_boost = _mc.nba_full_home_boost()

    # Opp-adjusted attack vs defense.
    lg = full_ppp_mean * full_pace
    home_ppg = (home.ppg * away.opp_ppg) / lg * home.scoring_mult * away.defense_mult
    away_ppg = (away.ppg * home.opp_ppg) / lg * away.scoring_mult * home.defense_mult

    matchup_pace = (home.pace + away.pace) / 2
    pace_factor = matchup_pace / 99.0
    if is_playoff:
        pace_factor *= 0.96  # match nba_predict.PLAYOFF_PACE_FACTOR

    home_ppg *= pace_factor
    away_ppg *= pace_factor
    if is_playoff:
        home_ppg *= 0.96  # match nba_predict.PLAYOFF_SCORING_FACTOR
        away_ppg *= 0.96

    # Home-court boost.
    home_ppg += full_home_boost / 2
    away_ppg -= full_home_boost / 2

    # B2B penalty.
    if home.is_b2b:
        home_ppg -= 2.5
    if away.is_b2b:
        away_ppg -= 2.5

    # Per-team residual + shared per-game shock (see constant comment).
    team_std = _mc.nba_full_team_std()
    shock_std = _mc.nba_full_game_shock_std()
    game_shock = rng.normal(0, shock_std, size=n_sims)
    home_resid = rng.normal(0, team_std, size=n_sims)
    away_resid = rng.normal(0, team_std, size=n_sims)

    home_score = np.maximum(60, home_ppg + game_shock + home_resid).round().astype(np.int16)
    away_score = np.maximum(60, away_ppg + game_shock + away_resid).round().astype(np.int16)

    return {"home_score": home_score, "away_score": away_score}


def aggregate_nba_full(raw: dict) -> dict:
    """Build full-game market probability dict from a sim result.

    Output shape mirrors aggregate_nba_q1: win_prob, expected_points,
    spreads (around the empirical anchor), totals (around the empirical
    anchor), team_totals per side, and total_oe parity.
    """
    h = raw["home_score"]
    a = raw["away_score"]
    n = len(h)
    total = h.astype(int) + a.astype(int)
    margin = h.astype(int) - a.astype(int)

    anchor_spread = -round(float(margin.mean()) * 2) / 2
    spreads: dict = {}
    for offset in (-7.5, -5.5, -3.5, -2.5, -1.5, -0.5,
                   0.5, 1.5, 2.5, 3.5, 5.5, 7.5):
        point = anchor_spread + offset
        home_cov = float((margin > -point).sum()) / n
        spreads[f"home_{point}"] = round(home_cov, 4)

    mean_total = float(total.mean())
    anchor = round(mean_total * 2) / 2
    totals: dict = {}
    for offset in (-10, -7.5, -5, -3.5, -2, -1, -0.5,
                   0, 0.5, 1, 2, 3.5, 5, 7.5, 10):
        line = anchor + offset
        over = float((total > line).sum()) / n
        under = float((total < line).sum()) / n
        totals[f"{line:.1f}"] = {
            "over": round(over, 4),
            "under": round(under, 4),
            "push": round(1.0 - over - under, 4),
        }

    def _team_full_dist(arr) -> dict:
        mean_v = float(arr.mean())
        anchor_v = round(mean_v * 2) / 2
        out = {}
        for off in (-10, -7.5, -5, -2.5, -0.5, 0, 0.5, 2.5, 5, 7.5, 10):
            line = anchor_v + off
            if line < 50:
                continue
            out[f"{line:.1f}"] = {
                "over": round(float((arr > line).sum()) / n, 4),
                "under": round(float((arr < line).sum()) / n, 4),
            }
        return out

    team_totals = {
        "home": {"expected": round(float(h.mean()), 2),
                 "lines": _team_full_dist(h)},
        "away": {"expected": round(float(a.mean()), 2),
                 "lines": _team_full_dist(a)},
    }

    odd_count = int((total % 2 == 1).sum())
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
        "spreads": spreads,
        "totals": totals,
        "team_totals": team_totals,
        "total_oe": total_oe,
    }
