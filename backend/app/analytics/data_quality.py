"""
Data quality and staleness detection.

Flags when feature data is older than expected, injury reports are stale,
or stats haven't been updated recently. Stale data reduces prediction
reliability and should either reduce conviction or skip the bet.

Also provides opponent-adjusted possession metrics and EV logging.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
#  Stale data detection
# ---------------------------------------------------------------------------

def check_data_freshness(
    features: Dict[str, Any],
    game_start_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Check if game features are based on fresh data.

    Flags specific data sources that are stale and computes an overall
    freshness score that can be used to reduce conviction.

    Args:
        features: Full game features dict from FeatureEngine.
        game_start_time: When the game starts (for relative staleness).

    Returns:
        Dict with freshness score, stale flags, and recommendations.
    """
    stale_flags = []
    freshness_penalties = 0.0
    now = datetime.now(timezone.utc)
    game_time = game_start_time or now

    # Check injury data staleness
    for side in ("home", "away"):
        injury_data = features.get(f"{side}_injuries", {})
        last_updated = injury_data.get("last_updated")
        if last_updated:
            if isinstance(last_updated, str):
                try:
                    last_updated = datetime.fromisoformat(last_updated)
                except (ValueError, TypeError):
                    last_updated = None

        if last_updated:
            hours_stale = (now - last_updated).total_seconds() / 3600
            if hours_stale > 24:
                stale_flags.append({
                    "source": f"{side}_injuries",
                    "hours_stale": round(hours_stale, 1),
                    "severity": "critical" if hours_stale > 48 else "warning",
                })
                freshness_penalties += 0.10 if hours_stale > 48 else 0.05
        else:
            # No timestamp = assume potentially stale
            stale_flags.append({
                "source": f"{side}_injuries",
                "hours_stale": None,
                "severity": "unknown",
            })
            freshness_penalties += 0.03

    # Check odds staleness
    odds = features.get("odds", {})
    odds_updated = odds.get("updated_at")
    if odds_updated:
        if isinstance(odds_updated, str):
            try:
                odds_updated = datetime.fromisoformat(odds_updated)
            except (ValueError, TypeError):
                odds_updated = None

    if odds_updated:
        hours_stale = (now - odds_updated).total_seconds() / 3600
        if hours_stale > 4:
            stale_flags.append({
                "source": "odds",
                "hours_stale": round(hours_stale, 1),
                "severity": "critical" if hours_stale > 12 else "warning",
            })
            freshness_penalties += 0.08 if hours_stale > 12 else 0.04

    # Check form data (is it from recent games?)
    for side in ("home", "away"):
        form5 = features.get(f"{side}_form_5", {})
        games_found = form5.get("games_found", 0)
        if games_found < 3:
            stale_flags.append({
                "source": f"{side}_form_5",
                "detail": f"Only {games_found} games found (need 3+)",
                "severity": "warning",
            })
            freshness_penalties += 0.05

    # Check goalie data freshness (NHL specific)
    for side in ("home", "away"):
        goalie = features.get(f"{side}_goalie", {})
        if goalie and goalie.get("goalie_name"):
            games_started = goalie.get("games_started", 0)
            if games_started < 5:
                stale_flags.append({
                    "source": f"{side}_goalie",
                    "detail": f"Only {games_started} starts (limited sample)",
                    "severity": "info",
                })
                freshness_penalties += 0.02

    # Overall freshness score (1.0 = perfectly fresh, 0.0 = completely stale)
    freshness_score = max(0.0, 1.0 - freshness_penalties)

    recommendation = "proceed"
    if freshness_score < 0.70:
        recommendation = "skip"
    elif freshness_score < 0.85:
        recommendation = "reduce_conviction"

    return {
        "freshness_score": round(freshness_score, 3),
        "stale_flags": stale_flags,
        "freshness_penalties": round(freshness_penalties, 3),
        "recommendation": recommendation,
        "stale_count": len(stale_flags),
    }


# ---------------------------------------------------------------------------
#  Opponent-adjusted possession
# ---------------------------------------------------------------------------

def compute_opponent_adjusted_cf(
    team_cf_pct: float,
    opponent_cf_pct: float,
    league_avg_cf: float = 50.0,
) -> float:
    """Compute opponent-adjusted Corsi For percentage.

    Raw CF% is inflated/deflated by opponent quality. A team with 55% CF%
    mostly against bad possession teams is less impressive than 52% CF%
    against elite possession teams.

    Adjustment formula:
        Adjusted CF% = team_cf + (league_avg - opponent_cf)

    If your opponent typically has 53% CF (above average), we add a bonus
    because your raw CF% was suppressed by facing a strong possession team.

    Args:
        team_cf_pct: Team's raw Corsi For % (0-100).
        opponent_cf_pct: Opponent's season average CF% (0-100).
        league_avg_cf: League average CF% (should be ~50%).

    Returns:
        Opponent-adjusted CF%.
    """
    if team_cf_pct <= 0 or opponent_cf_pct <= 0:
        return team_cf_pct

    # Adjustment: if opponent has CF% above league avg, boost team's CF%
    # because they were facing a tougher opponent.
    adjustment = league_avg_cf - opponent_cf_pct
    adjusted = team_cf_pct + adjustment

    # Clamp to reasonable range
    return round(max(35.0, min(65.0, adjusted)), 2)


def compute_adjusted_possession_factor(
    home_cf_pct: float,
    away_cf_pct: float,
    home_opp_cf_pct: float,
    away_opp_cf_pct: float,
    league_avg_cf: float = 50.0,
) -> Dict[str, float]:
    """Compute opponent-adjusted possession differential for a matchup.

    Returns both raw and adjusted possession metrics so the model can
    use the more reliable adjusted version.
    """
    adj_home = compute_opponent_adjusted_cf(home_cf_pct, away_cf_pct, league_avg_cf)
    adj_away = compute_opponent_adjusted_cf(away_cf_pct, home_cf_pct, league_avg_cf)

    raw_diff = home_cf_pct - away_cf_pct
    adj_diff = adj_home - adj_away

    return {
        "home_raw_cf": home_cf_pct,
        "away_raw_cf": away_cf_pct,
        "home_adjusted_cf": adj_home,
        "away_adjusted_cf": adj_away,
        "raw_differential": round(raw_diff, 2),
        "adjusted_differential": round(adj_diff, 2),
        "adjustment_impact": round(adj_diff - raw_diff, 2),
    }


# ---------------------------------------------------------------------------
#  EV logging
# ---------------------------------------------------------------------------

def compute_bet_ev(
    confidence: float,
    odds: float,
    edge: Optional[float] = None,
    units: float = 1.0,
) -> Dict[str, float]:
    """Compute detailed Expected Value metrics for a bet.

    EV = (prob_win * profit_if_win) - (prob_lose * stake)

    Also computes:
    - EV per unit
    - Break-even probability
    - Edge over break-even
    """
    if odds == 0 or confidence <= 0:
        return {
            "ev": 0.0,
            "ev_per_unit": 0.0,
            "break_even_prob": 0.5,
            "edge_over_breakeven": 0.0,
        }

    # Potential profit for a winning bet
    if odds > 0:
        profit_if_win = units * (odds / 100.0)
    else:
        profit_if_win = units * (100.0 / abs(odds))

    ev = (confidence * profit_if_win) - ((1.0 - confidence) * units)
    ev_per_unit = ev / units if units > 0 else 0.0

    # Break-even probability (implied from odds + juice)
    if odds > 0:
        break_even = 100.0 / (odds + 100.0)
    else:
        break_even = abs(odds) / (abs(odds) + 100.0)

    return {
        "ev": round(ev, 4),
        "ev_per_unit": round(ev_per_unit, 4),
        "break_even_prob": round(break_even, 4),
        "edge_over_breakeven": round(confidence - break_even, 4),
        "confidence": confidence,
        "odds": odds,
        "units": units,
    }


# ---------------------------------------------------------------------------
#  Score-state Markov transition probabilities
# ---------------------------------------------------------------------------

def markov_win_probability(
    home_xg: float,
    away_xg: float,
    home_score: int = 0,
    away_score: int = 0,
    periods_remaining: int = 3,
    period_fraction_remaining: float = 1.0,
) -> Dict[str, float]:
    """Estimate win probability using Markov chain score-state transitions.

    Models the game as a series of Poisson scoring events within each
    remaining period. Transitions through score states to compute
    final outcome probabilities.

    This is more accurate than static pre-game probabilities for:
    - Live betting (accounts for current score + time remaining)
    - Period props (period-by-period scoring rates)

    Args:
        home_xg: Home team expected goals (full game).
        away_xg: Away team expected goals (full game).
        home_score: Current home score.
        away_score: Current away score.
        periods_remaining: Number of full periods remaining.
        period_fraction_remaining: Fraction of current period remaining (0-1).

    Returns:
        Dict with home_win_prob, away_win_prob, tie_prob (regulation),
        and expected score projections.
    """
    import math

    if home_xg <= 0 or away_xg <= 0:
        return {
            "home_win_prob": 0.5,
            "away_win_prob": 0.5,
            "tie_prob": 0.0,
            "projected_home_score": home_score,
            "projected_away_score": away_score,
        }

    # Remaining xG (proportional to time remaining)
    # NHL: 3 periods, each ~33% of total xG
    total_periods = 3.0  # NHL standard
    fraction_remaining = (
        (periods_remaining - 1 + period_fraction_remaining) / total_periods
    )
    fraction_remaining = max(0, min(1, fraction_remaining))

    remaining_home_xg = home_xg * fraction_remaining
    remaining_away_xg = away_xg * fraction_remaining

    # Use Poisson probability matrix for remaining goals
    max_additional = 8
    home_win = 0.0
    away_win = 0.0
    tie = 0.0

    for h_add in range(max_additional):
        for a_add in range(max_additional):
            p_h = _poisson_pmf(remaining_home_xg, h_add)
            p_a = _poisson_pmf(remaining_away_xg, a_add)
            joint_prob = p_h * p_a

            final_home = home_score + h_add
            final_away = away_score + a_add

            if final_home > final_away:
                home_win += joint_prob
            elif final_away > final_home:
                away_win += joint_prob
            else:
                tie += joint_prob

    # Normalize (should be close to 1.0 already)
    total = home_win + away_win + tie
    if total > 0:
        home_win /= total
        away_win /= total
        tie /= total

    return {
        "home_win_prob": round(home_win, 4),
        "away_win_prob": round(away_win, 4),
        "tie_prob": round(tie, 4),
        "projected_home_score": round(home_score + remaining_home_xg, 2),
        "projected_away_score": round(away_score + remaining_away_xg, 2),
        "remaining_home_xg": round(remaining_home_xg, 3),
        "remaining_away_xg": round(remaining_away_xg, 3),
        "fraction_remaining": round(fraction_remaining, 3),
    }


def _poisson_pmf(lam: float, k: int) -> float:
    """Poisson probability mass function."""
    import math
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)
