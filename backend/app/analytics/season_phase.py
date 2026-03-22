"""
Season phase awareness for adaptive thresholds.

Early-season data is noisier (small samples, roster turnover, new systems).
This module adjusts model thresholds based on the current phase of the
season, reducing noise bets early and loosening thresholds as more data
becomes available.

Also handles playoff-specific adjustments (tighter games, different pace,
refs swallow whistles).
"""

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class SeasonPhase:
    """Describes the current phase of a season with adaptive thresholds."""

    phase: str          # "early", "mid", "late", "playoffs"
    games_played: int   # approximate games into the season
    description: str

    # Adaptive threshold multipliers (applied to base thresholds)
    min_edge_multiplier: float = 1.0
    min_confidence_multiplier: float = 1.0
    form_weight_season_multiplier: float = 1.0

    # Season-phase specific xG adjustments
    home_ice_adjustment: float = 0.0  # additional home ice modifier
    scoring_variance_factor: float = 1.0  # early season = higher variance


# ---------------------------------------------------------------------------
#  NHL season phases
# ---------------------------------------------------------------------------

def get_nhl_season_phase(
    game_date: Optional[date] = None,
    team_games_played: Optional[int] = None,
) -> SeasonPhase:
    """Determine the current NHL season phase.

    Uses game date and optionally team games played to classify where
    we are in the season. Each phase has different threshold multipliers.

    NHL season structure:
        - Oct 1 – Nov 15: Early season (~15 games)
        - Nov 16 – Feb 1: Mid season (~40-50 games)
        - Feb 2 – Apr 15: Late season (~70-82 games)
        - Apr 16+: Playoffs
    """
    today = game_date or date.today()
    month = today.month
    day = today.day

    # Playoff detection (mid-April through June)
    if (month == 4 and day >= 16) or month in (5, 6):
        return SeasonPhase(
            phase="playoffs",
            games_played=82,
            description="Playoffs — tighter games, lower scoring, refs swallow whistles",
            min_edge_multiplier=1.0,      # Standard thresholds in playoffs
            min_confidence_multiplier=1.0,
            form_weight_season_multiplier=1.2,  # Trust full-season data more
            home_ice_adjustment=0.05,     # Home ice matters more in playoffs
            scoring_variance_factor=0.85,  # Lower scoring in playoffs
        )

    # Early season (October through mid-November)
    if month == 10 or (month == 11 and day <= 15):
        gp = team_games_played or (15 if month == 11 else 5)
        return SeasonPhase(
            phase="early",
            games_played=gp,
            description="Early season — small samples, roster turnover, noisy data",
            min_edge_multiplier=1.5,       # Require 50% more edge (e.g., 4.5% vs 3%)
            min_confidence_multiplier=1.05, # Slightly higher confidence bar
            form_weight_season_multiplier=0.5,  # Don't trust season averages yet
            home_ice_adjustment=0.0,
            scoring_variance_factor=1.15,  # Higher variance early
        )

    # Mid season (mid-November through end of January)
    if (month == 11 and day > 15) or month == 12 or month == 1:
        gp = team_games_played or 40
        return SeasonPhase(
            phase="mid",
            games_played=gp,
            description="Mid season — data stabilizing, trends emerging",
            min_edge_multiplier=1.0,       # Standard thresholds
            min_confidence_multiplier=1.0,
            form_weight_season_multiplier=1.0,
            home_ice_adjustment=0.0,
            scoring_variance_factor=1.0,
        )

    # Late season (February through mid-April)
    gp = team_games_played or 65
    return SeasonPhase(
        phase="late",
        games_played=gp,
        description="Late season — reliable data, playoff push dynamics",
        min_edge_multiplier=0.9,          # Slightly lower threshold (more data = more trust)
        min_confidence_multiplier=1.0,
        form_weight_season_multiplier=1.1,  # Trust season data more
        home_ice_adjustment=0.0,
        scoring_variance_factor=0.95,  # Slightly lower variance (defensive systems tighten)
    )


# ---------------------------------------------------------------------------
#  NBA season phases
# ---------------------------------------------------------------------------

def get_nba_season_phase(
    game_date: Optional[date] = None,
    team_games_played: Optional[int] = None,
) -> SeasonPhase:
    """Determine the current NBA season phase.

    NBA season structure:
        - Oct 22 – Dec 1: Early season (~20 games)
        - Dec 2 – Feb 15: Mid season (~40-50 games)
        - Feb 16 – Apr 13: Late season (~70-82 games)
        - Apr 14+: Playoffs
    """
    today = game_date or date.today()
    month = today.month
    day = today.day

    # Playoffs (mid-April through June)
    if (month == 4 and day >= 14) or month in (5, 6):
        return SeasonPhase(
            phase="playoffs",
            games_played=82,
            description="NBA Playoffs — higher intensity, adjusted rotations",
            min_edge_multiplier=1.0,
            min_confidence_multiplier=1.0,
            form_weight_season_multiplier=1.2,
            home_ice_adjustment=1.0,  # Home court bonus in points
            scoring_variance_factor=0.90,
        )

    # Early season
    if month == 10 or month == 11:
        gp = team_games_played or 12
        return SeasonPhase(
            phase="early",
            games_played=gp,
            description="NBA Early season — load management, rotation experiments",
            min_edge_multiplier=1.4,
            min_confidence_multiplier=1.05,
            form_weight_season_multiplier=0.5,
            home_ice_adjustment=0.0,
            scoring_variance_factor=1.10,
        )

    # Mid season
    if month == 12 or month == 1 or (month == 2 and day <= 15):
        gp = team_games_played or 40
        return SeasonPhase(
            phase="mid",
            games_played=gp,
            description="NBA Mid season — All-Star break approaching",
            min_edge_multiplier=1.0,
            min_confidence_multiplier=1.0,
            form_weight_season_multiplier=1.0,
            home_ice_adjustment=0.0,
            scoring_variance_factor=1.0,
        )

    # Late season
    gp = team_games_played or 65
    return SeasonPhase(
        phase="late",
        games_played=gp,
        description="NBA Late season — playoff push, tank dynamics",
        min_edge_multiplier=0.9,
        min_confidence_multiplier=1.0,
        form_weight_season_multiplier=1.1,
        home_ice_adjustment=0.0,
        scoring_variance_factor=0.95,
    )


def get_season_phase(
    sport: str = "nhl",
    game_date: Optional[date] = None,
    team_games_played: Optional[int] = None,
) -> SeasonPhase:
    """Get season phase for any sport (dispatch)."""
    if sport == "nba":
        return get_nba_season_phase(game_date, team_games_played)
    return get_nhl_season_phase(game_date, team_games_played)


def apply_season_thresholds(
    base_min_edge: float,
    base_min_confidence: float,
    phase: SeasonPhase,
) -> Dict[str, float]:
    """Apply season-phase multipliers to base thresholds.

    Returns adjusted thresholds that should be used for filtering
    predictions during the current season phase.
    """
    return {
        "min_edge": round(base_min_edge * phase.min_edge_multiplier, 4),
        "min_confidence": round(base_min_confidence * phase.min_confidence_multiplier, 4),
        "phase": phase.phase,
        "description": phase.description,
    }
