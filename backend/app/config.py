"""
Application configuration settings.

Centralized configuration for database paths, API base URLs,
sport-specific settings, and application-wide constants.
"""

import os
import sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv
from pydantic import BaseModel


# Base directory is the backend/ folder
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env from the backend/ directory
_env_path = BASE_DIR / ".env"
_env_loaded = load_dotenv(_env_path)

# Startup diagnostics — print to stderr so they appear even before logging
# is configured.  This helps debug ".env not found" issues.
if _env_loaded:
    _key_val = os.environ.get("ODDS_API_KEY", "")
    _masked = f"{_key_val[:6]}...{_key_val[-4:]}" if len(_key_val) > 10 else "(empty)"
    print(f"[config] Loaded .env from {_env_path}  ODDS_API_KEY={_masked}", file=sys.stderr)
else:
    print(
        f"[config] WARNING: .env not found at {_env_path} — "
        f"ODDS_API_KEY will be empty. Copy .env.example to .env and add your key.",
        file=sys.stderr,
    )

# Data directory for SQLite database and any local data files
DATA_DIR = BASE_DIR / "data"


def _current_nhl_season() -> str:
    """Compute the current NHL season string (e.g. '20252026').

    The NHL season starts in October — if we're in Jan-Aug, we're in the
    second half of the previous year's season.
    """
    today = date.today()
    start_year = today.year if today.month >= 9 else today.year - 1
    return f"{start_year}{start_year + 1}"


class SportConfig(BaseModel):
    """Configuration for a specific sport."""

    name: str
    api_base_url: str
    default_season: str
    game_types: Dict[str, str]
    positions: List[str]
    periods: int
    overtime: bool
    shootout: bool


class ModelConfig(BaseModel):
    """Tunable constants for the Poisson prediction model.

    Every weight and threshold used in xG calculation and prediction
    generation lives here so it can be adjusted without code changes.
    """

    # League baselines
    league_avg_goals: float = 3.05
    league_avg_save_pct: float = 0.905
    league_avg_top6_ppg: float = 0.65

    # Home ice advantage (added to home xG)
    home_ice_advantage: float = 0.15

    # Form window weights (must sum to 1.0)
    weight_form_5: float = 0.50
    weight_form_10: float = 0.30
    weight_season: float = 0.20

    # Feature factor weights (how much each factor adjusts xG)
    h2h_factor: float = 0.10
    goalie_factor: float = 0.20
    skater_talent_factor: float = 0.10
    lineup_depletion_factor: float = 0.15

    # New factors from enhancements
    player_matchup_factor: float = 0.08
    team_matchup_scoring_factor: float = 0.06
    injury_impact_factor: float = 0.18
    special_teams_factor: float = 0.10
    back_to_back_penalty: float = 0.15
    rest_advantage_per_day: float = 0.05
    rest_advantage_cap: float = 0.15
    road_trip_fatigue_per_game: float = 0.02
    road_trip_fatigue_threshold: int = 2

    # Blending ratios
    splits_blend_weight: float = 0.15
    goalie_recent_weight: float = 0.60
    h2h_goal_adj_weight: float = 0.05
    defensive_regression: float = 0.60
    mean_regression: float = 0.18

    # Defense factor: blend goals-against with shots-against for stability.
    # 0.0 = pure goals-against, 1.0 = pure shots-against.
    defense_shot_blend: float = 0.35
    league_avg_shots_against: float = 30.0

    # Bivariate Poisson correlation parameter (0 = independent, higher = more correlated)
    scoring_correlation: float = 0.12

    # Period-specific scoring weights (how much period tendencies adjust xG)
    period_scoring_factor: float = 0.08

    # Schedule spot / situational awareness
    lookahead_penalty: float = 0.08       # playing a weak team before a rival
    divisional_under_adj: float = 0.06    # divisional games tend to go under
    timezone_penalty: float = 0.06        # west coast team playing east coast afternoon

    # Score state tendencies (live model)
    trailing_desperation_boost: float = 0.25   # trailing by 1 in 3rd, boost scoring rate
    leading_shell_reduction: float = 0.20      # leading by 2+, reduce scoring rate
    pulled_goalie_boost: float = 0.40          # pulled goalie xG boost for trailing team

    # PDO regression: blend of shooting% + save% (league avg = 100)
    # Teams far from 100 are due for regression.
    pdo_regression_factor: float = 0.10

    # Advanced metrics (Corsi-proxy, shot quality)
    corsi_possession_factor: float = 0.08     # how much CF% deviation adjusts xG
    shot_quality_factor: float = 0.06         # shooting% deviation adjustment
    advanced_metrics_min_games: int = 8       # min games before advanced metrics apply

    # 5v5 even-strength possession (from MoneyPuck)
    ev_corsi_factor: float = 0.10             # xG multiplier for 5v5 CF% deviation
    ev_corsi_min_games: int = 8               # min games before 5v5 factor applies
    ev_corsi_significance_threshold: float = 4.0  # CF% deviation to flag as "significant"

    # Close-game possession (CF% in 1-goal games / OT)
    close_game_corsi_factor: float = 0.06     # xG multiplier for close-game CF%
    close_game_min_games: int = 6             # min close games before applying
    close_game_margin: int = 1                # max score margin to qualify as "close"

    # Goalie tier classification
    goalie_tier_elite_sv: float = 0.920       # elite: >= .920 SV%
    goalie_tier_starter_sv: float = 0.905     # starter: >= .905 SV%
    goalie_tier_starter_min_gs: int = 20      # min games started to be "starter"
    goalie_mismatch_factor: float = 0.08      # additional xG adjustment for tier mismatch

    # Starter confirmation confidence
    starter_confidence_high: float = 0.90     # confirmed / obvious pattern
    starter_confidence_medium: float = 0.65   # likely but not confirmed
    starter_confidence_low: float = 0.40      # uncertain (B2B, fatigue)
    starter_fatigue_threshold: int = 3        # consecutive starts before fatigue concern

    # Composite edge score component weights (should sum to ~1.0)
    composite_weight_form: float = 0.14
    composite_weight_goalie: float = 0.14
    composite_weight_possession: float = 0.11
    composite_weight_close_possession: float = 0.07
    composite_weight_special_teams: float = 0.09
    composite_weight_schedule: float = 0.07
    composite_weight_injuries: float = 0.09
    composite_weight_h2h: float = 0.07
    composite_weight_matchup: float = 0.05
    composite_weight_market_edge: float = 0.09
    composite_weight_line_movement: float = 0.08

    # Faceoff contribution to defensive factor
    faceoff_defense_weight: float = 0.10

    # Momentum: weight recent results more heavily within form window
    momentum_decay: float = 0.85        # exponential decay per game (0.85 = most recent game 5.7x most distant)
    momentum_factor: float = 0.30       # how much momentum trend adjusts offensive rating

    # Goalie workload fatigue
    goalie_fatigue_starts_threshold: int = 3   # consecutive starts before fatigue kicks in
    goalie_fatigue_per_start: float = 0.02     # xG penalty per start above threshold

    # Goalie recent save% trend (L5 vs season — hot/cold streaks)
    goalie_trend_factor: float = 0.15          # how much a goalie hot/cold streak adjusts xG

    # Penalty discipline (high PIM team gives up more PP chances to opponent)
    penalty_discipline_factor: float = 0.10    # xG adjustment for discipline differential

    # Close-game record (win rate in 1-goal games — clutch factor)
    close_game_record_factor: float = 0.12     # xG adjustment for close-game performance
    close_game_record_min_games: int = 8       # min 1-goal games before factor applies

    # Scoring-first tendency (teams that score first win ~67% in NHL)
    scoring_first_factor: float = 0.10         # xG adjustment for scoring-first tendency
    scoring_first_min_games: int = 10          # min games with P1 data before factor applies

    # Signal convergence: when multiple strong signals agree, amplify xG gap
    convergence_threshold: int = 3             # number of aligned strong signals to trigger
    convergence_amplifier: float = 0.08        # additional xG adjustment when signals converge

    # xG bounds
    xg_floor: float = 1.6
    xg_ceiling: float = 4.0

    # Poisson parameters
    poisson_max_goals: int = 12
    total_lines: List[float] = [3.5, 4.5, 5.5, 6.5, 7.5, 8.5]
    puck_line: float = 1.5

    # Feature extraction windows
    form_window_short: int = 5
    form_window_medium: int = 10
    form_window_long: int = 20
    ot_window: int = 30
    skater_window: int = 10
    lineup_window: int = 20
    lineup_recent: int = 3
    lineup_appearance_threshold: float = 0.70
    h2h_window: int = 20
    schedule_lookback: int = 7

    # ML model settings
    ml_blend_weight: float = 0.3           # 0.0 = pure Poisson, 1.0 = pure ML
    ml_model_path: str = "data/ml_model.joblib"
    ml_min_training_games: int = 100       # minimum games before ML kicks in


class InjuryConfig(BaseModel):
    """Configuration for injury impact calculations."""

    # How often to refresh injury data (minutes)
    refresh_interval_minutes: int = 120

    # Injury status weights (how much of player's production is lost)
    status_weights: Dict[str, float] = {
        "out": 1.0,
        "ir": 1.0,
        "day-to-day": 0.7,
        "questionable": 0.5,
        "probable": 0.2,
    }

    # Position impact multipliers (some positions matter more)
    position_multipliers: Dict[str, float] = {
        "C": 1.0,
        "LW": 0.9,
        "RW": 0.9,
        "D": 0.85,
        "G": 1.5,  # goalie injuries are most impactful
    }

    # Cap on total injury-based xG reduction
    max_injury_reduction: float = 0.30


class MatchupConfig(BaseModel):
    """Configuration for player and team matchup analysis."""

    # Minimum games required for matchup data to be considered
    min_player_games_vs_team: int = 3
    min_team_h2h_games: int = 3

    # How many seasons back to look for matchup data
    seasons_lookback: int = 3

    # Weighting for recency in matchup data
    current_season_weight: float = 0.70
    prior_season_weight: float = 0.30

    # Player matchup deviation threshold
    # Only apply adjustment if player performs >X% different vs this team
    deviation_threshold: float = 0.15


class Settings(BaseModel):
    """
    Application-wide settings.

    Designed to be sport-agnostic with NHL as the default sport.
    All API keys are loaded from environment variables.
    """

    # Application
    app_name: str = "Sports Model Bettor"
    app_version: str = "1.0.0"
    debug: bool = os.environ.get("DEBUG", "false").lower() in ("true", "1", "yes")

    # Database
    db_dir: Path = DATA_DIR
    db_filename: str = "sports_betting.db"

    @property
    def db_path(self) -> Path:
        return self.db_dir / self.db_filename

    @property
    def database_url(self) -> str:
        return f"sqlite+aiosqlite:///{self.db_path}"

    # API Keys (loaded from environment)
    odds_api_key: Optional[str] = os.environ.get("ODDS_API_KEY", None)

    # API Base URLs
    nhl_api_base: str = "https://api-web.nhle.com/v1"
    odds_api_base: str = "https://api.the-odds-api.com/v4"

    # Default sport
    default_sport: str = "nhl"

    # Sport configurations
    sports: Dict[str, SportConfig] = {
        "nhl": SportConfig(
            name="NHL",
            api_base_url="https://api-web.nhle.com/v1",
            default_season=_current_nhl_season(),
            game_types={
                "preseason": "1",
                "regular": "2",
                "playoffs": "3",
                "allstar": "4",
            },
            positions=["C", "LW", "RW", "D", "G"],
            periods=3,
            overtime=True,
            shootout=True,
        ),
    }

    # Server
    host: str = "0.0.0.0"
    port: int = 8000

    # CORS
    cors_origins: List[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
    ]

    # Prediction thresholds
    min_confidence: float = 0.58
    min_edge: float = 0.05
    best_bet_edge: float = 0.08

    # Best-bet juice limits (American odds).
    # Lines steeper than these are excluded from "best bets" because
    # the juice makes them poor value even if the model is confident.
    # Favorites: no steeper than -170 (risk $170 to win $100)
    # Underdogs: no floor needed (all plus-money is fine)
    best_bet_max_favorite: float = -170.0
    # Overall implied-probability ceiling for best-bet candidates.
    # Synced with best_bet_max_favorite: -170 → 170/270 ≈ 0.6296.
    # Applied at the DB level where we only have implied prob.
    best_bet_max_implied: float = 0.6296

    # Scheduling
    scrape_interval_minutes: int = 30
    odds_refresh_interval_minutes: int = 15

    # Model tuning
    model: ModelConfig = ModelConfig()
    injury: InjuryConfig = InjuryConfig()
    matchup: MatchupConfig = MatchupConfig()

    def get_sport_config(self, sport: Optional[str] = None) -> SportConfig:
        """Get configuration for a specific sport, defaulting to the default sport."""
        sport = sport or self.default_sport
        if sport not in self.sports:
            raise ValueError(
                f"Sport '{sport}' not configured. Available: {list(self.sports.keys())}"
            )
        return self.sports[sport]


# Global settings singleton
settings = Settings()
