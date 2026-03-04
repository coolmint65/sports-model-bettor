"""
Application configuration settings.

Centralized configuration for database paths, API base URLs,
sport-specific settings, and application-wide constants.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv
from pydantic import BaseModel


# Base directory is the backend/ folder
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env from the backend/ directory
load_dotenv(BASE_DIR / ".env")

# Data directory for SQLite database and any local data files
DATA_DIR = BASE_DIR / "data"


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


class Settings(BaseModel):
    """
    Application-wide settings.

    Designed to be sport-agnostic with NHL as the default sport.
    All API keys are loaded from environment variables.
    """

    # Application
    app_name: str = "Sports Model Bettor"
    app_version: str = "1.0.0"
    debug: bool = True

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
            default_season="20252026",
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
    cors_origins: List[str] = ["*"]

    # Prediction thresholds
    min_confidence: float = 0.55
    min_edge: float = 0.03
    best_bet_edge: float = 0.08

    # Best-bet juice limits (American odds).
    # Lines steeper than these are excluded from "best bets" because
    # the juice makes them poor value even if the model is confident.
    # Favorites: no steeper than -180 (risk $180 to win $100)
    # Underdogs: no floor needed (all plus-money is fine)
    best_bet_max_favorite: float = -180.0
    # Overall implied-probability ceiling for best-bet candidates.
    # Synced with best_bet_max_favorite: -180 → 180/280 ≈ 0.6429.
    # Applied at the DB level where we only have implied prob.
    best_bet_max_implied: float = 0.6429

    # Scheduling
    scrape_interval_minutes: int = 30
    odds_refresh_interval_minutes: int = 15

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
