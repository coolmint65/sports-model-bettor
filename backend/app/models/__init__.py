"""
SQLAlchemy ORM models for the sports betting database.

Re-exports all model classes for convenient imports:
    from app.models import Team, Player, Game, ...
"""

from app.models.base import Base
from app.models.team import Team, TeamEVStats, TeamStats
from app.models.player import Player, PlayerStats, GoalieStats
from app.models.game import Game, GamePlayerStats, GameGoalieStats, HeadToHead
from app.models.prediction import Prediction, BetResult, TrackedBet
from app.models.injury import InjuryReport
from app.models.matchup import PlayerMatchupStats, TeamMatchupProfile

__all__ = [
    "Base",
    "Team",
    "TeamEVStats",
    "TeamStats",
    "Player",
    "PlayerStats",
    "GoalieStats",
    "Game",
    "GamePlayerStats",
    "GameGoalieStats",
    "HeadToHead",
    "Prediction",
    "BetResult",
    "TrackedBet",
    "InjuryReport",
    "PlayerMatchupStats",
    "TeamMatchupProfile",
]
