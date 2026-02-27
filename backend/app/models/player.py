"""
Player, PlayerStats, and GoalieStats ORM models.

Player represents an individual athlete on a team roster.
PlayerStats holds season-level skater statistics.
GoalieStats holds season-level goaltender statistics.
"""

from datetime import date, datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.game import GameGoalieStats, GamePlayerStats
    from app.models.team import Team


class Player(TimestampMixin, Base):
    """
    An individual player on a team roster.

    Attributes:
        external_id: ID from the external API (e.g., NHL API player ID).
        name: Full name of the player.
        team_id: FK to the team the player is currently on.
        position: Playing position (C, LW, RW, D, G).
        jersey_number: Sweater number.
        shoots_catches: Handedness - L or R.
        height: Height in inches.
        weight: Weight in pounds.
        birth_date: Date of birth.
        sport: Sport identifier, defaults to 'nhl'.
        active: Whether the player is currently active.
    """

    external_id: Mapped[str] = mapped_column(
        String(50), unique=True, nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(150), nullable=False)
    team_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=True, index=True
    )
    position: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    jersey_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    shoots_catches: Mapped[Optional[str]] = mapped_column(String(5), nullable=True)
    height: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    weight: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    birth_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    sport: Mapped[str] = mapped_column(String(20), nullable=False, default="nhl")
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # Relationships
    team: Mapped[Optional["Team"]] = relationship("Team", back_populates="players")
    season_stats: Mapped[List["PlayerStats"]] = relationship(
        "PlayerStats", back_populates="player", cascade="all, delete-orphan"
    )
    goalie_stats: Mapped[List["GoalieStats"]] = relationship(
        "GoalieStats", back_populates="player", cascade="all, delete-orphan"
    )
    game_stats: Mapped[List["GamePlayerStats"]] = relationship(
        "GamePlayerStats", back_populates="player", cascade="all, delete-orphan"
    )
    game_goalie_stats: Mapped[List["GameGoalieStats"]] = relationship(
        "GameGoalieStats", back_populates="player", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return (
            f"<Player(id={self.id}, name='{self.name}', "
            f"position='{self.position}', team_id={self.team_id})>"
        )


class PlayerStats(TimestampMixin, Base):
    """
    Season-level skater statistics for a player.

    One row per player per season. Covers goals, assists, points,
    and advanced metrics for skaters (non-goalies).
    """

    player_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("player.id"), nullable=False, index=True
    )
    season: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True
    )

    # Counting stats
    games_played: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    goals: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    assists: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    points: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    plus_minus: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pim: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Special teams
    ppg: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ppa: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    shg: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sha: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Other
    gwg: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    shots: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    shooting_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    toi_per_game: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    faceoff_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Metadata
    date_updated: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    player: Mapped["Player"] = relationship("Player", back_populates="season_stats")

    def __repr__(self) -> str:
        return (
            f"<PlayerStats(player_id={self.player_id}, season='{self.season}', "
            f"goals={self.goals}, assists={self.assists}, points={self.points})>"
        )


class GoalieStats(TimestampMixin, Base):
    """
    Season-level goaltender statistics.

    One row per goalie per season. Covers wins, losses, save percentage,
    goals-against average, and related goaltending metrics.
    """

    player_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("player.id"), nullable=False, index=True
    )
    season: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True
    )

    # Games
    games_played: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    games_started: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Record
    wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ot_losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Performance
    save_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    gaa: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    shutouts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Volume
    saves: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    shots_against: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    toi: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Quality
    quality_starts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Metadata
    date_updated: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    player: Mapped["Player"] = relationship("Player", back_populates="goalie_stats")

    def __repr__(self) -> str:
        return (
            f"<GoalieStats(player_id={self.player_id}, season='{self.season}', "
            f"record={self.wins}-{self.losses}-{self.ot_losses}, "
            f"save_pct={self.save_pct})>"
        )
