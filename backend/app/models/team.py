"""
Team and TeamStats ORM models.

Team represents a sports franchise (e.g., Boston Bruins).
TeamStats holds aggregated season-level statistics for a team.
"""

from datetime import date, datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.game import Game, HeadToHead
    from app.models.player import Player


class Team(TimestampMixin, Base):
    """
    A sports team / franchise.

    Attributes:
        external_id: ID from the external API (e.g., NHL API team ID).
        name: Full team name (e.g., "Boston Bruins").
        abbreviation: Three-letter code (e.g., "BOS").
        city: Home city or market name.
        division: Division within the league.
        conference: Conference within the league.
        sport: Sport identifier, defaults to 'nhl'.
        logo_url: URL to the team's logo image.
        active: Whether the team is currently active.
    """

    external_id: Mapped[str] = mapped_column(
        String(50), unique=True, nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    abbreviation: Mapped[str] = mapped_column(String(10), nullable=False)
    city: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    division: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    conference: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    sport: Mapped[str] = mapped_column(String(20), nullable=False, default="nhl")
    logo_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    __table_args__ = (
        UniqueConstraint("abbreviation", "sport", name="uq_team_abbrev_sport"),
    )

    # Relationships
    stats: Mapped[List["TeamStats"]] = relationship(
        "TeamStats", back_populates="team", cascade="all, delete-orphan"
    )
    players: Mapped[List["Player"]] = relationship(
        "Player", back_populates="team", cascade="all, delete-orphan"
    )
    home_games: Mapped[List["Game"]] = relationship(
        "Game",
        back_populates="home_team",
        foreign_keys="Game.home_team_id",
    )
    away_games: Mapped[List["Game"]] = relationship(
        "Game",
        back_populates="away_team",
        foreign_keys="Game.away_team_id",
    )

    def __repr__(self) -> str:
        return f"<Team(id={self.id}, name='{self.name}', abbreviation='{self.abbreviation}')>"


class TeamEVStats(TimestampMixin, Base):
    """
    Even-strength (5v5) advanced stats sourced from MoneyPuck.

    One row per team per season, refreshed daily. Provides true 5v5
    Corsi, Fenwick, and expected goals data that cannot be derived
    from standard boxscore stats alone.
    """

    team_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=False, index=True
    )
    season: Mapped[str] = mapped_column(String(20), nullable=False, index=True)

    # 5v5 possession metrics (percentages, 0-100)
    ev_cf_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ev_ff_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ev_xgf_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ev_shots_for_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Raw counts for context
    games_played: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # When this row was last fetched from MoneyPuck
    scrape_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    # Relationship
    team: Mapped["Team"] = relationship("Team")

    def __repr__(self) -> str:
        return (
            f"<TeamEVStats(team_id={self.team_id}, season='{self.season}', "
            f"ev_cf_pct={self.ev_cf_pct})>"
        )


class TeamStats(TimestampMixin, Base):
    """
    Aggregated season-level statistics for a team.

    One row per team per season. Updated periodically as games are played.
    """

    team_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=False, index=True
    )
    season: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True
    )

    __table_args__ = (
        UniqueConstraint("team_id", "season", name="uq_teamstats_team_season"),
    )

    # Record
    games_played: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ot_losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    points: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Scoring
    goals_for: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    goals_against: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    goals_for_per_game: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    goals_against_per_game: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )

    # Special teams
    power_play_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    penalty_kill_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Shots
    shots_for_per_game: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    shots_against_per_game: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )

    # Other
    faceoff_win_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Recent form (stored as strings, e.g., "3-1-1")
    record_last_5: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    record_last_10: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    record_last_20: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Division rank (from standings)
    division_rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Splits (stored as strings, e.g., "15-8-2")
    home_record: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    away_record: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Metadata
    date_updated: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    team: Mapped["Team"] = relationship("Team", back_populates="stats")

    def __repr__(self) -> str:
        return (
            f"<TeamStats(team_id={self.team_id}, season='{self.season}', "
            f"record={self.wins}-{self.losses}-{self.ot_losses})>"
        )
