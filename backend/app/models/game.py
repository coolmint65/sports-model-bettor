"""
Game, GamePlayerStats, GameGoalieStats, and HeadToHead ORM models.

Game represents a single scheduled or completed contest between two teams.
GamePlayerStats and GameGoalieStats hold per-game individual performance.
HeadToHead tracks season-level matchup history between two teams.
"""

from datetime import date, datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.types import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.player import Player
    from app.models.prediction import Prediction
    from app.models.team import Team


class Game(TimestampMixin, Base):
    """
    A single game between two teams.

    Tracks the full box-score detail: final scores, per-period scores,
    shots, overtime status, and game metadata.

    Attributes:
        external_id: Game ID from the external API.
        sport: Sport identifier (e.g., 'nhl').
        season: Season identifier (e.g., '20252026').
        game_type: Type of game (preseason, regular, playoffs).
        date: Calendar date of the game.
        start_time: Scheduled puck-drop time (UTC).
        home_team_id: FK to the home Team.
        away_team_id: FK to the away Team.
        venue: Arena name.
        status: One of 'scheduled', 'in_progress', 'final'.
        home_score / away_score: Final scores.
        home_score_p1..p3, ot: Per-period scores.
        home_shots / away_shots: Total shots on goal.
        went_to_overtime: Whether the game required OT.
        first_goal_team_id: Team that scored first.
        winning_team_id: Team that won the game.
    """

    external_id: Mapped[str] = mapped_column(
        String(50), unique=True, nullable=False, index=True
    )
    sport: Mapped[str] = mapped_column(String(20), nullable=False, default="nhl")
    season: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    game_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    start_time: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Teams
    home_team_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=False, index=True
    )
    away_team_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=False, index=True
    )

    # Venue and status
    venue: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="scheduled", index=True
    )

    # Final score
    home_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    away_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Period 1 scores
    home_score_p1: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    away_score_p1: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Period 2 scores
    home_score_p2: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    away_score_p2: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Period 3 scores
    home_score_p3: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    away_score_p3: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Overtime scores
    home_score_ot: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    away_score_ot: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Shots
    home_shots: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    away_shots: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Game outcome flags
    went_to_overtime: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True, default=False
    )
    first_goal_team_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=True
    )
    winning_team_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=True
    )

    # Live game clock info (transient, updated during live games)
    period: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    period_type: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    clock: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    clock_running: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    in_intermission: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    # Betting odds (American format, from The Odds API)
    home_moneyline: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    away_moneyline: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    over_under_line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    home_spread_line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Odds prices (American format juice)
    away_spread_line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    home_spread_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    away_spread_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    over_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    under_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    odds_updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # All available total lines from sportsbooks (JSON list)
    # e.g. [{"line": 4.5, "over_price": -180, "under_price": 150}, ...]
    all_total_lines: Mapped[Optional[str]] = mapped_column(
        JSON, nullable=True, default=None
    )
    # All available spread lines from sportsbooks (JSON list)
    # e.g. [{"line": 1.5, "home_spread": -1.5, "away_spread": 1.5, "home_price": ..., "away_price": ...}, ...]
    all_spread_lines: Mapped[Optional[str]] = mapped_column(
        JSON, nullable=True, default=None
    )

    # Both Teams to Score (BTTS) odds
    btts_yes_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    btts_no_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Regulation winner (3-way moneyline) odds
    reg_home_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    reg_away_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    reg_draw_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # 1st period odds (from sportsbooks)
    period1_home_ml: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    period1_away_ml: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    period1_draw_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    period1_spread_line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    period1_home_spread_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    period1_away_spread_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    period1_total_line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    period1_over_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    period1_under_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Pregame odds snapshot — frozen when the game goes live so live
    # odds can overwrite the main fields without losing the opening lines.
    pregame_home_moneyline: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pregame_away_moneyline: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pregame_over_under_line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pregame_home_spread_line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pregame_away_spread_line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pregame_home_spread_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pregame_away_spread_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pregame_over_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pregame_under_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Closing odds snapshot — captured when game status becomes final.
    # These are the last odds before puck drop, used for CLV analysis.
    closing_home_moneyline: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    closing_away_moneyline: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    closing_over_under_line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    closing_over_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    closing_under_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    closing_home_spread_line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    closing_home_spread_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    closing_away_spread_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Relationships
    home_team: Mapped["Team"] = relationship(
        "Team",
        back_populates="home_games",
        foreign_keys=[home_team_id],
    )
    away_team: Mapped["Team"] = relationship(
        "Team",
        back_populates="away_games",
        foreign_keys=[away_team_id],
    )
    first_goal_team: Mapped[Optional["Team"]] = relationship(
        "Team",
        foreign_keys=[first_goal_team_id],
    )
    winning_team: Mapped[Optional["Team"]] = relationship(
        "Team",
        foreign_keys=[winning_team_id],
    )
    player_stats: Mapped[List["GamePlayerStats"]] = relationship(
        "GamePlayerStats", back_populates="game", cascade="all, delete-orphan"
    )
    goalie_stats: Mapped[List["GameGoalieStats"]] = relationship(
        "GameGoalieStats", back_populates="game", cascade="all, delete-orphan"
    )
    predictions: Mapped[List["Prediction"]] = relationship(
        "Prediction", back_populates="game", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return (
            f"<Game(id={self.id}, external_id='{self.external_id}', "
            f"date={self.date}, home_team_id={self.home_team_id}, "
            f"away_team_id={self.away_team_id}, status='{self.status}')>"
        )


class GamePlayerStats(TimestampMixin, Base):
    """
    Per-game skater statistics.

    One row per player per game for skaters. Tracks goals, assists,
    shots, hits, blocked shots, and special-teams contributions.
    """

    __table_args__ = (
        UniqueConstraint("game_id", "player_id", name="uq_game_player"),
    )

    game_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("game.id"), nullable=False, index=True
    )
    player_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("player.id"), nullable=False, index=True
    )

    # Offense
    goals: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    assists: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    points: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    plus_minus: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Shooting and physical
    shots: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    hits: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    blocked_shots: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pim: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Ice time (minutes as float)
    toi: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Special teams
    pp_goals: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sh_goals: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Relationships
    game: Mapped["Game"] = relationship("Game", back_populates="player_stats")
    player: Mapped["Player"] = relationship("Player", back_populates="game_stats")

    def __repr__(self) -> str:
        return (
            f"<GamePlayerStats(game_id={self.game_id}, player_id={self.player_id}, "
            f"goals={self.goals}, assists={self.assists})>"
        )


class GameGoalieStats(TimestampMixin, Base):
    """
    Per-game goaltender statistics.

    One row per goalie per game. Tracks saves, shots against,
    save percentage, and the game decision.
    """

    __table_args__ = (
        UniqueConstraint("game_id", "player_id", name="uq_game_goalie"),
    )

    game_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("game.id"), nullable=False, index=True
    )
    player_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("player.id"), nullable=False, index=True
    )

    # Performance
    saves: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    shots_against: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    goals_against: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    save_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Ice time (minutes as float)
    toi: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Decision: W, L, or OTL
    decision: Mapped[Optional[str]] = mapped_column(String(5), nullable=True)

    # Relationships
    game: Mapped["Game"] = relationship("Game", back_populates="goalie_stats")
    player: Mapped["Player"] = relationship("Player", back_populates="game_goalie_stats")

    def __repr__(self) -> str:
        return (
            f"<GameGoalieStats(game_id={self.game_id}, player_id={self.player_id}, "
            f"saves={self.saves}, shots_against={self.shots_against}, "
            f"decision='{self.decision}')>"
        )


class HeadToHead(TimestampMixin, Base):
    """
    Season-level head-to-head record between two teams.

    Tracks the number of games played, wins for each side,
    total goals, overtime games, and last meeting details.
    Team1 always has the lower team ID to avoid duplicate rows.
    """

    __table_args__ = (
        UniqueConstraint("team1_id", "team2_id", "season", name="uq_h2h_season"),
    )

    team1_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=False, index=True
    )
    team2_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=False, index=True
    )
    season: Mapped[str] = mapped_column(String(20), nullable=False, index=True)

    # Record
    games_played: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    team1_wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    team2_wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ot_games: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Goals
    team1_goals: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    team2_goals: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Last meeting
    last_meeting_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    last_meeting_winner_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=True
    )

    # Relationships
    team1: Mapped["Team"] = relationship("Team", foreign_keys=[team1_id])
    team2: Mapped["Team"] = relationship("Team", foreign_keys=[team2_id])
    last_meeting_winner: Mapped[Optional["Team"]] = relationship(
        "Team", foreign_keys=[last_meeting_winner_id]
    )

    def __repr__(self) -> str:
        return (
            f"<HeadToHead(team1_id={self.team1_id}, team2_id={self.team2_id}, "
            f"season='{self.season}', record={self.team1_wins}-{self.team2_wins})>"
        )
