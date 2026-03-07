"""
PlayerMatchupStats and TeamMatchupProfile ORM models.

PlayerMatchupStats tracks how individual players perform against specific
opponent teams — surfacing players who historically dominate or struggle
in particular matchups.

TeamMatchupProfile stores computed tendencies between team pairs beyond
basic H2H win/loss records: scoring patterns, pace, special teams, and
period-level trends.
"""

from datetime import date, datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Date, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.player import Player
    from app.models.team import Team


class PlayerMatchupStats(TimestampMixin, Base):
    """
    Aggregated player performance against a specific opponent team.

    One row per (player, opponent_team, season). Enables the model to
    detect players who historically over- or under-perform against
    certain teams and adjust xG accordingly.

    Attributes:
        player_id: FK to the player.
        opponent_team_id: FK to the opponent team.
        season: Season identifier (e.g., '20252026') or 'career'.
        games_played: Number of games against this opponent.
        goals: Total goals scored against this opponent.
        assists: Total assists against this opponent.
        points: Total points against this opponent.
        shots: Total shots against this opponent.
        plus_minus: Cumulative plus/minus against this opponent.
        avg_toi: Average time on ice (minutes) against this opponent.
        ppg: Points per game against this opponent.
        gpg: Goals per game against this opponent.
        overall_ppg: Player's overall PPG (for deviation calculation).
        overall_gpg: Player's overall GPG (for deviation calculation).
    """

    __table_args__ = (
        UniqueConstraint(
            "player_id", "opponent_team_id", "season",
            name="uq_player_matchup_season",
        ),
    )

    player_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("player.id"), nullable=False, index=True
    )
    opponent_team_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=False, index=True
    )
    season: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True
    )

    # Counting stats vs this opponent
    games_played: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    goals: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    assists: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    points: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    shots: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    plus_minus: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    hits: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Per-game rates vs this opponent
    avg_toi: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ppg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    gpg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Overall rates (for deviation comparison)
    overall_ppg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    overall_gpg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Computed deviation: (matchup_ppg - overall_ppg) / overall_ppg
    ppg_deviation: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    gpg_deviation: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Metadata
    last_computed: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    player: Mapped["Player"] = relationship("Player")
    opponent_team: Mapped["Team"] = relationship("Team")

    def __repr__(self) -> str:
        return (
            f"<PlayerMatchupStats(player_id={self.player_id}, "
            f"vs_team={self.opponent_team_id}, season='{self.season}', "
            f"ppg={self.ppg}, deviation={self.ppg_deviation})>"
        )


class TeamMatchupProfile(TimestampMixin, Base):
    """
    Computed tendencies for a specific team-vs-team matchup.

    Goes beyond basic H2H win/loss records to capture scoring patterns,
    pace, special teams effectiveness, and period-level trends when
    these two teams play each other.

    Team1 always has the lower team ID to avoid duplicate rows.

    Attributes:
        team1_id / team2_id: The two teams (team1 < team2 by ID).
        season: Season identifier.
        games_played: Number of games between these teams.
        avg_total_goals: Average combined goals per game.
        scoring_variance: Standard deviation of total goals.
        avg_margin: Average victory margin (team1 perspective).
        ot_rate: Fraction of games that went to OT.
        team1_pp_goals_pg: Team1's PP goals per game in this matchup.
        team2_pp_goals_pg: Team2's PP goals per game in this matchup.
        avg_penalty_minutes: Average combined PIM per game.
        team1_p1_goals_avg: Team1's avg first-period goals in matchup.
        team2_p1_goals_avg: Team2's avg first-period goals in matchup.
        team1_p3_goals_avg: Team1's avg third-period goals in matchup.
        team2_p3_goals_avg: Team2's avg third-period goals in matchup.
        pace_indicator: Combined shots per game (proxy for game pace).
    """

    __table_args__ = (
        UniqueConstraint(
            "team1_id", "team2_id", "season",
            name="uq_team_matchup_season",
        ),
    )

    team1_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=False, index=True
    )
    team2_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=False, index=True
    )
    season: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True
    )

    # Scoring
    games_played: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_total_goals: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    scoring_variance: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    avg_margin: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # OT frequency
    ot_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Special teams in this matchup
    team1_pp_goals_pg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    team2_pp_goals_pg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    avg_penalty_minutes: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Period-level scoring
    team1_p1_goals_avg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    team2_p1_goals_avg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    team1_p3_goals_avg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    team2_p3_goals_avg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Pace
    pace_indicator: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Team1 scoring rates in this matchup
    team1_goals_pg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    team2_goals_pg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Metadata
    last_computed: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    team1: Mapped["Team"] = relationship("Team", foreign_keys=[team1_id])
    team2: Mapped["Team"] = relationship("Team", foreign_keys=[team2_id])

    def __repr__(self) -> str:
        return (
            f"<TeamMatchupProfile(team1={self.team1_id}, team2={self.team2_id}, "
            f"season='{self.season}', avg_total={self.avg_total_goals})>"
        )
