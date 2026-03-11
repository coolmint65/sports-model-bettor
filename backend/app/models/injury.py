"""
InjuryReport ORM model.

Tracks player injuries with status, type, and expected return dates.
Used by the prediction model to adjust expected goals based on
known absences and their severity.
"""

from datetime import date, datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.player import Player
    from app.models.team import Team


class InjuryReport(TimestampMixin, Base):
    """
    Tracks a player's injury status.

    Each row represents a single injury event for a player.
    Active injuries (active=True) are used in prediction adjustments.

    Attributes:
        player_id: FK to the injured player.
        team_id: FK to the player's team (denormalized for fast queries).
        status: Injury status (out, ir, day-to-day, questionable, probable).
        injury_type: Type of injury (upper body, lower body, illness, etc.).
        body_part: Specific body part if known.
        description: Free-text injury description.
        reported_date: When the injury was first reported.
        expected_return_date: Estimated return date if available.
        source: Where the injury report came from.
        active: Whether this injury is still current.
        player_ppg: Player's points per game at time of injury (for impact calc).
        player_gpg: Player's goals per game at time of injury.
        player_toi: Player's average TOI at time of injury.
    """

    player_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("player.id"), nullable=False, index=True
    )
    team_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=False, index=True
    )

    # Injury details
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="out", index=True
    )
    injury_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    body_part: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Dates
    reported_date: Mapped[date] = mapped_column(Date, nullable=False)
    expected_return_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    # Source
    source: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Lifecycle
    active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, index=True
    )

    # Player impact metrics (snapshotted at report time)
    player_ppg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    player_gpg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    player_toi: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Relationships
    player: Mapped["Player"] = relationship("Player")
    team: Mapped["Team"] = relationship("Team")

    def __repr__(self) -> str:
        return (
            f"<InjuryReport(id={self.id}, player_id={self.player_id}, "
            f"status='{self.status}', active={self.active})>"
        )
