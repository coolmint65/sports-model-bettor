"""
InjuryReport ORM model for tracking player injuries.

Stores injury status, type, and expected return information
from external sources (ESPN, NHL.com, etc.).
"""

from datetime import date, datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.player import Player
    from app.models.team import Team


class InjuryReport(TimestampMixin, Base):
    """
    Active injury report for a player.

    Tracks injury status, type/description, expected return date,
    and the source of the information.

    Attributes:
        player_id: FK to the injured player.
        team_id: FK to the player's team (denormalized for efficient queries).
        status: Injury designation (e.g., 'Out', 'Day-to-Day', 'IR', 'Questionable').
        injury_type: Body part or injury description (e.g., 'Upper Body', 'Lower Body', 'Concussion').
        detail: Additional detail text from the source.
        expected_return: Estimated return date if known.
        reported_at: When the injury was first reported (UTC).
        source: Data source (e.g., 'espn', 'nhl', 'tsn').
        is_active: Whether this injury report is still current.
    """

    player_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("player.id"), nullable=False, index=True
    )
    team_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("team.id"), nullable=True, index=True
    )

    status: Mapped[str] = mapped_column(
        String(50), nullable=False, default="Out"
    )
    injury_type: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True
    )
    detail: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True
    )
    expected_return: Mapped[Optional[date]] = mapped_column(
        Date, nullable=True
    )
    reported_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    source: Mapped[str] = mapped_column(
        String(30), nullable=False, default="espn"
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
    )

    # Relationships
    player: Mapped["Player"] = relationship("Player")
    team: Mapped[Optional["Team"]] = relationship("Team")

    def __repr__(self) -> str:
        return (
            f"<InjuryReport(player_id={self.player_id}, "
            f"status='{self.status}', injury='{self.injury_type}')>"
        )
