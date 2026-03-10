"""
OddsSnapshot ORM model for tracking line movement over time.

Each row represents a point-in-time snapshot of odds for a game,
captured during each odds sync cycle. This enables line movement
analysis and historical odds tracking.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class OddsSnapshot(Base):
    """
    Point-in-time snapshot of odds for a game.

    Captured each time odds are synced and a change is detected.
    Enables line movement charts and opening-vs-closing line analysis.

    Attributes:
        game_id: FK to the Game this snapshot belongs to.
        captured_at: UTC timestamp when this snapshot was taken.
        source: Which source provided these odds (e.g., 'odds_api', 'draftkings').
        home_moneyline: Home team moneyline (American format).
        away_moneyline: Away team moneyline (American format).
        over_under_line: Total goals line (e.g., 5.5).
        over_price: Over price (American format).
        under_price: Under price (American format).
        home_spread_line: Home puck line (e.g., -1.5).
        away_spread_line: Away puck line (e.g., +1.5).
        home_spread_price: Home spread price (American format).
        away_spread_price: Away spread price (American format).
    """

    game_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("game.id"), nullable=False, index=True
    )
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    source: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Moneyline
    home_moneyline: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    away_moneyline: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Totals
    over_under_line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    over_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    under_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Spread / Puck line
    home_spread_line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    away_spread_line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    home_spread_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    away_spread_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<OddsSnapshot(game_id={self.game_id}, "
            f"captured_at={self.captured_at}, "
            f"home_ml={self.home_moneyline}, away_ml={self.away_moneyline})>"
        )
