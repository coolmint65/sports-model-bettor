"""
Player prop odds ORM model.

Stores player-level prop betting lines fetched from The Odds API.
Each row represents one prop market for one player for one game,
with the best available line and price across all bookmakers.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.game import Game
    from app.models.player import Player


class PlayerPropOdds(TimestampMixin, Base):
    """
    A single player prop line for a specific game.

    Example row:
        game_id=42, player_name="Connor McDavid", market="player_points",
        line=0.5, over_price=-160, under_price=130
    """

    game_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("game.id"), nullable=False, index=True
    )
    player_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("player.id"), nullable=True, index=True
    )
    # Player name as returned by the sportsbook (used for matching)
    player_name: Mapped[str] = mapped_column(String(150), nullable=False)
    # Odds API market key, e.g. "player_points", "player_shots_on_goal"
    market: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    # The line (e.g. 0.5, 2.5, 27.5)
    line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    # American odds for over/under (or yes/no for anytime markets)
    over_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    under_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    # For anytime goal scorer: only over_price is used (yes price),
    # under_price may be null.

    # Source bookmaker that provided the best line
    bookmaker: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    # When the odds were last refreshed from the API
    odds_updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    game: Mapped["Game"] = relationship("Game", backref="player_props")
    player: Mapped[Optional["Player"]] = relationship("Player")

    __table_args__ = (
        UniqueConstraint(
            "game_id", "player_name", "market",
            name="uq_player_prop_game_player_market",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<PlayerPropOdds(game_id={self.game_id}, "
            f"player='{self.player_name}', market='{self.market}', "
            f"line={self.line}, over={self.over_price}, under={self.under_price})>"
        )
