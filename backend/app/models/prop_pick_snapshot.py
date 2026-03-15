"""
PropPickSnapshot ORM model.

Persists player prop picks at generation time so they remain stable
after the game goes final. Without this, the pick engine would
re-analyze using post-game stats and produce different (biased) picks.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.game import Game
    from app.models.player import Player


class PropPickSnapshot(TimestampMixin, Base):
    """
    A frozen player prop pick captured at generation time.

    Once created, these rows are never re-generated — they preserve
    the original analysis so that grading is honest (no look-ahead bias).

    Attributes:
        game_id: FK to the game this pick is for.
        player_id: FK to the player (nullable if matching failed).
        player_name: Player name as displayed.
        market: Prop market key (e.g. 'player_points').
        pick_side: 'over', 'under', or 'yes'.
        line: The prop line (e.g. 0.5, 2.5).
        odds: American odds for the picked side.
        model_prob: Our estimated probability at generation time.
        implied_prob: Sportsbook implied probability.
        edge: model_prob - implied_prob.
        confidence: Confidence score (0-1).
        avg_rate: Player's per-game rate used in analysis.
        games_sampled: Number of games in the analysis window.
        reasoning: Human-readable explanation.
        outcome: True = hit, False = miss, None = not yet graded.
    """

    game_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("game.id"), nullable=False, index=True
    )
    player_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("player.id"), nullable=True, index=True
    )
    player_name: Mapped[str] = mapped_column(String(150), nullable=False)
    market: Mapped[str] = mapped_column(String(80), nullable=False)
    pick_side: Mapped[str] = mapped_column(String(10), nullable=False)
    line: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    odds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    model_prob: Mapped[float] = mapped_column(Float, nullable=False)
    implied_prob: Mapped[float] = mapped_column(Float, nullable=False)
    edge: Mapped[float] = mapped_column(Float, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    avg_rate: Mapped[float] = mapped_column(Float, nullable=False)
    games_sampled: Mapped[int] = mapped_column(Integer, nullable=False)
    reasoning: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    outcome: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    # Relationships
    game: Mapped["Game"] = relationship("Game")
    player: Mapped[Optional["Player"]] = relationship("Player")

    __table_args__ = (
        UniqueConstraint(
            "game_id", "player_name", "market", "pick_side",
            name="uq_prop_pick_snapshot",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<PropPickSnapshot(game_id={self.game_id}, "
            f"player='{self.player_name}', market='{self.market}', "
            f"side='{self.pick_side}', edge={self.edge:.3f})>"
        )
