"""
Prediction, BetResult, and TrackedBet ORM models.

Prediction stores model-generated predictions for upcoming games,
including confidence scores, implied probabilities, edge calculations,
and recommendation flags. Each prediction has a ``phase`` that
distinguishes prematch picks (locked once generated) from live
in-game updates.

BetResult tracks the actual outcome of a settled prediction for
profit/loss tracking and model evaluation.

TrackedBet represents a bet that the user has explicitly chosen to
track from the dashboard.  It snapshots the prediction data at the
time of tracking, includes a recommended unit size, and carries its
own win/loss result lifecycle.
"""

from datetime import date, datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.game import Game


# Valid bet types for the prediction model.
BET_TYPES = (
    "ml",
    "total",
    "spread",
    "team_total",
    "period_total",
    "period_winner",
    "first_goal",
    "both_score",
    "overtime",
    "odd_even",
    "period1_btts",
    "period1_spread",
    "regulation_winner",
    "highest_scoring_period",
)

# Valid phases for a prediction row.
PHASES = ("prematch", "live")


class Prediction(TimestampMixin, Base):
    """
    A model-generated prediction for a specific game and bet type.

    Attributes:
        phase: "prematch" (locked once generated) or "live" (updated
            while the game is in progress).
    """

    game_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("game.id"), nullable=False, index=True
    )
    bet_type: Mapped[str] = mapped_column(
        String(30), nullable=False, index=True
    )
    prediction_value: Mapped[str] = mapped_column(
        String(100), nullable=False
    )
    confidence: Mapped[float] = mapped_column(
        Float, nullable=False
    )
    odds_implied_prob: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    edge: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    recommended: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    best_bet: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    reasoning: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )
    phase: Mapped[str] = mapped_column(
        String(20), nullable=False, default="prematch", server_default="prematch"
    )

    # Relationships
    game: Mapped["Game"] = relationship("Game", back_populates="predictions")
    result: Mapped[Optional["BetResult"]] = relationship(
        "BetResult", back_populates="prediction", uselist=False,
        cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return (
            f"<Prediction(id={self.id}, game_id={self.game_id}, "
            f"phase='{self.phase}', bet_type='{self.bet_type}', "
            f"prediction='{self.prediction_value}', "
            f"confidence={self.confidence:.3f}, edge={self.edge})>"
        )


class BetResult(TimestampMixin, Base):
    """Tracks the actual outcome of a settled prediction."""

    prediction_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("prediction.id"), unique=True, nullable=False, index=True
    )
    actual_outcome: Mapped[str] = mapped_column(
        String(100), nullable=False
    )
    was_correct: Mapped[bool] = mapped_column(
        Boolean, nullable=False
    )
    profit_loss: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.0
    )
    settled_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    prediction: Mapped["Prediction"] = relationship(
        "Prediction", back_populates="result"
    )

    def __repr__(self) -> str:
        return (
            f"<BetResult(id={self.id}, prediction_id={self.prediction_id}, "
            f"was_correct={self.was_correct}, profit_loss={self.profit_loss:.2f})>"
        )


class TrackedBet(TimestampMixin, Base):
    """A bet that the user explicitly chose to track from the dashboard.

    Snapshots all relevant prediction data at the time of tracking so
    the record is self-contained even if the underlying Prediction row
    is later regenerated or deleted.
    """

    __tablename__ = "tracked_bet"

    prediction_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("prediction.id", ondelete="SET NULL"),
        nullable=True, index=True,
    )
    game_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("game.id"), nullable=False, index=True
    )

    # Snapshot of the prediction at time of tracking
    bet_type: Mapped[str] = mapped_column(String(30), nullable=False)
    prediction_value: Mapped[str] = mapped_column(String(100), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    odds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    implied_probability: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    edge: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    units: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    phase: Mapped[str] = mapped_column(String(20), nullable=False, default="prematch")
    reasoning: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Denormalized game info for fast display
    home_team_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    away_team_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    home_team_abbr: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    away_team_abbr: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    game_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    # Lock lifecycle — bet is editable until the game starts, then frozen.
    locked_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Result lifecycle
    result: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True
    )  # "win", "loss", "push", None (pending)
    profit_loss: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    settled_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    prediction: Mapped[Optional["Prediction"]] = relationship("Prediction")
    game: Mapped["Game"] = relationship("Game")

    def __repr__(self) -> str:
        return (
            f"<TrackedBet(id={self.id}, game_id={self.game_id}, "
            f"bet_type='{self.bet_type}', units={self.units}, "
            f"result={self.result})>"
        )
