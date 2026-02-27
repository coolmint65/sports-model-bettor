"""
Prediction and BetResult ORM models.

Prediction stores model-generated predictions for upcoming games,
including confidence scores, implied probabilities, edge calculations,
and recommendation flags.

BetResult tracks the actual outcome of a settled prediction for
profit/loss tracking and model evaluation.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.game import Game


# Valid bet types for the prediction model.
# These are stored as plain strings (not a DB-level enum) for flexibility:
#   ml          - moneyline (who wins)
#   total       - over/under total goals
#   spread      - puck line / spread
#   team_total  - over/under for one team's goals
#   period_total - over/under for a specific period
#   period_winner - who wins a specific period
#   first_goal  - which team scores first
#   both_score  - both teams score at least one goal
#   overtime    - will the game go to overtime
#   odd_even    - total goals odd or even
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
)


class Prediction(TimestampMixin, Base):
    """
    A model-generated prediction for a specific game and bet type.

    Attributes:
        game_id: FK to the Game this prediction is for.
        bet_type: The market type (one of BET_TYPES).
        prediction_value: The predicted outcome as a string
            (e.g., 'home', 'away', 'over 5.5', 'under 5.5').
        confidence: Model confidence as a probability between 0 and 1.
        odds_implied_prob: The implied probability derived from the
            best available market odds (0-1).
        edge: The difference between model confidence and implied
            probability (confidence - odds_implied_prob). Positive
            values indicate value.
        recommended: Whether this bet meets the minimum edge threshold
            for recommendation.
        best_bet: Whether this bet meets the higher "best bet" edge
            threshold, indicating particularly strong value.
        reasoning: Free-text explanation of the model's rationale for
            this prediction.
        created_at: Inherited from TimestampMixin; when this prediction
            was generated.
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

    # Relationships
    game: Mapped["Game"] = relationship("Game", back_populates="predictions")
    result: Mapped[Optional["BetResult"]] = relationship(
        "BetResult", back_populates="prediction", uselist=False,
        cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return (
            f"<Prediction(id={self.id}, game_id={self.game_id}, "
            f"bet_type='{self.bet_type}', prediction='{self.prediction_value}', "
            f"confidence={self.confidence:.3f}, edge={self.edge})>"
        )


class BetResult(TimestampMixin, Base):
    """
    Tracks the actual outcome of a settled prediction.

    Created after a game is final and the prediction can be graded.

    Attributes:
        prediction_id: FK to the Prediction that was graded.
        actual_outcome: The real outcome as a string (matching the
            format of Prediction.prediction_value).
        was_correct: Whether the prediction matched the actual outcome.
        profit_loss: The profit or loss in units. Positive values
            represent profit. A standard flat-bet model uses +1.0
            for a win and -1.0 for a loss, but this field supports
            variable sizing.
        settled_at: When the result was settled / graded.
    """

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
