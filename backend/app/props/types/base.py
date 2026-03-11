"""Abstract base class for all prop type definitions."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from app.models.game import Game


class BaseProp(ABC):
    """
    Self-contained prop type definition.

    Each prop bundles its prediction logic, directional filter,
    odds mapping, and grading rule into one class.
    Adding a new prop = adding one file with one class.
    """

    bet_type: str  # e.g. "both_score", "period_total"
    display_name: str  # e.g. "BTTS", "P1 Over/Under"

    # League-average probability for this prop's best candidate after
    # dedup.  Used to compute signal strength when sportsbook odds are
    # unavailable.  Raw confidence is not comparable across prop types
    # (e.g. 48% reg-winner vs 25% OT-yes) because each type lives on a
    # different probability scale.  Signal strength normalises them:
    #
    #     signal = (confidence - baseline) / baseline
    #
    # A positive signal means the model sees something above the league
    # norm; the higher the signal, the more interesting the pick.
    # Subclasses MUST set this to a sensible NHL average.
    baseline: float  # e.g. 0.23 for overtime_yes

    @abstractmethod
    def predict(
        self,
        features: Dict[str, Any],
        matrix: List[List[float]],
        home_xg: float,
        away_xg: float,
    ) -> List[Dict[str, Any]]:
        """
        Generate prediction candidates from model features.

        Returns list of dicts with keys:
            side: str — the predicted outcome (e.g. "both_score_no", "p1_over_1.5")
            confidence: float — model probability (0-1)
            reasoning: str — human-readable explanation
        """

    @abstractmethod
    def filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply directional filtering rules.

        For example, BTTS keeps only "no", Overtime keeps only "yes".
        Props that bet both sides return candidates unchanged.
        """

    @abstractmethod
    def map_odds(
        self,
        candidates: List[Dict[str, Any]],
        odds_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Attach sportsbook odds data to candidates.

        Adds implied_probability, odds, and edge fields.
        Returns candidates unchanged if no odds data is available.
        """

    @abstractmethod
    def grade(
        self,
        prediction_value: str,
        game: Game,
        home_abbr: str = "",
    ) -> Optional[bool]:
        """
        Grade a settled prediction.

        Returns True (win), False (loss), or None (push/ungradeable).
        """

    @abstractmethod
    def determine_outcome(self, game: Game) -> Optional[str]:
        """
        Return the actual outcome string for a settled game.

        Used by BetResult records to store the canonical outcome.
        """
