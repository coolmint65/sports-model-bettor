"""Overtime prop type."""

from typing import Any, Dict, List, Optional

from app.models.game import Game
from app.props.types.base import BaseProp


class OvertimeProp(BaseProp):
    """
    Game Goes To Overtime — only bet "Yes".

    "No" is heavy juice. We only bet "Yes" when the model sees
    a high OT probability from the score matrix diagonal (regulation tie)
    blended with team OT tendency.
    """

    bet_type = "overtime"
    display_name = "Overtime"
    # ~23% of NHL regular-season games go to OT/SO
    baseline = 0.23

    # Weight for matrix-based vs historical OT tendency
    MATRIX_WEIGHT = 0.7
    HISTORY_WEIGHT = 0.3

    def predict(
        self,
        features: Dict[str, Any],
        matrix: List[List[float]],
        home_xg: float,
        away_xg: float,
    ) -> List[Dict[str, Any]]:
        n = len(matrix)
        # P(regulation tie) from score matrix = diagonal sum
        p_tie_matrix = sum(matrix[i][i] for i in range(n))

        # Blend with historical OT tendency
        home_ot = features.get("home_ot", {})
        away_ot = features.get("away_ot", {})
        home_ot_pct = home_ot.get("ot_pct", 0.0)
        away_ot_pct = away_ot.get("ot_pct", 0.0)

        has_history = (
            home_ot.get("games_found", 0) >= 5
            and away_ot.get("games_found", 0) >= 5
        )

        if has_history:
            avg_ot_pct = (home_ot_pct + away_ot_pct) / 2
            p_ot = (
                self.MATRIX_WEIGHT * p_tie_matrix
                + self.HISTORY_WEIGHT * avg_ot_pct
            )
        else:
            p_ot = p_tie_matrix

        home_name = features.get("home_team_name", "Home")
        away_name = features.get("away_team_name", "Away")

        reasoning_parts = [f"OT Yes at {p_ot:.1%} (matrix tie: {p_tie_matrix:.1%}"]
        if has_history:
            reasoning_parts.append(
                f", team OT rates: {home_ot_pct:.1%}/{away_ot_pct:.1%}"
            )
        reasoning_parts.append(
            f"). xG {home_xg:.2f}-{away_xg:.2f} for "
            f"{home_name} vs {away_name}."
        )

        candidates = [
            {
                "side": "overtime_yes",
                "confidence": round(p_ot, 4),
                "reasoning": "".join(reasoning_parts),
            },
        ]
        return candidates

    def filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Only bet "Yes" — No is heavy juice
        return [c for c in candidates if c["side"] == "overtime_yes"]

    def map_odds(
        self,
        candidates: List[Dict[str, Any]],
        odds_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        for c in candidates:
            price = odds_data.get("ot_yes_price")
            if price is not None:
                c["odds"] = price
                c["implied_probability"] = round(self._american_to_prob(price), 4)
            else:
                c["odds"] = None
                c["implied_probability"] = None
        return candidates

    def grade(
        self,
        prediction_value: str,
        game: Game,
        home_abbr: str = "",
    ) -> Optional[bool]:
        if game.went_to_overtime is None:
            return None
        if prediction_value == "overtime_yes":
            return game.went_to_overtime
        return not game.went_to_overtime

    def determine_outcome(self, game: Game) -> Optional[str]:
        if game.went_to_overtime is None:
            return None
        return "overtime_yes" if game.went_to_overtime else "overtime_no"

    @staticmethod
    def _american_to_prob(odds: float) -> float:
        if odds > 0:
            return 100 / (odds + 100)
        return abs(odds) / (abs(odds) + 100)
