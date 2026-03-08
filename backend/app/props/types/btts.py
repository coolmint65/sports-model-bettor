"""Both Teams To Score (BTTS) prop type."""

from typing import Any, Dict, List, Optional

from app.models.game import Game
from app.props.types.base import BaseProp


class BTTSProp(BaseProp):
    """
    Both Teams To Score — only bet "No".

    In hockey, BTTS-Yes is always -800+ juice. There's no value
    betting that both teams will score. BTTS-No has value when
    the model sees a high shutout probability.
    """

    bet_type = "both_score"
    display_name = "BTTS"

    def predict(
        self,
        features: Dict[str, Any],
        matrix: List[List[float]],
        home_xg: float,
        away_xg: float,
    ) -> List[Dict[str, Any]]:
        n = len(matrix)
        # P(BTTS) = P(home > 0 AND away > 0)
        p_btts = sum(
            matrix[i][j] for i in range(1, n) for j in range(1, n)
        )
        p_no = 1.0 - p_btts

        # P(at least one shutout) breakdown for reasoning
        p_home_shutout = sum(matrix[0][j] for j in range(n))  # home scores 0
        p_away_shutout = sum(matrix[i][0] for i in range(n))  # away scores 0

        home_name = features.get("home_team_name", "Home")
        away_name = features.get("away_team_name", "Away")

        candidates = [
            {
                "side": "both_score_no",
                "confidence": round(p_no, 4),
                "reasoning": (
                    f"BTTS No at {p_no:.1%}. "
                    f"{home_name} shutout prob {p_home_shutout:.1%}, "
                    f"{away_name} shutout prob {p_away_shutout:.1%} "
                    f"(xG {home_xg:.2f}-{away_xg:.2f})."
                ),
            },
        ]
        return candidates

    def filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Only bet "No" — Yes is always heavy juice in hockey
        return [c for c in candidates if c["side"] == "both_score_no"]

    def map_odds(
        self,
        candidates: List[Dict[str, Any]],
        odds_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        for c in candidates:
            price = odds_data.get("btts_no_price")
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
        if game.home_score is None or game.away_score is None:
            return None
        both_scored = game.home_score > 0 and game.away_score > 0
        if prediction_value == "both_score_no":
            return not both_scored
        return both_scored

    def determine_outcome(self, game: Game) -> Optional[str]:
        if game.home_score is None or game.away_score is None:
            return None
        if game.home_score > 0 and game.away_score > 0:
            return "both_score_yes"
        return "both_score_no"

    @staticmethod
    def _american_to_prob(odds: float) -> float:
        if odds > 0:
            return 100 / (odds + 100)
        return abs(odds) / (abs(odds) + 100)
