"""Regulation Winner (3-Way Moneyline) prop type."""

from typing import Any, Dict, List, Optional

from app.models.game import Game
from app.props.types.base import BaseProp


class RegulationWinnerProp(BaseProp):
    """
    Regulation Winner — who leads after 60 minutes (no OT).

    Three outcomes: home win, away win, draw (regulation tie).
    Derived directly from the score matrix diagonal decomposition.
    """

    bet_type = "regulation_winner"
    display_name = "Regulation Winner"

    def predict(
        self,
        features: Dict[str, Any],
        matrix: List[List[float]],
        home_xg: float,
        away_xg: float,
    ) -> List[Dict[str, Any]]:
        n = len(matrix)
        p_home = sum(matrix[i][j] for i in range(n) for j in range(n) if i > j)
        p_away = sum(matrix[i][j] for i in range(n) for j in range(n) if j > i)
        p_draw = sum(matrix[i][i] for i in range(n))

        home_abbr = features.get("home_team_abbr", "HOM")
        away_abbr = features.get("away_team_abbr", "AWY")
        home_name = features.get("home_team_name", "Home")
        away_name = features.get("away_team_name", "Away")

        candidates = [
            {
                "side": f"reg_{home_abbr}",
                "confidence": round(p_home, 4),
                "reasoning": (
                    f"{home_name} wins in regulation at {p_home:.1%} "
                    f"(xG {home_xg:.2f} vs {away_xg:.2f})."
                ),
            },
            {
                "side": f"reg_{away_abbr}",
                "confidence": round(p_away, 4),
                "reasoning": (
                    f"{away_name} wins in regulation at {p_away:.1%} "
                    f"(xG {away_xg:.2f} vs {home_xg:.2f})."
                ),
            },
            {
                "side": "reg_draw",
                "confidence": round(p_draw, 4),
                "reasoning": (
                    f"Regulation draw at {p_draw:.1%}. "
                    f"Game goes to OT with xG {home_xg:.2f}-{away_xg:.2f}."
                ),
            },
        ]
        return candidates

    def filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # All three outcomes are eligible
        return candidates

    def map_odds(
        self,
        candidates: List[Dict[str, Any]],
        odds_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        odds_keys = {
            "reg_home": "reg_home_price",
            "reg_away": "reg_away_price",
            "reg_draw": "reg_draw_price",
        }
        for c in candidates:
            # Normalize side to generic key for odds lookup
            side = c["side"]
            if side.startswith("reg_") and side != "reg_draw":
                generic = "reg_home" if "reg_" in side and side != "reg_draw" else side
                # Can't reliably distinguish home/away from abbr alone;
                # check if it matches the first or second candidate
            key = odds_keys.get(side)
            if not key:
                # side is like "reg_EDM" — map to home or away
                # We don't know which without context, so skip odds
                c["implied_probability"] = None
                c["odds"] = None
                continue
            price = odds_data.get(key)
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
        # Regulation score = sum of P1+P2+P3
        hp = [game.home_score_p1, game.home_score_p2, game.home_score_p3]
        ap = [game.away_score_p1, game.away_score_p2, game.away_score_p3]
        if any(s is None for s in hp) or any(s is None for s in ap):
            return None
        reg_home = sum(hp)
        reg_away = sum(ap)

        if prediction_value == "reg_draw":
            return reg_home == reg_away
        if prediction_value == f"reg_{home_abbr}":
            return reg_home > reg_away
        # Must be away team
        return reg_away > reg_home

    def determine_outcome(self, game: Game) -> Optional[str]:
        hp = [game.home_score_p1, game.home_score_p2, game.home_score_p3]
        ap = [game.away_score_p1, game.away_score_p2, game.away_score_p3]
        if any(s is None for s in hp) or any(s is None for s in ap):
            return None
        reg_home = sum(hp)
        reg_away = sum(ap)
        if reg_home > reg_away:
            return "reg_home"
        elif reg_away > reg_home:
            return "reg_away"
        return "reg_draw"

    @staticmethod
    def _american_to_prob(odds: float) -> float:
        if odds > 0:
            return 100 / (odds + 100)
        return abs(odds) / (abs(odds) + 100)
