"""First Goal (Team To Score First) prop type."""

from typing import Any, Dict, List, Optional

from app.models.game import Game
from app.props.types.base import BaseProp
from app.props.types.period_total import LEAGUE_PERIOD_AVG, _period_xg


class FirstGoalProp(BaseProp):
    """
    Team To Score First — both sides eligible.

    Approximated from P1 expected goals: the team with higher P1 xG
    is more likely to score first. Uses P1 period stats.
    """

    bet_type = "first_goal"
    display_name = "First Goal"

    def predict(
        self,
        features: Dict[str, Any],
        matrix: List[List[float]],
        home_xg: float,
        away_xg: float,
    ) -> List[Dict[str, Any]]:
        home_periods = features.get("home_periods", {})
        away_periods = features.get("away_periods", {})

        if (
            home_periods.get("games_found", 0) < 5
            or away_periods.get("games_found", 0) < 5
        ):
            return []

        # Use P1 expected goals as proxy for first-goal probability
        h_p1_xg, a_p1_xg = _period_xg(home_periods, away_periods, 1)
        total_p1 = h_p1_xg + a_p1_xg

        if total_p1 <= 0:
            return []

        p_home_first = h_p1_xg / total_p1
        p_away_first = a_p1_xg / total_p1

        home_abbr = features.get("home_team_abbr", "HOM")
        away_abbr = features.get("away_team_abbr", "AWY")
        home_name = features.get("home_team_name", "Home")
        away_name = features.get("away_team_name", "Away")

        candidates = [
            {
                "side": f"first_goal_{home_abbr}",
                "confidence": round(p_home_first, 4),
                "_position": "home",
                "reasoning": (
                    f"{home_name} scores first at {p_home_first:.1%} "
                    f"(P1 xG: {h_p1_xg:.2f} vs {a_p1_xg:.2f})."
                ),
            },
            {
                "side": f"first_goal_{away_abbr}",
                "confidence": round(p_away_first, 4),
                "_position": "away",
                "reasoning": (
                    f"{away_name} scores first at {p_away_first:.1%} "
                    f"(P1 xG: {a_p1_xg:.2f} vs {h_p1_xg:.2f})."
                ),
            },
        ]
        return candidates

    def filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Both sides eligible
        return candidates

    def map_odds(
        self,
        candidates: List[Dict[str, Any]],
        odds_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        for c in candidates:
            position = c.get("_position", "")
            price_key = f"first_goal_{position}_price" if position else None
            price = odds_data.get(price_key) if price_key else None
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
        # Game model has first_goal_team_id
        if game.first_goal_team_id is None:
            return None

        # Parse "first_goal_EDM"
        parts = prediction_value.split("first_goal_")
        if len(parts) < 2:
            return None
        predicted_abbr = parts[1]

        if predicted_abbr == home_abbr:
            return game.first_goal_team_id == game.home_team_id
        else:
            return game.first_goal_team_id == game.away_team_id

    def determine_outcome(self, game: Game) -> Optional[str]:
        if game.first_goal_team_id is None:
            return None
        if game.first_goal_team_id == game.home_team_id:
            return "first_goal_home"
        return "first_goal_away"

    @staticmethod
    def _american_to_prob(odds: float) -> float:
        if odds > 0:
            return 100 / (odds + 100)
        return abs(odds) / (abs(odds) + 100)
