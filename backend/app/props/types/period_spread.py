"""Period Spread prop type — 1st period -0.5 puck line."""

from typing import Any, Dict, List, Optional

from app.models.game import Game
from app.props.types.base import BaseProp
from app.props.types.period_total import _period_xg, _poisson_pmf


class PeriodSpreadProp(BaseProp):
    """
    1st Period Spread (-0.5) — same outcome as period winner ML but
    typically at significantly better odds (+160 vs -125).

    Only emits the -0.5 side (team must win the period outright).
    +0.5 is skipped because it's almost always heavy juice (-200+).
    """

    bet_type = "period_spread"
    display_name = "Period Spread"
    baseline = 0.32

    def predict(
        self,
        features: Dict[str, Any],
        matrix: List[List[float]],
        home_xg: float,
        away_xg: float,
    ) -> List[Dict[str, Any]]:
        home_periods = features.get("home_periods", {})
        away_periods = features.get("away_periods", {})

        has_period_data = (
            home_periods.get("games_found", 0) >= 5
            and away_periods.get("games_found", 0) >= 5
        )

        home_abbr = features.get("home_team_abbr", "HOM")
        away_abbr = features.get("away_team_abbr", "AWY")
        home_name = features.get("home_team_name", "Home")
        away_name = features.get("away_team_name", "Away")

        # Only P1 — sportsbooks don't widely offer P2/P3 spreads
        if has_period_data:
            h_xg, a_xg = _period_xg(home_periods, away_periods, 1)
        else:
            h_xg = max(home_xg / 3.0, 0.05)
            a_xg = max(away_xg / 3.0, 0.05)

        max_g = 6
        p_matrix = [
            [_poisson_pmf(i, h_xg) * _poisson_pmf(j, a_xg) for j in range(max_g)]
            for i in range(max_g)
        ]

        p_home = sum(
            p_matrix[i][j] for i in range(max_g) for j in range(max_g) if i > j
        )
        p_away = sum(
            p_matrix[i][j] for i in range(max_g) for j in range(max_g) if j > i
        )

        # Emit -0.5 for both sides (team must win the period)
        return [
            {
                "side": f"p1_{home_abbr}_-0.5",
                "confidence": round(p_home, 4),
                "_position": "home",
                "reasoning": (
                    f"P1 {home_name} -0.5 at {p_home:.1%} "
                    f"(period xG: {h_xg:.2f} vs {a_xg:.2f})."
                ),
            },
            {
                "side": f"p1_{away_abbr}_-0.5",
                "confidence": round(p_away, 4),
                "_position": "away",
                "reasoning": (
                    f"P1 {away_name} -0.5 at {p_away:.1%} "
                    f"(period xG: {a_xg:.2f} vs {h_xg:.2f})."
                ),
            },
        ]

    def filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Keep both sides — map_odds will only attach prices where the
        # sportsbook actually offers the -0.5 line.  The engine drops
        # candidates without odds from "recommended" ranking anyway.
        return candidates

    def map_odds(
        self,
        candidates: List[Dict[str, Any]],
        odds_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        for c in candidates:
            position = c.get("_position", "")
            # p1_home_spread_price / p1_away_spread_price
            price_key = f"p1_{position}_spread_price" if position else None
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
        # Parse "p1_EDM_-0.5"
        try:
            parts = prediction_value.split("_")
            # parts: ["p1", "EDM", "-0.5"]
            team_abbr = parts[1]
        except (IndexError, ValueError):
            return None

        home_p = getattr(game, "home_score_p1", None)
        away_p = getattr(game, "away_score_p1", None)
        if home_p is None or away_p is None:
            return None

        # -0.5 spread = team must win outright
        if team_abbr == home_abbr:
            return home_p > away_p
        else:
            return away_p > home_p

    def determine_outcome(self, game: Game) -> Optional[str]:
        hp = getattr(game, "home_score_p1", None)
        ap = getattr(game, "away_score_p1", None)
        if hp is None or ap is None:
            return None
        if hp > ap:
            return "p1_home_-0.5"
        elif ap > hp:
            return "p1_away_-0.5"
        return "p1_push"

    @staticmethod
    def _american_to_prob(odds: float) -> float:
        if odds > 0:
            return 100 / (odds + 100)
        return abs(odds) / (abs(odds) + 100)
