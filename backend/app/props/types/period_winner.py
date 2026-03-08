"""Period Winner prop type."""

import math
from typing import Any, Dict, List, Optional

from app.models.game import Game
from app.props.types.base import BaseProp
from app.props.types.period_total import LEAGUE_PERIOD_AVG, _period_xg, _poisson_pmf


class PeriodWinnerProp(BaseProp):
    """
    Period Winner — who wins each period (P1, P2, P3).

    Three outcomes per period: home, away, draw.
    All three outcomes are eligible. Uses same mini-Poisson
    as period totals.
    """

    bet_type = "period_winner"
    display_name = "Period Winner"
    # Average favourite wins a given period ~32%
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
        candidates = []

        for period_num in (1, 2, 3):
            if has_period_data:
                h_xg, a_xg = _period_xg(home_periods, away_periods, period_num)
            else:
                h_xg = max(home_xg / 3.0, 0.05)
                a_xg = max(away_xg / 3.0, 0.05)

            # Build mini score matrix
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
            p_draw = sum(p_matrix[i][i] for i in range(max_g))

            candidates.extend([
                {
                    "side": f"p{period_num}_{home_abbr}",
                    "confidence": round(p_home, 4),
                    "_position": "home",
                    "reasoning": (
                        f"P{period_num} {home_name} win at {p_home:.1%} "
                        f"(period xG: {h_xg:.2f} vs {a_xg:.2f})."
                    ),
                },
                {
                    "side": f"p{period_num}_{away_abbr}",
                    "confidence": round(p_away, 4),
                    "_position": "away",
                    "reasoning": (
                        f"P{period_num} {away_name} win at {p_away:.1%} "
                        f"(period xG: {a_xg:.2f} vs {h_xg:.2f})."
                    ),
                },
                {
                    "side": f"p{period_num}_draw",
                    "confidence": round(p_draw, 4),
                    "_position": "draw",
                    "reasoning": (
                        f"P{period_num} draw at {p_draw:.1%} "
                        f"(period xG: {h_xg:.2f}-{a_xg:.2f})."
                    ),
                },
            ])

        return candidates

    def filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Keep only the highest-confidence outcome per period.
        best: Dict[str, Dict[str, Any]] = {}  # key: "p1", "p2", "p3"
        for c in candidates:
            period = c["side"].split("_")[0]  # "p1"
            prev = best.get(period)
            if prev is None or c["confidence"] > prev["confidence"]:
                best[period] = c
        return list(best.values())

    def map_odds(
        self,
        candidates: List[Dict[str, Any]],
        odds_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        for c in candidates:
            side = c["side"]  # e.g. "p1_EDM", "p2_draw"
            parts = side.split("_", 1)
            if len(parts) < 2:
                c["odds"] = None
                c["implied_probability"] = None
                continue

            period = parts[0]  # "p1"
            position = c.get("_position", "")

            price_key = f"{period}_{position}_price" if position else None
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
        # Parse "p1_EDM" or "p2_draw"
        try:
            parts = prediction_value.split("_", 1)
            period_num = int(parts[0][1])
            outcome = parts[1]
        except (IndexError, ValueError):
            return None

        home_p = getattr(game, f"home_score_p{period_num}", None)
        away_p = getattr(game, f"away_score_p{period_num}", None)
        if home_p is None or away_p is None:
            return None

        if outcome == "draw":
            return home_p == away_p
        elif outcome == home_abbr:
            return home_p > away_p
        else:
            # Away team
            return away_p > home_p

    def determine_outcome(self, game: Game) -> Optional[str]:
        outcomes = []
        for p in (1, 2, 3):
            hp = getattr(game, f"home_score_p{p}", None)
            ap = getattr(game, f"away_score_p{p}", None)
            if hp is not None and ap is not None:
                if hp > ap:
                    outcomes.append(f"p{p}_home")
                elif ap > hp:
                    outcomes.append(f"p{p}_away")
                else:
                    outcomes.append(f"p{p}_draw")
        return "|".join(outcomes) if outcomes else None

    @staticmethod
    def _american_to_prob(odds: float) -> float:
        if odds > 0:
            return 100 / (odds + 100)
        return abs(odds) / (abs(odds) + 100)
