"""Period Totals (Over/Under per period) prop type."""

import math
from typing import Any, Dict, List, Optional

from app.models.game import Game
from app.props.types.base import BaseProp

# Standard lines to evaluate for each period
PERIOD_LINES = [0.5, 1.5, 2.5]

# League average goals per period (~1.02)
LEAGUE_PERIOD_AVG = 1.02


def _poisson_pmf(k: int, lam: float) -> float:
    """Simple Poisson PMF without scipy dependency."""
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def _period_xg(
    home_periods: Dict[str, Any],
    away_periods: Dict[str, Any],
    period_num: int,
) -> tuple:
    """
    Compute expected goals for a specific period.

    home_period_xg = home's avg scoring in this period + opponent's avg conceding
    Normalized against league average to avoid double-counting.
    """
    p = str(period_num)
    home_for = home_periods.get(f"avg_p{p}_for", LEAGUE_PERIOD_AVG)
    away_against = away_periods.get(f"avg_p{p}_against", LEAGUE_PERIOD_AVG)
    away_for = away_periods.get(f"avg_p{p}_for", LEAGUE_PERIOD_AVG)
    home_against = home_periods.get(f"avg_p{p}_against", LEAGUE_PERIOD_AVG)

    # Normalize: (team_attack * opp_defense) / league_avg
    home_p_xg = (home_for * away_against) / LEAGUE_PERIOD_AVG if LEAGUE_PERIOD_AVG > 0 else home_for
    away_p_xg = (away_for * home_against) / LEAGUE_PERIOD_AVG if LEAGUE_PERIOD_AVG > 0 else away_for

    return max(home_p_xg, 0.05), max(away_p_xg, 0.05)


class PeriodTotalProp(BaseProp):
    """
    Period Over/Under — evaluate O/U lines for each period (P1, P2, P3).

    Uses mini-Poisson models per period derived from team period stats.
    Both sides (over and under) are eligible.
    """

    bet_type = "period_total"
    display_name = "Period Total"

    def predict(
        self,
        features: Dict[str, Any],
        matrix: List[List[float]],
        home_xg: float,
        away_xg: float,
    ) -> List[Dict[str, Any]]:
        home_periods = features.get("home_periods", {})
        away_periods = features.get("away_periods", {})

        # Need enough history for period-level predictions
        if (
            home_periods.get("games_found", 0) < 10
            or away_periods.get("games_found", 0) < 10
        ):
            return []

        home_name = features.get("home_team_name", "Home")
        away_name = features.get("away_team_name", "Away")
        candidates = []

        for period_num in (1, 2, 3):
            h_xg, a_xg = _period_xg(home_periods, away_periods, period_num)
            total_xg = h_xg + a_xg

            # Build mini score matrix for this period (max 5 goals per side)
            max_g = 6
            p_matrix = [
                [_poisson_pmf(i, h_xg) * _poisson_pmf(j, a_xg) for j in range(max_g)]
                for i in range(max_g)
            ]

            for line in PERIOD_LINES:
                p_over = sum(
                    p_matrix[i][j]
                    for i in range(max_g)
                    for j in range(max_g)
                    if (i + j) > line
                )
                p_under = sum(
                    p_matrix[i][j]
                    for i in range(max_g)
                    for j in range(max_g)
                    if (i + j) < line
                )

                candidates.append({
                    "side": f"p{period_num}_over_{line}",
                    "confidence": round(p_over, 4),
                    "reasoning": (
                        f"P{period_num} Over {line} at {p_over:.1%} "
                        f"(period xG: {h_xg:.2f}+{a_xg:.2f}={total_xg:.2f})."
                    ),
                })
                candidates.append({
                    "side": f"p{period_num}_under_{line}",
                    "confidence": round(p_under, 4),
                    "reasoning": (
                        f"P{period_num} Under {line} at {p_under:.1%} "
                        f"(period xG: {h_xg:.2f}+{a_xg:.2f}={total_xg:.2f})."
                    ),
                })

        return candidates

    def filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Both sides eligible — pick best edge per period+line combo
        # For now, return all and let the engine sort by confidence
        return candidates

    def map_odds(
        self,
        candidates: List[Dict[str, Any]],
        odds_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        for c in candidates:
            # Expected keys: p1_over_price, p1_under_price, p1_total_line, etc.
            side = c["side"]  # e.g. "p1_over_1.5"
            parts = side.split("_")  # ["p1", "over", "1.5"]
            if len(parts) >= 3:
                period = parts[0]  # "p1"
                direction = parts[1]  # "over" or "under"
                price_key = f"{period}_{direction}_price"
                price = odds_data.get(price_key)
                if price is not None:
                    c["odds"] = price
                    c["implied_probability"] = round(self._american_to_prob(price), 4)
                else:
                    c["odds"] = None
                    c["implied_probability"] = None
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
        # Parse "p1_over_1.5" or "p2_under_0.5"
        try:
            parts = prediction_value.split("_")
            period_num = int(parts[0][1])  # "p1" -> 1
            direction = parts[1]
            line = float(parts[2])
        except (IndexError, ValueError):
            return None

        home_p = getattr(game, f"home_score_p{period_num}", None)
        away_p = getattr(game, f"away_score_p{period_num}", None)
        if home_p is None or away_p is None:
            return None

        total = home_p + away_p
        if total == line:
            return None  # Push
        if direction == "over":
            return total > line
        return total < line

    def determine_outcome(self, game: Game) -> Optional[str]:
        # Return period totals for all three periods
        outcomes = []
        for p in (1, 2, 3):
            hp = getattr(game, f"home_score_p{p}", None)
            ap = getattr(game, f"away_score_p{p}", None)
            if hp is not None and ap is not None:
                outcomes.append(f"p{p}_total_{hp + ap}")
        return "|".join(outcomes) if outcomes else None

    @staticmethod
    def _american_to_prob(odds: float) -> float:
        if odds > 0:
            return 100 / (odds + 100)
        return abs(odds) / (abs(odds) + 100)
