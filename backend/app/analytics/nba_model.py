"""
NBA prediction model using Gaussian (normal) distribution.

Basketball scoring is approximately normally distributed (mean ~112 pts,
stdev ~12 pts), making Gaussian CDF the right tool — unlike hockey's
Poisson distribution for rare scoring events.

This model produces the same output structure as BettingModel.predict_all()
so PredictionManager can use either model transparently.
"""

import logging
import math
from typing import Any, Dict, List, Optional, Tuple

from scipy.stats import norm

from app.config import settings

logger = logging.getLogger(__name__)

_nba = settings.nba_model


class NBABettingModel:
    """
    Statistical prediction model for NBA basketball betting.

    Uses normal distribution with weighted historical inputs to produce
    probabilities for moneyline, totals, and spreads.
    """

    def __init__(self) -> None:
        self.league_avg = _nba.league_avg_points
        self.home_court_adj = _nba.home_court_advantage
        self.std_dev = _nba.scoring_std_dev

    def _calc_expected_points(
        self, features: Dict[str, Any]
    ) -> Tuple[float, float]:
        """Calculate expected points for home and away teams.

        Combines recent form, season averages, and adjustments for
        home court, rest, and injuries.

        Returns:
            (home_expected_points, away_expected_points)
        """
        # Season averages
        home_season = features.get("home_season", {})
        away_season = features.get("away_season", {})
        home_ppg = home_season.get("goals_for_pg", self.league_avg)
        away_ppg = away_season.get("goals_for_pg", self.league_avg)
        home_papg = home_season.get("goals_against_pg", self.league_avg)
        away_papg = away_season.get("goals_against_pg", self.league_avg)

        # Recent form (L5, L10)
        home_form_5 = features.get("home_form_5", {})
        home_form_10 = features.get("home_form_10", {})
        away_form_5 = features.get("away_form_5", {})
        away_form_10 = features.get("away_form_10", {})

        # Weighted offensive rating
        w5 = _nba.weight_form_5
        w10 = _nba.weight_form_10
        ws = _nba.weight_season

        home_off = (
            w5 * home_form_5.get("avg_goals_for", home_ppg)
            + w10 * home_form_10.get("avg_goals_for", home_ppg)
            + ws * home_ppg
        )
        away_off = (
            w5 * away_form_5.get("avg_goals_for", away_ppg)
            + w10 * away_form_10.get("avg_goals_for", away_ppg)
            + ws * away_ppg
        )

        # Defensive quality: opponent's points against relative to league avg
        home_def_factor = away_papg / self.league_avg if self.league_avg > 0 else 1.0
        away_def_factor = home_papg / self.league_avg if self.league_avg > 0 else 1.0

        # Expected points = offense * opponent_defensive_factor
        home_xp = home_off * home_def_factor
        away_xp = away_off * away_def_factor

        # Home court advantage
        home_xp += self.home_court_adj

        # Rest / back-to-back adjustments
        home_schedule = features.get("home_schedule", {})
        away_schedule = features.get("away_schedule", {})

        if home_schedule.get("is_back_to_back", False):
            home_xp -= _nba.back_to_back_penalty
        if away_schedule.get("is_back_to_back", False):
            away_xp -= _nba.back_to_back_penalty

        # Rest advantage (extra days beyond 1)
        home_rest = home_schedule.get("days_rest", 1)
        away_rest = away_schedule.get("days_rest", 1)
        if home_rest > 1:
            bonus = min(
                (home_rest - 1) * _nba.rest_advantage_per_day,
                _nba.rest_advantage_cap,
            )
            home_xp += bonus
        if away_rest > 1:
            bonus = min(
                (away_rest - 1) * _nba.rest_advantage_per_day,
                _nba.rest_advantage_cap,
            )
            away_xp += bonus

        # Injury impact
        home_injuries = features.get("home_injuries", {})
        away_injuries = features.get("away_injuries", {})
        home_inj_reduction = home_injuries.get("xg_reduction", 0)
        away_inj_reduction = away_injuries.get("xg_reduction", 0)

        # Scale injury reduction from hockey's 0-0.3 xG range to NBA's point scale
        # A max 0.3 xG reduction in NHL ~ 10% of scoring -> ~11 points in NBA
        home_xp -= home_inj_reduction * (self.league_avg / 0.3) * _nba.injury_impact_factor
        away_xp -= away_inj_reduction * (self.league_avg / 0.3) * _nba.injury_impact_factor

        # Market prior blending (if available)
        if _nba.market_prior_weight > 0:
            home_xp, away_xp = self._blend_market_prior(
                home_xp, away_xp, features
            )

        # Clamp to reasonable bounds
        home_xp = max(_nba.xp_floor, min(_nba.xp_ceiling, home_xp))
        away_xp = max(_nba.xp_floor, min(_nba.xp_ceiling, away_xp))

        return home_xp, away_xp

    def _blend_market_prior(
        self,
        home_xp: float,
        away_xp: float,
        features: Dict[str, Any],
    ) -> Tuple[float, float]:
        """Blend model expected points with market-implied expected points."""
        from app.analytics.models import american_odds_to_implied_prob

        game = features.get("game_obj")
        if game is None:
            return home_xp, away_xp

        ou_line = getattr(game, "over_under_line", None)
        home_ml = getattr(game, "home_moneyline", None)
        away_ml = getattr(game, "away_moneyline", None)

        if ou_line is None or home_ml is None:
            return home_xp, away_xp

        # Market-implied total points
        market_total = float(ou_line)

        # Market-implied home win probability
        home_imp = american_odds_to_implied_prob(home_ml)
        away_imp = american_odds_to_implied_prob(away_ml) if away_ml else 1 - home_imp

        # Remove vig
        total_imp = home_imp + away_imp
        if total_imp > 0:
            home_fair = home_imp / total_imp
        else:
            home_fair = 0.5

        # Derive market-implied spread from win probability
        # Using inverse normal: spread = std_dev * Phi_inv(home_win_prob)
        market_spread = self.std_dev * norm.ppf(max(0.01, min(0.99, home_fair)))

        # Market-implied expected points
        market_home_xp = (market_total / 2) + (market_spread / 2)
        market_away_xp = (market_total / 2) - (market_spread / 2)

        # Blend
        w = _nba.market_prior_weight
        blended_home = (1 - w) * home_xp + w * market_home_xp
        blended_away = (1 - w) * away_xp + w * market_away_xp

        return blended_home, blended_away

    def predict_game(
        self, features: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate predictions for a single NBA game.

        Returns a dict with expected points and win/spread/total probabilities.
        """
        home_xp, away_xp = self._calc_expected_points(features)

        # Point spread (positive = home favored)
        spread = home_xp - away_xp

        # Combined standard deviation for the score difference
        combined_std = self.std_dev * math.sqrt(2)

        # Win probabilities using normal CDF
        home_win_prob = norm.cdf(spread / combined_std)
        away_win_prob = 1.0 - home_win_prob

        # Apply calibration shrinkage
        shrinkage = _nba.calibration_shrinkage
        home_win_prob = home_win_prob * (1 - shrinkage) + 0.5 * shrinkage
        away_win_prob = 1.0 - home_win_prob

        # Total points
        total_xp = home_xp + away_xp

        # Standard deviation for total (roughly sqrt(2) * individual std)
        total_std = self.std_dev * math.sqrt(2)

        return {
            "home_xp": round(home_xp, 1),
            "away_xp": round(away_xp, 1),
            "total_xp": round(total_xp, 1),
            "spread": round(spread, 1),
            "home_win_prob": round(home_win_prob, 4),
            "away_win_prob": round(away_win_prob, 4),
            "total_std": total_std,
            "combined_std": combined_std,
        }

    async def predict_all(
        self, features: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Generate all predictions for a game (ML, spread, total).

        Returns a list of prediction dicts matching the format used by
        BettingModel.predict_all() for seamless integration.
        """
        result = self.predict_game(features)
        predictions: List[Dict[str, Any]] = []

        home_abbr = features.get("home_team_abbr", "HOME")
        away_abbr = features.get("away_team_abbr", "AWAY")
        home_xp = result["home_xp"]
        away_xp = result["away_xp"]
        total_xp = result["total_xp"]
        spread = result["spread"]
        combined_std = result["combined_std"]
        total_std = result["total_std"]

        # Resolve game odds for implied probability
        game = features.get("game_obj")

        # --- Moneyline ---
        home_win = result["home_win_prob"]
        away_win = result["away_win_prob"]

        if home_win >= away_win:
            ml_pick = home_abbr
            ml_conf = home_win
            ml_odds = getattr(game, "home_moneyline", None) if game else None
        else:
            ml_pick = away_abbr
            ml_conf = away_win
            ml_odds = getattr(game, "away_moneyline", None) if game else None

        from app.analytics.models import american_odds_to_implied_prob
        ml_implied = american_odds_to_implied_prob(ml_odds) if ml_odds else None

        predictions.append({
            "bet_type": "ml",
            "prediction": ml_pick,
            "confidence": round(ml_conf, 4),
            "probability": round(ml_conf, 4),
            "odds": ml_odds,
            "implied_probability": round(ml_implied, 4) if ml_implied else None,
            "reasoning": (
                f"Model: {home_abbr} {home_xp} - {away_abbr} {away_xp} "
                f"(spread {spread:+.1f})"
            ),
        })

        # --- Spread ---
        game_spread_line = getattr(game, "home_spread_line", None) if game else None
        if game_spread_line is not None:
            # Probability of home team covering the spread
            adjusted_spread = spread + game_spread_line  # game_spread_line is negative for favorites
            cover_prob = norm.cdf(adjusted_spread / combined_std)

            # Apply shrinkage
            cover_prob = cover_prob * (1 - _nba.calibration_shrinkage) + 0.5 * _nba.calibration_shrinkage

            if cover_prob >= 0.5:
                spread_pick = f"{home_abbr}_{game_spread_line:+.1f}"
                spread_conf = cover_prob
                spread_odds = getattr(game, "home_spread_price", None) if game else None
            else:
                away_spread = getattr(game, "away_spread_line", None)
                spread_pick = f"{away_abbr}_{away_spread:+.1f}" if away_spread else f"{away_abbr}"
                spread_conf = 1.0 - cover_prob
                spread_odds = getattr(game, "away_spread_price", None) if game else None

            spread_implied = american_odds_to_implied_prob(spread_odds) if spread_odds else None

            predictions.append({
                "bet_type": "spread",
                "prediction": spread_pick,
                "confidence": round(spread_conf, 4),
                "probability": round(spread_conf, 4),
                "odds": spread_odds,
                "implied_probability": round(spread_implied, 4) if spread_implied else None,
                "reasoning": f"Model spread: {spread:+.1f} vs line {game_spread_line:+.1f}",
            })

        # --- Total ---
        ou_line = getattr(game, "over_under_line", None) if game else None
        if ou_line is not None:
            over_prob = 1.0 - norm.cdf((ou_line - total_xp) / total_std)
            under_prob = 1.0 - over_prob

            # Apply shrinkage
            over_prob = over_prob * (1 - _nba.calibration_shrinkage) + 0.5 * _nba.calibration_shrinkage
            under_prob = 1.0 - over_prob

            if over_prob >= under_prob:
                total_pick = f"over_{ou_line}"
                total_conf = over_prob
                total_odds = getattr(game, "over_price", None) if game else None
            else:
                total_pick = f"under_{ou_line}"
                total_conf = under_prob
                total_odds = getattr(game, "under_price", None) if game else None

            total_implied = american_odds_to_implied_prob(total_odds) if total_odds else None

            predictions.append({
                "bet_type": "total",
                "prediction": total_pick,
                "confidence": round(total_conf, 4),
                "probability": round(total_conf, 4),
                "odds": total_odds,
                "implied_probability": round(total_implied, 4) if total_implied else None,
                "reasoning": f"Model total: {total_xp:.1f} vs line {ou_line:.1f}",
            })

        # Sort by confidence descending
        predictions.sort(key=lambda p: p.get("confidence", 0), reverse=True)

        return predictions
