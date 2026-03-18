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

        Combines recent form, season averages, pace, offensive/defensive
        efficiency, and adjustments for home court, rest, and injuries.

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

        # Guard against hockey-level defaults leaking in when no NBA
        # stats exist.  If season avg is far below NBA range (< 50 ppg),
        # it's clearly a hockey default — reset to league average.
        if home_ppg < 50:
            home_ppg = self.league_avg
        if away_ppg < 50:
            away_ppg = self.league_avg
        if home_papg < 50:
            home_papg = self.league_avg
        if away_papg < 50:
            away_papg = self.league_avg

        # Recent form (L5, L10)
        home_form_5 = features.get("home_form_5", {})
        home_form_10 = features.get("home_form_10", {})
        away_form_5 = features.get("away_form_5", {})
        away_form_10 = features.get("away_form_10", {})

        # Use season PPG as default for form when games_found is 0
        # (form returns hockey-level 3.0 defaults otherwise)
        def _form_gf(form, default):
            if form.get("games_found", 0) == 0:
                return default
            val = form.get("avg_goals_for", default)
            return val if val and val >= 50 else default

        # Weighted offensive rating
        w5 = _nba.weight_form_5
        w10 = _nba.weight_form_10
        ws = _nba.weight_season

        home_off = (
            w5 * _form_gf(home_form_5, home_ppg)
            + w10 * _form_gf(home_form_10, home_ppg)
            + ws * home_ppg
        )
        away_off = (
            w5 * _form_gf(away_form_5, away_ppg)
            + w10 * _form_gf(away_form_10, away_ppg)
            + ws * away_ppg
        )

        # Defensive quality: opponent's points against relative to league avg
        home_def_factor = away_papg / self.league_avg if self.league_avg > 0 else 1.0
        away_def_factor = home_papg / self.league_avg if self.league_avg > 0 else 1.0

        # Expected points = offense * opponent_defensive_factor
        home_xp = home_off * home_def_factor
        away_xp = away_off * away_def_factor

        # ── Pace adjustment ──
        # If both teams have pace data, adjust total based on combined pace
        # relative to league average. Fast teams push totals up; slow teams
        # pull them down.
        league_pace = _nba.league_avg_pace
        home_pace = home_season.get("pace")
        away_pace = away_season.get("pace")
        if home_pace and away_pace and league_pace > 0:
            matchup_pace = (home_pace + away_pace) / 2
            pace_factor = matchup_pace / league_pace
            home_xp *= pace_factor
            away_xp *= pace_factor

        # ── Efficiency adjustment ──
        # Offensive/defensive rating (points per 100 possessions) provides
        # a pace-independent quality signal.  If available, blend it in.
        home_ortg = home_season.get("offensive_rating")
        away_ortg = away_season.get("offensive_rating")
        home_drtg = home_season.get("defensive_rating")
        away_drtg = away_season.get("defensive_rating")

        if home_ortg and away_drtg and league_pace > 0:
            # Expected home pts from efficiency: (home_ortg vs away_defense)
            # Scale to per-game by league pace estimate
            eff_home_xp = ((home_ortg + (self.league_avg * 100 / league_pace - away_drtg)) / 2) * league_pace / 100
            home_xp = 0.7 * home_xp + 0.3 * eff_home_xp

        if away_ortg and home_drtg and league_pace > 0:
            eff_away_xp = ((away_ortg + (self.league_avg * 100 / league_pace - home_drtg)) / 2) * league_pace / 100
            away_xp = 0.7 * away_xp + 0.3 * eff_away_xp

        # ── Turnover differential ──
        # Teams that commit fewer turnovers and force more create extra
        # possessions. This provides a scoring edge.
        home_tov = home_season.get("turnovers_pg")
        away_tov = away_season.get("turnovers_pg")
        home_stl = home_season.get("steals_pg")
        away_stl = away_season.get("steals_pg")
        if home_tov is not None and away_tov is not None and home_stl is not None and away_stl is not None:
            # Net turnovers forced: steals - turnovers committed
            home_net_tov = away_tov - home_tov + (home_stl - away_stl) * 0.5
            away_net_tov = home_tov - away_tov + (away_stl - home_stl) * 0.5
            # Each net turnover ~ 1 point of expected scoring edge
            home_xp += home_net_tov * 0.5
            away_xp += away_net_tov * 0.5

        # ── Three-point shooting gap ──
        # Teams with a big 3PT% advantage get a slight boost
        home_3pct = home_season.get("three_pt_pct")
        away_3pct = away_season.get("three_pt_pct")
        if home_3pct is not None and away_3pct is not None:
            three_pt_diff = (home_3pct - away_3pct)  # percentage points
            # ~0.3 pts per percentage point difference in 3PT%
            home_xp += three_pt_diff * 0.3
            away_xp -= three_pt_diff * 0.3

        # ── Rebounding advantage ──
        home_reb = home_season.get("rebounds_pg")
        away_reb = away_season.get("rebounds_pg")
        if home_reb is not None and away_reb is not None:
            reb_diff = home_reb - away_reb
            # Each extra rebound ~ 0.15 points of expected scoring
            home_xp += reb_diff * 0.15
            away_xp -= reb_diff * 0.15

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
        if game_spread_line is None:
            # No sportsbook line — use the model's predicted spread as the
            # reference line so we still generate a spread prediction.
            game_spread_line = -round(spread * 2) / 2  # round to nearest 0.5

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
            if away_spread is None:
                away_spread = -game_spread_line
            spread_pick = f"{away_abbr}_{away_spread:+.1f}"
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
        if ou_line is None:
            # No sportsbook line — use model's expected total rounded to
            # nearest 0.5 so we still generate a total prediction.
            ou_line = round(total_xp * 2) / 2

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
