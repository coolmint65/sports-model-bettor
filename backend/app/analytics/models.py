"""
Statistical prediction models for sports betting.

Uses a Poisson-based approach to model goal scoring in hockey. The model
combines weighted historical averages (recent form, season stats, home/away
splits, head-to-head history, and goalie quality) to estimate expected goals
for each team, then derives probabilities for various bet types.

No ML training is required -- this is a purely statistical model suitable
for v1 deployment. The Poisson distribution naturally models rare,
independent scoring events (goals in hockey).
"""

import logging
import math
from typing import Any, Dict, List, Tuple

from scipy.stats import poisson

from app.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model constants — all values now sourced from settings.model (ModelConfig).
# Module-level aliases kept for backward compatibility and brevity.
# ---------------------------------------------------------------------------

_mc = settings.model

LEAGUE_AVG_GOALS = _mc.league_avg_goals
HOME_ICE_ADVANTAGE = _mc.home_ice_advantage
WEIGHT_FORM_5 = _mc.weight_form_5
WEIGHT_FORM_10 = _mc.weight_form_10
WEIGHT_SEASON = _mc.weight_season
H2H_FACTOR = _mc.h2h_factor
GOALIE_FACTOR = _mc.goalie_factor
SKATER_TALENT_FACTOR = _mc.skater_talent_factor
LINEUP_DEPLETION_FACTOR = _mc.lineup_depletion_factor
LEAGUE_AVG_TOP6_PPG = _mc.league_avg_top6_ppg
LEAGUE_AVG_SAVE_PCT = _mc.league_avg_save_pct
TOTAL_LINES = _mc.total_lines
PUCK_LINE = _mc.puck_line
POISSON_MAX_GOALS = _mc.poisson_max_goals

# New enhancement factors
PLAYER_MATCHUP_FACTOR = _mc.player_matchup_factor
TEAM_MATCHUP_SCORING_FACTOR = _mc.team_matchup_scoring_factor
INJURY_IMPACT_FACTOR = _mc.injury_impact_factor
SPECIAL_TEAMS_FACTOR = _mc.special_teams_factor
BACK_TO_BACK_PENALTY = _mc.back_to_back_penalty
REST_ADVANTAGE_PER_DAY = _mc.rest_advantage_per_day
REST_ADVANTAGE_CAP = _mc.rest_advantage_cap


def american_odds_to_implied_prob(odds: float) -> float:
    """
    Convert American odds to implied probability.

    - Negative odds (favorite): implied = |odds| / (|odds| + 100)
    - Positive odds (underdog): implied = 100 / (odds + 100)

    Returns a probability between 0 and 1.
    """
    if odds < 0:
        return abs(odds) / (abs(odds) + 100.0)
    elif odds > 0:
        return 100.0 / (odds + 100.0)
    return 0.5  # Even money


def implied_prob_to_american_odds(prob: float) -> float:
    """
    Convert implied probability to American odds.

    Returns American odds (negative for favorites, positive for underdogs).
    """
    if prob <= 0 or prob >= 1:
        return 0.0
    if prob > 0.5:
        return -(prob / (1 - prob)) * 100.0
    else:
        return ((1 - prob) / prob) * 100.0


class BettingModel:
    """
    Statistical prediction model for NHL hockey betting.

    Uses Poisson distribution with weighted historical inputs to produce
    probabilities for moneyline, totals, and spreads. Optionally blends
    with an ML model for improved xG estimation when trained.
    """

    def __init__(self, ml_model=None) -> None:
        """Initialize the betting model with default parameters.

        Args:
            ml_model: Optional MLModel instance. If provided and trained,
                      predictions will blend Poisson xG with ML xG.
        """
        self.league_avg = LEAGUE_AVG_GOALS
        self.home_ice_adj = HOME_ICE_ADVANTAGE
        self.ml_model = ml_model

    # ------------------------------------------------------------------ #
    #  Core: Expected goals calculation                                   #
    # ------------------------------------------------------------------ #

    def _calc_expected_goals(
        self,
        features: Dict[str, Any],
    ) -> Tuple[float, float]:
        """
        Calculate expected goals for home and away teams using a weighted
        Poisson model.

        The expected goals for each team are calculated as:

        1. Weighted offensive average:
           - 60% last-5-game avg goals for
           - 25% last-10-game avg goals for
           - 15% season avg goals for

        2. Adjusted by opponent's defensive quality:
           - Multiply by (opponent goals_against_pg / league_avg)

        3. Home ice advantage:
           - Add HOME_ICE_ADVANTAGE to the home team's expected goals

        4. Head-to-head adjustment:
           - Slight nudge based on historical H2H performance

        5. Goalie quality adjustment:
           - If the opposing goalie is better/worse than average, adjust
             the expected goals accordingly

        Returns:
            Tuple of (home_xg, away_xg).
        """
        # ---- Home team offensive rating ----
        home_off = self._weighted_goals_for(
            features["home_form_5"]["avg_goals_for"],
            features["home_form_10"]["avg_goals_for"],
            features["home_season"]["goals_for_pg"],
        )

        # ---- Away team offensive rating ----
        away_off = self._weighted_goals_for(
            features["away_form_5"]["avg_goals_for"],
            features["away_form_10"]["avg_goals_for"],
            features["away_season"]["goals_for_pg"],
        )

        # ---- Momentum adjustment ----
        # momentum_avg_gf weights recent games exponentially heavier.
        # Compare to raw avg_goals_for to detect trending up/down.
        home_momentum = features["home_form_5"].get("momentum_avg_gf")
        away_momentum = features["away_form_5"].get("momentum_avg_gf")
        home_raw_gf = features["home_form_5"]["avg_goals_for"]
        away_raw_gf = features["away_form_5"]["avg_goals_for"]
        if home_momentum and home_raw_gf > 0:
            momentum_ratio = home_momentum / home_raw_gf - 1.0
            home_off *= 1.0 + momentum_ratio * _mc.momentum_factor
        if away_momentum and away_raw_gf > 0:
            momentum_ratio = away_momentum / away_raw_gf - 1.0
            away_off *= 1.0 + momentum_ratio * _mc.momentum_factor

        # ---- Defensive adjustments (opponent quality) ----
        # Home team faces away goalie/defense; away team faces home goalie/defense
        # Blend goals-against with shots-against for more stable defense ratings.
        home_def_factor = self._defensive_factor(
            features["home_season"]["goals_against_pg"],
            features["home_season"].get("shots_against_pg", 0.0),
            features["home_season"].get("faceoff_pct", 50.0),
        )
        away_def_factor = self._defensive_factor(
            features["away_season"]["goals_against_pg"],
            features["away_season"].get("shots_against_pg", 0.0),
            features["away_season"].get("faceoff_pct", 50.0),
        )

        # Home team expected goals = home offense * away defensive weakness
        home_xg = home_off * away_def_factor
        # Away team expected goals = away offense * home defensive weakness
        away_xg = away_off * home_def_factor

        # ---- Home ice advantage ----
        home_xg += self.home_ice_adj

        # ---- Head-to-head adjustment ----
        h2h = features.get("h2h", {})
        if h2h.get("games_found", 0) >= 3:
            # team1 in H2H is always the home team (how we called it)
            h2h_home_wr = h2h.get("team1_win_rate", 0.5)
            h2h_deviation = h2h_home_wr - 0.5
            home_xg += h2h_deviation * H2H_FACTOR * self.league_avg
            away_xg -= h2h_deviation * H2H_FACTOR * self.league_avg

            # Also adjust for H2H scoring
            h2h_home_goals = h2h.get("team1_avg_goals", self.league_avg)
            h2h_away_goals = h2h.get("team2_avg_goals", self.league_avg)
            h2h_goal_adj = _mc.h2h_goal_adj_weight
            home_xg = home_xg * (1 - h2h_goal_adj) + h2h_home_goals * h2h_goal_adj
            away_xg = away_xg * (1 - h2h_goal_adj) + h2h_away_goals * h2h_goal_adj

        # ---- Goalie quality adjustment (with starter confidence discount) ----
        away_goalie = features.get("away_goalie", {})
        home_goalie = features.get("home_goalie", {})
        home_starter_conf = features.get("home_starter_status", {}).get("starter_confidence", 1.0)
        away_starter_conf = features.get("away_starter_status", {}).get("starter_confidence", 1.0)

        # Apply goalie adjustment but scale by starter confidence
        home_xg_before = home_xg
        home_xg = self._apply_goalie_adjustment(home_xg, away_goalie)
        goalie_delta = home_xg - home_xg_before
        home_xg = home_xg_before + goalie_delta * away_starter_conf

        away_xg_before = away_xg
        away_xg = self._apply_goalie_adjustment(away_xg, home_goalie)
        goalie_delta = away_xg - away_xg_before
        away_xg = away_xg_before + goalie_delta * home_starter_conf

        # ---- Goalie tier mismatch ----
        home_tier = home_goalie.get("tier_rank", 2)
        away_tier = away_goalie.get("tier_rank", 2)
        tier_diff = home_tier - away_tier  # positive = home has better goalie
        if abs(tier_diff) >= 1:
            mismatch_adj = tier_diff * _mc.goalie_mismatch_factor
            away_xg *= (1.0 - mismatch_adj * 0.5)
            home_xg *= (1.0 + mismatch_adj * 0.5)

        # ---- Home/away splits adjustment ----
        home_splits = features.get("home_splits", {})
        away_splits = features.get("away_splits", {})
        split_w = _mc.splits_blend_weight
        if home_splits.get("games_found", 0) >= 5:
            split_off = home_splits.get("avg_goals_for", home_xg)
            home_xg = home_xg * (1.0 - split_w) + split_off * split_w
        if away_splits.get("games_found", 0) >= 5:
            split_off = away_splits.get("avg_goals_for", away_xg)
            away_xg = away_xg * (1.0 - split_w) + split_off * split_w

        # ---- Player talent adjustment ----
        # Teams with elite top-6 forwards score more; adjust xG accordingly.
        home_skaters = features.get("home_skaters", {})
        away_skaters = features.get("away_skaters", {})
        if home_skaters.get("games_found", 0) >= 5:
            talent_diff = home_skaters.get("top6_fwd_ppg", LEAGUE_AVG_TOP6_PPG) - LEAGUE_AVG_TOP6_PPG
            home_xg *= 1.0 + talent_diff * SKATER_TALENT_FACTOR
        if away_skaters.get("games_found", 0) >= 5:
            talent_diff = away_skaters.get("top6_fwd_ppg", LEAGUE_AVG_TOP6_PPG) - LEAGUE_AVG_TOP6_PPG
            away_xg *= 1.0 + talent_diff * SKATER_TALENT_FACTOR

        # ---- Lineup depletion adjustment ----
        # Missing regular players reduce a team's expected output.
        home_lineup = features.get("home_lineup", {})
        away_lineup = features.get("away_lineup", {})
        home_strength = home_lineup.get("lineup_strength", 1.0)
        away_strength = away_lineup.get("lineup_strength", 1.0)
        if home_strength < 1.0:
            depletion = (1.0 - home_strength) * LINEUP_DEPLETION_FACTOR
            home_xg *= (1.0 - depletion)
        if away_strength < 1.0:
            depletion = (1.0 - away_strength) * LINEUP_DEPLETION_FACTOR
            away_xg *= (1.0 - depletion)

        # ---- Injury impact adjustment ----
        # Uses structured injury data for more precise lineup impact.
        home_injuries = features.get("home_injuries", {})
        away_injuries = features.get("away_injuries", {})
        home_injury_impact = home_injuries.get("xg_reduction", 0.0)
        away_injury_impact = away_injuries.get("xg_reduction", 0.0)
        if home_injury_impact > 0:
            home_xg *= (1.0 - min(home_injury_impact, _mc.injury_impact_factor))
        if away_injury_impact > 0:
            away_xg *= (1.0 - min(away_injury_impact, _mc.injury_impact_factor))

        # ---- Player matchup adjustment ----
        # Key players who historically perform well/poorly against this opponent.
        home_matchup = features.get("home_player_matchup", {})
        away_matchup = features.get("away_player_matchup", {})
        home_matchup_boost = home_matchup.get("matchup_boost", 0.0)
        away_matchup_boost = away_matchup.get("matchup_boost", 0.0)
        if home_matchup_boost != 0.0:
            home_xg *= (1.0 + home_matchup_boost * PLAYER_MATCHUP_FACTOR)
        if away_matchup_boost != 0.0:
            away_xg *= (1.0 + away_matchup_boost * PLAYER_MATCHUP_FACTOR)

        # ---- Team matchup scoring tendency ----
        # Do these two teams produce higher/lower scoring games historically?
        team_matchup = features.get("team_matchup", {})
        if team_matchup.get("games_found", 0) >= _mc.form_window_short:
            matchup_avg_total = team_matchup.get("avg_total_goals", self.league_avg * 2)
            expected_total = self.league_avg * 2
            if matchup_avg_total > 0 and expected_total > 0:
                scoring_ratio = matchup_avg_total / expected_total
                scoring_adj = (scoring_ratio - 1.0) * TEAM_MATCHUP_SCORING_FACTOR
                home_xg *= (1.0 + scoring_adj)
                away_xg *= (1.0 + scoring_adj)

        # ---- Schedule fatigue adjustment ----
        # Back-to-back and rest days affect performance.
        home_schedule = features.get("home_schedule", {})
        away_schedule = features.get("away_schedule", {})
        if home_schedule.get("is_back_to_back", False):
            home_xg -= BACK_TO_BACK_PENALTY
        if away_schedule.get("is_back_to_back", False):
            away_xg -= BACK_TO_BACK_PENALTY

        home_rest_days = home_schedule.get("days_rest", 1)
        away_rest_days = away_schedule.get("days_rest", 1)
        if home_rest_days > 1:
            rest_bonus = min((home_rest_days - 1) * REST_ADVANTAGE_PER_DAY, REST_ADVANTAGE_CAP)
            home_xg += rest_bonus
        if away_rest_days > 1:
            rest_bonus = min((away_rest_days - 1) * REST_ADVANTAGE_PER_DAY, REST_ADVANTAGE_CAP)
            away_xg += rest_bonus

        # Road trip fatigue
        away_road_games = away_schedule.get("consecutive_road_games", 0)
        if away_road_games > _mc.road_trip_fatigue_threshold:
            road_penalty = (away_road_games - _mc.road_trip_fatigue_threshold) * _mc.road_trip_fatigue_per_game
            away_xg -= min(road_penalty, 0.10)

        # ---- Schedule spot / situational awareness ----
        # Lookahead: team playing a weak opponent before a divisional rival
        # tends to underperform (saving energy for the big game).
        if home_schedule.get("is_lookahead", False):
            home_xg -= _mc.lookahead_penalty
        if away_schedule.get("is_lookahead", False):
            away_xg -= _mc.lookahead_penalty

        # Letdown: team coming off a hard-fought divisional OT game
        if home_schedule.get("is_letdown", False):
            home_xg -= _mc.lookahead_penalty * 0.75
        if away_schedule.get("is_letdown", False):
            away_xg -= _mc.lookahead_penalty * 0.75

        # Divisional games tend to be tighter / go under
        if features.get("is_divisional", False):
            home_xg -= _mc.divisional_under_adj
            away_xg -= _mc.divisional_under_adj

        # Cross-conference travel (away team faces timezone disadvantage)
        if features.get("is_cross_conference", False):
            if away_schedule.get("is_travel_disadvantage", False):
                away_xg -= _mc.timezone_penalty

        # ---- Special teams matchup adjustment ----
        # PP efficiency vs opponent PK, and vice versa.
        home_special = features.get("home_special_teams", {})
        away_special = features.get("away_special_teams", {})
        if home_special and away_special:
            # Home PP vs Away PK
            home_pp = home_special.get("pp_pct", 20.0) / 100.0
            away_pk = away_special.get("pk_pct", 80.0) / 100.0
            home_pp_advantage = (home_pp - 0.20) - ((1.0 - away_pk) - 0.20)
            # Away PP vs Home PK
            away_pp = away_special.get("pp_pct", 20.0) / 100.0
            home_pk = home_special.get("pk_pct", 80.0) / 100.0
            away_pp_advantage = (away_pp - 0.20) - ((1.0 - home_pk) - 0.20)

            home_xg += home_pp_advantage * SPECIAL_TEAMS_FACTOR
            away_xg += away_pp_advantage * SPECIAL_TEAMS_FACTOR

        # ---- Period-specific scoring rate adjustment ----
        # Teams with strong/weak period tendencies should have xG adjusted.
        # A team that scores heavily in P1 but collapses in P3 has different
        # value than raw goals-per-game suggests.
        home_periods = features.get("home_periods", {})
        away_periods = features.get("away_periods", {})
        league_period_avg = self.league_avg / 3.0  # ~1.02 per period

        if home_periods.get("games_found", 0) >= 10:
            # Sum of period averages vs expected (league_avg)
            home_period_total = (
                home_periods.get("avg_p1_for", league_period_avg)
                + home_periods.get("avg_p2_for", league_period_avg)
                + home_periods.get("avg_p3_for", league_period_avg)
            )
            period_dev = home_period_total - self.league_avg
            home_xg += period_dev * _mc.period_scoring_factor

        if away_periods.get("games_found", 0) >= 10:
            away_period_total = (
                away_periods.get("avg_p1_for", league_period_avg)
                + away_periods.get("avg_p2_for", league_period_avg)
                + away_periods.get("avg_p3_for", league_period_avg)
            )
            period_dev = away_period_total - self.league_avg
            away_xg += period_dev * _mc.period_scoring_factor

        # ---- Advanced metrics adjustment (Corsi-proxy / shot quality) ----
        # Teams that dominate possession (high Corsi%) tend to outperform
        # raw scoring stats. Teams with high shooting% are due to regress
        # while those with suppressed shooting% will bounce back.
        home_advanced = features.get("home_advanced", {})
        away_advanced = features.get("away_advanced", {})
        adv_min_games = _mc.advanced_metrics_min_games

        if home_advanced.get("games_found", 0) >= adv_min_games:
            # Corsi possession: CF% above 50 means team controls play
            home_cf_pct = home_advanced.get("corsi_for_pct", 50.0)
            cf_deviation = (home_cf_pct - 50.0) / 100.0  # e.g., 54% → +0.04
            home_xg *= 1.0 + cf_deviation * _mc.corsi_possession_factor

            # Shot quality: shooting% above league avg (~8%) suggests better chances
            home_sh_pct = home_advanced.get("shooting_pct", 8.0)
            sh_deviation = (home_sh_pct - 8.0) / 100.0
            home_xg *= 1.0 + sh_deviation * _mc.shot_quality_factor

        if away_advanced.get("games_found", 0) >= adv_min_games:
            away_cf_pct = away_advanced.get("corsi_for_pct", 50.0)
            cf_deviation = (away_cf_pct - 50.0) / 100.0
            away_xg *= 1.0 + cf_deviation * _mc.corsi_possession_factor

            away_sh_pct = away_advanced.get("shooting_pct", 8.0)
            sh_deviation = (away_sh_pct - 8.0) / 100.0
            away_xg *= 1.0 + sh_deviation * _mc.shot_quality_factor

        # ---- 5v5 Even-strength possession adjustment ----
        # True 5v5 Corsi from MoneyPuck is more predictive than our
        # all-situations Corsi proxy since it filters out PP/PK noise.
        home_ev = features.get("home_ev_possession", {})
        away_ev = features.get("away_ev_possession", {})
        if home_ev.get("games_found", 0) >= _mc.ev_corsi_min_games:
            ev_deviation = (home_ev.get("ev_cf_pct", 50.0) - 50.0) / 100.0
            home_xg *= 1.0 + ev_deviation * _mc.ev_corsi_factor
        if away_ev.get("games_found", 0) >= _mc.ev_corsi_min_games:
            ev_deviation = (away_ev.get("ev_cf_pct", 50.0) - 50.0) / 100.0
            away_xg *= 1.0 + ev_deviation * _mc.ev_corsi_factor

        # ---- Close-game possession adjustment ----
        # CF% in close games (1-goal margin / OT) filters out score effects
        # from blowouts and is a better predictor of sustained quality.
        home_close = features.get("home_close_possession", {})
        away_close = features.get("away_close_possession", {})
        if home_close.get("close_games_found", 0) >= _mc.close_game_min_games:
            close_dev = (home_close.get("close_cf_pct", 50.0) - 50.0) / 100.0
            home_xg *= 1.0 + close_dev * _mc.close_game_corsi_factor
        if away_close.get("close_games_found", 0) >= _mc.close_game_min_games:
            close_dev = (away_close.get("close_cf_pct", 50.0) - 50.0) / 100.0
            away_xg *= 1.0 + close_dev * _mc.close_game_corsi_factor

        # ---- PDO regression (luck adjustment) ----
        # PDO = shooting% + save%. League average is ~1.000.
        # Teams with PDO far from 1.0 are running hot/cold and due to regress.
        # High PDO (>1.010) → xG inflated by luck → reduce.
        # Low PDO (<0.990) → xG depressed by bad luck → increase.
        pdo_factor = _mc.pdo_regression_factor
        if pdo_factor > 0:
            home_pdo = features.get("home_form_10", {}).get("pdo", 1.0)
            away_pdo = features.get("away_form_10", {}).get("pdo", 1.0)
            if home_pdo != 1.0:
                home_xg -= (home_pdo - 1.0) * pdo_factor * self.league_avg
            if away_pdo != 1.0:
                away_xg -= (away_pdo - 1.0) * pdo_factor * self.league_avg

        # ---- Goalie recent save% trend (hot/cold streaks) ----
        # A goalie whose L5 save% is significantly above/below their season
        # average is on a streak that should shift expected goals.
        goalie_trend_factor = _mc.goalie_trend_factor
        if goalie_trend_factor > 0:
            # Home goalie trend affects away_xg (a hot home goalie suppresses away scoring)
            home_g = features.get("home_goalie", {})
            away_g = features.get("away_goalie", {})
            home_l5_sv = home_g.get("last5_save_pct", 0.0)
            home_season_sv = home_g.get("season_save_pct", 0.0)
            if home_l5_sv > 0 and home_season_sv > 0:
                sv_trend = home_l5_sv - home_season_sv  # positive = hot streak
                away_xg *= 1.0 - sv_trend * goalie_trend_factor * 10.0  # scale: .010 sv% diff → 1.5% xG shift

            away_l5_sv = away_g.get("last5_save_pct", 0.0)
            away_season_sv = away_g.get("season_save_pct", 0.0)
            if away_l5_sv > 0 and away_season_sv > 0:
                sv_trend = away_l5_sv - away_season_sv
                home_xg *= 1.0 - sv_trend * goalie_trend_factor * 10.0

        # ---- Goalie vs. specific opponent adjustment ----
        # A goalie who historically performs poorly against this opponent
        # (low SV%, high GAA) should have opponent xG adjusted upward.
        goalie_vs_factor = _mc.goalie_vs_team_factor
        if goalie_vs_factor > 0:
            home_gvt = features.get("home_goalie_vs_team", {})
            away_gvt = features.get("away_goalie_vs_team", {})

            # Home goalie vs away team → affects away_xg
            if home_gvt.get("significant", False):
                vs_sv = home_gvt["vs_save_pct"]
                season_sv = features.get("home_goalie", {}).get("season_save_pct", 0.900)
                sv_diff = vs_sv - season_sv  # negative = worse vs this team
                away_xg *= 1.0 - sv_diff * goalie_vs_factor * 10.0

            # Away goalie vs home team → affects home_xg
            if away_gvt.get("significant", False):
                vs_sv = away_gvt["vs_save_pct"]
                season_sv = features.get("away_goalie", {}).get("season_save_pct", 0.900)
                sv_diff = vs_sv - season_sv
                home_xg *= 1.0 - sv_diff * goalie_vs_factor * 10.0

        # ---- Goalie venue splits adjustment ----
        # A goalie performing notably differently at home vs away shifts xG.
        venue_factor = _mc.goalie_venue_factor
        if venue_factor > 0:
            home_venue = features.get("home_goalie_venue", {})
            away_venue = features.get("away_goalie_venue", {})

            if home_venue.get("significant", False):
                venue_sv = home_venue["venue_save_pct"]
                season_sv = features.get("home_goalie", {}).get("season_save_pct", 0.900)
                sv_diff = venue_sv - season_sv
                away_xg *= 1.0 - sv_diff * venue_factor * 10.0

            if away_venue.get("significant", False):
                venue_sv = away_venue["venue_save_pct"]
                season_sv = features.get("away_goalie", {}).get("season_save_pct", 0.900)
                sv_diff = venue_sv - season_sv
                home_xg *= 1.0 - sv_diff * venue_factor * 10.0

        # ---- Goalie workload fatigue adjustment ----
        # A goalie who has faced heavy shot volume recently is more
        # fatigued than consecutive starts alone captures.
        wl_factor = _mc.goalie_workload_factor
        if wl_factor > 0:
            home_wl = features.get("home_goalie_workload", {})
            away_wl = features.get("away_goalie_workload", {})

            if home_wl.get("heavy_workload", False):
                away_xg *= home_wl.get("workload_factor", 1.0)
            if away_wl.get("heavy_workload", False):
                home_xg *= away_wl.get("workload_factor", 1.0)

        # ---- Pace / tempo matchup adjustment ----
        # Two fast teams create more total goals than individual averages
        # suggest. Two slow teams create fewer. Model the interaction.
        pace_factor = _mc.pace_interaction_factor
        if pace_factor > 0:
            home_pace = features.get("home_pace", {})
            away_pace = features.get("away_pace", {})
            if (home_pace.get("games_found", 0) >= _mc.pace_min_games and
                    away_pace.get("games_found", 0) >= _mc.pace_min_games):
                combined_pace = home_pace.get("pace", 60.0) + away_pace.get("pace", 60.0)
                league_avg_pace = 120.0  # 60 shots/game per team (2 teams)
                pace_deviation = (combined_pace - league_avg_pace) / league_avg_pace
                pace_adj = pace_deviation * pace_factor
                home_xg *= 1.0 + pace_adj
                away_xg *= 1.0 + pace_adj

        # ---- Score-close performance adjustment ----
        # Teams that perform well in tight games are more likely to
        # sustain that output than teams padding stats in blowouts.
        sc_factor = _mc.score_close_factor
        if sc_factor > 0:
            home_sc = features.get("home_score_close", {})
            away_sc = features.get("away_score_close", {})
            if home_sc.get("close_games_found", 0) >= _mc.score_close_min_games:
                close_off = home_sc.get("close_gf_pg", home_xg)
                home_xg = home_xg * (1.0 - sc_factor) + close_off * sc_factor
            if away_sc.get("close_games_found", 0) >= _mc.score_close_min_games:
                close_off = away_sc.get("close_gf_pg", away_xg)
                away_xg = away_xg * (1.0 - sc_factor) + close_off * sc_factor

        # ---- Penalty discipline adjustment ----
        # Undisciplined teams give opponents more power-play chances,
        # effectively boosting the opponent's expected goals.
        home_disc = features.get("home_discipline", {})
        away_disc = features.get("away_discipline", {})
        disc_factor = _mc.penalty_discipline_factor
        if disc_factor > 0 and home_disc.get("games_found", 0) >= 5 and away_disc.get("games_found", 0) >= 5:
            # Discipline rating: 0 = undisciplined (12+ PIM), 1 = disciplined (4 PIM)
            # Undisciplined team boosts opponent's xG
            home_disc_rating = home_disc.get("discipline_rating", 0.5)
            away_disc_rating = away_disc.get("discipline_rating", 0.5)
            # How much opponent benefits from our lack of discipline
            away_xg += (0.5 - home_disc_rating) * disc_factor
            home_xg += (0.5 - away_disc_rating) * disc_factor

        # ---- Close-game record (clutch factor) ----
        # Teams that consistently win/lose tight games have a clutch factor
        # that raw xG misses — mental toughness, coaching, late-game execution.
        home_close_rec = features.get("home_close_record", {})
        away_close_rec = features.get("away_close_record", {})
        close_factor = _mc.close_game_record_factor
        if close_factor > 0:
            min_close = _mc.close_game_record_min_games
            if home_close_rec.get("close_games_found", 0) >= min_close:
                close_dev = home_close_rec["close_game_win_rate"] - 0.5
                home_xg += close_dev * close_factor
            if away_close_rec.get("close_games_found", 0) >= min_close:
                close_dev = away_close_rec["close_game_win_rate"] - 0.5
                away_xg += close_dev * close_factor

        # ---- Scoring-first tendency ----
        # Teams that win the first period have a significant advantage.
        # NHL teams that score first win ~67% of the time.
        scoring_first_factor = _mc.scoring_first_factor
        if scoring_first_factor > 0:
            min_p1 = _mc.scoring_first_min_games
            home_sf_rate = home_close_rec.get("scoring_first_rate", 0.5)
            away_sf_rate = away_close_rec.get("scoring_first_rate", 0.5)
            if home_close_rec.get("close_games_found", 0) >= 5:
                home_xg += (home_sf_rate - 0.35) * scoring_first_factor  # 0.35 = league avg rate of leading after P1
            if away_close_rec.get("close_games_found", 0) >= 5:
                away_xg += (away_sf_rate - 0.35) * scoring_first_factor

        # ---- Feature #6: PP opportunity rate vs opponent ----
        # An undisciplined team facing an elite PP gives up more goals
        # than PP% and PK% alone capture. Adjust based on opportunity
        # rate differential.
        pp_factor = _mc.pp_opportunity_factor
        if pp_factor > 0:
            home_pp_opp = features.get("home_pp_opportunity", {})
            away_pp_opp = features.get("away_pp_opportunity", {})
            if (home_pp_opp.get("games_found", 0) >= _mc.pp_opportunity_min_games and
                    away_pp_opp.get("games_found", 0) >= _mc.pp_opportunity_min_games):
                # Net PP impact: positive = team generates more PP goals than it gives up
                home_net = home_pp_opp.get("net_pp_impact", 0.0)
                away_net = away_pp_opp.get("net_pp_impact", 0.0)
                home_xg += home_net * pp_factor
                away_xg += away_net * pp_factor

        # ---- Feature #7: Shooting quality against (HDSV% proxy) ----
        # Teams that face higher-quality shots have an inflated GAA.
        # A goalie with good GSAE is stopping harder shots than average.
        sq_factor = _mc.shot_quality_against_factor
        if sq_factor > 0:
            home_sq = features.get("home_shot_quality", {})
            away_sq = features.get("away_shot_quality", {})
            # Away team shoots against home defense. If home defense
            # faces hard shots (quality_index > 1) but still has good
            # GSAE, the defense is better than raw stats show.
            if home_sq.get("games_found", 0) >= _mc.shot_quality_min_games:
                gsae = home_sq.get("goals_saved_above_expected", 0.0)
                # Positive GSAE = stopping more than expected → reduce away xG
                away_xg -= gsae / max(home_sq["games_found"], 1) * sq_factor
            if away_sq.get("games_found", 0) >= _mc.shot_quality_min_games:
                gsae = away_sq.get("goals_saved_above_expected", 0.0)
                home_xg -= gsae / max(away_sq["games_found"], 1) * sq_factor

        # ---- Feature #9: Line combination stability ----
        # Unstable forward lines (missing regulars, recent trades) have
        # reduced chemistry. Penalize teams with low top-6 stability.
        ls_factor = _mc.line_stability_factor
        if ls_factor > 0:
            home_ls = features.get("home_line_stability", {})
            away_ls = features.get("away_line_stability", {})
            ls_threshold = _mc.line_stability_threshold
            if home_ls.get("games_found", 0) >= 5:
                stability = home_ls.get("top6_stability", 1.0)
                if stability < ls_threshold:
                    home_xg *= 1.0 - (ls_threshold - stability) * ls_factor
            if away_ls.get("games_found", 0) >= 5:
                stability = away_ls.get("top6_stability", 1.0)
                if stability < ls_threshold:
                    away_xg *= 1.0 - (ls_threshold - stability) * ls_factor

        # ---- Feature #11: Recency-weighted H2H ----
        # When the recency-weighted H2H diverges from the raw H2H,
        # the matchup dynamics are shifting. Apply the delta as an
        # additional adjustment.
        h2h_recency_factor = _mc.h2h_recency_factor
        if h2h_recency_factor > 0:
            h2h_w = features.get("h2h_weighted", {})
            if h2h_w.get("games_found", 0) >= 3:
                recency_shift = h2h_w.get("recency_shift", 0.0)
                # Positive shift = home team trending up in this matchup
                home_xg += recency_shift * h2h_recency_factor * self.league_avg
                away_xg -= recency_shift * h2h_recency_factor * self.league_avg

        # ---- Signal convergence multiplier ----
        # When multiple independent signals all point the same direction,
        # the prediction should be MORE confident, not washed out by
        # regression. Count strong pro-home signals and amplify if they
        # converge.
        convergence_signals = 0
        # Form advantage
        home_wr = features.get("home_form_5", {}).get("win_rate", 0.5)
        away_wr = features.get("away_form_5", {}).get("win_rate", 0.5)
        if home_wr - away_wr > 0.20:
            convergence_signals += 1
        elif away_wr - home_wr > 0.20:
            convergence_signals -= 1
        # Goalie tier advantage
        if abs(tier_diff) >= 1:
            convergence_signals += 1 if tier_diff > 0 else -1
        # Possession advantage
        home_cf = features.get("home_ev_possession", {}).get("ev_cf_pct", 50.0)
        away_cf = features.get("away_ev_possession", {}).get("ev_cf_pct", 50.0)
        if home_cf - away_cf > 3.0:
            convergence_signals += 1
        elif away_cf - home_cf > 3.0:
            convergence_signals -= 1
        # Rest/schedule advantage
        if home_schedule.get("is_back_to_back", False) and not away_schedule.get("is_back_to_back", False):
            convergence_signals -= 1
        elif away_schedule.get("is_back_to_back", False) and not home_schedule.get("is_back_to_back", False):
            convergence_signals += 1
        # Injury advantage
        home_inj_impact = home_injuries.get("xg_reduction", 0.0)
        away_inj_impact = away_injuries.get("xg_reduction", 0.0)
        if away_inj_impact - home_inj_impact > 0.05:
            convergence_signals += 1
        elif home_inj_impact - away_inj_impact > 0.05:
            convergence_signals -= 1

        # Apply convergence amplifier when threshold is met
        if abs(convergence_signals) >= _mc.convergence_threshold:
            amp = _mc.convergence_amplifier
            if convergence_signals > 0:
                home_xg += amp
                away_xg -= amp * 0.5
            else:
                away_xg += amp
                home_xg -= amp * 0.5

        # ---- Regression toward league average ----
        # Hot-streak form weights and weak-opponent defensive factors can
        # compound to produce unrealistic xG values.  Regress toward
        # the league average to dampen extremes while preserving signal.
        reg = _mc.mean_regression
        home_xg = home_xg * (1.0 - reg) + self.league_avg * reg
        away_xg = away_xg * (1.0 - reg) + self.league_avg * reg

        # ---- Floor / ceiling ----
        home_xg = max(_mc.xg_floor, min(_mc.xg_ceiling, home_xg))
        away_xg = max(_mc.xg_floor, min(_mc.xg_ceiling, away_xg))

        # ---- ML model blend ----
        # When a trained ML model is available, blend its xG predictions
        # with the Poisson-based xG. The blend weight controls how much
        # influence the ML model has (0 = pure Poisson, 1 = pure ML).
        if self.ml_model and self.ml_model.is_trained:
            blend = _mc.ml_blend_weight
            if blend > 0:
                try:
                    ml_home, ml_away = self.ml_model.predict_xg(features)
                    home_xg = home_xg * (1.0 - blend) + ml_home * blend
                    away_xg = away_xg * (1.0 - blend) + ml_away * blend
                    # Re-apply floor/ceiling after blending
                    home_xg = max(_mc.xg_floor, min(_mc.xg_ceiling, home_xg))
                    away_xg = max(_mc.xg_floor, min(_mc.xg_ceiling, away_xg))
                except Exception as e:
                    logger.warning("ML model prediction failed, using Poisson only: %s", e)

        return round(home_xg, 3), round(away_xg, 3)

    def _weighted_goals_for(
        self,
        form5: float,
        form10: float,
        season: float,
    ) -> float:
        """Compute a weighted average of goals scored across three windows."""
        return (
            WEIGHT_FORM_5 * form5
            + WEIGHT_FORM_10 * form10
            + WEIGHT_SEASON * season
        )

    @staticmethod
    def calibrate_probability(raw_prob: float) -> float:
        """Apply a simple calibration curve to model probabilities.

        Feature #12: Models tend to be overconfident at extremes (saying
        70% when the actual win rate is 62%) and underconfident near 50%.
        This applies a mild logistic shrinkage toward 50% to correct.

        Based on typical NHL model calibration:
        - 50% stays 50%
        - 60% becomes ~58%
        - 70% becomes ~66%
        - 80% becomes ~75%

        The strength is controlled by a shrinkage factor (0 = no change,
        1 = always 50%). We use 0.12 as a reasonable default.
        """
        if not _mc.calibration_enabled:
            return raw_prob
        # Mild shrinkage toward 50%
        shrinkage = 0.12
        calibrated = raw_prob * (1.0 - shrinkage) + 0.5 * shrinkage
        return round(max(0.01, min(0.99, calibrated)), 4)

    def _defensive_factor(
        self,
        goals_against_pg: float,
        shots_against_pg: float = 0.0,
        faceoff_pct: float = 50.0,
    ) -> float:
        """
        Calculate a defensive quality factor blending goals-against,
        shots-against, and faceoff win% for stability.

        A team that allows more than league average has a factor > 1.0
        (making the opponent's xG higher), and vice versa.

        Three inputs capture different defensive aspects:
        - Goals-against: outcome (noisy, goalie-dependent)
        - Shots-against: shot suppression (more repeatable)
        - Faceoff%: possession control (most repeatable, ~0.68 YoY r)
        """
        if self.league_avg == 0:
            return 1.0

        ga_ratio = goals_against_pg / self.league_avg

        # Blend in shots-against if available
        shot_blend = _mc.defense_shot_blend
        if shots_against_pg > 0 and shot_blend > 0:
            sa_ratio = shots_against_pg / _mc.league_avg_shots_against
            raw = ga_ratio * (1.0 - shot_blend) + sa_ratio * shot_blend
        else:
            raw = ga_ratio

        # Faceoff adjustment: teams winning >50% of draws control possession
        # → fewer opponent shots → better defense. Scale effect modestly.
        fo_weight = _mc.faceoff_defense_weight
        if fo_weight > 0 and faceoff_pct != 50.0:
            # Convert faceoff% to a ratio around 1.0 (50% = neutral)
            # Higher faceoff% = better defense = lower factor
            fo_adj = 1.0 - (faceoff_pct - 50.0) / 100.0
            raw = raw * (1.0 - fo_weight) + fo_adj * raw * fo_weight

        # Regress toward 1.0 using the configured regression factor
        return 1.0 + (raw - 1.0) * _mc.defensive_regression

    def _apply_goalie_adjustment(
        self,
        xg: float,
        opposing_goalie: Dict[str, Any],
    ) -> float:
        """
        Adjust expected goals based on opposing goalie quality.

        Uses a blend of recent (last 5) and season save percentage
        compared to the league average.
        """
        if not opposing_goalie or opposing_goalie.get("goalie_id") is None:
            return xg

        # Weighted goalie save percentage (favor recent form)
        last5_sv = opposing_goalie.get("last5_save_pct", LEAGUE_AVG_SAVE_PCT)
        season_sv = opposing_goalie.get("season_save_pct", LEAGUE_AVG_SAVE_PCT)
        recent_w = _mc.goalie_recent_weight
        goalie_sv = recent_w * last5_sv + (1.0 - recent_w) * season_sv

        # How much better/worse than average the goalie is
        sv_diff = goalie_sv - LEAGUE_AVG_SAVE_PCT

        # A better goalie (positive sv_diff) reduces expected goals
        adjustment = 1.0 - (sv_diff / (1.0 - LEAGUE_AVG_SAVE_PCT)) * GOALIE_FACTOR
        adjustment = max(0.7, min(1.3, adjustment))

        # Goalie fatigue: consecutive starts degrade performance
        consecutive = opposing_goalie.get("consecutive_starts", 0)
        threshold = _mc.goalie_fatigue_starts_threshold
        if consecutive > threshold:
            fatigue_penalty = (consecutive - threshold) * _mc.goalie_fatigue_per_start
            # Tired goalie = higher xG for the opponent (weaker saves)
            adjustment += min(fatigue_penalty, 0.10)

        return xg * adjustment

    # ------------------------------------------------------------------ #
    #  Poisson helpers                                                    #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _poisson_prob(lam: float, k: int) -> float:
        """Probability of exactly k goals given expected goals lam."""
        return float(poisson.pmf(k, lam))

    @staticmethod
    def _poisson_prob_under(lam: float, k: int) -> float:
        """Probability of k or FEWER goals (i.e., <= k)."""
        return float(poisson.cdf(k, lam))

    def _score_matrix(
        self,
        home_xg: float,
        away_xg: float,
        max_goals: int = POISSON_MAX_GOALS,
        correlation: float | None = None,
    ) -> List[List[float]]:
        """
        Build a joint probability matrix for (home_goals, away_goals)
        using a bivariate Poisson model.

        The bivariate Poisson adds a shared "game pace" component (lambda_c)
        that models the correlation between home and away scoring. In
        high-event games (bad goaltending, fast pace), both teams tend to
        score more. This improves total goals predictions specifically.

        When correlation=0, this reduces to independent Poisson.

        The model decomposes:
          home_goals = X + Z,  away_goals = Y + Z
        where X ~ Poisson(lam_h - lam_c), Y ~ Poisson(lam_a - lam_c),
        Z ~ Poisson(lam_c), and all are independent.

        P(home=i, away=j) = sum_{k=0}^{min(i,j)} P(X=i-k)*P(Y=j-k)*P(Z=k)
        """
        if correlation is None:
            correlation = _mc.scoring_correlation

        # Clamp correlation so individual lambdas stay positive
        lam_c = min(correlation, home_xg * 0.95, away_xg * 0.95)
        lam_c = max(lam_c, 0.0)

        lam_h = home_xg - lam_c
        lam_a = away_xg - lam_c

        # Precompute marginal PMFs
        pmf_h = [float(poisson.pmf(k, lam_h)) for k in range(max_goals + 1)]
        pmf_a = [float(poisson.pmf(k, lam_a)) for k in range(max_goals + 1)]

        # Fast path: independent Poisson when no correlation
        if lam_c == 0.0:
            return [
                [pmf_h[i] * pmf_a[j] for j in range(max_goals + 1)]
                for i in range(max_goals + 1)
            ]

        # Full bivariate Poisson with shared game-pace component
        pmf_c = [float(poisson.pmf(k, lam_c)) for k in range(max_goals + 1)]

        matrix = [[0.0] * (max_goals + 1) for _ in range(max_goals + 1)]
        for i in range(max_goals + 1):
            for j in range(max_goals + 1):
                p = 0.0
                for k in range(min(i, j) + 1):
                    p += pmf_h[i - k] * pmf_a[j - k] * pmf_c[k]
                matrix[i][j] = p
        return matrix

    # ------------------------------------------------------------------ #
    #  Prediction: Total Goals                                            #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _normalize_total_line(line: float) -> float:
        """Normalize a total line to a .5 increment.

        NHL sportsbooks almost always post totals ending in .5 (e.g., 5.5,
        6.5).  Some data sources return whole numbers (e.g., 6 or 7) due to
        rounding or different conventions.  A whole-number total introduces a
        push possibility that complicates probability calculations and
        produces prediction labels like "Under 7.0" that don't match what
        bettors actually see on the sportsbook.

        Strategy: snap whole numbers DOWN to .5 — e.g., 7 → 6.5, 6 → 5.5.
        This matches the odds scraper convention (int(x) - 1 + 0.5) and the
        assumption that whole numbers are rounded-up .5 lines.
        """
        if line % 1 == 0.5:
            return line  # already a .5 line
        # Whole number: snap down to the .5 below (7 → 6.5, 6 → 5.5).
        # Consistent with odds_api.py: float(int(ou_raw) - 1) + 0.5
        return float(int(line) - 1) + 0.5

    async def predict_total_goals(
        self,
        features: Dict[str, Any],
        *,
        _precomputed: Tuple[float, float, List[List[float]]] | None = None,
    ) -> Dict[str, Any]:
        """
        Predict total goals using the Poisson model.

        Calculates over/under probabilities for standard lines (4.5, 5.5, 6.5)
        plus the actual sportsbook O/U line when available.

        Returns:
            dict with home_xg, away_xg, total_xg, and probabilities for
            each over/under line.
        """
        if _precomputed:
            home_xg, away_xg, matrix = _precomputed
        else:
            home_xg, away_xg = self._calc_expected_goals(features)
            matrix = self._score_matrix(home_xg, away_xg)
        total_xg = home_xg + away_xg
        max_g = POISSON_MAX_GOALS

        # Build the set of lines to evaluate:
        # 1) Standard lines (3.5 through 8.5)
        # 2) The primary sportsbook line
        # 3) ALL available alternate lines from sportsbooks
        eval_lines = set(TOTAL_LINES)
        odds_data = features.get("odds", {})
        book_ou = odds_data.get("over_under_line")
        if book_ou is not None:
            normalized = self._normalize_total_line(float(book_ou))
            if normalized < 4.5 or normalized > 8.5:
                logger.warning(
                    "Discarding implausible sportsbook O/U line %.1f "
                    "(normalized from %.1f) — outside 4.5-8.5 range",
                    normalized, float(book_ou),
                )
                odds_data.pop("over_under_line", None)
                odds_data.pop("over_price", None)
                odds_data.pop("under_price", None)
            else:
                eval_lines.add(normalized)
                if normalized != float(book_ou):
                    logger.info(
                        "Normalized sportsbook O/U line %.1f -> %.1f",
                        float(book_ou), normalized,
                    )
                    odds_data["over_under_line"] = normalized

        # Add all available alternate total lines
        all_total_lines = odds_data.get("all_total_lines") or []
        for alt in all_total_lines:
            alt_line = alt.get("line", 0)
            if 4.0 <= alt_line <= 9.0:
                eval_lines.add(self._normalize_total_line(alt_line))

        lines = {}
        for line in sorted(eval_lines):
            over_prob = 0.0
            under_prob = 0.0
            # For .5 lines: int(5.5)=5, over means total>5 i.e. >=6. Correct.
            threshold = int(line)
            for i in range(max_g + 1):
                for j in range(max_g + 1):
                    total = i + j
                    if total > threshold:
                        over_prob += matrix[i][j]
                    else:
                        under_prob += matrix[i][j]

            lines[f"over_{line}"] = round(over_prob, 4)
            lines[f"under_{line}"] = round(under_prob, 4)

        return {
            "home_xg": home_xg,
            "away_xg": away_xg,
            "total_xg": round(total_xg, 3),
            "lines": lines,
        }

    # ------------------------------------------------------------------ #
    #  Prediction: Moneyline                                              #
    # ------------------------------------------------------------------ #

    async def predict_moneyline(
        self,
        features: Dict[str, Any],
        *,
        _precomputed: Tuple[float, float, List[List[float]]] | None = None,
    ) -> Dict[str, Any]:
        """
        Predict moneyline (win probability) for each team.

        Derives probabilities from the Poisson score matrix:
          - P(home win) = sum of all (i, j) where i > j
          - P(away win) = sum of all (i, j) where j > i
          - P(draw) = sum of all (i, j) where i == j (regulation draw -> OT)

        Since NHL games always have a winner, draw probability is split
        between home and away based on OT tendency.

        Returns:
            dict with home_win_prob, away_win_prob, draw_prob_regulation.
        """
        if _precomputed:
            home_xg, away_xg, matrix = _precomputed
        else:
            home_xg, away_xg = self._calc_expected_goals(features)
            matrix = self._score_matrix(home_xg, away_xg)
        max_g = POISSON_MAX_GOALS

        home_win = 0.0
        away_win = 0.0
        draw = 0.0

        for i in range(max_g + 1):
            for j in range(max_g + 1):
                if i > j:
                    home_win += matrix[i][j]
                elif j > i:
                    away_win += matrix[i][j]
                else:
                    draw += matrix[i][j]

        # In the NHL, draws go to OT/SO. Home team has slight OT advantage.
        home_ot = features.get("home_ot", {})
        away_ot = features.get("away_ot", {})
        home_ot_wr = home_ot.get("ot_win_rate", 0.52)
        away_ot_wr = away_ot.get("ot_win_rate", 0.48)

        # Normalize OT win rates
        ot_total = home_ot_wr + away_ot_wr
        if ot_total > 0:
            home_ot_share = home_ot_wr / ot_total
        else:
            home_ot_share = 0.52

        # Distribute draw probability
        home_win_total = home_win + draw * home_ot_share
        away_win_total = away_win + draw * (1.0 - home_ot_share)

        # Normalize to ensure they sum to 1.0
        total_prob = home_win_total + away_win_total
        if total_prob > 0:
            home_win_total /= total_prob
            away_win_total /= total_prob

        return {
            "home_win_prob": round(home_win_total, 4),
            "away_win_prob": round(away_win_total, 4),
            "draw_prob_regulation": round(draw, 4),
            "home_xg": home_xg,
            "away_xg": away_xg,
        }

    # ------------------------------------------------------------------ #
    #  Prediction: Spread / Puck Line                                     #
    # ------------------------------------------------------------------ #

    async def predict_spread(
        self,
        features: Dict[str, Any],
        *,
        _precomputed: Tuple[float, float, List[List[float]]] | None = None,
    ) -> Dict[str, Any]:
        """
        Predict spread (puck line) probabilities.

        Computes cover probabilities for the standard 1.5 puck line plus
        any additional spread lines available from sportsbooks.

        The actual bettable puck line pairs are:
          - Favorite -X.5 vs Underdog +X.5 (complements)

        Returns:
            dict with predicted_margin, spread probabilities for each side.
        """
        if _precomputed:
            home_xg, away_xg, matrix = _precomputed
        else:
            home_xg, away_xg = self._calc_expected_goals(features)
            matrix = self._score_matrix(home_xg, away_xg)
        max_g = POISSON_MAX_GOALS

        predicted_margin = round(home_xg - away_xg, 3)

        # Build set of spread lines to evaluate
        eval_spread_lines = {1.5}  # Always include standard puck line
        odds_data = features.get("odds", {})
        all_spread_lines = odds_data.get("all_spread_lines") or []
        for alt in all_spread_lines:
            line_val = alt.get("line", 1.5)
            if line_val < 2.5:
                eval_spread_lines.add(line_val)

        # Calculate spread probabilities for each line
        spreads = {}
        for spread_val in sorted(eval_spread_lines):
            home_minus = 0.0  # P(home wins by spread_val+): margin > spread_val
            away_minus = 0.0  # P(away wins by spread_val+): margin < -spread_val
            for i in range(max_g + 1):
                for j in range(max_g + 1):
                    m = i - j
                    if m > spread_val:
                        home_minus += matrix[i][j]
                    if m < -spread_val:
                        away_minus += matrix[i][j]

            spreads[f"home_-{spread_val}"] = round(home_minus, 4)
            spreads[f"home_+{spread_val}"] = round(1.0 - away_minus, 4)
            spreads[f"away_-{spread_val}"] = round(away_minus, 4)
            spreads[f"away_+{spread_val}"] = round(1.0 - home_minus, 4)

        return {
            "predicted_margin": predicted_margin,
            "home_xg": home_xg,
            "away_xg": away_xg,
            "spreads": spreads,
        }

    # ------------------------------------------------------------------ #
    #  Live-game adjustment                                                #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _calc_remaining_fraction(live_state: Dict[str, Any]) -> float:
        """Return what fraction of the 60-minute regulation game remains.

        Uses period number and clock (MM:SS counting down) from the live
        game state to compute how much game time is left.
        """
        period = live_state.get("period") or 1
        clock_str = live_state.get("clock")
        period_type = (live_state.get("period_type") or "").upper()

        PERIOD_SECS = 20 * 60   # 1200
        GAME_SECS = 60 * 60     # 3600

        # Overtime — treat as ~5 minutes of play
        if "OT" in period_type or period > 3:
            return 5 * 60 / GAME_SECS

        # Parse "MM:SS" clock
        period_remaining = 0
        if clock_str:
            try:
                parts = clock_str.strip().split(":")
                mins = int(parts[0])
                secs = int(parts[1]) if len(parts) > 1 else 0
                period_remaining = mins * 60 + secs
            except (ValueError, IndexError):
                pass

        remaining_full_periods = max(0, 3 - period)
        total_remaining = period_remaining + remaining_full_periods * PERIOD_SECS
        return max(total_remaining / GAME_SECS, 0.01)

    def adjust_for_live_state(
        self,
        predictions: List[Dict[str, Any]],
        features: Dict[str, Any],
        live_state: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Adjust pre-game predictions for a game currently in progress.

        Uses current score + remaining-time Poisson model to produce
        live-accurate probabilities.  Without this, a team trailing 0-4
        would keep its pre-game ~60 % confidence, creating a phantom edge
        against live sportsbook odds that already price in the deficit.
        """
        home_score = live_state.get("home_score") or 0
        away_score = live_state.get("away_score") or 0
        period = live_state.get("period") or 1

        # If we have no score data at all, don't adjust
        if home_score == 0 and away_score == 0 and period <= 1:
            return predictions

        remaining = self._calc_remaining_fraction(live_state)

        home_abbr = features.get("home_team_abbr", "HOM")
        away_abbr = features.get("away_team_abbr", "AWY")
        home_name = features.get("home_team_name", "Home")
        away_name = features.get("away_team_name", "Away")
        odds_data = features.get("odds", {})

        # Full-game xG (same model as pre-game)
        home_xg, away_xg = self._calc_expected_goals(features)

        # Scale to remaining time
        rem_home = max(home_xg * remaining, 0.05)
        rem_away = max(away_xg * remaining, 0.05)

        # ---- Score state tendencies ----
        # NHL teams play differently based on the score state.
        score_diff = home_score - away_score  # positive = home leads

        if period >= 3 and remaining < 0.35:
            if score_diff == -1:
                # Home trailing by 1 in 3rd: desperation scoring boost
                rem_home *= (1.0 + _mc.trailing_desperation_boost)
                rem_away *= (1.0 - _mc.leading_shell_reduction * 0.5)
            elif score_diff == 1:
                # Home leading by 1 in 3rd: opponent desperation
                rem_away *= (1.0 + _mc.trailing_desperation_boost)
                rem_home *= (1.0 - _mc.leading_shell_reduction * 0.5)
            elif score_diff <= -2:
                # Home trailing by 2+ in 3rd: pulled goalie territory
                if remaining < 0.10:  # last ~3.5 minutes
                    rem_home *= (1.0 + _mc.pulled_goalie_boost)
                    rem_away *= (1.0 + _mc.pulled_goalie_boost * 0.3)  # empty net goals
                else:
                    rem_home *= (1.0 + _mc.trailing_desperation_boost * 0.7)
            elif score_diff >= 2:
                # Home leading by 2+: conservative play, games go under
                rem_home *= (1.0 - _mc.leading_shell_reduction)
                rem_away *= (1.0 - _mc.leading_shell_reduction * 0.5)
                if remaining < 0.10:
                    # Opponent may pull goalie
                    rem_away *= (1.0 + _mc.pulled_goalie_boost)
                    rem_home *= (1.0 + _mc.pulled_goalie_boost * 0.3)

        # Ensure minimums
        rem_home = max(rem_home, 0.05)
        rem_away = max(rem_away, 0.05)

        # Poisson matrix for *remaining* goals only
        matrix = self._score_matrix(rem_home, rem_away)
        max_g = POISSON_MAX_GOALS

        # --- ML: live win probabilities considering current score ---
        home_wp = 0.0
        away_wp = 0.0
        reg_tie = 0.0
        for i in range(max_g + 1):
            for j in range(max_g + 1):
                fh = home_score + i
                fa = away_score + j
                p = matrix[i][j]
                if fh > fa:
                    home_wp += p
                elif fa > fh:
                    away_wp += p
                else:
                    reg_tie += p

        # Redistribute regulation ties via OT (slight home edge)
        ot_home = 0.52
        home_wp += reg_tie * ot_home
        away_wp += reg_tie * (1 - ot_home)
        total_p = home_wp + away_wp
        if total_p > 0:
            home_wp /= total_p
            away_wp /= total_p

        # Score description for reasoning text
        if home_score > away_score:
            score_note = f"{home_name} leads {home_score}-{away_score}"
        elif away_score > home_score:
            score_note = f"{away_name} leads {away_score}-{home_score}"
        else:
            score_note = f"Tied {home_score}-{away_score}"
        pct_left = f"{remaining:.0%}"

        adjusted: List[Dict[str, Any]] = []
        for pred in predictions:
            pred = dict(pred)  # shallow copy
            bt = pred["bet_type"]

            if bt == "ml":
                # Pick the team with higher live probability
                if home_wp >= away_wp:
                    pick, conf = home_abbr, home_wp
                    pick_name = home_name
                    pick_odds = odds_data.get("home_moneyline")
                else:
                    pick, conf = away_abbr, away_wp
                    pick_name = away_name
                    pick_odds = odds_data.get("away_moneyline")

                imp = (
                    round(american_odds_to_implied_prob(pick_odds), 4)
                    if pick_odds is not None
                    else None
                )
                pred["prediction"] = pick
                pred["confidence"] = round(conf, 4)
                pred["probability"] = round(conf, 4)
                pred["implied_probability"] = imp
                pred["odds"] = pick_odds
                pred["reasoning"] = (
                    f"LIVE \u2014 {score_note} (P{period}, {pct_left} remaining). "
                    f"Live win probability for {pick_name} ({pick}): {conf:.1%} "
                    f"(remaining xG: {rem_home:.2f} vs {rem_away:.2f})."
                )
                pred["details"] = {
                    "home_xg": home_xg,
                    "away_xg": away_xg,
                    "remaining_home_xg": round(rem_home, 3),
                    "remaining_away_xg": round(rem_away, 3),
                    "home_win_prob": round(home_wp, 4),
                    "away_win_prob": round(away_wp, 4),
                    "regulation_tie_prob": round(reg_tie, 4),
                    "remaining_fraction": round(remaining, 4),
                    "live": True,
                }

            elif bt == "total":
                try:
                    parts = pred["prediction"].split("_")
                    direction = parts[0]
                    line_val = float(parts[1])
                except (IndexError, ValueError):
                    adjusted.append(pred)
                    continue

                current_total = home_score + away_score
                threshold = int(line_val)

                # Skip already-decided totals — e.g. over 4.5 in a 4-3 game
                # is already won and shouldn't be recommended.
                if current_total > threshold:
                    continue

                over_p = sum(
                    matrix[i][j]
                    for i in range(max_g + 1)
                    for j in range(max_g + 1)
                    if current_total + i + j > threshold
                )
                under_p = 1.0 - over_p

                # Flip to the more likely side
                if over_p >= under_p:
                    direction, conf = "over", over_p
                    side_odds = odds_data.get("over_price")
                else:
                    direction, conf = "under", under_p
                    side_odds = odds_data.get("under_price")

                imp = (
                    round(american_odds_to_implied_prob(side_odds), 4)
                    if side_odds is not None
                    else None
                )
                rem_total = round(rem_home + rem_away, 2)
                pred["prediction"] = f"{direction}_{line_val}"
                pred["confidence"] = round(conf, 4)
                pred["probability"] = round(conf, 4)
                pred["implied_probability"] = imp
                pred["odds"] = side_odds
                pred["reasoning"] = (
                    f"LIVE \u2014 Current total: {current_total} ({pct_left} remaining). "
                    f"Projected remaining goals: {rem_total}. "
                    f"Live {direction} {line_val} probability: {conf:.1%}."
                )

            elif bt == "spread":
                try:
                    pred_parts = pred["prediction"].split("_", 1)
                    team_part = pred_parts[0]
                    spread_val = float(pred_parts[1])
                except (IndexError, ValueError):
                    adjusted.append(pred)
                    continue

                cur_margin = home_score - away_score
                is_home = team_part == home_abbr

                cover_p = 0.0
                for i in range(max_g + 1):
                    for j in range(max_g + 1):
                        fm = cur_margin + i - j
                        if is_home:
                            if fm > -spread_val:
                                cover_p += matrix[i][j]
                        else:
                            if fm < spread_val:
                                cover_p += matrix[i][j]

                pred["confidence"] = round(cover_p, 4)
                pred["probability"] = round(cover_p, 4)

                # Update implied probability from the correct spread price.
                # The model always uses the standard ±1.5 puck line, but
                # live sportsbooks may move the spread (e.g., to ±3.5).
                # Only use the book price if the line still matches ±1.5;
                # otherwise null out implied_prob so this bet is excluded
                # from best-bets (can't compare ±1.5 prob to ±3.5 price).
                book_spread = odds_data.get("home_spread_line")
                line_matches = (
                    book_spread is not None
                    and abs(abs(book_spread) - abs(spread_val)) < 0.2
                )
                if line_matches:
                    sprd_price = (
                        odds_data.get("home_spread_price")
                        if is_home
                        else odds_data.get("away_spread_price")
                    )
                    if sprd_price is not None:
                        pred["implied_probability"] = round(
                            american_odds_to_implied_prob(float(sprd_price)), 4
                        )
                        pred["odds"] = float(sprd_price)
                    else:
                        pred["implied_probability"] = None
                        pred["odds"] = None
                else:
                    # Spread line moved — edge comparison is invalid
                    pred["implied_probability"] = None
                    pred["odds"] = None

                pred["reasoning"] = (
                    f"LIVE \u2014 {score_note} (P{period}, {pct_left} remaining). "
                    f"Live {pred['prediction']} cover probability: {cover_p:.1%}."
                )

            adjusted.append(pred)

        adjusted.sort(key=lambda p: p.get("confidence", 0), reverse=True)
        return adjusted

    # ------------------------------------------------------------------ #
    #  Clean reasoning builder                                            #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _build_clean_reasons(
        features: Dict[str, Any],
        pick_abbr: str,
        opponent_abbr: str,
        bet_type: str = "ml",
        details: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Build clean, concise reasoning bullets separated by semicolons.

        Generates human-readable reasons (like Buddy's Analysis) instead of
        technical model stats. Returns semicolon-separated strings that the
        frontend splits into numbered bullet points.
        """
        reasons: List[str] = []
        home_abbr = features.get("home_team_abbr", "HOM")
        away_abbr = features.get("away_team_abbr", "AWY")
        is_home = pick_abbr == home_abbr
        pick_name = features.get("home_team_name" if is_home else "away_team_name", pick_abbr)
        opp_name = features.get("away_team_name" if is_home else "home_team_name", opponent_abbr)

        # Form / recent results
        pick_form = features.get("home_form_5" if is_home else "away_form_5", {})
        opp_form = features.get("away_form_5" if is_home else "home_form_5", {})
        pick_w = pick_form.get("wins", 0)
        pick_l = pick_form.get("losses", 0)
        opp_w = opp_form.get("wins", 0)
        opp_l = opp_form.get("losses", 0)

        if pick_w >= 4:
            reasons.append(f"{pick_abbr} on a hot streak ({pick_w}-{pick_l} in last 5)")
        elif pick_w >= 3:
            reasons.append(f"{pick_abbr} strong recent form ({pick_w}-{pick_l} in last 5)")

        if opp_l >= 4:
            reasons.append(f"{opponent_abbr} struggling ({opp_w}-{opp_l} in last 5)")
        elif opp_l >= 3:
            reasons.append(f"{opponent_abbr} poor recent form ({opp_w}-{opp_l} in last 5)")

        # Schedule / fatigue
        home_sched = features.get("home_schedule", {})
        away_sched = features.get("away_schedule", {})
        opp_sched = away_sched if is_home else home_sched

        if opp_sched.get("is_back_to_back"):
            reasons.append(f"{opponent_abbr} on back-to-back (fatigue advantage for {pick_abbr})")
        pick_sched = home_sched if is_home else away_sched
        if pick_sched.get("rest_days", 0) >= 3:
            reasons.append(f"{pick_abbr} well-rested ({pick_sched['rest_days']} days off)")

        # Home/away advantage
        if is_home:
            home_season = features.get("home_season", {})
            home_w = home_season.get("home_wins", 0)
            home_l = home_season.get("home_losses", 0)
            if home_w > 0 and home_w > home_l:
                reasons.append(f"Strong home record ({home_w}-{home_l} at home)")
        else:
            away_season = features.get("away_season", {})
            away_w = away_season.get("away_wins", 0)
            away_l = away_season.get("away_losses", 0)
            if away_w > 0 and away_w > away_l:
                reasons.append(f"Strong road record ({away_w}-{away_l} away)")

        # Head-to-head
        h2h = features.get("h2h", {})
        h2h_wins = h2h.get("home_wins" if is_home else "away_wins", 0)
        h2h_total = h2h.get("total_games", 0)
        if h2h_total >= 2 and h2h_wins > h2h_total / 2:
            reasons.append(f"Favorable head-to-head record ({h2h_wins}-{h2h_total - h2h_wins} in matchup)")

        # Lineup / injuries
        opp_lineup = features.get("away_lineup" if is_home else "home_lineup", {})
        if opp_lineup.get("missing_count", 0) >= 2:
            reasons.append(f"{opponent_abbr} missing {opp_lineup['missing_count']} key players")
        elif opp_lineup.get("missing_count", 0) == 1:
            reasons.append(f"{opponent_abbr} missing a key player")

        # Possession / advanced stats
        pick_season = features.get("home_season" if is_home else "away_season", {})
        opp_season = features.get("away_season" if is_home else "home_season", {})
        pick_cf = pick_season.get("corsi_for_pct", 50.0)
        opp_cf = opp_season.get("corsi_for_pct", 50.0)
        if pick_cf and opp_cf and pick_cf > 52 and pick_cf > opp_cf + 2:
            reasons.append("Possession advantage in recent games")

        # Total-specific reasons
        if bet_type == "total" and details:
            proj = details.get("projected_total")
            if proj is not None:
                reasons.append(f"Projected goal total supports this line")

        # Ensure at least one reason
        if not reasons:
            reasons.append(f"Model favors {pick_abbr} based on overall analysis")

        return "; ".join(reasons[:5])

    # ------------------------------------------------------------------ #
    #  Predict all: master method                                         #
    # ------------------------------------------------------------------ #

    async def predict_all(
        self,
        features: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Run all prediction methods and return a unified list of predictions.

        Each prediction dict contains:
          - bet_type: category of bet (ml, total, spread)
          - prediction: the recommended side (e.g., 'home', 'over_5.5')
          - confidence: model confidence in the prediction (0-1)
          - probability: raw model probability
          - reasoning: human-readable explanation

        Returns:
            list of prediction dicts, sorted by confidence descending.
        """
        predictions: List[Dict[str, Any]] = []

        home_name = features.get("home_team_name", "Home")
        away_name = features.get("away_team_name", "Away")
        home_abbr = features.get("home_team_abbr", "HOM")
        away_abbr = features.get("away_team_abbr", "AWY")

        # Precompute xG and score matrix once for all prediction methods.
        # Previously each method recomputed these independently.
        home_xg, away_xg = self._calc_expected_goals(features)
        matrix = self._score_matrix(home_xg, away_xg)
        _pre = (home_xg, away_xg, matrix)

        # ---- Feature #13: Consensus line aggregation ----
        # When multiple sources are available, blend consensus-based
        # implied probability with the single-book line for more
        # accurate edge measurement.
        consensus = features.get("consensus_line", {})
        consensus_weight = _mc.consensus_edge_weight if (
            consensus.get("sources_count", 0) >= _mc.consensus_min_sources
        ) else 0.0

        # Extract betting odds for implied probability calculations
        odds_data = features.get("odds", {})
        home_ml = odds_data.get("home_moneyline")
        away_ml = odds_data.get("away_moneyline")
        ou_line = odds_data.get("over_under_line")
        spread_line = odds_data.get("home_spread_line")
        over_price = odds_data.get("over_price")
        under_price = odds_data.get("under_price")
        home_spread_price = odds_data.get("home_spread_price")
        away_spread_price = odds_data.get("away_spread_price")

        # Build lineup context notes for reasoning strings
        lineup_notes = []
        home_lineup = features.get("home_lineup", {})
        away_lineup = features.get("away_lineup", {})
        if home_lineup.get("missing_count", 0) > 0:
            lineup_notes.append(
                f"{home_abbr} missing {home_lineup['missing_count']} regular(s) "
                f"({home_lineup['missing_points_per_game']:.1f} PPG absent)"
            )
        if away_lineup.get("missing_count", 0) > 0:
            lineup_notes.append(
                f"{away_abbr} missing {away_lineup['missing_count']} regular(s) "
                f"({away_lineup['missing_points_per_game']:.1f} PPG absent)"
            )
        lineup_note = " | ".join(lineup_notes) if lineup_notes else ""

        # ---- Moneyline ----
        try:
            ml = await self.predict_moneyline(features, _precomputed=_pre)
            home_wp = ml["home_win_prob"]
            away_wp = ml["away_win_prob"]

            if home_wp >= away_wp:
                ml_pred = home_abbr
                ml_prob = home_wp
                ml_reason = self._build_clean_reasons(
                    features, home_abbr, away_abbr, "ml", ml
                )
            else:
                ml_pred = away_abbr
                ml_prob = away_wp
                ml_reason = self._build_clean_reasons(
                    features, away_abbr, home_abbr, "ml", ml
                )

            # Calculate implied probability and edge from real odds
            # Feature #13: blend single-book implied with consensus
            ml_implied = None
            ml_odds_display = None
            if ml_pred == home_abbr and home_ml is not None:
                single_implied = american_odds_to_implied_prob(home_ml)
                consensus_implied = consensus.get("consensus_home_implied")
                if consensus_weight > 0 and consensus_implied:
                    ml_implied = single_implied * (1 - consensus_weight) + consensus_implied * consensus_weight
                else:
                    ml_implied = single_implied
                ml_odds_display = home_ml
            elif ml_pred == away_abbr and away_ml is not None:
                single_implied = american_odds_to_implied_prob(away_ml)
                consensus_implied = consensus.get("consensus_away_implied")
                if consensus_weight > 0 and consensus_implied:
                    ml_implied = single_implied * (1 - consensus_weight) + consensus_implied * consensus_weight
                else:
                    ml_implied = single_implied
                ml_odds_display = away_ml

            # Feature #12: Apply calibration to raw model probability
            ml_calibrated = self.calibrate_probability(ml_prob)

            predictions.append({
                "bet_type": "ml",
                "prediction": ml_pred,
                "confidence": ml_calibrated,
                "probability": round(ml_prob, 4),
                "implied_probability": round(ml_implied, 4) if ml_implied else None,
                "odds": ml_odds_display,
                "reasoning": ml_reason,
                "details": ml,
            })
        except Exception as e:
            logger.error("Moneyline prediction failed: %s", e)

        # ---- Total goals (evaluate ALL available lines) ----
        try:
            totals = await self.predict_total_goals(features, _precomputed=_pre)
            lines = totals.get("lines", {})
            total_xg = totals["total_xg"]

            # Build a price map: line_val → {over_price, under_price}
            # from all available sportsbook lines
            all_total_lines = odds_data.get("all_total_lines") or []
            price_map: Dict[float, Dict[str, float]] = {}

            # First, establish the primary line as the reference point
            ou_line = odds_data.get("over_under_line")
            primary_ou_val = float(ou_line) if ou_line is not None else None
            primary_over_implied = None
            if primary_ou_val is not None:
                primary_op = float(over_price) if over_price else -110
                primary_over_implied = american_odds_to_implied_prob(primary_op)
                price_map[primary_ou_val] = {
                    "over_price": primary_op,
                    "under_price": float(under_price) if under_price else -110,
                }

            for alt in all_total_lines:
                lv = alt.get("line", 0)
                alt_op = alt.get("over_price", -110)
                alt_up = alt.get("under_price", -110)

                # Cross-validate alt line odds against the primary line.
                if primary_ou_val is not None and primary_over_implied is not None:
                    alt_over_implied = american_odds_to_implied_prob(alt_op)
                    if alt_over_implied is not None:
                        if lv < primary_ou_val and alt_over_implied <= primary_over_implied:
                            logger.debug(
                                "Rejecting alt total %.1f: over implied %.3f "
                                "not > primary %.1f over implied %.3f",
                                lv, alt_over_implied,
                                primary_ou_val, primary_over_implied,
                            )
                            continue
                        if lv > primary_ou_val and alt_over_implied >= primary_over_implied:
                            logger.debug(
                                "Rejecting alt total %.1f: over implied %.3f "
                                "not < primary %.1f over implied %.3f",
                                lv, alt_over_implied,
                                primary_ou_val, primary_over_implied,
                            )
                            continue

                price_map[lv] = {
                    "over_price": alt_op,
                    "under_price": alt_up,
                }

            # Helper: build a prediction dict for one side of a total line.
            def _make_total_pred(
                direction: str, line_val: float, prob: float,
                odds_val: float | None, implied: float | None,
            ) -> Dict[str, Any]:
                edge = round(prob - implied, 4) if implied else None
                odds_display = (
                    f" (Odds: {'+' if odds_val > 0 else ''}{int(odds_val)})"
                    if odds_val else ""
                )
                implied_str = (
                    f" vs {implied:.1%} implied (edge {edge:+.1%})"
                    if implied is not None
                    else ""
                )
                return {
                    "bet_type": "total",
                    "prediction": f"{direction}_{line_val}",
                    "confidence": self.calibrate_probability(prob),
                    "probability": round(prob, 4),
                    "implied_probability": round(implied, 4) if implied else None,
                    "odds": odds_val,
                    "edge": edge,
                    "reasoning": self._build_clean_reasons(
                        features, home_abbr, away_abbr, "total",
                        {"projected_total": total_xg, "direction": direction, "line": line_val},
                    ),
                    "details": totals,
                }

            # Emit BOTH over and under for the primary sportsbook line
            # so users can see the model's view on both sides.
            if primary_ou_val is not None:
                over_key = f"over_{primary_ou_val}"
                under_key = f"under_{primary_ou_val}"
                over_p = lines.get(over_key, 0.5)
                under_p = lines.get(under_key, 0.5)
                prices = price_map.get(primary_ou_val, {})
                op_val = prices.get("over_price")
                up_val = prices.get("under_price")
                over_implied = american_odds_to_implied_prob(op_val) if op_val else None
                under_implied = american_odds_to_implied_prob(up_val) if up_val else None

                # Put the side with more edge (or higher prob) first
                # so the top_pick selector sees it as the "best" total.
                over_edge = (over_p - over_implied) if over_implied else 0
                under_edge = (under_p - under_implied) if under_implied else 0
                if over_edge >= under_edge:
                    order = [
                        ("over", over_p, op_val, over_implied),
                        ("under", under_p, up_val, under_implied),
                    ]
                else:
                    order = [
                        ("under", under_p, up_val, under_implied),
                        ("over", over_p, op_val, over_implied),
                    ]
                for d, prob, odds_v, impl in order:
                    predictions.append(
                        _make_total_pred(d, primary_ou_val, prob, odds_v, impl)
                    )
            else:
                # No sportsbook line — use the standard line closest to
                # the model's projected total.
                best_line = min(
                    TOTAL_LINES,
                    key=lambda l: abs(l - total_xg),
                )
                over_key = f"over_{best_line}"
                under_key = f"under_{best_line}"
                over_p = lines.get(over_key, 0.5)
                under_p = lines.get(under_key, 0.5)

                if over_p >= under_p:
                    order = [("over", over_p), ("under", under_p)]
                else:
                    order = [("under", under_p), ("over", over_p)]
                for d, prob in order:
                    predictions.append(
                        _make_total_pred(d, best_line, prob, None, None)
                    )
        except Exception as e:
            logger.error("Total goals prediction failed: %s", e)

        # ---- Spread / Puck Line (evaluate ALL available lines) ----
        try:
            spread = await self.predict_spread(features, _precomputed=_pre)
            spreads = spread.get("spreads", {})
            margin = spread["predicted_margin"]

            # Determine which team is the favorite
            if home_ml is not None and away_ml is not None:
                home_is_fav = home_ml < away_ml
            else:
                home_is_fav = margin > 0

            if home_is_fav:
                fav_abbr = home_abbr
                dog_abbr = away_abbr
            else:
                fav_abbr = away_abbr
                dog_abbr = home_abbr

            # Build spread price map from all available lines.
            # Each entry uses the signed home_spread value from the
            # data to correctly map home_price/away_price to the
            # right probability keys.
            #
            # First, establish the primary spread as reference for
            # cross-validation (same approach as totals).
            all_spread_lines_data = odds_data.get("all_spread_lines") or []
            spread_price_map: Dict[float, Dict[str, Any]] = {}

            primary_spread_line = None
            primary_home_spread_imp = None
            primary_away_spread_imp = None
            if spread_line is not None:
                primary_spread_line = abs(float(spread_line))
                if primary_spread_line < 2.5:
                    _php = float(home_spread_price) if home_spread_price else -110
                    _pap = float(away_spread_price) if away_spread_price else -110
                    primary_home_spread_imp = american_odds_to_implied_prob(_php)
                    primary_away_spread_imp = american_odds_to_implied_prob(_pap)
                    spread_price_map[primary_spread_line] = {
                        "home_price": _php,
                        "away_price": _pap,
                        "home_spread": float(spread_line),
                    }

            for alt in all_spread_lines_data:
                lv = alt.get("line", 1.5)
                if lv < 1.5:
                    continue  # NHL puck lines below ±1.5 don't exist
                if lv >= 2.5:
                    continue  # Lines ±2.5+ have extreme juice (-300 etc.)
                alt_hp = alt.get("home_price", -110)
                alt_ap = alt.get("away_price", -110)

                # Cross-validate alt spread odds against the primary.
                # For spreads, the relationship depends on spread direction:
                # As the spread gets LARGER, the underdog + side becomes
                # easier to cover (more cushion) → lower odds (more negative).
                # The favorite - side becomes harder → higher odds (more positive).
                #
                # For the SAME line (e.g., both 1.5), the odds should be
                # similar to the primary.  A huge discrepancy (e.g., +480 vs
                # -165 for the same 1.5 line) means bad data.
                if primary_spread_line is not None and lv == primary_spread_line:
                    # Same line as primary — odds should be in the same
                    # ballpark.  Reject if the implied prob differs by
                    # more than 25 percentage points from the primary.
                    alt_home_imp = american_odds_to_implied_prob(alt_hp)
                    alt_away_imp = american_odds_to_implied_prob(alt_ap)
                    if (primary_home_spread_imp is not None
                            and alt_home_imp is not None
                            and abs(alt_home_imp - primary_home_spread_imp) > 0.25):
                        logger.debug(
                            "Rejecting alt spread %.1f: home implied %.3f "
                            "vs primary %.3f (diff > 25pp)",
                            lv, alt_home_imp, primary_home_spread_imp,
                        )
                        continue
                    if (primary_away_spread_imp is not None
                            and alt_away_imp is not None
                            and abs(alt_away_imp - primary_away_spread_imp) > 0.25):
                        logger.debug(
                            "Rejecting alt spread %.1f: away implied %.3f "
                            "vs primary %.3f (diff > 25pp)",
                            lv, alt_away_imp, primary_away_spread_imp,
                        )
                        continue

                if lv not in spread_price_map:
                    spread_price_map[lv] = {
                        "home_price": alt_hp,
                        "away_price": alt_ap,
                        "home_spread": alt.get("home_spread", 0),
                    }

            # Evaluate all spread lines for best edge.
            # Use home_spread sign to correctly pair prices with
            # probability keys (NOT home_is_fav which can disagree).
            best_spread_edge = -999
            best_spread_pred = None
            best_spread_prob = 0.0
            best_spread_odds = -110.0
            best_spread_implied = 0.524
            best_spread_sign = "-1.5"
            best_spread_abbr = fav_abbr

            for lv, prices in spread_price_map.items():
                h_spread = prices.get("home_spread", 0)
                h_price = prices["home_price"]
                a_price = prices["away_price"]

                # Determine which prob key pairs with which price
                # based on the actual spread direction from the data.
                if h_spread < 0:
                    # Home is favorite for this line: home -lv, away +lv
                    side_checks = [
                        (spreads.get(f"home_-{lv}", 0.0), h_price, home_abbr, f"-{lv}"),
                        (spreads.get(f"away_+{lv}", 0.0), a_price, away_abbr, f"+{lv}"),
                    ]
                elif h_spread > 0:
                    # Home is underdog for this line: home +lv, away -lv
                    side_checks = [
                        (spreads.get(f"home_+{lv}", 0.0), h_price, home_abbr, f"+{lv}"),
                        (spreads.get(f"away_-{lv}", 0.0), a_price, away_abbr, f"-{lv}"),
                    ]
                else:
                    # No spread direction info; fall back to home_is_fav
                    if home_is_fav:
                        side_checks = [
                            (spreads.get(f"home_-{lv}", 0.0), h_price, home_abbr, f"-{lv}"),
                            (spreads.get(f"away_+{lv}", 0.0), a_price, away_abbr, f"+{lv}"),
                        ]
                    else:
                        side_checks = [
                            (spreads.get(f"away_-{lv}", 0.0), a_price, away_abbr, f"-{lv}"),
                            (spreads.get(f"home_+{lv}", 0.0), h_price, home_abbr, f"+{lv}"),
                        ]

                for s_prob, s_odds, s_abbr, s_sign in side_checks:
                    if s_prob <= 0 or s_odds == 0:
                        continue
                    s_implied = american_odds_to_implied_prob(s_odds)
                    s_edge = s_prob - s_implied

                    if s_edge > best_spread_edge:
                        best_spread_edge = s_edge
                        best_spread_pred = f"{s_abbr}_{s_sign}"
                        best_spread_prob = s_prob
                        best_spread_odds = s_odds
                        best_spread_implied = s_implied
                        best_spread_sign = s_sign
                        best_spread_abbr = s_abbr

            if best_spread_pred and best_spread_edge > -999:
                predictions.append({
                    "bet_type": "spread",
                    "prediction": best_spread_pred,
                    "confidence": self.calibrate_probability(best_spread_prob),
                    "probability": round(best_spread_prob, 4),
                    "implied_probability": round(best_spread_implied, 4),
                    "odds": best_spread_odds,
                    "edge": round(best_spread_edge, 4),
                    "reasoning": self._build_clean_reasons(
                        features, best_spread_abbr,
                        away_abbr if best_spread_abbr == home_abbr else home_abbr,
                        "spread", spread,
                    ),
                    "details": spread,
                })
            else:
                # Fallback: no price data, use standard 1.5 puck line.
                # Compare by estimated edge (not raw probability) to avoid
                # always picking the underdog +1.5, which mathematically
                # covers ~75% of the time but offers poor value.
                DEFAULT_FAV_MINUS_IMPLIED = 0.35  # -1.5 typical implied (~+170)
                DEFAULT_DOG_PLUS_IMPLIED = 0.65   # +1.5 typical implied (~-185)

                if home_is_fav:
                    fav_cover_prob = spreads.get("home_-1.5", 0.0)
                    dog_cover_prob = spreads.get("away_+1.5", 0.0)
                    fav_price = home_spread_price
                    dog_price = away_spread_price
                else:
                    fav_cover_prob = spreads.get("away_-1.5", 0.0)
                    dog_cover_prob = spreads.get("home_+1.5", 0.0)
                    fav_price = away_spread_price
                    dog_price = home_spread_price

                # Use actual odds if available, otherwise NHL puck line defaults
                fav_implied = (
                    american_odds_to_implied_prob(float(fav_price))
                    if fav_price is not None else DEFAULT_FAV_MINUS_IMPLIED
                )
                dog_implied = (
                    american_odds_to_implied_prob(float(dog_price))
                    if dog_price is not None else DEFAULT_DOG_PLUS_IMPLIED
                )
                fav_edge = fav_cover_prob - fav_implied
                dog_edge = dog_cover_prob - dog_implied

                if fav_edge >= dog_edge:
                    sb_abbr = fav_abbr
                    sb_sign = "-1.5"
                    sb_prob = fav_cover_prob
                    sb_price = fav_price
                else:
                    sb_abbr = dog_abbr
                    sb_sign = "+1.5"
                    sb_prob = dog_cover_prob
                    sb_price = dog_price

                spread_odds_display = None
                spread_implied = None
                if sb_price is not None and spread_line is not None:
                    spread_odds_display = float(sb_price)
                    spread_implied = american_odds_to_implied_prob(spread_odds_display)
                elif spread_line is not None:
                    spread_odds_display = -110.0
                    spread_implied = 0.524

                predictions.append({
                    "bet_type": "spread",
                    "prediction": f"{sb_abbr}_{sb_sign}",
                    "confidence": self.calibrate_probability(sb_prob),
                    "probability": round(sb_prob, 4),
                    "implied_probability": round(spread_implied, 4) if spread_implied else None,
                    "odds": spread_odds_display,
                    "reasoning": self._build_clean_reasons(
                        features, sb_abbr,
                        away_abbr if sb_abbr == home_abbr else home_abbr,
                        "spread", spread,
                    ),
                    "details": spread,
                })
        except Exception as e:
            logger.error("Spread prediction failed: %s", e)

        # ---- Props (isolated subsystem) ----
        try:
            from app.props import PropEngine
            prop_engine = PropEngine()
            prop_preds = prop_engine.run(features, odds_data, matrix, home_xg, away_xg)
            predictions.extend(prop_preds)
        except Exception as e:
            logger.error("Prop predictions failed: %s", e)

        # Compute edge for all predictions that have implied probability
        # but no edge yet (props don't compute it inline).
        for pred in predictions:
            if pred.get("edge") is None and pred.get("implied_probability") is not None:
                pred["edge"] = round(
                    (pred.get("confidence", 0) or 0) - pred["implied_probability"],
                    4,
                )

        # Compute composite edge score for each prediction
        for pred in predictions:
            pred["composite_edge"] = self.compute_composite_edge(features, pred)

        # Sort by confidence descending
        predictions.sort(key=lambda p: p["confidence"], reverse=True)

        return predictions

    # ------------------------------------------------------------------ #
    #  Feature #12: Calibration analysis                                  #
    # ------------------------------------------------------------------ #

    @staticmethod
    async def compute_calibration_stats(db) -> Dict[str, Any]:
        """Analyze historical prediction accuracy by confidence bucket.

        Groups settled moneyline predictions into 5% confidence buckets
        and compares predicted win rate vs actual win rate. This helps
        tune the calibration shrinkage factor.

        Returns:
            dict with buckets (list of {range, predicted, actual, count}),
            brier_score, and suggested_shrinkage.
        """
        from sqlalchemy import select, func, and_
        from app.models.prediction import Prediction, BetResult

        stmt = (
            select(Prediction.confidence, BetResult.was_correct)
            .join(BetResult, BetResult.prediction_id == Prediction.id)
            .where(
                and_(
                    Prediction.phase == "prematch",
                    Prediction.bet_type == "ml",
                    BetResult.was_correct.isnot(None),
                )
            )
        )
        result = await db.execute(stmt)
        rows = result.all()

        if len(rows) < _mc.calibration_min_predictions:
            return {"buckets": [], "brier_score": None, "suggested_shrinkage": 0.12, "sample_size": len(rows)}

        # Group into 5% buckets
        buckets: Dict[int, List] = {}
        for conf, correct in rows:
            bucket = int((conf or 0.5) * 20) * 5  # 0, 5, 10, ..., 95
            bucket = max(50, min(95, bucket))  # clamp to 50-95
            if bucket not in buckets:
                buckets[bucket] = []
            buckets[bucket].append(1 if correct else 0)

        bucket_list = []
        brier_sum = 0.0
        for b in sorted(buckets.keys()):
            outcomes = buckets[b]
            predicted = b / 100.0
            actual = sum(outcomes) / len(outcomes)
            bucket_list.append({
                "range": f"{b}-{b+5}%",
                "predicted": round(predicted, 2),
                "actual": round(actual, 3),
                "count": len(outcomes),
            })
            for o in outcomes:
                brier_sum += (predicted - o) ** 2

        brier_score = brier_sum / len(rows) if rows else None

        # Suggest shrinkage: if model is overconfident at extremes,
        # shrinkage should be higher. Simple heuristic: avg absolute
        # deviation between predicted and actual.
        if bucket_list:
            total_dev = sum(
                abs(b["predicted"] - b["actual"]) * b["count"]
                for b in bucket_list
            )
            avg_dev = total_dev / len(rows)
            # Map deviation to shrinkage: 0.05 dev → 0.10 shrinkage, 0.10 dev → 0.20
            suggested = min(0.25, max(0.05, avg_dev * 2.0))
        else:
            suggested = 0.12

        return {
            "buckets": bucket_list,
            "brier_score": round(brier_score, 4) if brier_score is not None else None,
            "suggested_shrinkage": round(suggested, 3),
            "sample_size": len(rows),
        }

    # ------------------------------------------------------------------ #
    #  Composite edge score                                               #
    # ------------------------------------------------------------------ #

    def compute_composite_edge(
        self,
        features: Dict[str, Any],
        prediction: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Compute a composite edge score (0-100) aggregating all factors.

        Each component is normalized to 0-1, then weighted and summed
        to produce a single signal-strength metric.
        """
        pred_team = prediction.get("prediction", "")
        home_abbr = features.get("home_team_abbr", "")
        is_home_pick = pred_team == home_abbr

        scores: Dict[str, float] = {}

        # Form: compare L5 win rates
        home_wr = features.get("home_form_5", {}).get("win_rate", 0.5)
        away_wr = features.get("away_form_5", {}).get("win_rate", 0.5)
        if is_home_pick:
            scores["form"] = min(1.0, max(0.0, (home_wr - away_wr + 1.0) / 2.0))
        else:
            scores["form"] = min(1.0, max(0.0, (away_wr - home_wr + 1.0) / 2.0))

        # Goalie: tier advantage
        home_tier = features.get("home_goalie", {}).get("tier_rank", 2)
        away_tier = features.get("away_goalie", {}).get("tier_rank", 2)
        tier_diff = (home_tier - away_tier) if is_home_pick else (away_tier - home_tier)
        scores["goalie"] = min(1.0, max(0.0, (tier_diff + 2.0) / 4.0))

        # 5v5 Possession
        home_ev = features.get("home_ev_possession", {}).get("ev_cf_pct", 50.0)
        away_ev = features.get("away_ev_possession", {}).get("ev_cf_pct", 50.0)
        my_ev = home_ev if is_home_pick else away_ev
        scores["possession"] = min(1.0, max(0.0, (my_ev - 45.0) / 10.0))

        # Close-game possession
        home_close = features.get("home_close_possession", {}).get("close_cf_pct", 50.0)
        away_close = features.get("away_close_possession", {}).get("close_cf_pct", 50.0)
        my_close = home_close if is_home_pick else away_close
        scores["close_possession"] = min(1.0, max(0.0, (my_close - 45.0) / 10.0))

        # Special teams
        home_pp = features.get("home_special_teams", {}).get("pp_pct", 20.0)
        away_pk = features.get("away_special_teams", {}).get("pk_pct", 80.0)
        away_pp = features.get("away_special_teams", {}).get("pp_pct", 20.0)
        home_pk = features.get("home_special_teams", {}).get("pk_pct", 80.0)
        if is_home_pick:
            st_edge = (home_pp - 20.0) + (home_pk - 80.0) - (away_pp - 20.0) - (away_pk - 80.0)
        else:
            st_edge = (away_pp - 20.0) + (away_pk - 80.0) - (home_pp - 20.0) - (home_pk - 80.0)
        scores["special_teams"] = min(1.0, max(0.0, (st_edge + 10.0) / 20.0))

        # Schedule (rest advantage, B2B)
        my_sched = features.get("home_schedule" if is_home_pick else "away_schedule", {})
        opp_sched = features.get("away_schedule" if is_home_pick else "home_schedule", {})
        sched_score = 0.5
        if opp_sched.get("is_back_to_back", False) and not my_sched.get("is_back_to_back", False):
            sched_score = 0.8
        elif my_sched.get("is_back_to_back", False) and not opp_sched.get("is_back_to_back", False):
            sched_score = 0.2
        my_rest = my_sched.get("days_rest", 1)
        opp_rest = opp_sched.get("days_rest", 1)
        if my_rest > opp_rest:
            sched_score = min(1.0, sched_score + 0.1)
        scores["schedule"] = sched_score

        # Injuries (opponent's injuries help us)
        opp_injuries = features.get("away_injuries" if is_home_pick else "home_injuries", {})
        my_injuries = features.get("home_injuries" if is_home_pick else "away_injuries", {})
        opp_reduction = opp_injuries.get("xg_reduction", 0.0)
        my_reduction = my_injuries.get("xg_reduction", 0.0)
        inj_edge = opp_reduction - my_reduction
        scores["injuries"] = min(1.0, max(0.0, (inj_edge + 0.1) / 0.2 * 0.5 + 0.5))

        # H2H
        h2h = features.get("h2h", {})
        if h2h.get("games_found", 0) >= 3:
            h2h_wr = h2h.get("team1_win_rate", 0.5)
            scores["h2h"] = h2h_wr if is_home_pick else (1.0 - h2h_wr)
        else:
            scores["h2h"] = 0.5

        # Player matchup
        my_matchup = features.get(
            "home_player_matchup" if is_home_pick else "away_player_matchup", {}
        )
        boost = my_matchup.get("matchup_boost", 0.0)
        scores["matchup"] = min(1.0, max(0.0, (boost + 0.1) / 0.2))

        # Market edge
        conf = prediction.get("confidence", 0.5) or 0.5
        implied = prediction.get("implied_probability") or conf
        edge = conf - implied
        scores["market_edge"] = min(1.0, max(0.0, (edge + 0.1) / 0.2))

        # Line movement (sharp money signal)
        lm = features.get("line_movement", {})
        sharp = lm.get("sharp_signal", "neutral")
        lm_score = 0.5  # neutral default
        if sharp == "sharp_home":
            lm_score = 0.85 if is_home_pick else 0.15
        elif sharp == "sharp_away":
            lm_score = 0.15 if is_home_pick else 0.85
        else:
            # Use raw moneyline movement magnitude for a subtler signal
            home_ml_move = lm.get("home_ml_move", 0.0) or 0.0
            if abs(home_ml_move) >= 5:
                # Negative move = home becoming more favored
                if home_ml_move < 0:
                    lm_score = 0.65 if is_home_pick else 0.35
                else:
                    lm_score = 0.35 if is_home_pick else 0.65
        scores["line_movement"] = lm_score

        # Penalty discipline advantage
        my_disc = features.get(
            "home_discipline" if is_home_pick else "away_discipline", {}
        )
        opp_disc = features.get(
            "away_discipline" if is_home_pick else "home_discipline", {}
        )
        my_disc_rating = my_disc.get("discipline_rating", 0.5)
        opp_disc_rating = opp_disc.get("discipline_rating", 0.5)
        # More disciplined team has an advantage (opponent takes more penalties)
        disc_edge = (1.0 - opp_disc_rating) - (1.0 - my_disc_rating)
        scores["discipline"] = min(1.0, max(0.0, (disc_edge + 0.5) / 1.0))

        # Close-game clutch factor
        my_close = features.get(
            "home_close_record" if is_home_pick else "away_close_record", {}
        )
        opp_close = features.get(
            "away_close_record" if is_home_pick else "home_close_record", {}
        )
        my_close_wr = my_close.get("close_game_win_rate", 0.5)
        scores["clutch"] = min(1.0, max(0.0, my_close_wr))

        # Weighted sum
        weights = {
            "form": _mc.composite_weight_form,
            "goalie": _mc.composite_weight_goalie,
            "possession": _mc.composite_weight_possession,
            "close_possession": _mc.composite_weight_close_possession,
            "special_teams": _mc.composite_weight_special_teams,
            "schedule": _mc.composite_weight_schedule,
            "injuries": _mc.composite_weight_injuries,
            "h2h": _mc.composite_weight_h2h,
            "matchup": _mc.composite_weight_matchup,
            "market_edge": _mc.composite_weight_market_edge,
            "line_movement": _mc.composite_weight_line_movement,
            "discipline": 0.05,
            "clutch": 0.06,
        }

        composite = sum(scores.get(k, 0.5) * w for k, w in weights.items())
        total_weight = sum(weights.values())
        composite_score = round((composite / total_weight) * 100, 1) if total_weight > 0 else 50.0

        if composite_score >= 71:
            grade = "very_strong"
        elif composite_score >= 51:
            grade = "strong"
        elif composite_score >= 31:
            grade = "moderate"
        else:
            grade = "weak"

        return {
            "composite_score": composite_score,
            "composite_grade": grade,
            "component_scores": {k: round(v, 3) for k, v in scores.items()},
        }
