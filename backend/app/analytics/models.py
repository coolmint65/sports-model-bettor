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
from typing import Any, Dict, List, Optional, Tuple

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
    """Convert American odds to implied probability (0-1).

    Delegates to the canonical implementation in services.odds.
    Returns 0.5 for even money (odds=0).
    """
    from app.services.odds import american_to_implied
    result = american_to_implied(odds)
    return result if result is not None else 0.5


def implied_prob_to_american_odds(prob: float) -> float:
    """Convert implied probability to American odds.

    Delegates to the canonical implementation in services.odds.
    Returns 0.0 for out-of-range probabilities.
    """
    from app.services.odds import implied_to_american
    result = implied_to_american(prob)
    return float(result) if result is not None else 0.0


class BettingModel:
    """
    Statistical prediction model for NHL hockey betting.

    Uses Poisson distribution with weighted historical inputs to produce
    probabilities for moneyline, totals, and spreads. Optionally blends
    with an ML model for improved xG estimation when trained.
    """

    # Class-level rolling calibrator — shared across instances.
    # Set externally by PredictionManager once enough historical data exists.
    _rolling_calibrator = None

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
    #  xG adjustment helpers (reduce repetition in _calc_expected_goals)  #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _clamp_xg(xg: float) -> float:
        """Clamp xG within configured floor/ceiling."""
        return max(_mc.xg_floor, min(_mc.xg_ceiling, xg))

    @staticmethod
    def _get_best_cf(
        ev: Dict[str, Any],
        close: Dict[str, Any],
        advanced: Dict[str, Any],
        adv_min_games: int,
    ) -> Optional[float]:
        """Pick best available Corsi For % from 5v5 EV > close-game > all-situations."""
        if ev.get("games_found", 0) >= _mc.ev_corsi_min_games:
            return ev.get("ev_cf_pct", 50.0)
        if close.get("close_games_found", 0) >= _mc.close_game_min_games:
            return close.get("close_cf_pct", 50.0)
        if advanced.get("games_found", 0) >= adv_min_games:
            return advanced.get("corsi_for_pct", 50.0)
        return None

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
        # Save pre-goalie xG to cap total goalie influence later.
        pre_goalie_home_xg = home_xg
        pre_goalie_away_xg = away_xg

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
        for side_key, xg_attr in [("home_skaters", "home"), ("away_skaters", "away")]:
            skaters = features.get(side_key, {})
            if skaters.get("games_found", 0) >= 5:
                talent_diff = skaters.get("top6_fwd_ppg", LEAGUE_AVG_TOP6_PPG) - LEAGUE_AVG_TOP6_PPG
                if xg_attr == "home":
                    home_xg *= 1.0 + talent_diff * SKATER_TALENT_FACTOR
                else:
                    away_xg *= 1.0 + talent_diff * SKATER_TALENT_FACTOR

        # ---- Lineup depletion adjustment ----
        # Missing regular players reduce a team's expected output.
        for side_key, xg_attr in [("home_lineup", "home"), ("away_lineup", "away")]:
            strength = features.get(side_key, {}).get("lineup_strength", 1.0)
            if strength < 1.0:
                depletion = (1.0 - strength) * LINEUP_DEPLETION_FACTOR
                if xg_attr == "home":
                    home_xg *= (1.0 - depletion)
                else:
                    away_xg *= (1.0 - depletion)

        # ---- Injury impact adjustment ----
        # Uses structured injury data for more precise lineup impact.
        home_injuries = features.get("home_injuries", {})
        away_injuries = features.get("away_injuries", {})
        for inj, xg_attr in [(home_injuries, "home"), (away_injuries, "away")]:
            impact = inj.get("xg_reduction", 0.0)
            if impact > 0:
                if xg_attr == "home":
                    home_xg *= (1.0 - min(impact, _mc.injury_impact_factor))
                else:
                    away_xg *= (1.0 - min(impact, _mc.injury_impact_factor))

        # ---- Player matchup adjustment ----
        # Key players who historically perform well/poorly against this opponent.
        for side_key, xg_attr in [("home_player_matchup", "home"), ("away_player_matchup", "away")]:
            boost = features.get(side_key, {}).get("matchup_boost", 0.0)
            if boost != 0.0:
                if xg_attr == "home":
                    home_xg *= (1.0 + boost * PLAYER_MATCHUP_FACTOR)
                else:
                    away_xg *= (1.0 + boost * PLAYER_MATCHUP_FACTOR)

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
        # Back-to-back, rest days, lookahead/letdown affect performance.
        home_schedule = features.get("home_schedule", {})
        away_schedule = features.get("away_schedule", {})

        for sched, xg_attr in [(home_schedule, "home"), (away_schedule, "away")]:
            adj = 0.0
            if sched.get("is_back_to_back", False):
                adj -= BACK_TO_BACK_PENALTY
            rest_days = sched.get("days_rest", 1)
            if rest_days > 1:
                adj += min((rest_days - 1) * REST_ADVANTAGE_PER_DAY, REST_ADVANTAGE_CAP)
            if sched.get("is_lookahead", False):
                adj -= _mc.lookahead_penalty
            if sched.get("is_letdown", False):
                adj -= _mc.lookahead_penalty * 0.75
            if xg_attr == "home":
                home_xg += adj
            else:
                away_xg += adj

        # Road trip fatigue (away team only)
        away_road_games = away_schedule.get("consecutive_road_games", 0)
        if away_road_games > _mc.road_trip_fatigue_threshold:
            road_penalty = (away_road_games - _mc.road_trip_fatigue_threshold) * _mc.road_trip_fatigue_per_game
            away_xg -= min(road_penalty, 0.10)

        # Divisional games tend to be tighter / go under
        if features.get("is_divisional", False):
            home_xg -= _mc.divisional_under_adj
            away_xg -= _mc.divisional_under_adj

        # Graduated travel fatigue (replaces binary is_travel_disadvantage)
        travel = features.get("travel", {})
        fatigue_score = travel.get("fatigue_score", 0.0)
        if fatigue_score > 0:
            travel_penalty = fatigue_score * _mc.travel_fatigue_factor
            away_xg -= travel_penalty

        # ---- Time-of-day body clock adjustment ----
        # West coast teams playing early East coast afternoon games
        # historically underperform. Only penalise the away team (the
        # home team is on their normal schedule).
        time_of_day = features.get("time_of_day", {})
        body_clock = time_of_day.get("body_clock_disadvantage", 0.0)
        if body_clock > 0.3:
            away_xg -= body_clock * _mc.early_start_penalty

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

        # ---- Referee tendency adjustment ----
        # Refs who call more penalties create more PP opportunities for both
        # teams, shifting the expected total. Strict refs push totals up;
        # lenient refs push them down.
        referee = features.get("referee", {})
        ref_factor = _mc.referee_penalty_factor
        if (ref_factor > 0
                and referee.get("found", False)
                and referee.get("games_officiated", 0) >= _mc.referee_min_games):
            ref_xg_adj = referee["xg_adjustment"]
            home_xg += ref_xg_adj * ref_factor
            away_xg += ref_xg_adj * ref_factor

        # ---- Period-specific scoring rate adjustment ----
        # Teams with strong/weak period tendencies should have xG adjusted.
        league_period_avg = self.league_avg / 3.0  # ~1.02 per period

        for side_key, xg_attr in [("home_periods", "home"), ("away_periods", "away")]:
            periods = features.get(side_key, {})
            if periods.get("games_found", 0) >= 10:
                period_total = sum(
                    periods.get(f"avg_p{p}_for", league_period_avg) for p in (1, 2, 3)
                )
                period_dev = period_total - self.league_avg
                if xg_attr == "home":
                    home_xg += period_dev * _mc.period_scoring_factor
                else:
                    away_xg += period_dev * _mc.period_scoring_factor

        # ---- Possession adjustment (unified, best-available metric) ----
        # Three possession metrics exist (5v5 EV Corsi, close-game Corsi,
        # all-situations Corsi) but they're highly correlated. Applying all
        # three triple-counts the same signal. Use the single best available
        # metric: 5v5 EV > close-game > all-situations proxy.
        home_advanced = features.get("home_advanced", {})
        away_advanced = features.get("away_advanced", {})
        home_ev = features.get("home_ev_possession", {})
        away_ev = features.get("away_ev_possession", {})
        home_close = features.get("home_close_possession", {})
        away_close = features.get("away_close_possession", {})
        adv_min_games = _mc.advanced_metrics_min_games
        possession_factor = _mc.unified_possession_factor

        for cf_used, is_home in [
            (self._get_best_cf(home_ev, home_close, home_advanced, adv_min_games), True),
            (self._get_best_cf(away_ev, away_close, away_advanced, adv_min_games), False),
        ]:
            if cf_used is not None:
                cf_deviation = (cf_used - 50.0) / 100.0
                if is_home:
                    home_xg *= 1.0 + cf_deviation * possession_factor
                else:
                    away_xg *= 1.0 + cf_deviation * possession_factor

        # Shot quality (shooting%) adjustment — kept separate as it measures
        # a distinct signal (finishing ability, not possession volume).
        for adv, xg_attr in [(home_advanced, "home"), (away_advanced, "away")]:
            if adv.get("games_found", 0) >= adv_min_games:
                sh_deviation = (adv.get("shooting_pct", 8.0) - 8.0) / 100.0
                if xg_attr == "home":
                    home_xg *= 1.0 + sh_deviation * _mc.shot_quality_factor
                else:
                    away_xg *= 1.0 + sh_deviation * _mc.shot_quality_factor

        # ---- PDO regression (luck adjustment) ----
        # PDO = shooting% + save%. League average is ~1.000.
        # Teams with PDO far from 1.0 are running hot/cold and due to regress.
        # High PDO (>1.010) → xG inflated by luck → reduce.
        # Low PDO (<0.990) → xG depressed by bad luck → increase.
        pdo_factor = _mc.pdo_regression_factor
        if pdo_factor > 0:
            for form_key, xg_attr in [("home_form_10", "home"), ("away_form_10", "away")]:
                pdo = features.get(form_key, {}).get("pdo", 1.0)
                if pdo != 1.0:
                    adj = (pdo - 1.0) * pdo_factor * self.league_avg
                    if xg_attr == "home":
                        home_xg -= adj
                    else:
                        away_xg -= adj

        # ---- Goalie recent save% trend (hot/cold streaks) ----
        # A goalie whose L5 save% is significantly above/below their season
        # average is on a streak that should shift expected goals.
        goalie_trend_factor = _mc.goalie_trend_factor
        if goalie_trend_factor > 0:
            # Each goalie's trend affects the OPPOSING team's xG
            for goalie_key, opp_attr in [("home_goalie", "away"), ("away_goalie", "home")]:
                g = features.get(goalie_key, {})
                l5_sv = g.get("last5_save_pct", 0.0)
                season_sv = g.get("season_save_pct", 0.0)
                if l5_sv > 0 and season_sv > 0:
                    sv_trend = l5_sv - season_sv  # positive = hot streak
                    mult = 1.0 - sv_trend * goalie_trend_factor * 10.0
                    if opp_attr == "away":
                        away_xg *= mult
                    else:
                        home_xg *= mult

        # ---- Goalie vs. specific opponent & venue splits adjustments ----
        # Both follow the same pattern: compare a goalie's save% in a specific
        # context to their season average, then adjust the OPPOSING team's xG.
        _goalie_sv_adjustments = []

        goalie_vs_factor = _mc.goalie_vs_team_factor
        if goalie_vs_factor > 0:
            for gvt_key, goalie_key, opp_attr in [
                ("home_goalie_vs_team", "home_goalie", "away"),
                ("away_goalie_vs_team", "away_goalie", "home"),
            ]:
                gvt = features.get(gvt_key, {})
                if gvt.get("significant", False):
                    sv_diff = gvt["vs_save_pct"] - features.get(goalie_key, {}).get("season_save_pct", 0.900)
                    _goalie_sv_adjustments.append((opp_attr, sv_diff, goalie_vs_factor))

        venue_factor = _mc.goalie_venue_factor
        if venue_factor > 0:
            for venue_key, goalie_key, opp_attr in [
                ("home_goalie_venue", "home_goalie", "away"),
                ("away_goalie_venue", "away_goalie", "home"),
            ]:
                venue = features.get(venue_key, {})
                if venue.get("significant", False):
                    sv_diff = venue["venue_save_pct"] - features.get(goalie_key, {}).get("season_save_pct", 0.900)
                    _goalie_sv_adjustments.append((opp_attr, sv_diff, venue_factor))

        for opp_attr, sv_diff, factor in _goalie_sv_adjustments:
            mult = 1.0 - sv_diff * factor * 10.0
            if opp_attr == "away":
                away_xg *= mult
            else:
                home_xg *= mult

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

        # ---- Cap total goalie influence ----
        # Six goalie factors can compound to dominate the entire prediction.
        # Cap the total xG delta from all goalie adjustments to prevent this.
        goalie_cap = _mc.goalie_max_xg_delta
        home_goalie_delta = home_xg - pre_goalie_home_xg
        away_goalie_delta = away_xg - pre_goalie_away_xg
        if abs(home_goalie_delta) > goalie_cap:
            home_xg = pre_goalie_home_xg + (goalie_cap if home_goalie_delta > 0 else -goalie_cap)
        if abs(away_goalie_delta) > goalie_cap:
            away_xg = pre_goalie_away_xg + (goalie_cap if away_goalie_delta > 0 else -goalie_cap)

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
        # Teams that perform well in tight games sustain output better
        # than teams padding stats in blowouts.
        sc_factor = _mc.score_close_factor
        if sc_factor > 0:
            for side_key, xg_attr in [("home_score_close", "home"), ("away_score_close", "away")]:
                sc = features.get(side_key, {})
                if sc.get("close_games_found", 0) >= _mc.score_close_min_games:
                    close_off = sc.get("close_gf_pg", home_xg if xg_attr == "home" else away_xg)
                    if xg_attr == "home":
                        home_xg = home_xg * (1.0 - sc_factor) + close_off * sc_factor
                    else:
                        away_xg = away_xg * (1.0 - sc_factor) + close_off * sc_factor

        # ---- Penalty discipline adjustment ----
        # Undisciplined teams give opponents more power-play chances.
        home_disc = features.get("home_discipline", {})
        away_disc = features.get("away_discipline", {})
        disc_factor = _mc.penalty_discipline_factor
        if disc_factor > 0 and home_disc.get("games_found", 0) >= 5 and away_disc.get("games_found", 0) >= 5:
            home_disc_rating = home_disc.get("discipline_rating", 0.5)
            away_disc_rating = away_disc.get("discipline_rating", 0.5)
            away_xg += (0.5 - home_disc_rating) * disc_factor
            home_xg += (0.5 - away_disc_rating) * disc_factor

        # ---- Close-game record (clutch factor) & Scoring-first tendency ----
        home_close_rec = features.get("home_close_record", {})
        away_close_rec = features.get("away_close_record", {})

        close_factor = _mc.close_game_record_factor
        if close_factor > 0:
            min_close = _mc.close_game_record_min_games
            for rec, xg_attr in [(home_close_rec, "home"), (away_close_rec, "away")]:
                if rec.get("close_games_found", 0) >= min_close:
                    close_dev = rec["close_game_win_rate"] - 0.5
                    if xg_attr == "home":
                        home_xg += close_dev * close_factor
                    else:
                        away_xg += close_dev * close_factor

        scoring_first_factor = _mc.scoring_first_factor
        if scoring_first_factor > 0:
            for rec, xg_attr in [(home_close_rec, "home"), (away_close_rec, "away")]:
                if rec.get("close_games_found", 0) >= 5:
                    sf_rate = rec.get("scoring_first_rate", 0.5)
                    adj = (sf_rate - 0.35) * scoring_first_factor
                    if xg_attr == "home":
                        home_xg += adj
                    else:
                        away_xg += adj

        # ---- PP opportunity rate vs opponent ----
        pp_factor = _mc.pp_opportunity_factor
        if pp_factor > 0:
            home_pp_opp = features.get("home_pp_opportunity", {})
            away_pp_opp = features.get("away_pp_opportunity", {})
            if (home_pp_opp.get("games_found", 0) >= _mc.pp_opportunity_min_games and
                    away_pp_opp.get("games_found", 0) >= _mc.pp_opportunity_min_games):
                home_xg += home_pp_opp.get("net_pp_impact", 0.0) * pp_factor
                away_xg += away_pp_opp.get("net_pp_impact", 0.0) * pp_factor

        # ---- Shot quality against (GSAE) ----
        # Positive GSAE = stopping more than expected → reduce opponent xG.
        sq_factor = _mc.shot_quality_against_factor
        if sq_factor > 0:
            for side_key, opp_attr in [("home_shot_quality", "away"), ("away_shot_quality", "home")]:
                sq = features.get(side_key, {})
                if sq.get("games_found", 0) >= _mc.shot_quality_min_games:
                    gsae = sq.get("goals_saved_above_expected", 0.0)
                    adj = gsae / max(sq["games_found"], 1) * sq_factor
                    if opp_attr == "away":
                        away_xg -= adj
                    else:
                        home_xg -= adj

        # ---- Line combination stability ----
        # Penalize teams with low top-6 stability (reduced chemistry).
        ls_factor = _mc.line_stability_factor
        if ls_factor > 0:
            ls_threshold = _mc.line_stability_threshold
            for side_key, xg_attr in [("home_line_stability", "home"), ("away_line_stability", "away")]:
                ls = features.get(side_key, {})
                if ls.get("games_found", 0) >= 5:
                    stability = ls.get("top6_stability", 1.0)
                    if stability < ls_threshold:
                        mult = 1.0 - (ls_threshold - stability) * ls_factor
                        if xg_attr == "home":
                            home_xg *= mult
                        else:
                            away_xg *= mult

        # ---- Recency-weighted H2H ----
        h2h_recency_factor = _mc.h2h_recency_factor
        if h2h_recency_factor > 0:
            h2h_w = features.get("h2h_weighted", {})
            if h2h_w.get("games_found", 0) >= 3:
                recency_shift = h2h_w.get("recency_shift", 0.0)
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
        home_xg = self._clamp_xg(home_xg)
        away_xg = self._clamp_xg(away_xg)

        # ---- ML model blend ----
        if self.ml_model and self.ml_model.is_trained:
            blend = _mc.ml_blend_weight
            if blend > 0:
                try:
                    ml_home, ml_away = self.ml_model.predict_xg(features)
                    home_xg = home_xg * (1.0 - blend) + ml_home * blend
                    away_xg = away_xg * (1.0 - blend) + ml_away * blend
                    home_xg = self._clamp_xg(home_xg)
                    away_xg = self._clamp_xg(away_xg)
                except Exception as e:
                    logger.warning("ML model prediction failed, using Poisson only: %s", e)

        # ---- Market-informed xG prior ----
        # Sportsbook lines encode sharp information from millions in handle.
        # Convert ML odds → implied win probability → implied xG differential,
        # then blend with model xG. This anchors predictions to market consensus
        # and is the single biggest variance reducer.
        if _mc.market_prior_enabled and _mc.market_prior_weight > 0:
            odds_data = features.get("odds", {})
            home_ml_odds = odds_data.get("home_moneyline")
            away_ml_odds = odds_data.get("away_moneyline")
            if home_ml_odds is not None and away_ml_odds is not None:
                home_implied = american_odds_to_implied_prob(home_ml_odds)
                away_implied = american_odds_to_implied_prob(away_ml_odds)
                # Remove vig: normalize implied probabilities to sum to 1.0
                total_implied = home_implied + away_implied
                if total_implied > 0:
                    home_implied /= total_implied
                    away_implied /= total_implied
                    # Convert win probability differential to xG differential.
                    # A team with 60% implied probability has roughly +0.3 xG edge.
                    # Use logit transform for a better mapping than linear:
                    # logit(p) maps 0.5→0, 0.6→0.405, 0.7→0.847
                    # Scale factor calibrated so 60% implied ≈ +0.30 xG edge.
                    model_total = home_xg + away_xg
                    if home_implied > 0.01 and home_implied < 0.99:
                        logit_home = math.log(home_implied / (1.0 - home_implied))
                        # Scale: logit(0.6)=0.405 → want ~0.30 xG diff → scale ≈ 0.74
                        xg_diff_implied = logit_home * 0.74
                        market_home_xg = model_total / 2.0 + xg_diff_implied / 2.0
                        market_away_xg = model_total / 2.0 - xg_diff_implied / 2.0
                        market_home_xg = self._clamp_xg(model_total / 2.0 + xg_diff_implied / 2.0)
                        market_away_xg = self._clamp_xg(model_total / 2.0 - xg_diff_implied / 2.0)
                        mw = _mc.market_prior_weight
                        home_xg = self._clamp_xg(home_xg * (1.0 - mw) + market_home_xg * mw)
                        away_xg = self._clamp_xg(away_xg * (1.0 - mw) + market_away_xg * mw)

        # ---- Line movement xG adjustment ----
        # Sharp money moves lines against public action. When the implied
        # probability shift exceeds the minimum threshold, nudge xG in the
        # direction the line is moving. This captures information from
        # professional bettors that the model might not otherwise see.
        line_mv = features.get("line_movement", {})
        ml_implied_shift = line_mv.get("ml_implied_shift", 0.0) or 0.0
        if abs(ml_implied_shift) > _mc.line_movement_min_shift:
            lm_factor = _mc.line_movement_factor
            league_avg = _mc.league_avg_goals
            # Positive shift = home team became more favored → boost home xG
            raw_adj = ml_implied_shift * lm_factor * league_avg
            # Cap the adjustment at ±0.15 xG to prevent overreaction
            capped_adj = max(-0.15, min(0.15, raw_adj))
            home_xg = self._clamp_xg(home_xg + capped_adj)
            away_xg = self._clamp_xg(away_xg - capped_adj)

        # ---- Contrarian / public betting xG adjustment ----
        # When the model disagrees with heavy public action (contrarian_value
        # exceeds the threshold), boost the model's preferred side slightly.
        # This is a confidence signal, not an xG override, so the max
        # adjustment is capped at ±contrarian_factor (default 0.08 xG).
        pub = features.get("public_signal", {})
        contrarian_val = pub.get("contrarian_value", 0.0) or 0.0
        if contrarian_val > _mc.contrarian_min_value and not pub.get(
            "model_agrees_with_public", True
        ):
            # Model's preferred side is opposite to public side.
            # Boost the side the model prefers (the non-public side).
            public_side = pub.get("ml_public_side")
            adj = contrarian_val * _mc.contrarian_factor
            # Cap at contrarian_factor to keep adjustment small
            adj = min(adj, _mc.contrarian_factor)
            if public_side == "home":
                # Public is on home → model prefers away → boost away xG
                away_xg += adj
                home_xg -= adj
            elif public_side == "away":
                # Public is on away → model prefers home → boost home xG
                home_xg += adj
                away_xg -= adj
            home_xg = self._clamp_xg(home_xg)
            away_xg = self._clamp_xg(away_xg)

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
    def calibrate_probability(raw_prob: float, bet_type: str = "ml") -> float:
        """Apply calibration to model probabilities.

        If a rolling calibrator is fitted (enough historical data), uses
        empirically-derived bin interpolation.  Otherwise falls back to
        static shrinkage toward 50%.

        Feature #12: Poisson models are structurally overconfident because
        hockey has massive randomness (puck bounces, posts, empty nets).

        Static shrinkage (fallback) uses different rates by bet type:
        - ML (moneyline): 0.10 — light shrinkage, preserves model signal
        - Spread/total: 0.22 — moderate shrinkage because Poisson structurally
          overestimates margin distributions (empty-net goals, OT, score effects
          aren't properly modeled)

        Controlled by calibration_shrinkage / calibration_spread_shrinkage.
        """
        if not _mc.calibration_enabled:
            return raw_prob

        # Use rolling calibrator if fitted
        if (
            BettingModel._rolling_calibrator is not None
            and BettingModel._rolling_calibrator.is_fitted
        ):
            return BettingModel._rolling_calibrator.calibrate(raw_prob, bet_type)

        # Static fallback: shrinkage toward 50%
        if bet_type in ("spread", "total"):
            shrinkage = _mc.calibration_spread_shrinkage
        else:
            shrinkage = _mc.calibration_shrinkage
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

        # OT scoring adjustment.
        # The score matrix models regulation only. When a game ties in
        # regulation (~23% of NHL games), OT/SO adds exactly 1 goal to
        # the total. We split the tie probability mass: for each tied
        # score (i, i), the total becomes 2i+1 instead of 2i.
        p_reg_tie = sum(matrix[i][i] for i in range(max_g + 1))

        lines = {}
        for line in sorted(eval_lines):
            over_prob = 0.0
            under_prob = 0.0
            threshold = int(line)
            for i in range(max_g + 1):
                for j in range(max_g + 1):
                    p = matrix[i][j]
                    if i == j:
                        # Regulation tie: game goes to OT/SO, adding 1 goal.
                        # Regulation total = 2i, final total = 2i + 1.
                        ot_total = i + j + 1
                        if ot_total > threshold:
                            over_prob += p
                        else:
                            under_prob += p
                    else:
                        total = i + j
                        if total > threshold:
                            over_prob += p
                        else:
                            under_prob += p

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

        # OT/SO-aware spread adjustment.
        # OT/SO games always end with a 1-goal margin, so:
        # - A team at -1.5 can NEVER cover in OT (must win in regulation by 2+)
        # - A team at +1.5 ALWAYS covers in OT (loser loses by exactly 1)
        # We compute P(regulation tie) and redistribute it accordingly.
        p_reg_tie = sum(matrix[i][i] for i in range(max_g + 1))

        # Calculate spread probabilities for each line
        spreads = {}
        for spread_val in sorted(eval_spread_lines):
            home_minus = 0.0  # P(home wins by spread_val+): margin > spread_val
            away_minus = 0.0  # P(away wins by spread_val+): margin < -spread_val
            for i in range(max_g + 1):
                for j in range(max_g + 1):
                    if i == j:
                        continue  # handle ties separately via OT logic
                    m = i - j
                    if m > spread_val:
                        home_minus += matrix[i][j]
                    if m < -spread_val:
                        away_minus += matrix[i][j]

            # For half-goal spreads (e.g. 1.5), OT games (1-goal margin)
            # never cover the minus side but always cover the plus side.
            if spread_val >= 1.5:
                # OT winner wins by exactly 1 → doesn't cover -1.5
                # OT loser loses by exactly 1 → covers +1.5
                # No adjustment to home_minus/away_minus needed (ties excluded above)
                pass
            else:
                # For spread_val < 1.5 (e.g. 0.5 alternate puck line),
                # OT winners DO cover, so add their share of the tie prob.
                home_ot = features.get("home_ot", {})
                away_ot = features.get("away_ot", {})
                home_ot_wr = home_ot.get("ot_win_rate", 0.52)
                away_ot_wr = away_ot.get("ot_win_rate", 0.48)
                ot_total = home_ot_wr + away_ot_wr
                home_ot_share = home_ot_wr / ot_total if ot_total > 0 else 0.52
                home_minus += p_reg_tie * home_ot_share
                away_minus += p_reg_tie * (1.0 - home_ot_share)

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

            # ---- Regulation Winner ML: substitute when juice is too steep ----
            # If the 2-way ML is a heavy favorite (steeper than -200), offer
            # the regulation time winner instead — better payout since you
            # absorb the OT/SO risk.  Only recommend if the model strongly
            # believes the team wins in regulation (not just overall).
            _REG_JUICE_THRESHOLD = -170.0
            if (
                ml_odds_display is not None
                and ml_odds_display < _REG_JUICE_THRESHOLD  # e.g., -250
            ):
                # Compute raw regulation win probability from matrix
                max_g = len(matrix)
                if ml_pred == home_abbr:
                    reg_win_prob = sum(
                        matrix[i][j] for i in range(max_g) for j in range(max_g) if i > j
                    )
                    reg_price = odds_data.get("reg_home_price")
                else:
                    reg_win_prob = sum(
                        matrix[i][j] for i in range(max_g) for j in range(max_g) if j > i
                    )
                    reg_price = odds_data.get("reg_away_price")

                if reg_price is not None and reg_win_prob > 0:
                    reg_implied = american_odds_to_implied_prob(reg_price)
                    reg_calibrated = self.calibrate_probability(reg_win_prob)
                    reg_edge = reg_calibrated - reg_implied if reg_implied else None

                    if (
                        reg_edge is not None
                        and reg_edge >= settings.min_edge
                        and reg_calibrated >= settings.min_confidence
                    ):
                        draw_pct = ml["draw_prob_regulation"]
                        predictions.append({
                            "bet_type": "ml",
                            "prediction": f"{ml_pred} (REG)",
                            "confidence": reg_calibrated,
                            "probability": round(reg_win_prob, 4),
                            "implied_probability": round(reg_implied, 4),
                            "odds": reg_price,
                            "reasoning": (
                                f"2-way ML too steep ({ml_odds_display:+.0f}). "
                                f"Regulation winner at {reg_price:+.0f} offers better value. "
                                f"Model: {reg_win_prob:.1%} reg win, {draw_pct:.1%} OT risk."
                            ),
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
                calibrated = self.calibrate_probability(prob, "total")
                edge = round(calibrated - implied, 4) if implied else None
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
                    "confidence": self.calibrate_probability(prob, "total"),
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
                # Use calibrated probabilities for edge comparison.
                over_edge = (self.calibrate_probability(over_p, "total") - over_implied) if over_implied else 0
                under_edge = (self.calibrate_probability(under_p, "total") - under_implied) if under_implied else 0
                if over_edge > under_edge:
                    order = [
                        ("over", over_p, op_val, over_implied),
                        ("under", under_p, up_val, under_implied),
                    ]
                elif under_edge > over_edge:
                    order = [
                        ("under", under_p, up_val, under_implied),
                        ("over", over_p, op_val, over_implied),
                    ]
                else:
                    # Tied edges: prefer side with better juice (lower implied)
                    o_imp = over_implied or 0.5
                    u_imp = under_implied or 0.5
                    if u_imp <= o_imp:
                        order = [
                            ("under", under_p, up_val, under_implied),
                            ("over", over_p, op_val, over_implied),
                        ]
                    else:
                        order = [
                            ("over", over_p, op_val, over_implied),
                            ("under", under_p, up_val, under_implied),
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
                    # Use calibrated probability for edge — raw Poisson
                    # structurally overestimates spread cover rates.
                    s_calibrated = self.calibrate_probability(s_prob, "spread")
                    s_edge = s_calibrated - s_implied

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
                    "confidence": self.calibrate_probability(best_spread_prob, "spread"),
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
                # Use calibrated probabilities for fallback edge comparison
                fav_cal = self.calibrate_probability(fav_cover_prob, "spread")
                dog_cal = self.calibrate_probability(dog_cover_prob, "spread")
                fav_edge = fav_cal - fav_implied
                dog_edge = dog_cal - dog_implied

                if fav_edge >= dog_edge:
                    sb_abbr = fav_abbr
                    sb_sign = "-1.5"
                    sb_prob = fav_cover_prob
                    sb_price = fav_price
                    sb_implied = fav_implied
                else:
                    sb_abbr = dog_abbr
                    sb_sign = "+1.5"
                    sb_prob = dog_cover_prob
                    sb_price = dog_price
                    sb_implied = dog_implied

                # Generate spread prediction whether or not we have real
                # sportsbook prices. Without prices, use default NHL puck
                # line implied probabilities (already computed above).
                if sb_price is not None:
                    spread_odds_display = float(sb_price)
                    spread_implied = american_odds_to_implied_prob(spread_odds_display)
                else:
                    spread_odds_display = None
                    spread_implied = sb_implied

                predictions.append({
                    "bet_type": "spread",
                    "prediction": f"{sb_abbr}_{sb_sign}",
                    "confidence": self.calibrate_probability(sb_prob, "spread"),
                    "probability": round(sb_prob, 4),
                    "implied_probability": round(spread_implied, 4),
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
            # Apply calibration to prop predictions — props use raw model
            # probabilities without shrinkage, producing inflated edges.
            # Props are structurally similar to totals (over/under on stats).
            for pp in prop_preds:
                raw_conf = pp.get("confidence", 0.5)
                pp["confidence"] = self.calibrate_probability(raw_conf, "total")
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

        # Compute composite edge score and bet confidence for each prediction
        for pred in predictions:
            composite = self.compute_composite_edge(features, pred)
            pred["composite_edge"] = composite
            pred["bet_confidence"] = composite.get("bet_confidence", 0.5)

        # Sort by bet confidence descending (how good of a bet is this?)
        predictions.sort(key=lambda p: p.get("bet_confidence", 0), reverse=True)

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
        from sqlalchemy import select, func
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
        my_close_rec = features.get(
            "home_close_record" if is_home_pick else "away_close_record", {}
        )
        my_close_wr = my_close_rec.get("close_game_win_rate", 0.5)
        scores["clutch"] = min(1.0, max(0.0, my_close_wr))

        # --- NEW FACTORS (tapping already-fetched data) ---

        # Goalie recent form: compare last-5 save% vs season average.
        # A hot goalie (.930+ L5) vs cold (.890 L5) is a massive edge.
        my_goalie_data = features.get("home_goalie" if is_home_pick else "away_goalie", {})
        opp_goalie_data = features.get("away_goalie" if is_home_pick else "home_goalie", {})
        my_l5_sv = my_goalie_data.get("last5_save_pct", 0.0) or 0.0
        my_season_sv = my_goalie_data.get("season_save_pct", 0.0) or 0.0
        opp_l5_sv = opp_goalie_data.get("last5_save_pct", 0.0) or 0.0
        opp_season_sv = opp_goalie_data.get("season_save_pct", 0.0) or 0.0
        # Compare relative form: my goalie trending up, opp trending down
        my_form_delta = my_l5_sv - my_season_sv     # positive = running hot
        opp_form_delta = opp_l5_sv - opp_season_sv  # positive = they're hot too
        goalie_form_edge = my_form_delta - opp_form_delta
        # Also factor in absolute L5 quality differential
        l5_diff = my_l5_sv - opp_l5_sv  # e.g. .930 - .900 = +0.030
        # Combine: form trend (scaled) + absolute recent quality
        goalie_form_raw = goalie_form_edge * 10.0 + l5_diff * 15.0  # normalize to ~-1..+1
        scores["goalie_form"] = min(1.0, max(0.0, (goalie_form_raw + 1.0) / 2.0))

        # Home ice advantage: teams with strong home records vs away teams
        my_splits = features.get("home_splits" if is_home_pick else "away_splits", {})
        opp_splits = features.get("away_splits" if is_home_pick else "home_splits", {})
        my_venue_wr = my_splits.get("win_rate", 0.5)
        opp_venue_wr = opp_splits.get("win_rate", 0.5)
        venue_diff = my_venue_wr - opp_venue_wr
        scores["home_ice"] = min(1.0, max(0.0, (venue_diff + 0.3) / 0.6))

        # Pace mismatch: high-pace teams score more but also allow more.
        # Our pick benefits when we're the faster team (more chances to capitalize).
        my_pace = features.get("home_pace" if is_home_pick else "away_pace", {})
        opp_pace = features.get("away_pace" if is_home_pick else "home_pace", {})
        my_shots = my_pace.get("shots_per_game", 30.0) or 30.0
        opp_shots_allowed = opp_pace.get("shots_allowed_per_game", 30.0) or 30.0
        # High shot generation vs opponent that allows a lot = good matchup
        pace_edge = (my_shots - 30.0) / 5.0 + (opp_shots_allowed - 30.0) / 5.0
        scores["pace"] = min(1.0, max(0.0, (pace_edge + 1.0) / 2.0))

        # Lineup strength: team with more complete lineup has an edge
        my_lineup = features.get("home_lineup" if is_home_pick else "away_lineup", {})
        opp_lineup = features.get("away_lineup" if is_home_pick else "home_lineup", {})
        my_strength = my_lineup.get("lineup_strength", 1.0)
        opp_strength = opp_lineup.get("lineup_strength", 1.0)
        strength_diff = my_strength - opp_strength
        scores["lineup_strength"] = min(1.0, max(0.0, (strength_diff + 0.3) / 0.6))

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
            "discipline": 0.04,
            "clutch": 0.05,
            "goalie_form": _mc.composite_weight_goalie_form,
            "home_ice": _mc.composite_weight_home_ice,
            "pace": _mc.composite_weight_pace,
            "lineup_strength": _mc.composite_weight_lineup_strength,
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
            "bet_confidence": self._compute_bet_confidence(
                scores, weights, total_weight, features, prediction
            ),
        }

    def _compute_bet_confidence(
        self,
        component_scores: Dict[str, float],
        weights: Dict[str, float],
        total_weight: float,
        features: Dict[str, Any],
        prediction: Dict[str, Any],
    ) -> float:
        """Delegate to shared conviction system. See conviction.py."""
        from app.analytics.conviction import compute_bet_conviction

        return compute_bet_conviction(
            component_scores, weights, features, prediction, sport="nhl"
        )
