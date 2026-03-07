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
    probabilities for moneyline, totals, and spreads.
    """

    def __init__(self) -> None:
        """Initialize the betting model with default parameters."""
        self.league_avg = LEAGUE_AVG_GOALS
        self.home_ice_adj = HOME_ICE_ADVANTAGE

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

        # ---- Defensive adjustments (opponent quality) ----
        # Home team faces away goalie/defense; away team faces home goalie/defense
        home_def_factor = self._defensive_factor(
            features["home_season"]["goals_against_pg"]
        )
        away_def_factor = self._defensive_factor(
            features["away_season"]["goals_against_pg"]
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

        # ---- Goalie quality adjustment ----
        away_goalie = features.get("away_goalie", {})
        home_goalie = features.get("home_goalie", {})

        home_xg = self._apply_goalie_adjustment(home_xg, away_goalie)
        away_xg = self._apply_goalie_adjustment(away_xg, home_goalie)

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

    def _defensive_factor(self, goals_against_pg: float) -> float:
        """
        Calculate a defensive quality factor.

        A team that allows more than league average has a factor > 1.0
        (making the opponent's xG higher), and vice versa.

        The raw ratio is regressed 40% toward 1.0 to prevent compounding
        when combined with form-weighted offense (which can already be
        elevated during hot streaks).
        """
        if self.league_avg == 0:
            return 1.0
        raw = goals_against_pg / self.league_avg
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
    ) -> List[List[float]]:
        """
        Build a joint probability matrix for (home_goals, away_goals).

        Returns a (max_goals+1) x (max_goals+1) matrix where entry [i][j]
        is P(home scores i goals AND away scores j goals).
        """
        matrix = []
        for i in range(max_goals + 1):
            row = []
            for j in range(max_goals + 1):
                p = self._poisson_prob(home_xg, i) * self._poisson_prob(away_xg, j)
                row.append(p)
            matrix.append(row)
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

        Strategy: snap to the nearest .5 value.  If rounding lands on a
        whole number, nudge up to .5 — e.g., 6 → 6.5, 7 → 6.5.
        """
        if line % 1 == 0.5:
            return line  # already a .5 line
        # Snap to nearest .5
        normalized = round(line * 2) / 2
        if normalized % 1 == 0:
            normalized += 0.5
        return normalized

    async def predict_total_goals(
        self,
        features: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Predict total goals using the Poisson model.

        Calculates over/under probabilities for standard lines (4.5, 5.5, 6.5)
        plus the actual sportsbook O/U line when available.

        Returns:
            dict with home_xg, away_xg, total_xg, and probabilities for
            each over/under line.
        """
        home_xg, away_xg = self._calc_expected_goals(features)
        total_xg = home_xg + away_xg

        matrix = self._score_matrix(home_xg, away_xg)
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
        home_xg, away_xg = self._calc_expected_goals(features)
        matrix = self._score_matrix(home_xg, away_xg)
        max_g = POISSON_MAX_GOALS

        predicted_margin = round(home_xg - away_xg, 3)

        # Build set of spread lines to evaluate
        eval_spread_lines = {1.5}  # Always include standard puck line
        odds_data = features.get("odds", {})
        all_spread_lines = odds_data.get("all_spread_lines") or []
        for alt in all_spread_lines:
            eval_spread_lines.add(alt.get("line", 1.5))

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
            ml = await self.predict_moneyline(features)
            home_wp = ml["home_win_prob"]
            away_wp = ml["away_win_prob"]

            if home_wp >= away_wp:
                ml_pred = home_abbr
                ml_prob = home_wp
                odds_note = ""
                if home_ml is not None:
                    odds_str = f"+{int(home_ml)}" if home_ml > 0 else str(int(home_ml))
                    odds_note = f" Sportsbook line: {odds_str}."
                ml_reason = (
                    f"{home_name} ({home_abbr}) are favored with {home_wp:.1%} win probability "
                    f"(xG: {ml['home_xg']:.2f} vs {ml['away_xg']:.2f}).{odds_note}"
                )
                if lineup_note:
                    ml_reason += f" Lineup: {lineup_note}."
            else:
                ml_pred = away_abbr
                ml_prob = away_wp
                odds_note = ""
                if away_ml is not None:
                    odds_str = f"+{int(away_ml)}" if away_ml > 0 else str(int(away_ml))
                    odds_note = f" Sportsbook line: {odds_str}."
                ml_reason = (
                    f"{away_name} ({away_abbr}) projected to win at {away_wp:.1%} "
                    f"(xG: {ml['away_xg']:.2f} vs {ml['home_xg']:.2f}).{odds_note}"
                )
                if lineup_note:
                    ml_reason += f" Lineup: {lineup_note}."

            # Calculate implied probability and edge from real odds
            ml_implied = None
            ml_odds_display = None
            if ml_pred == home_abbr and home_ml is not None:
                ml_implied = american_odds_to_implied_prob(home_ml)
                ml_odds_display = home_ml
            elif ml_pred == away_abbr and away_ml is not None:
                ml_implied = american_odds_to_implied_prob(away_ml)
                ml_odds_display = away_ml

            predictions.append({
                "bet_type": "ml",
                "prediction": ml_pred,
                "confidence": round(ml_prob, 4),
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
            totals = await self.predict_total_goals(features)
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
                # For lines below the primary, over implied should be HIGHER
                # (easier to go over a lower number).  For lines above, it
                # should be LOWER.  A line like O 4.5 at +110 when the main
                # line is 7.5 at -110 is clearly bad data (period total, etc).
                if primary_ou_val is not None and primary_over_implied is not None:
                    alt_over_implied = american_odds_to_implied_prob(alt_op)
                    if alt_over_implied is not None:
                        if lv < primary_ou_val and alt_over_implied <= primary_over_implied:
                            # Alt line is BELOW primary but over implied is
                            # not higher — odds are inconsistent (likely
                            # period total or bad data).
                            logger.debug(
                                "Rejecting alt total %.1f: over implied %.3f "
                                "not > primary %.1f over implied %.3f",
                                lv, alt_over_implied,
                                primary_ou_val, primary_over_implied,
                            )
                            continue
                        if lv > primary_ou_val and alt_over_implied >= primary_over_implied:
                            # Alt line is ABOVE primary but over implied is
                            # not lower — odds are inconsistent.
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

            # Evaluate ALL lines and find the one with the best edge
            # Edge = model_prob - implied_prob
            best_edge = -999
            best_pred = None
            best_pred_prob = 0.0
            best_pred_odds = -110.0
            best_pred_implied = 0.5
            best_pred_line = 0.0

            for line_key, prob in lines.items():
                # line_key is like "over_5.5" or "under_6.5"
                parts = line_key.split("_", 1)
                if len(parts) != 2:
                    continue
                direction = parts[0]  # "over" or "under"
                try:
                    line_val = float(parts[1])
                except ValueError:
                    continue

                # Look up actual sportsbook price for this line+direction
                if line_val in price_map:
                    price_key = f"{direction}_price"
                    odds_val = price_map[line_val].get(price_key, -110)
                    implied = american_odds_to_implied_prob(odds_val)
                    edge = prob - implied

                    if edge > best_edge:
                        best_edge = edge
                        best_pred = line_key
                        best_pred_prob = prob
                        best_pred_odds = odds_val
                        best_pred_implied = implied
                        best_pred_line = line_val

            if best_pred and best_edge > -999:
                direction = "over" if "over" in best_pred else "under"
                predictions.append({
                    "bet_type": "total",
                    "prediction": best_pred,
                    "confidence": round(best_pred_prob, 4),
                    "probability": round(best_pred_prob, 4),
                    "implied_probability": round(best_pred_implied, 4),
                    "odds": best_pred_odds,
                    "edge": round(best_edge, 4),
                    "reasoning": (
                        f"Model projects {total_xg:.1f} total goals. "
                        f"{direction.capitalize()} {best_pred_line} at "
                        f"{best_pred_prob:.1%} model prob vs "
                        f"{best_pred_implied:.1%} implied "
                        f"(edge {best_edge:+.1%}). "
                        f"Best line across {len(price_map)} available lines. "
                        f"Based on {home_abbr} xG {totals['home_xg']:.2f} + "
                        f"{away_abbr} xG {totals['away_xg']:.2f}."
                    ),
                    "details": totals,
                })
            elif ou_line is not None:
                # Fallback: use primary sportsbook line if no price_map
                ou_val = float(ou_line)
                over_key = f"over_{ou_val}"
                under_key = f"under_{ou_val}"
                over_p = lines.get(over_key, 0.5)
                under_p = lines.get(under_key, 0.5)

                if over_p >= under_p:
                    book_pred = over_key
                    book_prob = over_p
                else:
                    book_pred = under_key
                    book_prob = under_p

                direction = "over" if "over" in book_pred else "under"
                if direction == "over" and over_price is not None:
                    total_odds_val = float(over_price)
                elif direction == "under" and under_price is not None:
                    total_odds_val = float(under_price)
                else:
                    total_odds_val = -110.0
                total_implied_val = american_odds_to_implied_prob(total_odds_val)

                predictions.append({
                    "bet_type": "total",
                    "prediction": book_pred,
                    "confidence": round(book_prob, 4),
                    "probability": round(book_prob, 4),
                    "implied_probability": round(total_implied_val, 4),
                    "odds": total_odds_val,
                    "reasoning": (
                        f"Model projects {total_xg:.1f} total goals. "
                        f"{direction.capitalize()} {ou_val} (sportsbook line) at "
                        f"{book_prob:.1%} probability. "
                        f"Based on {home_abbr} xG {totals['home_xg']:.2f} + "
                        f"{away_abbr} xG {totals['away_xg']:.2f}."
                    ),
                    "details": totals,
                })
            else:
                # No sportsbook lines at all — pick the standard line
                # closest to the model's projected total, then recommend
                # over or under on that line.  This avoids always picking
                # the lowest line (e.g., over_5.5) just because it has the
                # highest raw probability.
                best_line = min(
                    TOTAL_LINES,
                    key=lambda l: abs(l - total_xg),
                )
                over_key = f"over_{best_line}"
                under_key = f"under_{best_line}"
                over_p = lines.get(over_key, 0.5)
                under_p = lines.get(under_key, 0.5)

                if over_p >= under_p:
                    best_total_pred = over_key
                    best_total_prob = over_p
                else:
                    best_total_pred = under_key
                    best_total_prob = under_p

                direction = "over" if "over" in best_total_pred else "under"
                predictions.append({
                    "bet_type": "total",
                    "prediction": best_total_pred,
                    "confidence": round(best_total_prob, 4),
                    "probability": round(best_total_prob, 4),
                    "implied_probability": None,
                    "odds": None,
                    "reasoning": (
                        f"Model projects {total_xg:.1f} total goals. "
                        f"{direction.capitalize()} {best_line} at {best_total_prob:.1%} probability "
                        f"(no sportsbook line available, using nearest standard line). "
                        f"Based on {home_abbr} xG {totals['home_xg']:.2f} + "
                        f"{away_abbr} xG {totals['away_xg']:.2f}."
                    ),
                    "details": totals,
                })
        except Exception as e:
            logger.error("Total goals prediction failed: %s", e)

        # ---- Spread / Puck Line (evaluate ALL available lines) ----
        try:
            spread = await self.predict_spread(features)
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
                    "confidence": round(best_spread_prob, 4),
                    "probability": round(best_spread_prob, 4),
                    "implied_probability": round(best_spread_implied, 4),
                    "odds": best_spread_odds,
                    "edge": round(best_spread_edge, 4),
                    "reasoning": (
                        f"Predicted margin: {margin:+.2f} goals. "
                        f"{best_spread_abbr} {best_spread_sign} covers at "
                        f"{best_spread_prob:.1%} model prob vs "
                        f"{best_spread_implied:.1%} implied "
                        f"(edge {best_spread_edge:+.1%}). "
                        f"Best across {len(spread_price_map)} spread lines."
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
                    "confidence": round(sb_prob, 4),
                    "probability": round(sb_prob, 4),
                    "implied_probability": round(spread_implied, 4) if spread_implied else None,
                    "odds": spread_odds_display,
                    "reasoning": (
                        f"Predicted margin: {margin:+.2f} goals. "
                        f"{sb_abbr} {sb_sign} covers at {sb_prob:.1%} probability."
                    ),
                    "details": spread,
                })
        except Exception as e:
            logger.error("Spread prediction failed: %s", e)

        # Compute edge for all predictions that have implied probability
        # but no edge yet (props don't compute it inline).
        for pred in predictions:
            if pred.get("edge") is None and pred.get("implied_probability") is not None:
                pred["edge"] = round(
                    (pred.get("confidence", 0) or 0) - pred["implied_probability"],
                    4,
                )

        # Sort by confidence descending
        predictions.sort(key=lambda p: p["confidence"], reverse=True)

        return predictions
