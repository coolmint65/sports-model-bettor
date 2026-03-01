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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model constants
# ---------------------------------------------------------------------------

# NHL league-average goals per team per game (roughly 3.0-3.1 in recent seasons)
LEAGUE_AVG_GOALS = 3.05

# Home ice advantage in expected goals (historical NHL average ~0.12-0.18)
HOME_ICE_ADVANTAGE = 0.15

# Weighting for form windows when computing expected goals
# 50% last 5 games, 30% last 10 games, 20% season averages
WEIGHT_FORM_5 = 0.50
WEIGHT_FORM_10 = 0.30
WEIGHT_SEASON = 0.20

# Head-to-head adjustment factor (scales the H2H deviation)
H2H_FACTOR = 0.10

# Goalie adjustment factor (how much goalie quality affects expected goals)
GOALIE_FACTOR = 0.20

# League average save percentage and GAA for baseline comparisons
LEAGUE_AVG_SAVE_PCT = 0.905
LEAGUE_AVG_GAA = 3.05

# Common betting lines
TOTAL_LINES = [4.5, 5.5, 6.5]
# Standard NHL puck line (favorite -1.5, underdog +1.5)
PUCK_LINE = 1.5

# Maximum number of goals to sum in Poisson calculations
POISSON_MAX_GOALS = 12


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
    probabilities for moneyline, totals, spreads, period outcomes, and props.
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
            h2h_goal_adj = 0.05  # small weight
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
        if home_splits.get("games_found", 0) >= 5:
            split_off = home_splits.get("avg_goals_for", home_xg)
            home_xg = home_xg * 0.85 + split_off * 0.15
        if away_splits.get("games_found", 0) >= 5:
            split_off = away_splits.get("avg_goals_for", away_xg)
            away_xg = away_xg * 0.85 + split_off * 0.15

        # ---- Regression toward league average ----
        # Hot-streak form weights and weak-opponent defensive factors can
        # compound to produce unrealistic xG values.  Regress 20% toward
        # the league average to dampen extremes while preserving signal.
        home_xg = home_xg * 0.80 + self.league_avg * 0.20
        away_xg = away_xg * 0.80 + self.league_avg * 0.20

        # ---- Floor / ceiling ----
        # No NHL team realistically projects above ~3.8 goals per game.
        home_xg = max(1.8, min(3.8, home_xg))
        away_xg = max(1.8, min(3.8, away_xg))

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
        # Regress toward 1.0: take only 60% of the deviation
        return 1.0 + (raw - 1.0) * 0.6

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
        goalie_sv = 0.6 * last5_sv + 0.4 * season_sv

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
    def _poisson_prob_over(lam: float, k: int) -> float:
        """Probability of MORE than k goals (i.e., > k)."""
        return float(1.0 - poisson.cdf(k, lam))

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

        # Build the set of lines to evaluate, including the actual book line
        eval_lines = set(TOTAL_LINES)
        odds_data = features.get("odds", {})
        book_ou = odds_data.get("over_under_line")
        if book_ou is not None:
            normalized = self._normalize_total_line(float(book_ou))
            # Reject implausible lines — NHL O/U is always 4.5-8.5.
            # A line outside this range is almost certainly a scraping error
            # and would produce inflated confidence on a non-existent bet.
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
                # Update the odds data so downstream code uses the corrected line
                if normalized != float(book_ou):
                    logger.info(
                        "Normalized sportsbook O/U line %.1f → %.1f",
                        float(book_ou), normalized,
                    )
                    odds_data["over_under_line"] = normalized

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

        Computes four cover probabilities for the standard 1.5 puck line:
          - home_-1.5: P(home wins by 2+)
          - home_+1.5: P(home doesn't lose by 2+)
          - away_-1.5: P(away wins by 2+)
          - away_+1.5: P(away doesn't lose by 2+)

        The actual bettable puck line pairs are:
          - Favorite -1.5 vs Underdog +1.5 (complements)

        Returns:
            dict with predicted_margin, spread probabilities for each side.
        """
        home_xg, away_xg = self._calc_expected_goals(features)
        matrix = self._score_matrix(home_xg, away_xg)
        max_g = POISSON_MAX_GOALS

        predicted_margin = round(home_xg - away_xg, 3)

        # Calculate all four spread probabilities for the 1.5 puck line.
        # margin = home_score - away_score (positive = home winning)
        home_minus_15 = 0.0  # P(home wins by 2+): margin > 1.5
        away_minus_15 = 0.0  # P(away wins by 2+): margin < -1.5
        for i in range(max_g + 1):
            for j in range(max_g + 1):
                margin = i - j
                if margin > 1.5:
                    home_minus_15 += matrix[i][j]
                if margin < -1.5:
                    away_minus_15 += matrix[i][j]

        spreads = {
            "home_-1.5": round(home_minus_15, 4),
            "home_+1.5": round(1.0 - away_minus_15, 4),
            "away_-1.5": round(away_minus_15, 4),
            "away_+1.5": round(1.0 - home_minus_15, 4),
        }

        return {
            "predicted_margin": predicted_margin,
            "home_xg": home_xg,
            "away_xg": away_xg,
            "spreads": spreads,
        }

    # ------------------------------------------------------------------ #
    #  Prediction: Period outcomes                                        #
    # ------------------------------------------------------------------ #

    async def predict_period_outcomes(
        self,
        features: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Predict period-by-period outcomes using period-level scoring data.

        For each period (1st, 2nd, 3rd):
          - Expected goals per team (derived from period stats)
          - Period winner probabilities
          - Both teams to score in period probability
          - Over/under 1.5 goals in period

        Returns:
            dict keyed by period (p1, p2, p3) with sub-dicts of probabilities.
        """
        home_periods = features.get("home_periods", {})
        away_periods = features.get("away_periods", {})

        period_labels = ["p1", "p2", "p3"]
        period_fields_for = ["avg_p1_for", "avg_p2_for", "avg_p3_for"]
        period_fields_against = ["avg_p1_against", "avg_p2_against", "avg_p3_against"]

        results = {}
        for idx, label in enumerate(period_labels):
            # Home team expected goals in this period
            home_p_for = home_periods.get(period_fields_for[idx], 0.8)
            away_p_against = away_periods.get(period_fields_against[idx], 0.8)
            # Blend team's scoring with opponent's allowing
            home_p_xg = (home_p_for + away_p_against) / 2.0

            # Away team expected goals in this period
            away_p_for = away_periods.get(period_fields_for[idx], 0.8)
            home_p_against = home_periods.get(period_fields_against[idx], 0.8)
            away_p_xg = (away_p_for + home_p_against) / 2.0

            # Add small home advantage for first period
            if idx == 0:
                home_p_xg += 0.05

            # Ensure non-negative
            home_p_xg = max(0.3, home_p_xg)
            away_p_xg = max(0.3, away_p_xg)

            # Period winner using Poisson
            p_matrix = self._score_matrix(home_p_xg, away_p_xg, max_goals=6)
            home_win_p = 0.0
            away_win_p = 0.0
            draw_p = 0.0
            for i in range(7):
                for j in range(7):
                    if i > j:
                        home_win_p += p_matrix[i][j]
                    elif j > i:
                        away_win_p += p_matrix[i][j]
                    else:
                        draw_p += p_matrix[i][j]

            # Both teams to score in period
            p_home_zero = self._poisson_prob(home_p_xg, 0)
            p_away_zero = self._poisson_prob(away_p_xg, 0)
            btts_period = (1.0 - p_home_zero) * (1.0 - p_away_zero)

            # Over/under 1.5 goals in period
            total_p_xg = home_p_xg + away_p_xg
            over_15 = 1.0 - self._poisson_prob_under(total_p_xg, 1)
            under_15 = self._poisson_prob_under(total_p_xg, 1)

            results[label] = {
                "home_xg": round(home_p_xg, 3),
                "away_xg": round(away_p_xg, 3),
                "total_xg": round(total_p_xg, 3),
                "home_win_prob": round(home_win_p, 4),
                "away_win_prob": round(away_win_p, 4),
                "draw_prob": round(draw_p, 4),
                "btts_prob": round(btts_period, 4),
                "over_1_5": round(over_15, 4),
                "under_1_5": round(under_15, 4),
            }

        return results

    # ------------------------------------------------------------------ #
    #  Prediction: Props                                                  #
    # ------------------------------------------------------------------ #

    async def predict_props(
        self,
        features: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Predict various proposition bets.

        - First goal: which team scores first
        - Both teams to score (BTTS)
        - Overtime probability
        - Odd/even total goals

        Returns:
            dict with probabilities for each prop.
        """
        home_xg, away_xg = self._calc_expected_goals(features)

        # ---- First goal probability ----
        # Approximation: probability of scoring first is proportional to
        # expected goals and first-period scoring rate
        home_patterns = features.get("home_patterns", {})
        away_patterns = features.get("away_patterns", {})
        home_fg_pct = home_patterns.get("first_goal_pct", 0.5)
        away_fg_pct = away_patterns.get("first_goal_pct", 0.5)

        # Blend model-based and empirical first goal rates
        model_fg_home = home_xg / (home_xg + away_xg) if (home_xg + away_xg) > 0 else 0.5
        first_goal_home = 0.6 * model_fg_home + 0.4 * home_fg_pct
        first_goal_away = 1.0 - first_goal_home

        # ---- Both teams to score ----
        # P(BTTS) = P(home >= 1) * P(away >= 1) using Poisson
        p_home_scores = 1.0 - self._poisson_prob(home_xg, 0)
        p_away_scores = 1.0 - self._poisson_prob(away_xg, 0)
        btts_prob = p_home_scores * p_away_scores

        # ---- Overtime probability ----
        # From the score matrix: P(regulation draw) corresponds to OT games
        matrix = self._score_matrix(home_xg, away_xg)
        max_g = POISSON_MAX_GOALS
        ot_prob = 0.0
        for i in range(max_g + 1):
            ot_prob += matrix[i][i]

        # Also factor in empirical OT tendency
        home_ot = features.get("home_ot", {})
        away_ot = features.get("away_ot", {})
        emp_ot_pct = (
            home_ot.get("ot_pct", 0.23) + away_ot.get("ot_pct", 0.23)
        ) / 2.0
        # Blend model and empirical
        ot_prob_final = 0.6 * ot_prob + 0.4 * emp_ot_pct

        # ---- Odd/even total ----
        total_xg = home_xg + away_xg
        odd_prob = 0.0
        even_prob = 0.0
        for i in range(max_g + 1):
            for j in range(max_g + 1):
                total = i + j
                if total % 2 == 1:
                    odd_prob += matrix[i][j]
                else:
                    even_prob += matrix[i][j]

        return {
            "first_goal_home": round(first_goal_home, 4),
            "first_goal_away": round(first_goal_away, 4),
            "btts_prob": round(btts_prob, 4),
            "btts_no_prob": round(1.0 - btts_prob, 4),
            "overtime_prob": round(ot_prob_final, 4),
            "regulation_prob": round(1.0 - ot_prob_final, 4),
            "odd_total_prob": round(odd_prob, 4),
            "even_total_prob": round(even_prob, 4),
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

            elif bt in ("first_goal", "period_winner", "period_total"):
                # These are resolved or irrelevant mid-game
                continue
            # else: keep other props (overtime, both_score, etc.) as-is

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
          - bet_type: category of bet (ml, total, spread, period, prop)
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

        # Validate spread sign against moneyline: favorite must have
        # negative spread.  If they disagree, flip sign and prices.
        if home_ml and away_ml and spread_line:
            home_is_fav = home_ml < away_ml
            if home_is_fav and spread_line > 0:
                spread_line = -abs(spread_line)
                odds_data["home_spread_line"] = spread_line
                odds_data["away_spread_line"] = abs(spread_line)
                home_spread_price, away_spread_price = away_spread_price, home_spread_price
            elif not home_is_fav and spread_line < 0:
                spread_line = abs(spread_line)
                odds_data["home_spread_line"] = spread_line
                odds_data["away_spread_line"] = -abs(spread_line)
                home_spread_price, away_spread_price = away_spread_price, home_spread_price

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

        # ---- Total goals ----
        try:
            totals = await self.predict_total_goals(features)
            lines = totals.get("lines", {})
            total_xg = totals["total_xg"]

            # Re-read ou_line after predict_total_goals may have normalized
            # a whole-number line (e.g., 7.0 → 6.5) in odds_data.
            ou_line = odds_data.get("over_under_line")

            # When we have a sportsbook O/U line, prioritise the book line
            # so best-bet recommendations match what the user can actually bet.
            book_line_pred = None
            book_line_prob = 0.0

            if ou_line is not None:
                ou_val = float(ou_line)
                over_key = f"over_{ou_val}"
                under_key = f"under_{ou_val}"
                over_p = lines.get(over_key, 0.5)
                under_p = lines.get(under_key, 0.5)

                if over_p >= under_p:
                    book_line_pred = over_key
                    book_line_prob = over_p
                else:
                    book_line_pred = under_key
                    book_line_prob = under_p

                direction = "over" if "over" in book_line_pred else "under"
                # Use actual sportsbook price if available
                if direction == "over" and over_price is not None:
                    total_odds_val = float(over_price)
                elif direction == "under" and under_price is not None:
                    total_odds_val = float(under_price)
                else:
                    total_odds_val = -110.0
                total_implied_val = american_odds_to_implied_prob(total_odds_val)

                predictions.append({
                    "bet_type": "total",
                    "prediction": book_line_pred,
                    "confidence": round(book_line_prob, 4),
                    "probability": round(book_line_prob, 4),
                    "implied_probability": round(total_implied_val, 4),
                    "odds": total_odds_val,
                    "reasoning": (
                        f"Model projects {total_xg:.1f} total goals. "
                        f"{direction.capitalize()} {ou_val} (sportsbook line) at "
                        f"{book_line_prob:.1%} probability. "
                        f"Based on {home_abbr} xG {totals['home_xg']:.2f} + "
                        f"{away_abbr} xG {totals['away_xg']:.2f}."
                    ),
                    "details": totals,
                })

            # Only generate a model-line prediction if there is NO sportsbook
            # O/U line.  When a book line exists the prediction above already
            # targets the actual bettable line; adding a softer standard line
            # (e.g., Over 4.5 when the book is 6.5) would dominate best-bets
            # with inflated confidence on a non-existent bet.
            if book_line_pred is None:
                best_total_pred = None
                best_total_prob = 0.0
                for line_val in TOTAL_LINES:
                    over_key = f"over_{line_val}"
                    under_key = f"under_{line_val}"
                    over_p = lines.get(over_key, 0.5)
                    under_p = lines.get(under_key, 0.5)

                    if over_p > best_total_prob:
                        best_total_prob = over_p
                        best_total_pred = over_key
                    if under_p > best_total_prob:
                        best_total_prob = under_p
                        best_total_pred = under_key

                if best_total_pred:
                    direction = "over" if "over" in best_total_pred else "under"
                    line_num = best_total_pred.split("_", 1)[1]
                    predictions.append({
                        "bet_type": "total",
                        "prediction": best_total_pred,
                        "confidence": round(best_total_prob, 4),
                        "probability": round(best_total_prob, 4),
                        "implied_probability": None,
                        "odds": None,
                        "reasoning": (
                            f"Model projects {total_xg:.1f} total goals. "
                            f"{direction.capitalize()} {line_num} at {best_total_prob:.1%} probability. "
                            f"Based on {home_abbr} xG {totals['home_xg']:.2f} + "
                            f"{away_abbr} xG {totals['away_xg']:.2f}."
                        ),
                        "details": totals,
                    })
        except Exception as e:
            logger.error("Total goals prediction failed: %s", e)

        # ---- Spread / Puck Line ----
        try:
            spread = await self.predict_spread(features)
            spreads = spread.get("spreads", {})
            margin = spread["predicted_margin"]

            # Determine which team is the favorite to assign the correct
            # puck line.  In real sportsbooks:
            #   - Favorite gets -1.5 (must win by 2+)
            #   - Underdog gets +1.5 (can lose by 1 and still cover)
            #
            # We use the moneyline to determine favorite.  If moneyline
            # data is missing, fall back to the predicted margin.
            if home_ml is not None and away_ml is not None:
                home_is_fav = home_ml < away_ml
            else:
                home_is_fav = margin > 0  # positive margin = home favored

            if home_is_fav:
                # Home is favorite: bettable lines are HOME -1.5 / AWAY +1.5
                fav_abbr = home_abbr
                fav_name = home_name
                dog_abbr = away_abbr
                dog_name = away_name
                fav_cover_prob = spreads.get("home_-1.5", 0.0)
                dog_cover_prob = spreads.get("away_+1.5", 0.0)
                fav_price = home_spread_price
                dog_price = away_spread_price
            else:
                # Away is favorite: bettable lines are AWAY -1.5 / HOME +1.5
                fav_abbr = away_abbr
                fav_name = away_name
                dog_abbr = home_abbr
                dog_name = home_name
                fav_cover_prob = spreads.get("away_-1.5", 0.0)
                dog_cover_prob = spreads.get("home_+1.5", 0.0)
                fav_price = away_spread_price
                dog_price = home_spread_price

            # Pick the side with higher cover probability
            if fav_cover_prob >= dog_cover_prob:
                best_abbr = fav_abbr
                best_spread_sign = "-1.5"
                best_spread_prob = fav_cover_prob
                sprd_price = fav_price
            else:
                best_abbr = dog_abbr
                best_spread_sign = "+1.5"
                best_spread_prob = dog_cover_prob
                sprd_price = dog_price

            display_val = f"{best_abbr}_{best_spread_sign}"

            spread_reason = (
                f"Predicted margin: {margin:+.2f} goals. "
                f"{best_abbr} {best_spread_sign} covers at {best_spread_prob:.1%} probability."
            )
            # Use actual spread price if available
            spread_implied = None
            spread_odds_display = None
            if sprd_price is not None and spread_line is not None:
                spread_odds_display = float(sprd_price)
                spread_implied = american_odds_to_implied_prob(spread_odds_display)
            elif spread_line is not None:
                spread_odds_display = -110.0
                spread_implied = 0.524
            predictions.append({
                "bet_type": "spread",
                "prediction": display_val,
                "confidence": round(best_spread_prob, 4),
                "probability": round(best_spread_prob, 4),
                "implied_probability": round(spread_implied, 4) if spread_implied else None,
                "odds": spread_odds_display,
                "reasoning": spread_reason,
                "details": spread,
            })
        except Exception as e:
            logger.error("Spread prediction failed: %s", e)

        # ---- Period outcomes ----
        try:
            periods = await self.predict_period_outcomes(features)
            for period_key, period_data in periods.items():
                period_num = period_key.upper()

                # Period winner
                hw = period_data["home_win_prob"]
                aw = period_data["away_win_prob"]
                dw = period_data["draw_prob"]
                best_period_outcome = max(
                    [("home", hw), ("away", aw), ("draw", dw)],
                    key=lambda x: x[1],
                )
                if best_period_outcome[1] > 0.38:
                    po_team = home_name if best_period_outcome[0] == "home" else (away_name if best_period_outcome[0] == "away" else "Draw")
                    period_reason = (
                        f"{po_team} favored in {period_num} ({best_period_outcome[1]:.1%} confidence). "
                        f"Expected goals: {home_abbr} {period_data['home_xg']:.2f} - {away_abbr} {period_data['away_xg']:.2f}."
                    )
                    predictions.append({
                        "bet_type": "period_winner",
                        "prediction": f"{period_key}_{best_period_outcome[0]}",
                        "confidence": round(best_period_outcome[1], 4),
                        "probability": round(best_period_outcome[1], 4),
                        "implied_probability": None,
                        "odds": None,
                        "reasoning": period_reason,
                        "details": {period_key: period_data},
                    })

                # Period total (over/under 1.5)
                over_15 = period_data.get("over_1_5", 0.5)
                under_15 = period_data.get("under_1_5", 0.5)
                if max(over_15, under_15) > 0.55:
                    pt_pred = f"{period_key}_over_1.5" if over_15 > under_15 else f"{period_key}_under_1.5"
                    pt_prob = max(over_15, under_15)
                    pt_direction = "Over" if "over" in pt_pred else "Under"
                    predictions.append({
                        "bet_type": "period_total",
                        "prediction": pt_pred,
                        "confidence": round(pt_prob, 4),
                        "probability": round(pt_prob, 4),
                        "implied_probability": None,
                        "odds": None,
                        "reasoning": (
                            f"{pt_direction} 1.5 goals in {period_num} ({pt_prob:.1%} confidence). "
                            f"Expected {period_data['total_xg']:.1f} goals in this period."
                        ),
                        "details": {period_key: period_data},
                    })
        except Exception as e:
            logger.error("Period prediction failed: %s", e)

        # ---- Props ----
        try:
            props = await self.predict_props(features)

            # First goal
            fg_home = props["first_goal_home"]
            fg_away = props["first_goal_away"]
            if fg_home >= fg_away:
                fg_pred = "home"
                fg_prob = fg_home
                fg_team = home_name
            else:
                fg_pred = "away"
                fg_prob = fg_away
                fg_team = away_name

            predictions.append({
                "bet_type": "first_goal",
                "prediction": fg_pred,
                "confidence": round(fg_prob, 4),
                "probability": round(fg_prob, 4),
                "implied_probability": None,
                "odds": None,
                "reasoning": f"{fg_team} projected to score first ({fg_prob:.1%} confidence).",
                "details": props,
            })

            # Both teams to score
            btts = props["btts_prob"]
            btts_pred = "yes" if btts > 0.5 else "no"
            btts_conf = btts if btts > 0.5 else (1.0 - btts)
            if btts_pred == "yes":
                btts_reason = (
                    f"Both teams expected to score ({btts_conf:.1%} confidence). "
                    f"{home_name} has a {props['first_goal_home']:.0%} chance of scoring "
                    f"and {away_name} has a {props['first_goal_away']:.0%} chance."
                )
            else:
                btts_reason = (
                    f"Shutout likely — one team may not score ({btts_conf:.1%} confidence). "
                    f"Overtime probability is {props['overtime_prob']:.0%}."
                )
            predictions.append({
                "bet_type": "both_score",
                "prediction": btts_pred,
                "confidence": round(btts_conf, 4),
                "probability": round(btts_conf, 4),
                "implied_probability": None,
                "odds": None,
                "reasoning": btts_reason,
                "details": props,
            })

            # Overtime
            ot_prob = props["overtime_prob"]
            ot_pred = "yes" if ot_prob > 0.5 else "no"
            ot_conf = ot_prob if ot_prob > 0.5 else (1.0 - ot_prob)
            if ot_pred == "yes":
                ot_reason = (
                    f"Game likely heads to OT ({ot_conf:.1%} confidence). "
                    f"Both teams evenly matched in recent form."
                )
            else:
                ot_reason = (
                    f"Regulation finish expected ({ot_conf:.1%} confidence). "
                    f"Clear separation in team quality suggests a decisive result."
                )
            predictions.append({
                "bet_type": "overtime",
                "prediction": ot_pred,
                "confidence": round(ot_conf, 4),
                "probability": round(ot_conf, 4),
                "implied_probability": None,
                "odds": None,
                "reasoning": ot_reason,
                "details": props,
            })

            # Odd/even total
            odd_p = props["odd_total_prob"]
            even_p = props["even_total_prob"]
            oe_pred = "odd" if odd_p > even_p else "even"
            oe_prob = max(odd_p, even_p)
            predictions.append({
                "bet_type": "odd_even",
                "prediction": oe_pred,
                "confidence": round(oe_prob, 4),
                "probability": round(oe_prob, 4),
                "implied_probability": None,
                "odds": None,
                "reasoning": (
                    f"Total goals projected to be {oe_pred} ({oe_prob:.1%} confidence)."
                ),
                "details": props,
            })
        except Exception as e:
            logger.error("Props prediction failed: %s", e)

        # Sort by confidence descending
        predictions.sort(key=lambda p: p["confidence"], reverse=True)

        return predictions
