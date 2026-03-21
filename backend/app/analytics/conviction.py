"""
League-agnostic bet conviction scoring.

Computes bet_confidence — a measure of how "sure" a bet is, independent
of sport. This captures the "lock" feeling: when every signal aligns,
no red flags exist, the model's edge is clear, and sharp money agrees.

Used by both NHL BettingModel and NBA NBABettingModel so conviction
logic is consistent across all sports.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def compute_bet_conviction(
    component_scores: Dict[str, float],
    weights: Dict[str, float],
    features: Dict[str, Any],
    prediction: Dict[str, Any],
    *,
    sport: str = "nhl",
) -> float:
    """Compute betting conviction (0.0–1.0): how sure are we this is a good bet?

    Unlike win probability (capped by sport randomness), bet conviction
    measures how strong, well-supported, and unanimous our edge is.
    It reaches 0.80–0.92 when everything lines up — the "lock" feeling.

    Components:
    1. Signal strength — weighted composite of all factors (0–1)
    2. Signal agreement — bonus when many signals point the same way
    3. Signal unanimity — extra bonus when ZERO signals contradict
    4. Strong-signal convergence — bonus when 5+ signals are > 0.65
    5. Data quality — penalty for missing starters, low sample sizes
    6. Market confirmation — bonus when sharp money agrees
    7. Edge magnitude — larger edges boost conviction
    8. Separation clarity — how far the expected scores are apart
    9. Contradiction penalty — red flags that should kill conviction

    Returns a value between 0.30 and 0.92.
    """
    total_weight = sum(weights.values())

    # --- 1. Base: weighted composite score ---
    base_score = (
        sum(component_scores.get(k, 0.5) * w for k, w in weights.items())
        / total_weight
        if total_weight > 0
        else 0.5
    )

    # --- 2. Signal agreement bonus ---
    favorable = sum(1 for v in component_scores.values() if v > 0.55)
    strongly_favorable = sum(1 for v in component_scores.values() if v > 0.65)
    unfavorable = sum(1 for v in component_scores.values() if v < 0.45)
    strongly_unfavorable = sum(1 for v in component_scores.values() if v < 0.35)

    # Only count signals that have an opinion (deviate from neutral).
    # Neutral signals (0.45–0.55) are uninformative — they shouldn't
    # dilute the agreement ratio.  With 17 NHL components, ~10 are
    # often neutral, making the old denominator (total_signals=17)
    # structurally suppress agreement even when active signals agree.
    active_signals = favorable + unfavorable
    if active_signals > 0:
        agreement = (favorable - unfavorable) / active_signals
    else:
        agreement = 0.0

    agreement_bonus = max(0.0, agreement) * 0.20

    # --- 3. Signal unanimity bonus ---
    # When ZERO signals contradict the pick, that's the "lock" feeling.
    # Even one contradicting signal breaks unanimity.
    if unfavorable == 0 and favorable >= 5:
        unanimity_bonus = 0.08
    elif unfavorable == 0 and favorable >= 3:
        unanimity_bonus = 0.04
    else:
        unanimity_bonus = 0.0

    # --- 4. Strong-signal convergence bonus ---
    if strongly_favorable >= 7:
        convergence_bonus = 0.12
    elif strongly_favorable >= 5:
        convergence_bonus = 0.08
    elif strongly_favorable >= 3:
        convergence_bonus = 0.04
    else:
        convergence_bonus = 0.0

    # --- 5. Data quality factor (0.7 to 1.0) ---
    data_quality = _assess_data_quality(features, prediction, sport)

    # --- 6. Market confirmation bonus ---
    market_bonus = _assess_market_confirmation(features, prediction)

    # --- 7. Edge magnitude bonus ---
    edge = prediction.get("edge") or 0.0
    edge_bonus = min(0.15, max(0.0, edge) * 2.5)

    # --- 8. Separation clarity bonus ---
    # When the model's expected scores are far apart, the pick direction
    # is much more reliable. Close games are coin flips.
    separation_bonus = _assess_separation(features, prediction, sport)

    # --- 9. Contradiction penalty ---
    # Red flags that should tank conviction even if other signals look good.
    contradiction_penalty = _assess_contradictions(
        component_scores, features, prediction, strongly_unfavorable
    )

    # --- Combine ---
    # Data quality is now subtractive, not multiplicative.
    # Old: base_score * 0.85 = 0.52 * 0.85 = 0.44 (crushed below 0.5)
    # New: base_score - 0.15 penalty = 0.52 - penalty (proportional, capped)
    # This prevents data quality from crushing the base score when it's
    # already near the neutral point.
    data_penalty = max(0.0, (1.0 - data_quality) * 0.6)  # 0.85 quality → 0.09 penalty
    raw = (
        base_score
        - data_penalty
        + agreement_bonus
        + unanimity_bonus
        + convergence_bonus
        + market_bonus
        + edge_bonus
        + separation_bonus
        - contradiction_penalty
    )

    # Previous versions had an NHL-specific +0.04 boost to compensate
    # for structural disadvantages (neutral signal dilution, multiplicative
    # data quality penalty).  Those root causes are now fixed:
    #   - Agreement uses only active signals (not total)
    #   - Data quality is subtractive (not multiplicative)
    # No sport-specific band-aid needed.

    # Scale to 0.30–0.92 range
    return round(max(0.30, min(0.92, raw)), 3)


def _assess_data_quality(
    features: Dict[str, Any],
    prediction: Dict[str, Any],
    sport: str,
) -> float:
    """Rate data quality from 0.7 to 1.0.

    Penalizes uncertain starters, low sample sizes, missing data.
    """
    from app.config import settings

    _mc = settings.model
    data_quality = 1.0

    pred_team = prediction.get("prediction", "")
    home_abbr = features.get("home_team_abbr", "")
    is_home_pick = pred_team == home_abbr

    if sport == "nhl":
        # Starter confidence: uncertain goalies add risk.
        # The xG calculation already discounts goalie factors by
        # starter_confidence, so this penalty should be modest to
        # avoid double-counting the uncertainty.
        home_goalie = features.get("home_goalie", {})
        away_goalie = features.get("away_goalie", {})
        my_goalie = home_goalie if is_home_pick else away_goalie
        starter_conf = my_goalie.get(
            "starter_confidence", _mc.starter_confidence_medium
        )
        if starter_conf < _mc.starter_confidence_high:
            data_quality -= 0.05
        if starter_conf < _mc.starter_confidence_medium:
            data_quality -= 0.07

        # Low sample size for advanced metrics
        my_ev = features.get(
            "home_ev_possession" if is_home_pick else "away_ev_possession", {}
        )
        if my_ev.get("games_found", 0) < _mc.ev_corsi_min_games:
            data_quality -= 0.05

    # H2H sample size (all sports)
    h2h = features.get("h2h", {})
    if h2h.get("games_found", 0) < 3:
        data_quality -= 0.05

    # Form sample size (all sports)
    my_form = features.get(
        "home_form_5" if is_home_pick else "away_form_5", {}
    )
    if my_form.get("games_found", 0) < 3:
        data_quality -= 0.05

    return max(0.70, data_quality)


def _assess_market_confirmation(
    features: Dict[str, Any],
    prediction: Dict[str, Any],
) -> float:
    """Bonus when sharp money and edge confirm the pick."""
    pred_team = prediction.get("prediction", "")
    home_abbr = features.get("home_team_abbr", "")
    is_home_pick = pred_team == home_abbr

    market_bonus = 0.0
    lm = features.get("line_movement", {})
    sharp = lm.get("sharp_signal", "neutral")

    if is_home_pick and sharp == "sharp_home":
        market_bonus = 0.12
    elif not is_home_pick and sharp == "sharp_away":
        market_bonus = 0.12

    edge = prediction.get("edge") or 0.0
    if edge > 0.03:
        market_bonus += 0.06
    if edge > 0.06:
        market_bonus += 0.04

    return market_bonus


def _assess_separation(
    features: Dict[str, Any],
    prediction: Dict[str, Any],
    sport: str,
) -> float:
    """Bonus for clear separation in expected scores.

    A game where the model expects 3.2 vs 2.3 xG (or 115 vs 105 xP)
    is much more trustworthy than 2.9 vs 2.8.
    """
    bet_type = prediction.get("bet_type", "ml")
    if bet_type != "ml":
        return 0.0

    details = prediction.get("details") or {}

    if sport == "nhl":
        home_xg = details.get("home_xg", 0)
        away_xg = details.get("away_xg", 0)
        if home_xg == 0 and away_xg == 0:
            return 0.0
        xg_gap = abs(home_xg - away_xg)
        # NHL xG range is compressed (1.8–4.0, ~2.2 effective range)
        # after mean regression, so thresholds must reflect that.
        # 0.35 xG gap is meaningful, 0.60+ is very clear.
        if xg_gap >= 0.60:
            return 0.06
        elif xg_gap >= 0.35:
            return 0.03
        elif xg_gap < 0.12:
            return -0.03  # coin flip — penalize
    elif sport == "nba":
        home_xp = details.get("home_xp", 0)
        away_xp = details.get("away_xp", 0)
        if home_xp == 0 and away_xp == 0:
            return 0.0
        xp_gap = abs(home_xp - away_xp)
        # NBA: 5+ point gap is solid, 8+ is very clear
        if xp_gap >= 8:
            return 0.06
        elif xp_gap >= 5:
            return 0.03
        elif xp_gap < 2:
            return -0.03  # coin flip

    return 0.0


def _assess_contradictions(
    component_scores: Dict[str, float],
    features: Dict[str, Any],
    prediction: Dict[str, Any],
    strongly_unfavorable: int,
) -> float:
    """Penalty for red flags that should kill conviction.

    Even one major contradiction should prevent a bet from being a "lock."
    """
    penalty = 0.0

    # Multiple strong contradictions — something is off
    if strongly_unfavorable >= 3:
        penalty += 0.10
    elif strongly_unfavorable >= 2:
        penalty += 0.06
    elif strongly_unfavorable >= 1:
        penalty += 0.03

    # Sharp money actively disagrees with our pick
    pred_team = prediction.get("prediction", "")
    home_abbr = features.get("home_team_abbr", "")
    is_home_pick = pred_team == home_abbr

    lm = features.get("line_movement", {})
    sharp = lm.get("sharp_signal", "neutral")
    if (is_home_pick and sharp == "sharp_away") or (
        not is_home_pick and sharp == "sharp_home"
    ):
        penalty += 0.08  # sharp money disagrees — big red flag

    # Negative edge (we're on the wrong side of the market)
    edge = prediction.get("edge") or 0.0
    if edge < -0.02:
        penalty += 0.05

    return penalty
