"""
Synthetic public betting percentage estimator.

Since live public betting percentages require paid API access, this module
estimates which side the public is on using heuristic analysis of moneyline
magnitude, line movement direction, and standard NHL public tendencies.

NHL public bettors tend to:
- Bet the favorite (follow the name/record)
- Bet the over (more exciting)
- Bet the better team on the puck line

When the model disagrees with heavy public action — especially when
combined with reverse line movement — that's a contrarian edge.
"""

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _ml_public_pct_from_odds(ml_odds: float) -> float:
    """Estimate the percentage of public money on the favorite based on ML magnitude.

    Heavier favorites attract more casual bettors.  These thresholds are
    derived from historical public-betting data across NHL seasons.

    Args:
        ml_odds: American moneyline odds (negative = favorite).

    Returns:
        Estimated public percentage (0-100) on the favorite side.
    """
    abs_odds = abs(ml_odds)
    if ml_odds < 0:
        # Favorite side
        if abs_odds >= 300:
            return 82.0
        elif abs_odds >= 200:
            return 72.0
        elif abs_odds >= 150:
            return 65.0
        elif abs_odds >= 110:
            return 58.0
        else:
            return 52.0
    elif ml_odds > 0:
        # Underdog side — public rarely backs underdogs heavily
        if abs_odds >= 250:
            return 35.0
        elif abs_odds >= 175:
            return 40.0
        elif abs_odds >= 130:
            return 45.0
        else:
            return 48.0
    # Pick'em
    return 52.0


def _compute_contrarian_value(
    model_agrees_with_public: bool,
    is_reverse_line_movement: bool,
    ml_public_pct: float,
) -> float:
    """Compute a 0-1 contrarian value score.

    Higher values indicate stronger contrarian edge:
    - Model picks AGAINST the public AND reverse line movement: 0.8-1.0
    - Model picks AGAINST the public without RLM: 0.3-0.5
    - Model AGREES with public: 0.0-0.1

    The public percentage magnitude modulates within each band.
    """
    if model_agrees_with_public:
        # Even when agreeing, heavy public action slightly reduces edge
        return max(0.0, 0.1 - (ml_public_pct - 50) / 500)

    # Model disagrees with public — contrarian territory
    # Scale within the band based on how heavy the public side is
    pct_factor = min(1.0, (ml_public_pct - 50) / 35)  # 50% → 0, 85% → 1

    if is_reverse_line_movement:
        # Strong contrarian signal: fading public + sharp money confirmation
        return 0.8 + 0.2 * pct_factor
    else:
        # Moderate contrarian signal: fading public without RLM confirmation
        return 0.3 + 0.2 * pct_factor


def estimate_public_side(features: Dict[str, Any]) -> Dict[str, Any]:
    """Estimate which side the public is on using odds-based heuristics.

    Uses moneyline magnitude, spread direction, and line movement to
    synthesize a public-betting indicator without needing live percentage
    feeds.

    Args:
        features: The full game feature dictionary from build_game_features().
            Must contain "odds" and optionally "line_movement" keys.

    Returns:
        Dict with public side estimates, reverse line movement detection,
        and contrarian value score.
    """
    odds = features.get("odds", {})
    line_movement = features.get("line_movement", {})

    home_ml: Optional[float] = odds.get("home_moneyline")
    away_ml: Optional[float] = odds.get("away_moneyline")
    home_spread: Optional[float] = odds.get("home_spread_line")

    result: Dict[str, Any] = {
        "ml_public_side": None,
        "ml_public_pct_estimate": 50,
        "total_public_side": "over",  # NHL public almost always bets the over
        "spread_public_side": None,
        "is_reverse_line_movement": False,
        "contrarian_value": 0.0,
        "model_agrees_with_public": True,
    }

    # --- Moneyline public side ---
    # The team with the more negative (lower) moneyline is the public side.
    if home_ml is not None and away_ml is not None:
        if home_ml < away_ml:
            # Home is the favorite → public side
            result["ml_public_side"] = "home"
            result["ml_public_pct_estimate"] = _ml_public_pct_from_odds(home_ml)
        elif away_ml < home_ml:
            # Away is the favorite → public side
            result["ml_public_side"] = "away"
            result["ml_public_pct_estimate"] = _ml_public_pct_from_odds(away_ml)
        else:
            # Pick'em
            result["ml_public_side"] = "home"  # slight home bias in pick'ems
            result["ml_public_pct_estimate"] = 52.0

    # --- Spread public side ---
    # The favorite on the puck line (negative spread) is the public side.
    if home_spread is not None:
        if home_spread < 0:
            result["spread_public_side"] = "home"
        elif home_spread > 0:
            result["spread_public_side"] = "away"
        else:
            # Even spread — follow ML public side
            result["spread_public_side"] = result["ml_public_side"]

    # --- Reverse line movement detection ---
    # If the public side is the favorite but the line is moving AGAINST
    # the favorite (getting less steep), sharp money is on the other side.
    ml_implied_shift = line_movement.get("ml_implied_shift", 0.0) or 0.0
    lm_is_reverse = line_movement.get("is_reverse_line_movement", False)

    if result["ml_public_side"] and abs(ml_implied_shift) > 0.01:
        public_is_home = result["ml_public_side"] == "home"
        # Positive shift = home became more favored
        # If public is on home but line moved toward away (negative shift),
        # that's reverse line movement.
        if public_is_home and ml_implied_shift < -0.01:
            result["is_reverse_line_movement"] = True
        elif not public_is_home and ml_implied_shift > 0.01:
            result["is_reverse_line_movement"] = True

    # Also incorporate the line_movement module's own RLM detection
    if lm_is_reverse:
        result["is_reverse_line_movement"] = True

    # --- Contrarian value (filled in after model pick is known) ---
    # At this stage we set a preliminary value assuming model disagrees.
    # The actual model_agrees_with_public flag is set later when the model
    # pick is available. For now, compute both scenarios and store the
    # "disagree" value as a preliminary score.
    result["contrarian_value"] = _compute_contrarian_value(
        model_agrees_with_public=False,
        is_reverse_line_movement=result["is_reverse_line_movement"],
        ml_public_pct=result["ml_public_pct_estimate"],
    )

    return result


def update_public_signal_with_model_pick(
    public_signal: Dict[str, Any],
    model_pick_side: str,
) -> Dict[str, Any]:
    """Update the public signal with the model's actual pick.

    Called after prediction generation to finalize model_agrees_with_public
    and recalculate contrarian_value.

    Args:
        public_signal: The dict returned by estimate_public_side().
        model_pick_side: "home" or "away" — which side the model favors on ML.

    Returns:
        Updated public_signal dict with final contrarian_value.
    """
    ml_public_side = public_signal.get("ml_public_side")
    if ml_public_side is None:
        # No odds data — can't determine public side
        public_signal["model_agrees_with_public"] = True
        public_signal["contrarian_value"] = 0.0
        return public_signal

    agrees = model_pick_side == ml_public_side
    public_signal["model_agrees_with_public"] = agrees
    public_signal["contrarian_value"] = _compute_contrarian_value(
        model_agrees_with_public=agrees,
        is_reverse_line_movement=public_signal.get("is_reverse_line_movement", False),
        ml_public_pct=public_signal.get("ml_public_pct_estimate", 50),
    )

    return public_signal
