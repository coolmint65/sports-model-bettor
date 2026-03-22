"""
Correlation-aware parlay analysis.

Identifies correlated parlay legs (e.g., team ML + game over) that
should be priced differently than independent legs. Standard parlay
math assumes independence — correlated legs are either underpriced
(positive correlation) or overpriced (negative correlation).

Correlation types:
1. Same-game ML + Over: if team wins, game is more likely to have goals
2. Same-game ML + Under: negative correlation (winning team often shells)
3. Spread + ML: highly correlated (near-identical outcomes)
4. Same-game spread + total: mild correlation
5. Cross-game: generally independent (safe for standard parlay math)
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
#  Correlation matrix for common leg combinations
# ---------------------------------------------------------------------------

# Correlation coefficients estimated from historical NHL data.
# Positive = outcomes tend to co-occur, negative = tend to oppose.
# These are approximate — exact values depend on matchup and line.
_NHL_CORRELATIONS: Dict[Tuple[str, str], float] = {
    # Same-game correlations
    ("ml", "total_over"): 0.15,        # Winning team often means more goals
    ("ml", "total_under"): -0.15,      # Inverse of above
    ("ml", "spread"): 0.85,            # Nearly identical outcomes in NHL (PL ±1.5)
    ("spread", "total_over"): 0.10,    # Covering spread → more scoring
    ("spread", "total_under"): -0.10,

    # Favorite ML + Over is the classic "public parlay" — slightly correlated
    # because strong teams score more, but not as much as people think.
    ("fav_ml", "total_over"): 0.18,
    ("dog_ml", "total_under"): 0.12,   # Upset + low-scoring game
    ("dog_ml", "total_over"): -0.05,   # Slight negative — upsets often low-scoring

    # Period props
    ("ml", "first_goal"): 0.25,        # Winner more likely to score first
    ("ml", "regulation_winner"): 0.90, # Nearly identical (only differs in OT games)
}

_NBA_CORRELATIONS: Dict[Tuple[str, str], float] = {
    ("ml", "total_over"): 0.12,
    ("ml", "total_under"): -0.12,
    ("ml", "spread"): 0.90,
    ("spread", "total_over"): 0.08,
    ("spread", "total_under"): -0.08,
}


def get_correlation(
    bet_type_1: str,
    prediction_1: str,
    bet_type_2: str,
    prediction_2: str,
    same_game: bool,
    sport: str = "nhl",
) -> float:
    """Get the estimated correlation between two parlay legs.

    Args:
        bet_type_1: First leg bet type (e.g., "ml", "total", "spread").
        prediction_1: First leg prediction value.
        bet_type_2: Second leg bet type.
        prediction_2: Second leg prediction value.
        same_game: Whether both legs are from the same game.
        sport: Sport identifier.

    Returns:
        Correlation coefficient (-1 to 1). 0 = independent.
    """
    if not same_game:
        return 0.0  # Cross-game legs are essentially independent

    correlations = _NBA_CORRELATIONS if sport == "nba" else _NHL_CORRELATIONS

    # Normalize bet types for lookup
    bt1 = _normalize_bet_type(bet_type_1, prediction_1)
    bt2 = _normalize_bet_type(bet_type_2, prediction_2)

    # Try direct lookup
    corr = correlations.get((bt1, bt2))
    if corr is not None:
        return corr

    # Try reverse
    corr = correlations.get((bt2, bt1))
    if corr is not None:
        return corr

    # Same bet type in same game = perfectly correlated
    if bt1 == bt2:
        return 1.0

    return 0.0


def adjust_parlay_probability(
    legs: List[Dict[str, Any]],
    sport: str = "nhl",
) -> Dict[str, Any]:
    """Adjust parlay probability accounting for leg correlations.

    Standard parlay math multiplies individual probabilities assuming
    independence. When legs are correlated, the true probability differs:
    - Positive correlation: true prob > independent assumption (underpriced)
    - Negative correlation: true prob < independent assumption (overpriced)

    Uses the formula:
        P(A and B) ≈ P(A) * P(B) + ρ * sqrt(P(A)*(1-P(A)) * P(B)*(1-P(B)))
    where ρ is the correlation coefficient.

    Args:
        legs: List of parlay leg dicts, each with:
            - confidence: float (win probability)
            - bet_type: str
            - prediction: str (prediction value)
            - game_id: int

    Returns:
        Dict with independent_prob, correlated_prob, correlation_adjustment,
        and per-pair correlation details.
    """
    if len(legs) < 2:
        return {
            "independent_prob": legs[0]["confidence"] if legs else 0,
            "correlated_prob": legs[0]["confidence"] if legs else 0,
            "correlation_adjustment": 0.0,
            "pair_correlations": [],
            "has_high_correlation": False,
        }

    # Independent probability (standard parlay math)
    independent_prob = 1.0
    for leg in legs:
        independent_prob *= leg.get("confidence", 0.5)

    # Compute pairwise correlations
    pair_correlations = []
    total_adjustment = 0.0

    for i in range(len(legs)):
        for j in range(i + 1, len(legs)):
            leg_a = legs[i]
            leg_b = legs[j]

            same_game = leg_a.get("game_id") == leg_b.get("game_id")

            corr = get_correlation(
                leg_a.get("bet_type", ""),
                leg_a.get("prediction", ""),
                leg_b.get("bet_type", ""),
                leg_b.get("prediction", ""),
                same_game=same_game,
                sport=sport,
            )

            if abs(corr) > 0.01:
                p_a = leg_a.get("confidence", 0.5)
                p_b = leg_b.get("confidence", 0.5)

                import math
                # Adjustment to joint probability from correlation
                adj = corr * math.sqrt(p_a * (1 - p_a) * p_b * (1 - p_b))
                total_adjustment += adj

                pair_correlations.append({
                    "leg_a": leg_a.get("label", f"Leg {i+1}"),
                    "leg_b": leg_b.get("label", f"Leg {j+1}"),
                    "correlation": round(corr, 3),
                    "probability_adjustment": round(adj, 4),
                    "same_game": same_game,
                })

    correlated_prob = max(0.001, min(0.999, independent_prob + total_adjustment))
    has_high_correlation = any(abs(pc["correlation"]) > 0.3 for pc in pair_correlations)

    return {
        "independent_prob": round(independent_prob, 4),
        "correlated_prob": round(correlated_prob, 4),
        "correlation_adjustment": round(total_adjustment, 4),
        "pair_correlations": pair_correlations,
        "has_high_correlation": has_high_correlation,
        "recommendation": _parlay_recommendation(
            independent_prob, correlated_prob, pair_correlations
        ),
    }


def _normalize_bet_type(bet_type: str, prediction: str) -> str:
    """Normalize bet type + prediction for correlation lookup."""
    bt = bet_type.lower()
    pred = prediction.lower()

    if bt == "total":
        if "over" in pred:
            return "total_over"
        elif "under" in pred:
            return "total_under"
    if bt == "first_goal":
        return "first_goal"
    if bt == "regulation_winner":
        return "regulation_winner"

    return bt


def _parlay_recommendation(
    independent_prob: float,
    correlated_prob: float,
    pair_correlations: List[Dict],
) -> str:
    """Generate a recommendation based on correlation analysis."""
    if not pair_correlations:
        return "clean"  # No correlations — standard parlay math applies

    max_corr = max(abs(pc["correlation"]) for pc in pair_correlations)

    if max_corr > 0.80:
        return "avoid"  # Nearly identical outcomes — not a real parlay
    if max_corr > 0.30 and correlated_prob > independent_prob:
        return "caution_underpriced"  # Sportsbook may underprice this combo
    if max_corr > 0.30 and correlated_prob < independent_prob:
        return "caution_overpriced"  # Sportsbook may overprice this combo
    if max_corr > 0.10:
        return "mild_correlation"

    return "clean"
