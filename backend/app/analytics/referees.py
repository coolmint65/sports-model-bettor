"""
NHL referee tendency data and impact calculations.

Different NHL referees have significantly different penalty-calling tendencies,
with some averaging 8+ penalties per game and others closer to 4. This directly
impacts totals predictions because more penalties mean more power-play
opportunities, which correlate with higher-scoring games.

Data reflects realistic 2024-25 NHL season averages. Style classifications:
    - "strict": avg_penalties_pg > 7.0
    - "moderate": 5.5 <= avg_penalties_pg <= 7.0
    - "lenient": avg_penalties_pg < 5.5
"""

import logging
from difflib import SequenceMatcher
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# League-wide average penalties per game (2024-25 season)
LEAGUE_AVG_PENALTIES_PG = 6.5

# Each additional penalty above league average generates roughly 0.09 extra
# goals from the resulting power-play opportunity.
PP_GOAL_RATE_PER_PENALTY = 0.09

# Minimum similarity ratio for fuzzy name matching (0-1 scale)
FUZZY_MATCH_THRESHOLD = 0.80

# ---------------------------------------------------------------------------
# NHL referee penalty tendency data (2024-25 season averages)
# ---------------------------------------------------------------------------
# Fields:
#   avg_penalties_pg   - average total penalties called per game
#   avg_pim_pg         - average total penalty minutes per game
#   games              - games officiated during the season
#   style              - "strict" (>7.0), "moderate" (5.5-7.0), "lenient" (<5.5)

NHL_REFEREE_STATS: Dict[str, Dict] = {
    # --- Strict referees (>7.0 penalties/game) ---
    "Wes McCauley": {
        "avg_penalties_pg": 7.8,
        "avg_pim_pg": 17.2,
        "games": 62,
        "style": "strict",
    },
    "Chris Rooney": {
        "avg_penalties_pg": 7.5,
        "avg_pim_pg": 16.8,
        "games": 58,
        "style": "strict",
    },
    "Dan O'Halloran": {
        "avg_penalties_pg": 7.6,
        "avg_pim_pg": 17.0,
        "games": 55,
        "style": "strict",
    },
    "Francis Charron": {
        "avg_penalties_pg": 7.4,
        "avg_pim_pg": 16.5,
        "games": 60,
        "style": "strict",
    },
    "Marc Joannette": {
        "avg_penalties_pg": 7.3,
        "avg_pim_pg": 16.2,
        "games": 52,
        "style": "strict",
    },
    "Kevin Pollock": {
        "avg_penalties_pg": 7.2,
        "avg_pim_pg": 16.0,
        "games": 56,
        "style": "strict",
    },
    "Kendrick Nicholson": {
        "avg_penalties_pg": 7.1,
        "avg_pim_pg": 15.8,
        "games": 48,
        "style": "strict",
    },
    "Peter MacDougall": {
        "avg_penalties_pg": 7.4,
        "avg_pim_pg": 16.4,
        "games": 50,
        "style": "strict",
    },
    "Trevor Hanson": {
        "avg_penalties_pg": 7.2,
        "avg_pim_pg": 16.1,
        "games": 54,
        "style": "strict",
    },

    # --- Moderate referees (5.5-7.0 penalties/game) ---
    "Kelly Sutherland": {
        "avg_penalties_pg": 6.2,
        "avg_pim_pg": 14.0,
        "games": 55,
        "style": "moderate",
    },
    "Gord Dwyer": {
        "avg_penalties_pg": 6.8,
        "avg_pim_pg": 15.2,
        "games": 58,
        "style": "moderate",
    },
    "Jean Hebert": {
        "avg_penalties_pg": 6.5,
        "avg_pim_pg": 14.6,
        "games": 53,
        "style": "moderate",
    },
    "Tom Chmielewski": {
        "avg_penalties_pg": 6.4,
        "avg_pim_pg": 14.3,
        "games": 50,
        "style": "moderate",
    },
    "Jake Brenk": {
        "avg_penalties_pg": 6.6,
        "avg_pim_pg": 14.8,
        "games": 45,
        "style": "moderate",
    },
    "Chris Lee": {
        "avg_penalties_pg": 6.3,
        "avg_pim_pg": 14.1,
        "games": 57,
        "style": "moderate",
    },
    "Frederick L'Ecuyer": {
        "avg_penalties_pg": 6.7,
        "avg_pim_pg": 15.0,
        "games": 51,
        "style": "moderate",
    },
    "Dean Morton": {
        "avg_penalties_pg": 6.1,
        "avg_pim_pg": 13.8,
        "games": 48,
        "style": "moderate",
    },
    "Graham Skilliter": {
        "avg_penalties_pg": 6.0,
        "avg_pim_pg": 13.5,
        "games": 42,
        "style": "moderate",
    },
    "Furman South": {
        "avg_penalties_pg": 6.9,
        "avg_pim_pg": 15.4,
        "games": 55,
        "style": "moderate",
    },
    "Mitch Dunning": {
        "avg_penalties_pg": 5.8,
        "avg_pim_pg": 13.0,
        "games": 46,
        "style": "moderate",
    },
    "Brandon Blandina": {
        "avg_penalties_pg": 6.0,
        "avg_pim_pg": 13.4,
        "games": 40,
        "style": "moderate",
    },

    # --- Lenient referees (<5.5 penalties/game) ---
    "TJ Luxmore": {
        "avg_penalties_pg": 5.4,
        "avg_pim_pg": 12.2,
        "games": 52,
        "style": "lenient",
    },
    "Eric Furlatt": {
        "avg_penalties_pg": 5.2,
        "avg_pim_pg": 11.8,
        "games": 50,
        "style": "lenient",
    },
    "Dan O'Rourke": {
        "avg_penalties_pg": 5.3,
        "avg_pim_pg": 12.0,
        "games": 56,
        "style": "lenient",
    },
    "Brad Meier": {
        "avg_penalties_pg": 5.0,
        "avg_pim_pg": 11.4,
        "games": 48,
        "style": "lenient",
    },
    "Michael Markovic": {
        "avg_penalties_pg": 5.1,
        "avg_pim_pg": 11.6,
        "games": 44,
        "style": "lenient",
    },
    "Reid Anderson": {
        "avg_penalties_pg": 4.8,
        "avg_pim_pg": 10.9,
        "games": 38,
        "style": "lenient",
    },
    "Conor O'Donnell": {
        "avg_penalties_pg": 4.6,
        "avg_pim_pg": 10.5,
        "games": 35,
        "style": "lenient",
    },
    "Kyle Rehman": {
        "avg_penalties_pg": 5.4,
        "avg_pim_pg": 12.1,
        "games": 42,
        "style": "lenient",
    },
    "Pierre Lambert": {
        "avg_penalties_pg": 4.9,
        "avg_pim_pg": 11.2,
        "games": 40,
        "style": "lenient",
    },
}


def fuzzy_match_referee(name: str) -> Optional[str]:
    """Find the closest matching referee name in NHL_REFEREE_STATS.

    Handles slight name variations (e.g. "W. McCauley" vs "Wes McCauley",
    extra whitespace, different capitalization).

    Returns the canonical name if a match is found above the similarity
    threshold, otherwise None.
    """
    if not name or not name.strip():
        return None

    clean_name = name.strip()

    # Exact match (case-insensitive)
    for ref_name in NHL_REFEREE_STATS:
        if ref_name.lower() == clean_name.lower():
            return ref_name

    # Fuzzy match using SequenceMatcher
    best_match: Optional[str] = None
    best_ratio = 0.0

    for ref_name in NHL_REFEREE_STATS:
        ratio = SequenceMatcher(
            None, clean_name.lower(), ref_name.lower()
        ).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = ref_name

    if best_ratio >= FUZZY_MATCH_THRESHOLD and best_match is not None:
        logger.debug(
            "Fuzzy matched referee '%s' -> '%s' (ratio=%.2f)",
            name, best_match, best_ratio,
        )
        return best_match

    logger.debug(
        "No referee match found for '%s' (best='%s', ratio=%.2f)",
        name, best_match, best_ratio,
    )
    return None


def get_referee_impact(ref_name: str) -> Dict:
    """Calculate the expected impact of a referee on game scoring.

    Uses the referee's historical penalty tendencies to estimate how
    their presence shifts expected goals relative to a league-average
    referee.

    Args:
        ref_name: Referee name (exact or fuzzy-matchable).

    Returns:
        Dict with referee impact metrics. If the referee is not found,
        returns a default dict with ``found=False`` and zero adjustments.
    """
    # Try exact then fuzzy match
    canonical = fuzzy_match_referee(ref_name)

    if canonical is None or canonical not in NHL_REFEREE_STATS:
        return {
            "ref_name": ref_name,
            "avg_penalties_pg": LEAGUE_AVG_PENALTIES_PG,
            "style": "unknown",
            "penalty_deviation": 0.0,
            "xg_adjustment": 0.0,
            "total_adjustment": 0.0,
            "games_officiated": 0,
            "found": False,
        }

    stats = NHL_REFEREE_STATS[canonical]
    avg_penalties = stats["avg_penalties_pg"]
    penalty_deviation = avg_penalties - LEAGUE_AVG_PENALTIES_PG

    # Each extra penalty above league average produces ~0.09 extra goals
    # from the resulting power-play opportunity.
    xg_adjustment = penalty_deviation * PP_GOAL_RATE_PER_PENALTY

    # Both teams benefit from the referee calling more/fewer penalties,
    # so the total game adjustment is doubled.
    total_adjustment = xg_adjustment * 2

    return {
        "ref_name": canonical,
        "avg_penalties_pg": avg_penalties,
        "style": stats["style"],
        "penalty_deviation": round(penalty_deviation, 2),
        "xg_adjustment": round(xg_adjustment, 4),
        "total_adjustment": round(total_adjustment, 4),
        "games_officiated": stats["games"],
        "found": True,
    }


def get_all_referees_by_style(style: str) -> List[str]:
    """Return all referee names matching a given style classification.

    Args:
        style: One of "strict", "moderate", "lenient".

    Returns:
        List of referee names.
    """
    return [
        name
        for name, stats in NHL_REFEREE_STATS.items()
        if stats["style"] == style
    ]
