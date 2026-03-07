"""
Shared constants used across the application.

Centralizes values that were previously duplicated in multiple modules
(API routes, scrapers, analytics) to ensure consistency.
"""

# Statuses that indicate a game is finished and its score is final.
# Used for filtering, grading predictions, and settling bets.
GAME_FINAL_STATUSES = ("final", "completed", "off", "official")

# Statuses that represent games we can generate predictions for.
# This is the complement of GAME_FINAL_STATUSES — every non-final
# status should be here so games are never deleted-but-not-regenerated.
GAME_PREDICTABLE_STATUSES = (
    "scheduled",
    "pregame",
    "preview",
    "in_progress",
    "live",
)

# The three core sportsbook market types the model generates predictions for.
MARKET_BET_TYPES = ("ml", "total", "spread")

# Shared browser headers for sportsbook scraping.
SCRAPER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
}


def is_heavy_juice(implied_prob: float | None, ceiling: float) -> bool:
    """Return True if *implied_prob* meets or exceeds the juice *ceiling*."""
    return implied_prob is not None and implied_prob >= ceiling


def composite_pick_score(
    confidence: float | None,
    edge: float | None,
    implied_prob: float | None,
) -> float:
    """Rank picks by a blend of confidence, edge, and juice quality.

    Higher score = better pick.

    Weights:
      - confidence (45%): how likely the bet is to win
      - edge       (35%): value over the market line
      - juice      (20%): payout quality (lower implied prob = less juice)

    Each component is normalized to 0-1 before weighting so no single
    factor dominates.  Edge is capped at 25% (the model hard-cap) for
    normalization purposes.
    """
    c = confidence or 0.0
    e = min(edge or 0.0, 0.25) / 0.25  # normalize 0-25% → 0-1
    j = 1.0 - (implied_prob or 0.5)     # lower implied = better juice
    return 0.45 * c + 0.35 * e + 0.20 * j
