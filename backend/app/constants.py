"""
Shared constants used across the application.

Centralizes values that were previously duplicated in multiple modules
(API routes, scrapers, analytics) to ensure consistency.
"""

# Statuses that indicate a game is finished and its score is final.
# Used for filtering, grading predictions, and settling bets.
GAME_FINAL_STATUSES = ("final", "completed", "off", "official")

# The three core sportsbook market types the model generates predictions for.
MARKET_BET_TYPES = ("ml", "total", "spread")
