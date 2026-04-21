"""
Line movement tracking — tracks opening vs current odds for edge detection.

When a line moves significantly (e.g. -150 to -180), the market is
telling us something. Sharp money, injury news, or lineup changes
can cause significant moves.

Usage:
    from engine.line_movement import get_line_movement, track_opening_odds
"""

import json
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

# Store opening odds in a simple JSON file (persists across restarts)
_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_OPENING_ODDS_FILE = _DATA_DIR / "opening_odds.json"


def _load_opening_odds() -> dict:
    """Load saved opening odds from file."""
    if _OPENING_ODDS_FILE.exists():
        try:
            with open(_OPENING_ODDS_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_opening_odds(data: dict):
    """Save opening odds to file."""
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(_OPENING_ODDS_FILE, "w") as f:
        json.dump(data, f, indent=2)


def track_opening_odds(sport: str, matchup_key: str, odds: dict):
    """
    Record opening odds for a game if not already tracked.

    Args:
        sport: "mlb" or "nhl"
        matchup_key: e.g. "2026-04-07_BOS@NYY"
        odds: current odds dict with home_ml, away_ml, over_under, etc.
    """
    if not odds or not odds.get("home_ml"):
        return

    all_odds = _load_opening_odds()
    key = f"{sport}:{matchup_key}"

    if key not in all_odds:
        all_odds[key] = {
            "home_ml": odds.get("home_ml"),
            "away_ml": odds.get("away_ml"),
            "over_under": odds.get("over_under"),
            "over_odds": odds.get("over_odds"),
            "under_odds": odds.get("under_odds"),
            "home_spread_point": odds.get("home_spread_point"),
            "away_spread_point": odds.get("away_spread_point"),
            "timestamp": time.time(),
        }
        _save_opening_odds(all_odds)
        logger.debug("Tracked opening odds for %s", key)


def get_line_movement(sport: str, matchup_key: str, current_odds: dict) -> dict | None:
    """
    Compare current odds to opening odds and return movement info.

    Returns:
        {
            "home_ml_open": -150, "home_ml_current": -175, "home_ml_move": -25,
            "away_ml_open": 130, "away_ml_current": 150, "away_ml_move": +20,
            "total_open": 8.5, "total_current": 8.0, "total_move": -0.5,
            "direction": "home",  # money moving toward home team
            "significance": "moderate",  # minor/moderate/major
        }
    """
    if not current_odds or not current_odds.get("home_ml"):
        return None

    all_odds = _load_opening_odds()
    key = f"{sport}:{matchup_key}"

    opening = all_odds.get(key)
    if not opening:
        # First time seeing this game — record opening odds
        track_opening_odds(sport, matchup_key, current_odds)
        return None

    result = {}

    # ML movement
    h_open = opening.get("home_ml")
    h_curr = current_odds.get("home_ml")
    a_open = opening.get("away_ml")
    a_curr = current_odds.get("away_ml")

    if h_open and h_curr:
        h_move = _compute_ml_shift(h_open, h_curr)
        a_move = _compute_ml_shift(a_open, a_curr) if a_open and a_curr else 0

        result["home_ml_open"] = h_open
        result["home_ml_current"] = h_curr
        result["home_ml_move"] = h_move
        result["away_ml_open"] = a_open
        result["away_ml_current"] = a_curr
        result["away_ml_move"] = a_move

        # Determine direction
        if h_move < -10:  # Home getting more expensive = money on home
            result["direction"] = "home"
        elif a_move < -10:
            result["direction"] = "away"
        else:
            result["direction"] = "neutral"

        # Significance
        abs_move = max(abs(h_move), abs(a_move))
        if abs_move >= 30:
            result["significance"] = "major"
        elif abs_move >= 15:
            result["significance"] = "moderate"
        elif abs_move >= 5:
            result["significance"] = "minor"
        else:
            result["significance"] = "none"

    # Total movement
    t_open = opening.get("over_under")
    t_curr = current_odds.get("over_under")
    if t_open and t_curr:
        result["total_open"] = t_open
        result["total_current"] = t_curr
        result["total_move"] = round(t_curr - t_open, 1)

    return result if result else None


def _compute_ml_shift(old_ml: int, new_ml: int) -> int:
    """Compute moneyline shift. Negative = line got more expensive (sharper)."""
    if old_ml < 0 and new_ml < 0:
        return new_ml - old_ml  # -150 to -175 = -25
    elif old_ml > 0 and new_ml > 0:
        return new_ml - old_ml  # +130 to +150 = +20
    elif old_ml > 0 and new_ml < 0:
        return -(old_ml + abs(new_ml))  # Flipped sides — big move
    elif old_ml < 0 and new_ml > 0:
        return abs(old_ml) + new_ml  # Flipped sides — big move
    return 0


def cleanup_old_odds(max_age_days: int = 3):
    """Remove opening odds older than max_age_days."""
    all_odds = _load_opening_odds()
    cutoff = time.time() - (max_age_days * 86400)
    cleaned = {k: v for k, v in all_odds.items()
               if v.get("timestamp", 0) > cutoff}
    if len(cleaned) < len(all_odds):
        _save_opening_odds(cleaned)
        logger.info("Cleaned %d old opening odds entries", len(all_odds) - len(cleaned))
