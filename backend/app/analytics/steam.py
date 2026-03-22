"""
Steam move and sharp money detection.

Detects rapid, coordinated line movement across multiple sportsbooks
(steam moves) that signal professional/syndicate money. Also tracks
line origination from sharp books (Pinnacle/CRIS) vs soft books.

Steam detection algorithm:
1. Track odds snapshots with timestamps across multiple books
2. Detect when 3+ books move in the same direction within a short window
3. Measure velocity of movement (magnitude / time)
4. Flag high-velocity coordinated moves as steam
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# Sharp book identifiers (these books are moved by professional bettors)
SHARP_BOOKS = {"pinnacle", "cris", "circa", "bookmaker", "bet365"}

# Soft book identifiers (retail books moved by public money)
SOFT_BOOKS = {"draftkings", "fanduel", "betmgm", "caesars", "pointsbet", "betrivers"}


def detect_steam_move(
    odds_snapshots: List[Dict[str, Any]],
    time_window_minutes: int = 30,
    min_books_moving: int = 3,
    min_movement_magnitude: float = 0.02,
) -> Dict[str, Any]:
    """Detect steam moves from a series of odds snapshots.

    A steam move occurs when multiple books move their lines in the
    same direction within a short time window, indicating coordinated
    professional action.

    Args:
        odds_snapshots: List of odds snapshot dicts, each containing:
            - bookmaker: str (book name)
            - timestamp: datetime
            - home_implied: float (home win implied probability)
            - away_implied: float
            - market: str ("h2h", "spreads", "totals")
        time_window_minutes: Max time window for coordinated movement.
        min_books_moving: Minimum books moving to qualify as steam.
        min_movement_magnitude: Minimum implied probability shift per book.

    Returns:
        Dict with steam detection results.
    """
    if len(odds_snapshots) < 2:
        return _no_steam_result()

    # Sort by timestamp
    sorted_snaps = sorted(odds_snapshots, key=lambda s: s.get("timestamp", datetime.min))

    # Group snapshots by bookmaker
    by_book: Dict[str, List[Dict]] = {}
    for snap in sorted_snaps:
        book = snap.get("bookmaker", "").lower()
        if book:
            by_book.setdefault(book, []).append(snap)

    if len(by_book) < 2:
        return _no_steam_result()

    # Calculate per-book movements
    movements: List[Dict[str, Any]] = []
    for book, snaps in by_book.items():
        if len(snaps) < 2:
            continue

        # Compare latest to earliest within window
        latest = snaps[-1]
        earliest = snaps[0]

        time_diff = _time_diff_minutes(earliest, latest)
        if time_diff <= 0 or time_diff > time_window_minutes * 2:
            continue

        home_shift = (
            (latest.get("home_implied", 0.5) or 0.5)
            - (earliest.get("home_implied", 0.5) or 0.5)
        )

        if abs(home_shift) >= min_movement_magnitude:
            movements.append({
                "bookmaker": book,
                "direction": "home" if home_shift > 0 else "away",
                "magnitude": abs(home_shift),
                "time_span_minutes": round(time_diff, 1),
                "velocity": abs(home_shift) / max(time_diff, 1) * 60,  # per hour
                "is_sharp_book": book in SHARP_BOOKS,
            })

    if not movements:
        return _no_steam_result()

    # Check for coordinated movement (multiple books same direction)
    home_movers = [m for m in movements if m["direction"] == "home"]
    away_movers = [m for m in movements if m["direction"] == "away"]

    steam_direction = None
    steam_movers = []

    if len(home_movers) >= min_books_moving:
        steam_direction = "home"
        steam_movers = home_movers
    elif len(away_movers) >= min_books_moving:
        steam_direction = "away"
        steam_movers = away_movers

    is_steam = steam_direction is not None

    # Check if sharp books led the movement
    sharp_led = False
    if is_steam:
        sharp_movers = [m for m in steam_movers if m["is_sharp_book"]]
        sharp_led = len(sharp_movers) > 0

    # Compute average velocity across movers
    avg_velocity = 0.0
    avg_magnitude = 0.0
    if steam_movers:
        avg_velocity = sum(m["velocity"] for m in steam_movers) / len(steam_movers)
        avg_magnitude = sum(m["magnitude"] for m in steam_movers) / len(steam_movers)

    # Steam strength (0-1)
    strength = 0.0
    if is_steam:
        # More books = stronger signal
        book_factor = min(1.0, len(steam_movers) / 6.0)
        # Higher velocity = more urgent/sharp
        velocity_factor = min(1.0, avg_velocity / 0.10)
        # Sharp books leading = much stronger
        sharp_factor = 1.3 if sharp_led else 1.0

        strength = min(1.0, (0.4 * book_factor + 0.6 * velocity_factor) * sharp_factor)

    return {
        "is_steam": is_steam,
        "steam_direction": steam_direction,
        "steam_strength": round(strength, 3),
        "books_moving": len(steam_movers),
        "total_books_tracked": len(by_book),
        "sharp_books_led": sharp_led,
        "avg_magnitude": round(avg_magnitude, 4),
        "avg_velocity_per_hour": round(avg_velocity, 4),
        "movements": movements,
    }


def analyze_line_origination(
    odds_snapshots: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Analyze which books moved first (line origination).

    Sharp books (Pinnacle, CRIS, Circa) set the market — when they
    move, soft books follow. If our model agrees with the sharp
    originator, that's a stronger signal than agreeing with soft books.

    Args:
        odds_snapshots: Same format as detect_steam_move.

    Returns:
        Dict with origination analysis.
    """
    if len(odds_snapshots) < 2:
        return {
            "originator": None,
            "originator_is_sharp": False,
            "sharp_vs_soft_agreement": "unknown",
            "sharp_direction": None,
            "soft_direction": None,
        }

    sorted_snaps = sorted(odds_snapshots, key=lambda s: s.get("timestamp", datetime.min))

    # Find the first book to move significantly
    by_book: Dict[str, List[Dict]] = {}
    for snap in sorted_snaps:
        book = snap.get("bookmaker", "").lower()
        if book:
            by_book.setdefault(book, []).append(snap)

    first_mover = None
    first_move_time = None
    first_move_direction = None

    for book, snaps in by_book.items():
        if len(snaps) < 2:
            continue
        shift = (
            (snaps[-1].get("home_implied", 0.5) or 0.5)
            - (snaps[0].get("home_implied", 0.5) or 0.5)
        )
        if abs(shift) >= 0.015:
            move_time = snaps[1].get("timestamp")
            if first_move_time is None or (move_time and move_time < first_move_time):
                first_move_time = move_time
                first_mover = book
                first_move_direction = "home" if shift > 0 else "away"

    # Check sharp vs soft consensus
    sharp_dir = _consensus_direction(by_book, SHARP_BOOKS)
    soft_dir = _consensus_direction(by_book, SOFT_BOOKS)

    agreement = "unknown"
    if sharp_dir and soft_dir:
        agreement = "aligned" if sharp_dir == soft_dir else "divergent"
    elif sharp_dir:
        agreement = "sharp_only"
    elif soft_dir:
        agreement = "soft_only"

    return {
        "originator": first_mover,
        "originator_is_sharp": first_mover in SHARP_BOOKS if first_mover else False,
        "origination_direction": first_move_direction,
        "sharp_vs_soft_agreement": agreement,
        "sharp_direction": sharp_dir,
        "soft_direction": soft_dir,
    }


def find_best_available_odds(
    current_odds: Dict[str, Dict[str, float]],
    side: str,
    bet_type: str = "h2h",
) -> Dict[str, Any]:
    """Find the best available odds across all books for a given side.

    Even 5-10 cents of juice savings compounds significantly over
    hundreds of bets. This identifies which book offers the best
    price for each side.

    Args:
        current_odds: Dict keyed by bookmaker, values are dicts with
            odds fields (e.g., "home_price", "away_price", etc.)
        side: "home" or "away" (or "over"/"under" for totals).
        bet_type: "h2h", "spreads", or "totals".

    Returns:
        Dict with best book, best odds, and comparison to consensus.
    """
    if not current_odds:
        return {"best_book": None, "best_odds": None, "savings_vs_avg": 0.0}

    price_key = _price_key(side, bet_type)

    book_odds = []
    for book, odds_data in current_odds.items():
        price = odds_data.get(price_key)
        if price is not None and price != 0:
            book_odds.append((book, float(price)))

    if not book_odds:
        return {"best_book": None, "best_odds": None, "savings_vs_avg": 0.0}

    # Best odds = highest American odds (most payout)
    best_book, best_price = max(book_odds, key=lambda x: x[1])

    # Compute average and implied probability savings
    avg_price = sum(p for _, p in book_odds) / len(book_odds)

    # Convert to implied probability difference
    best_implied = _american_to_implied(best_price)
    avg_implied = _american_to_implied(avg_price)
    savings = avg_implied - best_implied  # positive = you're getting better odds

    return {
        "best_book": best_book,
        "best_odds": round(best_price),
        "avg_odds": round(avg_price),
        "books_compared": len(book_odds),
        "savings_vs_avg": round(savings, 4),
        "all_books": [
            {"book": b, "odds": round(p)} for b, p in sorted(book_odds, key=lambda x: -x[1])
        ],
    }


# ---------------------------------------------------------------------------
#  Private helpers
# ---------------------------------------------------------------------------

def _no_steam_result() -> Dict[str, Any]:
    return {
        "is_steam": False,
        "steam_direction": None,
        "steam_strength": 0.0,
        "books_moving": 0,
        "total_books_tracked": 0,
        "sharp_books_led": False,
        "avg_magnitude": 0.0,
        "avg_velocity_per_hour": 0.0,
        "movements": [],
    }


def _time_diff_minutes(earlier: Dict, later: Dict) -> float:
    t1 = earlier.get("timestamp")
    t2 = later.get("timestamp")
    if not t1 or not t2:
        return 0.0
    if isinstance(t1, str):
        t1 = datetime.fromisoformat(t1)
    if isinstance(t2, str):
        t2 = datetime.fromisoformat(t2)
    diff = (t2 - t1).total_seconds() / 60.0
    return max(0, diff)


def _consensus_direction(
    by_book: Dict[str, List[Dict]],
    book_set: set,
) -> Optional[str]:
    """Determine consensus direction for a set of books."""
    home_votes = 0
    away_votes = 0
    for book, snaps in by_book.items():
        if book not in book_set or len(snaps) < 2:
            continue
        shift = (
            (snaps[-1].get("home_implied", 0.5) or 0.5)
            - (snaps[0].get("home_implied", 0.5) or 0.5)
        )
        if shift > 0.01:
            home_votes += 1
        elif shift < -0.01:
            away_votes += 1
    if home_votes > away_votes and home_votes > 0:
        return "home"
    if away_votes > home_votes and away_votes > 0:
        return "away"
    return None


def _price_key(side: str, bet_type: str) -> str:
    """Map side + bet_type to the odds dict key."""
    if bet_type == "h2h":
        return f"{side}_price"
    if bet_type == "totals":
        return f"{side}_price"
    if bet_type == "spreads":
        return f"{side}_spread_price"
    return f"{side}_price"


def _american_to_implied(odds: float) -> float:
    """Convert American odds to implied probability."""
    if odds == 0:
        return 0.5
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)
