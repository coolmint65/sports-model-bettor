"""
Odds validation utilities for the multi-source odds pipeline.

Provides mathematical sanity checks for American odds, implied
probabilities, vig ranges, and monotonicity constraints.  Used by
source fetchers, the merge logic, and the DB sync layer to catch
corrupted or impossible prices before they enter the system.
"""

import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Normal bookmaker vig: implied probabilities for both sides sum to 1.02-1.15.
# Wider range accounts for alt lines which can have higher vig.
VIG_MIN = 0.95
VIG_MAX = 1.25

# American odds must be <= -100 or >= +100 (no values in (-100, +100) except 0).
MIN_NEGATIVE_ODDS = -100
MIN_POSITIVE_ODDS = 100


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------

def american_to_implied(odds: float) -> Optional[float]:
    """Convert American odds to implied probability.

    Returns None for invalid odds (in the -100..+100 dead zone or zero).
    """
    if odds == 0:
        return None
    if -MIN_NEGATIVE_ODDS < odds < MIN_POSITIVE_ODDS and odds != 0:
        return None
    if odds > 0:
        return round(100.0 / (odds + 100.0), 6)
    return round(abs(odds) / (abs(odds) + 100.0), 6)


def implied_to_american(prob: float) -> Optional[float]:
    """Convert implied probability to American odds."""
    if prob is None or prob <= 0 or prob >= 1:
        return None
    if prob > 0.5:
        return round(-(prob / (1 - prob)) * 100)
    return round(((1 - prob) / prob) * 100)


# ---------------------------------------------------------------------------
# Validation functions
# ---------------------------------------------------------------------------

def is_valid_american_odds(odds: float) -> bool:
    """Check that a value is valid American odds (not in the dead zone)."""
    if odds == 0:
        return False
    if -100 < odds < 100:
        return False
    return True


def validate_odds_pair(odds_a: float, odds_b: float) -> bool:
    """Validate that two complementary odds form a valid market.

    For any two-outcome market (ML, over/under, spread), the sum of
    implied probabilities should be in [VIG_MIN, VIG_MAX].
    """
    imp_a = american_to_implied(odds_a)
    imp_b = american_to_implied(odds_b)
    if imp_a is None or imp_b is None:
        return False
    total = imp_a + imp_b
    return VIG_MIN <= total <= VIG_MAX


def validate_moneyline(home_ml: float, away_ml: float) -> bool:
    """Validate a moneyline pair."""
    if not is_valid_american_odds(home_ml) or not is_valid_american_odds(away_ml):
        return False
    return validate_odds_pair(home_ml, away_ml)


def validate_total_line_pair(
    line: float, over_price: float, under_price: float
) -> bool:
    """Validate a single total line's prices.

    Checks:
    1. Both prices are valid American odds
    2. Implied probabilities sum to normal vig range
    """
    if not is_valid_american_odds(over_price) or not is_valid_american_odds(under_price):
        return False
    return validate_odds_pair(over_price, under_price)


def validate_spread_pair(home_price: float, away_price: float) -> bool:
    """Validate a spread price pair."""
    if not is_valid_american_odds(home_price) or not is_valid_american_odds(away_price):
        return False
    return validate_odds_pair(home_price, away_price)


# ---------------------------------------------------------------------------
# Alt-line validation
# ---------------------------------------------------------------------------

def validate_alt_totals_monotonicity(
    lines: List[Dict], label: str = ""
) -> List[Dict]:
    """Filter alt totals to enforce price monotonicity.

    For total lines sorted ascending by line value:
    - Over prices must be non-increasing (O4.5 <= O5.5 <= O6.5 in magnitude,
      meaning more negative for lower lines)
    - Under prices must be non-decreasing (same logic, inverted)

    Lines that violate monotonicity are discarded with a warning.
    Also discards lines that fail vig validation.
    """
    if not lines:
        return []

    # First pass: discard lines with invalid vig
    valid = []
    for entry in lines:
        lv = entry.get("line", 0)
        op = entry.get("over_price", 0)
        up = entry.get("under_price", 0)
        if validate_total_line_pair(lv, op, up):
            valid.append(entry)
        else:
            logger.warning(
                "Alt total %s line %.1f rejected (invalid vig): O=%s U=%s",
                label, lv, op, up,
            )

    if not valid:
        return []

    # Sort by line ascending
    valid.sort(key=lambda x: x["line"])

    # Second pass: enforce monotonicity
    # Over implied prob should INCREASE as line increases (easier to go over a lower line)
    # So over_price should become less negative (increase) as line increases.
    result = [valid[0]]
    for i in range(1, len(valid)):
        prev = result[-1]
        curr = valid[i]
        prev_over_imp = american_to_implied(prev["over_price"]) or 0
        curr_over_imp = american_to_implied(curr["over_price"]) or 0

        # Over implied should decrease as line increases
        # (harder to go over a higher line)
        if curr_over_imp > prev_over_imp + 0.02:
            # Current line has HIGHER over implied than a lower line — wrong
            logger.warning(
                "Alt total %s line %.1f rejected (monotonicity): "
                "O implied %.3f > prev line %.1f O implied %.3f",
                label, curr["line"], curr_over_imp,
                prev["line"], prev_over_imp,
            )
            continue

        result.append(curr)

    return result


def validate_alt_spreads_monotonicity(
    lines: List[Dict], label: str = ""
) -> List[Dict]:
    """Filter alt spreads to enforce price consistency.

    Discards lines that fail vig validation (home_price + away_price).
    """
    if not lines:
        return []

    valid = []
    for entry in lines:
        hp = entry.get("home_price", 0)
        ap = entry.get("away_price", 0)
        if validate_spread_pair(hp, ap):
            valid.append(entry)
        else:
            logger.warning(
                "Alt spread %s line %.1f rejected (invalid vig): H=%s A=%s",
                label, entry.get("line", 0), hp, ap,
            )

    return sorted(valid, key=lambda x: x.get("line", 0))


# ---------------------------------------------------------------------------
# Source-level OddsEvent validation
# ---------------------------------------------------------------------------

def validate_odds_event_totals(
    alt_totals: List[Dict],
    primary_line: float,
    primary_over: float,
    primary_under: float,
    source: str,
    matchup: str,
) -> Tuple[List[Dict], float, float, float]:
    """Validate and clean an OddsEvent's total data.

    Returns (cleaned_alt_totals, primary_line, primary_over, primary_under).
    Invalid entries are removed; primary line is validated.
    """
    # Validate primary line
    if primary_line > 0 and primary_over != 0 and primary_under != 0:
        if not validate_total_line_pair(primary_line, primary_over, primary_under):
            logger.warning(
                "[%s] %s: primary O/U %.1f rejected (O=%s U=%s, bad vig)",
                source, matchup, primary_line, primary_over, primary_under,
            )
            primary_line = 0.0
            primary_over = 0.0
            primary_under = 0.0

    # Validate alt totals
    cleaned = []
    for alt in alt_totals:
        lv = alt.get("line", 0)
        op = alt.get("over_price", 0)
        up = alt.get("under_price", 0)
        if lv > 0 and op != 0 and up != 0:
            if validate_total_line_pair(lv, op, up):
                cleaned.append(alt)
            else:
                logger.debug(
                    "[%s] %s: alt total %.1f rejected (O=%s U=%s)",
                    source, matchup, lv, op, up,
                )

    return cleaned, primary_line, primary_over, primary_under


def validate_odds_event_spreads(
    alt_spreads: List[Dict],
    source: str,
    matchup: str,
) -> List[Dict]:
    """Validate and clean an OddsEvent's spread data."""
    cleaned = []
    for alt in alt_spreads:
        hp = alt.get("home_price", 0)
        ap = alt.get("away_price", 0)
        if hp != 0 and ap != 0:
            if validate_spread_pair(hp, ap):
                cleaned.append(alt)
            else:
                logger.debug(
                    "[%s] %s: alt spread %.1f rejected (H=%s A=%s)",
                    source, matchup, alt.get("line", 0), hp, ap,
                )

    return cleaned
