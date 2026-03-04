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


# Maximum absolute American odds for any spread/puck-line price.
# Legitimate NHL puck lines rarely exceed ±400 for the standard 1.5 line
# and ±600 for alt lines.  Values beyond this threshold (e.g. -5000)
# indicate a data source is returning moneyline prices in the spread
# field.  800 provides a comfortable margin for unusual alt lines.
MAX_SPREAD_PRICE_ABS = 800


def is_reasonable_spread_price(price: float) -> bool:
    """Check that a spread price is within a plausible range.

    Catches data-source errors where moneyline values (e.g. -5000, +700)
    are returned as spread prices.
    """
    return abs(price) <= MAX_SPREAD_PRICE_ABS


# ---------------------------------------------------------------------------
# Alt-line validation
# ---------------------------------------------------------------------------

def validate_alt_totals_monotonicity(
    lines: List[Dict], label: str = ""
) -> List[Dict]:
    """Filter alt totals to enforce price monotonicity.

    For total lines sorted ascending by line value:
    - Over implied prob must DECREASE as line increases
      (easier to go over a lower line)
    - Lines that violate monotonicity are discarded with a warning.

    Uses a majority-consensus approach: instead of blindly anchoring to
    the first (lowest) line, compute the expected monotonic direction
    between adjacent pairs and reject outliers that disagree with the
    majority.  This prevents a single bad low line (e.g. period total
    data) from poisoning the entire set.
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

    if len(valid) <= 1:
        return valid

    # Compute over-implied for each line
    imps = []
    for entry in valid:
        imp = american_to_implied(entry["over_price"])
        imps.append(imp if imp is not None else 0)

    # Second pass: detect outliers via majority voting.
    # For correctly priced lines, over implied should DECREASE as line
    # increases.  Count how many adjacent pairs follow this rule.
    decreasing_pairs = 0
    total_pairs = 0
    for i in range(len(imps) - 1):
        total_pairs += 1
        if imps[i + 1] <= imps[i] + 0.02:
            decreasing_pairs += 1

    # If the majority of pairs are decreasing, the sequence is mostly
    # correct and we do the standard forward sweep (reject violators).
    # If NOT, the first line is likely the outlier — try starting from
    # the end and working backwards to find the consistent subset.
    if total_pairs > 0 and decreasing_pairs >= total_pairs / 2:
        # Standard forward sweep
        result = [valid[0]]
        for i in range(1, len(valid)):
            prev_imp = american_to_implied(result[-1]["over_price"]) or 0
            curr_imp = imps[i]
            if curr_imp > prev_imp + 0.02:
                logger.warning(
                    "Alt total %s line %.1f rejected (monotonicity): "
                    "O implied %.3f > prev line %.1f O implied %.3f",
                    label, valid[i]["line"], curr_imp,
                    result[-1]["line"], prev_imp,
                )
                continue
            result.append(valid[i])
    else:
        # Majority of adjacent pairs are NOT decreasing — the low end
        # is likely corrupted.  Build the result from the high end.
        result = [valid[-1]]
        for i in range(len(valid) - 2, -1, -1):
            next_imp = american_to_implied(result[-1]["over_price"]) or 0
            curr_imp = imps[i]
            # Going backwards: curr line is LOWER, so its over implied
            # should be HIGHER (or equal within tolerance).
            if curr_imp < next_imp - 0.02:
                logger.warning(
                    "Alt total %s line %.1f rejected (reverse monotonicity): "
                    "O implied %.3f < next line %.1f O implied %.3f",
                    label, valid[i]["line"], curr_imp,
                    result[-1]["line"], next_imp,
                )
                continue
            result.append(valid[i])
        # Reverse to restore ascending order by line
        result.reverse()

    return result


def validate_alt_spreads_monotonicity(
    lines: List[Dict], label: str = ""
) -> List[Dict]:
    """Filter alt spreads to enforce price consistency and monotonicity.

    Discards lines that fail vig validation (home_price + away_price).

    For spreads sorted ascending by absolute line value:
    - The favorite (negative spread) side becomes HARDER to cover as
      the line increases, so favorite implied prob should DECREASE.
    - The underdog (positive spread) side becomes EASIER to cover,
      so underdog implied prob should INCREASE.

    Uses the same majority-consensus approach as totals to handle
    outlier anchors.
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

    if len(valid) <= 1:
        return sorted(valid, key=lambda x: x.get("line", 0))

    valid.sort(key=lambda x: x.get("line", 0))

    # Use home_spread sign to determine which side is the favorite.
    # The favorite price (negative spread side) implied should DECREASE
    # as line increases.  We check this via home_price since the
    # merge normalizes home_spread direction.
    # Determine if home is favorite from the first entry with a sign.
    home_is_fav = None
    for entry in valid:
        hs = entry.get("home_spread", 0)
        if hs < 0:
            home_is_fav = True
            break
        elif hs > 0:
            home_is_fav = False
            break

    if home_is_fav is None:
        # Can't determine direction; just return vig-valid lines
        return valid

    # The favorite price is home_price when home_is_fav, else away_price.
    # Favorite implied should DECREASE as line increases (harder to cover
    # a bigger spread).
    fav_key = "home_price" if home_is_fav else "away_price"
    fav_imps = []
    for entry in valid:
        imp = american_to_implied(entry[fav_key])
        fav_imps.append(imp if imp is not None else 0)

    # Majority voting on adjacent pairs
    decreasing_pairs = 0
    total_pairs = 0
    for i in range(len(fav_imps) - 1):
        total_pairs += 1
        if fav_imps[i + 1] <= fav_imps[i] + 0.02:
            decreasing_pairs += 1

    if total_pairs > 0 and decreasing_pairs >= total_pairs / 2:
        # Forward sweep
        result = [valid[0]]
        for i in range(1, len(valid)):
            prev_imp = american_to_implied(result[-1][fav_key]) or 0
            curr_imp = fav_imps[i]
            if curr_imp > prev_imp + 0.02:
                logger.warning(
                    "Alt spread %s line %.1f rejected (monotonicity): "
                    "fav implied %.3f > prev line %.1f fav implied %.3f",
                    label, valid[i]["line"], curr_imp,
                    result[-1]["line"], prev_imp,
                )
                continue
            result.append(valid[i])
    else:
        # Reverse sweep — low end is corrupted
        result = [valid[-1]]
        for i in range(len(valid) - 2, -1, -1):
            next_imp = american_to_implied(result[-1][fav_key]) or 0
            curr_imp = fav_imps[i]
            if curr_imp < next_imp - 0.02:
                logger.warning(
                    "Alt spread %s line %.1f rejected (reverse monotonicity): "
                    "fav implied %.3f < next line %.1f fav implied %.3f",
                    label, valid[i]["line"], curr_imp,
                    result[-1]["line"], next_imp,
                )
                continue
            result.append(valid[i])
        result.reverse()

    return result


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


def validate_odds_event_primary_spread(
    home_spread: float,
    away_spread: float,
    home_price: float,
    away_price: float,
    source: str,
    matchup: str,
) -> Tuple[float, float, float, float]:
    """Validate primary spread line and prices for an OddsEvent.

    Checks that prices are valid American odds, within a reasonable range
    (not moneyline-magnitude values), and form a valid vig pair.
    Returns (home_spread, away_spread, home_price, away_price) — zeroed
    out if validation fails.
    """
    if home_spread == 0 and away_spread == 0:
        return home_spread, away_spread, home_price, away_price

    # Check both prices are valid American odds
    if not is_valid_american_odds(home_price) or not is_valid_american_odds(away_price):
        logger.warning(
            "[%s] %s: primary spread prices invalid American odds "
            "(H=%.1f @ %+.0f, A=%.1f @ %+.0f) — zeroing",
            source, matchup, home_spread, home_price, away_spread, away_price,
        )
        return 0, 0, 0, 0

    # Reject extreme prices that indicate moneyline contamination
    if not is_reasonable_spread_price(home_price) or not is_reasonable_spread_price(away_price):
        logger.warning(
            "[%s] %s: primary spread prices out of range "
            "(H=%.1f @ %+.0f, A=%.1f @ %+.0f, max ±%d) — zeroing",
            source, matchup, home_spread, home_price,
            away_spread, away_price, MAX_SPREAD_PRICE_ABS,
        )
        return 0, 0, 0, 0

    # Check vig is reasonable
    if not validate_spread_pair(home_price, away_price):
        logger.warning(
            "[%s] %s: primary spread prices bad vig "
            "(H=%.1f @ %+.0f, A=%.1f @ %+.0f) — zeroing",
            source, matchup, home_spread, home_price, away_spread, away_price,
        )
        return 0, 0, 0, 0

    return home_spread, away_spread, home_price, away_price


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
            if not is_reasonable_spread_price(hp) or not is_reasonable_spread_price(ap):
                logger.warning(
                    "[%s] %s: alt spread %.1f rejected (price out of range: H=%s A=%s)",
                    source, matchup, alt.get("line", 0), hp, ap,
                )
            elif validate_spread_pair(hp, ap):
                cleaned.append(alt)
            else:
                logger.debug(
                    "[%s] %s: alt spread %.1f rejected (H=%s A=%s)",
                    source, matchup, alt.get("line", 0), hp, ap,
                )

    return cleaned
