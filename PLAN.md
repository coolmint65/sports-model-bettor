# Odds Pipeline Overhaul Plan

## Root Cause Analysis

The O4.5 at +115 bug, missing O/U, and recurring fix cycle all trace to fundamental gaps:

### 1. No Price Validation for Totals
Spread prices get sign-validated against moneyline (commits 1177b22, e670e9b), but
total line prices have ZERO validation. The merge takes `max(over_price)` across
sources independently — if any source maps the wrong line to a price, `max()` picks
the corrupted value. Example: O4.5 should be -800, but a source error maps +115
(actually U6.5's price) to O4.5, and `max(-800, +115) = +115`.

### 2. Alt Line Prices Not Paired by Source
Each alt total line's over_price and under_price are maximized independently across
sources (lines 2056-2076). This can produce impossible pairs where the over and under
come from different sources and don't represent the same market.

### 3. Default 0.0 Leaks Through as Real Data
`best_total/best_over/best_under` initialized to 0.0 → passes `is not None` checks →
writes `over_price=0` and `under_price=0` to DB. (Partially fixed in prior commit for
totals, but ML and spreads still have this issue.)

### 4. No Monotonicity Check on Alt Lines
For any set of total lines, over prices MUST decrease monotonically as the line
decreases (O4.5 is always more negative than O5.5 which is more negative than O6.5).
This invariant is never checked, so corrupted data persists undetected.

### 5. No Vig/Complementarity Check
For any given total line, `implied(over) + implied(under)` must be ~1.02-1.15
(bookmaker vig). If it's wildly off, the prices don't belong together. Never checked.

### 6. Live Odds Only Refresh on User View
No background auto-refresh. Predictions can use 2+ minute old data.

---

## Implementation Steps

### Step 1: Price Validation Utilities

Add `backend/app/scrapers/odds_validation.py` with:

```python
def american_to_implied(odds: float) -> float:
    """Convert American odds to implied probability."""

def implied_pair_valid(over_odds: float, under_odds: float) -> bool:
    """Check that over+under implied prob sums to ~1.02-1.15 (normal vig)."""

def validate_total_line_pair(line: float, over_price: float, under_price: float) -> bool:
    """Validate a single total line's prices make sense."""
    # 1. Both prices must be <= -100 or >= +100 (valid American odds)
    # 2. implied(over) + implied(under) in [0.95, 1.20]
    # 3. For lines <= 5.0, over must be negative (heavily favored)
    # 4. For lines >= 8.0, under must be negative (heavily favored)

def validate_alt_totals_monotonicity(lines: list[dict]) -> list[dict]:
    """Filter alt totals to enforce monotonicity.
    Over prices must decrease as line decreases.
    Under prices must increase as line decreases.
    Discard violating lines with warning."""

def validate_moneyline(home_ml: float, away_ml: float) -> bool:
    """Check ML pair is valid: implied sum ~1.02-1.15, both valid American."""

def validate_spread_prices(home_price: float, away_price: float) -> bool:
    """Check spread price pair: implied sum ~1.02-1.15."""
```

### Step 2: Source-Level Validation

In each fetcher (`_fetch_hardrock`, `_fetch_draftkings`, etc.), after building alt
totals, validate each (line, over_price, under_price) triple. Discard invalid entries
before they enter the merge. Log discards.

### Step 3: Fix Alt Line Merge — Pair-Based Selection

Replace the independent `max()` approach for alt totals with:

```python
# For each line value, collect (over_price, under_price) PAIRS from each source
# Only accept pairs where implied_pair_valid() passes
# Among valid pairs, pick the one with lowest total vig (best for bettor)
# If no valid pair exists for a line, discard it
```

This ensures over and under prices always come from the same source and represent
the same market. Same approach for alt spreads.

### Step 4: Monotonicity Enforcement

After building the final `all_total_lines`, run `validate_alt_totals_monotonicity()`.
Discard any line that violates the expected price ordering. This catches cross-source
corruption that slipped through Steps 2-3.

### Step 5: Fix Default Values

Change all "no data" defaults from 0.0 to None:
- `best_home_ml`, `best_away_ml` (line 1909-1910)
- `best_home_spread`, `best_away_spread`, prices (line 1943-1944)
- Update the merged dict to use conditional values
- Update the logger to handle None values

### Step 6: DB Write Validation

Before writing to Game record in `sync_odds`, validate:
- ML pair: `validate_moneyline()`
- O/U: `validate_total_line_pair()`
- Spread: `validate_spread_prices()`
- Skip writing invalid fields (preserve existing data)

### Step 7: Structured Per-Source Logging

Replace scattered logging with structured summaries:
```
[HardRock] COL@ANA: ML -200/+165, O/U 6.5 (-145/+120), 8 alt totals, 4 alt spreads
[DraftKings] COL@ANA: ML -195/+160, O/U 6.5 (-140/+115), 12 alt totals
[MERGE] COL@ANA: ML -195/+165, O/U 6.5 (-140/+120), 10 valid alt totals (2 rejected)
```

### Step 8: Utah Team Name Mapping

Verify _COMMON_TEAM_MAP has all variants for Utah (Mammoth, Hockey Club, HC, UHC).

---

## Implementation Order

1. **Step 1** — Validation utilities (foundation for everything else)
2. **Step 5** — None defaults (prevents data corruption, quick win)
3. **Step 3** — Pair-based alt line merge (core fix for the O4.5=+115 bug)
4. **Step 4** — Monotonicity enforcement (catches remaining corruption)
5. **Step 2** — Source-level validation (prevents bad data entering merge)
6. **Step 6** — DB write validation (last safety net)
7. **Step 7** — Structured logging (debuggability)
8. **Step 8** — Utah mapping (quick check)

## Files Modified

- NEW: `backend/app/scrapers/odds_validation.py`
- MODIFIED: `backend/app/scrapers/odds_multi.py` (merge logic, source fetchers, sync)
