# Plan: Auto-Settlement & Smarter Bet Grading

## Current State

Settlement has **three separate grading implementations** that are all manually triggered:

1. **`PredictionManager._grade_prediction()`** — grades `Prediction` → `BetResult` records, called only when `/predictions/stats` endpoint is hit
2. **`settle_tracked_bets()` API route** — grades `TrackedBet` records, called only when user clicks "Settle" button on History page
3. **`Backtester._check_outcome()`** — grades in backtest context only

**Closing odds snapshots** already work — `nhl_api.py` copies live odds to `closing_*` fields when a game goes final. CLV computation exists in `_get_closing_implied_prob()` but is rarely populated because grading is manual.

## What We're Building

A `SettlementService` that runs automatically when games go final, consolidating grading into one place.

## Implementation Steps

### Step 1: Create `backend/app/services/grading.py`

Extract shared grading logic from the 3 duplicate implementations into one source of truth:

- `check_outcome(bet_type, prediction_value, game) -> Optional[bool]` — determines if a bet won
- `compute_profit_loss(was_correct, odds, units) -> float` — odds-aware P/L calculation
- `determine_actual_outcome(game, bet_type, prediction_value) -> Optional[str]` — for BetResult records

Then update `PredictionManager`, `settle_tracked_bets` route, and `Backtester` to call these shared functions.

### Step 2: Create `backend/app/services/settlement.py`

A new service with a single public function: `settle_completed_games(session) -> dict`.

This function:
1. Queries for final games with unsettled predictions or tracked bets
2. **Grades all `Prediction` records** → creates `BetResult` rows with `was_correct`, `profit_loss`, `clv`, `closing_implied_prob`
3. **Settles all `TrackedBet` records** → sets `result`, `profit_loss`, `settled_at`
4. Returns summary: `{"predictions_graded": N, "tracked_bets_settled": N}`

### Step 3: Hook into the scheduler

In `live.py`'s `_scheduler_loop()`, call `settle_completed_games()` after each data sync cycle. This is natural because the sync is what updates game statuses to "final".

Broadcast a `settlements_update` WebSocket event when bets are settled.

### Step 4: Simplify the `/tracked/settle` endpoint

Change it from doing its own grading to calling `settle_completed_games()`. Manual button still works as fallback, same code path.

### Step 5: Frontend — auto-refresh on settlement

- Add listener for `settlements_update` WebSocket event to trigger History refetch
- Change "Settle Bets" button to just trigger a refetch (or remove it)
- Existing P/L chart and stats update automatically

### Step 6: Add confidence-tier ROI breakdown

Enhance the `/predictions/stats` response to include ROI grouped by confidence tier (50-60%, 60-70%, 70%+), validating model calibration.

## Files Changed

| File | Change |
|------|--------|
| `backend/app/services/grading.py` | **NEW** — shared grading functions |
| `backend/app/services/settlement.py` | **NEW** — auto-settlement service |
| `backend/app/live.py` | Hook settlement into scheduler loop |
| `backend/app/api/predictions.py` | Simplify `/tracked/settle` to use service; remove duplicate grading |
| `backend/app/analytics/predictions.py` | Use shared grading from `grading.py` |
| `backend/app/analytics/backtest.py` | Use shared grading from `grading.py` |
| `frontend/src/components/History.jsx` | Auto-refresh on settlement events |

## What We're NOT Doing

- No push detection for half-point lines (already impossible with .5 lines in NHL)
- No streak detection or advanced analytics (future work)
- No changes to prediction generation or model logic
- No new database columns or migrations needed
