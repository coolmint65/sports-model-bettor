"""
Live betting tracker — Phase 3c.

Snapshot-locked live picks: when the user clicks "lock" on a live edge
surfaced by ``engine.live.get_live_picks``, that pick is frozen into
``live_picks_nba`` / ``live_picks_nhl`` with the full game state at
lock time (period, clock, score, remaining seconds, market scope).
The pick stays pending until its scope closes:

  - Full-game (ML / SPREAD / TOTAL): game.status == 'final'
  - Quarter market (Q2/Q3/Q4): cur_period > N or game completed
  - Half market (H1): cur_period > 2 or game completed
  - Half market (H2): game completed
  - NHL period (P1/P2/P3): cur_period > N or game completed

When the scope closes, the settler reads actual scores/linescores from
the canonical games table, computes W/L/P + profit at the locked
American odds, and stamps result + settled_at on the row. Mirrors the
prematch tracker shape so the frontend tracker view consumes both via
the same endpoints (Phase 3d adds a "Live" tab/filter).

CLV is intentionally NOT tracked here in v1 — there's no fixed close
for live betting; the line moves continuously. We capture
``locked_at_odds`` and that's our anchor. Phase 4 may add a CLV-style
metric ("how did the model's prob compare to the closing live snapshot
of the same scope") if backtest ROI plateaus.

Public API:
    record_live_pick(sport, pick) → inserts a snapshot row
    settle_live_picks(sport)      → settles whatever's resolvable
    list_history(sport)           → ordered rows for the tracker UI
    list_pending(sport)           → pending picks for refresh badge
"""

from ._record import record_live_pick
from ._settle import settle_live_picks
from ._history import list_history, list_pending
from ._schema import ensure_table

__all__ = [
    "record_live_pick",
    "settle_live_picks",
    "list_history",
    "list_pending",
    "ensure_table",
]
