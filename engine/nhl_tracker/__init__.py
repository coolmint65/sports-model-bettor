"""
NHL Pick tracker - records model picks and settles them against results.

Usage:
    python -m engine.nhl_tracker --record     # Record today's picks
    python -m engine.nhl_tracker --settle     # Settle completed picks
    python -m engine.nhl_tracker --summary    # Print running totals

Package layout (split 2026-04-28 from a 1076-line module):
    _helpers     — _core_picks, _compute_clv, _extract_nhl_closing_for_pick,
                   _get_nhl_db, _NHL_DERIVATIVE_TYPES
    _scoreboard  — _fetch_nhl_scoreboard
    _record      — refresh_pending_for_today, record_picks, capture_closing_odds
    _settle      — settle_picks (full-game + period-specific markets)
    _summary     — get_pick_summary

Public API kept identical to the pre-split module so all
`from engine.nhl_tracker import X` callers (sync_nhl.bat,
backend/server.py, engine/derivative_tracker.py, engine/accuracy.py)
work unchanged.
"""

from ._helpers import (
    _core_picks,
    _compute_clv,
    _extract_nhl_closing_for_pick,
    _get_nhl_db,
)
from ._scoreboard import _fetch_nhl_scoreboard
from ._record import (
    refresh_pending_for_today,
    record_picks,
    capture_closing_odds,
)
from ._settle import settle_picks
from ._summary import get_pick_summary

__all__ = [
    "_core_picks", "_compute_clv", "_extract_nhl_closing_for_pick",
    "_get_nhl_db", "_fetch_nhl_scoreboard",
    "refresh_pending_for_today", "record_picks", "capture_closing_odds",
    "settle_picks", "get_pick_summary",
]
