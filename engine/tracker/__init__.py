"""
Pick tracker - records model picks and settles them against results.

Call record_picks() before games start to log today's picks.
Call settle_picks() after games finish to mark W/L and calculate profit.

Usage:
    python -m engine.tracker --record     # Record today's picks
    python -m engine.tracker --settle     # Settle completed picks
    python -m engine.tracker --summary    # Print running totals

Package layout (split 2026-04-28 from a 1405-line module):
    _helpers     — _core_picks, _compute_clv, _extract_closing_for_pick,
                   _fetch_closing_odds_for_pick, ESPN_BASE constant,
                   _MLB_DERIVATIVE_TYPES set
    _scoreboard  — _fetch_espn_scoreboard (ESPN fallback)
    _record      — record_picks, refresh_pending_for_today,
                   _record_from_scoreboard, capture_closing_odds
    _settle      — settle_picks (per-bet-type outcome math, big function)
    _summary     — get_pick_summary

Public API kept identical to the pre-split module so all
`from engine.tracker import X` callers (sync_*.bat, backend/server.py,
engine/derivative_tracker.py) work unchanged.
"""

from datetime import datetime

# Public API
from ._helpers import (
    _core_picks,
    _compute_clv,
    _extract_closing_for_pick,
    _fetch_closing_odds_for_pick,
    ESPN_BASE,
)
from ._record import (
    record_picks,
    refresh_pending_for_today,
    capture_closing_odds,
    _record_from_scoreboard,
)
from ._scoreboard import _fetch_espn_scoreboard
from ._settle import settle_picks
from ._summary import get_pick_summary

SEASON = datetime.now().year

__all__ = [
    "_core_picks", "_compute_clv", "_extract_closing_for_pick",
    "_fetch_closing_odds_for_pick", "ESPN_BASE", "SEASON",
    "record_picks", "refresh_pending_for_today", "capture_closing_odds",
    "_record_from_scoreboard", "_fetch_espn_scoreboard",
    "settle_picks", "get_pick_summary",
]


# CLI ────────────────────────────────────────────────
# Preserved so `python -m engine.tracker --record / --settle / --summary`
# still works after the package split. When __init__.py is invoked
# directly via -m, __name__ == "engine.tracker", which doesn't match
# "__main__" — the CLI lives in __main__.py instead.
