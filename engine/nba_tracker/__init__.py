"""
NBA Pick Tracker -- records model picks and settles them against results.

Q1 picks settle the moment Q1 ends; full-game ML/SPREAD/TOTAL/ALT
markets gate on game completion.

Usage:
    python -m engine.nba_tracker --record           # Record today's picks
    python -m engine.nba_tracker --capture-closing  # Snapshot closing odds
    python -m engine.nba_tracker --settle           # Settle completed picks
    python -m engine.nba_tracker --summary          # Print running totals

Package layout (split 2026-04-28 from a 1002-line module):
    _helpers     — _core_picks + _compute_clv +
                   _extract_nba_closing_for_pick + _normalize_espn_abbr
                   + _ALT_ABBRS + _ESPN_TO_INTERNAL_ABBR +
                   _NBA_DERIVATIVE_TYPES
    _scoreboard  — _fetch_nba_scoreboard + _parse_q1_scores
    _record      — refresh_pending_for_today + record_picks +
                   capture_closing_odds (Q1/Full family-aware)
    _settle      — settle_picks (Q1 + full-game + Q1 derivatives)
    _summary     — get_pick_summary + get_nba_pick_history

Public API kept identical to the pre-split module so all
`from engine.nba_tracker import X` callers (sync_nba.bat,
backend/server.py, engine/derivative_tracker.py, engine/nba_diagnose.py)
work unchanged.
"""

from ._helpers import (
    _core_picks, _compute_clv, _extract_nba_closing_for_pick,
    _normalize_espn_abbr, _ALT_ABBRS, _ESPN_TO_INTERNAL_ABBR,
    _NBA_DERIVATIVE_TYPES,
)
from ._scoreboard import _fetch_nba_scoreboard, _parse_q1_scores
from ._record import (
    refresh_pending_for_today, record_picks, capture_closing_odds,
)
from ._settle import settle_picks
from ._summary import get_pick_summary, get_nba_pick_history

# nba_-prefixed aliases used by backend/server.py — the routes import
# sport-prefixed names by convention so endpoint code reads clearly.
record_nba_picks = record_picks
settle_nba_picks = settle_picks
get_nba_pick_summary = get_pick_summary

__all__ = [
    "_core_picks", "_compute_clv", "_extract_nba_closing_for_pick",
    "_normalize_espn_abbr", "_ALT_ABBRS", "_ESPN_TO_INTERNAL_ABBR",
    "_fetch_nba_scoreboard", "_parse_q1_scores",
    "refresh_pending_for_today", "record_picks", "capture_closing_odds",
    "settle_picks", "get_pick_summary", "get_nba_pick_history",
    # Aliases
    "record_nba_picks", "settle_nba_picks", "get_nba_pick_summary",
]
