"""
Pick of the Day - single highest-conviction play per sport per day.

Unlike the regular pick tracker (which can record many picks), POTD is
the ONE pick the model is most confident in. It's locked the first time
it's generated for a given date and never changes - so we can measure
whether the model's top-conviction plays are actually its best bets.

Selection criteria:
1. Per-game candidate is the bet's `best_pick` (matches GameCard headline)
2. Cross-game ranking via _score_pick (capped_edge × confidence ×
   reliability × class_factor) — see scoring docstring
3. Must clear MIN_EDGE = 5% and have valid odds
4. NBA splits Q1 / Full views into sibling tables

Storage: dedicated tables in each sport's DB so POTD history is separate
from regular picks and can be summarized independently.

Usage:
    from engine.pick_of_day import get_or_create_potd, settle_potd, get_potd_summary
    potd = get_or_create_potd("mlb", games_with_bets)  # Returns today's POTD
    summary = get_potd_summary("mlb")                  # Running W/L/profit

Package layout (split 2026-04-28 from a single 1517-line module):
    _storage  — schema, connections, table-name helper
    _scoring  — _score_pick + per-market reliability + Kelly fraction
    _select   — select_potd, get_or_create_potd, _build_reasoning, _pick_side
    _refresh  — refresh_potd_for_line_movement, update_potd_closing_odds
    _settle   — settle_potd, _determine_outcome, _profit_on_settled, recalc_potd_profit
    _read     — get_today_potd, get_potd_summary

Public API kept identical to the pre-split module so all
`from engine.pick_of_day import X` callers (sync_*.bat scripts,
backend/server.py, etc.) work unchanged.
"""

# Public API — must match the pre-split module's exports.
from ._storage import (
    _get_conn,
    _ensure_potd_table,
    _potd_table,
    _implied_from_odds,
)
from ._scoring import (
    _score_pick,
    _kelly_fraction,
    _get_market_win_rate,
    _market_class_factor,
    MIN_EDGE,
    MLB_ALLOWED_TYPES_LEGACY,
    NHL_ALLOWED_TYPES_LEGACY,
    NBA_ALLOWED_TYPES_LEGACY,
)
from ._select import (
    select_potd,
    get_or_create_potd,
    _build_reasoning,
    _filter_picks_for_view,
    _pick_side,
)
from ._refresh import (
    refresh_potd_for_line_movement,
    update_potd_closing_odds,
)
from ._settle import (
    settle_potd,
    _determine_outcome,
    _profit_on_settled,
    recalc_potd_profit,
)
from ._read import (
    get_today_potd,
    get_potd_summary,
)


__all__ = [
    # storage
    "_get_conn", "_ensure_potd_table", "_potd_table", "_implied_from_odds",
    # scoring
    "_score_pick", "_kelly_fraction", "_get_market_win_rate",
    "_market_class_factor", "MIN_EDGE",
    "MLB_ALLOWED_TYPES_LEGACY", "NHL_ALLOWED_TYPES_LEGACY",
    "NBA_ALLOWED_TYPES_LEGACY",
    # select
    "select_potd", "get_or_create_potd", "_build_reasoning",
    "_filter_picks_for_view", "_pick_side",
    # refresh
    "refresh_potd_for_line_movement", "update_potd_closing_odds",
    # settle
    "settle_potd", "_determine_outcome", "_profit_on_settled",
    "recalc_potd_profit",
    # read
    "get_today_potd", "get_potd_summary",
]
