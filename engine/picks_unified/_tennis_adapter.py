"""Tennis → unified picks adapter.

Tennis has its own settler at ``engine.tennis_tracker`` because the
outcome math is genuinely different from team sports (set scores,
total games, set spreads). Rather than reimplement the grading logic
here, the adapter copies the result from the legacy ``tennis_picks``
table where the tennis settler has already graded the match.

This is the "mirror the per-sport settler" pattern from the soccer
POTD path — it keeps the unified table in sync without rebuilding the
sport-specific math.
"""
from __future__ import annotations

import logging

from ._game_key import GameKey
from ._settler import register_sport
from ._types import Pick, Result


logger = logging.getLogger(__name__)


def _resolve_state(key: GameKey) -> dict | None:
    """Return whatever tennis_picks knows about this match's outcome.
    Match state in tennis isn't a "final score" — it's the graded
    result of the bet itself."""
    try:
        from ..tennis_db import get_conn
    except Exception:
        return None
    conn = get_conn()
    rows = conn.execute(
        "SELECT bet_type, pick, result, odds "
        "FROM tennis_picks "
        "WHERE tour=? AND match_id=? AND result IN ('W','L','P') ",
        (key.league, key.native_id),
    ).fetchall()
    if not rows:
        return None
    # Index by (bet_type, pick) for outcome lookup
    return {"by_key": {(r["bet_type"], r["pick"]): dict(r) for r in rows}}


def _profit_for(result: Result, odds: int) -> float:
    if result == Result.WIN:
        return float(odds) if odds > 0 else (
            round(100.0 * 100.0 / abs(odds), 2) if odds else 0.0)
    if result == Result.LOSS:
        return -100.0
    return 0.0


def _resolve_outcome(pick: Pick, state: dict) -> tuple[Result, float] | None:
    by_key = state.get("by_key") or {}
    # Match by (bet_type, pick_text) — exact lookup against tennis_picks.
    key = (pick.bet_type, pick.pick_text)
    legacy = by_key.get(key)
    if not legacy:
        return None
    result_str = legacy.get("result")
    if result_str not in ("W", "L", "P"):
        return None
    result = Result(result_str)
    return result, _profit_for(result, int(pick.odds or 0))


def register() -> None:
    register_sport("tennis",
                    state_resolver=_resolve_state,
                    outcome_resolver=_resolve_outcome)


register()
