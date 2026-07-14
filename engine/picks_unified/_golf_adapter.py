"""Golf → unified adapter.

Outright sport — picks are on tournament winners / top-N finishes,
not on matchup outcomes. Native_id is the tournament_id. State resolver
returns the field-entries result; outcome resolver mirrors the legacy
``engine.golf._tracker`` grading by reading the already-settled
``picks`` row from the per-tour DB.
"""
from __future__ import annotations

import logging

from ._game_key import GameKey
from ._settler import register_sport
from ._types import Pick, Result


logger = logging.getLogger(__name__)


def _resolve_state(key: GameKey) -> dict | None:
    try:
        from ..golf._db import get_conn
    except Exception:
        return None
    conn = get_conn(key.league)
    rows = conn.execute(
        "SELECT bet_type, pick, result, odds "
        "FROM picks "
        "WHERE tournament_id=? AND result IN ('W','L','P')",
        (key.native_id,),
    ).fetchall()
    if not rows:
        return None
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
    legacy = by_key.get((pick.bet_type, pick.pick_text))
    if not legacy:
        return None
    result_str = legacy.get("result")
    if result_str not in ("W", "L", "P"):
        return None
    return Result(result_str), _profit_for(Result(result_str), int(pick.odds or 0))


def register() -> None:
    register_sport("golf",
                    state_resolver=_resolve_state,
                    outcome_resolver=_resolve_outcome)


register()
