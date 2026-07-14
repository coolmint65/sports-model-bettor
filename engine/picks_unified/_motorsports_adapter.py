"""Motorsports → unified adapter (F1, IndyCar, NASCAR).

Outright sport — picks are on race winners / podium finishes. Same
mirror pattern as tennis/golf: state resolver returns the already-
graded results from the legacy per-series picks table.
"""
from __future__ import annotations

import logging

from ._game_key import GameKey
from ._settler import register_sport
from ._types import Pick, Result


logger = logging.getLogger(__name__)


def _resolve_state(key: GameKey) -> dict | None:
    try:
        from ..motorsports._db import get_conn
    except Exception:
        return None
    conn = get_conn(key.league)
    rows = conn.execute(
        "SELECT bet_type, pick, result, odds "
        "FROM picks "
        "WHERE race_id=? AND result IN ('W','L','P')",
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
    rs = legacy.get("result")
    if rs not in ("W", "L", "P"):
        return None
    return Result(rs), _profit_for(Result(rs), int(pick.odds or 0))


def register() -> None:
    register_sport("motorsports",
                    state_resolver=_resolve_state,
                    outcome_resolver=_resolve_outcome)


register()
