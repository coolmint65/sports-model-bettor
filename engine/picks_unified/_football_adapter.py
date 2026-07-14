"""Football framework → unified adapter (UFL today, NFL/NCAAF later)."""
from __future__ import annotations

import logging

from ._game_key import GameKey
from ._settler import register_sport
from ._types import Pick, Result, Scope


logger = logging.getLogger(__name__)


def _resolve_state(key: GameKey) -> dict | None:
    try:
        from ..football._db import get_conn
    except Exception:
        return None
    conn = get_conn(key.league)
    row = conn.execute(
        "SELECT g.*, ht.abbreviation AS home_abbr, "
        "       at.abbreviation AS away_abbr "
        "FROM games g "
        "LEFT JOIN teams ht ON ht.id = g.home_team_id "
        "LEFT JOIN teams at ON at.id = g.away_team_id "
        "WHERE g.game_id = ? AND g.status = 'final' LIMIT 1",
        (key.native_id,),
    ).fetchone()
    return dict(row) if row else None


def _profit_for(result: Result, odds: int) -> float:
    if result == Result.WIN:
        return float(odds) if odds > 0 else (
            round(100.0 * 100.0 / abs(odds), 2) if odds else 0.0)
    if result == Result.LOSS:
        return -100.0
    return 0.0


def _extract_line(text: str) -> float | None:
    import re
    m = re.findall(r'-?\d+\.?\d*', text or "")
    if not m:
        return None
    try:
        return float(m[-1])
    except ValueError:
        return None


def _resolve_outcome(pick: Pick, state: dict) -> tuple[Result, float] | None:
    bt = (pick.bet_type or "").upper()
    odds = int(pick.odds or 0)
    pick_text = pick.pick_text or ""
    hs = state.get("home_score")
    as_ = state.get("away_score")
    if hs is None or as_ is None:
        return None
    hs, as_ = int(hs), int(as_)
    home_abbr = (state.get("home_abbr") or "").upper()
    away_abbr = (state.get("away_abbr") or "").upper()

    if bt == "ML":
        team = pick_text.strip().split()[0].upper() if pick_text else ""
        if team == home_abbr:
            result = (Result.WIN if hs > as_
                      else Result.PUSH if hs == as_ else Result.LOSS)
        elif team == away_abbr:
            result = (Result.WIN if as_ > hs
                      else Result.PUSH if as_ == hs else Result.LOSS)
        else:
            return None
        return result, _profit_for(result, odds)

    if "TOTAL" in bt:
        line = _extract_line(pick_text)
        if line is None:
            return None
        total = hs + as_
        is_over = "OVER" in pick_text.upper()
        if total > line:
            result = Result.WIN if is_over else Result.LOSS
        elif total < line:
            result = Result.LOSS if is_over else Result.WIN
        else:
            result = Result.PUSH
        return result, _profit_for(result, odds)

    if "SPREAD" in bt or bt == "PL":
        line = _extract_line(pick_text)
        if line is None:
            return None
        first = pick_text.strip().split()[0].upper()
        if first == home_abbr:
            margin = hs - as_
        elif first == away_abbr:
            margin = as_ - hs
        else:
            return None
        adj = margin + line
        if adj > 0:
            result = Result.WIN
        elif adj == 0:
            result = Result.PUSH
        else:
            result = Result.LOSS
        return result, _profit_for(result, odds)

    return None


def register() -> None:
    register_sport("football",
                    state_resolver=_resolve_state,
                    outcome_resolver=_resolve_outcome)


register()
