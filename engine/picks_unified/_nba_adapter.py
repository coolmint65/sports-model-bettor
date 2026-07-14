"""NBA → unified picks adapter.

ESPN event ids as native_id. Resolves against ``nba.db.nba_games``.
Handles Q1/Q2/Q3/Q4 + Full scopes, ML/SPREAD/TOTAL + ALT variants.
"""
from __future__ import annotations

import logging

from ._game_key import GameKey
from ._settler import register_sport
from ._types import Pick, Result, Scope


logger = logging.getLogger(__name__)


def _resolve_state(key: GameKey) -> dict | None:
    from ..nba_db import get_conn
    conn = get_conn()
    row = conn.execute(
        "SELECT g.*, ht.abbreviation AS home_abbr, "
        "       at.abbreviation AS away_abbr "
        "FROM nba_games g "
        "LEFT JOIN nba_teams ht ON g.home_team_id = ht.id "
        "LEFT JOIN nba_teams at ON g.away_team_id = at.id "
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


def _scope_score(scope: Scope, state: dict) -> tuple[int, int] | None:
    if scope == Scope.FULL:
        hs, as_ = state.get("home_score"), state.get("away_score")
    elif scope in (Scope.Q1, Scope.Q2, Scope.Q3, Scope.Q4):
        n = scope.value[1]  # '1' / '2' / '3' / '4'
        hs, as_ = state.get(f"home_q{n}"), state.get(f"away_q{n}")
    elif scope == Scope.H1:
        h1, h2 = state.get("home_q1"), state.get("home_q2")
        a1, a2 = state.get("away_q1"), state.get("away_q2")
        if None in (h1, h2, a1, a2):
            return None
        return int(h1) + int(h2), int(a1) + int(a2)
    elif scope == Scope.H2:
        h_full = state.get("home_score")
        a_full = state.get("away_score")
        h1, h2 = state.get("home_q1"), state.get("home_q2")
        a1, a2 = state.get("away_q1"), state.get("away_q2")
        if None in (h_full, a_full, h1, h2, a1, a2):
            return None
        return (int(h_full) - int(h1) - int(h2),
                int(a_full) - int(a1) - int(a2))
    else:
        return None
    if hs is None or as_ is None:
        return None
    return int(hs), int(as_)


def _resolve_outcome(pick: Pick, state: dict) -> tuple[Result, float] | None:
    bt = (pick.bet_type or "").upper()
    odds = int(pick.odds or 0)
    pick_text = pick.pick_text or ""

    score = _scope_score(pick.scope, state)
    if score is None:
        return None
    hs, as_ = score
    home_abbr = (state.get("home_abbr") or "").upper()
    away_abbr = (state.get("away_abbr") or "").upper()

    if bt.endswith("ML"):
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

    if "SPREAD" in bt:
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
    register_sport("nba",
                    state_resolver=_resolve_state,
                    outcome_resolver=_resolve_outcome)


register()
