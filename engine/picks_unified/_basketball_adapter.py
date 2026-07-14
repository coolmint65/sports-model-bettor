"""Basketball framework → unified picks adapter.

Single adapter handling every league registered in
``engine.basketball.LEAGUE_REGISTRY`` EXCEPT NBA (which has its own
adapter with ALT + Live + Props). The framework leagues all share:
  - per-league DB at ``data/basketball/{league}.db``
  - ``games`` table with home_q1..q4, away_q1..q4
  - ``picks`` table with the same shape across leagues

Q1/Full split for WNBA + AFL (the ones that emit Q1 picks). Q-period
math is identical to NBA.
"""
from __future__ import annotations

import logging

from ._game_key import GameKey
from ._settler import register_sport
from ._types import Pick, Result, Scope


logger = logging.getLogger(__name__)


def _resolve_state(key: GameKey) -> dict | None:
    """Resolve from data/basketball/{league}.db."""
    try:
        from ..basketball._db import get_conn, teams_table, games_table
    except Exception as e:
        logger.debug("basketball adapter unavailable: %s", e)
        return None
    league = key.league
    conn = get_conn(league)
    g_tbl = games_table(league)
    t_tbl = teams_table(league)
    row = conn.execute(
        f"SELECT g.*, ht.abbreviation AS home_abbr, "
        f"       at.abbreviation AS away_abbr "
        f"FROM {g_tbl} g "
        f"LEFT JOIN {t_tbl} ht ON g.home_team_id = ht.id "
        f"LEFT JOIN {t_tbl} at ON g.away_team_id = at.id "
        f"WHERE g.game_id = ? AND g.status = 'final' LIMIT 1",
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
        n = scope.value[1]
        hs, as_ = state.get(f"home_q{n}"), state.get(f"away_q{n}")
    elif scope == Scope.H1:
        h1, h2 = state.get("home_q1"), state.get("home_q2")
        a1, a2 = state.get("away_q1"), state.get("away_q2")
        if None in (h1, h2, a1, a2):
            return None
        return int(h1) + int(h2), int(a1) + int(a2)
    elif scope == Scope.H2:
        h_full, a_full = state.get("home_score"), state.get("away_score")
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
    register_sport("basketball",
                    state_resolver=_resolve_state,
                    outcome_resolver=_resolve_outcome)


register()
