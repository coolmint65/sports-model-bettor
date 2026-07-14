"""NHL → unified picks adapter.

NHL Stats API game ids (e.g. 2025030411) are the native_id. Resolves
against ``nhl.db.nhl_games``. Implements ML / O/U / PL (puck line) +
period scopes (P1/P2/P3) where present.
"""
from __future__ import annotations

import logging

from ._game_key import GameKey
from ._settler import register_sport
from ._types import Pick, Result, Scope


logger = logging.getLogger(__name__)


def _resolve_state(key: GameKey) -> dict | None:
    from ..nhl_db import get_conn
    conn = get_conn()
    row = conn.execute(
        "SELECT g.*, ht.abbreviation AS home_abbr, "
        "       at.abbreviation AS away_abbr "
        "FROM nhl_games g "
        "LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id "
        "LEFT JOIN nhl_teams at ON g.away_team_id = at.id "
        "WHERE g.game_id = ? AND g.status = 'final' LIMIT 1",
        (key.native_id,),
    ).fetchone()
    if not row:
        return None
    return dict(row)


def _profit_for(result: Result, odds: int) -> float:
    if result == Result.WIN:
        if odds > 0:
            return float(odds)
        return round(100.0 * 100.0 / abs(odds), 2) if odds else 0.0
    if result == Result.LOSS:
        return -100.0
    return 0.0


def _extract_line(text: str) -> float | None:
    import re
    if not text:
        return None
    m = re.findall(r'-?\d+\.?\d*', text)
    if not m:
        return None
    try:
        return float(m[-1])
    except ValueError:
        return None


def _scope_score(scope: Scope, state: dict) -> tuple[int, int] | None:
    """Return (home, away) scores for the pick's scope."""
    if scope == Scope.FULL:
        hs, as_ = state.get("home_score"), state.get("away_score")
    elif scope == Scope.P1:
        hs, as_ = state.get("home_p1"), state.get("away_p1")
    elif scope == Scope.P2:
        hs, as_ = state.get("home_p2"), state.get("away_p2")
    elif scope == Scope.P3:
        hs, as_ = state.get("home_p3"), state.get("away_p3")
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

    # ── ML / Period Winner / DNB ──────────────────────────
    if bt in ("ML", "P1 ML", "P2 ML", "P3 ML",
              "P1 DNB", "P2 DNB", "P3 DNB",
              "P1 WINNER", "P2 WINNER", "P3 WINNER"):
        team = pick_text.strip().split()[0].upper() if pick_text else ""
        if team == home_abbr:
            result = (Result.WIN if hs > as_
                      else Result.PUSH if hs == as_
                      else Result.LOSS)
        elif team == away_abbr:
            result = (Result.WIN if as_ > hs
                      else Result.PUSH if as_ == hs
                      else Result.LOSS)
        else:
            return None
        return result, _profit_for(result, odds)

    # ── O/U / Period Total / ALT O/U ──────────────────────
    if "TOTAL" in bt or bt in ("O/U", "ALT O/U"):
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

    # ── PL (puck line) / SPREAD ───────────────────────────
    if bt in ("PL", "SPREAD", "ALT PL") or "SPREAD" in bt:
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

    # ── BTS (both teams to score) ─────────────────────────
    if bt.endswith("BTS"):
        both = hs > 0 and as_ > 0
        is_yes = pick_text.strip().lower().startswith("yes")
        if is_yes:
            result = Result.WIN if both else Result.LOSS
        else:
            result = Result.LOSS if both else Result.WIN
        return result, _profit_for(result, odds)

    return None


def register() -> None:
    register_sport(
        "nhl",
        state_resolver=_resolve_state,
        outcome_resolver=_resolve_outcome,
    )


register()
