"""Soccer framework → unified adapter.

15+ leagues sharing schema at ``data/soccer/{league}.db``. Native_id
is the INT matches.id PK. Bet types: 1X2, ML, DC, DNB, AH, OU, BTTS,
plus H1_ variants.

Soccer's outcome math doesn't share handlers with team-sport
ML/SPREAD/TOTAL — DC has 3 possible winning combos, DNB voids on
draw, BTTS needs both > 0, AH has quarter-line splits. Implemented
inline here so the adapter is self-contained.
"""
from __future__ import annotations

import logging

from ._game_key import GameKey
from ._settler import register_sport
from ._types import Pick, Result, Scope


logger = logging.getLogger(__name__)


def _resolve_state(key: GameKey) -> dict | None:
    try:
        from ..soccer._db import get_conn
    except Exception:
        return None
    conn = get_conn(key.league)
    row = conn.execute(
        "SELECT m.id, m.date, m.status, "
        "       m.home_score, m.away_score, "
        "       m.home_score_ht, m.away_score_ht, "
        "       ht.abbreviation AS home_abbr, "
        "       at.abbreviation AS away_abbr "
        "FROM matches m "
        "JOIN teams ht ON ht.id = m.home_team_id "
        "JOIN teams at ON at.id = m.away_team_id "
        "WHERE m.id = ? AND m.status IN ('final','finished','ft') LIMIT 1",
        (int(key.native_id),),
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
    elif scope == Scope.H1:
        hs, as_ = state.get("home_score_ht"), state.get("away_score_ht")
    elif scope == Scope.H2:
        h_full, a_full = state.get("home_score"), state.get("away_score")
        h_ht, a_ht = state.get("home_score_ht"), state.get("away_score_ht")
        if None in (h_full, a_full, h_ht, a_ht):
            return None
        return int(h_full) - int(h_ht), int(a_full) - int(a_ht)
    else:
        return None
    if hs is None or as_ is None:
        return None
    return int(hs), int(as_)


def _resolve_outcome(pick: Pick, state: dict) -> tuple[Result, float] | None:
    bt_raw = (pick.bet_type or "")
    bt = bt_raw.upper()
    odds = int(pick.odds or 0)
    pick_text = pick.pick_text or ""
    side = (pick.side or "").lower()

    score = _scope_score(pick.scope, state)
    if score is None:
        return None
    hs, as_ = score
    home_abbr = (state.get("home_abbr") or "").upper()
    away_abbr = (state.get("away_abbr") or "").upper()

    # Strip the "H1_" prefix for the bet-type dispatch (H1_OU graded
    # against half-time totals, etc — scope already does the right
    # score lookup).
    canon = bt
    if canon.startswith("H1_"):
        canon = canon[3:]

    # ── 1X2 / ML — pick home/away/draw outright ───────────
    if canon in ("1X2", "ML"):
        if side == "home":
            result = (Result.WIN if hs > as_
                      else Result.LOSS if hs < as_ else Result.LOSS)
        elif side == "away":
            result = (Result.WIN if as_ > hs
                      else Result.LOSS if as_ < hs else Result.LOSS)
        elif side == "draw":
            result = Result.WIN if hs == as_ else Result.LOSS
        else:
            return None
        return result, _profit_for(result, odds)

    # ── DC (double chance) — two of {home, draw, away} ────
    if canon == "DC":
        # side is "home_draw", "draw_away", "home_away"
        won = False
        if side == "home_draw":
            won = hs >= as_
        elif side == "draw_away":
            won = as_ >= hs
        elif side == "home_away":
            won = hs != as_
        result = Result.WIN if won else Result.LOSS
        return result, _profit_for(result, odds)

    # ── DNB (draw no bet) — draw pushes ───────────────────
    if canon == "DNB":
        if hs == as_:
            return Result.PUSH, 0.0
        if side == "home":
            result = Result.WIN if hs > as_ else Result.LOSS
        elif side == "away":
            result = Result.WIN if as_ > hs else Result.LOSS
        else:
            return None
        return result, _profit_for(result, odds)

    # ── OU / TOTAL ────────────────────────────────────────
    if canon in ("OU", "TOTAL"):
        line = pick.line if pick.line is not None else _extract_line(pick_text)
        if line is None:
            return None
        total = hs + as_
        is_over = side == "over" or "OVER" in pick_text.upper()
        if total > line:
            result = Result.WIN if is_over else Result.LOSS
        elif total < line:
            result = Result.LOSS if is_over else Result.WIN
        else:
            result = Result.PUSH
        return result, _profit_for(result, odds)

    # ── BTTS (both teams to score) ────────────────────────
    if canon == "BTTS":
        both = hs > 0 and as_ > 0
        is_yes = side == "yes" or pick_text.lower().startswith("yes")
        result = (Result.WIN if both == is_yes
                  else (Result.WIN if (not both) and (not is_yes) else Result.LOSS))
        # Simplification: yes-and-both = W, yes-and-not-both = L,
        # no-and-not-both = W, no-and-both = L
        result = Result.WIN if (both == is_yes) else Result.LOSS
        return result, _profit_for(result, odds)

    # ── AH (asian handicap) ───────────────────────────────
    if canon == "AH":
        line = pick.line if pick.line is not None else _extract_line(pick_text)
        if line is None:
            return None
        if side == "home":
            margin = hs - as_
        elif side == "away":
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
    register_sport("soccer",
                    state_resolver=_resolve_state,
                    outcome_resolver=_resolve_outcome)


register()
