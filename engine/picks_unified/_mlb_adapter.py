"""MLB → unified picks adapter.

Registers two functions with the settler dispatch:
  - State resolver: native_id is the MLB Stats game_pk. Read from
    ``engine.db.get_conn`` (the existing MLB DB).
  - Outcome resolver: dispatches per bet_type. Only the primary markets
    (ML, RL, ALT RL, O/U, ALT O/U) are implemented in this first cut —
    F5/NRFI/Phase-1 derivative grading lives in the existing
    ``engine.tracker._settle`` module and gets ported in Phase 1.1.

Import this module at startup to register MLB with the settler.
"""
from __future__ import annotations

import logging

from ._game_key import GameKey
from ._settler import register_sport
from ._types import Pick, Result


logger = logging.getLogger(__name__)


# ── State resolver ────────────────────────────────────────────


def _resolve_state(key: GameKey) -> dict | None:
    """Look up the MLB game in the existing games table. Returns
    ``{home_score, away_score, status, mlb_game_pk}`` or None when
    the game isn't final yet."""
    from ..db import get_conn
    conn = get_conn()
    row = conn.execute(
        "SELECT g.*, ht.abbreviation AS home_abbr, "
        "       at.abbreviation AS away_abbr "
        "FROM games g "
        "LEFT JOIN teams ht ON g.home_team_id = ht.mlb_id "
        "LEFT JOIN teams at ON g.away_team_id = at.mlb_id "
        "WHERE g.mlb_game_id = ? AND g.status = 'final' LIMIT 1",
        (key.native_id,),
    ).fetchone()
    if not row:
        return None
    return dict(row)


# ── Outcome resolver ──────────────────────────────────────────


def _profit_for(result: Result, odds: int) -> float:
    """$100-unit-basis profit. Recorder/reader multiply by stake_units."""
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


def _resolve_outcome(pick: Pick, state: dict) -> tuple[Result, float] | None:
    bt = (pick.bet_type or "").upper()
    pick_text = pick.pick_text or ""
    odds = int(pick.odds or 0)

    hs = state.get("home_score")
    as_ = state.get("away_score")
    if hs is None or as_ is None:
        return None
    hs = int(hs)
    as_ = int(as_)
    home_abbr = (state.get("home_abbr") or "").upper()
    away_abbr = (state.get("away_abbr") or "").upper()

    # ── ML ────────────────────────────────────────────────
    if bt == "ML":
        pick_team = pick_text.strip().split()[0].upper() if pick_text else ""
        if pick_team == home_abbr:
            result = (Result.WIN if hs > as_
                      else Result.PUSH if hs == as_
                      else Result.LOSS)
        elif pick_team == away_abbr:
            result = (Result.WIN if as_ > hs
                      else Result.PUSH if as_ == hs
                      else Result.LOSS)
        else:
            logger.warning("mlb adapter: unrecognised ML pick team %r "
                           "(home=%s away=%s)", pick_text, home_abbr, away_abbr)
            return None
        return result, _profit_for(result, odds)

    # ── RL / ALT RL ───────────────────────────────────────
    if bt in ("RL", "ALT RL"):
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
        adjusted = margin + line
        if adjusted > 0:
            result = Result.WIN
        elif adjusted == 0:
            result = Result.PUSH
        else:
            result = Result.LOSS
        return result, _profit_for(result, odds)

    # ── O/U / ALT O/U ─────────────────────────────────────
    if bt in ("O/U", "ALT O/U"):
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

    # Unknown bet_type — log once, return None so the row stays pending.
    # F5 / NRFI / derivatives land here for now; they'll get ported in
    # the next pass.
    return None


# ── Registration ──────────────────────────────────────────────


def register() -> None:
    """Idempotent registration. Called by ``engine.picks_unified``'s
    public API the first time a route hits the MLB code path, or
    explicitly at module import."""
    register_sport(
        "mlb",
        state_resolver=_resolve_state,
        outcome_resolver=_resolve_outcome,
    )


# Auto-register on import for now. Test harnesses can call
# ``_settler._STATE_RESOLVERS.clear()`` to undo if needed.
register()
