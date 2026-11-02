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
        row = _resolve_by_espn_id(conn, key.native_id)
    if not row:
        return None
    return dict(row)


def _resolve_by_espn_id(conn, espn_id):
    """2026-10-23 (MintyBot): the card's paper/shadow rows reach picks_unified keyed by the
    card's ESPN event id (paper_shadow.game_id, e.g. 401908042), NOT MLB's game_pk
    (games.mlb_game_id, e.g. 849805) — so they could never settle (every MLB shadow row
    sat pending since the 10-14 cutover). Map ESPN id -> (date, matchup) via paper_shadow
    -> team abbreviations (alias-tolerant) -> the final games row (±1 day)."""
    try:
        ps = conn.execute(
            "SELECT date, matchup FROM paper_shadow WHERE game_id = ? LIMIT 1",
            (str(espn_id),)).fetchone()
    except Exception:
        return None
    if not ps:
        return None
    date, matchup = ps[0], (ps[1] or "")
    sep = " @ " if " @ " in matchup else "@"
    parts = [x.strip() for x in matchup.split(sep)]
    if len(parts) != 2 or not date:
        return None
    try:
        from ..abbr import aliases_for
        aw = list(dict.fromkeys(list(aliases_for(parts[0], sport="mlb") or []) + [parts[0]]))
        hm = list(dict.fromkeys(list(aliases_for(parts[1], sport="mlb") or []) + [parts[1]]))
    except Exception:
        aw, hm = [parts[0]], [parts[1]]
    q = ("SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr "
         "FROM games g "
         "JOIN teams ht ON g.home_team_id = ht.mlb_id "
         "JOIN teams at ON g.away_team_id = at.mlb_id "
         "WHERE g.status = 'final' "
         f"AND ht.abbreviation IN ({','.join('?' * len(hm))}) "
         f"AND at.abbreviation IN ({','.join('?' * len(aw))}) "
         "AND g.date BETWEEN date(?, '-1 day') AND date(?, '+1 day') "
         "ORDER BY ABS(julianday(g.date) - julianday(?)) LIMIT 1")
    try:
        return conn.execute(q, (*hm, *aw, date, date, date)).fetchone()
    except Exception as e:
        logger.debug("mlb adapter espn fallback failed for %s: %s", espn_id, e)
        return None


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
    # 2026-10-23 (MintyBot): the card's paper layer mirrors with the NEW labels
    # (RUN_LINE / TOTAL / ALT_*) and the number in the separate `line` column
    # (pick_text = "CLE" / "Over"), so these rows sat pending forever and were
    # auto-pushed as stale. Accept both label families + fall back to pick.line.
    if bt in ("RL", "ALT RL", "RUN_LINE", "ALT_RL", "ALT RUN_LINE", "RUNLINE"):
        line = _extract_line(pick_text)
        if line is None and pick.line is not None:
            line = float(pick.line)
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
    if bt in ("O/U", "ALT O/U", "TOTAL", "ALT_TOTAL", "ALT TOTAL"):
        line = _extract_line(pick_text)
        if line is None and pick.line is not None:
            line = float(pick.line)
        if line is None:
            return None
        total = hs + as_
        is_over = ("OVER" in pick_text.upper()) or ((pick.side or "").lower() == "over")
        if total > line:
            result = Result.WIN if is_over else Result.LOSS
        elif total < line:
            result = Result.LOSS if is_over else Result.WIN
        else:
            result = Result.PUSH
        return result, _profit_for(result, odds)

    # ── TEAM_TOTAL (full + F5) / F5_TOTAL ──────────────────
    # 2026-10-25 (MintyBot, Austin "Over 0.5 on which side?"): MLB derivative rows mirror
    # with pick_text '<TEAM> Over' / '<TEAM> F5 Over' (team first, F5 span token, side
    # last) and the number in `line`. Full spans grade on the team's final score; F5 spans
    # on the first five innings of the games.*_linescore JSON the state row already
    # carries (SELECT g.*). No team token / no 5-inning linescore -> None (stays pending),
    # never mis-graded against the wrong span.
    if bt in ("TEAM_TOTAL", "F5_TEAM_TOTAL", "F5_TOTAL"):
        import json as _json
        toks = pick_text.strip().upper().split()
        is_f5 = bt.startswith("F5") or "F5" in toks
        line = (float(pick.line) if pick.line is not None
                else _extract_line(" ".join(t for t in toks if t != "F5")))
        if line is None:
            return None
        side = (pick.side or "").lower()
        is_over = ("OVER" in toks) or side == "over"
        is_under = ("UNDER" in toks) or side == "under"
        if not (is_over or is_under):
            return None

        def _ls(col):
            try:
                v = state.get(col)
                return [int(x or 0) for x in _json.loads(v)] if v else None
            except Exception:
                return None

        if bt == "F5_TOTAL":
            lh, la = _ls("home_linescore"), _ls("away_linescore")
            if not lh or not la or len(lh) < 5 or len(la) < 5:
                return None
            val = sum(lh[:5]) + sum(la[:5])
        else:
            team = toks[0] if toks else ""
            if team == home_abbr:
                full_val, ls = hs, _ls("home_linescore")
            elif team == away_abbr:
                full_val, ls = as_, _ls("away_linescore")
            else:
                return None
            if is_f5:
                if not ls or len(ls) < 5:
                    return None
                val = sum(ls[:5])
            else:
                val = full_val
        if val > line:
            result = Result.WIN if is_over else Result.LOSS
        elif val < line:
            result = Result.LOSS if is_over else Result.WIN
        else:
            result = Result.PUSH
        return result, _profit_for(result, odds)

    # Unknown bet_type — log once, return None so the row stays pending.
    # NRFI / other derivatives land here for now; they'll get ported in
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
