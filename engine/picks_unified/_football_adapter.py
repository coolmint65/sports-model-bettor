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


_PERIOD_RE = __import__("re").compile(r"^(H1|H2|Q1|Q2|Q3|Q4)_(.+)$")


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

    # ── Period markets (2026-10-25): grade on the period's points from games.*_linescore.
    # Before this, "TOTAL" in bt / "SPREAD" in bt matched H1_TOTAL / H1_SPREAD / TEAM_TOTAL
    # and graded them against the FULL-GAME total / margin (wrong answers, confidently).
    _pm = _PERIOD_RE.match(bt)
    if _pm:
        from ..football._tracker import period_points
        pts = period_points(_pm.group(1), state.get("home_linescore"), state.get("away_linescore"))
        if pts is None:
            return None                         # quarter data not stored yet -> stays pending
        hs, as_ = pts
        bt = _pm.group(2)
        if bt == "ML3":
            tok = pick_text.strip().split()[0].upper() if pick_text else ""
            if tok == "TIE":
                won = hs == as_
            elif tok == home_abbr:
                won = hs > as_
            elif tok == away_abbr:
                won = as_ > hs
            else:
                return None
            result = Result.WIN if won else Result.LOSS
            return result, _profit_for(result, odds)

    if bt == "BTTS":                            # "Yes" / "No" / "Yes 20+": both teams >= N points
        toks = pick_text.strip().split()
        if not toks or toks[0].lower() not in ("yes", "no"):
            return None
        thr = int(toks[1].rstrip("+")) if len(toks) > 1 and toks[1].rstrip("+").isdigit() else 1
        both = hs >= thr and as_ >= thr
        won = both if toks[0].lower() == "yes" else not both
        result = Result.WIN if won else Result.LOSS
        return result, _profit_for(result, odds)

    if bt == "LEAD_EVERY_Q":                    # "TEAM Yes" / "TEAM No": ahead after each of Q1..Q4
        import json as _json
        toks = pick_text.strip().split()
        if len(toks) < 2 or toks[1].lower() not in ("yes", "no"):
            return None
        try:
            hl = _json.loads(state.get("home_linescore") or "null"); al = _json.loads(state.get("away_linescore") or "null")
        except Exception:
            return None
        if not hl or not al or len(hl) < 4 or len(al) < 4:
            return None
        ch = ca = 0; led = True; team = toks[0].upper()
        if team not in (home_abbr, away_abbr):
            return None
        for i in range(4):
            ch += int(hl[i] or 0); ca += int(al[i] or 0)
            led = led and ((ch > ca) if team == home_abbr else (ca > ch))
        won = led if toks[1].lower() == "yes" else not led
        result = Result.WIN if won else Result.LOSS
        return result, _profit_for(result, odds)

    if bt == "TEAM_TOTAL":                      # "TEAM Over N" — that team's OWN points
        line = _extract_line(pick_text)
        toks = pick_text.strip().split()
        team = toks[0].upper() if toks else ""
        if line is None or team not in (home_abbr, away_abbr):
            return None
        ts = hs if team == home_abbr else as_
        is_over = "OVER" in pick_text.upper()
        if ts > line:
            result = Result.WIN if is_over else Result.LOSS
        elif ts < line:
            result = Result.LOSS if is_over else Result.WIN
        else:
            result = Result.PUSH
        return result, _profit_for(result, odds)

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

    if bt in ("TOTAL", "ALT_TOTAL", "O/U"):    # GAME total only (TEAM_TOTAL handled above)
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

    if bt in ("SPREAD", "ALT_SPREAD"):
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
