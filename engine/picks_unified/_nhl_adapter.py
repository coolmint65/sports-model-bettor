"""NHL -> unified picks adapter.

Resolves game state from the ESPN scoreboard (the SAME canonical source
the native settler uses), keyed by the ESPN event id the pick's game_key
carries. The old implementation queried ``nhl.db.nhl_games`` by that id,
but nhl_games is keyed by NHL-API ids (e.g. 2026020165) -- a different
namespace from the ESPN ids (e.g. 401892548) in game_key -- so it always
returned None. The old markets (ML/O/U/PL/P1-Total) dodged this because
they settle natively and get mirrored; the shadow-only markets (Period
Spread / Period 3-Way / Regulation 3-Way) had no native row and so never
graded, then false-pushed at the 7-day stale cutoff.

Implements ML / O/U / PL (puck line), period scopes (P1/P2/P3), and the
extra markets (Regulation 3-Way, Period 3-Way, Period Spread). Readiness
is encoded in the resolved state: full-game scores appear only once the
game is completed; a period's scores appear only once that period is
locked -- the outcome resolver returns None (pending) otherwise.
"""
from __future__ import annotations

import logging
import re
import time

from ._game_key import GameKey
from ._settler import register_sport
from ._types import Pick, Result, Scope


logger = logging.getLogger(__name__)


# ESPN reports non-standard abbrs for a few teams (SJ, TB, LA, NJ, UTA)
# while picks may carry the league-standard ones (SJS, TBL, LAK, NJD,
# UTAH) or vice-versa. Canonicalise both sides before comparing so those
# teams grade instead of abstaining -> false-pushing (same bug the native
# settler fixed with _ABBR_ALIASES / _resolve_abbr).
_ALIAS_GROUPS = [{"SJ", "SJS"}, {"TB", "TBL"}, {"LA", "LAK"},
                 {"NJ", "NJD"}, {"UTA", "UTAH"}]


def _canon(abbr) -> str:
    a = str(abbr or "").strip().upper()
    for grp in _ALIAS_GROUPS:
        if a in grp:
            return sorted(grp)[0]
    return a


# ── ESPN scoreboard map (cached) ──────────────────────────────
_SB_CACHE: dict = {"ts": 0.0, "map": {}}
_SB_TTL_S = 120.0


def _pending_nhl_dates() -> list:
    """Dates (YYYY-MM-DD) with pending NHL picks in the unified table,
    each widened by +/-1 day for UTC drift, plus today +/- 1. Bounds the
    ESPN scoreboard fetch to only what might settle. Picks older than the
    settler's 7-day stale cutoff get auto-pushed before they ever reach
    here, so the window stays small."""
    from datetime import datetime, timedelta, timezone
    from ._schema import get_conn
    out: set = set()
    try:
        rows = get_conn().execute(
            "SELECT DISTINCT pick_date FROM picks "
            "WHERE sport='nhl' AND result IS NULL AND pick_date IS NOT NULL"
        ).fetchall()
    except Exception:
        rows = []
    for r in rows:
        try:
            d = datetime.strptime(str(r[0])[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        for off in (-1, 0, 1):
            out.add((d + timedelta(days=off)).isoformat())
    today = datetime.now(timezone.utc).date()
    for off in (-1, 0):
        out.add((today + timedelta(days=off)).isoformat())
    return sorted(out)


def _sb_map() -> dict:
    """ESPN game-state map {espn_id: state}, rebuilt at most every
    _SB_TTL_S. On fetch failure keeps the last good map."""
    now = time.time()
    if _SB_CACHE["map"] and (now - _SB_CACHE["ts"]) < _SB_TTL_S:
        return _SB_CACHE["map"]
    try:
        from ..nhl_tracker._scoreboard import build_game_state_map
        m = build_game_state_map(_pending_nhl_dates())
    except Exception as e:
        logger.warning("nhl unified settler: scoreboard map build failed: %s", e)
        m = _SB_CACHE["map"] or {}
    _SB_CACHE["map"] = m
    _SB_CACHE["ts"] = now
    return m


def _resolve_state(key: GameKey) -> dict | None:
    g = _sb_map().get(str(key.native_id))
    if not g:
        return None
    st: dict = {
        "home_abbr": g.get("home_abbr") or "",
        "away_abbr": g.get("away_abbr") or "",
        "home_forms": g.get("home_forms") or [],
        "away_forms": g.get("away_forms") or [],
        "went_to_ot": g.get("went_to_ot", False),
    }
    # Full-game scores only once the game is completed.
    if g.get("is_completed"):
        st["home_score"] = g.get("home_score")
        st["away_score"] = g.get("away_score")
    # Period scores only for periods that are locked (final).
    pl = g.get("periods_locked") or 0
    hp = g.get("home_periods") or []
    ap = g.get("away_periods") or []
    for n in (1, 2, 3):
        if pl >= n and len(hp) >= n and len(ap) >= n:
            st[f"home_p{n}"] = hp[n - 1]
            st[f"away_p{n}"] = ap[n - 1]
    return st


def _profit_for(result: Result, odds: int) -> float:
    if result == Result.WIN:
        if odds > 0:
            return float(odds)
        return round(100.0 * 100.0 / abs(odds), 2) if odds else 0.0
    if result == Result.LOSS:
        return -100.0
    return 0.0


def _extract_line(text: str) -> float | None:
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
    # Live start-of-period picks are FULL-GAME markets recorded at period n ("LIVE P3 O/U" = the game total, read
    # at the start of P3): grade them like the pre-game market. Without this strip they matched nothing and all
    # 65 live picks sat pending (MintyBot 2026-11-01, Austin "none of the 65 live picks have settled").
    bt = re.sub(r"^LIVE P\d+\s+", "", bt)
    odds = int(pick.odds or 0)
    pick_text = pick.pick_text or ""
    home_abbr = _canon(state.get("home_abbr"))
    away_abbr = _canon(state.get("away_abbr"))

    # ── NHL extra markets (MintyBot 2026-10-18) — grade from PERIOD scores,
    #    independent of inferred scope. Regulation = sum P1-P3 (excludes OT). ──
    def _per(p):
        h = state.get(f"home_p{p}"); a = state.get(f"away_p{p}")
        return (int(h), int(a)) if h is not None and a is not None else None

    if bt == "REGULATION 3-WAY":
        legs = [_per(p) for p in (1, 2, 3)]
        if any(x is None for x in legs):
            return None
        rh = sum(h for h, a in legs); ra = sum(a for h, a in legs)
        sel = pick_text.replace("REG", "", 1).strip().upper()
        if sel in ("TIE", "DRAW"):
            result = Result.WIN if rh == ra else Result.LOSS
        elif _canon(sel) == home_abbr:
            result = Result.WIN if rh > ra else Result.LOSS
        elif _canon(sel) == away_abbr:
            result = Result.WIN if ra > rh else Result.LOSS
        else:
            return None
        return result, _profit_for(result, odds)

    if bt == "PERIOD 3-WAY":
        m = re.match(r"P([123])\s+(.+)", pick_text.strip())
        if not m:
            return None
        leg = _per(m.group(1))
        if leg is None:
            return None
        ph, pa = leg; sel = m.group(2).strip().upper()
        if sel in ("TIE", "DRAW"):
            result = Result.WIN if ph == pa else Result.LOSS
        elif _canon(sel) == home_abbr:
            result = Result.WIN if ph > pa else Result.LOSS
        elif _canon(sel) == away_abbr:
            result = Result.WIN if pa > ph else Result.LOSS
        else:
            return None
        return result, _profit_for(result, odds)

    if bt == "PERIOD SPREAD":
        m = re.match(r"P([123])\s+(\S+)\s+([+-]?\d+\.?\d*)", pick_text.strip())
        if not m:
            return None
        leg = _per(m.group(1))
        if leg is None:
            return None
        ph, pa = leg; team = m.group(2).upper(); point = float(m.group(3))
        if _canon(team) == home_abbr:
            margin = ph - pa
        elif _canon(team) == away_abbr:
            margin = pa - ph
        else:
            return None
        adj = margin + point
        result = (Result.WIN if adj > 0 else Result.PUSH if adj == 0 else Result.LOSS)
        return result, _profit_for(result, odds)

    score = _scope_score(pick.scope, state)
    if score is None:
        return None
    hs, as_ = score

    # ── ML / Period Winner / DNB ──────────────────────────
    if bt in ("ML", "P1 ML", "P2 ML", "P3 ML",
              "P1 DNB", "P2 DNB", "P3 DNB",
              "P1 WINNER", "P2 WINNER", "P3 WINNER"):
        team = pick_text.strip().split()[0].upper() if pick_text else ""
        if _canon(team) == home_abbr:
            result = (Result.WIN if hs > as_
                      else Result.PUSH if hs == as_
                      else Result.LOSS)
        elif _canon(team) == away_abbr:
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
        if _canon(first) == home_abbr:
            margin = hs - as_
        elif _canon(first) == away_abbr:
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
