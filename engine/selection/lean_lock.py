"""Card lean lock (Austin 2026-10-27, Telegram 40146: "I believe this game had a lean, they should stay on
the cards when the game is live").

A card's pick slot (selection.card_lean: the graduated headline, else the ungraduated model lean) used to
be recomputed on every request and only for games that hadn't started -- every sport's best-bets path
skips live/final games -- so the lean vanished at tip-off. Now each card's lean is HELD per game:

  * scheduled: the first lean a card shows is kept (the card-lock rule, Austin 2026-10-06: a line moving
    our way is CLV, not a reason to re-pick). A graduated headline replaces a held lean -- it's the
    system's own pick.
  * live / final: the held lean is what the card keeps showing; a live recompute never reaches the card.
  * final: once the shadow lane (picks_unified, is_shadow=1) settles the same market, its W/L rides along.

Stored in data/card_lean_lock.json so a backend restart mid-game keeps it; entries older than 4 days are
pruned. Display only -- nothing here records or stakes a pick.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time

_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data")
_PATH = os.path.join(_DATA_DIR, "card_lean_lock.json")
_PU_DB = os.path.join(_DATA_DIR, "picks_unified.db")
_TTL_S = 4 * 86400
_MU = threading.Lock()
_DATA = None
_MTIME = None


def _mtime():
    try:
        return os.stat(_PATH).st_mtime_ns
    except OSError:
        return None


def _load():
    """The lock map, re-read whenever the file changed underneath us (a backfill script, or another
    process), so this process never overwrites entries it hasn't seen."""
    global _DATA, _MTIME
    m = _mtime()
    if _DATA is None or m != _MTIME:
        try:
            with open(_PATH, encoding="utf-8") as f:
                _DATA = json.load(f) or {}
        except Exception:
            _DATA = {}
        _MTIME = m
    return _DATA


def _save(d):
    global _MTIME
    now = time.time()
    for k in [k for k, v in d.items() if now - float((v or {}).get("at") or 0) > _TTL_S]:
        d.pop(k, None)
    tmp = _PATH + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, default=str)
        os.replace(tmp, _PATH)
        _MTIME = _mtime()
    except Exception:
        pass


def _key(sport, game_id):
    return f"{sport}|{game_id}"


def _ident(p):
    return ((p or {}).get("bet_type") or (p or {}).get("type"), (p or {}).get("pick") or (p or {}).get("pick_text"))


def _is_headline(p):
    """card_lean's graduated Pick Authority headline (not an ungraduated lean)."""
    return bool(p) and p.get("graduated") is True and p.get("lean") is False


def hold(sport, game_id, lean):
    """Scheduled game: the lean its card should show. The first lean seen is kept; a graduated headline
    replaces a held ungraduated lean (or a different held headline). Returns the held lean -- ``lean``
    itself when nothing is held yet."""
    if game_id is None or game_id == "":
        return lean
    k = _key(sport, game_id)
    with _MU:
        d = _load()
        cur = (d.get(k) or {}).get("lean")
        if lean and (cur is None or (_is_headline(lean) and _ident(lean) != _ident(cur))):
            d[k] = {"lean": dict(lean), "at": time.time()}
            _save(d)
            return lean
        return dict(cur) if cur is not None else lean


def held(sport, game_id):
    """The held lean for a game (a copy), or None."""
    if game_id is None or game_id == "":
        return None
    with _MU:
        cur = (_load().get(_key(sport, game_id)) or {}).get("lean")
    return dict(cur) if cur else None


def flag_graduation(sport, league, p):
    """Stamp a live/final card pick with its cell's CURRENT graduation (Austin 2026-10-29: SAC@NO's
    graduated Under showed "lean · not graduated" once live). Pre-game cards get the flag from best-bets,
    which drops started games; the recorded pick that fills a live card never carried it, so it read as
    ungraduated. Graduation stays live -- a cell that slips below the bar loses the tag mid-day -- so this
    asks edge_proven now rather than trusting the flag frozen at lock time. Mutates and returns ``p``."""
    if not p:
        return p
    try:
        import importlib
        from engine.selection import edge_proven
        sp = importlib.import_module("engine.selection.select_pick")
        bt = sp._bt(p)
        p["graduated"] = bool(edge_proven.is_eligible(
            sport, league, bt, edge_proven._condition(sp._pick_text(p), bt, sp._odds(p))))
    except Exception:
        p["graduated"] = bool(p.get("graduated"))
    p["lean"] = not p["graduated"]
    return p


def held_with_result(sport, league, game_id, game_key=None):
    """held() plus the shadow lane's settled W/L for the same market (picks_unified, is_shadow=1), so a
    final card shows whether its lean won. ``game_key`` overrides the default "<sport>:<league>:<id>"."""
    p = held(sport, game_id)
    if not p or p.get("result"):
        return p
    bt, txt = _ident(p)
    if not bt or not txt:
        return p
    try:
        con = sqlite3.connect(f"file:{_PU_DB}?mode=ro", uri=True, timeout=2)
        try:
            row = con.execute(
                "SELECT result FROM picks WHERE game_key=? AND bet_type=? AND pick_text=? "
                "AND is_shadow=1 AND result IS NOT NULL ORDER BY id LIMIT 1",
                (game_key or f"{sport}:{league}:{game_id}", bt, txt)).fetchone()
        finally:
            con.close()
        if row and row[0]:
            p["result"] = row[0]
    except Exception:
        pass
    if not p.get("result"):
        r = _grade_own(p, game_key or f"{sport}:{league}:{game_id}")
        if r:
            p["result"] = r
    return p


_SCOPE_PREFIX = (("Q1_", "q1"), ("Q2_", "q2"), ("Q3_", "q3"), ("Q4_", "q4"), ("H1_", "h1"), ("H2_", "h2"),
                 ("1H_", "h1"), ("2H_", "h2"), ("P1_", "p1"), ("P2_", "p2"), ("P3_", "p3"))


def _grade_own(p, game_key):
    """Grade the held lean itself against the final, through the sport's own unified-settler adapter
    (Austin 2026-10-30: "only two have grades"). The lean locks at the first line it sees, while the shadow
    lane records whatever line was up when it ran (CBA GUAL +8.5 held vs GUAL +7.5 recorded), so the
    exact-text match above misses. Returns "W"/"L"/"P", or None while the game isn't final or the adapter
    can't read the pick (abstain, never guess)."""
    try:
        import importlib
        from engine.picks_unified._game_key import GameKey
        from engine.picks_unified._settler import _STATE_RESOLVERS, _OUTCOME_RESOLVERS
        from engine.picks_unified._types import Pick
        key = GameKey.parse(game_key)
        if key.sport not in _OUTCOME_RESOLVERS:
            importlib.import_module(f"engine.picks_unified._{key.sport}_adapter")
        state_fn, outcome_fn = _STATE_RESOLVERS.get(key.sport), _OUTCOME_RESOLVERS.get(key.sport)
        if not state_fn or not outcome_fn:
            return None
        bt, txt = _ident(p)
        scope = p.get("scope") or next((sc for pre, sc in _SCOPE_PREFIX if str(bt).upper().startswith(pre)), "full")
        pick = Pick.from_row({
            "sport": key.sport, "league": key.league, "game_key": game_key, "pick_date": "", "matchup": "",
            "scope": scope, "bet_type": bt, "variant": "main", "pick_text": txt, "line": p.get("line"),
            "side": p.get("direction") or p.get("side"), "odds": int(p.get("odds") or 0),
        })
        state = state_fn(key)
        if state is None:
            return None
        out = outcome_fn(pick, state)
        if not out:
            return None
        res = out[0]
        res = getattr(res, "value", res)
        return res if res in ("W", "L", "P") else None
    except Exception:
        return None
