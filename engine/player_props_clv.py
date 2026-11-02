"""
Dedicated player-props closing-odds capture (CLV).

Props were the ONLY market whose closing line was captured as a side
effect of pick RE-GENERATION: ``engine.player_props_db.insert_pick``
refreshes ``closing_odds`` whenever ``generate_picks`` re-sights an
already-stored pick. That tied CLV capture to the +EV value gates (edge
floor, odds caps, the Over-only filter, the slate top-N cap) -- so a
pick's close was written only if the pick would STILL be selected as a
value bet at close. Short-odds Under value (Skater SOG / Points /
Goalie Saves) keeps re-qualifying and captured ~80%; long-odds,
Over-only or marginal markets (Skater Assists / Goals / Goalie Goals
Against) decay out of qualification by close and captured ~0%.

This module captures the close for EVERY pending prop pick: it matches
each stored pick to the live Hard Rock board by
``(player_name, bet_type, line, side)`` and writes the current price,
with NO value gating. It mirrors the dedicated capture every other
market already owns (``engine.tennis_clv``, ``engine.soccer._clv``,
``engine.basketball._clv``, ``engine.hockey._clv``). The generation
re-sighting path in ``insert_pick`` is left untouched, so SOG / Points
keep capturing exactly as before -- this only fills the gaps.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from .player_props_db import _conn_for, _ts_now

logger = logging.getLogger(__name__)

# sport -> Hard Rock props scraper fn name. Lazy-resolved in
# _fetch_board so one sport's scraper import error can't break another.
_FETCHER: dict[str, str] = {
    "nhl": "fetch_nhl_props",
    "nba": "fetch_nba_props",
    "mlb": "fetch_mlb_props",
    # NFL props v2 (2026-10-26): two-sided ALT + MULTILINE ladders in the same
    # {matchup: [rows]} shape, so a pick's line is usually still on the board at close.
    "nfl": "fetch_nfl_props",
}


def _norm(s: str | None) -> str:
    """Collapse whitespace + lowercase a player name for matching. Both
    the stored pick and the board row carry the HR name verbatim (the
    pick took player_name/bet_type/line/side from this same board at
    insert time), so a normalized exact match is reliable."""
    return " ".join((s or "").strip().lower().split())


def _fetch_board(sport: str) -> dict:
    fn = _FETCHER.get(sport)
    if not fn:
        return {}
    try:
        from scrapers import hardrock_props as _hp
        return getattr(_hp, fn)() or {}
    except Exception as e:  # network / auth / shape -- log and skip
        logger.warning("player_props CLV: board fetch failed for %s: %s",
                       sport, e)
        return {}


def _board_index(board: dict) -> dict:
    """``(norm player_name, bet_type, line) -> {'Over': odds, 'Under': odds}``.

    Keyed off the exact HR fields ``generate_picks`` reads, so a stored
    pick matches by exact (normalized) name + bet_type + float line, and
    the stored ``side`` selects the price. Both alt and main lines land
    in the index because each board row already carries its own line.
    """
    idx: dict = {}
    for rows in board.values():
        for p in rows:
            line = p.get("line")
            if line is None:
                continue
            try:
                key = (_norm(p.get("player_name")),
                       p.get("bet_type", ""), float(line))
            except (TypeError, ValueError):
                continue
            d = idx.setdefault(key, {})
            oo, uo = p.get("over_odds"), p.get("under_odds")
            if oo is not None:
                d["Over"] = int(oo)
            if uo is not None:
                d["Under"] = int(uo)
    return idx


def capture_closing_odds(sport: str) -> int:
    """Write the current HR price as ``closing_odds`` for every pending
    prop pick that matches the live board. Returns rows updated.

    Self-gating: no pending (ungraded) pick for a recent date -> returns
    0 without fetching the board. Overwrites on each call so the last
    price seen before the game grades out is the captured close -- same
    "last refresh before tip is the closing snapshot" semantics as the
    insert_pick re-sighting path, but without its value gates. Every
    write is guarded by ``result IS NULL`` and only touches picks found
    on the current board (a pick whose game already started/finished is
    no longer on the pre-match board, so it is never mis-stamped).
    """
    if sport not in _FETCHER:
        return 0
    conn = _conn_for(sport)
    # Restrict to picks whose game is plausibly today/upcoming so a stale
    # ungraded row can't match a coincidental same (player, line) on a
    # later slate. The board only ever holds current/near events anyway.
    since = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        pend = conn.execute(
            "SELECT id, player_name, bet_type, line, side "
            "FROM player_props_picks "
            "WHERE result IS NULL AND date >= ? "
            "AND bet_type IS NOT NULL AND line IS NOT NULL "
            "AND side IS NOT NULL",
            (since,),
        ).fetchall()
    except Exception as e:
        logger.warning("player_props CLV: pending query failed for %s: %s",
                       sport, e)
        return 0
    if not pend:
        return 0
    board = _fetch_board(sport)
    if not board:
        return 0
    idx = _board_index(board)
    if not idx:
        return 0
    _has_ts = any(
        d[1] == "closing_odds_captured_at"
        for d in conn.execute("PRAGMA table_info(player_props_picks)")
    )
    ts = _ts_now()
    n = 0
    for row in pend:
        try:
            line = float(row["line"])
        except (TypeError, ValueError):
            continue
        sides = idx.get((_norm(row["player_name"]), row["bet_type"], line))
        if not sides:
            continue
        odds = sides.get((row["side"] or "").strip().capitalize())
        if odds is None:
            continue
        try:
            if _has_ts:
                conn.execute(
                    "UPDATE player_props_picks SET closing_odds = ?, "
                    "closing_odds_captured_at = ? "
                    "WHERE id = ? AND result IS NULL",
                    (int(odds), ts, row["id"]))
            else:
                conn.execute(
                    "UPDATE player_props_picks SET closing_odds = ? "
                    "WHERE id = ? AND result IS NULL",
                    (int(odds), row["id"]))
            n += 1
        except Exception as e:
            logger.warning("player_props CLV: write failed for %s pick=%s: %s",
                           sport, row["id"], e)
    if n:
        conn.commit()
    return n


__all__ = ["capture_closing_odds"]