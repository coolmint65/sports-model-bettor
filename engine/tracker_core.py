"""Cross-sport tracker primitives — same shape ``picks_core`` did for
picks generation.

The three per-sport tracker modules (``engine.tracker``,
``engine.nhl_tracker``, ``engine.nba_tracker``) historically duplicated
~90% identical code for ``capture_closing_odds``, ``refresh_pending_for_today``,
``record_picks``. The NHL "only one pick per game" bug + the NHL CLV
1.1% coverage bug were both shipped because a fix landed in NBA and
nobody mirrored it to NHL.

This module hosts the universal logic; per-sport modules become thin
wrappers that pass a ``SportAdapter`` carrying the per-sport plumbing
(DB connection, table name, HR scraper, closing-odds extractor).

Adding a new gate or fix to a tracker is now a one-line edit here that
applies to every sport, not 3-4 places that drift out of sync.

Public API (exported)
---------------------
- ``SportAdapter``: dataclass carrying per-sport hooks.
- ``core_capture_closing_odds(adapter) -> int``: snapshot HR closing
  odds for all pending picks of the sport.

Future additions
----------------
- ``core_refresh_pending_for_today(adapter, bets, target_date)`` —
  next iteration once the per-sport variations are mapped.
- ``core_record_picks(adapter, date, ...)`` — biggest diff between
  sports; needs careful per-sport hook design.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

logger = logging.getLogger(__name__)


@dataclass
class SportAdapter:
    """Per-sport plumbing the core helpers consume.

    name              human label for log messages ('mlb' / 'nhl' / 'nba')
    get_conn          callable returning a DB connection for the sport
    picks_table       SQL table name (e.g. 'picks' / 'nhl_picks' / 'nba_picks')
    hr_fetch          callable () -> dict[matchup_key → odds]
    extract_closing   callable (bet_type, pick_text, home_abbr, game_odds)
                                -> int | None
    started           optional callable (pick_id) -> bool. True once the pick's
                      game has kicked off: its row is then frozen. For books that
                      keep a started game in the feed (HR soccer lists matches
                      IN-PLAY), so the "close" can't be an in-play price.
    """
    name: str
    get_conn: Callable
    picks_table: str
    hr_fetch: Callable
    extract_closing: Callable
    started: Callable | None = None


def _cc_now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _implied_am(o) -> float:
    """American odds -> implied probability (incl. vig)."""
    o = float(o)
    return abs(o) / (abs(o) + 100.0) if o < 0 else 100.0 / (o + 100.0)


def core_capture_closing_odds(adapter: SportAdapter) -> int:
    """Snapshot current Hard Rock odds for all pending picks of the
    sport. Returns the number of rows updated (new + refreshed).

    Refresh semantics:
    - Pending row with NULL closing_odds → first stamp.
    - Pending row with existing closing_odds that has changed → refresh.
    - Pending row whose game isn't in HR's current feed (game already
      started) → leave whatever closing_odds is there. The CLV pipeline
      (engine.live_worker hook + cron capture) is the source of truth
      for prematch snapshots; stale post-game stamps are not.

    Drop-in replacement for the per-sport ``capture_closing_odds``
    functions. Thin per-sport wrapper just calls this with the
    adapter.
    """
    conn = adapter.get_conn()
    # CLV needs to know a close was captured PRE-GAME to trust it -- stamp
    # when (edge-hunt). Column added per-sport; gate so sports without it
    # keep working.
    _has_ts = any(d[1] == "closing_odds_captured_at"
                  for d in conn.execute(f"PRAGMA table_info({adapter.picks_table})"))
    pending = conn.execute(
        f"SELECT id, matchup, bet_type, pick, closing_odds, odds "
        f"FROM {adapter.picks_table} WHERE result IS NULL"
    ).fetchall()
    if not pending:
        return 0

    try:
        all_odds = adapter.hr_fetch() or {}
    except Exception as e:
        logger.warning("%s closing capture: HR fetch failed: %s",
                       adapter.name.upper(), e)
        return 0
    if not all_odds:
        logger.info("%s closing capture: HR returned no games",
                    adapter.name.upper())
        return 0

    from .picks import match_odds as _match_odds
    updated = 0
    refreshed = 0
    for pick in pending:
        pick = dict(pick)
        if adapter.started is not None:
            try:
                if adapter.started(pick["id"]):
                    continue
            except Exception:
                continue   # can't tell if it kicked off -> never risk an in-play stamp
        matchup = pick["matchup"]
        sep = " @ " if " @ " in matchup else "@"
        parts = matchup.split(sep)
        if len(parts) != 2:
            continue
        away, home = parts[0].strip(), parts[1].strip()
        game_odds = _match_odds(home, away, all_odds)
        if not game_odds:
            continue
        try:
            closing = adapter.extract_closing(
                pick["bet_type"], pick["pick"], home, game_odds,
            )
        except Exception as e:
            logger.debug("%s extract_closing failed for %s/%s: %s",
                         adapter.name, matchup, pick["bet_type"], e)
            continue
        if closing is None:
            continue
        # HR ships sentinel "market locked/settled" placeholders like
        # -100000 or +500000 when a market is unavailable (game about
        # to start, or already tipped). Those aren't real closing odds;
        # persisting them corrupts CLV calculation. Real American odds
        # stay inside ±3000; a hard ±5000 gate rejects the sentinels
        # without touching any legitimate longshot.
        try:
            closing_int = int(closing)
        except (TypeError, ValueError):
            continue
        if abs(closing_int) < 100 or abs(closing_int) > 5000:
            continue
        # Side-sanity guard (Austin 2026-10-14 CLV fix): reject a closing that is
        # implausible vs the pick's ENTRY odds -- a sign flip (dog<->fav) or a
        # >15pp implied move is nearly always a MIS-CAPTURE (HR home/away couldn't
        # be verified, so match_odds returned the OPPONENT's line -- the fake +5.3pp
        # NHL ML CLV where away picks captured the home team's closing).
        _entry = pick.get("odds")
        if _entry:
            try:
                if abs(_implied_am(closing_int) - _implied_am(int(_entry))) > 0.15:
                    logger.debug("%s CLV reject implausible close: %s entry=%s close=%s",
                                 adapter.name, pick.get("pick"), _entry, closing_int)
                    continue
            except Exception:
                pass
        prior = pick.get("closing_odds")
        if prior is None:
            if _has_ts:
                conn.execute(
                    f"UPDATE {adapter.picks_table} SET closing_odds = ?, "
                    f"closing_odds_captured_at = ? WHERE id = ?",
                    (closing_int, _cc_now_iso(), pick["id"]),
                )
            else:
                conn.execute(
                    f"UPDATE {adapter.picks_table} SET closing_odds = ? "
                    f"WHERE id = ?",
                    (closing_int, pick["id"]),
                )
            updated += 1
        elif int(prior) != closing_int:
            if _has_ts:
                conn.execute(
                    f"UPDATE {adapter.picks_table} SET closing_odds = ?, "
                    f"closing_odds_captured_at = ? WHERE id = ?",
                    (closing_int, _cc_now_iso(), pick["id"]),
                )
            else:
                conn.execute(
                    f"UPDATE {adapter.picks_table} SET closing_odds = ? "
                    f"WHERE id = ?",
                    (closing_int, pick["id"]),
                )
            refreshed += 1

    conn.commit()
    logger.info("%s closing capture: %d new, %d refreshed (out of %d pending)",
                adapter.name.upper(), updated, refreshed, len(pending))
    return updated + refreshed


__all__ = [
    "SportAdapter",
    "core_capture_closing_odds",
]
