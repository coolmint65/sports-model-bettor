"""
Late-lineup refresh — detect lineup deltas vs the morning snapshot and
invalidate picks for affected games so the next best-bets run recomputes
with the confirmed lineup.

Why this exists
---------------
MLB picks compute at ~9 AM against projected lineups. Confirmed
lineups drop 2-3 hours before first pitch and often differ (day-of IL
moves, late scratches, lineup shuffles). The morning picks then price
the wrong team — we're giving a -140 recommendation on a team whose
cleanup hitter just went on paternity leave. This module re-reads the
confirmed lineup, compares against the stored snapshot, and when the
starting nine has changed materially it nukes the `picks_cache` entry
for that game. The next `/api/best-bets` call (or the scheduled
tracker re-record) then regenerates picks with the real lineup.

Scheduled use
-------------
Designed to be invoked on a cron / Windows Task Scheduler run 2-3x
during game day (e.g. at T-3h, T-2h, T-90m before first pitch).
Idempotent: if no deltas, no-op. Safe to over-invoke.

Public API:
  refresh_for_date(date=None, record_picks=True) -> dict
    summary = {games_checked, deltas, invalidated, re_recorded, errors}
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any
from ._tz import et_today_str

logger = logging.getLogger(__name__)


_DDL = """
CREATE TABLE IF NOT EXISTS lineup_snapshots (
    game_id        INTEGER NOT NULL,
    date           TEXT NOT NULL,
    home_lineup    TEXT NOT NULL,   -- JSON list of player IDs in order
    away_lineup    TEXT NOT NULL,
    home_pitcher_id INTEGER,
    away_pitcher_id INTEGER,
    fetched_at     TEXT NOT NULL,
    confirmed      INTEGER DEFAULT 0,  -- 1 once batting order is non-empty
    PRIMARY KEY (game_id)
);
CREATE INDEX IF NOT EXISTS idx_lineup_date ON lineup_snapshots(date);
"""


def _ensure_table() -> None:
    from .db import get_conn
    conn = get_conn()
    conn.executescript(_DDL)
    conn.commit()


def _lineup_ids(lineup_side: list[dict]) -> list[int]:
    return [int(p.get("id") or 0) for p in (lineup_side or []) if p.get("id")]


def _fetch_current(game_pk: int) -> dict | None:
    try:
        from scrapers.mlb_stats import fetch_game_lineups
        return fetch_game_lineups(game_pk)
    except Exception as e:
        logger.debug("fetch_game_lineups failed for %s: %s", game_pk, e)
        return None


def _load_snapshot(conn, game_id: int) -> dict | None:
    row = conn.execute(
        "SELECT home_lineup, away_lineup, home_pitcher_id, away_pitcher_id, "
        "       confirmed FROM lineup_snapshots WHERE game_id = ?",
        (game_id,),
    ).fetchone()
    if not row:
        return None
    try:
        return {
            "home": json.loads(row["home_lineup"]),
            "away": json.loads(row["away_lineup"]),
            "home_pitcher_id": row["home_pitcher_id"],
            "away_pitcher_id": row["away_pitcher_id"],
            "confirmed": bool(row["confirmed"]),
        }
    except Exception:
        return None


def _store_snapshot(conn, game_id: int, date: str, current: dict,
                    h_pid: int | None, a_pid: int | None) -> None:
    home_ids = _lineup_ids(current.get("home_lineup"))
    away_ids = _lineup_ids(current.get("away_lineup"))
    confirmed = 1 if (home_ids and away_ids) else 0
    conn.execute(
        "INSERT OR REPLACE INTO lineup_snapshots "
        "(game_id, date, home_lineup, away_lineup, "
        " home_pitcher_id, away_pitcher_id, fetched_at, confirmed) "
        "VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?)",
        (game_id, date, json.dumps(home_ids), json.dumps(away_ids),
         h_pid, a_pid, confirmed),
    )


def _materially_different(prev: dict, current: dict,
                          h_pid_now: int | None, a_pid_now: int | None) -> bool:
    """Return True when the stored snapshot's starter IDs differ from
    the current fetch. A starter change on either side counts — we don't
    try to distinguish a leadoff shuffle from a cleanup change because
    the model's expected-runs call pulls from the DB's team_stats plus
    pitcher inputs, both of which react to any lineup member changing
    their slot or being replaced entirely. Probable-pitcher changes also
    count (different arm = different prediction)."""
    if prev is None:
        return False  # Nothing to compare against; first snapshot isn't a delta.
    prev_home = prev.get("home") or []
    prev_away = prev.get("away") or []
    cur_home = _lineup_ids(current.get("home_lineup"))
    cur_away = _lineup_ids(current.get("away_lineup"))
    # If current lineup came back empty (game too far out / feed hasn't
    # published), skip — we don't treat "no data" as a delta.
    if not cur_home and not cur_away:
        return False
    if cur_home != prev_home:
        return True
    if cur_away != prev_away:
        return True
    if (prev.get("home_pitcher_id") or 0) != (h_pid_now or 0):
        return True
    if (prev.get("away_pitcher_id") or 0) != (a_pid_now or 0):
        return True
    return False


def _invalidate_picks_cache(game_matchup: tuple[str, str, str]) -> bool:
    """Delete today's picks_cache row for (date, home, away). Returns
    True if a row was deleted."""
    date, home, away = game_matchup
    try:
        from . import picks_cache
        picks_cache.ensure_table("mlb")
        from .db import get_conn
        conn = get_conn()
        cur = conn.execute(
            "DELETE FROM picks_cache "
            "WHERE date = ? AND home = ? AND away = ?",
            (date, home, away),
        )
        conn.commit()
        return (cur.rowcount or 0) > 0
    except Exception as e:
        logger.warning("picks_cache invalidation failed for %s @ %s: %s",
                       away, home, e)
        return False


def _invalidate_potd_if_affected(date: str, game_pk: int) -> bool:
    """If today's MLB POTD was built on this game, drop it so the next
    POTD generation re-selects from the fresh picks. Skips settled
    POTDs — those are historical and must not be rewritten."""
    try:
        from .db import get_conn
        conn = get_conn()
        # Ensure table exists — pick_of_day creates it lazily on first
        # use, so an isolated lineup refresh before any POTD run would
        # otherwise raise.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pick_of_day (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL UNIQUE, game_id TEXT,
                matchup TEXT NOT NULL, bet_type TEXT NOT NULL,
                pick TEXT NOT NULL, model_prob REAL, edge REAL,
                odds INTEGER, kelly_pct REAL, reasoning TEXT,
                result TEXT, profit REAL,
                created_at TEXT DEFAULT (datetime('now')), settled_at TEXT
            )
        """)
        row = conn.execute(
            "SELECT id, game_id, result FROM pick_of_day WHERE date = ?",
            (date,),
        ).fetchone()
        if not row:
            return False
        # game_id column is TEXT; mlb_game_id from the games table is
        # INTEGER. Compare as strings to avoid missing a match on types.
        stored_id = str(row["game_id"] or "")
        if stored_id != str(game_pk):
            return False
        if row["result"] in ("W", "L", "P"):
            # Already settled — leave it alone.
            return False
        conn.execute("DELETE FROM pick_of_day WHERE id = ?", (row["id"],))
        conn.commit()
        return True
    except Exception as e:
        logger.warning("POTD invalidation failed for game %s on %s: %s",
                       game_pk, date, e)
        return False


def refresh_for_date(date: str | None = None,
                     record_picks: bool = True) -> dict:
    """Walk every scheduled MLB game for the date, compare the current
    lineup to the stored snapshot, invalidate picks for games whose
    lineup has changed, and optionally re-run the tracker so the
    recorded pick for today reflects the confirmed lineup.

    Returns a summary dict suitable for logging or a status endpoint."""
    target_date = date or et_today_str()
    summary: dict[str, Any] = {
        "date": target_date,
        "games_checked": 0,
        "first_snapshot": 0,
        "deltas": 0,
        "invalidated": 0,
        "potd_invalidated": False,
        "re_recorded": False,
        "errors": [],
    }

    try:
        _ensure_table()
        from .db import get_conn
        conn = get_conn()
        games = conn.execute(
            "SELECT g.mlb_game_id, g.home_pitcher_id, g.away_pitcher_id, "
            "       ht.abbreviation AS home_abbr, "
            "       at.abbreviation AS away_abbr "
            "FROM games g "
            "LEFT JOIN teams ht ON g.home_team_id = ht.mlb_id "
            "LEFT JOIN teams at ON g.away_team_id = at.mlb_id "
            "WHERE g.date = ? AND g.mlb_game_id IS NOT NULL",
            (target_date,),
        ).fetchall()
    except Exception as e:
        summary["errors"].append(f"db query: {e}")
        return summary

    any_delta = False
    for g in games:
        summary["games_checked"] += 1
        game_pk = g["mlb_game_id"]
        h_abbr = g["home_abbr"]
        a_abbr = g["away_abbr"]
        h_pid = g["home_pitcher_id"]
        a_pid = g["away_pitcher_id"]

        current = _fetch_current(game_pk)
        if current is None:
            continue

        prev = _load_snapshot(conn, game_pk)
        if prev is None:
            # First snapshot for this game: store and move on.
            _store_snapshot(conn, game_pk, target_date, current, h_pid, a_pid)
            summary["first_snapshot"] += 1
            continue

        if _materially_different(prev, current, h_pid, a_pid):
            summary["deltas"] += 1
            any_delta = True
            if h_abbr and a_abbr:
                if _invalidate_picks_cache((target_date, h_abbr, a_abbr)):
                    summary["invalidated"] += 1
            # If this game was the POTD, drop it so the next POTD run
            # picks from the regenerated best-bets list. Settled POTDs
            # are preserved (see helper).
            if _invalidate_potd_if_affected(target_date, game_pk):
                summary["potd_invalidated"] = True
            # Update the stored snapshot so subsequent runs compare
            # against the confirmed lineup, not the stale morning one.
            _store_snapshot(conn, game_pk, target_date, current, h_pid, a_pid)
        else:
            # No delta, but refresh the fetched_at timestamp so callers
            # can see the snapshot is still current.
            _store_snapshot(conn, game_pk, target_date, current, h_pid, a_pid)

    conn.commit()

    # Re-run the nightly pick recorder IF we invalidated anything AND
    # the caller asked for it. Uses force=True to overwrite the stale
    # pick rows recorded under the morning lineup.
    if any_delta and record_picks:
        try:
            from . import tracker
            tracker.record_picks(date=target_date, force=True)
            summary["re_recorded"] = True
        except Exception as e:
            summary["errors"].append(f"record_picks: {e}")
            logger.warning("record_picks re-run failed: %s", e)

    if summary["deltas"]:
        logger.info("lineup_refresh[%s]: %d deltas / %d invalidated "
                    "(checked %d games, first snapshot on %d)",
                    target_date, summary["deltas"], summary["invalidated"],
                    summary["games_checked"], summary["first_snapshot"])
    return summary
