"""
Late-goalie refresh for NHL — detect announced-starter deltas and
invalidate stale picks. Parallels engine.lineup_refresh for MLB.

Why this exists
---------------
NHL starting goalies drive 40-60% of the win-probability signal. Teams
announce starters 30min-3h pre-puck-drop. If the morning picks assumed
Hellebuyck and the confirmed starter is the backup, the price we
recommended is inverted. This module snapshots starters per game and,
on delta, drops the picks_cache row + unsettled POTD for the affected
game so the next best-bets / predict call regenerates with the
confirmed goalie.

Public API:
  refresh_for_date(date=None, record_picks=True) -> dict summary
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


_DDL = """
CREATE TABLE IF NOT EXISTS goalie_snapshots (
    game_id         INTEGER NOT NULL,
    date            TEXT NOT NULL,
    home_goalie_id  INTEGER,
    away_goalie_id  INTEGER,
    confirmed       INTEGER DEFAULT 0,
    fetched_at      TEXT NOT NULL,
    PRIMARY KEY (game_id)
);
CREATE INDEX IF NOT EXISTS idx_goalie_snap_date ON goalie_snapshots(date);
"""


def _ensure_table() -> None:
    from .nhl_db import get_conn
    conn = get_conn()
    conn.executescript(_DDL)
    conn.commit()


def _fetch_current(game_id: int) -> dict | None:
    try:
        from scrapers.nhl_api import fetch_starting_goalies
        return fetch_starting_goalies(game_id)
    except Exception as e:
        logger.debug("fetch_starting_goalies failed for %s: %s", game_id, e)
        return None


def _load_snapshot(conn, game_id: int) -> dict | None:
    row = conn.execute(
        "SELECT home_goalie_id, away_goalie_id, confirmed "
        "FROM goalie_snapshots WHERE game_id = ?",
        (game_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "home_goalie_id": row["home_goalie_id"],
        "away_goalie_id": row["away_goalie_id"],
        "confirmed": bool(row["confirmed"]),
    }


def _store_snapshot(conn, game_id: int, date: str, current: dict) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO goalie_snapshots "
        "(game_id, date, home_goalie_id, away_goalie_id, confirmed, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, datetime('now'))",
        (game_id, date,
         current.get("home_goalie_id"), current.get("away_goalie_id"),
         1 if current.get("confirmed") else 0),
    )


def _materially_different(prev: dict, current: dict) -> bool:
    """A starter change on either side counts as a delta. We don't
    invalidate when the current fetch has no starter info (feed not
    yet published) since that would churn the cache on every run."""
    if not current.get("confirmed"):
        return False
    if prev is None:
        return False
    prev_h = prev.get("home_goalie_id") or 0
    prev_a = prev.get("away_goalie_id") or 0
    cur_h = current.get("home_goalie_id") or 0
    cur_a = current.get("away_goalie_id") or 0
    return (prev_h != cur_h) or (prev_a != cur_a)


def _invalidate_picks_cache(date: str, home: str, away: str) -> bool:
    try:
        from . import picks_cache
        picks_cache.ensure_table("nhl")
        from .nhl_db import get_conn
        conn = get_conn()
        cur = conn.execute(
            "DELETE FROM picks_cache "
            "WHERE date = ? AND home = ? AND away = ?",
            (date, home, away),
        )
        conn.commit()
        return (cur.rowcount or 0) > 0
    except Exception as e:
        logger.warning("NHL picks_cache invalidation failed for %s @ %s: %s",
                       away, home, e)
        return False


def _invalidate_potd_if_affected(date: str, game_id: int) -> bool:
    """If today's unsettled NHL POTD was built on this game, drop it."""
    try:
        from .nhl_db import get_conn
        conn = get_conn()
        # Lazy DDL in case POTD hasn't been generated yet this season.
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
        if str(row["game_id"] or "") != str(game_id):
            return False
        if row["result"] in ("W", "L", "P"):
            return False
        conn.execute("DELETE FROM pick_of_day WHERE id = ?", (row["id"],))
        conn.commit()
        return True
    except Exception as e:
        logger.warning("NHL POTD invalidation failed for game %s on %s: %s",
                       game_id, date, e)
        return False


def refresh_for_date(date: str | None = None,
                     record_picks: bool = True) -> dict:
    """Snapshot today's announced goalies, detect deltas vs the stored
    snapshot, invalidate picks + POTD for affected games, and optionally
    re-run the NHL tracker so the recorded pick matches the confirmed
    starter. Idempotent — safe to invoke every hour through the day."""
    target_date = date or datetime.now().strftime("%Y-%m-%d")
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
        from .nhl_db import get_conn
        conn = get_conn()
        games = conn.execute(
            "SELECT g.game_id, ht.abbreviation AS home_abbr, "
            "       at.abbreviation AS away_abbr "
            "FROM nhl_games g "
            "LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id "
            "LEFT JOIN nhl_teams at ON g.away_team_id = at.id "
            "WHERE g.date = ?",
            (target_date,),
        ).fetchall()
    except Exception as e:
        summary["errors"].append(f"db query: {e}")
        return summary

    any_delta = False
    for g in games:
        summary["games_checked"] += 1
        game_id = g["game_id"]
        h_abbr = g["home_abbr"]
        a_abbr = g["away_abbr"]

        current = _fetch_current(game_id)
        if current is None or not current.get("confirmed"):
            # Starter not yet announced — try again on the next run.
            continue

        prev = _load_snapshot(conn, game_id)
        if prev is None:
            _store_snapshot(conn, game_id, target_date, current)
            summary["first_snapshot"] += 1
            continue

        if _materially_different(prev, current):
            summary["deltas"] += 1
            any_delta = True
            if h_abbr and a_abbr:
                if _invalidate_picks_cache(target_date, h_abbr, a_abbr):
                    summary["invalidated"] += 1
            if _invalidate_potd_if_affected(target_date, game_id):
                summary["potd_invalidated"] = True

        _store_snapshot(conn, game_id, target_date, current)

    conn.commit()

    if any_delta and record_picks:
        try:
            from . import nhl_tracker
            nhl_tracker.record_picks(date=target_date, force=True)
            summary["re_recorded"] = True
        except Exception as e:
            summary["errors"].append(f"nhl_tracker.record_picks: {e}")
            logger.warning("NHL re-record after goalie delta failed: %s", e)

    if summary["deltas"]:
        logger.info("nhl_goalie_refresh[%s]: %d deltas / %d invalidated "
                    "(checked %d games, first snapshot on %d)",
                    target_date, summary["deltas"], summary["invalidated"],
                    summary["games_checked"], summary["first_snapshot"])
    return summary
