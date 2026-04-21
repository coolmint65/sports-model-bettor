"""
Persistent picks cache shared across surfaces.

Why this exists: `backend.server._PICKS_STORE` is a per-process in-memory
dict. On restart, best-bets has to run again before any other surface
(predict, tracker card, POTD) sees consistent picks for today's games.
This module persists the same blobs to each sport's tracker DB so a
restarted server can rehydrate the store from the last run's output
without blocking on a full recompute.

Storage model: one `picks_cache` table per sport DB (mlb, nhl, nba),
keyed by (date, home, away). Values are JSON blobs — the cache is
opaque to the DB; consumers deserialize on read.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def _conn_for(sport: str):
    """Return the tracker DB connection for the sport."""
    if sport == "mlb":
        from .db import get_conn
    elif sport == "nhl":
        from .nhl_db import get_conn
    elif sport == "nba":
        from .nba_db import get_conn
    else:
        raise ValueError(f"unknown sport: {sport}")
    return get_conn()


_DDL = """
CREATE TABLE IF NOT EXISTS picks_cache (
    date        TEXT NOT NULL,
    home        TEXT NOT NULL,
    away        TEXT NOT NULL,
    picks_json  TEXT NOT NULL,
    odds_json   TEXT NOT NULL,
    best_json   TEXT,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (date, home, away)
);
CREATE INDEX IF NOT EXISTS idx_picks_cache_date ON picks_cache(date);
"""


def ensure_table(sport: str) -> None:
    """Idempotent DDL. Safe to call on every put."""
    conn = _conn_for(sport)
    conn.executescript(_DDL)
    conn.commit()


def put(sport: str, home: str, away: str, picks: list, odds: dict,
        best_pick: dict | None, date: str | None = None) -> None:
    """UPSERT a picks blob for (sport, date, home, away). Overwrites
    any existing row so reruns of best-bets land the latest computation."""
    ensure_table(sport)
    conn = _conn_for(sport)
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    try:
        conn.execute(
            "INSERT OR REPLACE INTO picks_cache "
            "(date, home, away, picks_json, odds_json, best_json, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, datetime('now'))",
            (
                target_date, home, away,
                json.dumps(picks or []),
                json.dumps(odds or {}),
                json.dumps(best_pick) if best_pick else None,
            ),
        )
        conn.commit()
    except Exception as e:
        logger.warning("picks_cache.put failed for %s %s@%s: %s",
                       sport, away, home, e)


def get(sport: str, home: str, away: str,
        date: str | None = None) -> dict | None:
    """Fetch a cached picks blob. Returns the memory-store shape
    (``{"picks", "odds", "best_pick"}``) or None when absent."""
    try:
        ensure_table(sport)
        conn = _conn_for(sport)
        target_date = date or datetime.now().strftime("%Y-%m-%d")
        row = conn.execute(
            "SELECT picks_json, odds_json, best_json FROM picks_cache "
            "WHERE date = ? AND home = ? AND away = ?",
            (target_date, home, away),
        ).fetchone()
        if not row:
            return None
        return {
            "picks": json.loads(row["picks_json"]),
            "odds": json.loads(row["odds_json"]),
            "best_pick": json.loads(row["best_json"]) if row["best_json"] else None,
        }
    except Exception as e:
        logger.warning("picks_cache.get failed for %s %s@%s: %s",
                       sport, away, home, e)
        return None


def load_today(sport: str, date: str | None = None) -> list[tuple[str, str, dict]]:
    """Return every cached row for the date as
    ``[(home, away, {"picks", "odds", "best_pick"}), ...]``. Used on
    server startup to rehydrate the in-memory store."""
    try:
        ensure_table(sport)
        conn = _conn_for(sport)
        target_date = date or datetime.now().strftime("%Y-%m-%d")
        rows = conn.execute(
            "SELECT home, away, picks_json, odds_json, best_json "
            "FROM picks_cache WHERE date = ?",
            (target_date,),
        ).fetchall()
        out = []
        for r in rows:
            out.append((
                r["home"], r["away"],
                {
                    "picks": json.loads(r["picks_json"]),
                    "odds": json.loads(r["odds_json"]),
                    "best_pick": json.loads(r["best_json"]) if r["best_json"] else None,
                },
            ))
        return out
    except Exception as e:
        logger.warning("picks_cache.load_today failed for %s: %s", sport, e)
        return []


def prune_before(sport: str, date: str) -> int:
    """Delete rows older than ``date`` (exclusive). Keeps the table
    from growing unbounded over the season. Returns rows deleted."""
    try:
        ensure_table(sport)
        conn = _conn_for(sport)
        cur = conn.execute(
            "DELETE FROM picks_cache WHERE date < ?", (date,),
        )
        conn.commit()
        return cur.rowcount or 0
    except Exception as e:
        logger.warning("picks_cache.prune failed for %s: %s", sport, e)
        return 0
