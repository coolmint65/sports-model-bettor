"""Per-league baseball DB schema + thread-local connection cache.

Mirrors engine.football._db exactly — thread-local conns so the
FastAPI request path doesn't race on the same sqlite3 handle across
threads (see feedback_thread_local_conns.md for the why).
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from . import LEAGUE_REGISTRY, get_league_config


_thread_local = threading.local()
_locks: dict[str, threading.Lock] = {}


_DDL = """
CREATE TABLE IF NOT EXISTS teams (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    abbreviation    TEXT NOT NULL,
    short_name      TEXT,
    location        TEXT,
    logo_url        TEXT,
    updated_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_teams_abbr ON teams(abbreviation);

CREATE TABLE IF NOT EXISTS games (
    game_id         TEXT PRIMARY KEY,
    date            TEXT NOT NULL,
    start_time      TEXT,
    home_team_id    INTEGER NOT NULL,
    away_team_id    INTEGER NOT NULL,
    home_score      INTEGER,
    away_score      INTEGER,
    status          TEXT,
    season          INTEGER,
    venue           TEXT,
    is_postseason   INTEGER DEFAULT 0,
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (home_team_id) REFERENCES teams(id),
    FOREIGN KEY (away_team_id) REFERENCES teams(id)
);
CREATE INDEX IF NOT EXISTS idx_games_date ON games(date);
CREATE INDEX IF NOT EXISTS idx_games_status ON games(status);

CREATE TABLE IF NOT EXISTS picks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT NOT NULL,
    game_id         TEXT NOT NULL,
    matchup         TEXT,
    bet_type        TEXT NOT NULL,      -- ML / SPREAD / TOTAL
    pick            TEXT NOT NULL,
    model_prob      REAL,
    edge            REAL,
    odds            INTEGER,
    closing_odds    INTEGER,
    result          TEXT,
    profit          REAL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    settled_at      TEXT,
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);
-- Pick dedupe — FULL UNIQUE (no `WHERE result IS NULL` partial). The
-- partial released once a row settled, which let the recorder spam
-- the same family on every slate hit (NBA Q1_TOTAL saw 25+ copies of
-- the same pick before this was hardened across-the-board 2026-06-10).
CREATE UNIQUE INDEX IF NOT EXISTS uq_picks_family
    ON picks(date, game_id, bet_type, pick);
CREATE INDEX IF NOT EXISTS idx_picks_date ON picks(date);
CREATE INDEX IF NOT EXISTS idx_picks_game ON picks(game_id);

-- V3.2 signal log — every prediction writes 3 rows (ML, SPREAD, TOTAL)
-- so signal_explain has data to surface per pick.
CREATE TABLE IF NOT EXISTS prediction_signals (
    id              INTEGER PRIMARY KEY,
    sport           TEXT,
    league          TEXT,
    game_id         TEXT,
    game_date       TEXT,
    home_team_id    INTEGER,
    away_team_id    INTEGER,
    market          TEXT,
    factor_prob     REAL,
    mc_prob         REAL,
    gbm_prob        REAL,
    captured_at     TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_ps_game
    ON prediction_signals(game_id, market);
CREATE INDEX IF NOT EXISTS idx_ps_captured
    ON prediction_signals(captured_at);
"""


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_DDL)
    conn.commit()


def get_conn(league: str) -> sqlite3.Connection:
    """Per-thread cached sqlite3 connection. First-touch schema-ensure
    is serialized under a per-league lock; thereafter each thread has
    its own connection."""
    cache = getattr(_thread_local, "conns", None)
    if cache is None:
        cache = {}
        _thread_local.conns = cache
    if league in cache:
        return cache[league]
    cfg = get_league_config(league)
    path = Path(cfg["db_path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    lock = _locks.setdefault(league, threading.Lock())
    with lock:
        _ensure_schema(conn)
    cache[league] = conn
    return conn
