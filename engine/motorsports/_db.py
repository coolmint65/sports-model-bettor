"""Per-series SQLite layer for the motorsports framework.

Each series gets its own ``data/motorsports/{series}.db``. Schema is
shared across series because the entities (driver, team, race, result)
are universal — only the ingest path and predictor specifics differ.

Thread-local connection cache + WAL — same pattern as
``engine.basketball._db`` / ``engine.hockey._db``.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

from ._config import get_series_config

logger = logging.getLogger(__name__)

_local = threading.local()


def get_conn(series: str) -> sqlite3.Connection:
    """Thread-local DB connection for ``series``. Creates the file +
    schema on first call."""
    cache = getattr(_local, "conns", None)
    if cache is None:
        cache = {}
        _local.conns = cache
    conn = cache.get(series)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except sqlite3.Error:
            try:
                conn.close()
            except sqlite3.Error:
                pass
            cache.pop(series, None)

    cfg = get_series_config(series)
    db_path = Path(cfg["db_path"])
    if not db_path.is_absolute():
        db_path = (Path(__file__).resolve().parent.parent.parent / db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    _init_schema(conn)
    cache[series] = conn
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    """Universal motorsports schema. Driver/team/race/result tables
    follow Ergast vocabulary (qualifying_pos, finish_pos, status) so the
    ingest can map field-by-field without translation.

    ``ergast_id`` (driver/team/race) is the source-system identifier so
    backfills are idempotent. HR's internal driver ID is held in
    ``hr_ext_id`` on the drivers table when the odds parser resolves
    a name match — saves re-resolving every refresh."""
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS teams (
        id           INTEGER PRIMARY KEY,
        name         TEXT NOT NULL,
        abbreviation TEXT,
        ergast_id    TEXT UNIQUE,
        nationality  TEXT
    );

    CREATE TABLE IF NOT EXISTS drivers (
        id           INTEGER PRIMARY KEY,
        name         TEXT NOT NULL,         -- "Max Verstappen"
        abbreviation TEXT,                  -- "VER" (Ergast 'code')
        team_id      INTEGER,               -- current team
        ergast_id    TEXT UNIQUE,           -- "max_verstappen"
        hr_ext_id    TEXT,                  -- HR's internal driver ID, set lazily
        nationality  TEXT,
        FOREIGN KEY (team_id) REFERENCES teams(id)
    );
    CREATE INDEX IF NOT EXISTS idx_drivers_hr_ext ON drivers(hr_ext_id);

    CREATE TABLE IF NOT EXISTS races (
        race_id      TEXT PRIMARY KEY,      -- "{season}-{round}"
        season       INTEGER NOT NULL,
        round        INTEGER NOT NULL,
        name         TEXT NOT NULL,         -- "Canadian Grand Prix"
        circuit      TEXT,                  -- "Circuit Gilles Villeneuve"
        country      TEXT,
        race_date    TEXT NOT NULL,         -- ISO date YYYY-MM-DD
        race_time    TEXT,                  -- ISO datetime UTC (qualifying-cutoff aware)
        status       TEXT DEFAULT 'scheduled',  -- scheduled|complete|cancelled
        ergast_id    TEXT,
        hr_event_id  TEXT,                  -- HR's event ID once odds match
        circuit_wiki_url   TEXT,            -- Wikipedia page URL (from Ergast)
        circuit_image_url  TEXT,            -- Wikimedia track-layout thumbnail
        UNIQUE (season, round)
    );
    CREATE INDEX IF NOT EXISTS idx_races_date ON races(race_date);
    CREATE INDEX IF NOT EXISTS idx_races_status ON races(status);

    CREATE TABLE IF NOT EXISTS race_results (
        race_id          TEXT NOT NULL,
        driver_id        INTEGER NOT NULL,
        team_id          INTEGER,
        qualifying_pos   INTEGER,           -- grid position (1 = pole)
        finish_pos       INTEGER,           -- final classification (NULL if DNF/DSQ)
        status           TEXT,              -- "Finished"|"+1 Lap"|"Engine"|"Accident"|...
        laps             INTEGER,
        fastest_lap_rank INTEGER,           -- 1 = fastest lap of race
        points           REAL,
        PRIMARY KEY (race_id, driver_id),
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        FOREIGN KEY (driver_id) REFERENCES drivers(id)
    );
    CREATE INDEX IF NOT EXISTS idx_results_driver ON race_results(driver_id);

    -- Picks table mirrors the team-sports shape (date, market, prob,
    -- edge, odds, result, profit) so unified_tracker can ingest motor-
    -- sports picks without a separate adapter. ``pick`` is the driver
    -- abbreviation ("VER") for human-readable display; driver_id is
    -- the join key to drivers/results.
    CREATE TABLE IF NOT EXISTS picks (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        date          TEXT NOT NULL,
        race_id       TEXT NOT NULL,
        driver_id     INTEGER NOT NULL,
        bet_type      TEXT NOT NULL,        -- 'WINNER' | 'PODIUM'
        pick          TEXT NOT NULL,        -- driver abbreviation
        model_prob    REAL,
        edge          REAL,
        odds          INTEGER,              -- American odds
        result        TEXT,                 -- 'W'|'L'|'P'|NULL
        profit        REAL,
        closing_odds  INTEGER,
        created_at    TEXT DEFAULT (datetime('now')),
        settled_at    TEXT,
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        FOREIGN KEY (driver_id) REFERENCES drivers(id)
    );
    CREATE INDEX IF NOT EXISTS idx_picks_date ON picks(date);
    CREATE INDEX IF NOT EXISTS idx_picks_race ON picks(race_id);
    CREATE INDEX IF NOT EXISTS idx_picks_result ON picks(result);

    CREATE TABLE IF NOT EXISTS model_config (
        key   TEXT PRIMARY KEY,
        value TEXT
    );
    """)
    # Idempotent column adds for older DBs created before these
    # columns existed. PRAGMA + maybe one ALTER per cold open — cheap.
    _add_column_if_missing(conn, "races", "circuit_wiki_url", "TEXT")
    _add_column_if_missing(conn, "races", "circuit_image_url", "TEXT")
    _add_column_if_missing(conn, "picks", "stake_units", "REAL")
    _add_column_if_missing(conn, "picks", "closing_odds", "INTEGER")
    conn.commit()


def _add_column_if_missing(conn, table: str, column: str, col_type: str) -> None:
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


def close_all() -> None:
    """Close every cached connection on this thread. Tests use this."""
    cache = getattr(_local, "conns", None)
    if not cache:
        return
    for conn in cache.values():
        try:
            conn.close()
        except sqlite3.Error:
            pass
    _local.conns = {}
