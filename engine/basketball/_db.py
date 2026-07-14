"""Per-league SQLite layer for the basketball framework.

Each league gets its own ``data/basketball/<league>.db`` with a uniform
schema (``teams`` / ``games`` / ``picks`` — no league prefix). The
existing NBA stays at ``data/nba.db`` with the historical
``nba_teams`` / ``nba_games`` schema; the framework reads NBA through a
compatibility shim so the cutover doesn't churn ~24 NBA modules.

Thread-local connections + WAL mirror the per-sport pattern used in
engine.nba_db / engine.nhl_db / engine.tennis_db.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

from ._config import LEAGUE_REGISTRY, get_league_config

logger = logging.getLogger(__name__)


# Per-(league, thread) connection cache. Threads can safely share the
# framework but each holds its own SQLite handle to satisfy SQLite's
# threading rules.
_local = threading.local()

# NBA uses the legacy schema (`nba_teams`, `nba_games`); every other
# league uses the framework's uniform schema (`teams`, `games`). The
# shim is per-league at table-name resolution time, not a separate
# code path.
_LEGACY_SCHEMA_LEAGUES = {"nba"}


def get_conn(league: str) -> sqlite3.Connection:
    """Thread-local DB connection for ``league``. Creates the file +
    schema on first call. NBA falls through to its existing ``data/nba.db``
    so no migration is required."""
    cache = getattr(_local, "conns", None)
    if cache is None:
        cache = {}
        _local.conns = cache
    conn = cache.get(league)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except sqlite3.Error:
            try:
                conn.close()
            except sqlite3.Error:
                pass
            cache.pop(league, None)

    cfg = get_league_config(league)
    db_path = Path(cfg["db_path"])
    if not db_path.is_absolute():
        db_path = (Path(__file__).resolve().parent.parent.parent / db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    if league not in _LEGACY_SCHEMA_LEAGUES:
        _init_uniform_schema(conn)
    cache[league] = conn
    return conn


def teams_table(league: str) -> str:
    """Resolve the teams table name for ``league``. NBA uses
    ``nba_teams``; all framework-managed leagues use ``teams``."""
    return "nba_teams" if league == "nba" else "teams"


def games_table(league: str) -> str:
    """Resolve the games table name for ``league``. NBA uses
    ``nba_games``; all framework-managed leagues use ``games``."""
    return "nba_games" if league == "nba" else "games"


def picks_table(league: str) -> str:
    """Resolve the picks table name for ``league``. NBA uses
    ``nba_picks``; all framework-managed leagues use ``picks``."""
    return "nba_picks" if league == "nba" else "picks"


def _init_uniform_schema(conn: sqlite3.Connection) -> None:
    """Schema for non-NBA basketball leagues. Mirrors the NBA shape but
    drops the league prefix so the framework's SQL is identical across
    every league. NBA-specific columns (Q1 splits) preserved because
    international leagues will eventually accumulate quarter splits when
    we ingest them, even if we don't yet model period markets.

    external_id convention (per #314): SHOULD be the source system's
    native ID for cross-referencing. ESPN ingests use ESPN event IDs
    (which equal our internal game_id since ESPN's IDs are stable +
    int-friendly). RealGM doesn't expose IDs on schedule rows so we
    use our hashed game_id as a placeholder. Euroleague stores the
    upstream gamecode separately. theScore-backed paths set the
    field to the theScore event ID. Operators reading external_id
    should not assume one specific source format."""
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS teams (
        id           INTEGER PRIMARY KEY,
        name         TEXT NOT NULL,
        abbreviation TEXT NOT NULL,
        city         TEXT,
        venue        TEXT,
        external_id  TEXT,
        logo_url     TEXT     -- curated logo for non-ESPN leagues
    );

    CREATE TABLE IF NOT EXISTS games (
        game_id        TEXT PRIMARY KEY,
        date           TEXT NOT NULL,
        start_time     TEXT,           -- ISO-8601 UTC tipoff time
        home_team_id   INTEGER,
        away_team_id   INTEGER,
        home_score     INTEGER,
        away_score     INTEGER,
        home_q1        INTEGER, away_q1 INTEGER,
        home_q2        INTEGER, away_q2 INTEGER,
        home_q3        INTEGER, away_q3 INTEGER,
        home_q4        INTEGER, away_q4 INTEGER,
        status         TEXT DEFAULT 'scheduled',
        season         INTEGER,
        home_pace      REAL,
        away_pace      REAL,
        external_id    TEXT,
        game_type      TEXT             -- 'regular' | 'playoff' | 'preseason'
    );
    CREATE INDEX IF NOT EXISTS idx_games_date ON games(date);
    CREATE INDEX IF NOT EXISTS idx_games_status ON games(status);

    -- Best-effort migration: add game_type to existing tables that
    -- pre-date this column. SQLite is fine if the column already
    -- exists in newly-created tables; the IF NOT EXISTS guard
    -- prevents duplicate-add on subsequent boots.
    """)
    try:
        conn.execute("ALTER TABLE games ADD COLUMN game_type TEXT")
    except sqlite3.OperationalError:
        pass  # already exists
    conn.executescript("""

    -- Per-game per-team box-score stats (B1 — needed for pace + ratings)
    CREATE TABLE IF NOT EXISTS game_team_stats (
        game_id      TEXT NOT NULL,
        team_id      INTEGER NOT NULL,
        is_home      INTEGER NOT NULL,
        points       INTEGER,
        fga          INTEGER, fgm INTEGER,
        fta          INTEGER, ftm INTEGER,
        tpa          INTEGER, tpm INTEGER,
        orb          INTEGER, drb INTEGER,
        ast          INTEGER, stl INTEGER, blk INTEGER,
        tov          INTEGER,
        fouls        INTEGER,
        fastbreak    INTEGER, paint INTEGER, totp INTEGER,
        pace         REAL,
        ortg         REAL, drtg REAL,
        PRIMARY KEY (game_id, team_id)
    );
    CREATE INDEX IF NOT EXISTS idx_gts_team ON game_team_stats(team_id);

    CREATE TABLE IF NOT EXISTS picks (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        date         TEXT NOT NULL,
        game_id      TEXT,
        matchup      TEXT,
        bet_type     TEXT NOT NULL,
        pick         TEXT NOT NULL,
        model_prob   REAL,
        edge         REAL,
        odds         INTEGER,
        stake_units  REAL,
        result       TEXT,
        profit       REAL,
        closing_odds INTEGER,
        created_at   TEXT DEFAULT (datetime('now')),
        settled_at   TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_picks_date ON picks(date);
    CREATE INDEX IF NOT EXISTS idx_picks_result ON picks(result);
    -- Pending-only unique constraint. Same shape as MLB/NBA/NHL
    -- main pick tables. Without it, the route handler's concurrent
    -- FULL UNIQUE (no `WHERE result IS NULL` partial). The partial
    -- released once a row settled and the recorder happily spammed
    -- the same family on every slate hit — 25+ copies per game in
    -- NBA Q1_TOTAL before the 2026-06-10 hardening pass.
    CREATE UNIQUE INDEX IF NOT EXISTS uq_picks_family
        ON picks(date, game_id, bet_type, pick);

    CREATE TABLE IF NOT EXISTS model_config (
        key   TEXT PRIMARY KEY,
        value TEXT
    );
    """)
    # Idempotent column adds for older DBs created before these
    # columns existed. Cheap (PRAGMA + maybe one ALTER per cold open).
    _add_column_if_missing(conn, "teams", "logo_url", "TEXT")
    _add_column_if_missing(conn, "games", "start_time", "TEXT")
    conn.commit()


def _add_column_if_missing(conn, table: str, column: str,
                            col_type: str) -> None:
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


def close_all() -> None:
    """Close every cached connection on this thread. Tests use this
    to make sure the next test gets a fresh DB."""
    cache = getattr(_local, "conns", None)
    if not cache:
        return
    for conn in cache.values():
        try:
            conn.close()
        except sqlite3.Error:
            pass
    _local.conns = {}
