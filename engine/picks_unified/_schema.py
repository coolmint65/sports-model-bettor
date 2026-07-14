"""Unified picks DB schema + connection helper.

Single SQLite at ``data/picks_unified.db``. Per the design discussion,
we chose a single canonical table over per-sport tables — cross-sport
queries (unified tracker, dashboard ROI by stake-tier across sports)
are the use case.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path


_DB_PATH = (Path(__file__).resolve().parent.parent.parent
            / "data" / "picks_unified.db")

# Thread-local connection cache (FastAPI's request workers can hit
# this concurrently). Matches the pattern feedback_thread_local_conns.md
# already enforces for new sport frameworks.
_local = threading.local()


_DDL = """
CREATE TABLE IF NOT EXISTS picks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Identity
    sport      TEXT NOT NULL,
    league     TEXT NOT NULL,
    game_key   TEXT NOT NULL,
    pick_date  TEXT NOT NULL,
    matchup    TEXT NOT NULL,

    -- Market
    scope      TEXT NOT NULL,
    bet_type   TEXT NOT NULL,
    variant    TEXT NOT NULL DEFAULT 'main',
    pick_text  TEXT NOT NULL,
    side       TEXT,
    line       REAL,

    -- Pricing
    odds                INTEGER NOT NULL,
    closing_odds        INTEGER,
    closing_captured_at TEXT,

    -- Probabilities
    prob      REAL NOT NULL,
    prob_raw  REAL,

    -- Edge + sizing
    edge_pct    REAL NOT NULL,
    stake_units REAL NOT NULL DEFAULT 0,
    confidence  TEXT,
    is_shadow   INTEGER NOT NULL DEFAULT 0,

    -- Lifecycle
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    settled_at TEXT,
    result     TEXT,
    profit     REAL,

    -- Live-pick metadata
    pick_at_period       INTEGER,
    pick_at_clock_secs   INTEGER,
    pick_at_home_score   INTEGER,
    pick_at_away_score   INTEGER,

    -- Provenance
    model_version    TEXT,
    pipeline_version TEXT
);

-- Full unique on the canonical family. NO partial-on-pending —
-- that footgun was swept across all sports on 2026-06-10 and the
-- new schema starts unconditionally unique.
CREATE UNIQUE INDEX IF NOT EXISTS uq_picks_family
    ON picks(game_key, scope, bet_type, pick_text);

-- Hot read paths.
CREATE INDEX IF NOT EXISTS idx_picks_sport_date
    ON picks(sport, pick_date);
CREATE INDEX IF NOT EXISTS idx_picks_league_date
    ON picks(sport, league, pick_date);
CREATE INDEX IF NOT EXISTS idx_picks_game_key
    ON picks(game_key);
CREATE INDEX IF NOT EXISTS idx_picks_pending
    ON picks(result) WHERE result IS NULL;
CREATE INDEX IF NOT EXISTS idx_picks_created
    ON picks(created_at DESC);
"""


def get_conn() -> sqlite3.Connection:
    """Get the thread-local connection. Creates DB + schema on first call.

    WAL mode + 5s busy timeout to play nice with the worker process
    that's reading/writing in parallel (closing-odds capture, settle
    loop, settle/record piggyback on slate routes).
    """
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except sqlite3.Error:
            try:
                conn.close()
            except sqlite3.Error:
                pass
            _local.conn = None

    os.makedirs(_DB_PATH.parent, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(_DDL)
    _local.conn = conn
    return conn


def reset_for_tests() -> None:
    """Drop and recreate the schema. Test fixtures only."""
    global _DB_PATH
    if _DB_PATH.exists():
        _DB_PATH.unlink()
    # Bust the cached connection
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except sqlite3.Error:
            pass
        _local.conn = None
    get_conn()  # recreates
