"""Per-tour SQLite schema for the golf framework.

Four tables — every other module reads/writes through these:

    players        — athlete master (espn_id, name, country)
    tournaments    — events on the calendar (id, name, course, dates,
                     field size, status)
    field_entries  — player ↔ tournament join row + result (final pos,
                     score-to-par, made_cut). One row per player per
                     event so we can score historicals without joining
                     across an exploded JSON blob.
    picks          — emitted picks (winner / top_5 / top_10 / matchup),
                     tracked just like the team-sport per-league tables.

A "tour" here owns its own DB file (data/golf/{tour}.db). PGA and
LPGA don't share a player namespace cleanly (some duals exist via
Korn Ferry crossovers, but not enough to be worth a shared DB), so
each tour stays independent.
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from ._config import get_tour_config


_DDL = """
CREATE TABLE IF NOT EXISTS owgr_rankings (
    -- One row per resolved ESPN player. OWGR rank refreshed weekly.
    player_id      INTEGER PRIMARY KEY,
    rank           INTEGER,
    last_week_rank INTEGER,
    points_average REAL,
    points_total   REAL,
    country_code   TEXT,
    name           TEXT,
    week_end_date  TEXT,
    fetched_at     TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_owgr_rank ON owgr_rankings(rank);

CREATE TABLE IF NOT EXISTS player_sg (
    -- One row per (player, event) — DataGolf snapshots from the live
    -- strokes-gained page. Round 1-4 cumulative values per stat. Used
    -- by the predictor as a rolling-form refinement on score-to-par
    -- skill (SG is field- + course-adjusted; raw score-to-par isn't).
    player_id           INTEGER NOT NULL,
    event_name          TEXT NOT NULL,
    course_name         TEXT,
    rounds_completed    INTEGER,
    sg_total            REAL,
    sg_ott              REAL,
    sg_app              REAL,
    sg_arg              REAL,
    sg_putt             REAL,
    sg_t2g              REAL,
    sg_bs               REAL,
    finish_to_par       INTEGER,
    finish_position     TEXT,
    fetched_at          TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (player_id, event_name)
);
CREATE INDEX IF NOT EXISTS idx_player_sg_player ON player_sg(player_id);
CREATE INDEX IF NOT EXISTS idx_player_sg_fetched ON player_sg(fetched_at);

CREATE TABLE IF NOT EXISTS players (
    id          INTEGER PRIMARY KEY,        -- ESPN athlete id (stable)
    name        TEXT NOT NULL,
    short_name  TEXT,
    country     TEXT,
    flag_url    TEXT,
    headshot_url TEXT,
    updated_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS tournaments (
    id           TEXT PRIMARY KEY,           -- ESPN event id
    name         TEXT NOT NULL,
    short_name   TEXT,
    course       TEXT,
    start_date   TEXT NOT NULL,              -- ISO 8601 UTC
    end_date     TEXT,
    rounds       INTEGER DEFAULT 4,
    status       TEXT,                       -- scheduled / in / final / cancelled
    is_major     INTEGER DEFAULT 0,
    purse_usd    INTEGER,
    season       INTEGER,
    updated_at   TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_tournaments_start ON tournaments(start_date);
CREATE INDEX IF NOT EXISTS idx_tournaments_status ON tournaments(status);

CREATE TABLE IF NOT EXISTS field_entries (
    tournament_id  TEXT NOT NULL,
    player_id      INTEGER NOT NULL,
    -- Final position when known. For tied finishes ESPN ships the
    -- shared rank ("T5"); we store the integer rank and let the UI
    -- decorate the T.
    final_position INTEGER,
    score_to_par   INTEGER,                -- e.g. -8 for 280 on a par-72
    made_cut       INTEGER,                -- 1 / 0 / NULL (pre-cut)
    rounds_json    TEXT,                   -- per-round score blob
    withdrew       INTEGER DEFAULT 0,
    disqualified   INTEGER DEFAULT 0,
    PRIMARY KEY (tournament_id, player_id),
    FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
);
CREATE INDEX IF NOT EXISTS idx_field_player ON field_entries(player_id);

CREATE TABLE IF NOT EXISTS picks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    date          TEXT NOT NULL,            -- pick lock date (UTC)
    tournament_id TEXT NOT NULL,
    bet_type      TEXT NOT NULL,            -- WINNER / TOP_5 / TOP_10 / TOP_20 / MATCHUP
    player_id     INTEGER,                  -- single-player picks
    matchup_a     INTEGER,                  -- matchup picks (a vs b)
    matchup_b     INTEGER,
    pick          TEXT NOT NULL,            -- display "Scottie Scheffler"
    model_prob    REAL,
    edge          REAL,
    odds          INTEGER,
    result        TEXT,                     -- W / L / P (push for ties) / NULL
    profit        REAL,
    closing_odds  INTEGER,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    settled_at    TEXT,
    FOREIGN KEY (tournament_id) REFERENCES tournaments(id),
    FOREIGN KEY (player_id) REFERENCES players(id)
);
-- Open-pick dedup keys on (tournament, bet_type, player). Re-runs
-- across days on the same upcoming tournament must NOT accumulate
-- duplicate rows; date used to be part of this key and a slate hit on
-- two consecutive days produced two open rows for the same pick.
-- FULL UNIQUE (no partial). Settled rows released the partial and let
-- the recorder accumulate duplicates per tournament. Date stays OFF
-- the key on purpose — a golf tournament spans multiple days and the
-- same pick across days is the same bet.
CREATE UNIQUE INDEX IF NOT EXISTS uq_picks_family
    ON picks(tournament_id, bet_type, pick);
CREATE INDEX IF NOT EXISTS idx_picks_date ON picks(date);
CREATE INDEX IF NOT EXISTS idx_picks_tourney ON picks(tournament_id);
"""


_locks: dict[str, threading.Lock] = {}
_conns: dict[str, sqlite3.Connection] = {}


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_DDL)
    conn.commit()


def get_conn(tour: str) -> sqlite3.Connection:
    """Per-tour connection, lazily opened. Schema is created on first
    open. Thread-safe via per-tour lock — same pattern as the basketball
    framework."""
    if tour in _conns:
        return _conns[tour]
    lock = _locks.setdefault(tour, threading.Lock())
    with lock:
        if tour in _conns:
            return _conns[tour]
        cfg = get_tour_config(tour)
        path = Path(cfg["db_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), check_same_thread=False,
                                isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        _ensure_schema(conn)
        _conns[tour] = conn
        return conn
