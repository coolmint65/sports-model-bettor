"""Per-league football DB schema + connection cache.

Schema mirrors ``engine.hockey._db`` but renamed for football
semantics: ``home_score`` is final-game points (not goals), no
``has_overtime`` or ``has_shootout`` because OT/shootout in UFL just
counts toward final points without affecting market settlement.
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from . import LEAGUE_REGISTRY, get_league_config


# Per-thread connection cache. SQLite Python bindings serialize on the
# connection object internally, but concurrent reads/writes on the
# same connection from different threads (FastAPI runs each request
# on its own thread) still corrupt result sets — observed as
# "tuple index out of range", "bad parameter or API misuse" on the
# UFL slate route under load. threading.local gives each thread its
# own cached connection that lives for the thread's lifetime.
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
    status          TEXT,              -- scheduled / live / final / postponed / cancelled
    season          INTEGER,
    week            INTEGER,
    venue           TEXT,
    updated_at      TEXT DEFAULT (datetime('now')),
    home_linescore  TEXT,               -- JSON list of per-period points (Q1..Q4[,OT..]) — added 2026-10-25 (period markets need it to settle)
    away_linescore  TEXT,
    periods         INTEGER,            -- periods played at last ingest (final: 4 = regulation, >4 = OT)
    FOREIGN KEY (home_team_id) REFERENCES teams(id),
    FOREIGN KEY (away_team_id) REFERENCES teams(id)
);
CREATE INDEX IF NOT EXISTS idx_games_date ON games(date);
CREATE INDEX IF NOT EXISTS idx_games_status ON games(status);

CREATE TABLE IF NOT EXISTS game_stats (          -- ESPN box-score cache (2026-10-25, engine.football._boxscore)
    game_id       TEXT PRIMARY KEY,
    fetched_at    TEXT DEFAULT (datetime('now')),
    final         INTEGER,                       -- 1 = settlement-grade (stats present, or ESPN will never post them)
    home_abbr     TEXT,
    away_abbr     TEXT,
    team_stats    TEXT,                          -- JSON {"home": {espn stat name: displayValue}, "away": {...}}
    group_totals  TEXT,                          -- JSON per-side player stat-group totals (kicking, punting, passing, ...)
    scoring_plays TEXT,                          -- JSON list, chronological (side/kind/method/XP/2-pt parsed)
    drives        TEXT,                          -- JSON list, chronological (result bucket + field progress)
    facts         TEXT                           -- JSON normalized settlement facts (see _boxscore.py docstring)
);

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
    result          TEXT,               -- W / L / P / V / NULL
    profit          REAL,
    stake_units     REAL,               -- added 2026-09-03 (tracker wrote it; DDL never had it)
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    settled_at      TEXT,
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);
-- FULL UNIQUE on (date, game_id, bet_type, pick). Was partial-on-
-- pending; partial released after settle and the recorder spammed
-- duplicates. 2026-06-10 hardening pass swept the live DBs and
-- updated every schema source to use this shape.
CREATE UNIQUE INDEX IF NOT EXISTS uq_picks_family
    ON picks(date, game_id, bet_type, pick);
CREATE INDEX IF NOT EXISTS idx_picks_date ON picks(date);
CREATE INDEX IF NOT EXISTS idx_picks_game ON picks(game_id);

-- One PENDING paper (is_shadow=1, result IS NULL) pick per game, enforced
-- at the DB level (Austin 2026-10-10). record_paper()'s own "does a
-- pending row already exist for this game" check-then-insert is only safe
-- under strictly sequential execution; two overlapping write-side passes
-- for the same league (routes_football's old _SLATE_REFRESHING /
-- _WRITESIDE_RUNNING guards were non-atomic sets) could both pass that
-- check and both insert — confirmed in production: 43 games across
-- cfb+nfl ended up with 2-3 conflicting pending markets for one game on
-- 2026-10-10. record_paper's INSERT already uses INSERT OR IGNORE, so
-- once this index exists a second concurrent insert is atomically
-- rejected instead of relying solely on the app-level check. Safe to add
-- now that the 2026-10-10 cleanup voided every existing duplicate — this
-- only takes effect for a league the first time _ensure_schema runs in a
-- FRESH process (i.e. at next restart), never against an already-open
-- connection's already-ensured schema.
CREATE UNIQUE INDEX IF NOT EXISTS idx_picks_one_pending_paper
    ON picks(game_id) WHERE is_shadow=1 AND result IS NULL;

-- Per-prediction signal log. One row per (game_id, market) capture.
-- Feeds the V3.2 signal_explain endpoint + future per-signal track
-- record aggregations.
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
    # 2026-09-03 migration: cfb.db/nfl.db were created before `stake_units` existed, so
    # record_picks' INSERT and list_history's SELECT both failed ("no such column:
    # p.stake_units") -> no CFB/NFL picks ever recorded + the Records tab 500'd.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(picks)").fetchall()}
    if "stake_units" not in cols:
        conn.execute("ALTER TABLE picks ADD COLUMN stake_units REAL")
    # 2026-10-25 migration (MintyBot, Austin "fix up the 3-way, then 3-5"): per-period points + OT
    # flag on games. The H1/Q1 period picks (since 10-16) and the new H2/Q2-Q4 / period team
    # totals / 3-way rows could never settle — only final scores were stored.
    gcols = {r[1] for r in conn.execute("PRAGMA table_info(games)").fetchall()}
    for _col, _typ in (("home_linescore", "TEXT"), ("away_linescore", "TEXT"), ("periods", "INTEGER")):
        if _col not in gcols:
            conn.execute(f"ALTER TABLE games ADD COLUMN {_col} {_typ}")
    conn.commit()


def get_conn(league: str) -> sqlite3.Connection:
    """Per-thread cached sqlite3 connection. Schema is ensured the
    first time a league is touched by ANY thread (via a process-wide
    lock); thereafter each thread has its own connection object.
    """
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
    # First-touch schema ensure under a per-league lock so we don't
    # race executescript across threads on first boot.
    lock = _locks.setdefault(league, threading.Lock())
    with lock:
        _ensure_schema(conn)
    cache[league] = conn
    return conn
