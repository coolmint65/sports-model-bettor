"""
SQLite database layer for MLB data.

Stores teams, players, games, pitcher stats, batter stats,
bullpen data, park factors, and batter-vs-pitcher H2H matchups.
"""

import json
import logging
import sqlite3
from pathlib import Path

import threading

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "mlb.db"

# Thread-local storage for DB connections — each thread gets its own
_local = threading.local()


def get_conn() -> sqlite3.Connection:
    """Get a thread-local DB connection."""
    conn = getattr(_local, 'conn', None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            _local.conn = None

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    _init_schema(conn)
    _migrate(conn)
    _local.conn = conn
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    """Create tables if they don't exist."""
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS teams (
        id          INTEGER PRIMARY KEY,
        mlb_id      INTEGER UNIQUE NOT NULL,
        name        TEXT NOT NULL,
        abbreviation TEXT NOT NULL,
        city        TEXT,
        venue       TEXT,
        league      TEXT,       -- 'AL' or 'NL'
        division    TEXT,       -- 'East', 'Central', 'West'
        updated_at  TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS players (
        id          INTEGER PRIMARY KEY,
        mlb_id      INTEGER UNIQUE NOT NULL,
        name        TEXT NOT NULL,
        team_id     INTEGER ,
        position    TEXT,       -- 'P', 'C', '1B', etc.
        bats        TEXT,       -- 'R', 'L', 'S'
        throws      TEXT,       -- 'R', 'L'
        active      INTEGER DEFAULT 1,
        updated_at  TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS games (
        id              INTEGER PRIMARY KEY,
        mlb_game_id     INTEGER UNIQUE NOT NULL,
        date            TEXT NOT NULL,
        home_team_id    INTEGER ,
        away_team_id    INTEGER ,
        home_score      INTEGER,
        away_score      INTEGER,
        status          TEXT DEFAULT 'scheduled', -- scheduled, live, final
        home_pitcher_id INTEGER ,
        away_pitcher_id INTEGER ,
        venue           TEXT,
        day_night       TEXT,
        weather_temp    REAL,
        weather_wind    TEXT,
        umpire          TEXT,
        winning_pitcher INTEGER ,
        losing_pitcher  INTEGER ,
        save_pitcher    INTEGER ,
        season          INTEGER,
        -- Linescore: runs per inning as JSON arrays e.g. [0,1,0,2,0,0,1,0,0]
        home_linescore  TEXT,
        away_linescore  TEXT,
        updated_at      TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS pitcher_stats (
        id          INTEGER PRIMARY KEY,
        player_id   INTEGER NOT NULL ,
        season      INTEGER NOT NULL,
        team_id     INTEGER ,
        -- Counting stats
        games       INTEGER DEFAULT 0,
        games_started INTEGER DEFAULT 0,
        wins        INTEGER DEFAULT 0,
        losses      INTEGER DEFAULT 0,
        saves       INTEGER DEFAULT 0,
        innings     REAL DEFAULT 0,
        hits        INTEGER DEFAULT 0,
        runs        INTEGER DEFAULT 0,
        earned_runs INTEGER DEFAULT 0,
        walks       INTEGER DEFAULT 0,
        strikeouts  INTEGER DEFAULT 0,
        home_runs   INTEGER DEFAULT 0,
        -- Rate stats
        era         REAL,
        whip        REAL,
        k_per_9     REAL,
        bb_per_9    REAL,
        hr_per_9    REAL,
        k_pct       REAL,
        bb_pct      REAL,
        -- Advanced
        fip         REAL,
        x_fip       REAL,
        babip       REAL,
        lob_pct     REAL,
        gb_pct      REAL,
        hr_per_fb   REAL,
        -- Statcast
        avg_velocity REAL,
        max_velocity REAL,
        spin_rate   REAL,
        whiff_pct   REAL,
        barrel_pct_against REAL,
        hard_hit_pct_against REAL,
        xera        REAL,
        -- Splits
        era_home    REAL,
        era_away    REAL,
        era_vs_left REAL,
        era_vs_right REAL,
        updated_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(player_id, season)
    );

    CREATE TABLE IF NOT EXISTS batter_stats (
        id          INTEGER PRIMARY KEY,
        player_id   INTEGER NOT NULL ,
        season      INTEGER NOT NULL,
        team_id     INTEGER ,
        -- Counting stats
        games       INTEGER DEFAULT 0,
        plate_appearances INTEGER DEFAULT 0,
        at_bats     INTEGER DEFAULT 0,
        hits        INTEGER DEFAULT 0,
        doubles     INTEGER DEFAULT 0,
        triples     INTEGER DEFAULT 0,
        home_runs   INTEGER DEFAULT 0,
        rbi         INTEGER DEFAULT 0,
        stolen_bases INTEGER DEFAULT 0,
        walks       INTEGER DEFAULT 0,
        strikeouts  INTEGER DEFAULT 0,
        -- Rate stats
        avg         REAL,
        obp         REAL,
        slg         REAL,
        ops         REAL,
        k_pct       REAL,
        bb_pct      REAL,
        iso         REAL,
        babip       REAL,
        -- Advanced
        wrc_plus    REAL,
        woba        REAL,
        war         REAL,
        -- Statcast
        avg_exit_velo REAL,
        max_exit_velo REAL,
        barrel_pct  REAL,
        hard_hit_pct REAL,
        launch_angle REAL,
        xba         REAL,
        xslg        REAL,
        xwoba       REAL,
        -- Splits
        avg_vs_left REAL,
        ops_vs_left REAL,
        avg_vs_right REAL,
        ops_vs_right REAL,
        avg_home    REAL,
        avg_away    REAL,
        updated_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(player_id, season)
    );

    CREATE TABLE IF NOT EXISTS bullpen_stats (
        id          INTEGER PRIMARY KEY,
        team_id     INTEGER NOT NULL ,
        season      INTEGER NOT NULL,
        era         REAL,
        whip        REAL,
        k_per_9     REAL,
        bb_per_9    REAL,
        saves       INTEGER DEFAULT 0,
        blown_saves INTEGER DEFAULT 0,
        holds       INTEGER DEFAULT 0,
        innings     REAL DEFAULT 0,
        -- Usage / fatigue
        innings_last_3d REAL DEFAULT 0,
        innings_last_7d REAL DEFAULT 0,
        games_last_3d   INTEGER DEFAULT 0,
        updated_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(team_id, season)
    );

    CREATE TABLE IF NOT EXISTS h2h_matchups (
        id          INTEGER PRIMARY KEY,
        batter_id   INTEGER NOT NULL ,
        pitcher_id  INTEGER NOT NULL ,
        at_bats     INTEGER DEFAULT 0,
        hits        INTEGER DEFAULT 0,
        doubles     INTEGER DEFAULT 0,
        triples     INTEGER DEFAULT 0,
        home_runs   INTEGER DEFAULT 0,
        walks       INTEGER DEFAULT 0,
        strikeouts  INTEGER DEFAULT 0,
        avg         REAL,
        ops         REAL,
        updated_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(batter_id, pitcher_id)
    );

    CREATE TABLE IF NOT EXISTS park_factors (
        id          INTEGER PRIMARY KEY,
        venue       TEXT NOT NULL,
        team_id     INTEGER ,
        season      INTEGER,
        run_factor  REAL DEFAULT 1.0,   -- >1 hitter-friendly
        hr_factor   REAL DEFAULT 1.0,
        h_factor    REAL DEFAULT 1.0,
        bb_factor   REAL DEFAULT 1.0,
        updated_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(venue, season)
    );

    CREATE TABLE IF NOT EXISTS team_stats (
        id          INTEGER PRIMARY KEY,
        team_id     INTEGER NOT NULL ,
        season      INTEGER NOT NULL,
        -- Offense
        runs_pg     REAL,
        avg         REAL,
        obp         REAL,
        slg         REAL,
        ops         REAL,
        wrc_plus    REAL,
        k_pct       REAL,
        bb_pct      REAL,
        iso         REAL,
        babip       REAL,
        -- Pitching
        era         REAL,
        whip        REAL,
        k_per_9     REAL,
        bb_per_9    REAL,
        fip         REAL,
        -- Defense
        fielding_pct REAL,
        errors      INTEGER DEFAULT 0,
        -- Record
        wins        INTEGER DEFAULT 0,
        losses      INTEGER DEFAULT 0,
        run_diff    INTEGER DEFAULT 0,
        home_wins   INTEGER DEFAULT 0,
        home_losses INTEGER DEFAULT 0,
        away_wins   INTEGER DEFAULT 0,
        away_losses INTEGER DEFAULT 0,
        -- Streaks / form
        last_10_wins INTEGER DEFAULT 0,
        last_10_losses INTEGER DEFAULT 0,
        streak      TEXT,  -- 'W3', 'L2', etc.
        updated_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(team_id, season)
    );

    CREATE TABLE IF NOT EXISTS odds (
        id          INTEGER PRIMARY KEY,
        game_id     INTEGER NOT NULL REFERENCES games(mlb_game_id),
        source      TEXT DEFAULT 'consensus',
        home_ml     INTEGER,    -- e.g. -150
        away_ml     INTEGER,    -- e.g. +130
        spread      REAL,       -- run line, e.g. -1.5
        home_spread_odds INTEGER,
        away_spread_odds INTEGER,
        total       REAL,       -- O/U line
        over_odds   INTEGER,
        under_odds  INTEGER,
        updated_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(game_id, source)
    );

    CREATE TABLE IF NOT EXISTS umpires (
        id          INTEGER PRIMARY KEY,
        name        TEXT UNIQUE NOT NULL,
        games       INTEGER DEFAULT 0,
        -- Zone tendencies (career or recent season)
        k_pct       REAL,       -- Strikeout rate in games umped
        bb_pct      REAL,       -- Walk rate
        rpg         REAL,       -- Avg runs per game
        over_pct    REAL,       -- % of games that went over
        ba          REAL,       -- Batting avg in games umped
        run_factor  REAL DEFAULT 1.0,  -- >1 = more runs, <1 = fewer
        updated_at  TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS picks (
        id          INTEGER PRIMARY KEY,
        game_id     INTEGER,
        date        TEXT NOT NULL,
        matchup     TEXT,           -- "NYY @ BOS"
        bet_type    TEXT NOT NULL,   -- 'ml', 'ou', 'nrfi', 'rl', 'f5'
        pick        TEXT NOT NULL,   -- "NYY", "Over 8.5", "NRFI", etc.
        model_prob  REAL,
        edge        REAL,
        odds        INTEGER,
        result      TEXT,            -- 'W', 'L', 'P' (push), NULL (pending)
        profit      REAL,            -- dollars won/lost on $100 bet
        created_at  TEXT DEFAULT (datetime('now')),
        settled_at  TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_picks_date ON picks(date);
    CREATE INDEX IF NOT EXISTS idx_picks_result ON picks(result);

    -- Indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_games_date ON games(date);
    CREATE INDEX IF NOT EXISTS idx_games_status ON games(status);
    CREATE INDEX IF NOT EXISTS idx_games_season ON games(season);
    CREATE INDEX IF NOT EXISTS idx_games_home ON games(home_team_id);
    CREATE INDEX IF NOT EXISTS idx_games_away ON games(away_team_id);
    CREATE INDEX IF NOT EXISTS idx_pitcher_stats_player ON pitcher_stats(player_id, season);
    CREATE INDEX IF NOT EXISTS idx_batter_stats_player ON batter_stats(player_id, season);
    CREATE INDEX IF NOT EXISTS idx_batter_stats_team ON batter_stats(team_id, season);
    CREATE INDEX IF NOT EXISTS idx_h2h_batter ON h2h_matchups(batter_id);
    CREATE INDEX IF NOT EXISTS idx_h2h_pitcher ON h2h_matchups(pitcher_id);
    CREATE INDEX IF NOT EXISTS idx_team_stats_team ON team_stats(team_id, season);
    CREATE INDEX IF NOT EXISTS idx_picks_game ON picks(game_id);
    CREATE INDEX IF NOT EXISTS idx_players_team ON players(team_id);
    """)
    conn.commit()


def _migrate(conn: sqlite3.Connection) -> None:
    """Add columns that may be missing from older databases."""
    migrations = [
        ("games", "home_linescore", "TEXT"),
        ("games", "away_linescore", "TEXT"),
        ("games", "umpire", "TEXT"),
        ("picks", "closing_odds", "INTEGER"),
    ]

    # Check existing columns first to avoid ALTER TABLE errors
    for table, column, col_type in migrations:
        try:
            existing = conn.execute(f"PRAGMA table_info({table})").fetchall()
            col_names = [r[1] for r in existing]
            if column not in col_names:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                logger.info("Added column %s.%s", table, column)
        except Exception as e:
            logger.warning("Migration failed for %s.%s: %s", table, column, e)

    try:
        conn.commit()
    except Exception:
        pass


# ── Convenience helpers ──────────────────────────────────────

def upsert_team(mlb_id: int, name: str, abbreviation: str, city: str = "",
                venue: str = "", league: str = "", division: str = "") -> None:
    conn = get_conn()
    conn.execute("""
        INSERT INTO teams (mlb_id, name, abbreviation, city, venue, league, division)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mlb_id) DO UPDATE SET
            name=excluded.name, abbreviation=excluded.abbreviation,
            city=excluded.city, venue=excluded.venue,
            league=excluded.league, division=excluded.division,
            updated_at=datetime('now')
    """, (mlb_id, name, abbreviation, city, venue, league, division))
    conn.commit()


def upsert_player(mlb_id: int, name: str, team_id: int | None = None,
                   position: str = "", bats: str = "", throws: str = "") -> None:
    conn = get_conn()
    conn.execute("""
        INSERT INTO players (mlb_id, name, team_id, position, bats, throws)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(mlb_id) DO UPDATE SET
            name=excluded.name, team_id=excluded.team_id,
            position=excluded.position, bats=excluded.bats,
            throws=excluded.throws, updated_at=datetime('now')
    """, (mlb_id, name, team_id, position, bats, throws))
    conn.commit()


def upsert_game(mlb_game_id: int, **kwargs) -> None:
    conn = get_conn()
    fields = ["date", "home_team_id", "away_team_id", "home_score", "away_score",
              "status", "home_pitcher_id", "away_pitcher_id", "venue", "day_night",
              "weather_temp", "weather_wind", "winning_pitcher", "losing_pitcher",
              "save_pitcher", "season", "home_linescore", "away_linescore"]
    values = {k: kwargs.get(k) for k in fields}
    values["mlb_game_id"] = mlb_game_id

    cols = ", ".join(values.keys())
    placeholders = ", ".join(["?"] * len(values))
    updates = ", ".join(f"{k}=excluded.{k}" for k in fields if kwargs.get(k) is not None)
    if updates:
        updates += ", updated_at=datetime('now')"

    conn.execute(f"""
        INSERT INTO games ({cols}) VALUES ({placeholders})
        ON CONFLICT(mlb_game_id) DO UPDATE SET {updates}
    """, list(values.values()))
    conn.commit()


def get_team_by_abbr(abbr: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM teams WHERE abbreviation = ?",
                       (abbr.upper(),)).fetchone()
    return dict(row) if row else None


def get_team_by_id(mlb_id: int) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM teams WHERE mlb_id = ?", (mlb_id,)).fetchone()
    return dict(row) if row else None


def get_all_teams() -> list[dict]:
    conn = get_conn()
    rows = conn.execute("SELECT * FROM teams ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def get_today_games(date: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT g.*,
               ht.name as home_name, ht.abbreviation as home_abbr,
               at.name as away_name, at.abbreviation as away_abbr,
               hp.name as home_pitcher_name, ap.name as away_pitcher_name
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.mlb_id
        JOIN teams at ON g.away_team_id = at.mlb_id
        LEFT JOIN players hp ON g.home_pitcher_id = hp.mlb_id
        LEFT JOIN players ap ON g.away_pitcher_id = ap.mlb_id
        WHERE g.date = ?
        ORDER BY g.mlb_game_id
    """, (date,)).fetchall()
    return [dict(r) for r in rows]


def get_team_record(team_id: int, season: int) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM team_stats WHERE team_id = ? AND season = ?",
                       (team_id, season)).fetchone()
    return dict(row) if row else None


def get_pitcher_season(player_id: int, season: int) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM pitcher_stats WHERE player_id = ? AND season = ?",
                       (player_id, season)).fetchone()
    return dict(row) if row else None


def get_batter_season(player_id: int, season: int) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM batter_stats WHERE player_id = ? AND season = ?",
                       (player_id, season)).fetchone()
    return dict(row) if row else None


def get_bullpen(team_id: int, season: int) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM bullpen_stats WHERE team_id = ? AND season = ?",
                       (team_id, season)).fetchone()
    return dict(row) if row else None


def get_h2h(batter_id: int, pitcher_id: int) -> dict | None:
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM h2h_matchups WHERE batter_id = ? AND pitcher_id = ?",
        (batter_id, pitcher_id)).fetchone()
    return dict(row) if row else None


def get_park_factor(venue: str, season: int | None = None) -> dict | None:
    conn = get_conn()
    if season:
        row = conn.execute(
            "SELECT * FROM park_factors WHERE venue = ? AND season = ?",
            (venue, season)).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM park_factors WHERE venue = ? ORDER BY season DESC LIMIT 1",
            (venue,)).fetchone()
    return dict(row) if row else None


def get_recent_games(team_id: int, n: int = 10) -> list[dict]:
    """Get last N games for a team (home or away)."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT g.*,
               ht.abbreviation as home_abbr,
               at.abbreviation as away_abbr
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.mlb_id
        JOIN teams at ON g.away_team_id = at.mlb_id
        WHERE (g.home_team_id = ? OR g.away_team_id = ?)
          AND g.status = 'final'
        ORDER BY g.date DESC
        LIMIT ?
    """, (team_id, team_id, n)).fetchall()
    return [dict(r) for r in rows]


def get_pitcher_recent_starts(pitcher_id: int, n: int = 5) -> list[dict]:
    """Get last N starts for a pitcher."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT g.*, t.abbreviation as opp_abbr
        FROM games g
        LEFT JOIN teams t ON (
            CASE WHEN g.home_pitcher_id = ? THEN g.away_team_id
                 ELSE g.home_team_id END) = t.mlb_id
        WHERE (g.home_pitcher_id = ? OR g.away_pitcher_id = ?)
          AND g.status = 'final'
        ORDER BY g.date DESC
        LIMIT ?
    """, (pitcher_id, pitcher_id, pitcher_id, n)).fetchall()
    return [dict(r) for r in rows]


def get_team_h2h_vs_pitcher(team_id: int, pitcher_id: int) -> list[dict]:
    """Get all H2H matchup data for batters on a team vs a specific pitcher."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT h.*, p.name as batter_name, p.bats
        FROM h2h_matchups h
        JOIN players p ON h.batter_id = p.mlb_id
        WHERE p.team_id = ? AND h.pitcher_id = ?
          AND h.at_bats >= 3
        ORDER BY h.at_bats DESC
    """, (team_id, pitcher_id)).fetchall()
    return [dict(r) for r in rows]


def get_umpire(name: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM umpires WHERE name = ?", (name,)).fetchone()
    return dict(row) if row else None
