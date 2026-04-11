"""
SQLite database layer for NBA data.

Stores teams, games (with quarter-by-quarter scores), Q1 profile stats,
picks, model config, and odds history.  Focused on 1st quarter spread
prediction.
"""

import logging
import sqlite3
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "nba.db"

# Thread-local storage for DB connections — each thread gets its own
_local = threading.local()


def get_conn() -> sqlite3.Connection:
    """Get a thread-local DB connection."""
    conn = getattr(_local, "conn", None)
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
    conn.execute("PRAGMA foreign_keys=OFF")
    _init_schema(conn)
    _local.conn = conn
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    """Create tables if they don't exist."""
    conn.executescript("""
    -- NBA teams
    CREATE TABLE IF NOT EXISTS nba_teams (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        abbreviation TEXT NOT NULL,
        city TEXT,
        conference TEXT,
        division TEXT,
        venue TEXT
    );

    -- NBA games with quarter-by-quarter scores
    CREATE TABLE IF NOT EXISTS nba_games (
        game_id TEXT PRIMARY KEY,
        date TEXT NOT NULL,
        home_team_id INTEGER,
        away_team_id INTEGER,
        home_score INTEGER,
        away_score INTEGER,
        home_q1 INTEGER,
        away_q1 INTEGER,
        home_q2 INTEGER,
        away_q2 INTEGER,
        home_q3 INTEGER,
        away_q3 INTEGER,
        home_q4 INTEGER,
        away_q4 INTEGER,
        status TEXT DEFAULT 'scheduled',
        season INTEGER,
        home_pace REAL,
        away_pace REAL
    );

    -- Team Q1 profile stats (computed from games)
    CREATE TABLE IF NOT EXISTS nba_q1_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id INTEGER NOT NULL,
        season INTEGER NOT NULL,
        games INTEGER DEFAULT 0,
        q1_ppg REAL,
        q1_opp_ppg REAL,
        q1_margin REAL,
        q1_home_ppg REAL,
        q1_home_opp_ppg REAL,
        q1_away_ppg REAL,
        q1_away_opp_ppg REAL,
        q1_cover_pct REAL,
        q1_over_pct REAL,
        pace REAL,
        off_rating REAL,
        def_rating REAL,
        fg_pct REAL,
        three_pct REAL,
        ft_rate REAL,
        reb_rate REAL,
        fast_start_pct REAL,
        slow_start_pct REAL,
        UNIQUE(team_id, season)
    );

    -- NBA picks (Q1 specific)
    CREATE TABLE IF NOT EXISTS nba_picks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id TEXT,
        date TEXT NOT NULL,
        matchup TEXT,
        bet_type TEXT NOT NULL,
        pick TEXT NOT NULL,
        model_prob REAL,
        edge REAL,
        odds INTEGER,
        result TEXT,
        profit REAL,
        created_at TEXT DEFAULT (datetime('now')),
        settled_at TEXT
    );

    -- NBA model config (calibration weights)
    CREATE TABLE IF NOT EXISTS nba_model_config (
        key TEXT PRIMARY KEY,
        value REAL,
        updated_at TEXT DEFAULT (datetime('now'))
    );

    -- NBA odds history
    CREATE TABLE IF NOT EXISTS nba_odds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_date TEXT NOT NULL,
        home_abbr TEXT NOT NULL,
        away_abbr TEXT NOT NULL,
        home_ml INTEGER,
        away_ml INTEGER,
        q1_spread REAL,
        q1_spread_home_odds INTEGER,
        q1_spread_away_odds INTEGER,
        q1_total REAL,
        q1_over_odds INTEGER,
        q1_under_odds INTEGER,
        full_game_spread REAL,
        over_under REAL,
        provider TEXT DEFAULT 'DraftKings',
        captured_at TEXT DEFAULT (datetime('now')),
        UNIQUE(game_date, home_abbr, away_abbr)
    );

    -- Indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_nba_games_date ON nba_games(date);
    CREATE INDEX IF NOT EXISTS idx_nba_games_season ON nba_games(season);
    CREATE INDEX IF NOT EXISTS idx_nba_games_home ON nba_games(home_team_id);
    CREATE INDEX IF NOT EXISTS idx_nba_games_away ON nba_games(away_team_id);
    CREATE INDEX IF NOT EXISTS idx_nba_q1_stats_team ON nba_q1_stats(team_id, season);
    CREATE INDEX IF NOT EXISTS idx_nba_picks_date ON nba_picks(date);
    CREATE INDEX IF NOT EXISTS idx_nba_odds_date ON nba_odds(game_date);
    """)
    conn.commit()


# -- Convenience helpers --------------------------------------------------


def get_nba_team(team_id: int) -> dict | None:
    """Get an NBA team by its ESPN ID."""
    conn = get_conn()
    row = conn.execute("SELECT * FROM nba_teams WHERE id = ?",
                       (team_id,)).fetchone()
    return dict(row) if row else None


def get_nba_team_by_abbr(abbr: str) -> dict | None:
    """Get an NBA team by its abbreviation (e.g. 'LAL')."""
    conn = get_conn()
    row = conn.execute("SELECT * FROM nba_teams WHERE abbreviation = ?",
                       (abbr.upper(),)).fetchone()
    return dict(row) if row else None


def get_all_nba_teams() -> list[dict]:
    """Get all NBA teams ordered by name."""
    conn = get_conn()
    rows = conn.execute("SELECT * FROM nba_teams ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def get_team_q1_stats(team_id: int, season: int) -> dict | None:
    """Get Q1 profile stats for a team/season."""
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM nba_q1_stats WHERE team_id = ? AND season = ?",
        (team_id, season)).fetchone()
    return dict(row) if row else None


def get_recent_nba_games(team_id: int, n: int = 20) -> list[dict]:
    """Get last N finished games for a team (home or away)."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT g.*,
               ht.abbreviation as home_abbr, ht.name as home_name,
               at.abbreviation as away_abbr, at.name as away_name
        FROM nba_games g
        JOIN nba_teams ht ON g.home_team_id = ht.id
        JOIN nba_teams at ON g.away_team_id = at.id
        WHERE (g.home_team_id = ? OR g.away_team_id = ?)
          AND g.status = 'final'
        ORDER BY g.date DESC
        LIMIT ?
    """, (team_id, team_id, n)).fetchall()
    return [dict(r) for r in rows]


def get_today_nba_games(date: str) -> list[dict]:
    """Get all games for a given date."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT g.*,
               ht.name as home_name, ht.abbreviation as home_abbr,
               at.name as away_name, at.abbreviation as away_abbr
        FROM nba_games g
        JOIN nba_teams ht ON g.home_team_id = ht.id
        JOIN nba_teams at ON g.away_team_id = at.id
        WHERE g.date = ?
        ORDER BY g.game_id
    """, (date,)).fetchall()
    return [dict(r) for r in rows]


def get_h2h_nba(team1_id: int, team2_id: int, seasons: int = 3) -> list[dict]:
    """Get head-to-head games between two teams over the last N seasons."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT g.*,
               ht.abbreviation as home_abbr,
               at.abbreviation as away_abbr
        FROM nba_games g
        JOIN nba_teams ht ON g.home_team_id = ht.id
        JOIN nba_teams at ON g.away_team_id = at.id
        WHERE ((g.home_team_id = ? AND g.away_team_id = ?)
            OR (g.home_team_id = ? AND g.away_team_id = ?))
          AND g.status = 'final'
          AND g.season >= (
              SELECT COALESCE(MAX(season), 0) - ? + 1 FROM nba_games
          )
        ORDER BY g.date DESC
    """, (team1_id, team2_id, team2_id, team1_id, seasons)).fetchall()
    return [dict(r) for r in rows]


# -- Upsert helpers -------------------------------------------------------


def upsert_nba_team(team_id: int, name: str, abbreviation: str,
                    city: str = "", conference: str = "",
                    division: str = "", venue: str = "") -> None:
    conn = get_conn()
    conn.execute("""
        INSERT INTO nba_teams (id, name, abbreviation, city, conference, division, venue)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name, abbreviation=excluded.abbreviation,
            city=excluded.city, conference=excluded.conference,
            division=excluded.division, venue=excluded.venue
    """, (team_id, name, abbreviation, city, conference, division, venue))
    conn.commit()


def upsert_nba_game(game_id: str, **kwargs) -> None:
    """Insert or update an NBA game."""
    conn = get_conn()

    # Check if game already exists -- if so, just UPDATE the provided fields
    existing = conn.execute("SELECT 1 FROM nba_games WHERE game_id = ?",
                            (game_id,)).fetchone()

    if existing:
        # Only update fields that were explicitly provided
        updates = {k: v for k, v in kwargs.items() if v is not None}
        if updates:
            set_clause = ", ".join(f"{k} = ?" for k in updates)
            conn.execute(
                f"UPDATE nba_games SET {set_clause} WHERE game_id = ?",
                list(updates.values()) + [game_id]
            )
            conn.commit()
        return

    # New game -- INSERT with all provided fields
    fields = [
        "date", "home_team_id", "away_team_id", "home_score", "away_score",
        "home_q1", "away_q1", "home_q2", "away_q2",
        "home_q3", "away_q3", "home_q4", "away_q4",
        "status", "season", "home_pace", "away_pace",
    ]
    values = {k: kwargs.get(k) for k in fields}
    values["game_id"] = game_id

    # Skip if missing required fields
    if not values.get("date"):
        logger.warning("Cannot insert game %s without date", game_id)
        return

    cols = ", ".join(values.keys())
    placeholders = ", ".join(["?"] * len(values))

    conn.execute(f"""
        INSERT OR IGNORE INTO nba_games ({cols}) VALUES ({placeholders})
    """, list(values.values()))
    conn.commit()


def upsert_q1_stats(team_id: int, season: int, **stats) -> None:
    """Insert or update Q1 profile stats for a team/season."""
    conn = get_conn()
    conn.execute("""
        INSERT INTO nba_q1_stats (
            team_id, season, games,
            q1_ppg, q1_opp_ppg, q1_margin,
            q1_home_ppg, q1_home_opp_ppg,
            q1_away_ppg, q1_away_opp_ppg,
            q1_cover_pct, q1_over_pct,
            pace, off_rating, def_rating,
            fg_pct, three_pct, ft_rate, reb_rate,
            fast_start_pct, slow_start_pct
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(team_id, season) DO UPDATE SET
            games=excluded.games,
            q1_ppg=excluded.q1_ppg, q1_opp_ppg=excluded.q1_opp_ppg,
            q1_margin=excluded.q1_margin,
            q1_home_ppg=excluded.q1_home_ppg,
            q1_home_opp_ppg=excluded.q1_home_opp_ppg,
            q1_away_ppg=excluded.q1_away_ppg,
            q1_away_opp_ppg=excluded.q1_away_opp_ppg,
            q1_cover_pct=excluded.q1_cover_pct,
            q1_over_pct=excluded.q1_over_pct,
            pace=excluded.pace, off_rating=excluded.off_rating,
            def_rating=excluded.def_rating,
            fg_pct=excluded.fg_pct, three_pct=excluded.three_pct,
            ft_rate=excluded.ft_rate, reb_rate=excluded.reb_rate,
            fast_start_pct=excluded.fast_start_pct,
            slow_start_pct=excluded.slow_start_pct
    """, (
        team_id, season,
        stats.get("games", 0),
        stats.get("q1_ppg"), stats.get("q1_opp_ppg"),
        stats.get("q1_margin"),
        stats.get("q1_home_ppg"), stats.get("q1_home_opp_ppg"),
        stats.get("q1_away_ppg"), stats.get("q1_away_opp_ppg"),
        stats.get("q1_cover_pct"), stats.get("q1_over_pct"),
        stats.get("pace"), stats.get("off_rating"),
        stats.get("def_rating"),
        stats.get("fg_pct"), stats.get("three_pct"),
        stats.get("ft_rate"), stats.get("reb_rate"),
        stats.get("fast_start_pct"), stats.get("slow_start_pct"),
    ))
    conn.commit()


def compute_q1_stats_from_games(team_id: int, season: int) -> dict | None:
    """
    Aggregate Q1 stats from completed games for a team/season.
    Returns the computed stats dict and upserts into nba_q1_stats.
    """
    conn = get_conn()

    # All completed games for this team in this season with Q1 scores
    rows = conn.execute("""
        SELECT * FROM nba_games
        WHERE (home_team_id = ? OR away_team_id = ?)
          AND season = ? AND status = 'final'
          AND home_q1 IS NOT NULL AND away_q1 IS NOT NULL
        ORDER BY date
    """, (team_id, team_id, season)).fetchall()

    if not rows:
        return None

    games = [dict(r) for r in rows]
    total = len(games)

    # Accumulators
    q1_scored = []
    q1_allowed = []
    q1_home_scored = []
    q1_home_allowed = []
    q1_away_scored = []
    q1_away_allowed = []
    leading_q1 = 0
    trailing_q1 = 0

    for g in games:
        is_home = g["home_team_id"] == team_id
        if is_home:
            scored = g["home_q1"]
            allowed = g["away_q1"]
            q1_home_scored.append(scored)
            q1_home_allowed.append(allowed)
        else:
            scored = g["away_q1"]
            allowed = g["home_q1"]
            q1_away_scored.append(scored)
            q1_away_allowed.append(allowed)

        q1_scored.append(scored)
        q1_allowed.append(allowed)

        if scored > allowed:
            leading_q1 += 1
        elif scored < allowed:
            trailing_q1 += 1

    def _avg(vals: list) -> float | None:
        return round(sum(vals) / len(vals), 2) if vals else None

    stats = {
        "games": total,
        "q1_ppg": _avg(q1_scored),
        "q1_opp_ppg": _avg(q1_allowed),
        "q1_margin": _avg([s - a for s, a in zip(q1_scored, q1_allowed)]),
        "q1_home_ppg": _avg(q1_home_scored),
        "q1_home_opp_ppg": _avg(q1_home_allowed),
        "q1_away_ppg": _avg(q1_away_scored),
        "q1_away_opp_ppg": _avg(q1_away_allowed),
        "q1_cover_pct": None,  # requires odds data to compute
        "q1_over_pct": None,   # requires odds data to compute
        "pace": None,          # filled by team stats if available
        "off_rating": None,
        "def_rating": None,
        "fg_pct": None,
        "three_pct": None,
        "ft_rate": None,
        "reb_rate": None,
        "fast_start_pct": round(leading_q1 / total, 3) if total else None,
        "slow_start_pct": round(trailing_q1 / total, 3) if total else None,
    }

    upsert_q1_stats(team_id, season, **stats)
    return stats
