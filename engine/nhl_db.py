"""
SQLite database layer for NHL data.

Stores teams, players (goalies + skaters), games, goalie stats,
skater stats, team stats, and model calibration config.
"""

import logging
import sqlite3
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "nhl.db"

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
    -- NHL teams
    CREATE TABLE IF NOT EXISTS nhl_teams (
        id INTEGER PRIMARY KEY,  -- NHL API team ID
        name TEXT NOT NULL,
        abbreviation TEXT NOT NULL,
        city TEXT,
        division TEXT,
        conference TEXT,
        venue TEXT
    );

    -- NHL players (goalies + skaters)
    CREATE TABLE IF NOT EXISTS nhl_players (
        id INTEGER PRIMARY KEY,  -- NHL API player ID
        name TEXT NOT NULL,
        team_id INTEGER,
        position TEXT,  -- G, D, C, LW, RW
        shoots_catches TEXT,  -- L or R
        active INTEGER DEFAULT 1
    );

    -- NHL games
    CREATE TABLE IF NOT EXISTS nhl_games (
        game_id INTEGER PRIMARY KEY,
        date TEXT NOT NULL,
        home_team_id INTEGER,
        away_team_id INTEGER,
        home_score INTEGER,
        away_score INTEGER,
        home_goalie_id INTEGER,
        away_goalie_id INTEGER,
        status TEXT DEFAULT 'scheduled',  -- scheduled, live, final
        home_shots INTEGER,
        away_shots INTEGER,
        home_pp_goals INTEGER, home_pp_opps INTEGER,
        away_pp_goals INTEGER, away_pp_opps INTEGER,
        home_faceoff_pct REAL,
        away_faceoff_pct REAL,
        home_hits INTEGER, away_hits INTEGER,
        home_blocks INTEGER, away_blocks INTEGER,
        season INTEGER,
        game_type INTEGER DEFAULT 2  -- 2=regular, 3=playoff
    );

    -- Goalie season stats
    CREATE TABLE IF NOT EXISTS goalie_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL,
        season INTEGER NOT NULL,
        games INTEGER DEFAULT 0,
        wins INTEGER DEFAULT 0,
        losses INTEGER DEFAULT 0,
        ot_losses INTEGER DEFAULT 0,
        save_pct REAL,
        gaa REAL,
        shutouts INTEGER DEFAULT 0,
        saves INTEGER DEFAULT 0,
        shots_against INTEGER DEFAULT 0,
        UNIQUE(player_id, season)
    );

    -- Skater season stats
    CREATE TABLE IF NOT EXISTS skater_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL,
        season INTEGER NOT NULL,
        games INTEGER DEFAULT 0,
        goals INTEGER DEFAULT 0,
        assists INTEGER DEFAULT 0,
        points INTEGER DEFAULT 0,
        plus_minus INTEGER DEFAULT 0,
        pim INTEGER DEFAULT 0,
        shots INTEGER DEFAULT 0,
        hits INTEGER DEFAULT 0,
        blocks INTEGER DEFAULT 0,
        UNIQUE(player_id, season)
    );

    -- NHL team season stats (for standings + model)
    CREATE TABLE IF NOT EXISTS nhl_team_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id INTEGER NOT NULL,
        season INTEGER NOT NULL,
        wins INTEGER DEFAULT 0,
        losses INTEGER DEFAULT 0,
        ot_losses INTEGER DEFAULT 0,
        points INTEGER DEFAULT 0,
        goals_for INTEGER DEFAULT 0,
        goals_against INTEGER DEFAULT 0,
        pp_pct REAL,
        pk_pct REAL,
        shots_per_game REAL,
        shots_against_per_game REAL,
        faceoff_pct REAL,
        corsi_pct REAL,
        fenwick_pct REAL,
        xgf_pct REAL,
        UNIQUE(team_id, season)
    );

    -- NHL model calibration
    CREATE TABLE IF NOT EXISTS nhl_model_config (
        key TEXT PRIMARY KEY,
        value REAL,
        updated_at TEXT DEFAULT (datetime('now'))
    );

    -- Indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_nhl_games_date ON nhl_games(date);
    CREATE INDEX IF NOT EXISTS idx_nhl_games_season ON nhl_games(season);
    CREATE INDEX IF NOT EXISTS idx_nhl_games_home ON nhl_games(home_team_id);
    CREATE INDEX IF NOT EXISTS idx_nhl_games_away ON nhl_games(away_team_id);
    CREATE INDEX IF NOT EXISTS idx_goalie_stats_player ON goalie_stats(player_id, season);
    CREATE INDEX IF NOT EXISTS idx_skater_stats_player ON skater_stats(player_id, season);
    CREATE INDEX IF NOT EXISTS idx_nhl_team_stats_team ON nhl_team_stats(team_id, season);
    CREATE INDEX IF NOT EXISTS idx_nhl_players_team ON nhl_players(team_id);
    """)
    conn.commit()


# ── Convenience helpers ──────────────────────────────────────


def get_nhl_team(team_id: int) -> dict | None:
    """Get an NHL team by its API ID."""
    conn = get_conn()
    row = conn.execute("SELECT * FROM nhl_teams WHERE id = ?",
                       (team_id,)).fetchone()
    return dict(row) if row else None


def get_nhl_team_by_abbr(abbr: str) -> dict | None:
    """Get an NHL team by its abbreviation (e.g. 'TOR')."""
    conn = get_conn()
    row = conn.execute("SELECT * FROM nhl_teams WHERE abbreviation = ?",
                       (abbr.upper(),)).fetchone()
    return dict(row) if row else None


def get_all_nhl_teams() -> list[dict]:
    """Get all NHL teams ordered by name."""
    conn = get_conn()
    rows = conn.execute("SELECT * FROM nhl_teams ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def get_goalie_stats(player_id: int, season: int) -> dict | None:
    """Get goalie season stats."""
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM goalie_stats WHERE player_id = ? AND season = ?",
        (player_id, season)).fetchone()
    return dict(row) if row else None


def get_team_goalies(team_id: int, season: int) -> list[dict]:
    """Get all goalies for a team with their season stats."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT p.*, gs.games, gs.wins, gs.losses, gs.ot_losses,
               gs.save_pct, gs.gaa, gs.shutouts, gs.saves, gs.shots_against
        FROM nhl_players p
        LEFT JOIN goalie_stats gs ON p.id = gs.player_id AND gs.season = ?
        WHERE p.team_id = ? AND p.position = 'G' AND p.active = 1
        ORDER BY gs.games DESC
    """, (season, team_id)).fetchall()
    return [dict(r) for r in rows]


def get_recent_nhl_games(team_id: int, n: int = 10) -> list[dict]:
    """Get last N finished games for a team (home or away)."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT g.*,
               ht.abbreviation as home_abbr, ht.name as home_name,
               at.abbreviation as away_abbr, at.name as away_name
        FROM nhl_games g
        JOIN nhl_teams ht ON g.home_team_id = ht.id
        JOIN nhl_teams at ON g.away_team_id = at.id
        WHERE (g.home_team_id = ? OR g.away_team_id = ?)
          AND g.status = 'final'
        ORDER BY g.date DESC
        LIMIT ?
    """, (team_id, team_id, n)).fetchall()
    return [dict(r) for r in rows]


def get_nhl_team_record(team_id: int, season: int) -> dict | None:
    """Get team W-L-OTL record from team_stats."""
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM nhl_team_stats WHERE team_id = ? AND season = ?",
        (team_id, season)).fetchone()
    return dict(row) if row else None


def get_h2h_nhl(team1_id: int, team2_id: int, seasons: int = 3) -> list[dict]:
    """Get head-to-head games between two teams over the last N seasons."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT g.*,
               ht.abbreviation as home_abbr,
               at.abbreviation as away_abbr
        FROM nhl_games g
        JOIN nhl_teams ht ON g.home_team_id = ht.id
        JOIN nhl_teams at ON g.away_team_id = at.id
        WHERE ((g.home_team_id = ? AND g.away_team_id = ?)
            OR (g.home_team_id = ? AND g.away_team_id = ?))
          AND g.status = 'final'
          AND g.season >= (
              SELECT COALESCE(MAX(season), 0) - ? + 1 FROM nhl_games
          )
        ORDER BY g.date DESC
    """, (team1_id, team2_id, team2_id, team1_id, seasons)).fetchall()
    return [dict(r) for r in rows]


def get_today_nhl_games(date: str) -> list[dict]:
    """Get all games for a given date."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT g.*,
               ht.name as home_name, ht.abbreviation as home_abbr,
               at.name as away_name, at.abbreviation as away_abbr
        FROM nhl_games g
        JOIN nhl_teams ht ON g.home_team_id = ht.id
        JOIN nhl_teams at ON g.away_team_id = at.id
        WHERE g.date = ?
        ORDER BY g.game_id
    """, (date,)).fetchall()
    return [dict(r) for r in rows]


# ── Upsert helpers ───────────────────────────────────────────


def upsert_nhl_team(team_id: int, name: str, abbreviation: str,
                    city: str = "", division: str = "",
                    conference: str = "", venue: str = "") -> None:
    conn = get_conn()
    conn.execute("""
        INSERT INTO nhl_teams (id, name, abbreviation, city, division, conference, venue)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name, abbreviation=excluded.abbreviation,
            city=excluded.city, division=excluded.division,
            conference=excluded.conference, venue=excluded.venue
    """, (team_id, name, abbreviation, city, division, conference, venue))
    conn.commit()


def upsert_nhl_player(player_id: int, name: str, team_id: int | None = None,
                      position: str = "", shoots_catches: str = "") -> None:
    conn = get_conn()
    conn.execute("""
        INSERT INTO nhl_players (id, name, team_id, position, shoots_catches)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name, team_id=excluded.team_id,
            position=excluded.position, shoots_catches=excluded.shoots_catches,
            active=1
    """, (player_id, name, team_id, position, shoots_catches))
    conn.commit()


def upsert_nhl_game(game_id: int, **kwargs) -> None:
    """Insert or update an NHL game."""
    conn = get_conn()
    fields = [
        "date", "home_team_id", "away_team_id", "home_score", "away_score",
        "home_goalie_id", "away_goalie_id", "status",
        "home_shots", "away_shots",
        "home_pp_goals", "home_pp_opps", "away_pp_goals", "away_pp_opps",
        "home_faceoff_pct", "away_faceoff_pct",
        "home_hits", "away_hits", "home_blocks", "away_blocks",
        "season", "game_type",
    ]
    values = {k: kwargs.get(k) for k in fields}
    values["game_id"] = game_id

    cols = ", ".join(values.keys())
    placeholders = ", ".join(["?"] * len(values))
    updates = ", ".join(f"{k}=excluded.{k}" for k in fields if kwargs.get(k) is not None)
    if not updates:
        updates = "game_id=excluded.game_id"  # no-op update

    conn.execute(f"""
        INSERT INTO nhl_games ({cols}) VALUES ({placeholders})
        ON CONFLICT(game_id) DO UPDATE SET {updates}
    """, list(values.values()))
    conn.commit()


def upsert_goalie_stats(player_id: int, season: int, **kwargs) -> None:
    conn = get_conn()
    conn.execute("""
        INSERT INTO goalie_stats (player_id, season, games, wins, losses,
            ot_losses, save_pct, gaa, shutouts, saves, shots_against)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_id, season) DO UPDATE SET
            games=excluded.games, wins=excluded.wins, losses=excluded.losses,
            ot_losses=excluded.ot_losses, save_pct=excluded.save_pct,
            gaa=excluded.gaa, shutouts=excluded.shutouts,
            saves=excluded.saves, shots_against=excluded.shots_against
    """, (
        player_id, season,
        kwargs.get("games", 0), kwargs.get("wins", 0),
        kwargs.get("losses", 0), kwargs.get("ot_losses", 0),
        kwargs.get("save_pct"), kwargs.get("gaa"),
        kwargs.get("shutouts", 0), kwargs.get("saves", 0),
        kwargs.get("shots_against", 0),
    ))
    conn.commit()


def upsert_skater_stats(player_id: int, season: int, **kwargs) -> None:
    conn = get_conn()
    conn.execute("""
        INSERT INTO skater_stats (player_id, season, games, goals, assists,
            points, plus_minus, pim, shots, hits, blocks)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_id, season) DO UPDATE SET
            games=excluded.games, goals=excluded.goals, assists=excluded.assists,
            points=excluded.points, plus_minus=excluded.plus_minus,
            pim=excluded.pim, shots=excluded.shots,
            hits=excluded.hits, blocks=excluded.blocks
    """, (
        player_id, season,
        kwargs.get("games", 0), kwargs.get("goals", 0),
        kwargs.get("assists", 0), kwargs.get("points", 0),
        kwargs.get("plus_minus", 0), kwargs.get("pim", 0),
        kwargs.get("shots", 0), kwargs.get("hits", 0),
        kwargs.get("blocks", 0),
    ))
    conn.commit()


def upsert_nhl_team_stats(team_id: int, season: int, **kwargs) -> None:
    conn = get_conn()
    conn.execute("""
        INSERT INTO nhl_team_stats (team_id, season, wins, losses, ot_losses,
            points, goals_for, goals_against, pp_pct, pk_pct,
            shots_per_game, shots_against_per_game, faceoff_pct,
            corsi_pct, fenwick_pct, xgf_pct)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(team_id, season) DO UPDATE SET
            wins=excluded.wins, losses=excluded.losses,
            ot_losses=excluded.ot_losses, points=excluded.points,
            goals_for=excluded.goals_for, goals_against=excluded.goals_against,
            pp_pct=excluded.pp_pct, pk_pct=excluded.pk_pct,
            shots_per_game=excluded.shots_per_game,
            shots_against_per_game=excluded.shots_against_per_game,
            faceoff_pct=excluded.faceoff_pct,
            corsi_pct=excluded.corsi_pct, fenwick_pct=excluded.fenwick_pct,
            xgf_pct=excluded.xgf_pct
    """, (
        team_id, season,
        kwargs.get("wins", 0), kwargs.get("losses", 0),
        kwargs.get("ot_losses", 0), kwargs.get("points", 0),
        kwargs.get("goals_for", 0), kwargs.get("goals_against", 0),
        kwargs.get("pp_pct"), kwargs.get("pk_pct"),
        kwargs.get("shots_per_game"), kwargs.get("shots_against_per_game"),
        kwargs.get("faceoff_pct"),
        kwargs.get("corsi_pct"), kwargs.get("fenwick_pct"),
        kwargs.get("xgf_pct"),
    ))
    conn.commit()
