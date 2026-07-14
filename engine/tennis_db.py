"""
Tennis SQLite DB.

Single ``data/tennis.db`` keyed on ``tour`` ('atp' | 'wta'). ATP and
WTA share schema but never share players or ratings, so they live
side-by-side in one DB to keep schema migrations / readers simple
without forcing a separate file per tour.

Tables
------

tennis_players
    One row per (tour, player_id). player_id matches Sackmann's
    integer ID for that tour. Carries DOB, hand (R/L), height, country
    so downstream features can blend without extra joins.

tennis_matches
    One row per completed match. Key = (tour, match_id) where
    match_id is synthesized as ``{tournament_date}-{tournament_id}-{match_num}``
    so the natural key survives Sackmann re-ingests cleanly.

tennis_elo
    Per-player surface ratings. Updated by engine.tennis_elo trainer.

Why one DB rather than per-tour
-------------------------------
- Schema is identical
- No cross-tour joins needed in the predictor (matches don't span
  tours)
- Reduces config sprawl (one connection, one pragma block)
- Mirrors the project's "one DB per sport" convention — tennis is one
  sport with two tours
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

logger = logging.getLogger(__name__)


_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "tennis.db"
_LOCK = threading.Lock()


def _path() -> Path:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return _DB_PATH


def get_conn() -> sqlite3.Connection:
    """Per-thread connection. WAL so live tracker writes don't block
    backfill / training reads. busy_timeout=10s so concurrent writers
    (backend server holding picks_cache + settler invoking store_results
    at the same time) retry briefly instead of hanging indefinitely.
    Settler froze 2026-05-28 when store_results raced the live picks
    cache writer with no timeout configured."""
    conn = sqlite3.connect(str(_path()), isolation_level=None,
                            timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


_DDL = """
CREATE TABLE IF NOT EXISTS tennis_players (
    tour          TEXT NOT NULL,         -- 'atp' | 'wta'
    player_id     INTEGER NOT NULL,      -- Sackmann ID
    name          TEXT NOT NULL,
    name_first    TEXT,
    name_last     TEXT,
    hand          TEXT,                  -- 'R' | 'L' | 'U'
    dob           TEXT,                  -- YYYY-MM-DD or YYYYMMDD
    country       TEXT,                  -- IOC code
    height_cm     INTEGER,
    PRIMARY KEY (tour, player_id)
);
CREATE INDEX IF NOT EXISTS idx_tennis_players_name
    ON tennis_players (tour, name);

CREATE TABLE IF NOT EXISTS tennis_matches (
    tour              TEXT NOT NULL,
    match_id          TEXT NOT NULL,      -- {tourney_date}-{tourney_id}-{match_num}
    tourney_id        TEXT,
    tourney_name      TEXT,
    tourney_level     TEXT,                -- G/M/A/F/D for ATP; G/T1/T2/... for WTA
    tourney_date      TEXT,                -- YYYY-MM-DD
    surface           TEXT,                -- Hard / Clay / Grass / Carpet
    draw_size         INTEGER,
    best_of           INTEGER,             -- 3 or 5
    round             TEXT,                -- F / SF / QF / R16 / R32 / R64 / R128 / RR / BR
    minutes           INTEGER,             -- match duration
    -- Players (winner = w, loser = l)
    winner_id         INTEGER,
    winner_name       TEXT,
    winner_seed       INTEGER,
    winner_entry      TEXT,                -- WC / Q / LL / etc.
    winner_hand       TEXT,
    winner_age        REAL,
    winner_rank       INTEGER,
    winner_rank_pts   INTEGER,
    loser_id          INTEGER,
    loser_name        TEXT,
    loser_seed        INTEGER,
    loser_entry       TEXT,
    loser_hand        TEXT,
    loser_age         REAL,
    loser_rank        INTEGER,
    loser_rank_pts    INTEGER,
    score             TEXT,                -- '6-3 7-6(5) 6-2'
    -- Match stats (set, match-level totals; per-player columns)
    w_ace             INTEGER,
    w_df              INTEGER,
    w_svpt            INTEGER,
    w_1stIn           INTEGER,
    w_1stWon          INTEGER,
    w_2ndWon          INTEGER,
    w_SvGms           INTEGER,
    w_bpSaved         INTEGER,
    w_bpFaced         INTEGER,
    l_ace             INTEGER,
    l_df              INTEGER,
    l_svpt            INTEGER,
    l_1stIn           INTEGER,
    l_1stWon          INTEGER,
    l_2ndWon          INTEGER,
    l_SvGms           INTEGER,
    l_bpSaved         INTEGER,
    l_bpFaced         INTEGER,
    PRIMARY KEY (tour, match_id)
);
CREATE INDEX IF NOT EXISTS idx_tennis_matches_date
    ON tennis_matches (tour, tourney_date);
CREATE INDEX IF NOT EXISTS idx_tennis_matches_surface
    ON tennis_matches (tour, surface, tourney_date);
CREATE INDEX IF NOT EXISTS idx_tennis_matches_winner
    ON tennis_matches (tour, winner_id, tourney_date);
CREATE INDEX IF NOT EXISTS idx_tennis_matches_loser
    ON tennis_matches (tour, loser_id, tourney_date);

CREATE TABLE IF NOT EXISTS tennis_elo (
    tour          TEXT NOT NULL,
    player_id     INTEGER NOT NULL,
    surface       TEXT NOT NULL,         -- 'all' | 'Hard' | 'Clay' | 'Grass' | 'Carpet'
    rating        REAL NOT NULL,
    rd            REAL NOT NULL,         -- rating deviation
    matches       INTEGER NOT NULL DEFAULT 0,
    last_match    TEXT,                  -- YYYY-MM-DD
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (tour, player_id, surface)
);
CREATE INDEX IF NOT EXISTS idx_tennis_elo_surface
    ON tennis_elo (tour, surface, rating);

-- Today's draw — populated by scrapers.tennis_espn. One row per
-- match (scheduled / live / completed). The picker walks this table
-- to emit picks each morning. Schema mirrors what the team-sport
-- games tables look like so downstream consumers (settler, frontend)
-- have a familiar shape.
CREATE TABLE IF NOT EXISTS tennis_scheduled_matches (
    tour              TEXT NOT NULL,
    match_id          TEXT NOT NULL,      -- ESPN competition id
    date              TEXT NOT NULL,      -- YYYY-MM-DD (local-naive)
    start_time        TEXT,               -- ISO; UTC
    tournament        TEXT,
    tournament_id     TEXT,
    surface           TEXT,               -- Hard / Clay / Grass / Carpet (or NULL when ambiguous)
    best_of           INTEGER,
    round             TEXT,
    status            TEXT NOT NULL DEFAULT 'pre',  -- pre / in / post
    p1_name           TEXT NOT NULL,
    p1_country        TEXT,
    p1_id             INTEGER,             -- Sackmann id (resolved at ingest time)
    p1_image          TEXT,                -- ESPN athlete headshot href
    p1_flag           TEXT,                -- ESPN country flag href
    p2_name           TEXT NOT NULL,
    p2_country        TEXT,
    p2_id             INTEGER,
    p2_image          TEXT,
    p2_flag           TEXT,
    score             TEXT,                -- 'X-Y X-Y' format when status >= post
    winner            TEXT,                -- 'p1' | 'p2' | NULL
    fetched_at        TEXT NOT NULL,
    PRIMARY KEY (tour, match_id)
);
CREATE INDEX IF NOT EXISTS idx_tennis_sch_date
    ON tennis_scheduled_matches (tour, date);
CREATE INDEX IF NOT EXISTS idx_tennis_sch_status
    ON tennis_scheduled_matches (tour, status);

-- Settle-time fallback: results pulled from a non-ESPN source
-- (currently tennisexplorer.com) so HR-supplement picks on
-- Challenger / ITF / sub-tour events have something to resolve
-- against. Sackmann's CSVs cover these tours but only ship
-- annually, so for current-season minor-tour matches this is the
-- only result source. Keyed on (source, source_match_id) so
-- multiple sources can co-exist without colliding.
CREATE TABLE IF NOT EXISTS tennis_match_results (
    source            TEXT NOT NULL,          -- 'tennisexplorer' | future
    source_match_id   TEXT NOT NULL,          -- te match id
    date              TEXT NOT NULL,          -- YYYY-MM-DD
    tour              TEXT NOT NULL,          -- 'atp' | 'wta'
    tournament        TEXT,
    p1_name           TEXT NOT NULL,          -- "Sinner J." style (TE format)
    p1_id             INTEGER,                -- resolved Sackmann id (NULL ok)
    p1_te_id          TEXT,                   -- TE profile slug (for photo fetch)
    p2_name           TEXT NOT NULL,
    p2_id             INTEGER,
    p2_te_id          TEXT,
    winner            TEXT NOT NULL,          -- 'p1' | 'p2'
    score             TEXT,                   -- 'X-Y X-Y' winner-first
    fetched_at        TEXT NOT NULL,
    PRIMARY KEY (source, source_match_id)
);
CREATE INDEX IF NOT EXISTS idx_tennis_results_date
    ON tennis_match_results (date);
CREATE INDEX IF NOT EXISTS idx_tennis_results_players
    ON tennis_match_results (tour, p1_id, p2_id, date);
"""


def ensure_tables() -> None:
    """Idempotent DDL. Call before any reader/writer path."""
    with _LOCK:
        conn = get_conn()
        conn.executescript(_DDL)
        # Additive migrations for tennis_scheduled_matches columns
        # added after the initial schema. SQLite ignores existing
        # columns gracefully via try/except per ALTER.
        for col in ("p1_image", "p1_flag", "p2_image", "p2_flag"):
            try:
                conn.execute(
                    f"ALTER TABLE tennis_scheduled_matches ADD COLUMN {col} TEXT"
                )
            except Exception:
                pass  # column already exists
        # te_id columns added after tennis_match_results shipped — the
        # original schema didn't carry the TE profile slug, which the
        # photo fallback needs to fetch headshots for sub-tour players.
        for col in ("p1_te_id", "p2_te_id"):
            try:
                conn.execute(
                    f"ALTER TABLE tennis_match_results ADD COLUMN {col} TEXT"
                )
            except Exception:
                pass


def get_player_by_id(tour: str, player_id: int) -> dict | None:
    ensure_tables()
    row = get_conn().execute(
        "SELECT * FROM tennis_players WHERE tour = ? AND player_id = ?",
        (tour, int(player_id)),
    ).fetchone()
    return dict(row) if row else None


def get_player_by_name(tour: str, name: str) -> dict | None:
    """Loose name lookup. Matches exact name first, then tries the
    Sackmann-style "First Last" formatting."""
    ensure_tables()
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM tennis_players WHERE tour = ? AND name = ?",
        (tour, name),
    ).fetchone()
    return dict(row) if row else None


# ── Fuzzy name resolver ───────────────────────────────────────

def _normalize_name(s: str) -> str:
    """Strip accents and lowercase. Used for fuzzy comparisons across
    sources (Sackmann, ESPN, HR) which all spell some names slightly
    differently — accents, hyphens, middle names."""
    if not s:
        return ""
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", s)
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    return ascii_only.lower().strip()


# Process-scoped cache of the "active players" rowset. Building it is
# a multi-million-row JOIN against tennis_matches; without caching,
# resolve_player_id rebuilt the list per call (3930 calls × 80ms ≈
# 5 min of pointless work during a 5-day TE settle). Keyed by
# (tour, recent_only); cleared explicitly or via a TTL when the
# Sackmann corpus refreshes.
_active_rows_cache: dict[tuple[str, bool], list] = {}


def clear_resolution_cache() -> None:
    """Drop the active-players cache. Call after a Sackmann corpus
    refresh to pick up newly-arrived players."""
    _active_rows_cache.clear()


def _active_player_rows(conn, tour: str, recent_only: bool) -> list:
    key = (tour, recent_only)
    if key in _active_rows_cache:
        return _active_rows_cache[key]
    if recent_only:
        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=3 * 365)
                  ).strftime("%Y-%m-%d")
        rows = conn.execute(
            """SELECT DISTINCT p.player_id, p.name, p.name_first, p.name_last
               FROM tennis_players p
               JOIN (
                 SELECT winner_id AS pid FROM tennis_matches
                  WHERE tour = ? AND tourney_date >= ?
                 UNION
                 SELECT loser_id AS pid FROM tennis_matches
                  WHERE tour = ? AND tourney_date >= ?
               ) m ON m.pid = p.player_id
               WHERE p.tour = ?""",
            (tour, cutoff, tour, cutoff, tour),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT player_id, name, name_first, name_last "
            "FROM tennis_players WHERE tour = ?",
            (tour,),
        ).fetchall()
    _active_rows_cache[key] = rows
    return rows


def resolve_player_id(tour: str, name: str,
                       *, recent_only: bool = True) -> int | None:
    """Map a player name (from any source) to the canonical Sackmann
    id. Strategy in order:

      1. Exact match on ``tennis_players.name``
      2. Accent-stripped lowercase exact match
      3. Last-name uniquely matches when restricted to active players
         (matches >= 5 in the last 3 years)
      4. Substring match on first-+-last token, again restricted to
         active players

    ``recent_only=True`` filters candidates to players with at least
    one match in the last 3 calendar years before falling back to the
    full roster — keeps "Carlos Alcaraz" from colliding with a 1995
    journeyman who shares part of the name.
    """
    if not name or not tour:
        return None
    ensure_tables()
    conn = get_conn()

    # 1. Exact match
    row = conn.execute(
        "SELECT player_id FROM tennis_players WHERE tour = ? AND name = ?",
        (tour, name),
    ).fetchone()
    if row:
        return int(row["player_id"])

    # Build (or read from cache) the active candidate set.
    active_rows = _active_player_rows(conn, tour, recent_only)

    target = _normalize_name(name)
    target_tokens = target.split()

    # 2. Accent-stripped exact match
    for r in active_rows:
        if _normalize_name(r["name"]) == target:
            return int(r["player_id"])

    # 3. Last-name unique match (most ESPN-style "C. Alcaraz" shorthand
    # collapses cleanly here, and full "Carlos Alcaraz Garfia" matches
    # against last name "Alcaraz" once the accent strip is applied).
    target_last = target_tokens[-1] if target_tokens else ""
    if target_last:
        last_matches = [r for r in active_rows
                        if _normalize_name(r["name_last"] or "") == target_last]
        if len(last_matches) == 1:
            return int(last_matches[0]["player_id"])

    # 4. First + last substring match (handles "Iga Swiatek" matching
    # the canonical "Iga Świątek" after accent strip + the rare case
    # where the last-name-only branch had multiple candidates).
    target_first = target_tokens[0] if target_tokens else ""
    if target_first and target_last:
        substr_matches = [
            r for r in active_rows
            if _normalize_name(r["name_first"] or "").startswith(target_first[:1])
            and _normalize_name(r["name_last"] or "") == target_last
        ]
        if len(substr_matches) == 1:
            return int(substr_matches[0]["player_id"])
        # Multiple → return the one with the most matches (proxy for
        # most-active player by name)
        if substr_matches:
            best = None
            best_n = -1
            for r in substr_matches:
                n = conn.execute(
                    "SELECT COUNT(*) AS c FROM tennis_matches "
                    "WHERE tour = ? AND (winner_id = ? OR loser_id = ?)",
                    (tour, r["player_id"], r["player_id"]),
                ).fetchone()["c"]
                if n > best_n:
                    best_n = n
                    best = r
            if best is not None:
                return int(best["player_id"])

    return None


def head_to_head(tour: str, p1_id: int, p2_id: int,
                 *, surface: str | None = None,
                 limit: int = 10) -> dict:
    """Lifetime H2H between two players.

    Returns ``{p1_wins, p2_wins, last_n: [{date, tournament, surface,
    score, winner_id}]}``. Pass ``surface`` to filter (e.g. 'Clay'
    for surface-specific H2H). ``limit`` caps the most-recent meeting
    list returned for the detail panel.
    """
    ensure_tables()
    conn = get_conn()
    sql = (
        "SELECT tourney_date, tourney_name, surface, score, "
        "       winner_id, loser_id "
        "FROM tennis_matches "
        "WHERE tour = ? "
        "  AND ((winner_id = ? AND loser_id = ?) "
        "       OR (winner_id = ? AND loser_id = ?))"
    )
    params: list = [tour, int(p1_id), int(p2_id), int(p2_id), int(p1_id)]
    if surface:
        sql += " AND surface = ?"
        params.append(surface)
    sql += " ORDER BY tourney_date DESC"
    rows = [dict(r) for r in conn.execute(sql, tuple(params)).fetchall()]
    p1_wins = sum(1 for r in rows if r["winner_id"] == int(p1_id))
    p2_wins = len(rows) - p1_wins
    return {
        "p1_wins": p1_wins,
        "p2_wins": p2_wins,
        "total": len(rows),
        "last_n": rows[: max(1, int(limit))],
    }


def recent_form(tour: str, player_id: int,
                *, surface: str | None = None,
                limit: int = 10) -> dict:
    """Most recent N matches for a player.

    Returns ``{wins, losses, record_str ('W-L'), last_n: [...]}``.
    ``surface`` optionally filters (use the match's actual surface
    for an apples-to-apples form read).
    """
    ensure_tables()
    conn = get_conn()
    sql = (
        "SELECT tourney_date, tourney_name, surface, score, "
        "       winner_id, winner_name, loser_id, loser_name "
        "FROM tennis_matches "
        "WHERE tour = ? AND (winner_id = ? OR loser_id = ?)"
    )
    params: list = [tour, int(player_id), int(player_id)]
    if surface:
        sql += " AND surface = ?"
        params.append(surface)
    sql += " ORDER BY tourney_date DESC LIMIT ?"
    params.append(max(1, int(limit)))
    rows = [dict(r) for r in conn.execute(sql, tuple(params)).fetchall()]
    wins = sum(1 for r in rows if r["winner_id"] == int(player_id))
    losses = len(rows) - wins
    return {
        "wins": wins,
        "losses": losses,
        "record": f"{wins}-{losses}",
        "last_n": rows,
    }


__all__ = [
    "get_conn", "ensure_tables",
    "get_player_by_id", "get_player_by_name",
    "resolve_player_id",
    "head_to_head", "recent_form",
]
