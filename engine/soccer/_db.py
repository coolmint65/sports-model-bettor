"""Per-league SQLite for the soccer framework.

Each league gets its own ``data/soccer/<league>.db`` so a schema change
only blocks the one league that needs the migration. Thread-local
connections + WAL mirror the basketball framework.

Schema notes (soccer-specific):

  matches.home_score / away_score are stored separately (not a margin)
  because every soccer market — 1X2, OU, BTTS, correct score — derives
  from the two numbers. NBA-style margin-only storage would force a
  goal-pair reconstruction at every pick-settle pass.

  match_events is reserved for future per-event ingest (goal minute,
  cards) but kept empty until a market needs it; today we only need the
  final score. The table exists so adding cards/cornering markets later
  doesn't require an ALTER TABLE on a million-row backfill.

  picks.bet_type taxonomy:
      ML / 1X2  — home/draw/away three-way moneyline
      OU        — over/under total goals (line in `pick` field)
      BTTS      — both teams to score yes/no
      AH        — Asian handicap (later)
      DC        — double chance (later)
      DNB       — draw no bet (later)
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

from ._config import LEAGUE_REGISTRY, get_league_config

logger = logging.getLogger(__name__)


_local = threading.local()


_DDL = """
CREATE TABLE IF NOT EXISTS teams (
    id            INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    short_name    TEXT,
    abbreviation  TEXT,
    country       TEXT,
    logo_url      TEXT,
    primary_color TEXT,
    external_id   TEXT,        -- source-system id (ESPN, SofaScore, etc.)
    updated_at    TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_teams_name ON teams(name);
CREATE INDEX IF NOT EXISTS idx_teams_external ON teams(external_id);


CREATE TABLE IF NOT EXISTS matches (
    -- One row per fixture. `id` is the source's stable event id (ESPN
    -- event ids are int-friendly so we use them directly; SofaScore
    -- and football-data sources fall through `external_id` instead and
    -- we mint a local rowid).
    id              INTEGER PRIMARY KEY,
    external_id     TEXT,
    date            TEXT NOT NULL,        -- match local-day in ISO 'YYYY-MM-DD'
    start_time      TEXT,                 -- ISO 8601 UTC kickoff
    home_team_id    INTEGER NOT NULL,
    away_team_id    INTEGER NOT NULL,
    home_score      INTEGER,              -- final regulation+stoppage
    away_score      INTEGER,
    home_score_ht   INTEGER,              -- score at halftime (1st-half markets)
    away_score_ht   INTEGER,
    -- Optional ET/PEN columns. Cups + tournament knockouts only.
    home_score_et   INTEGER,              -- after extra time
    away_score_et   INTEGER,
    home_pens       INTEGER,              -- shootout result
    away_pens       INTEGER,
    has_extra_time  INTEGER DEFAULT 0,
    has_shootout    INTEGER DEFAULT 0,
    status          TEXT,                 -- scheduled / live / final / postponed / cancelled
    status_detail   TEXT,                 -- 'HT' / '90+3' / 'FT' / 'AET' / 'PEN'
    season          INTEGER,
    matchweek       INTEGER,              -- gameweek number when applicable
    competition     TEXT,                 -- 'league' / 'cup' / 'group_stage' / etc.
    round_name      TEXT,                 -- 'Group A' / 'Round of 16' / 'Final'
    venue           TEXT,
    neutral_site    INTEGER DEFAULT 0,    -- WC group stage, finals at neutral grounds
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (home_team_id) REFERENCES teams(id),
    FOREIGN KEY (away_team_id) REFERENCES teams(id)
);
CREATE INDEX IF NOT EXISTS idx_matches_date     ON matches(date);
CREATE INDEX IF NOT EXISTS idx_matches_status   ON matches(status);
CREATE INDEX IF NOT EXISTS idx_matches_season   ON matches(season);
CREATE INDEX IF NOT EXISTS idx_matches_external ON matches(external_id);


CREATE TABLE IF NOT EXISTS match_events (
    -- Reserved for per-event detail (goal minute, scorer, cards). The
    -- final-score model doesn't need it; markets that do (HT/FT, anytime
    -- scorer, cards) will populate this lazily once we wire them.
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id     INTEGER NOT NULL,
    minute       INTEGER,
    period       TEXT,                    -- '1H' / '2H' / 'ET1' / 'ET2' / 'PEN'
    event_type   TEXT,                    -- 'goal' / 'yellow' / 'red' / 'penalty_goal' / etc.
    team_id      INTEGER,
    player_name  TEXT,                    -- raw name; player_id table comes later
    description  TEXT,
    FOREIGN KEY (match_id) REFERENCES matches(id)
);
CREATE INDEX IF NOT EXISTS idx_events_match ON match_events(match_id);


CREATE TABLE IF NOT EXISTS picks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    date          TEXT NOT NULL,            -- pick lock date (UTC)
    match_id      INTEGER NOT NULL,
    matchup       TEXT NOT NULL,            -- "HOME vs AWAY" display string
    bet_type      TEXT NOT NULL,            -- ML / OU / BTTS / AH / DC / DNB
    pick          TEXT NOT NULL,            -- display ("Home", "Draw", "Over 2.5", "BTTS Yes")
    side          TEXT,                     -- 'home' / 'draw' / 'away' / 'over' / 'under' / 'yes' / 'no'
    line          REAL,                     -- OU / AH numeric (NULL for 1X2)
    model_prob    REAL,
    edge          REAL,
    odds          INTEGER,
    closing_odds  INTEGER,
    result        TEXT,                     -- W / L / P / V
    profit        REAL,
    created_at    TEXT DEFAULT (datetime('now')),
    settled_at    TEXT,
    FOREIGN KEY (match_id) REFERENCES matches(id)
);
-- FULL UNIQUE. Was partial-on-pending which let the recorder spam
-- the same family after settle. match_id keyed (no date) because a
-- match still has a single canonical bet regardless of when the
-- slate ticked. 2026-06-10 hardening pass.
CREATE UNIQUE INDEX IF NOT EXISTS uq_picks_family
    ON picks(match_id, bet_type, pick);
CREATE INDEX IF NOT EXISTS idx_picks_date  ON picks(date);
CREATE INDEX IF NOT EXISTS idx_picks_match ON picks(match_id);


CREATE TABLE IF NOT EXISTS team_elo (
    -- Team-level rolling Elo, one row per team. Updated by the
    -- backfill + per-match settle path. Used by the predictor as the
    -- skill prior that goal-rate Poisson lambdas are anchored on.
    team_id        INTEGER PRIMARY KEY,
    elo            REAL NOT NULL,
    matches_played INTEGER DEFAULT 0,
    last_match_id  INTEGER,
    updated_at     TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (team_id) REFERENCES teams(id)
);
"""


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_DDL)
    # Lazy column adds — applied after the CREATE TABLE IF NOT EXISTS
    # block so existing databases pick up new columns without a manual
    # migration. Each addition is wrapped in try/except so re-running
    # is idempotent.
    for ddl in (
        # 'home' = home team at true home venue (full +home_adv tilt)
        # 'neutral' = neither team at home (no tilt)
        # 'away' = away team at true home venue (-home_adv tilt; the
        #          predictor flips the sign so the away side gets the
        #          boost rather than zeroing it out, which loses signal
        #          on host nations playing as the "away" label at their
        #          own ground — discovered on 2026 WC).
        "ALTER TABLE matches ADD COLUMN home_side TEXT",
        "ALTER TABLE matches ADD COLUMN venue_country TEXT",
    ):
        try:
            conn.execute(ddl)
        except sqlite3.OperationalError:
            pass
    conn.commit()


def get_conn(league: str) -> sqlite3.Connection:
    """Thread-local DB connection for ``league``. Creates the file +
    schema on first call."""
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
    _init_schema(conn)
    cache[league] = conn
    return conn


# Naming helpers — kept for parity with engine.basketball._db so any
# cross-sport helper can stay sport-agnostic.

def teams_table(league: str) -> str:  # noqa: ARG001
    return "teams"


def matches_table(league: str) -> str:  # noqa: ARG001
    return "matches"


def picks_table(league: str) -> str:  # noqa: ARG001
    return "picks"
