"""
Live picks DDL + per-sport DB router.

Schema mirrors the prematch ``nba_picks`` / ``nhl_picks`` tables (id,
game_id, date, matchup, bet_type, pick, model_prob, edge, odds, result,
profit, created_at, settled_at, closing_odds) and adds the snapshot
columns needed for live betting:

    pick_at_period       INTEGER   period at lock time (1-4 NBA, 1-3 NHL)
    pick_at_clock        TEXT      display clock at lock ("8:42")
    pick_at_clock_secs   INTEGER   parsed seconds-remaining-in-period
    pick_at_home_score   INTEGER   home score at lock
    pick_at_away_score   INTEGER   away score at lock
    pick_at_remaining_s  INTEGER   regulation seconds left at lock
    market_scope         TEXT      'full' | 'Q2'..'H2' for NBA;
                                   'full' | 'P1'..'P3' for NHL

The market_scope drives settlement: 'full' settles on game final;
'Q3' settles on period > 3; 'P2' settles on period > 2; 'H1' settles
on period > 2; 'H2' settles on completed.

Both DBs (nba.db, nhl.db) hold their own live_picks_<sport> table.
MLB live betting is out of scope for Phase 3.
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)


_DDL_NBA = """
CREATE TABLE IF NOT EXISTS live_picks_nba (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id              TEXT    NOT NULL,
    date                 TEXT    NOT NULL,
    matchup              TEXT    NOT NULL,
    bet_type             TEXT    NOT NULL,
    pick                 TEXT    NOT NULL,
    model_prob           REAL,
    edge                 REAL,
    odds                 INTEGER NOT NULL,
    result               TEXT,
    profit               REAL,
    created_at           TEXT    NOT NULL,
    settled_at           TEXT,
    closing_odds         INTEGER,
    pick_at_period       INTEGER,
    pick_at_clock        TEXT,
    pick_at_clock_secs   INTEGER,
    pick_at_home_score   INTEGER,
    pick_at_away_score   INTEGER,
    pick_at_remaining_s  INTEGER,
    market_scope         TEXT NOT NULL DEFAULT 'full'
);
CREATE INDEX IF NOT EXISTS idx_live_picks_nba_pending
    ON live_picks_nba (result) WHERE result IS NULL;
CREATE INDEX IF NOT EXISTS idx_live_picks_nba_date
    ON live_picks_nba (date);
"""

_DDL_NHL = """
CREATE TABLE IF NOT EXISTS live_picks_nhl (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id              TEXT    NOT NULL,
    date                 TEXT    NOT NULL,
    matchup              TEXT    NOT NULL,
    bet_type             TEXT    NOT NULL,
    pick                 TEXT    NOT NULL,
    model_prob           REAL,
    edge                 REAL,
    odds                 INTEGER NOT NULL,
    result               TEXT,
    profit               REAL,
    created_at           TEXT    NOT NULL,
    settled_at           TEXT,
    closing_odds         INTEGER,
    pick_at_period       INTEGER,
    pick_at_clock        TEXT,
    pick_at_clock_secs   INTEGER,
    pick_at_home_score   INTEGER,
    pick_at_away_score   INTEGER,
    pick_at_remaining_s  INTEGER,
    market_scope         TEXT NOT NULL DEFAULT 'full'
);
CREATE INDEX IF NOT EXISTS idx_live_picks_nhl_pending
    ON live_picks_nhl (result) WHERE result IS NULL;
CREATE INDEX IF NOT EXISTS idx_live_picks_nhl_date
    ON live_picks_nhl (date);
"""


_BB_FRAMEWORK_SPORTS = ("wnba", "ncaam", "afl")


def _ddl_for(sport: str) -> str:
    """Return a CREATE TABLE statement for ``sport``'s live picks table.
    NBA/NHL keep their canonical names for backwards compatibility;
    basketball-framework leagues (WNBA/NCAAM/AFL) use the same shape
    under live_picks_<sport>."""
    sport = sport.lower()
    if sport == "nba":
        return _DDL_NBA
    if sport == "nhl":
        return _DDL_NHL
    tbl = f"live_picks_{sport}"
    return f"""
CREATE TABLE IF NOT EXISTS {tbl} (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id              TEXT    NOT NULL,
    date                 TEXT    NOT NULL,
    matchup              TEXT    NOT NULL,
    bet_type             TEXT    NOT NULL,
    pick                 TEXT    NOT NULL,
    model_prob           REAL,
    edge                 REAL,
    odds                 INTEGER NOT NULL,
    result               TEXT,
    profit               REAL,
    created_at           TEXT    NOT NULL,
    settled_at           TEXT,
    closing_odds         INTEGER,
    pick_at_period       INTEGER,
    pick_at_clock        TEXT,
    pick_at_clock_secs   INTEGER,
    pick_at_home_score   INTEGER,
    pick_at_away_score   INTEGER,
    pick_at_remaining_s  INTEGER,
    market_scope         TEXT NOT NULL DEFAULT 'full'
);
CREATE INDEX IF NOT EXISTS idx_{tbl}_pending
    ON {tbl} (result) WHERE result IS NULL;
CREATE INDEX IF NOT EXISTS idx_{tbl}_date
    ON {tbl} (date);
"""


def get_conn(sport: str):
    """Return the per-sport SQLite connection. Reuses the canonical
    sport DB so tracker rows live next to schedule/games tables."""
    sport = sport.lower()
    if sport == "nba":
        from ..nba_db import get_conn as _nba
        return _nba()
    if sport == "nhl":
        from ..nhl_db import get_conn as _nhl
        return _nhl()
    if sport in _BB_FRAMEWORK_SPORTS:
        from ..basketball._db import get_conn as _bb
        return _bb(sport)
    raise ValueError(f"live_tracker: unsupported sport {sport!r}")


def ensure_table(sport: str) -> None:
    """Idempotent DDL. Call at startup of any reader/writer path.

    Also runs lightweight ALTER TABLE migrations for columns added
    after the original schema landed — SQLite ignores ALTER ADD COLUMN
    on duplicates so this is safe to call repeatedly.
    """
    sport = sport.lower()
    conn = get_conn(sport)
    conn.executescript(_ddl_for(sport))
    _migrate_conviction_score(sport, conn)


def _migrate_conviction_score(sport: str, conn) -> None:
    """Add ``conviction_score`` column to live_picks_<sport> when
    missing. Per-pick conviction (prob² × min(edge, 12) / 30) gets
    stored at lock time so post-settle reports can analyse
    conviction-vs-result without recomputing.

    Idempotent — pragma-checks the existing column list before
    altering.
    """
    tbl = table_name(sport)
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({tbl})").fetchall()}
    if "conviction_score" not in cols:
        try:
            conn.execute(f"ALTER TABLE {tbl} ADD COLUMN conviction_score REAL")
            conn.commit()
        except Exception as e:
            logger.warning("conviction_score migration on %s failed: %s",
                           tbl, e)
    # Phase 5e/5f triangulation columns. Each intermission pick stores
    # what each layer (analytical / live_gbm / state_mc) predicted so
    # we can auto-tune ensemble weights once 30 days of settled rows
    # have accumulated. NULL on continuous-live picks where the
    # triangulation isn't computed; intermission picks always populate.
    for col in ("model_prob_analytical", "model_prob_gbm",
                 "model_prob_state_mc"):
        if col not in cols:
            try:
                conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} REAL")
                conn.commit()
            except Exception as e:
                logger.warning("%s migration on %s failed: %s",
                               col, tbl, e)


def table_name(sport: str) -> str:
    """Canonical table name per sport. Centralised so query strings
    don't drift across modules."""
    s = sport.lower()
    if s in ("nba", "nhl"):
        return f"live_picks_{s}"
    if s in _BB_FRAMEWORK_SPORTS:
        return f"live_picks_{s}"
    raise KeyError(f"live_tracker: unsupported sport {sport!r}")
