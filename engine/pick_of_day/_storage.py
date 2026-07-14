"""POTD schema + connection helpers.

Two physical tables per sport (only NBA uses both):
  - pick_of_day        — Q1 view (NBA) + the only view (MLB / NHL)
  - pick_of_day_full   — NBA full-game view, sibling table

Both share the same column shape: matchup, bet_type, pick, odds, edge,
model_prob, kelly_pct, reasoning, result, profit, closing_odds,
closing_odds_updated_at, created_at, settled_at. The full sibling
exists because the Q1 and Full POTD coexist on the same date for NBA;
keeping them in one table with a `view` column would force every read
to filter on it and would have broken every existing call site.
"""

from __future__ import annotations


def _get_conn(sport: str):
    """Get DB connection for the given sport.

    Basketball framework leagues (wnba, euroleague, ncaam, etc.) route
    to ``engine.basketball._db.get_conn(league)`` — each lives in its
    own DB at ``data/basketball/<league>.db`` and shares the framework's
    uniform schema."""
    if sport == "mlb":
        from ..db import get_conn
        return get_conn()
    elif sport == "nhl":
        from ..nhl_db import get_conn
        return get_conn()
    elif sport == "nba":
        from ..nba_db import get_conn
        return get_conn()
    elif sport == "tennis":
        from ..tennis_db import get_conn, ensure_tables
        ensure_tables()
        return get_conn()
    else:
        # Basketball framework league dispatch.
        from ..basketball import LEAGUE_REGISTRY
        from ..basketball._db import get_conn as bb_get_conn
        if sport in LEAGUE_REGISTRY:
            return bb_get_conn(sport)
        # Hockey framework league dispatch (AHL / PWHL / future). Each
        # uses the per-league theScore-backed DB at engine.sports.{league}.db.
        try:
            from ..hockey import HOCKEY_LEAGUE_REGISTRY
            if sport in HOCKEY_LEAGUE_REGISTRY:
                mod = __import__(f"engine.sports.{sport}.db",
                                  fromlist=["get_conn"])
                return mod.get_conn()
        except Exception:
            pass
        # Baseball framework dispatch (college today, others later).
        # Same shape as basketball/hockey — per-league DB under
        # data/baseball/<league>.db.
        try:
            from ..baseball import LEAGUE_REGISTRY as _BB_REG
            from ..baseball._db import get_conn as base_get_conn
            if sport in _BB_REG:
                return base_get_conn(sport)
        except Exception:
            pass
        # Soccer framework dispatch. Per-league DB under
        # data/soccer/<league>.db; shared "picks" table convention.
        try:
            from ..soccer import LEAGUE_REGISTRY as _SOC_REG
            from ..soccer._db import get_conn as soc_get_conn
            if sport in _SOC_REG:
                return soc_get_conn(sport)
        except Exception:
            pass
        # Football framework dispatch (UFL today, NFL/NCAAF later).
        # Per-league DB under data/football/<league>.db.
        try:
            from ..football import LEAGUE_REGISTRY as _FB_REG
            from ..football._db import get_conn as fb_get_conn
            if sport in _FB_REG:
                return fb_get_conn(sport)
        except Exception:
            pass
        raise ValueError(f"Unknown sport: {sport}")


def _ensure_potd_table(sport: str) -> None:
    """Create the POTD table(s) if they don't exist + run idempotent
    column-add migrations for closing_odds tracking.

    Tennis special case: ATP and WTA POTDs coexist on the same date
    (one per tour), so the tennis schema uses a composite uniqueness
    constraint on (date, tour) rather than UNIQUE(date) alone. Other
    sports use the original schema unchanged."""
    conn = _get_conn(sport)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pick_of_day (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL UNIQUE,
            game_id     TEXT,
            matchup     TEXT NOT NULL,
            bet_type    TEXT NOT NULL,
            pick        TEXT NOT NULL,
            model_prob  REAL,
            edge        REAL,
            odds        INTEGER,
            kelly_pct   REAL,
            reasoning   TEXT,
            result      TEXT,
            profit      REAL,
            created_at  TEXT DEFAULT (datetime('now')),
            settled_at  TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_potd_date ON pick_of_day(date)")
    cols = {r[1] for r in conn.execute("PRAGMA table_info(pick_of_day)").fetchall()}
    if "closing_odds" not in cols:
        conn.execute("ALTER TABLE pick_of_day ADD COLUMN closing_odds INTEGER")
    if "closing_odds_updated_at" not in cols:
        conn.execute("ALTER TABLE pick_of_day ADD COLUMN closing_odds_updated_at TEXT")
    conn.commit()
    if sport == "tennis":
        _migrate_tennis_per_tour(conn)
    if sport == "nba":
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pick_of_day_full (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT NOT NULL UNIQUE,
                game_id     TEXT,
                matchup     TEXT NOT NULL,
                bet_type    TEXT NOT NULL,
                pick        TEXT NOT NULL,
                model_prob  REAL,
                edge        REAL,
                odds        INTEGER,
                kelly_pct   REAL,
                reasoning   TEXT,
                result      TEXT,
                profit      REAL,
                closing_odds INTEGER,
                closing_odds_updated_at TEXT,
                created_at  TEXT DEFAULT (datetime('now')),
                settled_at  TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_potd_full_date ON pick_of_day_full(date)")
        conn.commit()


def _migrate_tennis_per_tour(conn) -> None:
    """One-time migration for tennis pick_of_day: drop UNIQUE(date),
    add `tour` column, install UNIQUE(date, tour) so ATP + WTA POTDs
    coexist for the same date.

    Idempotent: detects whether the migration has run by checking for
    the `tour` column; if present the migration is skipped.
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(pick_of_day)").fetchall()}
    if "tour" in cols:
        return
    conn.execute("ALTER TABLE pick_of_day RENAME TO pick_of_day_legacy")
    conn.execute("""
        CREATE TABLE pick_of_day (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL,
            tour        TEXT,
            game_id     TEXT,
            matchup     TEXT NOT NULL,
            bet_type    TEXT NOT NULL,
            pick        TEXT NOT NULL,
            model_prob  REAL,
            edge        REAL,
            odds        INTEGER,
            kelly_pct   REAL,
            reasoning   TEXT,
            result      TEXT,
            profit      REAL,
            closing_odds INTEGER,
            closing_odds_updated_at TEXT,
            created_at  TEXT DEFAULT (datetime('now')),
            settled_at  TEXT
        )
    """)
    legacy_cols = {r[1] for r in conn.execute(
        "PRAGMA table_info(pick_of_day_legacy)").fetchall()}
    common = [c for c in (
        "id", "date", "game_id", "matchup", "bet_type", "pick",
        "model_prob", "edge", "odds", "kelly_pct", "reasoning",
        "result", "profit", "closing_odds", "closing_odds_updated_at",
        "created_at", "settled_at",
    ) if c in legacy_cols]
    if common:
        col_list = ", ".join(common)
        conn.execute(
            f"INSERT INTO pick_of_day ({col_list}) "
            f"SELECT {col_list} FROM pick_of_day_legacy"
        )
    conn.execute("DROP TABLE pick_of_day_legacy")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_potd_date ON pick_of_day(date)")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_potd_date_tour "
        "ON pick_of_day(date, COALESCE(tour, ''))"
    )
    conn.commit()


def _potd_table(sport: str, view: str = "q1") -> str:
    """NBA Full-game POTD lives in pick_of_day_full; everything else
    in pick_of_day."""
    if sport == "nba" and view == "full":
        return "pick_of_day_full"
    return "pick_of_day"


def _implied_from_odds(odds: int) -> float:
    """American → implied probability. Returns 0.5 for falsy odds."""
    if not odds:
        return 0.5
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 100 / (odds + 100)
