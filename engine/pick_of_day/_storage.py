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
    """Get DB connection for the given sport."""
    if sport == "mlb":
        from ..db import get_conn
        return get_conn()
    elif sport == "nhl":
        from ..nhl_db import get_conn
        return get_conn()
    elif sport == "nba":
        from ..nba_db import get_conn
        return get_conn()
    else:
        raise ValueError(f"Unknown sport: {sport}")


def _ensure_potd_table(sport: str) -> None:
    """Create the POTD table(s) if they don't exist + run idempotent
    column-add migrations for closing_odds tracking."""
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
