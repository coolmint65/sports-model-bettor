"""Queue-placer database schema — the immutable placement log.

One row per placement attempt (dry-run and real). Never updated except
to fill in the settle-time result. Never deleted — this table is the
audit trail for real money.

Storage: data/queue_placer/placements.db (separate from the sports
DBs so a heavy tracker read never contends with a placement write).
"""
from __future__ import annotations
import sqlite3
import threading
from pathlib import Path


_DB_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "queue_placer" / "placements.db"
)

_DDL = """
CREATE TABLE IF NOT EXISTS placements (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Dedup key: (sport, event_id, bet_type, pick_key). A single (game,
    -- market, selection) fires at most once per calendar day. Lesson #3
    -- from the Table Tennis architecture: prematch and live schedulers
    -- both fire on the same pick → double-stake without this guard.
    dedup_key         TEXT NOT NULL,
    sport             TEXT NOT NULL,
    event_id          TEXT,
    bet_type          TEXT NOT NULL,
    pick              TEXT NOT NULL,
    matchup           TEXT,
    -- What the queue wanted to place
    requested_odds    INTEGER NOT NULL,
    requested_stake_u REAL NOT NULL,
    requested_stake_d REAL NOT NULL,
    -- What HR actually gave us (lesson #4: log what we GOT). NULL
    -- until placement completes.
    placed_odds       INTEGER,
    placed_stake_d    REAL,
    line_drift_pp     REAL,   -- implied-prob delta, positive = worse
    -- Mode + status
    mode              TEXT NOT NULL,   -- 'dry_run' | 'live'
    status            TEXT NOT NULL,   -- see _status_labels below
    reject_reason     TEXT,
    hr_ticket_id      TEXT,
    hr_response_json  TEXT,
    -- Grading (populated by the queue's tracker, not the placer)
    result            TEXT,            -- W | L | P | V
    profit_dollars    REAL,
    settled_at        TEXT,
    -- Timing
    queued_at         TEXT NOT NULL DEFAULT (datetime('now')),
    submitted_at      TEXT,
    completed_at      TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_placements_dedup_day
    ON placements(dedup_key, DATE(queued_at));
CREATE INDEX IF NOT EXISTS idx_placements_status
    ON placements(status, queued_at);
CREATE INDEX IF NOT EXISTS idx_placements_sport_date
    ON placements(sport, DATE(queued_at));
"""


# Valid status values. Documented centrally so the placer + UI + tests
# all agree on the vocabulary.
STATUS_VALUES = frozenset({
    "queued",             # accepted; sitting in the relay queue
    "submitted",          # relay confirmed forward to HR
    "placed",             # HR accepted the bet (ticket id returned)
    "rejected_line",      # HR rejected — line moved beyond tolerance
    "rejected_market",    # HR rejected — market suspended
    "rejected_stake",     # HR rejected — bad stake / insufficient funds
    "rejected_other",     # HR rejected — other reason
    "rejected_dedup",     # Blocked pre-relay — already placed today
    "rejected_cap",       # Blocked pre-relay — cap breached
    "rejected_halt",      # Blocked pre-relay — circuit breaker fired
    "rejected_offline",   # Relay unreachable
    "expired",            # Timed out (game started before placement)
    "dry_run",            # Would-place log only, no relay call
})


_LOCK = threading.Lock()
_conn: sqlite3.Connection | None = None


def get_conn() -> sqlite3.Connection:
    """Return a shared connection to the placements DB. Initializes the
    schema on first call. Thread-safe via a module-level lock."""
    global _conn
    with _LOCK:
        if _conn is None:
            _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            c = sqlite3.connect(str(_DB_PATH),
                                 check_same_thread=False,
                                 isolation_level=None)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL")
            c.executescript(_DDL)
            _conn = c
        return _conn
