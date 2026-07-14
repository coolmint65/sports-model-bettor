"""
Intermission event detector.

Phase 5g. Watches the live game state + PBP and fires a one-shot
event each time a period transitions from active to ended:

  - NBA: end of Q1, end of Q2 (halftime), end of Q3, end of regulation
  - NHL: end of P1, end of P2, end of P3 (regulation)

The signal is what 5h / 5i / 5j subscribe to so the period predictor
runs ONCE per intermission (not on every tick) and writes its picks
to the existing live tracker. Without an edge-triggered detector the
predictor would either over-fire (every poll while clock=0:00) or
under-fire (miss the brief window before the next period starts).

Detection logic
---------------
Two complementary signals — both must reconcile or we don't fire:

  1. ``live_pbp`` carries explicit "Period End" / "End of Nth" plays
     emitted by ESPN at the buzzer. Type-text match catches NBA
     "end period" and NHL "Period End" plays consistently.
  2. ``live_state.status.detail`` carries human strings like
     "End of 1st Quarter", "Halftime", "End of 1st Period". Useful as
     a fallback when ESPN's PBP is laggy.

Either signal alone is enough to fire. The dedupe guard is what
keeps double-fires off the table.

Storage
-------
Fires get persisted to ``live_intermissions`` so:
  - A worker restart doesn't re-fire intermissions that already
    triggered earlier in the same game.
  - The predictor (different process) can read which intermissions
    are queued for action.

Schema::

    live_intermissions(
        sport TEXT NOT NULL,
        game_id TEXT NOT NULL,
        period INTEGER NOT NULL,        -- the period that JUST ENDED
        kind TEXT NOT NULL,             -- 'period_end' / 'halftime'
        detected_at TEXT NOT NULL,      -- UTC ISO
        consumed_at TEXT,               -- predictor stamps when it ran
        PRIMARY KEY (sport, game_id, period)
    )

The ``consumed_at`` column is what 5h/5i flip after they generate
their picks. Without it a worker restart while the predictor was
mid-run could lose the trigger; the row stays unconsumed until
something completes the cycle.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


_DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "live.db"
_LOCK = threading.Lock()


def _conn() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def ensure_intermissions_table() -> None:
    """Idempotent DDL. Called by detect_and_fire so a fresh deploy
    doesn't trip on a missing table."""
    with _LOCK:
        conn = _conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS live_intermissions (
                sport        TEXT NOT NULL,
                game_id      TEXT NOT NULL,
                period       INTEGER NOT NULL,
                kind         TEXT NOT NULL,
                detected_at  TEXT NOT NULL,
                consumed_at  TEXT,
                PRIMARY KEY (sport, game_id, period)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_intermissions_unconsumed "
            "ON live_intermissions(consumed_at) "
            "WHERE consumed_at IS NULL"
        )


# ── State signal ───────────────────────────────────────────────

_PERIOD_END_RE = re.compile(
    r"end\s+of\s+(?:the\s+)?(\d+)(?:st|nd|rd|th)?\s+(?:quarter|period|half)",
    re.IGNORECASE,
)
_HALFTIME_RE = re.compile(r"^\s*halftime\s*$", re.IGNORECASE)


def _state_period_ended(status: dict, sport: str) -> int | None:
    """Read live_state.status and return the period number that just
    ended, or None if not at an intermission boundary."""
    if not status:
        return None
    detail = (status.get("detail") or "").strip()
    if not detail:
        return None
    m = _PERIOD_END_RE.search(detail)
    if m:
        try:
            return int(m.group(1))
        except (TypeError, ValueError):
            return None
    if _HALFTIME_RE.match(detail):
        # Halftime → end of Q2 for 4-quarter sports (NBA/WNBA/AFL), end
        # of H1 (period 1) for NCAAM and other 2-half sports. NHL doesn't
        # have a halftime so this stays None for hockey.
        if sport in ("nba", "wnba", "afl"):
            return 2
        if sport == "ncaam":
            return 1
        return None
    return None


# ── PBP signal ─────────────────────────────────────────────────

_PBP_END_TYPES = {
    # ESPN PBP type_text values seen during 5b live testing
    "period end",      # NHL ("End of 1st Period")
    "end period",      # NBA buzzer
    "end of period",
}


def _pbp_period_ended(plays: list[dict]) -> int | None:
    """If the most recent play in ``plays`` is a period-end marker,
    return that period number. Otherwise None."""
    if not plays:
        return None
    last = plays[-1]
    type_text = (last.get("type_text") or "").strip().lower()
    if type_text in _PBP_END_TYPES:
        return int(last.get("period") or 0) or None
    # Fallback — check the play text for "End of {N}{st,nd,rd,th}"
    text = (last.get("text") or "").strip().lower()
    m = _PERIOD_END_RE.search(text)
    if m:
        try:
            return int(m.group(1))
        except (TypeError, ValueError):
            return None
    return None


# ── Public API ─────────────────────────────────────────────────

def _kind_for(sport: str, period: int) -> str:
    """Label the intermission. Halftime gets its own kind because the
    predictor treats it differently (longer compute window, full-game
    re-projection vs single-period re-projection)."""
    if sport in ("nba", "wnba", "afl") and period == 2:
        return "halftime"
    if sport == "ncaam" and period == 1:
        return "halftime"
    return "period_end"


def detect_and_fire(sport: str, game_id: str,
                    state: dict | None,
                    plays: list[dict] | None) -> dict | None:
    """Inspect the game's current state + PBP, fire a new intermission
    event when one is detected and not already on file. Returns the
    inserted row dict on fire, None on no-op (no intermission, or
    already fired for this period).

    Idempotent — re-running with the same inputs after a fire is a
    no-op because the (sport, game_id, period) PK rejects duplicates.
    """
    ensure_intermissions_table()
    state = state or {}
    plays = plays or []
    status = state.get("status") or {}

    # Try state first (cheap), fall back to PBP. Both should agree at
    # the boundary; either alone is enough to fire.
    period = _state_period_ended(status, sport)
    if period is None:
        period = _pbp_period_ended(plays)
    if period is None:
        return None
    if period <= 0:
        return None

    # Don't fire on regulation-ended plays once the game has already
    # rolled past — only the final period that just ENDED. NBA Q4 end
    # mid-overtime would otherwise trigger a redundant fire.
    cur_period = (status.get("period") or 0)
    if cur_period and cur_period > period + 1 and status.get("state") == "in":
        return None

    kind = _kind_for(sport, period)
    detected_at = datetime.now(timezone.utc).isoformat()
    with _LOCK:
        conn = _conn()
        cur = conn.execute(
            "INSERT OR IGNORE INTO live_intermissions "
            "(sport, game_id, period, kind, detected_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (sport, str(game_id), int(period), kind, detected_at),
        )
        if cur.rowcount == 0:
            return None  # already fired earlier
    return {
        "sport": sport, "game_id": str(game_id),
        "period": int(period), "kind": kind,
        "detected_at": detected_at,
    }


def list_unconsumed(sport: str | None = None) -> list[dict]:
    """Return every intermission row that hasn't been consumed by a
    predictor yet. Used by 5h/5i to find work to do."""
    ensure_intermissions_table()
    conn = _conn()
    if sport:
        rows = conn.execute(
            "SELECT * FROM live_intermissions "
            "WHERE consumed_at IS NULL AND sport = ? "
            "ORDER BY detected_at",
            (sport,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM live_intermissions "
            "WHERE consumed_at IS NULL "
            "ORDER BY detected_at"
        ).fetchall()
    return [dict(r) for r in rows]


def mark_consumed(sport: str, game_id: str, period: int) -> bool:
    """Stamp consumed_at on the row so the predictor doesn't re-process
    a single intermission. Returns True iff a row was updated."""
    ensure_intermissions_table()
    consumed_at = datetime.now(timezone.utc).isoformat()
    with _LOCK:
        conn = _conn()
        cur = conn.execute(
            "UPDATE live_intermissions SET consumed_at = ? "
            "WHERE sport = ? AND game_id = ? AND period = ? "
            "  AND consumed_at IS NULL",
            (consumed_at, sport, str(game_id), int(period)),
        )
        return cur.rowcount > 0


def purge_old(max_age_s: int = 24 * 3600) -> int:
    """Drop intermission rows older than ``max_age_s``. Default 24h —
    the predictor only consumes within minutes, so anything older is
    a completed game whose row no longer matters."""
    ensure_intermissions_table()
    conn = _conn()
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_s
    cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
    cur = conn.execute(
        "DELETE FROM live_intermissions WHERE detected_at < ?", (cutoff_iso,),
    )
    return cur.rowcount


__all__ = [
    "ensure_intermissions_table",
    "detect_and_fire",
    "list_unconsumed",
    "mark_consumed",
    "purge_old",
]
