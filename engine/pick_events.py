"""
Per-game pick event log.

Why this exists: a user looking at a BestBets card sees the model's
CURRENT recommendation for a game, but has no breadcrumb when that
recommendation appears, swaps, vanishes, or shifts in edge. This
module persists every meaningful transition so the card can render
a "📜 history" tooltip (DAL ML pulled at 18:42 — line moved from
+115 to -180, model now sees no edge).

Events tracked:
    appeared   — first time the model has any qualifying pick on this
                 game today.
    swapped    — best_pick.bet_type or .pick changed (e.g. DAL ML →
                 MIN -1.5).
    pulled     — game still on the slate but model lost edge; no
                 best_pick currently.
    line_shift — same pick + bet type, but odds moved enough that
                 edge changed by ≥ LINE_SHIFT_THRESHOLD_PP. Cuts
                 noise from intra-poll micro-moves.

Designed for safe re-entry: detect_transitions is idempotent within
a poll cycle (same input twice → no duplicate rows) because the
"last state" reference is the most recent event row, not in-memory.

Storage: one `pick_events` table per sport DB (mlb / nhl / nba),
keyed by (date, game_id). Pruning happens via prune_before(date).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# Edge change threshold for emitting a line_shift event. Anything
# below this is treated as noise (juice wobbles, in-flight line
# adjustments) and suppressed so the breadcrumb doesn't fill with
# "edge changed from 5.1% to 5.3%" rows.
LINE_SHIFT_THRESHOLD_PP = 2.0


_DDL = """
CREATE TABLE IF NOT EXISTS pick_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    date         TEXT NOT NULL,
    game_id      TEXT NOT NULL,
    matchup      TEXT NOT NULL,
    ts           TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    prev_bet_type TEXT,
    prev_pick     TEXT,
    prev_odds     INTEGER,
    prev_edge     REAL,
    curr_bet_type TEXT,
    curr_pick     TEXT,
    curr_odds     INTEGER,
    curr_edge     REAL,
    reason       TEXT
);
CREATE INDEX IF NOT EXISTS idx_pick_events_game ON pick_events(date, game_id);
CREATE INDEX IF NOT EXISTS idx_pick_events_ts ON pick_events(ts);
"""


def _conn_for(sport: str):
    if sport == "mlb":
        from .db import get_conn
    elif sport == "nhl":
        from .nhl_db import get_conn
    elif sport == "nba":
        from .nba_db import get_conn
    else:
        raise ValueError(f"unknown sport: {sport}")
    return get_conn()


def ensure_table(sport: str) -> None:
    """Idempotent DDL. Safe to call on every put."""
    conn = _conn_for(sport)
    conn.executescript(_DDL)
    conn.commit()


def _last_state(sport: str, game_id: str, date: str) -> dict | None:
    """Return the most recent pick-state for ``game_id`` today, or
    None when no event has been recorded yet."""
    try:
        ensure_table(sport)
        conn = _conn_for(sport)
        row = conn.execute(
            "SELECT event_type, curr_bet_type, curr_pick, curr_odds, curr_edge "
            "FROM pick_events "
            "WHERE date = ? AND game_id = ? "
            "ORDER BY id DESC LIMIT 1",
            (date, str(game_id)),
        ).fetchone()
        if not row:
            return None
        # A "pulled" event has NULL curr_*; treat it as "no current pick"
        # so the next non-null pick correctly reads as appeared (vs
        # swapped from a stale curr_*).
        if row["event_type"] == "pulled":
            return {"present": False}
        return {
            "present": True,
            "bet_type": row["curr_bet_type"],
            "pick":     row["curr_pick"],
            "odds":     row["curr_odds"],
            "edge":     row["curr_edge"],
        }
    except Exception as e:
        logger.warning("pick_events._last_state failed for %s/%s: %s",
                       sport, game_id, e)
        return None


def _insert(sport: str, date: str, game_id: str, matchup: str,
            event_type: str, prev: dict | None, curr: dict | None,
            reason: str | None = None) -> None:
    ensure_table(sport)
    conn = _conn_for(sport)
    try:
        conn.execute(
            "INSERT INTO pick_events "
            "(date, game_id, matchup, ts, event_type, "
            " prev_bet_type, prev_pick, prev_odds, prev_edge, "
            " curr_bet_type, curr_pick, curr_odds, curr_edge, reason) "
            "VALUES (?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                date, str(game_id), matchup, event_type,
                (prev or {}).get("bet_type"),
                (prev or {}).get("pick"),
                (prev or {}).get("odds"),
                (prev or {}).get("edge"),
                (curr or {}).get("bet_type"),
                (curr or {}).get("pick"),
                (curr or {}).get("odds"),
                (curr or {}).get("edge"),
                reason,
            ),
        )
        conn.commit()
    except Exception as e:
        logger.warning("pick_events._insert failed for %s/%s: %s",
                       sport, game_id, e)


def detect_transitions(sport: str, current_picks: list[dict],
                        date: str | None = None) -> dict:
    """Compare current best-bets snapshot to the last logged state and
    emit events for every meaningful change.

    Args:
        sport: 'mlb' | 'nhl' | 'nba'
        current_picks: list of dicts with keys
            game_id, matchup, best_pick (or None if game still on
            slate but model lost edge). best_pick when present
            carries {type, pick, odds, edge}.
        date: defaults to today (YYYY-MM-DD).

    Returns:
        Counts dict {appeared, swapped, pulled, line_shift}.
    """
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    counts = {"appeared": 0, "swapped": 0, "pulled": 0, "line_shift": 0}

    for entry in current_picks:
        game_id = entry.get("game_id")
        matchup = entry.get("matchup", "")
        bp = entry.get("best_pick")
        if not game_id:
            continue

        prev = _last_state(sport, game_id, target_date)
        curr = None
        if bp:
            curr = {
                "bet_type": bp.get("type"),
                "pick":     bp.get("pick"),
                "odds":     bp.get("odds"),
                "edge":     bp.get("edge"),
            }

        # No prior state, no current pick -> nothing to log.
        if prev is None and curr is None:
            continue

        # Appeared: first time we've seen a pick for this game today.
        if (prev is None or not prev.get("present")) and curr is not None:
            _insert(sport, target_date, game_id, matchup,
                    "appeared", None, curr)
            counts["appeared"] += 1
            continue

        # Pulled: had a pick, now we don't.
        if prev is not None and prev.get("present") and curr is None:
            _insert(sport, target_date, game_id, matchup,
                    "pulled",
                    {"bet_type": prev.get("bet_type"),
                     "pick":     prev.get("pick"),
                     "odds":     prev.get("odds"),
                     "edge":     prev.get("edge")},
                    None,
                    reason="Model no longer sees qualifying edge")
            counts["pulled"] += 1
            continue

        # Both present — compare for swap or line shift.
        if prev is not None and prev.get("present") and curr is not None:
            same_bet = (prev.get("bet_type") == curr["bet_type"]
                        and prev.get("pick") == curr["pick"])
            if not same_bet:
                _insert(sport, target_date, game_id, matchup,
                        "swapped",
                        {"bet_type": prev.get("bet_type"),
                         "pick":     prev.get("pick"),
                         "odds":     prev.get("odds"),
                         "edge":     prev.get("edge")},
                        curr,
                        reason=(f"{prev.get('bet_type')} {prev.get('pick')} "
                                f"→ {curr['bet_type']} {curr['pick']}"))
                counts["swapped"] += 1
                continue

            # Same pick — check edge delta.
            prev_edge = prev.get("edge") or 0.0
            curr_edge = curr.get("edge") or 0.0
            if abs(curr_edge - prev_edge) >= LINE_SHIFT_THRESHOLD_PP:
                direction = "improved" if curr_edge > prev_edge else "shrunk"
                _insert(sport, target_date, game_id, matchup,
                        "line_shift",
                        {"bet_type": prev.get("bet_type"),
                         "pick":     prev.get("pick"),
                         "odds":     prev.get("odds"),
                         "edge":     prev.get("edge")},
                        curr,
                        reason=(f"Edge {direction} "
                                f"{prev_edge:.1f}% → {curr_edge:.1f}%"))
                counts["line_shift"] += 1

    return counts


def list_events(sport: str, game_id: str | None = None,
                hours: int = 24, date: str | None = None) -> list[dict]:
    """Return recent events. When ``game_id`` is provided, scope to
    that game; otherwise return all events for the date.

    The dashboard 📜 popover reads this with game_id set so each card
    only loads its own thread.
    """
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    try:
        ensure_table(sport)
        conn = _conn_for(sport)
        if game_id:
            rows = conn.execute(
                "SELECT * FROM pick_events "
                "WHERE date = ? AND game_id = ? "
                "  AND ts >= datetime('now', ?) "
                "ORDER BY id DESC",
                (target_date, str(game_id), f"-{int(hours)} hours"),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM pick_events "
                "WHERE date = ? AND ts >= datetime('now', ?) "
                "ORDER BY id DESC",
                (target_date, f"-{int(hours)} hours"),
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning("pick_events.list_events failed for %s: %s", sport, e)
        return []


def prune_before(sport: str, date: str) -> int:
    """Delete event rows older than ``date`` (exclusive). Keeps the
    table from growing unbounded over the season."""
    try:
        ensure_table(sport)
        conn = _conn_for(sport)
        cur = conn.execute(
            "DELETE FROM pick_events WHERE date < ?", (date,),
        )
        conn.commit()
        return cur.rowcount or 0
    except Exception as e:
        logger.warning("pick_events.prune failed for %s: %s", sport, e)
        return 0
