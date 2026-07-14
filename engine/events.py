"""Event log — append-only source of truth for the prediction loop.

Phase A1 of the self-healing architecture: every observable thing the
model does becomes an event in ``data/events.db``. Calibration tables,
edge floors, and dynamic reliability eventually become *materialized
views* over this log instead of independently-mutated SQLite columns.

Why an event log instead of just keeping the existing per-sport picks
tables as the source:

  - **Replayability.** Bug in calibration logic? Re-materialize from
    history. Today an overshrink bug ships and corrupts the live table
    — no rollback.
  - **A/B-able.** Want to compare two calibration approaches? Run two
    materializations in parallel against the same event stream
    (foundation for A2 versioned models + A4 isotonic shadow).
  - **Drift-detectable.** Continuous projections from the same source
    let drift checks compare projection-vs-realized cleanly, not
    "this table vs that table" with separate write paths and lag.

Event types (extensible):
  - ``decision``       — pick generated (accepted OR rejected at any gate)
  - ``settle``         — pick resolved (W / L / P / V) with profit
  - ``odds_capture``   — HR opening or closing odds snapshot
  - ``drift_signal``   — distribution_drift threshold crossed (A3 hook)
  - ``model_version``  — new model version registered (A2 hook)
  - ``refit``          — calibration / floor / reliability refit run

Storage: a single ``events`` table indexed on (event_type, ts), sport,
pick_id, game_id. Per-event payload is JSON so adding new fields per
event_type doesn't require a migration.

Append-only by convention — there is no UPDATE path. To "correct" an
event, emit a compensating event (e.g. an ``unsettle`` followed by a
new ``settle``). Keeps the audit trail intact.

Public API:
    init_db()                 — create the events table on first call
    write_event(...)          — append one event
    read_events(...)          — query by type/sport/scope
    write_decision(...)       — typed helper for decision events
    write_settle(...)         — typed helper for settle events
    write_odds_capture(...)   — typed helper for odds events
    write_drift_signal(...)   — typed helper for drift events
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)


# Single shared events DB. Sized for 10+ years of model activity at
# current pick volumes; SQLite is comfortable up to ~10M rows with the
# indexes below. Repartition into per-year tables if we ever cross
# that threshold.
_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "events.db"

_local = threading.local()


# ── Connection management ─────────────────────────────────────

def _get_conn() -> sqlite3.Connection:
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except sqlite3.Error:
            try:
                conn.close()
            except sqlite3.Error:
                pass
            _local.conn = None

    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    _ensure_schema(conn)
    _local.conn = conn
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS events (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        ts            TEXT NOT NULL,             -- ISO-8601 UTC of the
                                                 -- modeled instant (not insert time)
        event_type    TEXT NOT NULL,
        sport         TEXT NOT NULL,             -- mlb / nhl / nba / tennis /
                                                 -- wnba / euroleague / ...
        league        TEXT,                      -- basketball framework sub-key
                                                 -- when sport='basketball'-class;
                                                 -- NULL for legacy sports
        game_id       TEXT,
        pick_id       INTEGER,                   -- FK back to source picks table
                                                 -- (no enforced ref since multiple
                                                 -- per-sport DBs)
        bet_type      TEXT,
        pick_text     TEXT,
        model_version TEXT,                      -- A2 hook: which model emitted this
        payload       TEXT NOT NULL,             -- JSON; type-specific fields
        created_at    TEXT NOT NULL DEFAULT (datetime('now'))
    );

    -- Index strategy: most reads will be "all events of type X for
    -- sport Y in the last N days" → composite index on (event_type,
    -- sport, ts). Drift signals + materialized-view refits hit this
    -- shape constantly.
    CREATE INDEX IF NOT EXISTS idx_events_type_sport_ts
        ON events(event_type, sport, ts);
    CREATE INDEX IF NOT EXISTS idx_events_pick
        ON events(pick_id) WHERE pick_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_events_game
        ON events(game_id) WHERE game_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_events_ts
        ON events(ts);
    """)
    conn.commit()


def init_db() -> None:
    """Idempotent — create the events table if missing. Safe to call
    multiple times; mostly here for explicit setup in scripts."""
    _get_conn()


# ── Generic write ─────────────────────────────────────────────

def write_event(
    *,
    event_type: str,
    sport: str,
    payload: dict,
    ts: str | None = None,
    league: str | None = None,
    game_id: str | None = None,
    pick_id: int | None = None,
    bet_type: str | None = None,
    pick_text: str | None = None,
    model_version: str | None = None,
) -> int:
    """Append one event. Returns the new event id.

    Errors are caught + logged; the event log MUST never block a pick
    decision from being recorded. Pick generation has shipped through
    a working code path for years — adding event logging to it can't
    take that down."""
    try:
        conn = _get_conn()
        ts_final = ts or datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "INSERT INTO events (ts, event_type, sport, league, "
            "  game_id, pick_id, bet_type, pick_text, model_version, payload) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                ts_final, event_type, sport, league,
                game_id, pick_id, bet_type, pick_text, model_version,
                json.dumps(payload, default=str),
            ),
        )
        conn.commit()
        return int(cur.lastrowid or 0)
    except Exception as e:
        logger.warning("event log write failed (%s/%s): %s",
                       event_type, sport, e)
        return 0


# ── Typed helpers ────────────────────────────────────────────

def write_decision(
    *,
    sport: str,
    pick_id: int | None,
    game_id: str | None,
    bet_type: str,
    pick_text: str,
    raw_prob: float | None,
    calibrated_prob: float | None,
    odds: int | None,
    edge_pct: float | None,
    accepted: bool,
    rejected_at_gate: str | None = None,
    model_signals: dict | None = None,
    calibration_source: str | None = None,
    league: str | None = None,
    model_version: str | None = None,
    extra: dict | None = None,
    ts: str | None = None,
) -> int:
    """Decision event — emitted whenever the picker considered a pick,
    whether the gates accepted or rejected it. Provenance for every
    "what did the model think at time T" question.

    ``rejected_at_gate``: name of the rejection point ('belief_gate',
    'edge_floor', 'odds_cap', 'juice_wall', etc) when ``accepted=False``.
    None when accepted."""
    payload = {
        "raw_prob": raw_prob,
        "calibrated_prob": calibrated_prob,
        "odds": odds,
        "implied_prob": _implied_from_odds(odds) if odds else None,
        "edge_pct": edge_pct,
        "accepted": accepted,
        "rejected_at_gate": rejected_at_gate,
        "model_signals": model_signals or {},
        "calibration_source": calibration_source,
    }
    if extra:
        payload.update(extra)
    return write_event(
        event_type="decision", sport=sport, league=league,
        pick_id=pick_id, game_id=game_id,
        bet_type=bet_type, pick_text=pick_text,
        model_version=model_version, payload=payload, ts=ts,
    )


def write_settle(
    *,
    sport: str,
    pick_id: int,
    result: str,                         # 'W' / 'L' / 'P' / 'V'
    profit: float,
    game_id: str | None = None,
    bet_type: str | None = None,
    pick_text: str | None = None,
    actual_score: dict | None = None,
    league: str | None = None,
    extra: dict | None = None,
    ts: str | None = None,
) -> int:
    """Settle event — when a pick resolves. Pairs with a prior
    decision event via pick_id."""
    payload = {
        "result": result,
        "profit": profit,
        "actual_score": actual_score,
    }
    if extra:
        payload.update(extra)
    return write_event(
        event_type="settle", sport=sport, league=league,
        pick_id=pick_id, game_id=game_id,
        bet_type=bet_type, pick_text=pick_text,
        payload=payload, ts=ts,
    )


def write_odds_capture(
    *,
    sport: str,
    game_id: str,
    bet_type: str,
    pick_text: str | None,
    odds: int,
    line: float | None = None,
    odds_type: str = "closing",          # 'opening' | 'closing' | 'live'
    league: str | None = None,
    ts: str | None = None,
) -> int:
    """Odds-capture event — opening / closing / live HR snapshot for a
    market we're tracking. CLV math eventually consumes these."""
    payload = {
        "odds_type": odds_type,
        "odds": odds,
        "line": line,
    }
    return write_event(
        event_type="odds_capture", sport=sport, league=league,
        game_id=game_id, bet_type=bet_type, pick_text=pick_text,
        payload=payload, ts=ts,
    )


def write_drift_signal(
    *,
    sport: str,
    metric: str,                          # e.g. 'kl_divergence', 'roi_z'
    value: float,
    threshold: float,
    scope: dict,                          # e.g. {'bet_type': 'ML', 'direction': 'over'}
    league: str | None = None,
    ts: str | None = None,
) -> int:
    """Drift-signal event — emitted when distribution_drift detects a
    threshold crossing. A3 wires the worker to react to these by
    triggering an out-of-cycle calibration refit."""
    payload = {
        "metric": metric,
        "value": value,
        "threshold": threshold,
        "scope": scope,
    }
    return write_event(
        event_type="drift_signal", sport=sport, league=league,
        bet_type=scope.get("bet_type"),
        payload=payload, ts=ts,
    )


def write_refit(
    *,
    sport: str,
    component: str,                       # 'empirical_calibration', 'edge_floors', etc
    summary: dict,
    triggered_by: str = "cron",           # 'cron' | 'drift' | 'manual'
    league: str | None = None,
    model_version: str | None = None,
    ts: str | None = None,
) -> int:
    """Refit event — record that a component was refit. Lets us tell
    "drift triggered this refit" from "cron did". Also feeds the model
    health UI."""
    payload = {
        "component": component,
        "triggered_by": triggered_by,
        "summary": summary,
    }
    return write_event(
        event_type="refit", sport=sport, league=league,
        model_version=model_version, payload=payload, ts=ts,
    )


# ── Reads ────────────────────────────────────────────────────

def read_events(
    *,
    event_type: str | None = None,
    sport: str | None = None,
    league: str | None = None,
    pick_id: int | None = None,
    game_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    limit: int = 1000,
) -> list[dict]:
    """Pull events filtered by any combination of (type, sport, scope,
    time window). Returns dicts with payload already JSON-decoded."""
    conn = _get_conn()
    clauses: list[str] = []
    params: list = []
    if event_type:
        clauses.append("event_type = ?")
        params.append(event_type)
    if sport:
        clauses.append("sport = ?")
        params.append(sport)
    if league:
        clauses.append("league = ?")
        params.append(league)
    if pick_id is not None:
        clauses.append("pick_id = ?")
        params.append(int(pick_id))
    if game_id:
        clauses.append("game_id = ?")
        params.append(str(game_id))
    if since:
        clauses.append("ts >= ?")
        params.append(since)
    if until:
        clauses.append("ts < ?")
        params.append(until)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = conn.execute(
        f"SELECT id, ts, event_type, sport, league, game_id, pick_id, "
        f"  bet_type, pick_text, model_version, payload "
        f"FROM events{where} ORDER BY ts ASC, id ASC LIMIT ?",
        (*params, int(limit)),
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["payload"] = json.loads(d["payload"]) if d["payload"] else {}
        except json.JSONDecodeError:
            d["payload"] = {}
        out.append(d)
    return out


def count_events(
    *,
    event_type: str | None = None,
    sport: str | None = None,
    since: str | None = None,
    until: str | None = None,
) -> int:
    conn = _get_conn()
    clauses: list[str] = []
    params: list = []
    if event_type:
        clauses.append("event_type = ?"); params.append(event_type)
    if sport:
        clauses.append("sport = ?"); params.append(sport)
    if since:
        clauses.append("ts >= ?"); params.append(since)
    if until:
        clauses.append("ts < ?"); params.append(until)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return int(conn.execute(
        f"SELECT COUNT(*) FROM events{where}", params,
    ).fetchone()[0])


def latest_event(event_type: str, sport: str | None = None) -> dict | None:
    """Most-recent event of a given type — useful for "when was the
    last calibration refit?" style queries."""
    conn = _get_conn()
    if sport:
        row = conn.execute(
            "SELECT id, ts, payload FROM events "
            "WHERE event_type = ? AND sport = ? "
            "ORDER BY ts DESC, id DESC LIMIT 1",
            (event_type, sport),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT id, ts, payload FROM events "
            "WHERE event_type = ? ORDER BY ts DESC, id DESC LIMIT 1",
            (event_type,),
        ).fetchone()
    if not row:
        return None
    out = dict(row)
    try:
        out["payload"] = json.loads(out["payload"])
    except json.JSONDecodeError:
        out["payload"] = {}
    return out


def latest_refit(component: str, sport: str) -> dict | None:
    """Most-recent ``refit`` event for a given (component, sport).

    A8: replaces unconditional hourly refits. The worker calls this to
    answer "did anything (drift, cron floor, manual) refit this
    component recently?" If yes, skip — the loop is event-driven now.
    If no, fire a safety-floor refit and write a new refit event.

    Returns the row dict (id, ts, payload) or None.
    """
    conn = _get_conn()
    rows = conn.execute(
        "SELECT id, ts, payload FROM events "
        "WHERE event_type = 'refit' AND sport = ? "
        "ORDER BY ts DESC, id DESC LIMIT 50",
        (sport,),
    ).fetchall()
    for r in rows:
        try:
            p = json.loads(r["payload"]) if r["payload"] else {}
        except json.JSONDecodeError:
            continue
        if (p.get("component") or "") == component:
            out = dict(r)
            out["payload"] = p
            return out
    return None


def needs_refit(component: str, sport: str, max_age_h: int) -> bool:
    """A8 floor check: True if no refit event for (component, sport)
    exists within ``max_age_h``. False if a recent refit (drift-fired
    or cron-floor) already covers us — in which case the worker skips.

    The drift loop's refit events count, so a sport that's healthy
    (no drift signals) just naturally hits the floor cadence; a sport
    that's drifting refits sooner and the floor never fires."""
    last = latest_refit(component, sport)
    if not last:
        return True
    try:
        ts = last["ts"]
        last_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, KeyError):
        return True
    age = datetime.now(timezone.utc) - last_dt
    return age >= timedelta(hours=max_age_h)


# ── Misc ────────────────────────────────────────────────────

def _implied_from_odds(odds: int | float | None) -> float | None:
    if odds is None:
        return None
    try:
        o = int(odds)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    if o < 0:
        return abs(o) / (abs(o) + 100)
    return 100 / (o + 100)


def db_path() -> Path:
    """Exposed for tests + the parity-check tooling."""
    return _DB_PATH


def close_local() -> None:
    """Close the thread-local connection. Tests use this between cases."""
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except sqlite3.Error:
            pass
        _local.conn = None
