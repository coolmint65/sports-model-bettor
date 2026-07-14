"""
Shared state cache for the live engine.

The live worker (services/live_worker/main.py) runs as a separate
process from the FastAPI server. They can't share Python memory, so
state crosses the boundary via SQLite at `data/live.db` in WAL mode:
worker writes; API reads; multiple readers OK.

Schema is intentionally tiny — one row per (sport, game_id) holding
the latest state blob serialized as JSON. Each refresh overwrites in
place. No history retention here (pick_events still owns the
breadcrumb trail; this cache is current state only).

A row is "stale" when `last_updated_at` is older than `STALE_AFTER_S`
(default 5 min). Stale rows mean the worker isn't writing — either
the game ended (and we'll garbage-collect via DELETE WHERE date <
yesterday) or the worker is down. The /api/_health check surfaces
worker liveness from this signal.

Public API:
    upsert_state(sport, game_id, state)
    get_state(sport, game_id)             — current state or None if stale
    list_active(sport, max_age_s)         — every fresh row for the sport
    purge_stale(max_age_s)                — DELETE rows older than cutoff
"""

from __future__ import annotations
import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "live.db"
_STORE_LOCK = threading.Lock()  # only protects this process's connections

# Default: a row stale > 5 min means the worker missed a write cycle
# (15s/30s polls so 5 min is ~10-20 missed ticks — definitely down).
STALE_AFTER_S = 300


def _conn() -> sqlite3.Connection:
    """Per-thread connection. WAL so the worker's writes don't block
    API reads."""
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def ensure_table() -> None:
    """Idempotent DDL. Call on every writer/reader startup so a fresh
    deploy doesn't 500 on first read."""
    with _STORE_LOCK:
        conn = _conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS live_state (
                sport             TEXT NOT NULL,
                game_id           TEXT NOT NULL,
                last_updated_at   TEXT NOT NULL,
                state_json        TEXT NOT NULL,
                PRIMARY KEY (sport, game_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_live_state_updated ON live_state(last_updated_at)"
        )
        # Phase 5b play-by-play. Append-only; (sport, game_id, play_id)
        # is the natural key — ESPN re-emits the full plays array on
        # every summary call, so INSERT OR IGNORE makes re-fetches
        # idempotent. Storing raw_json lets 5c-5f derive features
        # (lineups, foul state, possession) without re-hitting ESPN.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS live_pbp (
                sport             TEXT NOT NULL,
                game_id           TEXT NOT NULL,
                play_id           TEXT NOT NULL,
                sequence          INTEGER,
                period            INTEGER NOT NULL,
                clock_secs        INTEGER,
                clock_display     TEXT,
                home_score        INTEGER,
                away_score        INTEGER,
                team_id           TEXT,
                type_id           TEXT,
                type_text         TEXT,
                text              TEXT,
                scoring_play      INTEGER DEFAULT 0,
                score_value       INTEGER DEFAULT 0,
                shooting_play     INTEGER DEFAULT 0,
                wallclock         TEXT,
                participants_json TEXT,
                raw_json          TEXT,
                fetched_at        TEXT NOT NULL,
                PRIMARY KEY (sport, game_id, play_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pbp_game_period "
            "ON live_pbp(sport, game_id, period, sequence)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pbp_fetched_at "
            "ON live_pbp(fetched_at)"
        )


def upsert_state(sport: str, game_id: str, state: dict) -> None:
    """Worker entry point. Replaces the row for (sport, game_id) with
    the freshest state blob + a UTC timestamp."""
    ensure_table()
    payload = json.dumps(state, default=str)
    ts = datetime.now(timezone.utc).isoformat()
    with _STORE_LOCK:
        conn = _conn()
        conn.execute(
            "INSERT INTO live_state (sport, game_id, last_updated_at, state_json) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(sport, game_id) DO UPDATE SET "
            "  last_updated_at = excluded.last_updated_at, "
            "  state_json = excluded.state_json",
            (sport, str(game_id), ts, payload),
        )


def get_state(sport: str, game_id: str,
              max_age_s: int = STALE_AFTER_S) -> dict | None:
    """API reader. Returns the parsed state blob if fresh, None if
    stale or missing. `max_age_s` lets an integration override the
    freshness window (e.g., debug endpoints can pass a generous
    1-hour window to inspect old state)."""
    ensure_table()
    conn = _conn()
    row = conn.execute(
        "SELECT last_updated_at, state_json FROM live_state "
        "WHERE sport = ? AND game_id = ?",
        (sport, str(game_id)),
    ).fetchone()
    if not row:
        return None
    if _age_seconds(row["last_updated_at"]) > max_age_s:
        return None
    try:
        return json.loads(row["state_json"])
    except json.JSONDecodeError as e:
        logger.warning("live_state JSON decode failed for %s/%s: %s",
                       sport, game_id, e)
        return None


def list_active(sport: str | None = None,
                max_age_s: int = STALE_AFTER_S) -> list[dict]:
    """Every fresh row, optionally filtered by sport. Used by the
    /api/{sport}/live-bets path to enumerate in-progress games."""
    ensure_table()
    conn = _conn()
    if sport:
        rows = conn.execute(
            "SELECT sport, game_id, last_updated_at, state_json "
            "FROM live_state WHERE sport = ? ORDER BY game_id",
            (sport,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT sport, game_id, last_updated_at, state_json "
            "FROM live_state ORDER BY sport, game_id"
        ).fetchall()
    out: list[dict] = []
    for r in rows:
        if _age_seconds(r["last_updated_at"]) > max_age_s:
            continue
        try:
            payload = json.loads(r["state_json"])
        except json.JSONDecodeError:
            continue
        out.append({
            "sport": r["sport"],
            "game_id": r["game_id"],
            "last_updated_at": r["last_updated_at"],
            "state": payload,
        })
    return out


def purge_stale(max_age_s: int = STALE_AFTER_S * 6) -> int:
    """DELETE rows older than max_age_s (default 30 min). Worker calls
    this periodically so completed games don't accumulate."""
    ensure_table()
    conn = _conn()
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_s
    cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
    cur = conn.execute(
        "DELETE FROM live_state WHERE last_updated_at < ?", (cutoff_iso,),
    )
    return cur.rowcount


# ── PBP storage ────────────────────────────────────────────────

def upsert_pbp(sport: str, game_id: str, plays: list[dict]) -> int:
    """Persist normalized plays. INSERT OR IGNORE on the natural key
    so re-fetches of the full ESPN plays array don't re-write rows.
    Returns the count of rows actually inserted (new plays this call)."""
    if not plays:
        return 0
    ensure_table()
    ts = datetime.now(timezone.utc).isoformat()
    inserted = 0
    with _STORE_LOCK:
        conn = _conn()
        for p in plays:
            try:
                participants = json.dumps(p.get("participants") or [])
                raw = json.dumps(p.get("raw") or {}, default=str)
                cur = conn.execute(
                    "INSERT OR IGNORE INTO live_pbp ("
                    "  sport, game_id, play_id, sequence, period, "
                    "  clock_secs, clock_display, home_score, away_score, "
                    "  team_id, type_id, type_text, text, "
                    "  scoring_play, score_value, shooting_play, "
                    "  wallclock, participants_json, raw_json, fetched_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (sport, str(game_id), str(p["play_id"]),
                     p.get("sequence") or 0,
                     p.get("period") or 0,
                     p.get("clock_secs"),
                     p.get("clock_display"),
                     p.get("home_score"),
                     p.get("away_score"),
                     p.get("team_id"),
                     p.get("type_id"),
                     p.get("type_text"),
                     p.get("text"),
                     1 if p.get("scoring_play") else 0,
                     p.get("score_value") or 0,
                     1 if p.get("shooting_play") else 0,
                     p.get("wallclock"),
                     participants,
                     raw,
                     ts),
                )
                if cur.rowcount > 0:
                    inserted += 1
            except Exception as e:
                logger.warning("upsert_pbp failed for %s/%s/%s: %s",
                               sport, game_id, p.get("play_id"), e)
    return inserted


def get_pbp(sport: str, game_id: str,
            period: int | None = None) -> list[dict]:
    """Read all stored plays for a game, ordered by (period, sequence).
    Optional ``period`` filter for "give me just Q1" lookups (the
    intermission predictor consumes this)."""
    ensure_table()
    conn = _conn()
    if period is not None:
        rows = conn.execute(
            "SELECT * FROM live_pbp "
            "WHERE sport = ? AND game_id = ? AND period = ? "
            "ORDER BY sequence",
            (sport, str(game_id), int(period)),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM live_pbp "
            "WHERE sport = ? AND game_id = ? "
            "ORDER BY period, sequence",
            (sport, str(game_id)),
        ).fetchall()
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        d["scoring_play"] = bool(d.get("scoring_play"))
        d["shooting_play"] = bool(d.get("shooting_play"))
        try:
            d["participants"] = json.loads(d.pop("participants_json") or "[]")
        except (TypeError, ValueError):
            d["participants"] = []
        # Drop raw_json from the default read shape — it's there for
        # debugging / re-derivation but bloats the standard read.
        d.pop("raw_json", None)
        out.append(d)
    return out


def purge_old_pbp(max_age_s: int = STALE_AFTER_S * 6) -> int:
    """Drop pbp rows older than ``max_age_s``. Default mirrors
    ``purge_stale`` (30 min) so completed-game plays don't accumulate
    indefinitely."""
    ensure_table()
    conn = _conn()
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_s
    cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
    cur = conn.execute(
        "DELETE FROM live_pbp WHERE fetched_at < ?", (cutoff_iso,),
    )
    return cur.rowcount


def worker_heartbeat(sport: str | None = None) -> dict:
    """Health-check helper. Returns the most-recent write per sport,
    so /api/_health can flag the worker as down when a sport hasn't
    been written in too long."""
    ensure_table()
    conn = _conn()
    if sport:
        row = conn.execute(
            "SELECT MAX(last_updated_at) AS ts, COUNT(*) AS n "
            "FROM live_state WHERE sport = ?", (sport,),
        ).fetchone()
        return {"sport": sport, "last_write": row["ts"], "row_count": row["n"]}
    rows = conn.execute(
        "SELECT sport, MAX(last_updated_at) AS ts, COUNT(*) AS n "
        "FROM live_state GROUP BY sport"
    ).fetchall()
    return {r["sport"]: {"last_write": r["ts"], "row_count": r["n"]}
            for r in rows}


def _age_seconds(iso_ts: str) -> float:
    try:
        ts = datetime.fromisoformat(iso_ts)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - ts).total_seconds()
    except (ValueError, TypeError):
        return float("inf")
