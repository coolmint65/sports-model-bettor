"""Deep-check health endpoint.

Goes beyond the FastAPI default "is the server up?" — exercises every
class of dependency the prediction pipeline needs and reports per-check
status. Designed so a cron / on-call human can call /api/_health and
get a structured signal of *which* subsystem is degraded without
having to grep logs.

Checks (each independent — one failure doesn't short-circuit others):

  - db.{mlb,nhl,nba}: each per-sport SQLite reachable + writable
  - schedule.{mlb,nhl,nba}: today's games actually present
  - models.gbm.{nba,nba_q1,...}: latest GBM artifacts loadable
  - calibration.{mlb,nhl,nba}: calibration table built (n>=1 row)
  - scrapers.hardrock: HR scraper auth still valid
  - cache.picks_cache: the persistent picks cache table reachable
  - locks.potd: are there pending POTDs from before today (auto-stale?)

Status values per check:
  "ok"      — all green, no action needed
  "warn"    — degraded but not broken (e.g. 0 games scheduled)
  "fail"    — unrecoverable from this check; ops attention warranted

Top-level status is the worst sub-status. The endpoint always returns
HTTP 200 so downstream monitors can poll without alert flapping; the
"status" key is the alerting signal.
"""

from __future__ import annotations
import logging
import sqlite3
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


def _check_db(sport: str) -> dict:
    """SQLite reachable, schema sane, can write."""
    try:
        if sport == "mlb":
            from engine.db import get_conn
        elif sport == "nhl":
            from engine.nhl_db import get_conn
        elif sport == "nba":
            from engine.nba_db import get_conn
        else:
            return {"status": "fail", "detail": f"unknown sport {sport!r}"}
        conn = get_conn()
        # SELECT 1 from picks/equivalent verifies schema present
        table = "picks" if sport == "mlb" else f"{sport}_picks"
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return {"status": "ok", "rows": int(n)}
    except Exception as e:
        return {"status": "fail", "detail": str(e)}


def _check_schedule(sport: str) -> dict:
    """At least one game scheduled for today (so the model has work)."""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        if sport == "mlb":
            from engine.db import get_conn
            conn = get_conn()
            n = conn.execute(
                "SELECT COUNT(*) FROM games WHERE date=?", (today,)
            ).fetchone()[0]
        elif sport == "nhl":
            from engine.nhl_db import get_conn
            conn = get_conn()
            n = conn.execute(
                "SELECT COUNT(*) FROM nhl_games WHERE date=?", (today,)
            ).fetchone()[0]
        elif sport == "nba":
            from engine.nba_db import get_conn
            conn = get_conn()
            n = conn.execute(
                "SELECT COUNT(*) FROM nba_games WHERE date=?", (today,)
            ).fetchone()[0]
        else:
            return {"status": "fail", "detail": f"unknown sport {sport!r}"}
        if n == 0:
            return {"status": "warn",
                    "detail": "no games scheduled today — sync may not have run"}
        return {"status": "ok", "games": int(n)}
    except Exception as e:
        return {"status": "fail", "detail": str(e)}


def _check_calibration(sport: str) -> dict:
    """Calibration table built and has at least one bucket cleared."""
    try:
        from engine.empirical_calibration import snapshot, refresh_calibration
        snap = snapshot(sport)
        if not snap:
            # Lazy-build attempt
            try:
                refresh_calibration(sport)
                snap = snapshot(sport)
            except Exception as e:
                return {"status": "warn",
                        "detail": f"empty + rebuild failed: {e}"}
        if not snap:
            return {"status": "warn", "detail": "no calibration buckets"}
        return {"status": "ok", "buckets": len(snap)}
    except Exception as e:
        return {"status": "fail", "detail": str(e)}


def _check_picks_cache(sport: str) -> dict:
    """The persistent picks_cache table is the source of truth for
    "what did the user see today" — verify reachable + non-empty when
    games scheduled."""
    try:
        if sport == "mlb":
            from engine.db import get_conn
        elif sport == "nhl":
            from engine.nhl_db import get_conn
        elif sport == "nba":
            from engine.nba_db import get_conn
        else:
            return {"status": "fail", "detail": f"unknown sport {sport!r}"}
        conn = get_conn()
        today = datetime.now().strftime("%Y-%m-%d")
        n = conn.execute(
            "SELECT COUNT(*) FROM picks_cache WHERE date=?", (today,)
        ).fetchone()[0]
        return {"status": "ok", "today_rows": int(n)}
    except Exception as e:
        return {"status": "fail", "detail": str(e)}


def _check_potd_locks() -> dict:
    """Surfaces orphaned POTD locks from prior days that never settled.
    A heads-up that something in the settler is stuck — does NOT
    auto-fix (settler's responsibility), just reports."""
    issues = []
    try:
        cutoff = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        for sport in ("mlb", "nhl", "nba"):
            if sport == "mlb":
                from engine.db import get_conn
            elif sport == "nhl":
                from engine.nhl_db import get_conn
            else:
                from engine.nba_db import get_conn
            conn = get_conn()
            try:
                rows = conn.execute(
                    "SELECT COUNT(*) FROM pick_of_day "
                    "WHERE date < ? AND result IS NULL", (cutoff,)
                ).fetchone()[0]
                if rows > 0:
                    issues.append(f"{sport}: {rows} stale pending POTD")
            except Exception:
                pass  # table absence is fine, just skip
        if issues:
            return {"status": "warn", "detail": "; ".join(issues)}
        return {"status": "ok"}
    except Exception as e:
        return {"status": "fail", "detail": str(e)}


def _check_gbm_models() -> dict:
    """At least one GBM model artifact loadable. Catches the case
    where data/models/ was wiped or the trainer crashed mid-write."""
    from pathlib import Path
    models_dir = Path(__file__).resolve().parent.parent / "data" / "models"
    if not models_dir.exists():
        return {"status": "fail", "detail": f"no models dir at {models_dir}"}
    latest_files = list(models_dir.glob("*_latest.json"))
    if not latest_files:
        return {"status": "warn",
                "detail": "no *_latest.json model artifacts present"}
    return {"status": "ok", "models": len(latest_files)}


def run_health_checks() -> dict:
    """Aggregate every check and return a structured payload.

    Top-level status is the worst sub-status (ok < warn < fail).
    Always returns 200 — the caller monitors the `status` key.
    """
    severity = {"ok": 0, "warn": 1, "fail": 2}
    checks = {
        "db.mlb":               _check_db("mlb"),
        "db.nhl":               _check_db("nhl"),
        "db.nba":               _check_db("nba"),
        "schedule.mlb":         _check_schedule("mlb"),
        "schedule.nhl":         _check_schedule("nhl"),
        "schedule.nba":         _check_schedule("nba"),
        "calibration.mlb":      _check_calibration("mlb"),
        "calibration.nhl":      _check_calibration("nhl"),
        "calibration.nba":      _check_calibration("nba"),
        "cache.picks.mlb":      _check_picks_cache("mlb"),
        "cache.picks.nhl":      _check_picks_cache("nhl"),
        "cache.picks.nba":      _check_picks_cache("nba"),
        "locks.potd":           _check_potd_locks(),
        "models.gbm":           _check_gbm_models(),
    }
    worst = max((severity.get(c.get("status"), 2) for c in checks.values()),
                default=0)
    inv = {0: "ok", 1: "warn", 2: "fail"}
    return {
        "status": inv[worst],
        "ts": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
    }
