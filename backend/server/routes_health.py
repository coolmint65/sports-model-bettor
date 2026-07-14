"""Coverage radar.

`/api/health/coverage` exposes the last-decision / last-settle / last-
refit time per (sport, bet_type) so ops can spot a dead lane at a
glance. Backs the bulletproof rollout — if a 24/7 worker tick stops
firing, the lane goes yellow within a few hours and red within 24h.

Reads from ``data/events.db``, the canonical event log (every pick
decision + settle + refit mirrors here per A1 architecture). No
join against per-sport pick tables needed — the radar's source of
truth is identical regardless of which legacy DB the underlying pick
lives in.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter

router = APIRouter()
logger = logging.getLogger(__name__)


_EVENTS_DB = (Path(__file__).resolve().parent.parent.parent
               / "data" / "events.db")

# Stale thresholds in MINUTES. A lane that hasn't logged a decision
# in this many minutes is flagged. Tuned per market velocity — live
# state moves second-by-second, prematch picks refresh every 5m, so
# even a generous 120-min yellow / 24h red catches genuine outages
# without false positives during off-hours.
_YELLOW_MIN = 120
_RED_MIN = 60 * 24


def _conn() -> sqlite3.Connection | None:
    if not _EVENTS_DB.exists():
        return None
    c = sqlite3.connect(str(_EVENTS_DB))
    c.row_factory = sqlite3.Row
    return c


def _classify(last_ts: str | None, *,
                yellow_min: int = _YELLOW_MIN,
                red_min: int = _RED_MIN) -> tuple[str, float | None]:
    """Return (status, age_minutes). Status: green/yellow/red/never.
    None timestamp → 'never' (we have NO record of this lane ever
    emitting an event)."""
    if not last_ts:
        return "never", None
    try:
        ts = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return "never", None
    age_s = (datetime.now(timezone.utc) - ts).total_seconds()
    age_min = age_s / 60.0
    if age_min < yellow_min:
        return "green", age_min
    if age_min < red_min:
        return "yellow", age_min
    return "red", age_min


@router.get("/api/health/coverage")
def api_health_coverage() -> dict:
    """Per-(sport, bet_type) freshness map.

    Response shape::

        {
          "now": ISO timestamp,
          "lanes": [
            {"sport", "bet_type",
             "last_decision_at", "last_settle_at", "last_refit_at",
             "decisions_24h", "decisions_age_min", "status"},
            ...
          ],
          "summary": {"green": N, "yellow": N, "red": N, "never": N}
        }

    Lanes are sorted with reds first (most actionable), then yellows,
    then greens. The ``status`` is computed from
    ``last_decision_at`` only — settle + refit are informational.
    """
    conn = _conn()
    if conn is None:
        return {"error": "events.db missing"}

    # Build per-(sport, bet_type) summary in one pass.
    try:
        rows = conn.execute(
            "SELECT sport, bet_type, event_type, "
            "       MAX(ts) AS last_ts, "
            "       SUM(CASE WHEN event_type='decision' AND ts >= datetime('now', '-1 day') THEN 1 ELSE 0 END) AS d24 "
            "FROM events "
            "WHERE sport IS NOT NULL AND bet_type IS NOT NULL "
            "GROUP BY sport, bet_type, event_type"
        ).fetchall()
    except Exception as e:
        logger.warning("health coverage query failed: %s", e)
        return {"error": str(e)}

    lanes: dict[tuple[str, str], dict] = {}
    for r in rows:
        key = (r["sport"], r["bet_type"])
        lane = lanes.setdefault(key, {
            "sport": r["sport"], "bet_type": r["bet_type"],
            "last_decision_at": None, "last_settle_at": None,
            "last_refit_at": None, "decisions_24h": 0,
        })
        et = r["event_type"]
        if et == "decision":
            lane["last_decision_at"] = r["last_ts"]
            lane["decisions_24h"] = int(r["d24"] or 0)
        elif et == "settle":
            lane["last_settle_at"] = r["last_ts"]
        elif et == "refit":
            lane["last_refit_at"] = r["last_ts"]

    out: list[dict] = []
    summary = {"green": 0, "yellow": 0, "red": 0, "never": 0}
    for lane in lanes.values():
        status, age_min = _classify(lane["last_decision_at"])
        lane["status"] = status
        lane["decisions_age_min"] = round(age_min, 1) if age_min else None
        summary[status] = summary.get(status, 0) + 1
        out.append(lane)

    # Sort: red → yellow → green → never, then by age desc within tier.
    order = {"red": 0, "yellow": 1, "green": 2, "never": 3}
    out.sort(key=lambda x: (order.get(x["status"], 9),
                              -(x["decisions_age_min"] or 0)))

    return {
        "now": datetime.now(timezone.utc).isoformat(),
        "thresholds": {
            "yellow_min": _YELLOW_MIN,
            "red_min": _RED_MIN,
        },
        "summary": summary,
        "lanes": out,
    }


@router.get("/api/health/coverage/{sport}")
def api_health_coverage_sport(sport: str) -> dict:
    """Per-sport drill-down. Returns the same lane shape filtered to
    ``sport``. Cheap (the parent endpoint already filters server-
    side; this just narrows the result set)."""
    parent = api_health_coverage()
    if parent.get("error"):
        return parent
    lanes = [l for l in parent["lanes"] if l["sport"] == sport]
    summary = {"green": 0, "yellow": 0, "red": 0, "never": 0}
    for l in lanes:
        summary[l["status"]] = summary.get(l["status"], 0) + 1
    return {
        "now": parent["now"],
        "sport": sport,
        "thresholds": parent["thresholds"],
        "summary": summary,
        "lanes": lanes,
    }
