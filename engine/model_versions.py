"""Versioned models + shadow promotion (A2).

Replaces the overwrite-on-refit pattern. Every refit creates a new
*version* of a component (calibration table, GBM model, blend
parameters, etc.). The live system points at one version per component;
candidate versions can run in shadow alongside, accumulating decision
events with their own ``model_version`` stamp. A promotion gate
compares performance — candidate replaces live only if it beats live
by a configured margin over a configured sample.

A1 made this possible: every decision now lands in the events log
already. A2 just adds the versioning layer + the gate.

Schema (single shared table at ``data/events.db``):
  ``model_versions``: (component, version_id, status, config_json,
                       created_at, promoted_at, retired_at, parent_id)

Statuses:
  - ``candidate`` — registered, not yet emitting picks
  - ``shadow``    — emitting picks alongside live; metrics accumulating
  - ``live``      — the currently-promoted version; only one per component
  - ``retired``   — superseded by a newer version
  - ``rejected``  — failed promotion gate

Public API:
  register_version(component, config) -> version_id
  list_versions(component, status=None) -> list[dict]
  get_live(component) -> version_id | None
  set_live(component, version_id)
  set_shadow(component, version_id)
  retire_version(version_id)

  evaluate_version(component, version_id, *, since=None, sport=None) -> dict
      Returns Brier + ROI + n for that version's decisions in the events log.

  promote_if_better(component, candidate_id, *,
                    min_n=200, min_brier_improvement=0.005,
                    min_roi_improvement=0.0, sport=None) -> dict
      Compares candidate vs live; promotes candidate if gates pass.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

from . import events as _events

logger = logging.getLogger(__name__)


# ── Schema ────────────────────────────────────────────────────

def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS model_versions (
        version_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        component     TEXT NOT NULL,    -- e.g. 'empirical_calibration_mlb'
        status        TEXT NOT NULL,    -- candidate / shadow / live / retired / rejected
        config_json   TEXT NOT NULL,    -- the per-version config blob
        parent_id     INTEGER,          -- prior version this was derived from
        created_at    TEXT NOT NULL DEFAULT (datetime('now')),
        promoted_at   TEXT,
        retired_at    TEXT,
        notes         TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_mv_component_status
        ON model_versions(component, status);
    CREATE INDEX IF NOT EXISTS idx_mv_status
        ON model_versions(status);
    """)
    conn.commit()


def _conn() -> sqlite3.Connection:
    """Reuse the events DB. Same file = same transaction guarantees as
    decision/settle writes; no cross-DB consistency to worry about."""
    c = _events._get_conn()
    _ensure_schema(c)
    return c


# ── Registry ─────────────────────────────────────────────────

def register_version(
    component: str,
    config: dict,
    *,
    parent_id: int | None = None,
    status: str = "candidate",
    notes: str | None = None,
) -> int:
    """Register a new version of ``component``. Returns the version_id.

    The ``config`` dict is the only thing that changes between versions
    — it's the input that downstream code consults at decision time
    (calibration buckets, blend coefficients, GBM model file path,
    etc). Version_id is the stable handle decisions stamp themselves
    with so we can tie performance back to a config."""
    conn = _conn()
    cur = conn.execute(
        "INSERT INTO model_versions (component, status, config_json, "
        "  parent_id, notes) VALUES (?, ?, ?, ?, ?)",
        (component, status, json.dumps(config, default=str),
         parent_id, notes),
    )
    conn.commit()
    return int(cur.lastrowid or 0)


def list_versions(component: str | None = None,
                   status: str | None = None) -> list[dict]:
    conn = _conn()
    where: list[str] = []
    params: list = []
    if component:
        where.append("component = ?"); params.append(component)
    if status:
        where.append("status = ?"); params.append(status)
    sql = "SELECT * FROM model_versions"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC"
    rows = conn.execute(sql, params).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["config"] = json.loads(d["config_json"])
        except json.JSONDecodeError:
            d["config"] = {}
        out.append(d)
    return out


def live_version_stamp(component: str) -> str | None:
    """Convenience for picks code: returns the live version_id as a
    string suitable for stamping into events.write_decision's
    model_version field. None when no version is live yet — callers
    should pass that through unchanged so the decision event still
    records (just without a version stamp)."""
    vid = get_live(component)
    return str(vid) if vid else None


def get_live(component: str) -> int | None:
    """Return the currently-live version_id for the component, or None
    if no version has been promoted yet."""
    conn = _conn()
    row = conn.execute(
        "SELECT version_id FROM model_versions "
        "WHERE component = ? AND status = 'live' LIMIT 1",
        (component,),
    ).fetchone()
    return int(row["version_id"]) if row else None


def get_shadows(component: str) -> list[int]:
    conn = _conn()
    rows = conn.execute(
        "SELECT version_id FROM model_versions "
        "WHERE component = ? AND status = 'shadow'",
        (component,),
    ).fetchall()
    return [int(r["version_id"]) for r in rows]


def get_config(version_id: int) -> dict | None:
    conn = _conn()
    row = conn.execute(
        "SELECT config_json FROM model_versions WHERE version_id = ?",
        (version_id,),
    ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row["config_json"])
    except json.JSONDecodeError:
        return None


def set_live(component: str, version_id: int) -> None:
    """Promote ``version_id`` to live. Demotes any prior live version
    to retired in the same transaction so there's never two live
    versions for the same component."""
    conn = _conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE model_versions SET status='retired', retired_at=? "
        "WHERE component = ? AND status = 'live'",
        (now, component),
    )
    conn.execute(
        "UPDATE model_versions SET status='live', promoted_at=? "
        "WHERE version_id = ?",
        (now, version_id),
    )
    conn.commit()
    logger.info("[%s] live -> v%d", component, version_id)


def set_shadow(component: str, version_id: int) -> None:
    """Move a candidate to shadow status (start emitting alongside live)."""
    conn = _conn()
    conn.execute(
        "UPDATE model_versions SET status='shadow' WHERE version_id = ?",
        (version_id,),
    )
    conn.commit()


def retire_version(version_id: int, *, status: str = "retired") -> None:
    """Permanently take a version out of rotation."""
    conn = _conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE model_versions SET status=?, retired_at=? "
        "WHERE version_id = ?",
        (status, now, version_id),
    )
    conn.commit()


# ── Evaluation ───────────────────────────────────────────────

def evaluate_version(
    component: str,
    version_id: int,
    *,
    since: str | None = None,
    sport: str | None = None,
) -> dict:
    """Compute Brier + ROI + n for a version's decisions.

    Joins decision events stamped with this ``model_version`` to their
    matching settle events. Returns ``{n, brier, roi, hits, profit}``
    or ``{n: 0}`` when the version hasn't emitted enough picks to
    score yet.

    Only counts settled decisions (W/L) — pushes/voids excluded from
    Brier so non-event refunds don't drag the score."""
    conn = _conn()
    where: list[str] = ["e.event_type = 'decision'",
                          "e.model_version = ?"]
    params: list = [str(version_id)]
    if sport:
        where.append("e.sport = ?"); params.append(sport)
    if since:
        where.append("e.ts >= ?"); params.append(since)
    where_sql = " AND ".join(where)
    rows = conn.execute(
        f"SELECT e.id, e.sport, e.pick_id, e.payload, "
        f"       s.payload AS settle_payload "
        f"FROM events e "
        f"LEFT JOIN events s "
        f"  ON s.event_type='settle' AND s.sport=e.sport "
        f"  AND s.pick_id=e.pick_id "
        f"WHERE {where_sql}",
        params,
    ).fetchall()

    n = 0
    brier_sum = 0.0
    profit_sum = 0.0
    hits = 0
    settled_for_brier = 0
    for r in rows:
        try:
            payload = json.loads(r["payload"])
            settle = json.loads(r["settle_payload"]) if r["settle_payload"] else None
        except json.JSONDecodeError:
            continue
        if not settle:
            continue
        result = settle.get("result")
        if result not in ("W", "L"):
            continue
        prob = payload.get("calibrated_prob") or payload.get("raw_prob")
        if prob is None:
            continue
        outcome = 1 if result == "W" else 0
        brier_sum += (float(prob) - outcome) ** 2
        settled_for_brier += 1
        profit_sum += float(settle.get("profit") or 0)
        if outcome == 1:
            hits += 1
        n += 1

    return {
        "version_id": version_id,
        "component": component,
        "n": n,
        "brier": round(brier_sum / settled_for_brier, 5) if settled_for_brier else None,
        "roi": round(profit_sum / n, 2) if n else 0.0,
        "hit_rate": round(hits / n, 4) if n else None,
        "profit": round(profit_sum, 2),
    }


# ── Promotion gate ───────────────────────────────────────────

def promote_if_better(
    component: str,
    candidate_id: int,
    *,
    min_n: int = 200,
    min_brier_improvement: float = 0.005,
    min_roi_improvement: float = 0.0,
    sport: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Compare candidate vs live; promote candidate only when ALL gates
    pass.

    Gates (all must hold):
      - candidate has at least ``min_n`` settled decisions
      - candidate's Brier is at least ``min_brier_improvement`` lower
        than live's (lower Brier = better calibration)
      - candidate's ROI is at least ``min_roi_improvement`` higher
        than live's

    Returns a verdict dict with the comparison + action taken.
    """
    live_id = get_live(component)
    cand = evaluate_version(component, candidate_id, sport=sport)

    if cand["n"] < min_n:
        return {
            "verdict": "wait",
            "reason": f"candidate n={cand['n']} below min_n={min_n}",
            "candidate": cand,
            "live": evaluate_version(component, live_id, sport=sport)
                if live_id else None,
        }

    if live_id is None:
        # No incumbent — auto-promote candidate
        if not dry_run:
            set_live(component, candidate_id)
        return {
            "verdict": "promote",
            "reason": "no live incumbent",
            "candidate": cand,
            "live": None,
        }

    live = evaluate_version(component, live_id, sport=sport)
    cand_brier = cand.get("brier")
    live_brier = live.get("brier")
    brier_delta = (live_brier - cand_brier) if (cand_brier is not None and live_brier is not None) else None
    roi_delta = cand["roi"] - live["roi"]

    if brier_delta is None:
        return {
            "verdict": "wait",
            "reason": "live has no Brier history yet",
            "candidate": cand,
            "live": live,
        }

    gates_passed = (
        brier_delta >= min_brier_improvement
        and roi_delta >= min_roi_improvement
    )
    if gates_passed:
        if not dry_run:
            set_live(component, candidate_id)
        return {
            "verdict": "promote",
            "reason": (f"Brier improved by {brier_delta:.4f} "
                       f"(>= {min_brier_improvement}); ROI delta "
                       f"{roi_delta:+.2f} (>= {min_roi_improvement})"),
            "candidate": cand,
            "live": live,
            "brier_delta": round(brier_delta, 5),
            "roi_delta": round(roi_delta, 2),
        }
    return {
        "verdict": "reject",
        "reason": (f"gates failed: Brier delta {brier_delta:+.4f}, "
                   f"ROI delta {roi_delta:+.2f}"),
        "candidate": cand,
        "live": live,
        "brier_delta": round(brier_delta, 5),
        "roi_delta": round(roi_delta, 2),
    }


# ── CLI ──────────────────────────────────────────────────────

def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.model_versions")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list")
    p_list.add_argument("--component", default=None)
    p_list.add_argument("--status", default=None)

    p_eval = sub.add_parser("eval")
    p_eval.add_argument("component")
    p_eval.add_argument("version_id", type=int)
    p_eval.add_argument("--sport", default=None)

    p_prom = sub.add_parser("promote-if-better")
    p_prom.add_argument("component")
    p_prom.add_argument("candidate_id", type=int)
    p_prom.add_argument("--min-n", type=int, default=200)
    p_prom.add_argument("--min-brier", type=float, default=0.005)
    p_prom.add_argument("--min-roi", type=float, default=0.0)
    p_prom.add_argument("--sport", default=None)
    p_prom.add_argument("--dry-run", action="store_true")

    p_set = sub.add_parser("set-live")
    p_set.add_argument("component")
    p_set.add_argument("version_id", type=int)

    p_reg = sub.add_parser("register")
    p_reg.add_argument("component")
    p_reg.add_argument("config_json")
    p_reg.add_argument("--parent", type=int, default=None)
    p_reg.add_argument("--status", default="candidate")
    p_reg.add_argument("--notes", default=None)

    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")

    if args.cmd == "list":
        out = list_versions(args.component, args.status)
    elif args.cmd == "eval":
        out = evaluate_version(args.component, args.version_id,
                                  sport=args.sport)
    elif args.cmd == "promote-if-better":
        out = promote_if_better(
            args.component, args.candidate_id,
            min_n=args.min_n,
            min_brier_improvement=args.min_brier,
            min_roi_improvement=args.min_roi,
            sport=args.sport, dry_run=args.dry_run,
        )
    elif args.cmd == "set-live":
        set_live(args.component, args.version_id)
        out = {"set_live": args.version_id, "component": args.component}
    elif args.cmd == "register":
        try:
            cfg = json.loads(args.config_json)
        except json.JSONDecodeError as e:
            print(f"bad config JSON: {e}")
            return 1
        vid = register_version(args.component, cfg,
                                parent_id=args.parent,
                                status=args.status, notes=args.notes)
        out = {"version_id": vid}
    else:
        ap.error(f"unknown cmd {args.cmd}")
        return 1
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
