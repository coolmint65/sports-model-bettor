"""Materialized views over the event log (A1).

Phase A1's payoff: calibration tables, edge floors, dynamic reliability
become *projections* of the event stream rather than independently-
mutated SQLite columns. The event log is canonical; the views are
re-derivable at any point.

This module provides one materializer per consumer. Each:
  1. Reads ``decision`` + ``settle`` events from the events DB.
  2. Aggregates into the consumer's expected shape.
  3. Returns the projection — caller decides whether to dual-write
     into the legacy table or read directly.

Today (during the migration) callers still read the legacy tables;
this module is the *parallel* path being validated. After parity is
proven, the legacy paths get cut over to read from these projections.

Initial scope:
  - empirical_calibration projection (sport → bet_type → buckets)
  - edge_floors projection per (sport, bet_type, direction)
  - reliability projection per (sport, bet_type)

Each is a pure function of the event stream — same input, same output.
That's the property that makes A2 (versioned models) and A4 (isotonic
shadow) possible: project the same events through different code and
compare.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Iterable

from . import events

logger = logging.getLogger(__name__)


# ── Calibration projection ────────────────────────────────────

# Same bucket grid empirical_calibration uses today — keeps the
# parity comparison apples-to-apples.
_BUCKETS = [
    (0.30, 0.40),
    (0.40, 0.50),
    (0.50, 0.55),
    (0.55, 0.60),
    (0.60, 0.65),
    (0.65, 0.70),
    (0.70, 0.80),
    (0.80, 1.01),
]


def _bucket_for(p: float) -> tuple[float, float] | None:
    for lo, hi in _BUCKETS:
        if lo <= p < hi:
            return lo, hi
    return None


def materialize_calibration(sport: str | None = None,
                              direction_aware: bool = True) -> dict:
    """Project empirical-calibration buckets from settled decisions.

    Returns ``{sport: {bet_type: [{bucket, n, avg_pred, realized_wr}]}}``.
    direction_aware splits Over/Under and NRFI/YRFI into separate
    sub-buckets, mirroring what empirical_calibration does today via
    its ``_direction_label`` helper.
    """
    conn = events._get_conn()
    where = "WHERE event_type = 'decision'"
    params: list = []
    if sport:
        where += " AND sport = ?"
        params.append(sport)
    decisions = conn.execute(
        f"SELECT id, sport, pick_id, bet_type, pick_text, payload FROM events "
        f"{where} AND pick_id IS NOT NULL",
        params,
    ).fetchall()

    # Build pick_id → settle event index for quick join.
    settle_where = "WHERE event_type = 'settle'"
    settle_params: list = []
    if sport:
        settle_where += " AND sport = ?"
        settle_params.append(sport)
    settles = conn.execute(
        f"SELECT pick_id, sport, payload FROM events {settle_where}",
        settle_params,
    ).fetchall()
    settle_by_key: dict[tuple[str, int], dict] = {}
    import json
    for s in settles:
        try:
            p = json.loads(s["payload"]) if s["payload"] else {}
        except json.JSONDecodeError:
            continue
        settle_by_key[(s["sport"], int(s["pick_id"]))] = p

    # Aggregate per (sport, bet_type [, direction], bucket)
    agg: dict[tuple, dict] = defaultdict(
        lambda: {"n": 0, "wins": 0, "sum_pred": 0.0}
    )
    for d in decisions:
        try:
            payload = json.loads(d["payload"]) if d["payload"] else {}
        except json.JSONDecodeError:
            continue
        if not payload.get("accepted"):
            continue
        s_payload = settle_by_key.get((d["sport"], int(d["pick_id"])))
        if not s_payload:
            continue
        result = s_payload.get("result")
        if result not in ("W", "L"):  # P/V don't move calibration
            continue
        # Use calibrated_prob (post-cal) for the bucket — that's what
        # the pick was placed at. raw_prob is the model's natural output.
        prob = payload.get("calibrated_prob") or payload.get("raw_prob")
        if prob is None:
            continue
        bucket = _bucket_for(float(prob))
        if not bucket:
            continue
        bt = (d["bet_type"] or "").upper()
        direction = _direction_label(bt, d["pick_text"] or "") \
            if direction_aware else None
        key = (d["sport"], bt, direction, bucket)
        cell = agg[key]
        cell["n"] += 1
        cell["sum_pred"] += float(prob)
        if result == "W":
            cell["wins"] += 1

    # Reshape into the API surface
    out: dict[str, dict[str, list[dict]]] = {}
    for (sp, bt, direction, bucket), cell in agg.items():
        sport_table = out.setdefault(sp, {})
        bt_key = bt if direction is None else f"{bt}|{direction}"
        bucket_list = sport_table.setdefault(bt_key, [])
        bucket_list.append({
            "bucket": list(bucket),
            "n": cell["n"],
            "avg_pred": round(cell["sum_pred"] / cell["n"], 4),
            "realized_wr": round(cell["wins"] / cell["n"], 4),
        })
    # Sort buckets within each list
    for sp in out:
        for bt_key in out[sp]:
            out[sp][bt_key].sort(key=lambda b: b["bucket"][0])
    return out


def _direction_label(bet_type: str, pick_text: str) -> str | None:
    """Mirror engine.empirical_calibration._direction_label so the
    projection's bucket keying matches the legacy table."""
    if not pick_text:
        return None
    pk = pick_text.strip().lower()
    if pk.startswith("over"):
        return "over"
    if pk.startswith("under"):
        return "under"
    bt = (bet_type or "").strip().lower()
    if bt in ("1st inn", "1stinn", "nrfi"):
        if "nrfi" in pk:
            return "nrfi"
        if "yrfi" in pk:
            return "yrfi"
    return None


# ── Edge-floor projection ────────────────────────────────────

def materialize_edge_floors(sport: str | None = None) -> dict:
    """Project per-(sport, bet_type, direction) realized ROI from
    settled decisions. Caller decides what to gate as NOPLAY.

    Returns ``{sport: {bet_type|direction: {n, hits, profit, roi}}}``.
    ROI is per-$100-stake convention to match edge_floors module."""
    conn = events._get_conn()
    where = "WHERE event_type = 'decision'"
    params: list = []
    if sport:
        where += " AND sport = ?"
        params.append(sport)
    decisions = conn.execute(
        f"SELECT id, sport, pick_id, bet_type, pick_text, payload FROM events "
        f"{where} AND pick_id IS NOT NULL",
        params,
    ).fetchall()

    settle_where = "WHERE event_type = 'settle'"
    settle_params: list = []
    if sport:
        settle_where += " AND sport = ?"
        settle_params.append(sport)
    import json
    settles = conn.execute(
        f"SELECT pick_id, sport, payload FROM events {settle_where}",
        settle_params,
    ).fetchall()
    settle_by_key: dict[tuple[str, int], dict] = {}
    for s in settles:
        try:
            p = json.loads(s["payload"]) if s["payload"] else {}
        except json.JSONDecodeError:
            continue
        settle_by_key[(s["sport"], int(s["pick_id"]))] = p

    agg: dict[tuple, dict] = defaultdict(
        lambda: {"n": 0, "wins": 0, "losses": 0, "profit": 0.0}
    )
    for d in decisions:
        try:
            payload = json.loads(d["payload"]) if d["payload"] else {}
        except json.JSONDecodeError:
            continue
        if not payload.get("accepted"):
            continue
        s_payload = settle_by_key.get((d["sport"], int(d["pick_id"])))
        if not s_payload:
            continue
        result = s_payload.get("result")
        bt = (d["bet_type"] or "").upper()
        direction = _direction_label(bt, d["pick_text"] or "")
        key = (d["sport"], bt, direction)
        cell = agg[key]
        if result == "W":
            cell["n"] += 1
            cell["wins"] += 1
            cell["profit"] += float(s_payload.get("profit") or 0)
        elif result == "L":
            cell["n"] += 1
            cell["losses"] += 1
            cell["profit"] += float(s_payload.get("profit") or 0)
        # P/V don't count for ROI

    out: dict[str, dict[str, dict]] = {}
    for (sp, bt, direction), cell in agg.items():
        sport_table = out.setdefault(sp, {})
        key = bt if direction is None else f"{bt}|{direction}"
        n = cell["n"]
        sport_table[key] = {
            "n": n,
            "wins": cell["wins"],
            "losses": cell["losses"],
            "profit": round(cell["profit"], 2),
            "roi": round(cell["profit"] / n, 2) if n else 0.0,
        }
    return out


# ── Reliability projection ───────────────────────────────────

def materialize_reliability(sport: str | None = None) -> dict:
    """Per-(sport, bet_type) hit-rate aggregation from settled
    decisions. dynamic_reliability would consume this in lieu of its
    current per-sport picks-table query."""
    floors = materialize_edge_floors(sport=sport)
    out: dict[str, dict[str, dict]] = {}
    for sp, types in floors.items():
        sport_table = out.setdefault(sp, {})
        # Collapse direction sub-buckets back into pure bet_type rows
        agg: dict[str, dict] = defaultdict(
            lambda: {"n": 0, "wins": 0, "losses": 0, "profit": 0.0}
        )
        for key, cell in types.items():
            bt = key.split("|", 1)[0]
            row = agg[bt]
            row["n"] += cell["n"]
            row["wins"] += cell["wins"]
            row["losses"] += cell["losses"]
            row["profit"] += cell["profit"]
        for bt, row in agg.items():
            n = row["n"]
            sport_table[bt] = {
                "n": n,
                "hit_rate": round(row["wins"] / n, 4) if n else None,
                "roi": round(row["profit"] / n, 2) if n else 0.0,
            }
    return out


# ── CLI ─────────────────────────────────────────────────────

def _cli() -> int:
    import argparse, json, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.events_materialize")
    ap.add_argument("--projection", required=True,
                    choices=("calibration", "edge_floors", "reliability"))
    ap.add_argument("--sport", default=None)
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    if args.projection == "calibration":
        out = materialize_calibration(sport=args.sport)
    elif args.projection == "edge_floors":
        out = materialize_edge_floors(sport=args.sport)
    else:
        out = materialize_reliability(sport=args.sport)
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
