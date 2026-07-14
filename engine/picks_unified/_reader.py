"""Canonical reader API for the unified picks table.

Tracker, dashboard, summary — all routed through here. No per-sport
forks. The stake-weighted ROI math, the by-bet-type tiles, the recent
history list — all once.
"""
from __future__ import annotations

from typing import Any

from ._schema import get_conn
from ._types import Pick


# ── List / fetch ──────────────────────────────────────────────


def get_pick_by_id(pick_id: int) -> Pick | None:
    row = get_conn().execute(
        "SELECT * FROM picks WHERE id=?", (pick_id,)
    ).fetchone()
    return Pick.from_row(dict(row)) if row else None


def list_picks(*,
               sport: str | None = None,
               league: str | None = None,
               game_key: str | None = None,
               result: str | None = None,  # None | "pending" | "W" | "L" | etc.
               date_from: str | None = None,
               date_to: str | None = None,
               limit: int = 200,
               include_shadows: bool = True,
               ) -> list[Pick]:
    """Filtered fetch of pick rows. Ordered newest-first by creation
    time so a tracker call gets recent activity first."""
    conn = get_conn()
    clauses = []
    params: list[Any] = []
    if sport:
        clauses.append("sport=?")
        params.append(sport)
    if league:
        clauses.append("league=?")
        params.append(league)
    if game_key:
        clauses.append("game_key=?")
        params.append(game_key)
    if result is not None:
        if result == "pending":
            clauses.append("result IS NULL")
        else:
            clauses.append("result=?")
            params.append(result)
    if date_from:
        clauses.append("pick_date >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("pick_date <= ?")
        params.append(date_to)
    if not include_shadows:
        clauses.append("is_shadow = 0")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = conn.execute(
        f"SELECT * FROM picks {where} "
        f"ORDER BY created_at DESC LIMIT ?",
        (*params, limit),
    ).fetchall()
    return [Pick.from_row(dict(r)) for r in rows]


# ── Summary (stake-weighted) ──────────────────────────────────


def get_summary(*,
                sport: str | None = None,
                league: str | None = None,
                date_from: str | None = None,
                date_to: str | None = None,
                ) -> dict:
    """Return the standard ``{overall, by_type, recent}`` summary
    every tracker UI consumes.

    Stake-weighted: ``profit_total = SUM(profit * stake_units)`` and
    ROI denom is ``SUM(stake_units WHERE result IN W,L)``. Shadow
    picks (stake=0) contribute nothing to profit or denom but still
    count toward W/L tallies — they're "what the model would have
    bet" telemetry.
    """
    conn = get_conn()

    clauses = []
    params: list[Any] = []
    if sport:
        clauses.append("sport=?")
        params.append(sport)
    if league:
        clauses.append("league=?")
        params.append(league)
    if date_from:
        clauses.append("pick_date >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("pick_date <= ?")
        params.append(date_to)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    # Overall
    overall_row = conn.execute(
        f"""SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) AS wins,
              SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) AS losses,
              SUM(CASE WHEN result='P' THEN 1 ELSE 0 END) AS pushes,
              SUM(CASE WHEN result='V' THEN 1 ELSE 0 END) AS voids,
              SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) AS pending,
              COALESCE(SUM(profit * stake_units), 0) AS profit,
              COALESCE(SUM(CASE WHEN result IN ('W','L')
                                  THEN stake_units ELSE 0 END), 0) AS stake_settled
            FROM picks {where}""",
        params,
    ).fetchone()

    tw = overall_row["wins"] or 0
    tl = overall_row["losses"] or 0
    tstake = overall_row["stake_settled"] or 0

    overall = {
        "total":   overall_row["total"] or 0,
        "wins":    tw,
        "losses":  tl,
        "pushes":  overall_row["pushes"] or 0,
        "voids":   overall_row["voids"] or 0,
        "pending": overall_row["pending"] or 0,
        "profit":  round(overall_row["profit"] or 0, 2),
        "win_pct": round(tw / (tw + tl) * 100, 1) if (tw + tl) else 0.0,
        "roi":     round((overall_row["profit"] or 0) / tstake, 1)
                    if tstake else 0.0,
    }

    # By bet_type
    by_type_rows = conn.execute(
        f"""SELECT
              bet_type,
              COUNT(*) AS total,
              SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) AS wins,
              SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) AS losses,
              SUM(CASE WHEN result='P' THEN 1 ELSE 0 END) AS pushes,
              SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) AS pending,
              COALESCE(SUM(profit * stake_units), 0) AS profit,
              COALESCE(SUM(CASE WHEN result IN ('W','L')
                                  THEN stake_units ELSE 0 END), 0) AS stake_settled
            FROM picks {where}
            GROUP BY bet_type
            ORDER BY total DESC""",
        params,
    ).fetchall()

    by_type = {}
    for r in by_type_rows:
        w = r["wins"] or 0; l = r["losses"] or 0
        st = r["stake_settled"] or 0
        by_type[r["bet_type"]] = {
            "total":   r["total"] or 0,
            "wins":    w,
            "losses":  l,
            "pushes":  r["pushes"] or 0,
            "pending": r["pending"] or 0,
            "profit":  round(r["profit"] or 0, 2),
            "win_pct": round(w / (w + l) * 100, 1) if (w + l) else 0.0,
            "roi":     round((r["profit"] or 0) / st, 1) if st else 0.0,
        }

    # Recent (project stake-weighted profit per row so display matches totals)
    recent_rows = conn.execute(
        f"SELECT * FROM picks {where} ORDER BY created_at DESC LIMIT 30",
        params,
    ).fetchall()
    recent = []
    for r in recent_rows:
        d = dict(r)
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * (d.get("stake_units") or 0), 2)
        recent.append(d)

    return {"overall": overall, "by_type": by_type, "recent": recent}
