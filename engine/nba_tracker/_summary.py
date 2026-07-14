"""NBA tracker summary + history reads."""

from __future__ import annotations

from ._helpers import _compute_clv


def get_pick_summary() -> dict:
    """Get running totals across all NBA picks.

    Profit + ROI are stake-weighted: each row contributes
    ``profit * stake_units`` to the numerator and ``stake_units`` to
    the unit-staked denominator. Legacy rows with NULL stake_units fall
    back to 1.0u so historical 1u-basis totals stay consistent.
    """
    from ..nba_db import get_conn

    conn = get_conn()

    summary = {}
    for bt in ["Q1_SPREAD", "Q1_TOTAL", "Q1_ML",
               "ML", "SPREAD", "TOTAL", "ALT SPREAD", "ALT TOTAL"]:
        row = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
                COALESCE(SUM(profit * COALESCE(stake_units, 1.0)), 0) as profit,
                COALESCE(SUM(CASE WHEN result IN ('W','L')
                                    THEN COALESCE(stake_units, 1.0)
                                    ELSE 0 END), 0) as stake_units_settled
            FROM nba_picks WHERE bet_type = ?
        """, (bt,)).fetchone()

        total = row["total"] or 0
        w = row["wins"] or 0
        l = row["losses"] or 0
        settled_count = w + l
        staked_u = row["stake_units_settled"] or 0
        summary[bt] = {
            "total": total,
            "wins": w,
            "losses": l,
            "pushes": row["pushes"] or 0,
            "pending": row["pending"] or 0,
            "profit": round(row["profit"], 2),
            "win_pct": round(w / settled_count * 100, 1) if settled_count > 0 else 0,
            "roi": round(row["profit"] / staked_u, 1) if staked_u > 0 else 0,
        }

    recent = conn.execute("""
        SELECT * FROM nba_picks ORDER BY created_at DESC LIMIT 30
    """).fetchall()

    totals = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
            COALESCE(SUM(profit * COALESCE(stake_units, 1.0)), 0) as profit,
            COALESCE(SUM(CASE WHEN result IN ('W','L')
                                THEN COALESCE(stake_units, 1.0)
                                ELSE 0 END), 0) as stake_units_settled
        FROM nba_picks
    """).fetchone()

    tw = totals["wins"] or 0
    tl = totals["losses"] or 0
    tstaked = totals["stake_units_settled"] or 0

    clv_rows = conn.execute("""
        SELECT odds, closing_odds FROM nba_picks
        WHERE result IS NOT NULL AND odds IS NOT NULL AND closing_odds IS NOT NULL
    """).fetchall()
    clv_values = []
    for r in clv_rows:
        clv = _compute_clv(r["odds"], r["closing_odds"])
        if clv is not None:
            clv_values.append(clv)
    avg_clv = round(sum(clv_values) / len(clv_values), 2) if clv_values else None

    # Project stake-weighted profit onto each recent row so the per-row
    # P/L column matches the summed totals above.
    recent_out = []
    for r in recent:
        d = dict(r)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        recent_out.append(d)

    return {
        "by_type": summary,
        "overall": {
            "total": totals["total"] or 0,
            "wins": tw,
            "losses": tl,
            "pending": totals["pending"] or 0,
            "profit": round(totals["profit"] or 0, 2),
            "win_pct": round(tw / (tw + tl) * 100, 1) if (tw + tl) > 0 else 0,
            "roi": round((totals["profit"] or 0) / tstaked, 1) if tstaked > 0 else 0,
            "avg_clv": avg_clv,
            "clv_sample": len(clv_values),
        },
        "recent": recent_out,
    }


def get_nba_pick_history(limit: int = 30) -> list[dict]:
    """Return the most recent NBA picks for the tracker history tab.

    Mirrors get_nba_pick_summary() but peels off just the `recent` list
    so the /api/nba/tracker/history endpoint can stream a flat array.
    """
    summary = get_pick_summary() or {}
    recent = summary.get("recent") or []
    return recent[:limit]
