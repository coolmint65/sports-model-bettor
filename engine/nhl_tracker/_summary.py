"""NHL tracker summary read."""

from __future__ import annotations

from ._helpers import _get_nhl_db, _compute_clv


def get_pick_summary() -> dict:
    """Get running totals across all NHL picks.

    Profit + ROI are stake-weighted (see engine.tracker._summary for
    rationale). Legacy NULL stake_units rows fall back to 1.0u so
    historical totals remain consistent.
    """
    conn = _get_nhl_db()

    summary = {}
    for bt in ["ML", "O/U", "PL"]:
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
            FROM nhl_picks WHERE bet_type = ?
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
            "pushes": row["pushes"],
            "pending": row["pending"],
            "profit": round(row["profit"], 2),
            "win_pct": round(w / settled_count * 100, 1) if settled_count > 0 else 0,
            "roi": round(row["profit"] / staked_u, 1) if staked_u > 0 else 0,
        }

    recent = conn.execute("""
        SELECT * FROM nhl_picks ORDER BY created_at DESC LIMIT 30
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
        FROM nhl_picks
    """).fetchone()

    tw = totals["wins"] or 0
    tl = totals["losses"] or 0
    tstaked = totals["stake_units_settled"] or 0

    clv_rows = conn.execute("""
        SELECT odds, closing_odds FROM nhl_picks
        WHERE result IS NOT NULL AND odds IS NOT NULL AND closing_odds IS NOT NULL
    """).fetchall()
    clv_values = []
    for r in clv_rows:
        clv = _compute_clv(r["odds"], r["closing_odds"])
        if clv is not None:
            clv_values.append(clv)
    avg_clv = round(sum(clv_values) / len(clv_values), 2) if clv_values else None

    # Project stake-weighted profit per row so display matches totals.
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
