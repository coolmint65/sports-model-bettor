"""Tracker summary read.

Per-bet-type tiles + overall record + recent picks + average CLV.
Read-only; never writes to the DB.
"""

from __future__ import annotations

from ..db import get_conn
from ._helpers import _compute_clv


def get_pick_summary() -> dict:
    """Get running totals across all recorded picks."""
    conn = get_conn()

    summary = {}
    # Map canonical keys to all possible bet_type values (old lowercase
    # + new uppercase) so legacy rows still aggregate cleanly.
    bt_aliases = {
        "ML": ("ML", "ml"),
        "O/U": ("O/U", "ou"),
        "1st INN": ("1st INN", "nrfi"),
        "RL": ("RL", "rl"),
        "F5 ML": ("F5 ML",),
        "F5 O/U": ("F5 O/U",),
        "F5 RL": ("F5 RL",),
    }
    for bt, aliases in bt_aliases.items():
        placeholders = ",".join("?" for _ in aliases)
        # Profit + ROI are stake-weighted: each row contributes
        #   profit * stake_units to the numerator
        #   stake_units to the unit-staked denominator
        # so that 0.5u / 0.75u / 1.0u picks reflect their actual size.
        # Legacy rows with NULL stake_units fall back to 1.0u so the
        # arithmetic stays consistent with the historical 1u-basis.
        row = conn.execute(f"""
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
            FROM picks WHERE bet_type IN ({placeholders})
        """, aliases).fetchone()

        total = row["total"] or 0
        w = row["wins"] or 0
        l = row["losses"] or 0
        settled = w + l
        staked_u = row["stake_units_settled"] or 0
        # ROI denom is staked $: stake_units × 100. So roi_pct =
        # profit_$ / (stake_units × 100) × 100 = profit / stake_units.
        summary[bt] = {
            "total": total,
            "wins": w,
            "losses": l,
            "pushes": row["pushes"],
            "pending": row["pending"],
            "profit": round(row["profit"], 2),
            "win_pct": round(w / settled * 100, 1) if settled > 0 else 0,
            "roi": round(row["profit"] / staked_u, 1) if staked_u > 0 else 0,
        }

    # Aggregate F5 tile -- the UI tile is a single "First 5 Innings" card
    # summing ML + O/U + RL variants. Per-market splits remain under the
    # individual keys for anyone wanting the breakdown.
    f5_rows = [summary.get(k) for k in ("F5 ML", "F5 O/U", "F5 RL")]
    f5_rows = [r for r in f5_rows if r]
    if f5_rows:
        agg_total = sum(r["total"] for r in f5_rows)
        agg_w = sum(r["wins"] for r in f5_rows)
        agg_l = sum(r["losses"] for r in f5_rows)
        agg_p = sum((r["pushes"] or 0) for r in f5_rows)
        agg_pend = sum((r["pending"] or 0) for r in f5_rows)
        agg_profit = round(sum(r["profit"] for r in f5_rows), 2)
        settled = agg_w + agg_l
        summary["F5"] = {
            "total": agg_total,
            "wins": agg_w,
            "losses": agg_l,
            "pushes": agg_p,
            "pending": agg_pend,
            "profit": agg_profit,
            "win_pct": round(agg_w / settled * 100, 1) if settled > 0 else 0,
            "roi": round(agg_profit / settled, 1) if settled > 0 else 0,
        }

    recent = conn.execute("""
        SELECT * FROM picks ORDER BY created_at DESC LIMIT 20
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
        FROM picks
    """).fetchone()

    tw = totals["wins"] or 0
    tl = totals["losses"] or 0
    tstaked = totals["stake_units_settled"] or 0

    # Compute CLV across all settled picks that have closing odds
    clv_rows = conn.execute("""
        SELECT odds, closing_odds FROM picks
        WHERE result IS NOT NULL AND odds IS NOT NULL AND closing_odds IS NOT NULL
    """).fetchall()
    clv_values = []
    for r in clv_rows:
        clv = _compute_clv(r["odds"], r["closing_odds"])
        if clv is not None:
            clv_values.append(clv)
    avg_clv = round(sum(clv_values) / len(clv_values), 2) if clv_values else None

    # Recent rows: project stake-weighted profit onto each row so the
    # per-row P/L column matches the summed totals.
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
