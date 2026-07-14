"""POTD read APIs — never write to the DB.

Used by the dashboard to render the POTD card and the running summary.
get_today_potd is also called from get_or_create_potd inside _select to
return the canonical DB row shape after locking — that's why this lives
in its own module (avoids a circular import with _select).
"""

from __future__ import annotations
from datetime import datetime

from ._storage import _get_conn, _ensure_potd_table, _potd_table
from .._tz import et_today_str


def get_potd_summary(sport: str, limit: int = 30) -> dict:
    """Return running POTD totals + recent history for the dashboard."""
    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    # Stake-weighted profit + ROI (mirrors per-sport tracker math).
    # Sport-framework POTD tables that pre-date the stake_units column
    # silently fall through the COALESCE to 1.0u so legacy 1u-basis
    # totals stay consistent.
    has_stake = bool(
        conn.execute(
            "SELECT 1 FROM pragma_table_info('pick_of_day') "
            " WHERE name = 'stake_units'"
        ).fetchone()
    )
    if has_stake:
        overall = conn.execute("""
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
            FROM pick_of_day
        """).fetchone()
    else:
        overall = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
                COALESCE(SUM(profit), 0) as profit,
                COALESCE(SUM(CASE WHEN result IN ('W','L') THEN 1.0
                                    ELSE 0 END), 0) as stake_units_settled
            FROM pick_of_day
        """).fetchone()

    overall = dict(overall)
    w = overall.get("wins") or 0
    l = overall.get("losses") or 0
    # ROI denominator is stake-units of settled picks (W+L). Pushes
    # don't move ROI either direction so they're excluded from both
    # sides of the ratio.
    settled_total = w + l
    staked_u = overall.get("stake_units_settled") or 0

    recent = conn.execute(
        "SELECT * FROM pick_of_day ORDER BY date DESC LIMIT ?", (limit,)
    ).fetchall()
    # Project stake-weighted profit onto each recent row.
    recent_out = []
    for r in recent:
        d = dict(r)
        if has_stake and d.get("profit") is not None:
            stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
            d["profit"] = round(d["profit"] * stake_u, 2)
        recent_out.append(d)

    return {
        "total": overall.get("total") or 0,
        "wins": w,
        "losses": l,
        "pushes": overall.get("pushes") or 0,
        "pending": overall.get("pending") or 0,
        "profit": round(overall.get("profit") or 0, 2),
        "win_pct": round(w / settled_total * 100, 1) if settled_total > 0 else 0,
        "roi": round((overall.get("profit") or 0) / staked_u, 1) if staked_u > 0 else 0,
        "recent": recent_out,
    }


def get_today_potd(sport: str, date: str | None = None,
                   view: str = "q1",
                   tour: str | None = None) -> dict | None:
    """Fetch just today's POTD (doesn't create one).

    Annotates the response with a computed `clv` field when both odds
    and closing_odds are present, so the UI doesn't have to redo the
    arithmetic. Positive CLV = we got a better price than the close.

    ``view`` only matters for NBA. 'q1' reads pick_of_day, 'full' reads
    pick_of_day_full.

    ``tour`` only matters for tennis. 'atp' or 'wta' selects the
    per-tour POTD; ATP and WTA can coexist for the same date.
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    target_date = date or et_today_str()
    table = _potd_table(sport, view)
    if sport == "tennis" and tour:
        row = conn.execute(
            f"SELECT * FROM {table} WHERE date = ? AND tour = ?",
            (target_date, tour),
        ).fetchone()
    else:
        row = conn.execute(
            f"SELECT * FROM {table} WHERE date = ?", (target_date,)
        ).fetchone()
    if not row:
        return None
    out = dict(row)
    bet_odds = out.get("odds")
    close = out.get("closing_odds")
    if bet_odds and close:
        bet_imp = abs(bet_odds) / (abs(bet_odds) + 100) if bet_odds < 0 \
                  else 100 / (bet_odds + 100)
        close_imp = abs(close) / (abs(close) + 100) if close < 0 \
                    else 100 / (close + 100)
        out["clv"] = round((close_imp - bet_imp) * 100, 2)
    return out
