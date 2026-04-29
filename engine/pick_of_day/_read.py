"""POTD read APIs — never write to the DB.

Used by the dashboard to render the POTD card and the running summary.
get_today_potd is also called from get_or_create_potd inside _select to
return the canonical DB row shape after locking — that's why this lives
in its own module (avoids a circular import with _select).
"""

from __future__ import annotations
from datetime import datetime

from ._storage import _get_conn, _ensure_potd_table, _potd_table


def get_potd_summary(sport: str, limit: int = 30) -> dict:
    """Return running POTD totals + recent history for the dashboard."""
    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    overall = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
            SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
            COALESCE(SUM(profit), 0) as profit
        FROM pick_of_day
    """).fetchone()

    overall = dict(overall)
    w = overall.get("wins") or 0
    l = overall.get("losses") or 0
    settled_total = w + l

    recent = conn.execute(
        "SELECT * FROM pick_of_day ORDER BY date DESC LIMIT ?", (limit,)
    ).fetchall()

    return {
        "total": overall.get("total") or 0,
        "wins": w,
        "losses": l,
        "pushes": overall.get("pushes") or 0,
        "pending": overall.get("pending") or 0,
        "profit": round(overall.get("profit") or 0, 2),
        "win_pct": round(w / settled_total * 100, 1) if settled_total > 0 else 0,
        "roi": round((overall.get("profit") or 0) / settled_total, 1) if settled_total > 0 else 0,
        "recent": [dict(r) for r in recent],
    }


def get_today_potd(sport: str, date: str | None = None,
                   view: str = "q1") -> dict | None:
    """Fetch just today's POTD (doesn't create one).

    Annotates the response with a computed `clv` field when both odds
    and closing_odds are present, so the UI doesn't have to redo the
    arithmetic. Positive CLV = we got a better price than the close.

    ``view`` only matters for NBA. 'q1' reads pick_of_day, 'full' reads
    pick_of_day_full.
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    table = _potd_table(sport, view)
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
