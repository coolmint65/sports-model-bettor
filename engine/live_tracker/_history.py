"""
Read paths for the live tracker — history (settled + pending) and
pending-only counts for the refresh badge.

Mirrors the prematch tracker reader shape so the frontend's existing
TrackerView can switch between prematch and live via a single sport
prop + scope filter (Phase 3d).
"""

from __future__ import annotations
import logging

from ._schema import ensure_table, get_conn, table_name

logger = logging.getLogger(__name__)


def list_history(sport: str, limit: int | None = 200) -> list[dict]:
    """Return live picks ordered most-recent first. Includes pending
    + settled rows so the tracker dashboard shows an in-flight live
    pick alongside its just-resolved peers.

    Each row carries the snapshot fields (pick_at_period / clock /
    score) so the UI can render "locked at Q3 6:42, DET +5" alongside
    the W/L outcome.
    """
    sport = sport.lower()
    ensure_table(sport)
    conn = get_conn(sport)
    tbl = table_name(sport)
    sql = f"SELECT * FROM {tbl} ORDER BY datetime(created_at) DESC"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    rows = conn.execute(sql).fetchall()
    return [dict(r) for r in rows]


def list_pending(sport: str) -> list[dict]:
    """Return pending picks only — used by the live-bets refresh badge
    to show 'N live picks active' when the user is scrolling a
    different tab."""
    sport = sport.lower()
    ensure_table(sport)
    conn = get_conn(sport)
    tbl = table_name(sport)
    rows = conn.execute(
        f"SELECT * FROM {tbl} WHERE result IS NULL "
        f"ORDER BY datetime(created_at) DESC"
    ).fetchall()
    return [dict(r) for r in rows]
