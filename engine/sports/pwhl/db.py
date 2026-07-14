"""PWHL DB layer — same shape as AHL via the shared theScore schema."""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from .._thescore_ingest import init_schema
from . import DB_PATH, TEAMS_TABLE, GAMES_TABLE

_DB_FULL = Path(__file__).resolve().parents[3] / DB_PATH
_local = threading.local()


def get_conn() -> sqlite3.Connection:
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except sqlite3.Error:
            try:
                conn.close()
            except sqlite3.Error:
                pass
            _local.conn = None
    _DB_FULL.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_FULL))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    init_schema(conn, teams_table=TEAMS_TABLE, games_table=GAMES_TABLE)
    _local.conn = conn
    return conn


def get_team(team_id: int) -> dict | None:
    row = get_conn().execute(
        f"SELECT * FROM {TEAMS_TABLE} WHERE id = ?", (team_id,),
    ).fetchone()
    return dict(row) if row else None


def get_team_by_name(name_or_short: str) -> dict | None:
    n = (name_or_short or "").lower()
    row = get_conn().execute(
        f"SELECT * FROM {TEAMS_TABLE} "
        f"WHERE LOWER(full_name) = ? OR LOWER(short_name) = ? "
        f"OR LOWER(abbreviation) = ?",
        (n, n, n),
    ).fetchone()
    return dict(row) if row else None
