"""Dedicated SQLite connection for the NFL player-props paper model.

Isolated from the football GAME-model DB (``data/football/nfl.db``):
this file opens ``data/football/nfl_player.db`` which holds ONLY the
shared player-props tables (``player_game_logs``, ``player_props_picks``,
``player_props_picks_pot_day``). Keeping the paper model in its own
file guarantees it can never corrupt or lock the game model's tables.

Per-thread connection cache mirrors ``engine.football._db`` (FastAPI
runs each request on its own thread; a shared connection object
corrupts result sets under concurrency).
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

# engine/sports/nfl/_propdb.py -> parents[3] == repo root (C:\sports-model-bettor)
_DB_PATH = (Path(__file__).resolve().parents[3]
            / "data" / "football" / "nfl_player.db")

_thread_local = threading.local()


def get_conn() -> sqlite3.Connection:
    """Return this thread's cached connection to the NFL props DB."""
    conn = getattr(_thread_local, "conn", None)
    if conn is not None:
        return conn
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    _thread_local.conn = conn
    return conn


def db_path() -> str:
    return str(_DB_PATH)
