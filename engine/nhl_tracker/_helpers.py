"""NHL tracker helpers + DB connection."""

from __future__ import annotations
import logging
import sqlite3
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

_local = threading.local()

# Phase 1 derivative bet types — excluded from main NHL tracker
# recording at sync time. Derivatives flow through
# engine.derivative_tracker for isolated paper-bet evaluation.
_NHL_DERIVATIVE_TYPES: set[str] = {
    "Team Total", "Period Total", "Period BTS", "Period DNB",
    "Total O/E", "Overtime", "BTS",
}


def _core_picks(picks: list[dict]) -> list[dict]:
    return [p for p in picks if p.get("type") not in _NHL_DERIVATIVE_TYPES]


def _compute_clv(bet_odds, closing_odds):
    """Compute closing line value.
    Positive CLV = got better price than closing line = sharp.
    """
    if not bet_odds or not closing_odds:
        return None
    bet_implied = abs(bet_odds) / (abs(bet_odds) + 100) if bet_odds < 0 else 100 / (bet_odds + 100)
    close_implied = abs(closing_odds) / (abs(closing_odds) + 100) if closing_odds < 0 else 100 / (closing_odds + 100)
    return round((close_implied - bet_implied) * 100, 2)


def _extract_nhl_closing_for_pick(bet_type: str, pick_text: str,
                                  home_abbr: str, game_odds: dict) -> int | None:
    """Pure helper: pick the right closing-odds field for an NHL pick
    out of a Hard Rock odds bucket. Mirrors
    engine.tracker._extract_closing_for_pick for the NHL-specific
    bet_types (ML / PL / O/U / ALT PL)."""
    if not game_odds:
        return None
    pk = pick_text or ""
    parts = pk.split()
    if bet_type == "ML":
        return (game_odds.get("home_ml") if pk == home_abbr
                else game_odds.get("away_ml"))
    if bet_type in ("O/U", "OU"):
        return (game_odds.get("over_odds") if "Over" in pk
                else game_odds.get("under_odds"))
    if bet_type == "PL":
        pick_team = parts[0] if parts else ""
        return (game_odds.get("home_spread_odds") if pick_team == home_abbr
                else game_odds.get("away_spread_odds"))
    if bet_type == "ALT PL":
        try:
            line = float(parts[1]) if len(parts) >= 2 else None
        except (ValueError, IndexError):
            line = None
        pick_team = parts[0] if parts else ""
        is_home = pick_team == home_abbr
        if line is not None:
            for alt in game_odds.get("alt_spreads", []) or []:
                if alt.get("point") == line or alt.get("point") == -line:
                    return (alt.get("home_odds") if is_home
                            else alt.get("away_odds"))
        return (game_odds.get("home_spread_odds") if is_home
                else game_odds.get("away_spread_odds"))
    return None


def _get_nhl_db():
    """Get NHL picks DB connection (SQLite, separate from MLB)."""
    db_path = Path(__file__).resolve().parent.parent.parent / "data" / "nhl.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        _local.conn = conn

    conn = _local.conn

    conn.execute("""
        CREATE TABLE IF NOT EXISTS nhl_picks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT,
            date TEXT NOT NULL,
            matchup TEXT,
            bet_type TEXT NOT NULL,
            pick TEXT NOT NULL,
            model_prob REAL,
            edge REAL,
            odds INTEGER,
            closing_odds INTEGER,
            result TEXT,
            profit REAL,
            created_at TEXT DEFAULT (datetime('now')),
            settled_at TEXT
        )
    """)

    # Migration: add closing_odds column to existing databases
    try:
        existing = conn.execute("PRAGMA table_info(nhl_picks)").fetchall()
        col_names = [r[1] for r in existing]
        if "closing_odds" not in col_names:
            conn.execute("ALTER TABLE nhl_picks ADD COLUMN closing_odds INTEGER")
    except Exception as e:
        # Column already exists or table just created with it.
        logger.debug("nhl_picks closing_odds migration skipped: %s", e)

    conn.commit()
    return conn
