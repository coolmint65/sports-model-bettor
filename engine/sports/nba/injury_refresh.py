"""
Late-injury refresh for NBA — detect confirmed-OUT status flips vs the
morning snapshot and invalidate stale picks. Parallels MLB lineup
refresh + NHL goalie refresh.

Why this exists
---------------
NBA injury reports update throughout game day. Starters flip from
"questionable" → "OUT" (or vice versa) 30min–2h before tip and those
changes swing Q1 prices by 3-6 points. The morning picks that priced
Luka as probable become stale the moment he's ruled out — this module
snapshots confirmed-OUT player sets per team, compares vs the stored
snapshot, and on any delta in a game's home or away OUT set, drops
picks_cache + unsettled POTD for that game.

Only material (OUT / DNP / Suspended) status flips trigger invalidation.
Shifts within "questionable" / "doubtful" / "probable" don't drop the
cache — that would thrash every hour since those statuses shuffle
constantly without the player's availability actually changing.

Public API:
  refresh_for_date(date=None, record_picks=True) -> dict summary
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any
from ..._tz import et_today_str

logger = logging.getLogger(__name__)


_DDL = """
CREATE TABLE IF NOT EXISTS injury_snapshots (
    game_id         TEXT NOT NULL,
    date            TEXT NOT NULL,
    home_out_ids    TEXT NOT NULL,   -- JSON list[int] of confirmed-OUT player IDs
    away_out_ids    TEXT NOT NULL,
    home_team_id    INTEGER,
    away_team_id    INTEGER,
    fetched_at      TEXT NOT NULL,
    PRIMARY KEY (game_id)
);
CREATE INDEX IF NOT EXISTS idx_injury_snap_date ON injury_snapshots(date);
"""

# Keep in sync with engine.nba_injuries._OUT_STATUSES. Intentional
# duplication so the refresh module doesn't force an import cycle.
_OUT_KEYWORDS = {
    "out", "injured", "suspended", "g league", "two-way",
    "not with team", "dnp", "did not play",
}


def _ensure_table() -> None:
    from .db import get_conn
    conn = get_conn()
    conn.executescript(_DDL)
    conn.commit()


def _is_out(status: str) -> bool:
    s = (status or "").strip().lower()
    if not s:
        return False
    return any(k in s for k in _OUT_KEYWORDS)


def _fetch_fresh_injuries() -> None:
    """Pull the latest NBA injury report into the nba_injuries table.
    Silent on failure — a refresh run that can't reach ESPN simply
    compares current DB state against the stored snapshot (which
    won't reveal any new deltas, but also won't crash the caller)."""
    try:
        from scrapers.nba_espn import fetch_nba_injuries
        fetch_nba_injuries()
    except Exception as e:
        logger.warning("nba injury fetch failed: %s", e)


def _out_player_ids_for_team(team_id: int) -> list[int]:
    """Return the sorted list of confirmed-OUT player IDs for a team.
    Sorted so snapshot comparisons are order-independent."""
    try:
        from .db import get_team_injuries
        rows = get_team_injuries(team_id)
    except Exception as e:
        logger.debug("get_team_injuries failed for %s: %s", team_id, e)
        return []
    out_ids: list[int] = []
    for r in rows:
        if not _is_out(r.get("status") or ""):
            continue
        pid = r.get("player_id")
        if pid is None:
            continue
        try:
            out_ids.append(int(pid))
        except (TypeError, ValueError):
            continue
    return sorted(set(out_ids))


def _load_snapshot(conn, game_id: str) -> dict | None:
    row = conn.execute(
        "SELECT home_out_ids, away_out_ids FROM injury_snapshots "
        "WHERE game_id = ?",
        (game_id,),
    ).fetchone()
    if not row:
        return None
    try:
        return {
            "home": set(json.loads(row["home_out_ids"])),
            "away": set(json.loads(row["away_out_ids"])),
        }
    except Exception:
        return None


def _store_snapshot(conn, game_id: str, date: str,
                    home_team_id: int | None, away_team_id: int | None,
                    home_out: list[int], away_out: list[int]) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO injury_snapshots "
        "(game_id, date, home_out_ids, away_out_ids, "
        " home_team_id, away_team_id, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?, datetime('now'))",
        (str(game_id), date,
         json.dumps(home_out), json.dumps(away_out),
         home_team_id, away_team_id),
    )


def _invalidate_picks_cache(date: str, home: str, away: str) -> bool:
    try:
        from . import picks_cache
        picks_cache.ensure_table("nba")
        from .db import get_conn
        conn = get_conn()
        cur = conn.execute(
            "DELETE FROM picks_cache "
            "WHERE date = ? AND home = ? AND away = ?",
            (date, home, away),
        )
        conn.commit()
        return (cur.rowcount or 0) > 0
    except Exception as e:
        logger.warning("NBA picks_cache invalidation failed for %s @ %s: %s",
                       away, home, e)
        return False


def _invalidate_potd_if_affected(date: str, game_id: str) -> bool:
    try:
        from .db import get_conn
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pick_of_day (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL UNIQUE, game_id TEXT,
                matchup TEXT NOT NULL, bet_type TEXT NOT NULL,
                pick TEXT NOT NULL, model_prob REAL, edge REAL,
                odds INTEGER, kelly_pct REAL, reasoning TEXT,
                result TEXT, profit REAL,
                created_at TEXT DEFAULT (datetime('now')), settled_at TEXT
            )
        """)
        row = conn.execute(
            "SELECT id, game_id, result FROM pick_of_day WHERE date = ?",
            (date,),
        ).fetchone()
        if not row:
            return False
        if str(row["game_id"] or "") != str(game_id):
            return False
        if row["result"] in ("W", "L", "P"):
            return False
        conn.execute("DELETE FROM pick_of_day WHERE id = ?", (row["id"],))
        conn.commit()
        return True
    except Exception as e:
        logger.warning("NBA POTD invalidation failed for game %s on %s: %s",
                       game_id, date, e)
        return False


def refresh_for_date(date: str | None = None,
                     record_picks: bool = True) -> dict:
    """Snapshot today's confirmed-OUT player sets per game, detect
    deltas vs the morning snapshot, invalidate stale picks + POTD, and
    optionally re-run the NBA tracker with force=True.

    Only flips in the OUT set count as deltas; day-to-day status
    reshuffles do not. Safe to run every hour on game day."""
    target_date = date or et_today_str()
    summary: dict[str, Any] = {
        "date": target_date,
        "games_checked": 0,
        "first_snapshot": 0,
        "deltas": 0,
        "invalidated": 0,
        "potd_invalidated": False,
        "re_recorded": False,
        "errors": [],
    }

    # Always refresh the injury table first so we're comparing against
    # the latest report, not whatever was last written by scrape time.
    _fetch_fresh_injuries()

    try:
        _ensure_table()
        from .db import get_conn
        conn = get_conn()
        games = conn.execute(
            "SELECT g.game_id, g.home_team_id, g.away_team_id, "
            "       ht.abbreviation AS home_abbr, "
            "       at.abbreviation AS away_abbr "
            "FROM nba_games g "
            "LEFT JOIN nba_teams ht ON g.home_team_id = ht.id "
            "LEFT JOIN nba_teams at ON g.away_team_id = at.id "
            "WHERE g.date = ?",
            (target_date,),
        ).fetchall()
    except Exception as e:
        summary["errors"].append(f"db query: {e}")
        return summary

    any_delta = False
    for g in games:
        summary["games_checked"] += 1
        game_id = str(g["game_id"])
        h_abbr = g["home_abbr"]
        a_abbr = g["away_abbr"]
        h_team = g["home_team_id"]
        a_team = g["away_team_id"]

        home_out = _out_player_ids_for_team(h_team) if h_team else []
        away_out = _out_player_ids_for_team(a_team) if a_team else []

        prev = _load_snapshot(conn, game_id)
        if prev is None:
            _store_snapshot(conn, game_id, target_date, h_team, a_team,
                            home_out, away_out)
            summary["first_snapshot"] += 1
            continue

        home_changed = set(home_out) != prev["home"]
        away_changed = set(away_out) != prev["away"]

        if home_changed or away_changed:
            summary["deltas"] += 1
            any_delta = True
            if h_abbr and a_abbr:
                if _invalidate_picks_cache(target_date, h_abbr, a_abbr):
                    summary["invalidated"] += 1
            if _invalidate_potd_if_affected(target_date, game_id):
                summary["potd_invalidated"] = True

        _store_snapshot(conn, game_id, target_date, h_team, a_team,
                        home_out, away_out)

    conn.commit()

    if any_delta and record_picks:
        try:
            from . import nba_tracker
            nba_tracker.record_picks(date=target_date, force=True)
            summary["re_recorded"] = True
        except Exception as e:
            summary["errors"].append(f"nba_tracker.record_picks: {e}")
            logger.warning("NBA re-record after injury delta failed: %s", e)

    if summary["deltas"]:
        logger.info("nba_injury_refresh[%s]: %d deltas / %d invalidated "
                    "(checked %d games, first snapshot on %d)",
                    target_date, summary["deltas"], summary["invalidated"],
                    summary["games_checked"], summary["first_snapshot"])
    return summary
