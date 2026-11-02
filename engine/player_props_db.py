"""
Player-props DB schema (Phase 2f-i).

Three tables per sport that mirror the existing pick / tracker
patterns the rest of the engine uses:

    player_game_logs           historical per-player per-game stats,
                                used to train MC pickers (2g/2h/2i)
                                and to settle prop picks against
                                actual observed values.

    player_props_picks         the live tracker — one row per pick
                                the model emits at sync/best-bets
                                time. Mirrors `derivative_picks` in
                                shape so the existing tracker UI
                                shells (PicksTable primitive) can
                                drop straight in.

    player_props_picks_pot_day per-sport prop pick-of-the-day, mirrors
                                `derivative_picks_pot_day`.

Stats are stored as a JSON blob so each sport can carry its own
shape (MLB pitcher: Ks/BB/IP; MLB batter: HR/H/TB/RBI; NBA: PTS/REB/
AST/3PM/STL/BLK; NHL: SOG/G/A/SAVES/SHOTS_AGAINST). The bet_type
column carries the granular market label so settlement logic can
key off it.

Idempotent DDL — `ensure_tables(sport)` is safe to call on every
write. Mirrors the pattern in `engine.pick_events` and
`engine.picks_cache`.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any
from ._tz import et_today_str

logger = logging.getLogger(__name__)


def _ts_now() -> str:
    """Current UTC timestamp for closing_odds_captured_at.

    Mirrors ``engine.tracker_core._cc_now_iso`` exactly so props CLV
    timestamps are directly comparable to the main-line tracker's.
    """
    from datetime import timezone
    return datetime.now(timezone.utc).isoformat()


def _conn_for(sport: str):
    if sport == "mlb":
        from .db import get_conn
        return get_conn()
    if sport == "nhl":
        from .nhl_db import get_conn
        return get_conn()
    if sport == "nba":
        from .nba_db import get_conn
        return get_conn()
    if sport == "wnba":
        # WNBA lives under the basketball framework, not its own engine
        # module. Route to the framework's per-league DB so prop tables
        # sit alongside games/teams/picks for the same league.
        from .basketball._db import get_conn
        return get_conn("wnba")
    if sport == "nfl":
        # NFL anytime-TD PAPER model. Isolated DB (data/football/
        # nfl_player.db) that holds ONLY prop tables — never the
        # football GAME model's nfl.db. See engine.sports.nfl._propdb.
        from .sports.nfl._propdb import get_conn
        return get_conn()
    raise ValueError(f"unknown sport: {sport}")


_DDL = """
CREATE TABLE IF NOT EXISTS player_game_logs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id    INTEGER NOT NULL,
    player_name  TEXT NOT NULL,
    game_id      TEXT NOT NULL,
    date         TEXT NOT NULL,
    team_id      INTEGER,
    opp_team_id  INTEGER,
    is_home      INTEGER,
    -- Sport-specific stats live in this JSON blob so a single schema
    -- holds MLB pitcher counting stats, NBA box-score lines, NHL
    -- skater/goalie stats, and any future sport.
    stats_json   TEXT NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(player_id, game_id)
);
CREATE INDEX IF NOT EXISTS idx_pgl_player_date ON player_game_logs(player_id, date);
CREATE INDEX IF NOT EXISTS idx_pgl_game        ON player_game_logs(game_id);
CREATE INDEX IF NOT EXISTS idx_pgl_date        ON player_game_logs(date);

CREATE TABLE IF NOT EXISTS player_props_picks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id       TEXT NOT NULL,
    date          TEXT NOT NULL,
    matchup       TEXT NOT NULL,
    player_id     INTEGER NOT NULL,
    player_name   TEXT NOT NULL,
    bet_type      TEXT NOT NULL,    -- e.g. 'Pitcher Ks O/U', 'Batter HR'
    pick          TEXT NOT NULL,    -- e.g. 'Skubal Over 7.5'
    line          REAL,             -- 7.5 (numeric line for settlement)
    side          TEXT,             -- 'Over' / 'Under' / 'Yes' / 'No'
    model_prob    REAL,
    edge          REAL,
    odds          INTEGER,
    confidence    TEXT,             -- 'strong' / 'moderate' / 'lean'
    stake_units   REAL,             -- Quarter-Kelly recommendation (1u = $100)
    headshot      TEXT,             -- player photo URL (nullable; NHL props 2026-10-01)
    team_logo     TEXT,             -- team logo URL (nullable; NHL props 2026-10-01)
    -- Settlement
    actual_value  REAL,             -- actual Ks/HR/PRA/SOG observed
    result        TEXT,             -- 'W' / 'L' / 'P' / NULL
    profit        REAL,
    closing_odds  INTEGER,
    closing_odds_captured_at TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    settled_at    TEXT,
    UNIQUE(game_id, player_id, bet_type, pick)
);
CREATE INDEX IF NOT EXISTS idx_pp_picks_date    ON player_props_picks(date);
CREATE INDEX IF NOT EXISTS idx_pp_picks_game    ON player_props_picks(game_id);
CREATE INDEX IF NOT EXISTS idx_pp_picks_player  ON player_props_picks(player_id);
CREATE INDEX IF NOT EXISTS idx_pp_picks_pending ON player_props_picks(date) WHERE result IS NULL;

CREATE TABLE IF NOT EXISTS player_props_picks_pot_day (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT NOT NULL UNIQUE,
    pick_id         INTEGER NOT NULL,
    selected_at     TEXT NOT NULL DEFAULT (datetime('now')),
    selection_score REAL,
    reasoning       TEXT,
    FOREIGN KEY(pick_id) REFERENCES player_props_picks(id)
);
"""


def _migrate_prop_columns(conn) -> None:
    """Add columns introduced after the original DDL, for DBs created
    before they existed — mirrors the pre-existing closing_odds /
    closing_odds_captured_at situation (those predate this file's
    migration helper entirely; the live DB already had them applied
    out-of-band, which is why insert_pick/settle_pick still carry a
    defensive PRAGMA check of their own). headshot/team_logo (2026-10-01,
    NHL props) go through this one instead. Checked via PRAGMA each
    call, same idempotent-every-write pattern as ensure_tables itself."""
    cols = {d[1] for d in conn.execute("PRAGMA table_info(player_props_picks)")}
    if "headshot" not in cols:
        conn.execute("ALTER TABLE player_props_picks ADD COLUMN headshot TEXT")
    if "team_logo" not in cols:
        conn.execute("ALTER TABLE player_props_picks ADD COLUMN team_logo TEXT")
    # Props POTD by proven accuracy (Austin 2026-11-01 "Yes to all"): the lock row carries the
    # cell it was picked from (bet_type + Over/Under), whether that cell was PROVEN, the cell's
    # record at lock time (JSON), and the stake = the card's stake chip. NULL on older rows.
    pcols = {d[1] for d in conn.execute("PRAGMA table_info(player_props_picks_pot_day)")}
    for name, typ in (("cell", "TEXT"), ("proven", "INTEGER"),
                      ("cell_record", "TEXT"), ("stake_units", "REAL")):
        if name not in pcols:
            conn.execute(f"ALTER TABLE player_props_picks_pot_day ADD COLUMN {name} {typ}")


def ensure_tables(sport: str) -> None:
    """Create the three player-props tables for ``sport`` if missing.
    Idempotent — safe to call on every write."""
    conn = _conn_for(sport)
    conn.executescript(_DDL)
    _migrate_prop_columns(conn)
    conn.commit()


def ensure_all() -> dict:
    """Create tables across all three sports. Returns per-sport status."""
    out = {}
    for sport in ("mlb", "nhl", "nba"):
        try:
            ensure_tables(sport)
            out[sport] = "ok"
        except Exception as e:
            logger.warning("player_props ensure_tables failed for %s: %s",
                           sport, e)
            out[sport] = f"error: {e}"
    return out


# ── Game-log helpers ──────────────────────────────────────────────────

def upsert_game_log(sport: str, player_id: int, player_name: str,
                    game_id: str, date: str,
                    stats: dict, *,
                    team_id: int | None = None,
                    opp_team_id: int | None = None,
                    is_home: bool | None = None) -> None:
    """Idempotent UPSERT of one (player, game) game-log row.

    Re-running with updated stats overwrites the JSON blob so
    settlement passes can replace partial late-night box scores once
    they're finalized.
    """
    ensure_tables(sport)
    conn = _conn_for(sport)
    try:
        conn.execute(
            "INSERT INTO player_game_logs "
            "(player_id, player_name, game_id, date, team_id, opp_team_id, is_home, stats_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(player_id, game_id) DO UPDATE SET "
            "  stats_json = excluded.stats_json, "
            "  team_id    = COALESCE(excluded.team_id, player_game_logs.team_id), "
            "  opp_team_id= COALESCE(excluded.opp_team_id, player_game_logs.opp_team_id), "
            "  is_home    = COALESCE(excluded.is_home, player_game_logs.is_home)",
            (
                int(player_id), player_name, str(game_id), date,
                team_id, opp_team_id,
                int(is_home) if is_home is not None else None,
                json.dumps(stats or {}),
            ),
        )
        conn.commit()
    except Exception as e:
        logger.warning("player_props.upsert_game_log failed for %s p=%s g=%s: %s",
                       sport, player_id, game_id, e)


def get_player_logs(sport: str, player_id: int,
                    since_date: str | None = None,
                    limit: int = 50) -> list[dict]:
    """Recent game logs for a player, newest first."""
    ensure_tables(sport)
    conn = _conn_for(sport)
    if since_date:
        rows = conn.execute(
            "SELECT * FROM player_game_logs "
            "WHERE player_id = ? AND date >= ? "
            "ORDER BY date DESC LIMIT ?",
            (int(player_id), since_date, int(limit)),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM player_game_logs "
            "WHERE player_id = ? "
            "ORDER BY date DESC LIMIT ?",
            (int(player_id), int(limit)),
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["stats"] = json.loads(d.pop("stats_json", "{}"))
        except Exception:
            d["stats"] = {}
        out.append(d)
    return out


# ── Props pick helpers ────────────────────────────────────────────────

def insert_pick(sport: str, *,
                game_id: str, date: str, matchup: str,
                player_id: int, player_name: str,
                bet_type: str, pick: str, line: float | None,
                side: str | None,
                model_prob: float | None,
                edge: float | None,
                odds: int | None,
                confidence: str | None = None,
                stake_units: float | None = None,
                headshot: str | None = None,
                team_logo: str | None = None) -> int | None:
    """Insert a fresh prop pick. UNIQUE constraint on (game, player,
    bet_type, pick) keeps re-runs of best-bets from duplicating rows
    — repeat calls quietly no-op when the same pick already exists.
    Returns the pick id (existing or new), or None on failure.

    ``headshot``/``team_logo`` (optional, 2026-10-01) are display-only
    URLs set at insert time and never touched again — they're static
    per player/team, so a repeat sighting of an already-existing pick
    (the "already existed" branch below) doesn't need to refresh them.
    """
    ensure_tables(sport)
    # Compute stake_units when caller didn't pass one (every caller
    # should now, but legacy paths still work).
    if stake_units is None and model_prob is not None and odds is not None:
        try:
            from ._pick_helpers import stake_units_for
            stake_units = stake_units_for(model_prob, edge or 0, odds=odds)
        except Exception:
            stake_units = None
    conn = _conn_for(sport)
    try:
        cur = conn.execute(
            "INSERT OR IGNORE INTO player_props_picks "
            "(game_id, date, matchup, player_id, player_name, "
            " bet_type, pick, line, side, model_prob, edge, odds, confidence, "
            " stake_units, headshot, team_logo) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                str(game_id), date, matchup,
                int(player_id), player_name,
                bet_type, pick, line, side,
                model_prob, edge, odds, confidence,
                stake_units, headshot, team_logo,
            ),
        )
        conn.commit()
        if cur.lastrowid:
            return cur.lastrowid
        # Already existed — fetch its id for the caller.
        row = conn.execute(
            "SELECT id FROM player_props_picks "
            "WHERE game_id = ? AND player_id = ? AND bet_type = ? AND pick = ?",
            (str(game_id), int(player_id), bet_type, pick),
        ).fetchone()
        if row and odds is not None:
            # Closing-odds capture (2026-08-09): every re-sighting of a
            # still-pending pick carries the board's CURRENT price, so the
            # last refresh before tip is the closing snapshot — props had
            # no CLV data at all before this. Semantics: it's the price at
            # the pick's last QUALIFYING sighting (if the edge dies late,
            # the refresh stops there). A settlement-time writer passing
            # settle_pick(closing_odds=...) still wins over this value.
            # CLV needs to know a close was captured PRE-GAME to trust it --
            # stamp when, guarded so a DB without the column still works
            # (mirrors engine.tracker_core.core_capture_closing_odds).
            _has_ts = any(d[1] == "closing_odds_captured_at" for d in
                          conn.execute("PRAGMA table_info(player_props_picks)"))
            if _has_ts:
                conn.execute(
                    "UPDATE player_props_picks SET closing_odds = ?, "
                    "closing_odds_captured_at = ? "
                    "WHERE id = ? AND result IS NULL",
                    (int(odds), _ts_now(), row["id"]))
            else:
                conn.execute(
                    "UPDATE player_props_picks SET closing_odds = ? "
                    "WHERE id = ? AND result IS NULL",
                    (int(odds), row["id"]))
            conn.commit()
        return row["id"] if row else None
    except Exception as e:
        logger.warning("player_props.insert_pick failed for %s/%s %s: %s",
                       sport, player_name, bet_type, e)
        return None


def settle_pick(sport: str, pick_id: int, *,
                actual_value: float, result: str, profit: float,
                closing_odds: int | None = None) -> bool:
    """Mark a pick settled with its actual observed value + W/L/P + P/L.
    Idempotent: safe to call multiple times — last call wins."""
    ensure_tables(sport)
    conn = _conn_for(sport)
    try:
        _has_ts = any(d[1] == "closing_odds_captured_at" for d in
                      conn.execute("PRAGMA table_info(player_props_picks)"))
        if _has_ts:
            # A pick first seen at settlement time (never re-sighted while
            # still pending) never went through insert_pick's re-sight
            # stamp above, so it would otherwise carry a closing_odds
            # with no timestamp forever. Stamp it here too, but only when
            # the row is about to hold a non-null closing_odds (same
            # COALESCE the closing_odds column itself uses) and doesn't
            # already have one -- never overwrite an earlier, more
            # accurate pre-game stamp.
            conn.execute(
                "UPDATE player_props_picks SET "
                "  actual_value = ?, result = ?, profit = ?, "
                "  closing_odds = COALESCE(?, closing_odds), "
                "  closing_odds_captured_at = COALESCE(closing_odds_captured_at, "
                "    CASE WHEN COALESCE(?, closing_odds) IS NOT NULL THEN ? ELSE NULL END), "
                "  settled_at = datetime('now') "
                "WHERE id = ?",
                (actual_value, result, profit, closing_odds, closing_odds,
                 _ts_now(), int(pick_id)),
            )
        else:
            conn.execute(
                "UPDATE player_props_picks SET "
                "  actual_value = ?, result = ?, profit = ?, "
                "  closing_odds = COALESCE(?, closing_odds), "
                "  settled_at = datetime('now') "
                "WHERE id = ?",
                (actual_value, result, profit, closing_odds, int(pick_id)),
            )
        conn.commit()
        return True
    except Exception as e:
        logger.warning("player_props.settle_pick failed for %s pick=%s: %s",
                       sport, pick_id, e)
        return False


def list_picks(sport: str, *,
               date: str | None = None,
               pending_only: bool = False,
               limit: int = 200) -> list[dict]:
    """Return prop picks. Filter by date when given; otherwise the
    most recent ``limit`` rows. ``pending_only`` keeps the tracker
    pending list cheap to query."""
    ensure_tables(sport)
    conn = _conn_for(sport)
    where = []
    params: list[Any] = []
    if date:
        where.append("date = ?")
        params.append(date)
    if pending_only:
        where.append("result IS NULL")
    sql = "SELECT * FROM player_props_picks"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY date DESC, id DESC LIMIT ?"
    params.append(int(limit))
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def set_potd(sport: str, pick_id: int, *,
             date: str | None = None,
             selection_score: float | None = None,
             reasoning: str | None = None,
             cell: str | None = None,
             proven: bool | None = None,
             cell_record: str | None = None,
             stake_units: float | None = None) -> bool:
    """Set today's prop POTD. UNIQUE on date so re-running overwrites
    cleanly — the selector can re-rank as the slate develops without
    leaving stale POTD rows behind."""
    ensure_tables(sport)
    conn = _conn_for(sport)
    target = date or et_today_str()
    try:
        conn.execute(
            "INSERT INTO player_props_picks_pot_day "
            "(date, pick_id, selection_score, reasoning, "
            " cell, proven, cell_record, stake_units) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(date) DO UPDATE SET "
            "  pick_id = excluded.pick_id, "
            "  selected_at = datetime('now'), "
            "  selection_score = excluded.selection_score, "
            "  reasoning = excluded.reasoning, "
            "  cell = excluded.cell, proven = excluded.proven, "
            "  cell_record = excluded.cell_record, "
            "  stake_units = excluded.stake_units",
            (target, int(pick_id), selection_score, reasoning,
             cell, (None if proven is None else int(bool(proven))),
             cell_record, stake_units),
        )
        conn.commit()
        return True
    except Exception as e:
        logger.warning("player_props.set_potd failed for %s: %s", sport, e)
        return False


def get_potd(sport: str, date: str | None = None) -> dict | None:
    """Return today's prop POTD row joined with the underlying pick,
    or None when none has been selected yet."""
    ensure_tables(sport)
    conn = _conn_for(sport)
    target = date or et_today_str()
    row = conn.execute(
        "SELECT pot.date AS potd_date, pot.selected_at, pot.selection_score, "
        "       pot.reasoning, pot.cell, pot.proven, pot.cell_record, "
        "       pot.stake_units AS potd_stake_units, pot.id AS pot_id, p.* "
        "FROM player_props_picks_pot_day pot "
        "JOIN player_props_picks p ON p.id = pot.pick_id "
        "WHERE pot.date = ?",
        (target,),
    ).fetchone()
    return dict(row) if row else None


def prune_logs_before(sport: str, date: str) -> int:
    """Drop game-log rows older than ``date`` (exclusive). Keeps the
    historical training window bounded — multi-season backfill of
    NBA box scores would otherwise grow the table fast."""
    ensure_tables(sport)
    conn = _conn_for(sport)
    try:
        cur = conn.execute(
            "DELETE FROM player_game_logs WHERE date < ?", (date,),
        )
        conn.commit()
        return cur.rowcount or 0
    except Exception as e:
        logger.warning("player_props.prune_logs_before failed for %s: %s",
                       sport, e)
        return 0
