"""
Derivative pick tracker — paper-bet log for Phase 1 derivative markets.

Separate from the main picks/nhl_picks/nba_picks tracker so derivative
performance can be evaluated in isolation. Derivatives carry low
reliability weights (0.40-0.50) and rarely make best_pick on the main
tracker; without their own log we'd have ~zero history to know if
they're worth keeping enabled.

Schema mirrors the main picks table per sport (same columns, separate
table). Settlement reuses the per-sport settler functions in
``engine.tracker``, ``engine.nhl_tracker``, ``engine.nba_tracker`` —
those already understand every derivative bet type from Phase 1e.

Usage:
    record_top_derivatives("mlb", n_per_game=3)   # logs at sync time
    settle_derivative_picks("mlb")                # settles after games
    get_summary("mlb")                             # by-bet-type stats
    get_history("mlb", limit=200)                 # recent rows
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


_DERIV_TABLE = {
    "mlb": "derivative_picks",
    "nhl": "nhl_derivative_picks",
    "nba": "nba_derivative_picks",
}

_MAIN_TABLE = {
    "mlb": "picks",
    "nhl": "nhl_picks",
    "nba": "nba_picks",
}


def _conn(sport: str):
    if sport == "mlb":
        from .db import get_conn
    elif sport == "nhl":
        from .nhl_db import get_conn
    elif sport == "nba":
        from .nba_db import get_conn
    else:
        raise ValueError(f"unknown sport: {sport}")
    return get_conn()


def _ensure_table(sport: str) -> None:
    """Create the per-sport derivative_picks table if missing.

    Mirrors the relevant columns from the main picks table for that
    sport so the existing settlers can be re-used by table-name swap.
    """
    conn = _conn(sport)
    table = _DERIV_TABLE[sport]
    if sport == "mlb":
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id     TEXT NOT NULL,
                date        TEXT NOT NULL,
                matchup     TEXT NOT NULL,
                bet_type    TEXT NOT NULL,
                pick        TEXT NOT NULL,
                model_prob  REAL,
                edge        REAL,
                odds        INTEGER,
                result      TEXT,
                profit      REAL,
                created_at  TEXT DEFAULT (datetime('now')),
                settled_at  TEXT,
                closing_odds INTEGER,
                UNIQUE(game_id, date, bet_type, pick)
            )
        """)
    else:
        # NHL/NBA tracker schemas don't have game_id as TEXT NOT NULL
        # in the same way; keep the column set close to their main
        # tables for settler compatibility.
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id     TEXT NOT NULL,
                date        TEXT NOT NULL,
                matchup     TEXT NOT NULL,
                bet_type    TEXT NOT NULL,
                pick        TEXT NOT NULL,
                model_prob  REAL,
                edge        REAL,
                odds        INTEGER,
                result      TEXT,
                profit      REAL,
                created_at  TEXT DEFAULT (datetime('now')),
                settled_at  TEXT,
                closing_odds INTEGER,
                UNIQUE(game_id, date, bet_type, pick)
            )
        """)
    conn.commit()


def record_top_derivatives(sport: str, bets: list[dict],
                            n_per_game: int = 3,
                            min_edge: float = 4.0,
                            target_date: str | None = None) -> dict:
    """Log the top N derivative picks per game from today's best-bets
    payload. Idempotent via UNIQUE(game_id, date, bet_type, pick) — same
    pick recorded again silently does nothing. Updates prob/edge/odds on
    re-record so the row stays current as the line drifts (mirrors the
    main tracker's auto-refresh pattern from Phase 1f).

    Returns ``{"inserted": N, "updated": N, "skipped": N}``.
    """
    _ensure_table(sport)
    conn = _conn(sport)
    table = _DERIV_TABLE[sport]
    target_date = target_date or datetime.now().strftime("%Y-%m-%d")

    from .config import DERIVATIVE_BLOCKED_MARKETS, DERIVATIVE_EDGE_FLOOR
    blocked = DERIVATIVE_BLOCKED_MARKETS.get(sport, frozenset())
    floor_overrides = DERIVATIVE_EDGE_FLOOR.get(sport, {})

    inserted = updated = skipped = 0
    for bet in bets:
        derivs = bet.get("derivative_picks") or []
        if not derivs:
            continue
        game_id = str(bet.get("game_id") or "")
        matchup = bet.get("matchup", "")
        for p in derivs[:n_per_game]:
            bt = p.get("type")
            if bt in blocked:
                skipped += 1
                continue
            edge = p.get("edge") or 0
            effective_floor = floor_overrides.get(bt, min_edge)
            if edge < effective_floor:
                skipped += 1
                continue
            existing = conn.execute(
                f"SELECT id, result FROM {table} "
                f"WHERE game_id = ? AND date = ? AND bet_type = ? AND pick = ?",
                (game_id, target_date, p.get("type"), p.get("pick")),
            ).fetchone()
            if existing:
                if existing["result"] is None:
                    # Refresh prob/edge/odds for unsettled rows so the
                    # record stays current as HR moves the line.
                    conn.execute(
                        f"UPDATE {table} SET model_prob = ?, edge = ?, "
                        f"odds = ? WHERE id = ?",
                        (p.get("prob"), p.get("edge"), p.get("odds"),
                         existing["id"]),
                    )
                    updated += 1
                else:
                    skipped += 1
                continue
            try:
                conn.execute(
                    f"INSERT INTO {table} (game_id, date, matchup, "
                    f"bet_type, pick, model_prob, edge, odds) "
                    f"VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (game_id, target_date, matchup,
                     p.get("type"), p.get("pick"),
                     p.get("prob"), p.get("edge"), p.get("odds")),
                )
                inserted += 1
            except Exception as e:
                logger.warning("Could not insert derivative pick: %s", e)
    conn.commit()
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def settle_derivative_picks(sport: str) -> dict:
    """Settle pending derivative picks. Re-uses the per-sport settler
    by temporarily aliasing the derivative_picks table to picks (via a
    direct settler call with the right table)."""
    # Easiest path: copy main settler logic but read/write to the
    # derivative table. Both settlers already handle every Phase 1
    # bet type. We monkey-patch at the lowest level by just running
    # the settle SQL inline using the same pick-resolution rules.
    # For now, pragmatic approach: re-import settle_picks but tell it
    # to operate on the derivative table via a context shim.
    if sport == "mlb":
        return _settle_mlb_derivatives()
    elif sport == "nhl":
        return _settle_nhl_derivatives()
    elif sport == "nba":
        return _settle_nba_derivatives()
    raise ValueError(f"unknown sport: {sport}")


def _resolve_mlb_game_pk(conn, espn_or_pk: str, date: str, matchup: str) -> int | None:
    """Translate the recorded derivative game_id to the MLB Stats API
    game_pk that the main MLB settler joins against. The best-bets payload
    stores ESPN event IDs (9-digit, e.g. '401815077') because the public
    scoreboard is ESPN-sourced; the `games` table uses MLB Stats game_pk
    (6-digit, e.g. 824932). Without this translation the trampoline's
    settle_picks() can never match a derivative pick to its row in
    `games`, leaving every MLB derivative pending forever.

    Lookup is by date + (away_abbr, home_abbr) parsed from the matchup
    label, which is unique-per-day for MLB.
    """
    if not matchup or " @ " not in matchup:
        return None
    a_abbr, h_abbr = matchup.split(" @ ", 1)
    # games.home_team_id stores teams.mlb_id (the MLB Stats API id), not
    # teams.id. Joining on the wrong column silently returns nothing.
    # Accept the canonical abbreviation OR any registered alias (CHW/CWS,
    # WSH/WAS, ARI/AZ, etc.) so a matchup string in either dialect still
    # resolves — without this, "WSH @ CHW" picks stayed pending because
    # our teams table has Chicago White Sox under "CWS".
    from .abbr import aliases_for as _aliases
    h_aliases = _aliases(h_abbr.strip(), sport="mlb") or [h_abbr.strip()]
    a_aliases = _aliases(a_abbr.strip(), sport="mlb") or [a_abbr.strip()]
    h_placeholders = ",".join("?" * len(h_aliases))
    a_placeholders = ",".join("?" * len(a_aliases))
    row = conn.execute(
        f"SELECT g.mlb_game_id FROM games g "
        f"JOIN teams ht ON ht.mlb_id = g.home_team_id AND ht.abbreviation IN ({h_placeholders}) "
        f"JOIN teams at ON at.mlb_id = g.away_team_id AND at.abbreviation IN ({a_placeholders}) "
        f"WHERE g.date = ? LIMIT 1",
        (*h_aliases, *a_aliases, date),
    ).fetchone()
    return row["mlb_game_id"] if row else None


def _settle_mlb_derivatives() -> dict:
    """Run MLB settle_picks logic against the derivative_picks table."""
    from . import tracker as _tk
    from .db import get_conn
    conn = get_conn()
    pending = conn.execute(
        "SELECT * FROM derivative_picks WHERE result IS NULL"
    ).fetchall()
    if not pending:
        return {"settled": 0, "wins": 0, "losses": 0, "pushes": 0}
    settled = wins = losses = pushes = 0
    for p in pending:
        p = dict(p)
        from .tracker import settle_picks
        # Translate ESPN event ID -> MLB Stats game_pk so the main settler
        # can match this temp row against the games table. Without this,
        # every MLB derivative stays PEND.
        resolved_pk = _resolve_mlb_game_pk(conn, p["game_id"], p["date"],
                                           p["matchup"])
        if resolved_pk is None:
            # Game not in DB yet (e.g. scoreboard hasn't synced). Leave
            # the row pending and try again next settler tick.
            continue
        cur = conn.execute(
            "INSERT INTO picks (game_id, date, matchup, bet_type, pick, "
            "model_prob, edge, odds) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (resolved_pk, p["date"], p["matchup"],
             p["bet_type"], p["pick"],
             p["model_prob"], p["edge"], p["odds"]),
        )
        temp_id = cur.lastrowid
        conn.commit()
        try:
            settle_picks()
            row = conn.execute(
                "SELECT result, profit FROM picks WHERE id = ?", (temp_id,),
            ).fetchone()
            if row and row["result"] is not None:
                conn.execute(
                    "UPDATE derivative_picks SET result = ?, profit = ?, "
                    "settled_at = datetime('now') WHERE id = ?",
                    (row["result"], row["profit"], p["id"]),
                )
                if row["result"] == "W":
                    wins += 1
                elif row["result"] == "L":
                    losses += 1
                else:
                    pushes += 1
                settled += 1
        finally:
            conn.execute("DELETE FROM picks WHERE id = ?", (temp_id,))
            conn.commit()
    return {"settled": settled, "wins": wins, "losses": losses,
            "pushes": pushes}


def _settle_nhl_derivatives() -> dict:
    """NHL twin of _settle_mlb_derivatives. Same trampoline pattern."""
    from .nhl_db import get_conn
    from .nhl_tracker import settle_picks
    conn = get_conn()
    pending = conn.execute(
        "SELECT * FROM nhl_derivative_picks WHERE result IS NULL"
    ).fetchall()
    if not pending:
        return {"settled": 0, "wins": 0, "losses": 0, "pushes": 0}
    settled = wins = losses = pushes = 0
    for p in pending:
        p = dict(p)
        cur = conn.execute(
            "INSERT INTO nhl_picks (game_id, date, matchup, bet_type, pick, "
            "model_prob, edge, odds) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (p["game_id"], p["date"], p["matchup"],
             p["bet_type"], p["pick"],
             p["model_prob"], p["edge"], p["odds"]),
        )
        temp_id = cur.lastrowid
        conn.commit()
        try:
            settle_picks()
            row = conn.execute(
                "SELECT result, profit FROM nhl_picks WHERE id = ?", (temp_id,),
            ).fetchone()
            if row and row["result"] is not None:
                conn.execute(
                    "UPDATE nhl_derivative_picks SET result = ?, profit = ?, "
                    "settled_at = datetime('now') WHERE id = ?",
                    (row["result"], row["profit"], p["id"]),
                )
                if row["result"] == "W":
                    wins += 1
                elif row["result"] == "L":
                    losses += 1
                else:
                    pushes += 1
                settled += 1
        finally:
            conn.execute("DELETE FROM nhl_picks WHERE id = ?", (temp_id,))
            conn.commit()
    return {"settled": settled, "wins": wins, "losses": losses,
            "pushes": pushes}


def _settle_nba_derivatives() -> dict:
    """NBA twin of _settle_mlb_derivatives. Same trampoline pattern."""
    from .nba_db import get_conn
    from .nba_tracker import settle_picks
    conn = get_conn()
    pending = conn.execute(
        "SELECT * FROM nba_derivative_picks WHERE result IS NULL"
    ).fetchall()
    if not pending:
        return {"settled": 0, "wins": 0, "losses": 0, "pushes": 0}
    settled = wins = losses = pushes = 0
    for p in pending:
        p = dict(p)
        cur = conn.execute(
            "INSERT INTO nba_picks (game_id, date, matchup, bet_type, pick, "
            "model_prob, edge, odds) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (p["game_id"], p["date"], p["matchup"],
             p["bet_type"], p["pick"],
             p["model_prob"], p["edge"], p["odds"]),
        )
        temp_id = cur.lastrowid
        conn.commit()
        try:
            settle_picks()
            row = conn.execute(
                "SELECT result, profit FROM nba_picks WHERE id = ?", (temp_id,),
            ).fetchone()
            if row and row["result"] is not None:
                conn.execute(
                    "UPDATE nba_derivative_picks SET result = ?, profit = ?, "
                    "settled_at = datetime('now') WHERE id = ?",
                    (row["result"], row["profit"], p["id"]),
                )
                if row["result"] == "W":
                    wins += 1
                elif row["result"] == "L":
                    losses += 1
                else:
                    pushes += 1
                settled += 1
        finally:
            conn.execute("DELETE FROM nba_picks WHERE id = ?", (temp_id,))
            conn.commit()
    return {"settled": settled, "wins": wins, "losses": losses,
            "pushes": pushes}


def get_summary(sport: str) -> dict:
    """Per-bet-type W/L/P totals + ROI for the dedicated derivative
    tracker UI. Same shape as the main pick-tracker summary."""
    _ensure_table(sport)
    conn = _conn(sport)
    table = _DERIV_TABLE[sport]
    rows = conn.execute(f"""
        SELECT bet_type,
               COUNT(*) as total,
               SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
               SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
               SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
               COALESCE(SUM(profit), 0) as profit
        FROM {table}
        GROUP BY bet_type
    """).fetchall()

    summary: dict[str, dict] = {}
    grand = {"total": 0, "wins": 0, "losses": 0, "pushes": 0,
             "pending": 0, "profit": 0.0}
    for r in rows:
        d = dict(r)
        settled_n = (d["wins"] or 0) + (d["losses"] or 0)
        d["win_pct"] = round((d["wins"] / settled_n) * 100, 1) if settled_n else 0
        d["roi"] = round((d["profit"] / settled_n), 1) if settled_n else 0
        summary[d["bet_type"]] = d
        for k in ("total", "wins", "losses", "pushes", "pending"):
            grand[k] += d.get(k) or 0
        grand["profit"] += d.get("profit") or 0
    settled_n = grand["wins"] + grand["losses"]
    grand["win_pct"] = round((grand["wins"] / settled_n) * 100, 1) if settled_n else 0
    grand["roi"] = round((grand["profit"] / settled_n), 1) if settled_n else 0
    summary["_grand"] = grand
    return summary


def get_history(sport: str, limit: int = 200) -> list[dict]:
    """Recent derivative picks (newest first), for the UI table."""
    _ensure_table(sport)
    conn = _conn(sport)
    table = _DERIV_TABLE[sport]
    rows = conn.execute(f"""
        SELECT * FROM {table}
        ORDER BY COALESCE(settled_at, created_at) DESC
        LIMIT ?
    """, (limit,)).fetchall()
    return [dict(r) for r in rows]


# ── Derivative Pick of the Day (per sport) ──────────────────

def _ensure_potd_table(sport: str) -> None:
    """Create the derivative POTD table if missing. Mirrors the main
    pick_of_day schema so the UI can render it the same way."""
    conn = _conn(sport)
    table = f"{_DERIV_TABLE[sport]}_pot_day"
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL UNIQUE,
            game_id     TEXT,
            matchup     TEXT NOT NULL,
            bet_type    TEXT NOT NULL,
            pick        TEXT NOT NULL,
            model_prob  REAL,
            edge        REAL,
            odds        INTEGER,
            kelly_pct   REAL,
            reasoning   TEXT,
            result      TEXT,
            profit      REAL,
            created_at  TEXT DEFAULT (datetime('now')),
            settled_at  TEXT
        )
    """)
    conn.commit()


def _kelly_fraction(prob: float, odds: int) -> float:
    """Standard Kelly fraction for a given prob + American odds."""
    if not odds or prob <= 0 or prob >= 1:
        return 0.0
    if odds > 0:
        b = odds / 100.0
    else:
        b = 100.0 / abs(odds)
    q = 1 - prob
    f = (b * prob - q) / b
    return max(0.0, f)


def select_derivative_potd(sport: str, bets: list[dict],
                            min_edge: float = 4.0) -> dict | None:
    """Pick the best derivative across the day's slate. Considers
    every game's `derivative_picks` (the live model output) and ranks
    by edge × per-market reliability so a 12% Period DNB doesn't
    automatically beat a 10% F5 Team Total just because its raw edge
    is higher.
    """
    from .config import DERIVATIVE_BLOCKED_MARKETS, DERIVATIVE_EDGE_FLOOR
    from .dynamic_reliability import get_reliability as _get_reliability
    blocked = DERIVATIVE_BLOCKED_MARKETS.get(sport, frozenset())
    floor_overrides = DERIVATIVE_EDGE_FLOOR.get(sport, {})

    candidates: list[dict] = []
    for game in bets or []:
        if game.get("is_locked"):
            continue
        for p in (game.get("derivative_picks") or []):
            bt = p.get("type")
            if bt in blocked:
                continue
            edge = p.get("edge") or 0
            effective_floor = floor_overrides.get(bt, min_edge)
            if edge < effective_floor:
                continue
            if not p.get("odds"):
                continue
            # Auto-tuned reliability — see engine.dynamic_reliability.
            # The edge value is already calibration-shrunk via
            # empirical_calibration.calibrate() upstream in *_picks.py.
            reliability = _get_reliability(sport, bt)
            score = edge * reliability
            candidates.append({
                "game_id": str(game.get("game_id", "")),
                "matchup": game.get("matchup", ""),
                "type": p.get("type"),
                "pick": p.get("pick"),
                "prob": p.get("prob"),
                "edge": edge,
                "odds": p.get("odds"),
                "score": score,
                "time": game.get("time", ""),
                "venue": game.get("venue", ""),
            })
    if not candidates:
        return None
    candidates.sort(key=lambda c: -c["score"])
    best = candidates[0]
    # Normalize key names so the locked POTD row + API response use
    # the same `bet_type` field the frontend / main pick_of_day use.
    best["bet_type"] = best.pop("type", None)
    best["model_prob"] = best.pop("prob", None)
    best["kelly_pct"] = round(
        _kelly_fraction(best.get("model_prob") or 0, best["odds"]) * 100, 1)
    return best


def get_or_create_derivative_potd(sport: str, bets: list[dict] | None = None) -> dict | None:
    """Return today's locked derivative POTD or compute + lock one
    from the supplied bets list."""
    _ensure_potd_table(sport)
    conn = _conn(sport)
    table = f"{_DERIV_TABLE[sport]}_pot_day"
    today = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(f"SELECT * FROM {table} WHERE date = ?", (today,)).fetchone()
    if row:
        return dict(row)
    if not bets:
        return None
    sel = select_derivative_potd(sport, bets)
    if not sel:
        return None
    conn.execute(
        f"INSERT OR IGNORE INTO {table} "
        "(date, game_id, matchup, bet_type, pick, model_prob, edge, odds, kelly_pct) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (today, sel["game_id"], sel["matchup"], sel["bet_type"], sel["pick"],
         sel["model_prob"], sel["edge"], sel["odds"], sel.get("kelly_pct", 0)),
    )
    conn.commit()
    out = dict(sel)
    out["date"] = today
    return out


def get_today_derivative_potd(sport: str) -> dict | None:
    """Read-only fetch of today's derivative POTD (None if not yet selected)."""
    _ensure_potd_table(sport)
    conn = _conn(sport)
    table = f"{_DERIV_TABLE[sport]}_pot_day"
    today = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(f"SELECT * FROM {table} WHERE date = ?", (today,)).fetchone()
    return dict(row) if row else None
