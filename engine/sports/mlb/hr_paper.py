# -*- coding: utf-8 -*-
"""Paper ledger for the matchup-adjusted HR model (engine.sports.mlb.hr_matchup).

Records the +edge 1+HR picks each day and grades them vs actual HRs. PAPER only
-- never places. This is the validation trail: does our P(HR) match reality, and
do the +edge picks beat the market's price? Real money is gated on this proving
out (calibration + beat-the-close), same discipline as everything else.
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone

_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))), "data", "mlb.db")
MIN_EDGE = 3.0   # record 1+HR picks with >= this model edge (paper)


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB)
    c.row_factory = sqlite3.Row
    return c


def _ensure(conn) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS hr_matchup_paper ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " date TEXT, player_name TEXT, mlb_id INTEGER, odds INTEGER,"
        " our_p REAL, market_p REAL, edge REAL,"
        " result TEXT, hit_hr INTEGER, profit REAL,"
        " created_at TEXT, settled_at TEXT,"
        " UNIQUE(date, mlb_id))")
    conn.commit()


def record_today(date: str | None = None, min_edge: float = MIN_EDGE) -> dict:
    """Price today's 1+HR props and record the +edge ones to the paper ledger."""
    from .hr_matchup import price_todays_props
    conn = _conn()
    _ensure(conn)
    d = date or datetime.now().strftime("%Y-%m-%d")
    priced = price_todays_props(conn, d, min_edge=min_edge)
    now = datetime.now(timezone.utc).isoformat()
    ins = 0
    for r in priced:
        before = conn.total_changes
        conn.execute(
            "INSERT OR IGNORE INTO hr_matchup_paper "
            "(date,player_name,mlb_id,odds,our_p,market_p,edge,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (d, r["player_name"], r["mlb_id"], int(r["odds"]),
             r["our_p"], r["market_p"], r["edge"], now))
        ins += (conn.total_changes - before)
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM hr_matchup_paper WHERE date=?",
                     (d,)).fetchone()[0]
    conn.close()
    return {"date": d, "edge_picks": len(priced), "recorded": n, "new": ins}


def _hit_hr(conn, mlb_id, date) -> "int | None":
    """1 if the batter homered that date, 0 if played-no-HR, None if unknown."""
    r = conn.execute(
        "SELECT stats_json FROM player_game_logs WHERE player_id=? AND date=?",
        (mlb_id, date)).fetchone()
    if not r or not r["stats_json"]:
        return None
    try:
        s = json.loads(r["stats_json"])
    except Exception:
        return None
    for k in ("homeRuns", "home_runs", "hr", "HR", "homeruns", "homers"):
        if k in s:
            try:
                return 1 if int(s[k]) >= 1 else 0
            except (TypeError, ValueError):
                pass
    return None


def _profit(odds: int, won: bool) -> float:
    """$100-notional paper P/L: a win pays the American odds, a loss is -100."""
    if won:
        return float(odds) if odds > 0 else 10000.0 / abs(odds)
    return -100.0


def grade_pending() -> dict:
    """Grade any ungraded picks whose game HR count is now known."""
    conn = _conn()
    _ensure(conn)
    pend = conn.execute(
        "SELECT id, mlb_id, date, odds FROM hr_matchup_paper "
        "WHERE result IS NULL").fetchall()
    now = datetime.now(timezone.utc).isoformat()
    graded = wins = 0
    for p in pend:
        hit = _hit_hr(conn, p["mlb_id"], p["date"])
        if hit is None:
            continue
        won = hit == 1
        conn.execute(
            "UPDATE hr_matchup_paper SET result=?, hit_hr=?, profit=?, "
            "settled_at=? WHERE id=?",
            ("W" if won else "L", hit, _profit(int(p["odds"]), won),
             now, p["id"]))
        graded += 1
        wins += int(won)
    conn.commit()
    conn.close()
    return {"graded": graded, "wins": wins}


def summary() -> dict:
    """Running paper record."""
    conn = _conn()
    _ensure(conn)
    row = conn.execute(
        "SELECT COUNT(*) n, "
        " SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) w, "
        " SUM(CASE WHEN result IN ('W','L') THEN 1 ELSE 0 END) decided, "
        " SUM(COALESCE(profit,0)) profit, "
        " SUM(CASE WHEN result IN ('W','L') THEN 100 ELSE 0 END) staked "
        "FROM hr_matchup_paper").fetchone()
    conn.close()
    d = dict(row)
    d["roi_pct"] = round(d["profit"] / d["staked"] * 100.0, 1) if d["staked"] else None
    return d


if __name__ == "__main__":
    # Daily driver: grade yesterday's picks, then record today's + print record.
    print("grade_pending:", grade_pending())
    print("record_today: ", record_today())
    print("summary:      ", summary())
