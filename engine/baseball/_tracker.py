"""Baseball pick tracker + settler — same shape as engine.football
._tracker, swapped "points" for "runs" in the resolution logic."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

from ._db import get_conn

logger = logging.getLogger(__name__)


def _payout(odds: int, won: bool) -> float:
    if not won:
        return -100.0
    if odds > 0:
        return float(odds)
    if odds < 0:
        return round(100.0 * 100.0 / abs(odds), 2)
    return 0.0


def record_picks(league: str, game_id: str, picks: Iterable[dict]) -> dict:
    """Baseball framework recorder. Enforces one-pick-per-game-per-family
    via the shared dedup helper. ML/SPREAD/TOTAL collapse to 'full'
    family — only one survives in pending state. Re-records after
    settlement are blocked by the settled-dup check. User flagged
    2026-05-26: college baseball CAM @ NE had ML + SPREAD both stored
    because the prior recorder iterated and inserted everything."""
    from .._dedup_helpers import (
        enforce_one_per_game_per_family, is_settled_dup,
    )
    conn = get_conn(league)
    # Pick.date must match games.date (league-local calendar) so the
    # slate query buckets correctly. utcnow stamps late-night ET picks
    # under the next UTC day (Copa pattern). Pull from the game row.
    g = conn.execute(
        "SELECT date FROM games WHERE game_id = ?", (str(game_id),)
    ).fetchone()
    pick_date = (g["date"] if g and g["date"]
                  else datetime.utcnow().strftime("%Y-%m-%d"))
    picks_list = list(picks or [])
    enforce_one_per_game_per_family(
        conn, table="picks", game_id=str(game_id), date=pick_date,
        picks=picks_list,
    )
    out = {"recorded": 0, "duplicate": 0, "errors": 0, "blocked_settled": 0}
    for p in picks_list:
        if is_settled_dup(conn, table="picks", game_id=str(game_id),
                            bet_type=p.get("type") or ""):
            out["blocked_settled"] += 1
            continue
        try:
            conn.execute(
                "INSERT INTO picks "
                "(date, game_id, matchup, bet_type, pick, "
                " model_prob, edge, odds, stake_units) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (pick_date, str(game_id), p.get("matchup") or "",
                  p.get("type"), p.get("pick"),
                  float(p.get("prob") or 0.0),
                  float(p.get("edge") or 0.0),
                  int(p.get("odds") or 0),
                  p.get("stake_units")),
            )
            out["recorded"] += 1
        except Exception as e:
            if "UNIQUE" in str(e).upper():
                out["duplicate"] += 1
            else:
                logger.warning("[baseball:%s] record pick failed: %s",
                                league, e)
                out["errors"] += 1
    conn.commit()

    # Phase-2 cutover: dual-write into picks_unified (canonical store).
    try:
        from ..picks_unified._legacy_bridge import mirror_to_unified
        for p in picks_list:
            mirror_to_unified(
                sport="baseball", league=league,
                native_game_id=game_id,
                pick_date=pick_date,
                matchup=p.get("matchup") or "",
                bet_type=p.get("type") or "",
                pick_text=p.get("pick") or "",
                odds=int(p.get("odds") or 0),
                prob=float(p.get("prob") or 0.0),
                edge_pct=float(p.get("edge") or 0.0),
                stake_units=float(p.get("stake_units") or 0.0),
            )
    except Exception as e:
        logger.debug("picks_unified mirror (baseball/%s) skipped: %s", league, e)

    return out


def _resolve(bet_type: str, pick: str, h_score: int, a_score: int,
              h_abbr: str, a_abbr: str, odds: int
              ) -> tuple[str | None, float | None]:
    bt = (bet_type or "").upper()
    margin_h = h_score - a_score
    total = h_score + a_score
    if bt == "ML":
        if pick == h_abbr:
            return ("W" if margin_h > 0 else "L" if margin_h < 0 else "P",
                     _payout(odds, margin_h > 0))
        if pick == a_abbr:
            return ("W" if margin_h < 0 else "L" if margin_h > 0 else "P",
                     _payout(odds, margin_h < 0))
        return None, None
    if bt == "SPREAD":
        parts = pick.split()
        if len(parts) < 2:
            return None, None
        team = parts[0]
        try:
            line = float(parts[1])
        except ValueError:
            return None, None
        if team == h_abbr:
            cover = margin_h + line
        elif team == a_abbr:
            cover = -margin_h + line
        else:
            return None, None
        if cover > 0:
            return "W", _payout(odds, True)
        if cover < 0:
            return "L", _payout(odds, False)
        return "P", 0.0
    if bt == "TOTAL":
        parts = pick.split()
        if len(parts) < 2:
            return None, None
        direction = parts[0].lower()
        try:
            line = float(parts[1])
        except ValueError:
            return None, None
        if direction == "over":
            if total > line:
                return "W", _payout(odds, True)
            if total < line:
                return "L", _payout(odds, False)
            return "P", 0.0
        if direction == "under":
            if total < line:
                return "W", _payout(odds, True)
            if total > line:
                return "L", _payout(odds, False)
            return "P", 0.0
    return None, None


def settle_picks(league: str) -> dict:
    conn = get_conn(league)
    # Self-heal: auto-push picks pending >7 days. Same threshold the
    # rest of the per-sport settlers use. Catches games whose status
    # never flipped to 'final' (postseason regional brackets sometimes
    # stay 'scheduled' for days when ESPN gets the result late).
    from datetime import datetime as _dt2, timedelta as _td2
    stale_cutoff = (_dt2.now() - _td2(days=7)).strftime("%Y-%m-%d")
    auto_pushed = 0
    for row in conn.execute(
        "SELECT id, date, bet_type, pick FROM picks "
        "WHERE result IS NULL AND date < ?", (stale_cutoff,),
    ).fetchall():
        conn.execute(
            "UPDATE picks SET result='P', profit=0, "
            "  settled_at=datetime('now') WHERE id=?", (row["id"],),
        )
        logger.warning(
            "[baseball:%s] settle: auto-pushed stale pending id=%s "
            "date=%s %s/%s — older than 7 days",
            league, row["id"], row["date"], row["bet_type"], row["pick"],
        )
        auto_pushed += 1
    if auto_pushed:
        conn.commit()
    pending = conn.execute(
        "SELECT p.id, p.game_id, p.bet_type, p.pick, p.odds, "
        "       g.home_score, g.away_score, g.status, "
        "       ht.abbreviation AS h_abbr, at.abbreviation AS a_abbr "
        "FROM picks p "
        "LEFT JOIN games g ON g.game_id = p.game_id "
        "LEFT JOIN teams ht ON ht.id = g.home_team_id "
        "LEFT JOIN teams at ON at.id = g.away_team_id "
        "WHERE p.result IS NULL"
    ).fetchall()
    out = {"checked": len(pending), "settled": 0,
            "wins": 0, "losses": 0, "pushes": 0}
    for r in pending:
        if (r["status"] or "").lower() != "final":
            continue
        if r["home_score"] is None or r["away_score"] is None:
            continue
        verdict, profit = _resolve(
            r["bet_type"], r["pick"],
            int(r["home_score"]), int(r["away_score"]),
            r["h_abbr"] or "", r["a_abbr"] or "",
            int(r["odds"] or 0),
        )
        if verdict is None:
            continue
        conn.execute(
            "UPDATE picks SET result = ?, profit = ?, "
            "  settled_at = ? WHERE id = ?",
            (verdict, profit, datetime.utcnow().isoformat(), r["id"]),
        )
        out["settled"] += 1
        if verdict == "W":
            out["wins"] += 1
        elif verdict == "L":
            out["losses"] += 1
        else:
            out["pushes"] += 1
    conn.commit()
    if out["settled"]:
        logger.info("[baseball:%s] settled %d (W=%d L=%d P=%d)",
                    league, out["settled"], out["wins"], out["losses"],
                    out["pushes"])
    return out


def list_history(league: str, limit: int = 200) -> list[dict]:
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT p.id, p.date, p.game_id, p.bet_type, p.pick, "
        "       p.model_prob, p.edge, p.odds, p.result, p.profit, "
        "       p.stake_units, "
        "       p.closing_odds, p.created_at, p.settled_at, p.matchup, "
        "       g.home_score, g.away_score, g.status, "
        "       ht.abbreviation AS h_abbr, at.abbreviation AS a_abbr "
        "FROM picks p "
        "LEFT JOIN games g ON g.game_id = p.game_id "
        "LEFT JOIN teams ht ON ht.id = g.home_team_id "
        "LEFT JOIN teams at ON at.id = g.away_team_id "
        "ORDER BY p.created_at DESC LIMIT ?",
        (int(limit),),
    ).fetchall()
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        # Legacy picks were recorded with an empty matchup (picks_core
        # dropped the field). Compose from the team JOIN as a fallback
        # so the tracker always shows the matchup, even on rows that
        # pre-date the fix.
        if not d.get("matchup") and d.get("h_abbr") and d.get("a_abbr"):
            d["matchup"] = f"{d['a_abbr']} @ {d['h_abbr']}"
        out.append(d)
    return out
