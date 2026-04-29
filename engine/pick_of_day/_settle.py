"""POTD settlement.

Two entry points:
  - settle_potd(sport)        — sweep pending POTDs, mark W/L/P
  - recalc_potd_profit(sport) — rewrite stored profit using current
                                $100-unit formula (idempotent)

Outcome resolution lives in `_determine_outcome` — the largest function
in the package. Per-bet-type handlers for ML / O/U / RL / PL / 1st INN
/ Q1_ML / Q1_SPREAD / Q1_TOTAL. Game lookup is two-step: try by
`game_id`, then fall back to date + team-name substring (the ESPN id
stored on POTDs doesn't always match the games table primary key).
"""

from __future__ import annotations
import json as _json
import logging
import re

from ._storage import _get_conn, _ensure_potd_table

logger = logging.getLogger(__name__)


def settle_potd(sport: str) -> dict:
    """Settle any pending POTDs whose games have completed."""
    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    pending = conn.execute(
        "SELECT * FROM pick_of_day WHERE result IS NULL"
    ).fetchall()

    if not pending:
        return {"settled": 0}

    settled = 0
    wins = 0
    losses = 0

    for potd in pending:
        potd = dict(potd)
        result, profit = _determine_outcome(sport, conn, potd)

        if result is None:
            continue  # Game not finished yet

        conn.execute("""
            UPDATE pick_of_day SET result = ?, profit = ?, settled_at = datetime('now')
            WHERE id = ?
        """, (result, profit, potd["id"]))
        settled += 1
        if result == "W":
            wins += 1
        elif result == "L":
            losses += 1

    conn.commit()
    return {"settled": settled, "wins": wins, "losses": losses}


def _determine_outcome(sport: str, conn, potd: dict) -> tuple[str | None, float]:
    """Figure out whether a POTD won, lost, or pushed. Returns (result, profit).
    Returns (None, 0) if the game isn't finished yet."""
    date = potd["date"]
    matchup = potd["matchup"]
    bet_type = potd["bet_type"]
    pick = potd["pick"]
    odds = potd.get("odds") or -110

    # Parse matchup - handles both "AWAY @ HOME" and "Away Team at Home Team"
    try:
        if " @ " in matchup:
            away_part, home_part = [s.strip() for s in matchup.split(" @ ")]
        elif " at " in matchup:
            away_part, home_part = [s.strip() for s in matchup.split(" at ")]
        else:
            return None, 0
    except ValueError:
        return None, 0

    game_id = potd.get("game_id")

    if sport == "mlb":
        row = None
        if game_id:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM games g
                LEFT JOIN teams ht ON g.home_team_id = ht.mlb_id
                LEFT JOIN teams at ON g.away_team_id = at.mlb_id
                WHERE g.mlb_game_id = ? AND g.status = 'final'
                LIMIT 1
            """, (game_id,)).fetchone()
        if not row:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM games g
                LEFT JOIN teams ht ON g.home_team_id = ht.mlb_id
                LEFT JOIN teams at ON g.away_team_id = at.mlb_id
                WHERE g.date = ? AND g.status = 'final'
                  AND (ht.name LIKE ? OR ht.abbreviation = ?)
                  AND (at.name LIKE ? OR at.abbreviation = ?)
                LIMIT 1
            """, (date, f"%{home_part}%", home_part, f"%{away_part}%", away_part)).fetchone()
    elif sport == "nhl":
        row = None
        if game_id:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM nhl_games g
                LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nhl_teams at ON g.away_team_id = at.id
                WHERE g.game_id = ? AND g.status = 'final'
                LIMIT 1
            """, (game_id,)).fetchone()
        if not row:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM nhl_games g
                LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nhl_teams at ON g.away_team_id = at.id
                WHERE g.date = ? AND g.status = 'final'
                  AND (ht.name LIKE ? OR ht.abbreviation = ?)
                  AND (at.name LIKE ? OR at.abbreviation = ?)
                LIMIT 1
            """, (date, f"%{home_part}%", home_part, f"%{away_part}%", away_part)).fetchone()
    elif sport == "nba":
        row = None
        if game_id:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                FROM nba_games g
                LEFT JOIN nba_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nba_teams at ON g.away_team_id = at.id
                WHERE g.game_id = ? AND g.status = 'final'
                LIMIT 1
            """, (game_id,)).fetchone()
        if not row:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                FROM nba_games g
                LEFT JOIN nba_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nba_teams at ON g.away_team_id = at.id
                WHERE g.date = ? AND g.status = 'final'
                  AND (ht.name LIKE ? OR ht.abbreviation = ?)
                  AND (at.name LIKE ? OR at.abbreviation = ?)
                LIMIT 1
            """, (date, f"%{home_part}%", home_part, f"%{away_part}%", away_part)).fetchone()
    else:
        return None, 0

    if not row:
        # Differentiate "game not finished yet" from "we can't find
        # this game at all" — the latter usually means a date/team
        # -name mismatch between the POTD row and the games table.
        logger.debug(
            "POTD settle: no final game row for %s %s / %s (game_id=%s, "
            "home_part=%r, away_part=%r)",
            sport, date, matchup, game_id, home_part, away_part,
        )
        return None, 0

    row = dict(row)
    hs = row.get("home_score", 0) or 0
    as_ = row.get("away_score", 0) or 0

    result = None
    home_abbr = row.get("home_abbr", "")
    away_abbr = row.get("away_abbr", "")

    if bet_type == "ML":
        home_won = hs > as_
        pick_home = (pick == home_abbr
                     or home_part in pick
                     or (home_abbr and home_abbr in pick))
        won = (pick_home and home_won) or (not pick_home and not home_won)
        result = "W" if won else "L"

    elif bet_type in ("O/U", "ALT O/U"):
        # ALT O/U is identical to O/U for settlement — same "Over N.N" /
        # "Under N.N" pick label, just at a different line value.
        total = hs + as_
        if "Over" in pick:
            line = float(pick.split()[-1])
            if total > line:
                result = "W"
            elif total < line:
                result = "L"
            else:
                result = "P"
        else:
            line = float(pick.split()[-1])
            if total < line:
                result = "W"
            elif total > line:
                result = "L"
            else:
                result = "P"

    elif bet_type in ("RL", "PL", "ALT RL", "ALT PL"):
        spread_match = re.search(r'([+-]?\d+\.?\d*)\s*$', pick)
        spread = float(spread_match.group(1)) if spread_match else 1.5

        pick_is_home = (home_abbr and home_abbr in pick) or (home_part and home_part in pick)
        if pick_is_home:
            margin = hs - as_
        else:
            margin = as_ - hs

        covered = margin + spread > 0
        pushed = margin + spread == 0
        if pushed:
            result = "P"
        else:
            result = "W" if covered else "L"

    elif bet_type == "1st INN":
        if sport == "mlb":
            home_ls = row.get("home_linescore")
            away_ls = row.get("away_linescore")
            try:
                h_inn = _json.loads(home_ls) if home_ls else []
                a_inn = _json.loads(away_ls) if away_ls else []
                scoreless = (len(h_inn) > 0 and len(a_inn) > 0
                             and h_inn[0] == 0 and a_inn[0] == 0)
            except (_json.JSONDecodeError, TypeError):
                scoreless = False
            if pick == "NRFI":
                result = "W" if scoreless else "L"
            else:
                result = "W" if not scoreless else "L"

    elif bet_type == "Q1_ML":
        hq1 = row.get("home_q1", 0) or 0
        aq1 = row.get("away_q1", 0) or 0
        pick_home = ((home_abbr and home_abbr in pick)
                     or (home_part and home_part in pick))
        home_won_q1 = hq1 > aq1
        if hq1 == aq1:
            result = "P"
        else:
            won = (pick_home and home_won_q1) or (not pick_home and not home_won_q1)
            result = "W" if won else "L"

    elif bet_type == "Q1_SPREAD":
        hq1 = row.get("home_q1", 0) or 0
        aq1 = row.get("away_q1", 0) or 0
        m = re.search(r"([+-]\d+\.?\d*)", pick)
        spread = float(m.group(1)) if m else 0.0
        pick_home = ((home_abbr and home_abbr in pick)
                     or (home_part and home_part in pick))
        if pick_home:
            margin = hq1 - aq1
        else:
            margin = aq1 - hq1
        covered = margin + spread > 0
        pushed = margin + spread == 0
        if pushed:
            result = "P"
        else:
            result = "W" if covered else "L"

    elif bet_type == "Q1_TOTAL":
        hq1 = row.get("home_q1", 0) or 0
        aq1 = row.get("away_q1", 0) or 0
        q1_total = hq1 + aq1
        m = re.search(r"(\d+\.?\d*)", pick)
        line = float(m.group(1)) if m else 0.0
        is_over = "Over" in pick or "over" in pick
        if q1_total == line:
            result = "P"
        elif is_over:
            result = "W" if q1_total > line else "L"
        else:
            result = "W" if q1_total < line else "L"

    if result is None:
        # Row was found but no outcome handler matched the bet_type.
        # Without logging, the POTD silently stays pending forever.
        logger.warning(
            "POTD settle: no outcome handler for bet_type=%r on %s %s/%s",
            bet_type, sport, date, matchup,
        )
        return None, 0

    return result, _profit_on_settled(result, odds)


def _profit_on_settled(result: str | None, odds: int | float | None) -> float:
    """$100-unit profit formula used by settle_potd. Exposed so
    recalc_potd_profit can rewrite stored profits to match current code
    when an older run left them on a different unit sizing."""
    if result == "W":
        o = odds if odds is not None else -110
        return round(o if o > 0 else 10000 / abs(o), 2)
    if result == "L":
        return -100.0
    return 0.0


def recalc_potd_profit(sport: str) -> dict:
    """Rewrite the ``profit`` column on every settled POTD row using
    the current $100-unit formula. No-op on pending rows (result NULL).

    Idempotent — running it twice produces the same values.
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    rows = conn.execute(
        "SELECT id, result, odds, profit FROM pick_of_day "
        "WHERE result IN ('W', 'L', 'P')"
    ).fetchall()

    updated = 0
    for r in rows:
        new_profit = _profit_on_settled(r["result"], r["odds"])
        if r["profit"] is None or abs((r["profit"] or 0) - new_profit) > 0.01:
            conn.execute(
                "UPDATE pick_of_day SET profit = ? WHERE id = ?",
                (new_profit, r["id"]),
            )
            updated += 1

    conn.commit()
    return {"sport": sport, "settled_rows": len(rows), "updated": updated}
