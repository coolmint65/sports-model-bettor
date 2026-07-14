"""
Player-prop Pick-of-the-Day selector (Phase 2j).

Selects the best prop pick from today's slate per sport and locks
it into ``player_props_picks_pot_day``. Lock-once-per-day mirrors
the main POTD pattern: subsequent calls return the locked POTD
regardless of how the slate develops, so the UI shows a stable
"as of locking time" pick.

Selection score = edge × reliability bonus. Reliability bonus
boosts confidence-tier picks the same way derivative POTD does,
so a +5% strong moneyline beats a +6% lean alt-line. Until
2j-backtest tunes per-bet-type reliability, all bet_types use the
same conservative tier multipliers.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from .player_props_db import (
    list_picks, set_potd, get_potd, _conn_for,
)
from ._tz import et_today_str

logger = logging.getLogger(__name__)


# Confidence-tier reliability multipliers. Strong > Moderate > Lean
# so a +5% strong pick scores above a +8% lean. Conservative starting
# values; tune from settled history once 2j-backtest accumulates.
_CONF_MULT = {"strong": 1.20, "moderate": 1.05, "lean": 0.90}


def _selection_score(pick: dict) -> float:
    """Score = edge × confidence multiplier. Picks with no confidence
    fall to lean's multiplier."""
    edge = float(pick.get("edge") or 0.0)
    conf = (pick.get("confidence") or "lean").lower()
    return edge * _CONF_MULT.get(conf, _CONF_MULT["lean"])


def select_potd(sport: str, date: str | None = None,
                min_edge: float = 6.0) -> dict | None:
    """Pick the top-scoring prop from the upcoming slate. Returns the
    selected pick dict (with selection_score added) or None when no
    qualifying pick exists.

    ``min_edge`` floor avoids POTDs we don't actually believe in --
    a thin slate shouldn't force a marginal pick into the headline.

    Search window: ``target_date`` first, falling back to the next
    two days. NBA games end before midnight EDT and tomorrow's slate
    is dated +1 day before today's calendar even rolls; without the
    fallback the NBA tab silently lost its POTD between the last game
    of one playoff night and midnight local time.

    Filters out picks for games already in 'live' / 'final' /
    'postponed' status — POTD shouldn't surface a pick the user can
    no longer place.
    """
    from datetime import timedelta as _td
    anchor_str = date or et_today_str()
    try:
        anchor = datetime.strptime(anchor_str, "%Y-%m-%d")
    except ValueError:
        anchor = datetime.now()
    # Walk today, today+1, today+2 in order — first day with eligible
    # picks wins. Stops as soon as we have a viable pool, so a thin
    # tomorrow only surfaces when today is genuinely empty.
    for offset in (0, 1, 2):
        d = (anchor + _td(days=offset)).strftime("%Y-%m-%d")
        candidates = list_picks(sport, date=d, limit=1000)
        eligible = [p for p in candidates
                    if (p.get("edge") or 0.0) >= min_edge
                    and p.get("result") is None
                    and not _game_already_started(sport, p.get("game_id"))]
        if eligible:
            eligible.sort(key=_selection_score, reverse=True)
            top = eligible[0]
            top["selection_score"] = _selection_score(top)
            return top
    return None


def _game_already_started(sport: str, game_id) -> bool:
    """Returns True if the underlying game has 'live' / 'final' /
    'postponed' status. POTD shouldn't surface picks on games the
    user can no longer place."""
    if not game_id:
        return False
    try:
        if sport == "mlb":
            from .db import get_conn as _gc
            row = _gc().execute(
                "SELECT status FROM games WHERE mlb_game_id = ? LIMIT 1",
                (str(game_id),),
            ).fetchone()
        elif sport == "nba":
            from .nba_db import get_conn as _gc
            row = _gc().execute(
                "SELECT status FROM nba_games WHERE game_id = ? LIMIT 1",
                (str(game_id),),
            ).fetchone()
        elif sport == "nhl":
            from .nhl_db import get_conn as _gc
            row = _gc().execute(
                "SELECT status FROM nhl_games WHERE game_id = ? LIMIT 1",
                (str(game_id),),
            ).fetchone()
        else:
            return False
    except Exception:
        return False
    if not row:
        return False
    return row["status"] in ("live", "final", "postponed")


def get_or_create_potd(sport: str, date: str | None = None,
                      min_edge: float = 6.0) -> dict | None:
    """Get today's prop POTD, locking it on first call.

    Once selected, the POTD stays frozen for the day -- the same
    semantics as ``engine.pick_of_day.get_or_create_potd``.
    """
    target_date = date or et_today_str()
    existing = get_potd(sport, target_date)
    if existing:
        return existing
    selected = select_potd(sport, target_date, min_edge=min_edge)
    if not selected:
        return None
    pick_id = selected.get("id")
    if not pick_id:
        return None
    reasoning = (
        f"{selected.get('player_name')} {selected.get('pick')}"
        f" — model gives {(selected.get('model_prob') or 0)*100:.0f}%"
        f" vs market {(_implied(selected.get('odds')) or 0)*100:.0f}%."
        f" Edge +{selected.get('edge', 0):.1f}%."
    )
    set_potd(sport, int(pick_id),
             date=target_date,
             selection_score=selected["selection_score"],
             reasoning=reasoning)
    return get_potd(sport, target_date)


def _implied(odds: int | None) -> float | None:
    if odds is None:
        return None
    n = float(odds)
    if n == 0:
        return None
    return 100.0 / (n + 100.0) if n > 0 else abs(n) / (abs(n) + 100.0)


def refresh_potd_for_line_movement(sport: str,
                                   date: str | None = None) -> dict:
    """Repoint a locked prop POTD at the current pending pick for the
    same player + bet_type + side, when HR has moved the line since
    the lock.

    Selection itself stays frozen (same player, same stat, same Over/
    Under direction) — only the line / odds / edge update. The
    underlying pick row that the POTD points at gets swapped to the
    fresher line; the now-orphaned old row is deleted in the same
    pass so the picker's keep-set logic stays clean.

    Returns ``{updated, reason}`` per call.
    """
    if sport not in ("mlb", "nhl", "nba"):
        return {"updated": 0, "reason": f"unsupported sport {sport!r}"}
    target = date or et_today_str()
    conn = _conn_for(sport)

    pot = conn.execute(
        "SELECT pot.id AS pot_id, pot.pick_id, pot.reasoning, "
        "       p.game_id, p.player_id, p.player_name, p.bet_type, "
        "       p.side, p.pick, p.line, p.odds, p.edge, p.model_prob "
        "FROM player_props_picks_pot_day pot "
        "JOIN player_props_picks p ON p.id = pot.pick_id "
        "WHERE pot.date = ? AND p.result IS NULL",
        (target,),
    ).fetchone()
    if not pot:
        return {"updated": 0, "reason": "no live POTD"}
    pot = dict(pot)

    # Find a sibling pending pick for the same (game, player, bet_type,
    # side) at a different line/odds. Order DESC so the most recent
    # insert wins on ties — that's the picker's current top.
    sibling = conn.execute(
        "SELECT id, pick, line, odds, edge, model_prob "
        "FROM player_props_picks "
        "WHERE game_id = ? AND player_id = ? AND bet_type = ? "
        "  AND side = ? AND id != ? AND result IS NULL "
        "ORDER BY id DESC LIMIT 1",
        (pot["game_id"], int(pot["player_id"]), pot["bet_type"],
         pot["side"], int(pot["pick_id"])),
    ).fetchone()
    if not sibling:
        return {"updated": 0, "reason": "no sibling line"}
    sibling = dict(sibling)

    # No-op if line + price match (common case — picker just re-ran
    # without HR moving anything).
    if (sibling["pick"] == pot["pick"]
            and sibling["odds"] == pot["odds"]
            and sibling["edge"] == pot["edge"]):
        return {"updated": 0, "reason": "no change"}

    # Repoint POTD at the new pick row, drop the old (now orphaned).
    new_reasoning = (
        f"{pot['player_name']} {sibling['pick']}"
        f" — model gives {(sibling['model_prob'] or 0) * 100:.0f}%"
        f" vs market {(_implied(sibling['odds']) or 0) * 100:.0f}%."
        f" Edge +{sibling['edge']:.1f}%."
    )
    # Re-score using the same helper as selection so the badge stays
    # consistent. Confidence pulled from the new line's pick row.
    sibling_for_score = conn.execute(
        "SELECT edge, confidence FROM player_props_picks WHERE id = ?",
        (int(sibling["id"]),),
    ).fetchone()
    new_score = _selection_score(dict(sibling_for_score) if sibling_for_score
                                 else {"edge": sibling["edge"]})
    conn.execute(
        "UPDATE player_props_picks_pot_day "
        "SET pick_id = ?, reasoning = ?, selection_score = ? "
        "WHERE id = ?",
        (int(sibling["id"]), new_reasoning, new_score,
         int(pot["pot_id"])),
    )
    conn.execute(
        "DELETE FROM player_props_picks WHERE id = ?",
        (int(pot["pick_id"]),),
    )
    conn.commit()
    logger.info(
        "prop POTD %s line-refresh: %r %+d edge=%.1f -> %r %+d edge=%.1f",
        sport, pot["pick"], int(pot["odds"] or 0), float(pot["edge"] or 0),
        sibling["pick"], int(sibling["odds"] or 0),
        float(sibling["edge"] or 0),
    )
    return {"updated": 1,
            "old_pick": pot["pick"], "old_odds": pot["odds"],
            "new_pick": sibling["pick"], "new_odds": sibling["odds"]}


def get_potd_summary(sport: str) -> dict:
    """Aggregate W/L/profit across the prop POTD lock table joined to
    the props picks table for results. Same shape as
    `engine.pick_of_day._read.get_potd_summary` so the response can
    pass straight to PotdHero's `summary` prop. Lets the prop POTD
    card show its own running record consistent with core POTD."""
    from .player_props_db import _conn_for, ensure_tables
    ensure_tables(sport)
    conn = _conn_for(sport)
    # Stake-weighted profit + ROI (mirrors per-sport trackers).
    row = conn.execute("""
        SELECT
            COUNT(p.id) AS total,
            SUM(CASE WHEN p.result = 'W' THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN p.result = 'L' THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN p.result = 'P' THEN 1 ELSE 0 END) AS pushes,
            COALESCE(SUM(p.profit * COALESCE(p.stake_units, 1.0)), 0) AS profit,
            COALESCE(SUM(CASE WHEN p.result IN ('W','L')
                                THEN COALESCE(p.stake_units, 1.0)
                                ELSE 0 END), 0) AS stake_units_settled
        FROM player_props_picks_pot_day pd
        JOIN player_props_picks p ON p.id = pd.pick_id
    """).fetchone()
    overall = dict(row) if row else {}
    w = overall.get("wins") or 0
    l = overall.get("losses") or 0
    settled = w + l
    staked_u = overall.get("stake_units_settled") or 0
    return {
        "total": overall.get("total") or 0,
        "wins": w,
        "losses": l,
        "pushes": overall.get("pushes") or 0,
        "profit": round(overall.get("profit") or 0, 2),
        "win_pct": round(w / settled * 100, 1) if settled > 0 else 0,
        "roi": round((overall.get("profit") or 0) / staked_u, 1) if staked_u > 0 else 0,
    }


__all__ = ["select_potd", "get_or_create_potd",
           "refresh_potd_for_line_movement",
           "get_potd_summary"]
