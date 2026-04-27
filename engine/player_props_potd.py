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
    """Pick the top-scoring prop from today's picks. Returns the
    selected pick dict (with selection_score added) or None when
    no qualifying pick exists.

    ``min_edge`` floor avoids POTDs we don't actually believe in --
    a thin slate shouldn't force a marginal pick into the headline.

    Filters out picks for games already in 'live' or 'final' state.
    Without this, late-night UTC date drift (NBA games starting at
    8pm EDT crossing midnight UTC) lets yesterday's already-played
    games bleed into today's POTD pool — surfaced as "POTD is for a
    game from yesterday" by the user.
    """
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    candidates = list_picks(sport, date=target_date, limit=1000)
    eligible = [p for p in candidates
                if (p.get("edge") or 0.0) >= min_edge
                and p.get("result") is None
                and not _game_already_started(sport, p.get("game_id"))]
    if not eligible:
        return None
    eligible.sort(key=_selection_score, reverse=True)
    top = eligible[0]
    top["selection_score"] = _selection_score(top)
    return top


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
    target_date = date or datetime.now().strftime("%Y-%m-%d")
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


__all__ = ["select_potd", "get_or_create_potd"]
