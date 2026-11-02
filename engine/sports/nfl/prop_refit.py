"""Weekly NFL player-prop refit: the model-vs-market blend weight per stat, fitted on graded
lines (MintyBot 2026-10-26, Austin "Yes to weekly refit").

The math lives in engine.prop_refit (shared with NHL). Here: the NFL source -- every main
line engine.sports.nfl.prop_picks prices is logged to nfl_player.db ``prop_eval_log`` and
graded from the nflverse logs -- and the weights file data/football/nfl_prop_weights.json,
which prop_picks reads per bet type. The worker's NFL props pass calls maybe_refit().
"""
from __future__ import annotations

import logging
import sqlite3

from ... import prop_refit as _core
from .player_props import _DB, _conn

logger = logging.getLogger(__name__)

DEFAULT_W = _core.DEFAULT_W
MARKETS = ("Passing Yards", "Rushing Yards", "Receiving Yards", "Receptions", "Rushing Attempts",
           "Pass Attempts", "Completions", "Rush+Rec Yards")
_PATH = _DB.parent / "nfl_prop_weights.json"


def _graded_rows() -> list:
    c = _conn()
    try:
        return c.execute(
"SELECT bet_type, "
            "CASE WHEN pick_seen IS NOT NULL THEN pick_line ELSE first_line END AS ref_line, "
            "CASE WHEN pick_seen IS NOT NULL THEN pick_over ELSE first_over END AS ref_over, "
            "CASE WHEN pick_seen IS NOT NULL THEN pick_under ELSE first_under END AS ref_under, "
            "CASE WHEN pick_seen IS NOT NULL THEN pick_p_over ELSE first_p_over END AS ref_p_over, "
            "last_line, last_over, last_under, actual "
            "FROM prop_eval_log WHERE actual IS NOT NULL").fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        c.close()


def refit() -> dict:
    doc = _core.fit(_graded_rows(), MARKETS)
    _core.write(_PATH, doc)
    logger.info("nfl prop refit: %s", {bt: (e["n"], e["w"], e["status"], e.get("clv_move_pp"))
                                       for bt, e in doc["markets"].items()})
    return doc


def maybe_refit() -> dict | None:
    """Refit when the weights file is missing or a week old; else None."""
    return refit() if _core.due(_PATH) else None


def weight_for(bet_type: str) -> float:
    return _core.weight_for(_PATH, bet_type)


__all__ = ["refit", "maybe_refit", "weight_for", "DEFAULT_W"]
