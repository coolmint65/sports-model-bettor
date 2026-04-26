"""
Player-props settler (Phase 2f-iii).

Mirrors ``engine.derivative_tracker.settle_derivative_picks`` but
points at ``player_props_picks``. Unlike derivatives, props don't
trampoline through the main game settler — game outcomes can't tell
us how many strikeouts a pitcher recorded. Instead the settler reads
``player_game_logs`` (populated by Phase 2g/2h/2i ingest) and compares
the observed stat against each pick's line.

Outcome rule is uniform Over/Under: ``actual > line`` wins for Over,
``actual < line`` wins for Under, ties push. Alt yes/no markets are
encoded by the scraper as Over X.5 (a "5+ Strikeouts" alt becomes
``line=4.5, side='Over'``) so this module never needs an ``is_alt``
branch — the .5 line + Over rule already gets actual≥5 right.

Because game-log ingest doesn't ship until 2g, this module
short-circuits cleanly when no log row exists for a pick: the row
stays pending. Once 2g lands, the same call drains the queue.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from .player_props_db import _conn_for, list_picks, settle_pick

logger = logging.getLogger(__name__)


# bet_type → key in the ``stats_json`` blob written by 2g/2h/2i ingest.
# Centralizing here so the ingest can target the same keys without
# duplicating the contract.
_MLB_STAT_KEY: dict[str, str] = {
    # Pitcher / batter Ks and BBs are namespaced per role so two-way
    # players (Ohtani-style) keep both lines on one row without a
    # collision. Other stats are uniquely pitcher-only or batter-only.
    "Pitcher Ks O/U":         "k_p",
    "Pitcher Walks O/U":      "bb_p",
    "Pitcher Outs Recorded":  "outs",
    "Pitcher Earned Runs":    "er",
    "Pitcher Hits Allowed":   "h_allowed",
    "Batter HR":              "hr",
    "Batter Hits O/U":        "h",
    "Batter TB":              "tb",
    "Batter RBI":             "rbi",
    "Batter Runs Scored":     "r",
    "Batter Stolen Bases":    "sb",
    "Batter Walks":           "bb_b",
    "Batter Strikeouts":      "k_b",
}


_NBA_STAT_KEY: dict[str, str] = {
    "Player Points":    "pts",
    "Player Rebounds":  "reb",
    "Player Assists":   "ast",
    "Player PRA":       "pra",        # synthesized from pts+reb+ast at lookup
    "Player 3PM":       "tpm",
    "Player Steals":    "stl",
    "Player Blocks":    "blk",
    "Player Turnovers": "to",
    "Player FT Made":   "ftm",
}

_NHL_STAT_KEY: dict[str, str] = {
    "Skater SOG":         "sog",
    "Skater Points":      "p",          # synthesized from g+a at lookup
    "Skater Goals":       "g",
    "Skater Assists":     "a",
    "Skater Hits":        "hits",
    "Skater Blocks":      "blocks",
    "Goalie Saves":       "saves",
    "Goalie Goals Against": "ga",
}


def _stat_key_for(sport: str, bet_type: str) -> str | None:
    if sport == "mlb":
        return _MLB_STAT_KEY.get(bet_type)
    if sport == "nba":
        return _NBA_STAT_KEY.get(bet_type)
    if sport == "nhl":
        return _NHL_STAT_KEY.get(bet_type)
    return None


def _determine_outcome(line: float | None, side: str | None,
                       actual: float | None) -> str | None:
    """Returns 'W', 'L', 'P', or None when inputs are missing.

    Standard Over/Under semantics — Over wins when ``actual > line``,
    Under wins when ``actual < line``, equal pushes. Alt yes/no
    markets are normalized to Over X.5 by the scraper so the same
    rule covers them without a special case.
    """
    if line is None or actual is None or not side:
        return None
    s = side.strip().lower()
    if s in ("over", "yes"):
        if actual > line:  return "W"
        if actual < line:  return "L"
        return "P"
    if s in ("under", "no"):
        if actual < line:  return "W"
        if actual > line:  return "L"
        return "P"
    return None


def _profit_for(result: str, odds: int | None, stake: float = 100.0) -> float:
    """American-odds payout for a $stake bet. Push returns 0 (stake
    refunded by convention — we record P/L=0)."""
    if result == "P" or odds is None:
        return 0.0
    if result == "W":
        if odds > 0:
            return stake * (odds / 100.0)
        return stake * (100.0 / abs(odds))
    if result == "L":
        return -stake
    return 0.0


# Composite stat keys synthesized from base stats at settle time. Stored
# as derived rather than written into player_game_logs so a box-score
# correction on any base stat propagates without a re-ingest.
_COMPOSITE_KEYS: dict[str, tuple[str, ...]] = {
    "pra": ("pts", "reb", "ast"),    # NBA Player PRA
    "p":   ("g", "a"),                # NHL Skater Points
}


def _lookup_actual(sport: str, player_id: int, game_id: str,
                   stat_key: str) -> float | None:
    """Return the observed stat value from player_game_logs, or None
    when the log row doesn't exist or doesn't carry that stat. Composite
    keys (PRA, Points) sum their base stats — handles missing components
    by short-circuiting to None so we don't false-settle on partial data.
    """
    conn = _conn_for(sport)
    row = conn.execute(
        "SELECT stats_json FROM player_game_logs "
        "WHERE player_id = ? AND game_id = ?",
        (int(player_id), str(game_id)),
    ).fetchone()
    if not row:
        return None
    try:
        stats = json.loads(row["stats_json"] or "{}")
    except (ValueError, TypeError):
        return None

    components = _COMPOSITE_KEYS.get(stat_key)
    if components:
        total = 0.0
        for c in components:
            v = stats.get(c)
            if v is None:
                return None
            try:
                total += float(v)
            except (TypeError, ValueError):
                return None
        return total

    val = stats.get(stat_key)
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def settle_player_props(sport: str) -> dict:
    """Settle pending prop picks for ``sport``. Idempotent — re-runs
    only touch rows still pending at call time. Returns a per-call
    counter dict mirroring ``settle_derivative_picks``."""
    if sport not in ("mlb", "nhl", "nba"):
        raise ValueError(f"unknown sport: {sport}")

    pending = list_picks(sport, pending_only=True, limit=10_000)
    if not pending:
        return {"settled": 0, "wins": 0, "losses": 0, "pushes": 0}

    settled = wins = losses = pushes = 0
    for pick in pending:
        bet_type = pick.get("bet_type") or ""
        stat_key = _stat_key_for(sport, bet_type)
        if not stat_key:
            # Bet type we don't yet know how to settle. Leave pending —
            # adding the stat key here is a one-line fix when it
            # surfaces, no need to mass-push existing rows.
            continue
        actual = _lookup_actual(sport, int(pick["player_id"]),
                                str(pick["game_id"]), stat_key)
        if actual is None:
            # Game log not in DB yet — happens during the window
            # between game completion and the post-game ingest. Leave
            # pending; the next settler tick picks it up.
            continue
        result = _determine_outcome(pick.get("line"), pick.get("side"), actual)
        if result is None:
            continue
        profit = _profit_for(result, pick.get("odds"))
        ok = settle_pick(sport, int(pick["id"]),
                         actual_value=actual, result=result, profit=profit)
        if not ok:
            continue
        settled += 1
        if result == "W":   wins   += 1
        elif result == "L": losses += 1
        else:               pushes += 1

    if settled:
        logger.info("player_props %s: settled=%d W=%d L=%d P=%d",
                    sport, settled, wins, losses, pushes)
    return {"settled": settled, "wins": wins, "losses": losses, "pushes": pushes}


__all__ = [
    "settle_player_props",
    "_determine_outcome",
    "_profit_for",
    "_stat_key_for",
]
