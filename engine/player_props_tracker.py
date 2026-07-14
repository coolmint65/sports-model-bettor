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
    if sport == "wnba":
        # WNBA shares NBA's bet-type → stat-key mapping (same box-stats,
        # same HR market labels).
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


def _game_is_final(sport: str, game_id: str) -> bool:
    """Return True when the per-sport games table marks this game final.
    Used to detect DNP props — if the game is final but no log exists
    for the player, the prop should void rather than sit pending."""
    table_col = {
        "mlb":  ("games", "mlb_game_id"),  # picks store espn_event_id; mlb games keyed on mlb_game_id
        "nhl":  ("nhl_games", "game_id"),
        "nba":  ("nba_games", "game_id"),
        "wnba": ("games", "game_id"),      # basketball framework db
    }.get(sport)
    if not table_col:
        return False
    table, col = table_col
    try:
        if sport == "mlb":
            from .db import get_conn
            conn = get_conn()
        elif sport == "nhl":
            from .nhl_db import get_conn
            conn = get_conn()
        elif sport == "wnba":
            from .basketball._db import get_conn
            conn = get_conn("wnba")
        else:
            from .nba_db import get_conn
            conn = get_conn()
        if sport == "mlb":
            # MLB props store the MLB Stats game_pk (matches games.mlb_game_id).
            # The games table doesn't carry a separate espn_event_id column —
            # earlier code referenced one that doesn't exist and silently
            # returned False, suppressing every DNP-void.
            row = conn.execute(
                f"SELECT status FROM {table} WHERE mlb_game_id = ? LIMIT 1",
                (str(game_id),),
            ).fetchone()
        else:
            row = conn.execute(
                f"SELECT status FROM {table} WHERE {col} = ? LIMIT 1",
                (str(game_id),),
            ).fetchone()
        return bool(row and (row["status"] or "").lower() == "final")
    except Exception as e:
        logger.debug("_game_is_final lookup failed for %s/%s: %s",
                     sport, game_id, e)
        return False


def settle_player_props(sport: str) -> dict:
    """Settle pending prop picks for ``sport``. Idempotent — re-runs
    only touch rows still pending at call time. Returns a per-call
    counter dict mirroring ``settle_derivative_picks``.

    DNP handling: a pending pick whose game is marked ``final`` but
    has no player_game_logs row settles as Push (profit=0). Most books
    void DNP props — encoding as Push matches that behaviour and stops
    the row sitting pending forever after a player scratch / late-OUT.
    """
    if sport not in ("mlb", "nhl", "nba", "wnba"):
        raise ValueError(f"unknown sport: {sport}")

    pending = list_picks(sport, pending_only=True, limit=10_000)
    if not pending:
        return {"settled": 0, "wins": 0, "losses": 0, "pushes": 0}

    # Self-heal: the settler depends on the games table reporting
    # status='final' (via _game_is_final) before it can resolve a DNP
    # or pull a player log. The morning sync fetches once and never
    # re-runs as games progress — so a 4pm-tip game that finishes at
    # 7pm stays as status='scheduled' in the games table all evening,
    # blocking every prop on it from settling. User report 2026-04-28:
    # 'Nothing to settle' even though BOS/PHI was final. Refresh
    # today's games before settling so live → final transitions land.
    # Throttled per process (3 min) so back-to-back settle clicks
    # don't fan-out repeated ESPN fetches.
    import time as _t
    if not hasattr(settle_player_props, "_last_games_refresh"):
        settle_player_props._last_games_refresh = {}
    last_g = settle_player_props._last_games_refresh.get(sport, 0)
    if (_t.time() - last_g) > 180:
        try:
            from datetime import datetime as _dt, timedelta as _td
            today = _dt.now().strftime("%Y-%m-%d")
            yesterday = (_dt.now() - _td(days=1)).strftime("%Y-%m-%d")
            if sport == "mlb":
                from scrapers.mlb_stats import fetch_schedule as _fs
                _fs(yesterday, today)
            elif sport == "nhl":
                from scrapers.nhl_api import fetch_schedule as _fs
                _fs(yesterday, today)
            elif sport == "wnba":
                # WNBA lives under the basketball framework — refresh
                # both yesterday and today so any late finals land.
                from engine.basketball._espn_ingest import ingest_today as _fs
                _fs("wnba", date=yesterday)
                _fs("wnba", date=today)
            else:  # nba
                from scrapers.nba_espn import fetch_schedule as _fs
                _fs(yesterday, today)
            settle_player_props._last_games_refresh[sport] = _t.time()
            logger.debug("Pre-settle games refresh ran for %s", sport)
        except Exception as e:
            logger.warning("Pre-settle games-table refresh failed for %s: %s",
                           sport, e)

    # Self-heal: if any pending pick points at a finalized game with
    # no log row, pull recent finalized box scores so DNP-void only
    # fires when the player genuinely didn't appear, not when our
    # ingest just missed the night. Throttled per process (5 min) so
    # the API endpoint doesn't trigger an MLB-Stats fan-out on every
    # poll.
    import time as _t
    if not hasattr(settle_player_props, "_last_ingest"):
        settle_player_props._last_ingest = {}
    last = settle_player_props._last_ingest.get(sport, 0)
    needs_ingest = False
    if (_t.time() - last) > 300:
        for pick in pending[:25]:  # cheap sample; full check happens below
            stat_key = _stat_key_for(sport, pick.get("bet_type") or "")
            if not stat_key:
                continue
            actual = _lookup_actual(sport, int(pick["player_id"]),
                                    str(pick["game_id"]), stat_key)
            if actual is None and _game_is_final(sport, str(pick["game_id"])):
                needs_ingest = True
                break
    if needs_ingest:
        try:
            if sport == "mlb":
                from .mlb_player_logs import ingest_recent_finals as _ingest
            elif sport == "nhl":
                from .nhl_player_logs import ingest_recent_finals as _ingest
            elif sport == "wnba":
                from .basketball._wnba_player_logs import (
                    ingest_recent_finals as _ingest,
                )
            else:
                from .nba_player_logs import ingest_recent_finals as _ingest
            _ingest(lookback_days=3)
            settle_player_props._last_ingest[sport] = _t.time()
        except Exception as e:
            logger.warning("Pre-settle log refresh failed for %s: %s", sport, e)

    settled = wins = losses = pushes = voided = 0
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
            # Game log not in DB yet. If the game itself is final the
            # player didn't play — void as Push so the row leaves the
            # pending queue. Otherwise leave pending; next tick gets it.
            if _game_is_final(sport, str(pick["game_id"])):
                ok = settle_pick(sport, int(pick["id"]),
                                 actual_value=None, result="P", profit=0.0)
                if ok:
                    settled += 1
                    pushes += 1
                    voided += 1
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
        logger.info("player_props %s: settled=%d W=%d L=%d P=%d (DNP voids=%d)",
                    sport, settled, wins, losses, pushes, voided)
    return {"settled": settled, "wins": wins, "losses": losses,
            "pushes": pushes, "voided_dnp": voided}


__all__ = [
    "settle_player_props",
    "_determine_outcome",
    "_profit_for",
    "_stat_key_for",
]
