"""
MLB per-player game-log ingest (Phase 2g-i).

Pulls per-player pitching + batting stat lines from the MLB Stats API
boxscore endpoint and writes them to ``player_game_logs`` (the table
2f-i created). The settler in ``engine.player_props_tracker`` reads
these rows to mark prop picks W/L/P; without this ingest every pick
stays pending forever.

Source: ``https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live``
under ``liveData.boxscore.teams.{home,away}.players.{playerId}``.

Stat keys written (matches ``_MLB_STAT_KEY`` in player_props_tracker):

    Pitcher: k_p, bb_p, outs, er, h_allowed
    Batter:  k_b, bb_b, hr, h, tb, rbi, r, sb

``k`` and ``bb`` are namespaced per role because Ohtani-style two-way
players have BOTH pitching and batting lines for the same game and a
non-namespaced merge would silently lose one set. Other counts are
uniquely pitcher-only or batter-only so they collapse cleanly.

``batting_order`` (1-9) is stashed alongside the stats so 2g-iii's
projected-lineup logic can derive a rolling 14-day batting order
without a separate table. MLB's API encodes order as ``N00``
(100=leadoff, 900=#9, 101=substitute) — we strip the trailing
positional suffix.

Idempotent: UPSERT keyed on ``(player_id, game_id)`` so re-runs
overwrite stale partial pulls (e.g. mid-game ingest replaced by
finalized boxscore).
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from typing import Any

from .player_props_db import upsert_game_log

logger = logging.getLogger(__name__)

MLB_API = "https://statsapi.mlb.com/api/v1"
MLB_API_V11 = "https://statsapi.mlb.com/api/v1.1"

# Be polite to the upstream — quick sync may walk multiple recent days.
_REQUEST_INTERVAL_S = 0.25


def _fetch_json(url: str, timeout: int = 20) -> dict | None:
    """Fetch JSON with one retry on transient network failures (mid-stream
    chunk drops, socket resets, 5xx). Multi-thousand-game backfills
    surface these every few minutes — retry-once tolerates them without
    failing the run."""
    import http.client
    last_err: Exception | None = None
    for attempt in (1, 2):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if 500 <= e.code < 600 and attempt == 1:
                last_err = e
                time.sleep(1.0)
                continue
            logger.warning("MLB Stats HTTP %s for %s", e.code, url)
            return None
        except (urllib.error.URLError, TimeoutError,
                http.client.HTTPException, OSError) as e:
            last_err = e
            if attempt == 1:
                time.sleep(1.0)
                continue
            logger.warning("MLB Stats network error after retry (%s): %s",
                           type(e).__name__, e)
            return None
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("MLB Stats bad JSON: %s", e)
            return None
    if last_err is not None:
        logger.warning("MLB Stats giving up on %s: %s", url, last_err)
    return None


def _safe_int(v: Any) -> int:
    """MLB API returns counts as ints already, but some fields ship
    as strings (e.g. ``inningsPitched`` = '6.1'). Defensive cast."""
    if v is None:
        return 0
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return int(v)
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def _normalized_batting_order(raw: Any) -> int | None:
    """MLB encodes order as 100/200/.../900 with sub-position suffix.
    Returns 1-9 for starters, None for non-starters / pinch roles."""
    n = _safe_int(raw)
    if n <= 0:
        return None
    # Trailing 0 = starter; non-zero last digit = substitute. Only
    # the starter slot matters for projected-lineup inference.
    if n % 10 != 0:
        return None
    pos = n // 100
    return pos if 1 <= pos <= 9 else None


def _build_pitcher_stats(p: dict) -> dict:
    """Extract pitcher counting stats. Empty dict if the player didn't
    pitch in this game (i.e. ``stats.pitching`` is missing or empty)."""
    pitching = (p.get("stats") or {}).get("pitching") or {}
    if not pitching:
        return {}
    return {
        "k_p":       _safe_int(pitching.get("strikeOuts")),
        "bb_p":      _safe_int(pitching.get("baseOnBalls")),
        "outs":      _safe_int(pitching.get("outs")),
        "er":        _safe_int(pitching.get("earnedRuns")),
        "h_allowed": _safe_int(pitching.get("hits")),
        "is_starter": 1 if _safe_int(pitching.get("gamesStarted")) > 0 else 0,
    }


def _build_batter_stats(p: dict) -> dict:
    """Extract batter counting stats. Empty dict when the player had
    no batting line (DH-only pitcher, or didn't appear at the plate)."""
    batting = (p.get("stats") or {}).get("batting") or {}
    if not batting:
        return {}
    out = {
        "hr":   _safe_int(batting.get("homeRuns")),
        "h":    _safe_int(batting.get("hits")),
        "tb":   _safe_int(batting.get("totalBases")),
        "rbi":  _safe_int(batting.get("rbi")),
        "r":    _safe_int(batting.get("runs")),
        "sb":   _safe_int(batting.get("stolenBases")),
        "bb_b": _safe_int(batting.get("baseOnBalls")),
        "k_b":  _safe_int(batting.get("strikeOuts")),
        "ab":   _safe_int(batting.get("atBats")),
        "pa":   _safe_int(batting.get("plateAppearances")),
    }
    order = _normalized_batting_order(p.get("battingOrder"))
    if order is not None:
        out["batting_order"] = order
    return out


def _ingest_team(side: str, team_block: dict, opp_team_id: int,
                 game_id: str, game_date: str) -> int:
    """Walk one team's player map and upsert every player who appeared.
    Returns the count of rows written."""
    is_home = (side == "home")
    team_id = (team_block.get("team") or {}).get("id")
    players = team_block.get("players") or {}
    rows = 0
    for pid_key, p in players.items():
        person = p.get("person") or {}
        player_id = _safe_int(person.get("id"))
        player_name = person.get("fullName") or pid_key
        if not player_id:
            continue
        pitching = _build_pitcher_stats(p)
        batting = _build_batter_stats(p)
        if not pitching and not batting:
            # Player on the roster but didn't appear in the game (e.g.
            # bench bat that wasn't used). Skip — settler will leave any
            # prop picks pending and the next refresh corrects.
            continue
        # Merge the two — namespaced keys (k_p/k_b, bb_p/bb_b) avoid
        # collisions for two-way players like Ohtani who carry both.
        stats: dict[str, Any] = {**pitching, **batting}
        upsert_game_log(
            "mlb",
            player_id=player_id, player_name=player_name,
            game_id=str(game_id), date=game_date,
            stats=stats,
            team_id=team_id, opp_team_id=opp_team_id,
            is_home=is_home,
        )
        rows += 1
    return rows


def ingest_game(game_pk: int) -> int:
    """Pull one game's boxscore and write per-player rows. Returns the
    number of player rows written (0 on fetch failure)."""
    url = f"{MLB_API_V11}/game/{game_pk}/feed/live"
    data = _fetch_json(url)
    if not data:
        return 0
    game_data = (data.get("gameData") or {})
    status = (game_data.get("status") or {}).get("abstractGameCode", "")
    if status != "F":
        # Only ingest finalized games. Mid-game pulls would write
        # partial counts that look like a low-scoring blowout — let
        # the next sync tick after final pick this up cleanly.
        logger.debug("game %s not final (status=%s); skipping", game_pk, status)
        return 0
    boxscore = (data.get("liveData") or {}).get("boxscore") or {}
    teams = boxscore.get("teams") or {}
    if "home" not in teams or "away" not in teams:
        return 0
    # Date for the row — prefer officialDate (handles late-night games
    # that span midnight UTC) over the raw datetime field.
    dt = (game_data.get("datetime") or {})
    game_date = (dt.get("officialDate") or dt.get("originalDate") or
                 dt.get("dateTime", "")[:10])
    home_team_id = (teams["home"].get("team") or {}).get("id") or 0
    away_team_id = (teams["away"].get("team") or {}).get("id") or 0
    rows = 0
    rows += _ingest_team("home", teams["home"], away_team_id,
                         str(game_pk), game_date)
    rows += _ingest_team("away", teams["away"], home_team_id,
                         str(game_pk), game_date)
    return rows


def ingest_recent_finals(lookback_days: int = 30) -> dict:
    """Backfill the last ``lookback_days`` days of MLB games marked
    final in our schedule DB. Idempotent UPSERTs make this safe to
    re-run; the cold-start invocation pre-warms the table for the MC
    extension in 2g-ii.

    Returns counts ``{games: N, rows: M, skipped: K}``.
    """
    from .db import get_conn
    conn = get_conn()
    today = datetime.now().strftime("%Y-%m-%d")
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    cur = conn.execute(
        "SELECT mlb_game_id FROM games "
        "WHERE date >= ? AND date <= ? AND status = 'final' "
        "ORDER BY date",
        (cutoff, today),
    )
    game_ids = [r[0] if not hasattr(r, "keys") else r["mlb_game_id"]
                for r in cur.fetchall()]
    games = rows = skipped = 0
    for gpk in game_ids:
        if not gpk:
            skipped += 1
            continue
        n = ingest_game(int(gpk))
        if n:
            games += 1
            rows += n
        else:
            skipped += 1
        time.sleep(_REQUEST_INTERVAL_S)
    logger.info("ingest_recent_finals: games=%d rows=%d skipped=%d",
                games, rows, skipped)
    return {"games": games, "rows": rows, "skipped": skipped}


def ingest_today() -> dict:
    """Quick-sync hook: pull boxscores for any of today's games that
    are now ``final``. Pairs with the broader ``ingest_recent_finals``
    backfill — call this on every quick sync to drain the day's
    settled games into ``player_game_logs`` with minimal latency."""
    from .db import get_conn
    conn = get_conn()
    today = datetime.now().strftime("%Y-%m-%d")
    cur = conn.execute(
        "SELECT mlb_game_id FROM games "
        "WHERE date = ? AND status = 'final'",
        (today,),
    )
    game_ids = [r[0] if not hasattr(r, "keys") else r["mlb_game_id"]
                for r in cur.fetchall()]
    games = rows = 0
    for gpk in game_ids:
        if not gpk:
            continue
        n = ingest_game(int(gpk))
        if n:
            games += 1
            rows += n
        time.sleep(_REQUEST_INTERVAL_S)
    logger.info("ingest_today: games=%d rows=%d", games, rows)
    return {"games": games, "rows": rows}


__all__ = ["ingest_game", "ingest_recent_finals", "ingest_today"]
