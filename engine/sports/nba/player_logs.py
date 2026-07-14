"""
NBA per-player game-log ingest (Phase 2h-i prep).

Mirrors ``engine.mlb_player_logs`` for ESPN's NBA boxscore. Pulls
per-game stat lines for every athlete who played and writes them to
``player_game_logs`` (NBA db) so the player-props settler has data
and the distribution fitter can lock NBA shape choices ahead of the
2h-ii MC extension.

Source: ``site.api.espn.com/.../basketball/nba/summary?event={id}``
under ``boxscore.players[].statistics[0]`` — ``athletes`` is a list
of dicts whose ``stats`` array is positionally aligned with the
parent block's ``labels`` array (typically:
[MIN, PTS, FG, 3PT, FT, REB, AST, TO, STL, BLK, OREB, DREB, PF, +/-]).

Stat keys written (matches ``_NBA_PROP_TYPES`` consumers):

    pts, reb, ast, tpm, ftm, to, stl, blk, min, oreb, dreb

PRA isn't stored separately — settler computes ``pts + reb + ast``
from those three keys at settle time so we don't fork-store derived
counts that drift on box correction.

Idempotent: UPSERT keyed on ``(player_id, game_id)``.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from typing import Any

from ..._tz import et_today_str

from ...player_props_db import upsert_game_log

logger = logging.getLogger(__name__)

ESPN_NBA_SUMMARY = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary"
)

_REQUEST_INTERVAL_S = 0.25


def _fetch_json(url: str, timeout: int = 20) -> dict | None:
    """Fetch JSON with one retry on transient network failures.

    ESPN occasionally truncates chunked responses mid-stream
    (http.client.IncompleteRead), and during a multi-thousand-game
    backfill that surfaces every few minutes. Retrying once handles
    the transient case without spamming the API.
    """
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
            logger.warning("ESPN NBA HTTP %s for %s", e.code, url)
            return None
        except (urllib.error.URLError, TimeoutError,
                http.client.HTTPException, OSError) as e:
            last_err = e
            if attempt == 1:
                time.sleep(1.0)
                continue
            logger.warning("ESPN NBA network error after retry (%s): %s",
                           type(e).__name__, e)
            return None
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("ESPN NBA bad JSON: %s", e)
            return None
    if last_err is not None:
        logger.warning("ESPN NBA giving up on %s: %s", url, last_err)
    return None


def _safe_int(v: Any) -> int:
    if v is None or v == "":
        return 0
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return int(v)
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return 0


def _split_made_attempted(s: str) -> int:
    """ESPN packs 'made-attempted' fields as '3-15'. Returns made.
    Falls back to 0 on any parse failure."""
    if not isinstance(s, str) or "-" not in s:
        return _safe_int(s)
    head, _, _tail = s.partition("-")
    return _safe_int(head)


_LABEL_TO_KEY = {
    "MIN":  ("min",   _safe_int),
    "PTS":  ("pts",   _safe_int),
    "3PT":  ("tpm",   _split_made_attempted),
    "FT":   ("ftm",   _split_made_attempted),
    "REB":  ("reb",   _safe_int),
    "AST":  ("ast",   _safe_int),
    "TO":   ("to",    _safe_int),
    "STL":  ("stl",   _safe_int),
    "BLK":  ("blk",   _safe_int),
    "OREB": ("oreb",  _safe_int),
    "DREB": ("dreb",  _safe_int),
}


def _parse_athlete_stats(stats: list, labels: list) -> dict:
    """Walk the ``stats`` array against ``labels``, mapping ESPN's
    label codes to our canonical stat keys. Skips columns we don't
    track (FG raw, PF, +/-)."""
    if not stats or not labels:
        return {}
    out: dict[str, int] = {}
    for label, raw in zip(labels, stats):
        key_fn = _LABEL_TO_KEY.get(label.upper() if isinstance(label, str) else "")
        if key_fn is None:
            continue
        key, parser = key_fn
        out[key] = parser(raw)
    return out


def _ingest_team(team_block: dict, opp_team_id: int,
                 game_id: str, game_date: str, is_home: bool) -> int:
    rows = 0
    team = team_block.get("team") or {}
    team_id = _safe_int(team.get("id"))
    stat_blocks = team_block.get("statistics") or []
    if not stat_blocks:
        return 0
    block = stat_blocks[0]
    labels = block.get("labels") or block.get("keys") or []
    athletes = block.get("athletes") or []
    for entry in athletes:
        if entry.get("didNotPlay"):
            continue
        athlete = entry.get("athlete") or {}
        player_id = _safe_int(athlete.get("id"))
        player_name = athlete.get("displayName") or athlete.get("shortName") or ""
        if not player_id:
            continue
        stats = _parse_athlete_stats(entry.get("stats") or [], labels)
        if not stats:
            continue
        # Skip players with 0 minutes — keeps tiny-sample noise out of
        # the distribution fit. The ingest is harmless for them, but
        # filtering at write time also keeps `player_game_logs` from
        # bloating with garbage rows.
        if stats.get("min", 0) <= 0:
            continue
        if entry.get("starter"):
            stats["starter"] = 1
        upsert_game_log(
            "nba",
            player_id=player_id, player_name=player_name,
            game_id=str(game_id), date=game_date,
            stats=stats,
            team_id=team_id, opp_team_id=opp_team_id,
            is_home=is_home,
        )
        rows += 1
    return rows


def ingest_game(event_id: str) -> int:
    """Pull one NBA event's boxscore and write per-player rows.
    Returns the row count (0 on fetch failure or non-final game)."""
    url = f"{ESPN_NBA_SUMMARY}?event={event_id}"
    data = _fetch_json(url)
    if not data:
        return 0
    header = data.get("header") or {}
    comp = (header.get("competitions") or [{}])[0]
    status = ((comp.get("status") or {}).get("type") or {}).get("name", "")
    if status != "STATUS_FINAL":
        logger.debug("event %s not final (status=%s); skipping",
                     event_id, status)
        return 0
    box = data.get("boxscore") or {}
    teams = box.get("players") or []
    if len(teams) < 2:
        return 0
    # ESPN wraps the teams as a 2-element list; grab team IDs first
    # so each side knows the opponent.
    tid = [_safe_int((t.get("team") or {}).get("id")) for t in teams]
    # Game date — ESPN ships ISO at the header level
    game_date = ""
    for src in ((header.get("competitions") or [{}])[0], header):
        d = src.get("date") or ""
        if isinstance(d, str) and len(d) >= 10:
            game_date = d[:10]
            break
    # Determine which side is home from the competitors block.
    home_side_id = None
    competitors = comp.get("competitors") or []
    for c in competitors:
        if c.get("homeAway") == "home":
            home_side_id = _safe_int((c.get("team") or {}).get("id"))
            break
    rows = 0
    for i, team_block in enumerate(teams):
        opp = tid[1 - i] if len(tid) >= 2 else 0
        is_home = (tid[i] == home_side_id) if home_side_id else (i == 1)
        rows += _ingest_team(team_block, opp, str(event_id),
                             game_date, is_home)
    return rows


def ingest_recent_finals(lookback_days: int = 30) -> dict:
    """Backfill the last ``lookback_days`` of NBA games marked final
    in ``nba_games``. Idempotent — UPSERTs make re-runs safe."""
    from .db import get_conn
    conn = get_conn()
    today = et_today_str()
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT game_id FROM nba_games "
        "WHERE date >= ? AND date <= ? AND status = 'final' "
        "ORDER BY date",
        (cutoff, today),
    ).fetchall()
    game_ids = [r["game_id"] if hasattr(r, "keys") else r[0] for r in rows]
    games = total_rows = skipped = 0
    for gid in game_ids:
        if not gid:
            skipped += 1
            continue
        n = ingest_game(str(gid))
        if n:
            games += 1
            total_rows += n
        else:
            skipped += 1
        time.sleep(_REQUEST_INTERVAL_S)
    logger.info("nba ingest_recent_finals: games=%d rows=%d skipped=%d",
                games, total_rows, skipped)
    return {"games": games, "rows": total_rows, "skipped": skipped}


def ingest_today() -> dict:
    """Quick-sync drain: pull box for any of today's games now final."""
    from .db import get_conn
    conn = get_conn()
    today = et_today_str()
    rows = conn.execute(
        "SELECT game_id FROM nba_games WHERE date = ? AND status = 'final'",
        (today,),
    ).fetchall()
    game_ids = [r["game_id"] if hasattr(r, "keys") else r[0] for r in rows]
    games = total = 0
    for gid in game_ids:
        if not gid:
            continue
        n = ingest_game(str(gid))
        if n:
            games += 1
            total += n
        time.sleep(_REQUEST_INTERVAL_S)
    logger.info("nba ingest_today: games=%d rows=%d", games, total)
    return {"games": games, "rows": total}


__all__ = ["ingest_game", "ingest_recent_finals", "ingest_today"]
