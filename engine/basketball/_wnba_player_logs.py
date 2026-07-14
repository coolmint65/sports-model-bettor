"""WNBA per-player game-log ingest.

Direct port of ``engine.sports.nba.player_logs`` to the basketball
framework: same ESPN boxscore shape, same stat parsing, but routes
through ``player_props_db`` with ``sport='wnba'`` (which maps to the
basketball framework's wnba.db, see player_props_db._conn_for) and
queries the framework's ``games`` table for finalized event ids.

Public entry points:
    ingest_game(event_id)        — write one event's box
    ingest_recent_finals(days)   — backfill the last N days of finals
    ingest_today()               — refresh today's finalized slate
"""

from __future__ import annotations

import http.client
import json
import logging
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from typing import Any

from ..player_props_db import upsert_game_log
from .._tz import et_today_str

logger = logging.getLogger(__name__)

ESPN_WNBA_SUMMARY = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/summary"
)
_REQUEST_INTERVAL_S = 0.25


def _fetch_json(url: str, timeout: int = 20) -> dict | None:
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
            logger.warning("ESPN WNBA HTTP %s for %s", e.code, url)
            return None
        except (urllib.error.URLError, TimeoutError,
                http.client.HTTPException, OSError) as e:
            last_err = e
            if attempt == 1:
                time.sleep(1.0)
                continue
            logger.warning("ESPN WNBA network error (%s): %s",
                           type(e).__name__, e)
            return None
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("ESPN WNBA bad JSON: %s", e)
            return None
    if last_err is not None:
        logger.warning("ESPN WNBA giving up on %s: %s", url, last_err)
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
    if not isinstance(s, str) or "-" not in s:
        return _safe_int(s)
    head, _, _tail = s.partition("-")
    return _safe_int(head)


# Mirrors NBA — WNBA's ESPN boxscore uses identical column labels.
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
        if stats.get("min", 0) <= 0:
            continue
        if entry.get("starter"):
            stats["starter"] = 1
        upsert_game_log(
            "wnba",
            player_id=player_id, player_name=player_name,
            game_id=str(game_id), date=game_date,
            stats=stats,
            team_id=team_id, opp_team_id=opp_team_id,
            is_home=is_home,
        )
        rows += 1
    return rows


def ingest_game(event_id: str) -> int:
    url = f"{ESPN_WNBA_SUMMARY}?event={event_id}"
    data = _fetch_json(url)
    if not data:
        return 0
    header = data.get("header") or {}
    comp = (header.get("competitions") or [{}])[0]
    status = ((comp.get("status") or {}).get("type") or {}).get("name", "")
    if status != "STATUS_FINAL":
        return 0
    box = data.get("boxscore") or {}
    teams = box.get("players") or []
    if len(teams) < 2:
        return 0
    tid = [_safe_int((t.get("team") or {}).get("id")) for t in teams]
    game_date = ""
    for src in ((header.get("competitions") or [{}])[0], header):
        d = src.get("date") or ""
        if isinstance(d, str) and len(d) >= 10:
            game_date = d[:10]
            break
    home_side_id = None
    for c in comp.get("competitors") or []:
        if c.get("homeAway") == "home":
            home_side_id = _safe_int((c.get("team") or {}).get("id"))
            break
    rows = 0
    for i, team_block in enumerate(teams):
        opp = tid[1 - i] if len(tid) >= 2 else 0
        is_home = (tid[i] == home_side_id) if home_side_id else (i == 1)
        rows += _ingest_team(team_block, opp, str(event_id), game_date, is_home)
    return rows


def _final_event_ids(date_filter: str | None = None,
                      lookback_days: int | None = None) -> list[str]:
    """ESPN ``external_id``s for final WNBA games in the requested
    window. ``date_filter`` pins a single day; ``lookback_days`` pulls
    a trailing window (used for backfills)."""
    from ._db import get_conn
    conn = get_conn("wnba")
    if date_filter:
        rows = conn.execute(
            "SELECT external_id FROM games "
            "WHERE date = ? AND status = 'final'",
            (date_filter,),
        ).fetchall()
    elif lookback_days is not None:
        cutoff = (datetime.now() - timedelta(days=int(lookback_days))
                  ).strftime("%Y-%m-%d")
        rows = conn.execute(
            "SELECT external_id FROM games "
            "WHERE date >= ? AND status = 'final' ORDER BY date",
            (cutoff,),
        ).fetchall()
    else:
        rows = []
    return [r["external_id"] for r in rows if r["external_id"]]


def ingest_recent_finals(lookback_days: int = 30) -> dict:
    """Backfill the last ``lookback_days`` of finalized WNBA games."""
    event_ids = _final_event_ids(lookback_days=lookback_days)
    games = total_rows = skipped = 0
    for eid in event_ids:
        n = ingest_game(str(eid))
        if n:
            games += 1
            total_rows += n
        else:
            skipped += 1
        time.sleep(_REQUEST_INTERVAL_S)
    logger.info("wnba ingest_recent_finals: games=%d rows=%d skipped=%d",
                games, total_rows, skipped)
    return {"games": games, "rows": total_rows, "skipped": skipped}


def ingest_today() -> dict:
    """Same-day drain: pull box for any of today's WNBA finals."""
    event_ids = _final_event_ids(date_filter=et_today_str())
    games = total = 0
    for eid in event_ids:
        n = ingest_game(str(eid))
        if n:
            games += 1
            total += n
        time.sleep(_REQUEST_INTERVAL_S)
    logger.info("wnba ingest_today: games=%d rows=%d", games, total)
    return {"games": games, "rows": total}
