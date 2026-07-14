"""
NHL per-player game-log ingest (Phase 2i-i prep).

Mirrors ``engine.mlb_player_logs`` / ``engine.nba_player_logs`` for
NHL's gamecenter boxscore. Skater + goalie stat lines into
``player_game_logs`` (NHL db) so the player-props settler has data
and the distribution fitter can lock NHL shape choices ahead of the
2i-ii MC extension.

Source: ``api-web.nhle.com/v1/gamecenter/{game_id}/boxscore`` under
``playerByGameStats.{homeTeam,awayTeam}.{forwards,defense,goalies}``.
NHL's API requires a User-Agent header (returns 403 otherwise) — set
in ``_HEADERS`` below.

Stat keys written:

    Skaters: g, a, sog, hits, blocks, toi_min, faceoff_pct
    Goalies: saves, shots_against, ga, toi_min

Skater Points (goals + assists) isn't stored separately — the settler
sums ``g + a`` on read so we don't fork-store derived counts.

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

from ...player_props_db import upsert_game_log
from ..._tz import et_today_str

logger = logging.getLogger(__name__)

NHL_BOXSCORE = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
_HEADERS = {"User-Agent": "Mozilla/5.0 (sports-model-bettor)"}

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
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if 500 <= e.code < 600 and attempt == 1:
                last_err = e
                time.sleep(1.0)
                continue
            logger.warning("NHL HTTP %s for %s", e.code, url)
            return None
        except (urllib.error.URLError, TimeoutError,
                http.client.HTTPException, OSError) as e:
            last_err = e
            if attempt == 1:
                time.sleep(1.0)
                continue
            logger.warning("NHL network error after retry (%s): %s",
                           type(e).__name__, e)
            return None
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("NHL bad JSON: %s", e)
            return None
    if last_err is not None:
        logger.warning("NHL giving up on %s: %s", url, last_err)
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


def _toi_to_minutes(toi: Any) -> int:
    """Convert NHL's "MM:SS" time-on-ice string to whole minutes."""
    if not isinstance(toi, str) or ":" not in toi:
        return _safe_int(toi)
    mm, _, ss = toi.partition(":")
    return _safe_int(mm)


def _build_skater_stats(p: dict) -> dict:
    out = {
        "g":      _safe_int(p.get("goals")),
        "a":      _safe_int(p.get("assists")),
        "sog":    _safe_int(p.get("sog")),
        "hits":   _safe_int(p.get("hits")),
        "blocks": _safe_int(p.get("blockedShots")),
        "toi_min": _toi_to_minutes(p.get("toi")),
    }
    fp = p.get("faceoffWinningPctg")
    if isinstance(fp, (int, float)) and fp > 0:
        out["faceoff_pct"] = float(fp)
    return out


def _build_goalie_stats(p: dict) -> dict:
    out = {
        "saves":         _safe_int(p.get("saves")),
        "shots_against": _safe_int(p.get("shotsAgainst")),
        "ga":            _safe_int(p.get("goalsAgainst")),
        "toi_min":       _toi_to_minutes(p.get("toi")),
    }
    if p.get("starter"):
        out["starter"] = 1
    return out


def _ingest_team(side_block: dict, side_team_id: int, opp_team_id: int,
                 game_id: str, game_date: str, is_home: bool) -> int:
    rows = 0
    for section in ("forwards", "defense"):
        for p in side_block.get(section) or []:
            player_id = _safe_int(p.get("playerId"))
            if not player_id:
                continue
            name = (p.get("name") or {}).get("default", "") or ""
            stats = _build_skater_stats(p)
            if stats.get("toi_min", 0) <= 0:
                # Dressed but didn't play (rare). Skip to keep the
                # distribution fit clean.
                continue
            stats["position"] = section[:1].upper()  # F or D
            upsert_game_log(
                "nhl",
                player_id=player_id, player_name=name,
                game_id=str(game_id), date=game_date,
                stats=stats,
                team_id=side_team_id, opp_team_id=opp_team_id,
                is_home=is_home,
            )
            rows += 1
    for p in side_block.get("goalies") or []:
        player_id = _safe_int(p.get("playerId"))
        if not player_id:
            continue
        name = (p.get("name") or {}).get("default", "") or ""
        stats = _build_goalie_stats(p)
        if stats.get("toi_min", 0) <= 0:
            continue
        stats["position"] = "G"
        upsert_game_log(
            "nhl",
            player_id=player_id, player_name=name,
            game_id=str(game_id), date=game_date,
            stats=stats,
            team_id=side_team_id, opp_team_id=opp_team_id,
            is_home=is_home,
        )
        rows += 1
    return rows


def ingest_game(game_id: int) -> int:
    """Pull one NHL game's boxscore and write per-player rows. Returns
    the row count (0 on fetch failure or non-final game)."""
    url = NHL_BOXSCORE.format(game_id=game_id)
    data = _fetch_json(url)
    if not data:
        return 0
    state = data.get("gameState", "")
    if state not in ("OFF", "FINAL"):
        # NHL marks completed games as OFF (game over, ratings done) or
        # FINAL. Mid-game pulls would write partial counts; let the
        # next sync tick pick this up.
        logger.debug("game %s not final (state=%s); skipping", game_id, state)
        return 0
    pbs = data.get("playerByGameStats") or {}
    home = pbs.get("homeTeam") or {}
    away = pbs.get("awayTeam") or {}
    home_team_id = _safe_int((data.get("homeTeam") or {}).get("id"))
    away_team_id = _safe_int((data.get("awayTeam") or {}).get("id"))
    game_date = (data.get("gameDate") or "")[:10]
    rows = 0
    rows += _ingest_team(home, home_team_id, away_team_id, str(game_id),
                         game_date, is_home=True)
    rows += _ingest_team(away, away_team_id, home_team_id, str(game_id),
                         game_date, is_home=False)
    return rows


def ingest_recent_finals(lookback_days: int = 30) -> dict:
    """Backfill the last ``lookback_days`` of NHL games marked final
    in ``nhl_games``."""
    from .db import get_conn
    conn = get_conn()
    today = et_today_str()
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT game_id FROM nhl_games "
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
        n = ingest_game(int(gid))
        if n:
            games += 1
            total_rows += n
        else:
            skipped += 1
        time.sleep(_REQUEST_INTERVAL_S)
    logger.info("nhl ingest_recent_finals: games=%d rows=%d skipped=%d",
                games, total_rows, skipped)
    return {"games": games, "rows": total_rows, "skipped": skipped}


def ingest_today() -> dict:
    """Quick-sync drain: pull box for any of today's games now final."""
    from .db import get_conn
    conn = get_conn()
    today = et_today_str()
    rows = conn.execute(
        "SELECT game_id FROM nhl_games WHERE date = ? AND status = 'final'",
        (today,),
    ).fetchall()
    game_ids = [r["game_id"] if hasattr(r, "keys") else r[0] for r in rows]
    games = total = 0
    for gid in game_ids:
        if not gid:
            continue
        n = ingest_game(int(gid))
        if n:
            games += 1
            total += n
        time.sleep(_REQUEST_INTERVAL_S)
    logger.info("nhl ingest_today: games=%d rows=%d", games, total)
    return {"games": games, "rows": total}


__all__ = ["ingest_game", "ingest_recent_finals", "ingest_today"]
