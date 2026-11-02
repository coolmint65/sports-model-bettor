"""NFL per-player game-log ingest for the Anytime-TD paper model.

Mirrors ``engine.sports.nhl.player_logs`` but pulls from ESPN's NFL
game *summary* endpoint (the parallel of NHL's gamecenter boxscore).
Writes one aggregated row per (player, game) into ``player_game_logs``
(NFL props DB) so the anytime-TD model has recent opportunity + TD
history and the settler has actual observed TDs.

Source
------
    https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={game_id}

``boxscore.players`` is a 2-element list (one block per team). Each
block has ``team`` (abbreviation/id) and ``statistics`` — a list of
stat *categories* (passing / rushing / receiving / kickReturns /
puntReturns / defensive / interceptions / …). Each category carries
a ``keys`` array (stable stat identifiers) aligned index-for-index
with each athlete's ``stats`` array, so we look up values by key name
rather than by fragile hardcoded column indices.

ESPN blocks browser User-Agents on some hosts (403); a curl UA is
used throughout (see MEMORY: espn_ua_403_blocks_browser_uas).

Stat keys written into ``stats_json``:
    rush_att, rush_td, targets, rec, rec_td, pass_att, pass_td,
    ret_td (kick+punt return TDs), def_td (defensive + INT return TDs),
    off_td (rush_td + rec_td — what the model projects),
    td_total (ANY TD — what the anytime-TD market settles on),
    team (abbr), opp (abbr)

``game_id`` is the ESPN event id, identical to
``engine.football._db`` ``games.game_id`` — so the settler can join a
pending pick's game to its final status and box score.

Idempotent: UPSERT keyed on (player_id, game_id).
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

NFL_SUMMARY = ("https://site.api.espn.com/apis/site/v2/sports/"
               "football/nfl/summary?event={game_id}")
_HEADERS = {"User-Agent": "curl/8.4.0"}
_REQUEST_INTERVAL_S = 0.30


def _fetch_json(url: str, timeout: int = 25) -> dict | None:
    """Fetch JSON with one retry on transient network failures."""
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
            logger.warning("NFL summary HTTP %s for %s", e.code, url)
            return None
        except (urllib.error.URLError, TimeoutError,
                http.client.HTTPException, OSError) as e:
            last_err = e
            if attempt == 1:
                time.sleep(1.0)
                continue
            logger.warning("NFL summary network error after retry (%s): %s",
                           type(e).__name__, e)
            return None
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("NFL summary bad JSON: %s", e)
            return None
    if last_err is not None:
        logger.warning("NFL summary giving up on %s: %s", url, last_err)
    return None


def _int(v: Any) -> int:
    """Parse a leading integer from an ESPN stat cell ('12', '1/1',
    '2-9', '-'). Returns 0 when no integer is present."""
    if v is None:
        return 0
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip()
    if not s:
        return 0
    # Leading signed integer (stops at '/', '-', '.', space, etc.)
    num = ""
    for i, ch in enumerate(s):
        if ch.isdigit() or (ch == "-" and i == 0):
            num += ch
        else:
            break
    try:
        return int(num) if num not in ("", "-") else 0
    except ValueError:
        return 0


def _after_slash(v: Any) -> int:
    """Parse the denominator of a 'made/att' cell, e.g. '20/34' -> 34."""
    if v is None:
        return 0
    s = str(v)
    if "/" in s:
        return _int(s.split("/", 1)[1])
    return _int(s)


def _cat_value(cat: dict, athlete_stats: list, key: str,
               parser=_int) -> int:
    """Pull one stat value for an athlete by ESPN ``keys`` name.
    Returns 0 when the key or value is absent."""
    keys = cat.get("keys") or []
    try:
        idx = keys.index(key)
    except ValueError:
        return 0
    if idx >= len(athlete_stats):
        return 0
    return parser(athlete_stats[idx])


def _parse_team_block(block: dict) -> dict[int, dict]:
    """Aggregate one team's box score into ``{player_id: stat_dict}``.
    A player appearing in several categories (a RB who also catches
    passes) is merged into a single row."""
    out: dict[int, dict] = {}
    team = block.get("team") or {}
    team_abbr = (team.get("abbreviation") or "").upper()
    team_id = _int(team.get("id"))

    def rec_for(ath_obj: dict) -> dict | None:
        ath = ath_obj.get("athlete") or {}
        pid = _int(ath.get("id"))
        if not pid:
            return None
        r = out.get(pid)
        if r is None:
            r = out[pid] = {
                "player_id": pid,
                "player_name": ath.get("displayName") or "",
                "jersey": ath.get("jersey") or "",
                "rush_att": 0, "rush_td": 0,
                "targets": 0, "rec": 0, "rec_td": 0,
                "pass_att": 0, "pass_td": 0,
                "ret_td": 0, "def_td": 0,
                "team_id": team_id, "team": team_abbr,
            }
        return r

    for cat in block.get("statistics") or []:
        name = (cat.get("name") or "").lower()
        for ath_obj in cat.get("athletes") or []:
            stats = ath_obj.get("stats") or []
            r = rec_for(ath_obj)
            if r is None:
                continue
            if name == "rushing":
                r["rush_att"] += _cat_value(cat, stats, "rushingAttempts")
                r["rush_td"] += _cat_value(cat, stats, "rushingTouchdowns")
            elif name == "receiving":
                r["rec"] += _cat_value(cat, stats, "receptions")
                r["rec_td"] += _cat_value(cat, stats, "receivingTouchdowns")
                r["targets"] += _cat_value(cat, stats, "receivingTargets")
            elif name == "passing":
                r["pass_att"] += _cat_value(
                    cat, stats, "completions/passingAttempts", _after_slash)
                r["pass_td"] += _cat_value(cat, stats, "passingTouchdowns")
            elif name == "kickreturns":
                r["ret_td"] += _cat_value(cat, stats, "kickReturnTouchdowns")
            elif name == "puntreturns":
                r["ret_td"] += _cat_value(cat, stats, "puntReturnTouchdowns")
            elif name == "defensive":
                r["def_td"] += _cat_value(cat, stats, "defensiveTouchdowns")
            elif name == "interceptions":
                r["def_td"] += _cat_value(cat, stats, "interceptionTouchdowns")
    return out


def _is_final(summary: dict) -> bool:
    """True when ESPN reports the game complete."""
    try:
        comps = (summary.get("header") or {}).get("competitions") or []
        st = ((comps[0].get("status") or {}).get("type") or {}) if comps else {}
        if st.get("completed") is True:
            return True
        return (st.get("state") or "").lower() == "post"
    except Exception:
        return False


def ingest_game(game_id: str, *, force: bool = False) -> int:
    """Pull one NFL game's summary and write per-player rows. Returns
    the row count (0 on fetch failure or non-final game). ``force``
    skips the final-state check (useful for backfill of known finals)."""
    url = NFL_SUMMARY.format(game_id=game_id)
    data = _fetch_json(url)
    if not data:
        return 0
    if not force and not _is_final(data):
        logger.debug("NFL game %s not final; skipping", game_id)
        return 0
    box = (data.get("boxscore") or {}).get("players") or []
    if len(box) < 2:
        return 0
    game_date = ""
    try:
        comps = (data.get("header") or {}).get("competitions") or []
        game_date = (comps[0].get("date") or "")[:10] if comps else ""
    except Exception:
        game_date = ""
    if not game_date:
        game_date = et_today_str()

    parsed = [_parse_team_block(b) for b in box[:2]]
    abbrs = [(box[i].get("team") or {}).get("abbreviation", "") for i in range(2)]
    rows = 0
    for side in (0, 1):
        opp_abbr = (abbrs[1] if side == 0 else abbrs[0]) or ""
        for pid, r in parsed[side].items():
            r["opp"] = opp_abbr.upper()
            r["off_td"] = r["rush_td"] + r["rec_td"]
            r["td_total"] = (r["rush_td"] + r["rec_td"]
                             + r["ret_td"] + r["def_td"])
            # Skip players with no offensive involvement at all — they
            # can never be an anytime-TD candidate and only bloat the
            # log table (whole defenses appear in the 'defensive' cat).
            if (r["rush_att"] == 0 and r["targets"] == 0
                    and r["rec"] == 0 and r["pass_att"] == 0
                    and r["td_total"] == 0):
                continue
            stats = {k: v for k, v in r.items()
                     if k not in ("player_id", "player_name", "team_id")}
            upsert_game_log(
                "nfl",
                player_id=r["player_id"], player_name=r["player_name"],
                game_id=str(game_id), date=game_date,
                stats=stats,
                team_id=r["team_id"] or None,
                is_home=(side == 0),
            )
            rows += 1
    return rows


def _recent_game_ids(lookback_days: int) -> list[tuple[str, str]]:
    """Read candidate (game_id, status) from the football GAME model's
    nfl.db, READ-ONLY. Includes final + live games in the window so a
    just-finished game whose status hasn't flipped yet is still tried
    (ingest_game re-checks ESPN's final state before writing)."""
    from engine.football._db import get_conn
    conn = get_conn("nfl")
    today = et_today_str()
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT game_id, status FROM games "
        "WHERE date >= ? AND date <= ? "
        "  AND COALESCE(status,'') IN ('final','live') "
        "ORDER BY date",
        (cutoff, today),
    ).fetchall()
    return [(str(r["game_id"]), (r["status"] or "")) for r in rows if r["game_id"]]


def ingest_recent_finals(lookback_days: int = 45) -> dict:
    """Backfill player logs for recent NFL finals listed in nfl.db."""
    games = total = skipped = 0
    for gid, status in _recent_game_ids(lookback_days):
        n = ingest_game(gid, force=(status == "final"))
        if n:
            games += 1
            total += n
        else:
            skipped += 1
        time.sleep(_REQUEST_INTERVAL_S)
    logger.info("nfl ingest_recent_finals: games=%d rows=%d skipped=%d",
                games, total, skipped)
    return {"games": games, "rows": total, "skipped": skipped}


def ingest_today() -> dict:
    """Drain any of today's NFL games that are now final."""
    return ingest_recent_finals(lookback_days=2)


__all__ = ["ingest_game", "ingest_recent_finals", "ingest_today"]
