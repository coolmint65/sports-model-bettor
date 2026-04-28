"""
MLB Prediction Engine API.

Serves MLB schedule, team data, and game predictions.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import json
import re
import time
import logging
import urllib.request
from collections import defaultdict, deque
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from engine.db import (
    get_conn, get_all_teams, get_team_by_id, get_team_by_abbr,
    get_today_games, get_team_record, get_pitcher_season,
    get_bullpen, get_recent_games,
)
from engine.mlb_predict import predict_matchup

logger = logging.getLogger(__name__)

SEASON = datetime.now().year
SERVER_STARTED_AT = time.time()

app = FastAPI(title="MLB Prediction Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Rate limiting ────────────────────────────────────────────
# Sliding-window per-IP rate limit. The frontend is single-user / local,
# so the cap is generous; the goal is to catch a runaway client (infinite
# refetch loop, accidental polling) before it melts the backend or burns
# Odds API credits, not to enforce a multi-tenant quota.
RATE_LIMIT_WINDOW_SEC = 60
RATE_LIMIT_MAX_REQUESTS = 600   # 10 req/sec sustained per IP
_rate_buckets: dict[str, deque] = defaultdict(deque)
# Endpoints exempt from the limit (cheap reads, monitoring).
_RATE_LIMIT_EXEMPT_PREFIXES = ("/health", "/docs", "/openapi.json", "/redoc")


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    path = request.url.path
    if not any(path.startswith(p) for p in _RATE_LIMIT_EXEMPT_PREFIXES):
        client = (request.client.host if request.client else "unknown") or "unknown"
        bucket = _rate_buckets[client]
        now = time.time()
        cutoff = now - RATE_LIMIT_WINDOW_SEC
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        if len(bucket) >= RATE_LIMIT_MAX_REQUESTS:
            retry_after = int(bucket[0] + RATE_LIMIT_WINDOW_SEC - now) + 1
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many requests. Slow down."},
                headers={"Retry-After": str(max(retry_after, 1))},
            )
        bucket.append(now)
    return await call_next(request)

# ── ESPN integration for live scoreboard ────────────────────

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"
_scoreboard_cache: dict[str, tuple[float, list]] = {}
CACHE_TTL = 120
MAX_CACHE_ENTRIES = 50  # Prevent unbounded memory growth


def _odds_from_scoreboard_cache(home_abbr: str, away_abbr: str,
                                sport: str = "mlb") -> dict:
    """Look up odds for a matchup in any cached scoreboard payload.

    /api/predict was triggering a full Odds API fetch on every request
    even though /api/scoreboard had already attached the same odds to
    each game in its cache. Try the cache first; the caller falls back
    to a fresh fetch on miss. Tries alternate abbreviation forms
    (ARI/AZ, UTA/UTH, BKN/BRK, etc.) so an ESPN-keyed scoreboard cache
    can still answer an Odds-API-keyed query.

    Walks both _scoreboard_cache (MLB + NHL both use it) and
    _nba_scoreboard_cache so all three sports benefit from the skip.
    Matters especially at month-end when the Odds API key is near its
    credit limit -- duplicate fetches from predict endpoints were the
    difference between a 20K/mo plan lasting 30 days vs burning out.
    """
    from engine.abbr import alt_abbr
    candidates = {(home_abbr, away_abbr)}
    h_alt = alt_abbr(home_abbr, sport)
    a_alt = alt_abbr(away_abbr, sport)
    candidates.update({(h_alt, away_abbr), (home_abbr, a_alt), (h_alt, a_alt)})

    caches = [_scoreboard_cache]
    # _nba_scoreboard_cache is defined further down the module; guard
    # with globals() so the order doesn't matter.
    nba_cache = globals().get("_nba_scoreboard_cache")
    if nba_cache:
        caches.append(nba_cache)

    for cache in caches:
        for _ts, games in cache.values():
            for g in games:
                gh = g.get("home", {}).get("abbreviation", "")
                ga = g.get("away", {}).get("abbreviation", "")
                if (gh, ga) in candidates and g.get("odds"):
                    return g["odds"]
    return {}


def _fetch_espn_json(url: str) -> dict | None:
    for attempt in range(2):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            # Both attempts log so an ESPN outage is visible. Without
            # this the only symptom is empty scoreboards / fallbacks
            # that look like "the model thinks no games tonight".
            level = logging.DEBUG if attempt == 0 else logging.WARNING
            logger.log(level, "ESPN fetch failed (attempt %d) for %s: %s",
                       attempt + 1, url, e)
            if attempt == 0:
                time.sleep(1)
    return None


# ── Endpoints ───────────────────────────────────────────────

@app.get("/api/model-overrides/{sport}")
def api_model_overrides(sport: str, include_expired: bool = False):
    """List active runtime overrides for a sport.

    These are written by ``engine.train --apply`` when a direction
    allow-flag's data is statistically below break-even. The UI can
    surface a badge on suppressed markets so the user knows their
    rec is being silenced by data, not by a config edit.
    """
    from engine.model_overrides import list_overrides
    if sport not in ("mlb", "nhl", "nba"):
        raise HTTPException(status_code=400, detail=f"Unknown sport: {sport}")
    return {"sport": sport, "overrides": list_overrides(sport, include_expired)}


@app.delete("/api/model-overrides/{sport}/{flag}")
def api_model_override_revert(sport: str, flag: str):
    """Manually clear an override (e.g., after investigating)."""
    from engine.model_overrides import revert_override
    if sport not in ("mlb", "nhl", "nba"):
        raise HTTPException(status_code=400, detail=f"Unknown sport: {sport}")
    cleared = revert_override(sport, flag)
    return {"cleared": cleared, "sport": sport, "flag": flag}


@app.get("/health")
def health():
    """Health check covering DB connectivity, sync recency, Odds API
    credit balance, and per-sport row counts. Designed to be polled by
    Task Scheduler / external monitors -- a 'degraded' status indicates
    a sport's data is stale enough that picks may be wrong."""
    from engine.db import DB_PATH as MLB_DB

    payload: dict = {
        "status": "ok",
        "uptime_seconds": int(time.time() - SERVER_STARTED_AT),
        "started_at": datetime.fromtimestamp(SERVER_STARTED_AT, timezone.utc).isoformat(),
        "sports": {},
    }
    degraded_reasons: list[str] = []

    def _sport_status(label: str, db_path, picks_table: str | None,
                       conn_factory) -> dict:
        try:
            conn = conn_factory()
            teams = conn.execute("SELECT COUNT(*) AS c FROM teams").fetchone()["c"]
            last_game_row = conn.execute(
                "SELECT MAX(updated_at) AS t FROM games"
            ).fetchone()
            last_sync = last_game_row["t"] if last_game_row else None
            picks_count = 0
            if picks_table:
                try:
                    picks_count = conn.execute(
                        f"SELECT COUNT(*) AS c FROM {picks_table}"
                    ).fetchone()["c"]
                except Exception:
                    picks_count = 0
            db_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
            stale_hours = None
            if last_sync:
                try:
                    last_dt = datetime.fromisoformat(last_sync.replace(" ", "T"))
                    stale_hours = (datetime.now() - last_dt).total_seconds() / 3600
                    if stale_hours > 24:
                        degraded_reasons.append(
                            f"{label} last sync was {stale_hours:.1f}h ago"
                        )
                except Exception:
                    stale_hours = None
            return {
                "ok": True,
                "db_path": str(db_path),
                "db_size_mb": round(db_size / 1_000_000, 2),
                "teams": teams,
                "picks": picks_count,
                "last_sync_at": last_sync,
                "stale_hours": round(stale_hours, 2) if stale_hours is not None else None,
            }
        except Exception as e:
            degraded_reasons.append(f"{label}: {e}")
            return {"ok": False, "error": str(e)}

    payload["sports"]["mlb"] = _sport_status("mlb", MLB_DB, "picks", get_conn)

    # NHL + NBA are optional -- only report if their modules import cleanly
    try:
        from engine.nhl_db import get_conn as _nhl_conn, DB_PATH as NHL_DB
        payload["sports"]["nhl"] = _sport_status("nhl", NHL_DB, "nhl_picks", _nhl_conn)
    except Exception:
        pass
    try:
        from engine.nba_db import get_conn as _nba_conn, DB_PATH as NBA_DB
        payload["sports"]["nba"] = _sport_status("nba", NBA_DB, "nba_picks", _nba_conn)
    except Exception:
        pass

    # Odds API credit balance (populated by scrapers.odds_api on each fetch)
    try:
        from scrapers.odds_api import get_credits_status
        credits = get_credits_status()
        payload["odds_api"] = credits
        if credits.get("remaining") is not None and credits["remaining"] < 1000:
            degraded_reasons.append(
                f"Odds API credits low: {credits['remaining']} remaining"
            )
    except Exception as e:
        payload["odds_api"] = {"error": str(e)}

    if degraded_reasons:
        payload["status"] = "degraded"
        payload["degraded_reasons"] = degraded_reasons

    return payload


@app.get("/api/teams")
def api_teams():
    """Return all 30 MLB teams."""
    teams = get_all_teams()
    result = []
    for t in teams:
        record = get_team_record(t["mlb_id"], SEASON)
        result.append({
            "id": t["mlb_id"],
            "name": t["name"],
            "abbreviation": t["abbreviation"],
            "city": t.get("city", ""),
            "venue": t.get("venue", ""),
            "league": t.get("league", ""),
            "division": t.get("division", ""),
            "record": f"{record['wins']}-{record['losses']}" if record else "",
            "streak": record.get("streak", "") if record else "",
            "last_10": f"{record.get('last_10_wins', 0)}-{record.get('last_10_losses', 0)}" if record else "",
            "run_diff": record.get("run_diff", 0) if record else 0,
        })
    return result


@app.get("/api/teams/{team_id}")
def api_team_detail(team_id: int):
    """Return full team data with stats."""
    team = get_team_by_id(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")

    record = get_team_record(team_id, SEASON) or {}
    bp = get_bullpen(team_id, SEASON) or {}
    recent = get_recent_games(team_id, 10)

    return {
        "team": team,
        "record": record,
        "bullpen": bp,
        "recent_games": recent,
    }


def _get_scoreboard(date: str = "") -> list[dict]:
    """Core scoreboard logic - reusable by other endpoints."""
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    espn_date = target_date.replace("-", "")

    cache_key = f"mlb:{espn_date}"
    now = time.time()
    if cache_key in _scoreboard_cache:
        ts, cached = _scoreboard_cache[cache_key]
        if now - ts < CACHE_TTL:
            return cached

    games = []

    # Primary: ESPN
    url = f"{ESPN_BASE}/baseball/mlb/scoreboard?dates={espn_date}"
    logger.info("Fetching scoreboard: %s", url)
    espn_data = _fetch_espn_json(url)

    games = []
    if espn_data:
        events = espn_data.get("events", [])
        logger.info("ESPN returned %d events for date %s", len(events), espn_date)
        # Debug: log first team's keys to see logo format
        if events:
            try:
                first_comp = events[0].get("competitions", [{}])[0]
                first_team = first_comp.get("competitors", [{}])[0].get("team", {})
                logger.info("ESPN team keys: %s", list(first_team.keys()))
            except Exception as e:
                # Logging-only debug; never block scoreboard. But if the
                # ESPN payload shape changes (key rename, missing
                # competitors), log it so the next on-call eye knows
                # which side broke. Without this the only symptom would
                # be quietly-wrong team logos.
                logger.debug("ESPN team-keys debug log failed: %s", e)
        games = _parse_espn_scoreboard(espn_data)
    else:
        logger.warning("ESPN returned no data for %s", url)

    # Yesterday-still-live carryover (default-today path only).
    # Late MLB games (West Coast 10:30pm PT first pitch) cross midnight
    # UTC and drop off today's slate while still in the 5th-6th inning.
    if date == "":
        from datetime import timedelta as _td
        yest = (datetime.now() - _td(days=1)).strftime("%Y%m%d")
        try:
            yest_data = _fetch_espn_json(
                f"{ESPN_BASE}/baseball/mlb/scoreboard?dates={yest}")
            if yest_data:
                yest_games = _parse_espn_scoreboard(yest_data)
                seen_ids = {g.get("id") for g in games}
                for g in yest_games:
                    state = (g.get("status") or {}).get("state", "")
                    if state == "in" and g.get("id") not in seen_ids:
                        games.append(g)
                        logger.info("MLB: carried over live game from "
                                    "yesterday: %s", g.get("id"))
        except Exception as e:
            logger.debug("MLB yesterday-live carryover failed: %s", e)

    # ESPN fallback: try without date param
    if not games and date == "":
        fallback_url = f"{ESPN_BASE}/baseball/mlb/scoreboard"
        logger.info("ESPN fallback (no date): %s", fallback_url)
        espn_data = _fetch_espn_json(fallback_url)
        if espn_data:
            events = espn_data.get("events", [])
            logger.info("Fallback returned %d events", len(events))
            games = _parse_espn_scoreboard(espn_data)

    # If still no games, try tomorrow
    if not games and date == "":
        from datetime import timedelta as _td
        tomorrow = (datetime.now() + _td(days=1)).strftime("%Y%m%d")
        espn_data = _fetch_espn_json(f"{ESPN_BASE}/baseball/mlb/scoreboard?dates={tomorrow}")
        if espn_data:
            games = _parse_espn_scoreboard(espn_data)

    # Secondary fallback: MLB Stats API (if ESPN is completely down)
    if not games:
        logger.warning("ESPN unavailable, falling back to MLB Stats API")
        games = _mlb_api_scoreboard(target_date)

    # Enrich with our DB data
    games = _enrich_games(games, target_date)

    # Fetch real odds. Hard Rock is the authoritative book for this
    # model — its alt-spread/team-total markets are what derivative
    # picks resolve against and its 5-juice convention is what the
    # tracker prices closing odds in. The Odds API was first here, but
    # its -110/-110 default juice quietly displaced HR's actual prices
    # on every game it covered. HR-first means cards, picks, and the
    # CLV column all read from one source.
    odds_matched = 0
    from engine.picks import match_odds as _match
    try:
        from scrapers.hardrock_odds import fetch_mlb as _hr_mlb
        logger.info("Calling Hard Rock...")
        hr_odds = _hr_mlb() or {}
        logger.info("Hard Rock returned %d games", len(hr_odds))
        if hr_odds:
            for game in games:
                h_abbr = game["home"].get("abbreviation", "")
                a_abbr = game["away"].get("abbreviation", "")
                # Pass the game's start time so doubleheader games route
                # to the right HR odds bucket via the AWAY@HOME@HHMM
                # suffixed key. Without this both COL/NYM DH games
                # would share Game 1's odds blob.
                matched_odds = _match(h_abbr, a_abbr, hr_odds,
                                      start_time=game.get("date"))
                if matched_odds:
                    game["odds"] = matched_odds
                    odds_matched += 1
            logger.info("Hard Rock: matched %d/%d games", odds_matched, len(games))
    except Exception as e:
        logger.warning("Hard Rock fetch failed: %s", e, exc_info=True)

    # Fill any gaps from The Odds API
    if odds_matched < len(games):
        try:
            from scrapers.odds_api import fetch_odds
            api_odds = fetch_odds()
            if api_odds:
                for game in games:
                    if game.get("odds"):
                        continue
                    h_abbr = game["home"].get("abbreviation", "")
                    a_abbr = game["away"].get("abbreviation", "")
                    matched_odds = _match(h_abbr, a_abbr, api_odds)
                    if matched_odds:
                        game["odds"] = matched_odds
                        odds_matched += 1
                logger.info("Odds API: matched %d/%d games", odds_matched, len(games))
        except Exception as e:
            logger.warning("Odds API fallback failed: %s", e, exc_info=True)

    # Final fallback: ESPN per-game odds for games still without odds
    if odds_matched < len(games):
        try:
            from scrapers.espn_odds import fetch_all_game_odds
            games_needing_odds = [g for g in games
                                  if not g.get("odds") or not g["odds"].get("home_ml")]
            if games_needing_odds:
                espn_odds = fetch_all_game_odds(games_needing_odds)
                for game in games:
                    gid = game.get("id")
                    if gid and gid in espn_odds and (
                        not game.get("odds") or not game["odds"].get("home_ml")):
                        game["odds"] = espn_odds[gid]
                        game["odds"]["provider"] = "ESPN"
        except Exception as e:
            logger.warning("ESPN per-game odds failed: %s", e)

    # Track line movement for each game
    try:
        from engine.line_movement import get_line_movement, track_opening_odds
        for game in games:
            if not game.get("odds"):
                continue
            h = game["home"].get("abbreviation", "")
            a = game["away"].get("abbreviation", "")
            gdate = (game.get("date", "") or "")[:10]
            if not (h and a and gdate):
                continue
            key = f"{gdate}_{a}@{h}"
            movement = get_line_movement("mlb", key, game["odds"])
            if movement:
                game["line_movement"] = movement
            else:
                track_opening_odds("mlb", key, game["odds"])
    except Exception as e:
        logger.debug("MLB line movement tracking failed: %s", e)

    # Persist today's odds (including NRFI + F5 markets) to the odds
    # table so backtests can evaluate against real historical prices
    # instead of synthetic -120/-110 defaults.
    try:
        from engine.odds_history import store_mlb_odds
        store_mlb_odds([g for g in games if g.get("odds")])
    except Exception as e:
        logger.debug("MLB odds persistence failed: %s", e)

    _scoreboard_cache[cache_key] = (now, games)
    # Evict oldest entries if cache grows too large
    if len(_scoreboard_cache) > MAX_CACHE_ENTRIES:
        oldest = min(_scoreboard_cache, key=lambda k: _scoreboard_cache[k][0])
        del _scoreboard_cache[oldest]
    return games


@app.get("/api/scoreboard")
def api_scoreboard(date: str = Query(default="")):
    """Return today's MLB games."""
    return _get_scoreboard(date)


def _mlb_api_scoreboard(date: str) -> list[dict]:
    """Fallback scoreboard using MLB Stats API when ESPN is down."""
    MLB_API = "https://statsapi.mlb.com/api/v1"
    url = (f"{MLB_API}/schedule?sportId=1&date={date}"
           f"&hydrate=probablePitcher,linescore,team")
    data = _fetch_espn_json(url)  # Reuse the fetch helper
    if not data:
        return []

    games = []
    for date_entry in data.get("dates", []):
        for g in date_entry.get("games", []):
            status_code = g.get("status", {}).get("abstractGameCode", "")
            home = g.get("teams", {}).get("home", {})
            away = g.get("teams", {}).get("away", {})

            home_team = home.get("team", {})
            away_team = away.get("team", {})

            home_pp = home.get("probablePitcher", {})
            away_pp = away.get("probablePitcher", {})

            state = {"P": "pre", "S": "pre", "L": "in", "I": "in", "F": "post"}.get(status_code, "pre")

            game = {
                "id": str(g.get("gamePk", "")),
                "game_pk": g.get("gamePk", 0),
                "date": g.get("gameDate", ""),
                "name": f"{away_team.get('name', '')} @ {home_team.get('name', '')}",
                "short_name": f"{away_team.get('abbreviation', '')} @ {home_team.get('abbreviation', '')}",
                "home": {
                    "espn_id": str(home_team.get("id", "")),
                    "name": home_team.get("name", ""),
                    "abbreviation": home_team.get("abbreviation", ""),
                    "score": str(home.get("score", "0")),
                    "record": f"{home.get('leagueRecord', {}).get('wins', 0)}-{home.get('leagueRecord', {}).get('losses', 0)}",
                    "logo": "",
                    "winner": False,
                },
                "away": {
                    "espn_id": str(away_team.get("id", "")),
                    "name": away_team.get("name", ""),
                    "abbreviation": away_team.get("abbreviation", ""),
                    "score": str(away.get("score", "0")),
                    "record": f"{away.get('leagueRecord', {}).get('wins', 0)}-{away.get('leagueRecord', {}).get('losses', 0)}",
                    "logo": "",
                    "winner": False,
                },
                "home_pitcher": {
                    "name": home_pp.get("fullName", "TBD"),
                    "id": home_pp.get("id"),
                } if home_pp else None,
                "away_pitcher": {
                    "name": away_pp.get("fullName", "TBD"),
                    "id": away_pp.get("id"),
                } if away_pp else None,
                "status": {
                    "state": state,
                    "detail": g.get("status", {}).get("detailedState", ""),
                    "description": "",
                    "completed": state == "post",
                    "inning": g.get("linescore", {}).get("currentInning", 0),
                    "inning_half": g.get("linescore", {}).get("inningHalf", ""),
                },
                "venue": g.get("venue", {}).get("name", ""),
                "broadcast": "",
                "odds": None,
            }
            games.append(game)

    logger.info("MLB API fallback returned %d games", len(games))
    return games


_ESPN_EVENT_ID_RE = re.compile(r"(?:^|~)e:(\d+)(?:$|~)")


def _safe_game_pk(uid: str, event_id: str) -> int:
    """Extract a numeric game PK from ESPN uid or event id.

    Supported uid formats (from ESPN docs as of 2024):
        s:1~l:10~e:401814725   -- standard scoreboard event
        e:401814725             -- bare event-id form

    Uses an explicit regex anchored on '~e:<digits>' so a partial match
    on a different segment (e.g. an opaque token containing 'e:') can't
    accidentally hijack the result. Falls back to the event_id field if
    the uid has no e:-segment, and returns 0 only when both are empty
    or non-numeric.
    """
    if uid:
        m = _ESPN_EVENT_ID_RE.search(uid)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass
    try:
        return int(event_id)
    except (ValueError, TypeError):
        if uid or event_id:
            logger.debug("_safe_game_pk: could not parse uid=%r event_id=%r", uid, event_id)
        return 0


def _parse_espn_scoreboard(data: dict) -> list[dict]:
    """Parse ESPN scoreboard into clean game objects."""
    events = data.get("events", [])
    games = []

    for event in events:
        comps = event.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue

        status = comp.get("status", {})
        status_type = status.get("type", {})

        home_team = None
        away_team = None
        for c in competitors:
            team = c.get("team", {})
            raw_score = c.get("score", "0")
            if isinstance(raw_score, dict):
                score = raw_score.get("displayValue", raw_score.get("value", "0"))
            else:
                score = str(raw_score)

            entry = {
                "espn_id": team.get("id", ""),
                "name": team.get("displayName", team.get("name", "")),
                "abbreviation": team.get("abbreviation", ""),
                "score": score,
                "record": (c.get("records", [{}])[0].get("summary", "")
                          if c.get("records") else ""),
                "logo": "",
                "winner": c.get("winner", False),
            }
            # ESPN sends logos in different formats
            logo = team.get("logo", "")
            if isinstance(logo, str) and logo:
                entry["logo"] = logo
            elif isinstance(logo, dict):
                entry["logo"] = logo.get("href", "")
            else:
                logos = team.get("logos", [])
                if logos and isinstance(logos, list):
                    first = logos[0]
                    if isinstance(first, str):
                        entry["logo"] = first
                    elif isinstance(first, dict):
                        entry["logo"] = first.get("href", "")

            if c.get("homeAway") == "home":
                home_team = entry
            else:
                away_team = entry

        if not home_team or not away_team:
            continue

        # Probable pitchers from ESPN
        home_pp = None
        away_pp = None
        for c in competitors:
            pp = c.get("probables", [])
            if pp:
                pitcher = pp[0].get("athlete", {})
                pitcher_info = {
                    "name": pitcher.get("displayName", "TBD"),
                    "id": pitcher.get("id"),
                    "headshot": pitcher.get("headshot", "") if isinstance(pitcher.get("headshot"), str) else pitcher.get("headshot", {}).get("href", ""),
                    "stats": [],
                }
                # Extract pitcher stats from ESPN
                for s in pp[0].get("statistics", []):
                    pitcher_info["stats"].append({
                        "name": s.get("abbreviation", s.get("name", "")),
                        "value": s.get("displayValue", ""),
                    })
                if c.get("homeAway") == "home":
                    home_pp = pitcher_info
                else:
                    away_pp = pitcher_info

        game = {
            "id": event.get("id", ""),
            "game_pk": _safe_game_pk(event.get("uid", ""), event.get("id", "")),
            "date": event.get("date", ""),
            "name": event.get("name", ""),
            "short_name": event.get("shortName", ""),
            "home": home_team,
            "away": away_team,
            "home_pitcher": home_pp,
            "away_pitcher": away_pp,
            "status": {
                "state": status_type.get("state", "pre"),
                "detail": status_type.get("shortDetail",
                          status_type.get("detail", "")),
                "description": status_type.get("description", ""),
                "completed": status_type.get("completed", False),
                "inning": status.get("period", 0),
                "inning_half": status.get("type", {}).get("description", ""),
            },
            "venue": comp.get("venue", {}).get("fullName", ""),
            "broadcast": "",
            "odds": None,
        }

        # Broadcast
        broadcasts = comp.get("broadcasts", [])
        if broadcasts:
            names = []
            for b in broadcasts:
                for n in b.get("names", []):
                    names.append(n)
            game["broadcast"] = ", ".join(names[:2])

        # Odds
        odds = comp.get("odds", [])
        if odds:
            o = odds[0]
            home_odds = o.get("homeTeamOdds", {}) or {}
            away_odds = o.get("awayTeamOdds", {}) or {}

            # Log raw odds structure on first game for debugging
            if not games:
                logger.info("ESPN odds top-level keys: %s", list(o.keys()))
                logger.info("ESPN homeTeamOdds: %s", dict(home_odds))
                logger.info("ESPN awayTeamOdds: %s", dict(away_odds))
                # Log any additional odds entries (some have spread/total as separate items)
                if len(odds) > 1:
                    logger.info("ESPN odds[1]: %s", odds[1])

            game["odds"] = {
                "spread": o.get("details", ""),
                "over_under": o.get("overUnder"),
                # Moneyline
                "home_ml": home_odds.get("moneyLine"),
                "away_ml": away_odds.get("moneyLine"),
                # Run line (spread odds)
                "home_spread": home_odds.get("spreadOdds") or home_odds.get("spread"),
                "away_spread": away_odds.get("spreadOdds") or away_odds.get("spread"),
                "home_spread_line": home_odds.get("spreadLine") or home_odds.get("line"),
                "away_spread_line": away_odds.get("spreadLine") or away_odds.get("line"),
                # Over/Under odds
                "over_odds": o.get("overOdds") or home_odds.get("overOdds"),
                "under_odds": o.get("underOdds") or away_odds.get("underOdds"),
            }

        games.append(game)

    return games


# ESPN uses different abbreviations than MLB Stats API for some teams
# Maps ESPN abbreviation → DB abbreviation (bidirectional)
_ESPN_ABBR_MAP = {
    "CHW": "CWS",   # White Sox - ESPN sometimes uses CHW
    "CWS": "CWS",
    "WSH": "WSH",   # Nationals
    "WAS": "WSH",
    "ARI": "AZ",    # Diamondbacks - ESPN uses ARI, DB has AZ
    "AZ": "AZ",
    "SF": "SF",      # Giants
    "SD": "SD",      # Padres
    "TB": "TB",      # Rays
    "KC": "KC",      # Royals
    "OAK": "ATH",   # Athletics - DB has ATH
    "ATH": "ATH",
}


def _resolve_abbr(espn_abbr: str):
    """Try to find a team by ESPN abbreviation, with fallback mapping."""
    team = get_team_by_abbr(espn_abbr)
    if team:
        return team
    # Try mapped abbreviation
    mapped = _ESPN_ABBR_MAP.get(espn_abbr)
    if mapped and mapped != espn_abbr:
        team = get_team_by_abbr(mapped)
        if team:
            return team
    # Try by team name substring
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM teams WHERE name LIKE ? LIMIT 1",
        (f"%{espn_abbr}%",)
    ).fetchone()
    return dict(row) if row else None


# Abbreviation aliases live in engine.abbr (single canonical source).
# Local thunk preserves the existing _alt_abbr() call sites.
from engine.abbr import alt_abbr as _engine_alt_abbr


def _alt_abbr(abbr: str) -> str:
    return _engine_alt_abbr(abbr, "mlb")


def _enrich_games(games: list[dict], date: str) -> list[dict]:
    """Enrich ESPN game data with our DB records/stats."""
    for game in games:
        home_abbr = game["home"].get("abbreviation", "")
        away_abbr = game["away"].get("abbreviation", "")

        home_db = _resolve_abbr(home_abbr)
        away_db = _resolve_abbr(away_abbr)

        if home_db:
            game["home"]["team_id"] = home_db["mlb_id"]
            rec = get_team_record(home_db["mlb_id"], SEASON)
            if rec:
                game["home"]["db_record"] = f"{rec['wins']}-{rec['losses']}"
                game["home"]["streak"] = rec.get("streak", "")
                game["home"]["last_10"] = f"{rec.get('last_10_wins', 0)}-{rec.get('last_10_losses', 0)}"

        if away_db:
            game["away"]["team_id"] = away_db["mlb_id"]
            rec = get_team_record(away_db["mlb_id"], SEASON)
            if rec:
                game["away"]["db_record"] = f"{rec['wins']}-{rec['losses']}"
                game["away"]["streak"] = rec.get("streak", "")
                game["away"]["last_10"] = f"{rec.get('last_10_wins', 0)}-{rec.get('last_10_losses', 0)}"

    return games


# Lock semantics: a pick locks ONCE THE GAME STARTS, not before.
# Pre-game, the card best_pick stays current — if HR moves the line
# enough that the model picks a different bet, the card swaps to the
# new pick (and the tracker pending row swaps too via
# refresh_pending_for_today). What we want to prevent is mid-game
# noise re-ranking the headline after the user could have placed
# their bet. So lock fires when game time has passed (game started),
# never before.
def _is_game_locked(game_iso_time: str | None) -> bool:
    """True iff the game has already started. Pre-game returns False
    so the model can keep updating the card / tracker pending in
    response to line moves. Once `gt < now`, lock fires and downstream
    code (best-bets card, tracker refresh) skips swap logic."""
    if not game_iso_time:
        return False
    try:
        s = game_iso_time.rstrip("Z")
        gt = datetime.fromisoformat(s)
        from datetime import timezone as _tz
        if gt.tzinfo is None:
            gt = gt.replace(tzinfo=_tz.utc)
        return gt < datetime.now(_tz.utc)
    except Exception:
        return False


def _is_game_imminent(game_iso_time: str | None,
                      window_s: int = 3600) -> bool:
    """True iff game starts within `window_s` seconds (default 1hr)
    and hasn't started yet. Used to bypass the 30-min pred cache for
    soon-to-tip games — when seconds matter (line moves, late lineup
    scratches), we want the freshest possible prediction every call."""
    if not game_iso_time:
        return False
    try:
        s = game_iso_time.rstrip("Z")
        gt = datetime.fromisoformat(s)
        from datetime import timezone as _tz
        if gt.tzinfo is None:
            gt = gt.replace(tzinfo=_tz.utc)
        delta = (gt - datetime.now(_tz.utc)).total_seconds()
        return 0 < delta <= window_s
    except Exception:
        return False


# Phase 1 derivative bet types per sport. Used to:
#   1. Top up `all_picks` so derivatives surface on cards even when their
#      reliability-adjusted EV pushes them out of the top 4.
#   2. Carve a separate `derivative_picks` field for the dedicated UI
#      panel — keeps high-conviction main markets uncluttered while
#      letting users browse + paper-bet the derivative shelf.
# 1st INN is intentionally NOT in these sets — it has its own dedicated
# FirstInningPicks card and is treated as a core market, not Phase 1.
_MLB_DERIV_TYPES: set[str] = {
    "Team Total", "F5 Team Total", "Inning Total", "Inning BTS",
    "1st Inn Winner", "F5 Winner", "Total O/E", "Extra Innings",
}
_NHL_DERIV_TYPES: set[str] = {
    "Team Total", "Period Total", "Period BTS", "Period DNB",
    "Total O/E", "Overtime", "BTS",
}
_NBA_DERIV_TYPES: set[str] = {
    "Q1 Team Total", "Q1 Total O/E",
}

# Player-prop bet-type sets (Phase 2f-i foundation).
# Filtering pattern matches _*_DERIV_TYPES — anything in these sets is
# a player prop, routed to the player_props_picks table + tracker UI
# instead of the core/derivative trackers. Predictors that emit them
# land in 2g (MLB), 2h (NBA), 2i (NHL).
_MLB_PROP_TYPES: set[str] = {
    "Pitcher Ks O/U", "Pitcher Walks O/U", "Pitcher Outs Recorded",
    "Pitcher Earned Runs", "Pitcher Hits Allowed",
    "Batter HR", "Batter Hits O/U", "Batter TB", "Batter RBI",
    "Batter Runs Scored", "Batter Stolen Bases", "Batter Strikeouts",
    "Batter Walks",
}
_NBA_PROP_TYPES: set[str] = {
    "Player Points", "Player Rebounds", "Player Assists",
    "Player PRA", "Player 3PM", "Player Steals", "Player Blocks",
    "Player Turnovers", "Player FT Made",
}
_NHL_PROP_TYPES: set[str] = {
    "Skater SOG", "Skater Points", "Skater Goals", "Skater Assists",
    "Skater Hits", "Skater Blocks",
    "Goalie Saves", "Goalie Goals Against",
}


class PredictRequest(BaseModel):
    home_team_id: int
    away_team_id: int
    home_pitcher_id: int | None = None
    away_pitcher_id: int | None = None
    venue: str | None = None


# Per-game prediction cache. Dashboard load (/api/best-bets) runs the
# full MC+GBM chain across ~15 games, which is multi-minute cold. Repeat
# refreshes within the TTL hit memory and return instantly. Bumped from
# 5 min to 30 min on 2026-04-24 — pre-derivative the only stochastic
# input was MC seed (now deterministic, see _mc_seed below) and odds
# (which we fetch fresh per call). 30 min lets a slate of predictions
# stay stable across natural reload cadence (user opens app, refreshes
# a few times) while still picking up the next sync's lineup snapshot
# for the late-day check.
import threading as _threading
import hashlib as _hashlib
_PRED_CACHE: dict = {}
_PRED_CACHE_LOCK = _threading.Lock()
_PRED_CACHE_TTL_S = 1800


def _mc_seed(*parts) -> int:
    """Derive a deterministic 32-bit seed from a tuple of identifiers
    (sport, date, team IDs, pitcher IDs, etc.). Two predictions for the
    same matchup on the same date get the same seed → same MC output →
    stable picks across reloads. Different matchups → different seeds →
    no cross-game correlation. md5 (not Python hash()) so the seed is
    stable across processes; ``hash()`` is randomized per interpreter
    boot when PYTHONHASHSEED isn't pinned."""
    payload = "|".join(str(p) if p is not None else "" for p in parts)
    digest = _hashlib.md5(payload.encode()).digest()
    return int.from_bytes(digest[:4], "big")

# ── Picks store ──────────────────────────────────────────────
# Single source of truth for today's picks. Best-bets writes here;
# predict endpoint + tracker read from here. Keyed by
# "{sport}:{away}@{home}" → {"picks": [...], "odds": {...}, "best_pick": {...}}.
# In-memory for fast reads, but mirrored to each sport's tracker DB
# (engine.picks_cache) so a server restart doesn't lose the day's picks
# and force best-bets to run again before the card is consistent.
_PICKS_STORE: dict[str, dict] = {}
_PICKS_STORE_LOCK = _threading.Lock()


def _picks_store_put(sport: str, home: str, away: str, picks: list, odds: dict):
    """Write picks to the store. Updates on every best-bets run so
    the card always reflects the latest computation. Mirrors the write
    to the sport's picks_cache table so the blob survives restarts.

    Stamps the blob with today's date so reads after midnight can
    discard yesterday's payload — without this, same-matchup games
    on consecutive days (PHI @ ATL, then PHI @ ATL again tomorrow)
    return yesterday's stale live odds because the in-memory key
    (sport:away@home) doesn't include the date.
    """
    key = f"{sport}:{away}@{home}"
    from engine.picks import get_best_pick
    best_pick = get_best_pick(picks) if picks else None
    today = datetime.now().strftime("%Y-%m-%d")
    with _PICKS_STORE_LOCK:
        _PICKS_STORE[key] = {
            "picks": picks,
            "odds": odds,
            "best_pick": best_pick,
            "date": today,
        }
    # Persist outside the lock: the tracker DB write can take tens of ms
    # on a slow disk and there's no reason to block concurrent reads on it.
    try:
        from engine import picks_cache
        picks_cache.put(sport, home, away, picks, odds, best_pick)
    except Exception as e:
        logger.warning("picks_cache persist failed for %s %s@%s: %s",
                       sport, away, home, e)


def _picks_store_get(sport: str, home: str, away: str) -> dict | None:
    """Read picks for a game. Returns {"picks", "odds", "best_pick", "date"}
    or None. Falls through to the picks_cache table when the in-memory
    store is cold.

    Date-aware: the in-memory key doesn't include the date, so when a
    same-matchup game runs on consecutive days the cache would return
    yesterday's blob. Comparing blob["date"] to today and dropping
    stale entries on read keeps the post-midnight slate clean even
    when the server hasn't restarted.
    """
    from engine.abbr import aliases_for
    today = datetime.now().strftime("%Y-%m-%d")
    with _PICKS_STORE_LOCK:
        for h in aliases_for(home, sport):
            for a in aliases_for(away, sport):
                for key in (f"{sport}:{a}@{h}", f"{sport}:{h}@{a}"):
                    blob = _PICKS_STORE.get(key)
                    if blob is None:
                        continue
                    if blob.get("date") == today:
                        return blob
                    # Stale entry from a previous calendar day —
                    # evict so subsequent fetches go to picks_cache
                    # (which IS date-keyed) for today's data.
                    _PICKS_STORE.pop(key, None)
    # Cache miss — check on-disk cache (date-keyed for today).
    try:
        from engine import picks_cache
        for h in aliases_for(home, sport):
            for a in aliases_for(away, sport):
                blob = picks_cache.get(sport, h, a)
                if blob is None:
                    blob = picks_cache.get(sport, a, h)
                if blob is not None:
                    blob.setdefault("date", today)
                    with _PICKS_STORE_LOCK:
                        _PICKS_STORE[f"{sport}:{a}@{h}"] = blob
                    return blob
    except Exception as e:
        logger.warning("picks_cache lookup failed for %s %s@%s: %s",
                       sport, away, home, e)
    return None


def _picks_store_clear():
    """Clear the picks store (for new day or manual reset). In-memory
    only; the DB cache is retained so load_today can rehydrate it."""
    with _PICKS_STORE_LOCK:
        _PICKS_STORE.clear()


def _picks_store_rehydrate():
    """Warm _PICKS_STORE from each sport's picks_cache table at startup.
    Also prunes stale rows (>7 days) to keep the cache compact."""
    from datetime import timedelta
    try:
        from engine import picks_cache
    except Exception as e:
        logger.warning("picks_cache unavailable at startup: %s", e)
        return
    today = datetime.now().strftime("%Y-%m-%d")
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    loaded = 0
    for sport in ("mlb", "nhl", "nba"):
        try:
            picks_cache.prune_before(sport, cutoff)
            for home, away, blob in picks_cache.load_today(sport, today):
                key = f"{sport}:{away}@{home}"
                with _PICKS_STORE_LOCK:
                    _PICKS_STORE[key] = blob
                loaded += 1
        except Exception as e:
            logger.warning("picks_cache rehydrate failed for %s: %s", sport, e)
    if loaded:
        logger.info("Rehydrated %d picks from picks_cache for %s", loaded, today)


@app.on_event("startup")
async def _rehydrate_picks_store_on_startup() -> None:
    """Populate the in-memory picks store from the persistent cache so
    the very first request after a restart sees the same picks the last
    best-bets run produced — no blocking recompute on cold start."""
    _picks_store_rehydrate()


def _get_recorded_pick(sport: str, matchup: str, date: str) -> dict | None:
    """Read a previously recorded pick from the tracker DB.

    Returns {"type", "pick", "prob", "edge", "odds", "confidence"} or None.
    This is the source of truth — once recorded, this pick shouldn't change.
    """
    try:
        if sport == "mlb":
            from engine.db import get_conn
            conn = get_conn()
            table = "picks"
        elif sport == "nhl":
            from engine.nhl_db import get_conn as _nhl_conn
            conn = _nhl_conn()
            table = "nhl_picks"
        elif sport == "nba":
            from engine.nba_db import get_conn as _nba_conn
            conn = _nba_conn()
            table = "nba_picks"
        else:
            return None

        row = conn.execute(
            f"SELECT bet_type, pick, model_prob, edge, odds FROM {table} "
            f"WHERE date = ? AND matchup = ? AND result IS NULL "
            f"ORDER BY edge DESC LIMIT 1",
            (date, matchup),
        ).fetchone()

        if not row:
            return None

        edge = row["edge"] or 0
        from engine.config import EDGE_STRONG, EDGE_MODERATE, EDGE_LEAN
        if edge >= EDGE_STRONG:
            conf = "strong"
        elif edge >= EDGE_MODERATE:
            conf = "moderate"
        elif edge >= EDGE_LEAN:
            conf = "lean"
        else:
            conf = "skip"

        return {
            "type": row["bet_type"],
            "pick": row["pick"],
            "prob": row["model_prob"],
            "edge": edge,
            "odds": row["odds"],
            "confidence": conf,
            "from_tracker": True,
        }
    except Exception as e:
        logger.debug("Could not read recorded pick for %s %s: %s", sport, matchup, e)
        return None


# Per-sport /best-bets progress state for the spinner. Frontend polls
# /api/<sport>/best-bets/progress to render a percentage instead of an
# indeterminate spinner during the multi-minute cold load. Each sport
# has its own counter so parallel dashboard mounts don't stomp on each
# other's totals.
_BB_PROGRESS: dict = {
    "mlb": {"total": 0, "done": 0, "phase": "idle",
            "started_at": None, "finished_at": None},
    "nhl": {"total": 0, "done": 0, "phase": "idle",
            "started_at": None, "finished_at": None},
    "nba": {"total": 0, "done": 0, "phase": "idle",
            "started_at": None, "finished_at": None},
}
_BB_PROGRESS_LOCK = _threading.Lock()


def _bb_progress_set(sport: str = "mlb", **fields) -> None:
    with _BB_PROGRESS_LOCK:
        _BB_PROGRESS.setdefault(sport, {}).update(fields)


def _bb_progress_increment(sport: str = "mlb") -> None:
    with _BB_PROGRESS_LOCK:
        bucket = _BB_PROGRESS.setdefault(sport, {})
        bucket["done"] = bucket.get("done", 0) + 1


# Single-flight registry. Concurrent /api/<sport>/best-bets calls (mount
# load + 5-min refresh interval racing each other, or two tabs open)
# would otherwise both run the full per-game prediction chain AND both
# bump the same shared progress counter -- producing "32/15 (100%)" in
# the spinner and doubling the workload. _BB_INFLIGHT holds the running
# request's Future so latecomers wait for its result instead of racing.
import concurrent.futures as _futures
_BB_INFLIGHT: dict[str, _futures.Future] = {}
_BB_INFLIGHT_LOCK = _threading.Lock()


def _bb_single_flight(sport: str, runner):
    """Run `runner()` once per sport at a time. If another request is
    already executing the same sport's work, wait for and return its
    result. Used by all three /best-bets endpoints."""
    with _BB_INFLIGHT_LOCK:
        existing = _BB_INFLIGHT.get(sport)
        if existing is not None and not existing.done():
            fut = existing
            owner = False
        else:
            fut = _futures.Future()
            _BB_INFLIGHT[sport] = fut
            owner = True

    if not owner:
        # Another caller is already running. Wait for their result.
        return fut.result()

    try:
        result = runner()
        fut.set_result(result)
        return result
    except BaseException as e:
        fut.set_exception(e)
        raise
    finally:
        with _BB_INFLIGHT_LOCK:
            if _BB_INFLIGHT.get(sport) is fut:
                _BB_INFLIGHT.pop(sport, None)


def _nhl_resolve_game_type(conn, home_tid: int, away_tid: int, date_s: str) -> int:
    """Resolve NHL game_type (2=regular, 3=playoff) for a live matchup.

    Checks the scheduled nhl_games row first; falls back to the calendar
    window (Apr-Jun = playoff) when the row isn't there yet. The GBM
    feature extractor reads game_type and derives `is_playoff`; hardcoding
    2 at inference masked the playoff signal the model learned in
    training, which is exactly the opposite of what we want during the
    postseason.
    """
    try:
        row = conn.execute(
            "SELECT game_type FROM nhl_games "
            "WHERE date = ? AND home_team_id = ? AND away_team_id = ? "
            "LIMIT 1",
            (date_s, home_tid, away_tid),
        ).fetchone()
        if row and row["game_type"] is not None:
            return int(row["game_type"])
    except Exception:
        pass
    try:
        month = int((date_s or "")[5:7])
    except (ValueError, TypeError):
        month = 0
    return 3 if month in (4, 5, 6) else 2


def _bb_progress_snapshot(sport: str = "mlb") -> dict:
    with _BB_PROGRESS_LOCK:
        return dict(_BB_PROGRESS.get(sport, {}))


def _bb_reset_on_exit(sport: str):
    """Decorator wrapping a /best-bets endpoint with two guarantees:

    1. Single-flight: only one underlying compute runs per sport at a
       time. Concurrent callers (mount + 5-min refresh racing, two open
       tabs) share the in-flight result instead of duplicating the
       full per-game prediction chain. Without this the shared progress
       counter overshoots ("32/15 (100%)" in the spinner) because two
       runners both increment the same `done`.
    2. Progress reset: the sport's progress bucket is forced back to
       phase=idle on exit -- even when the inner function raises -- so
       a mid-loop crash can never leave the spinner stuck at
       "Computing 4/15 games" forever.
    """
    import functools as _functools

    def decorator(fn):
        @_functools.wraps(fn)
        def wrapper(*args, **kwargs):
            def _runner():
                try:
                    return fn(*args, **kwargs)
                finally:
                    import time as _time
                    _bb_progress_set(sport, phase="idle",
                                     finished_at=_time.time())
            return _bb_single_flight(sport, _runner)
        return wrapper
    return decorator


def _pred_cache_get(key: tuple):
    import time as _time
    with _PRED_CACHE_LOCK:
        entry = _PRED_CACHE.get(key)
        if not entry:
            return None
        if _time.time() - entry[0] > _PRED_CACHE_TTL_S:
            _PRED_CACHE.pop(key, None)
            return None
        return entry[1]


def _pred_cache_put(key: tuple, value: dict) -> None:
    import time as _time
    with _PRED_CACHE_LOCK:
        _PRED_CACHE[key] = (_time.time(), value)
        # Cap cache size at 64 to bound memory; evict oldest by walltime.
        if len(_PRED_CACHE) > 64:
            oldest = min(_PRED_CACHE.items(), key=lambda kv: kv[1][0])[0]
            _PRED_CACHE.pop(oldest, None)


def _predict_mlb_full(home_team_id: int, away_team_id: int,
                      home_pitcher_id: int | None,
                      away_pitcher_id: int | None,
                      venue: str | None,
                      use_cache: bool = True) -> dict:
    """Run the full MLB prediction chain: factor + MC + GBM + ensemble.

    Single source of truth shared by /api/predict and /api/best-bets so
    dashboard picks blend MC + GBM like the GameDetail drill-down. If the
    factor model returns an error, the dict is returned as-is; callers
    guard with `if "error" in result` as usual.

    `use_cache=True` (default) returns memoized results for the same
    (home, away, pitcher pair, venue) tuple within _PRED_CACHE_TTL_S.
    Pass False from on-demand handlers that need a guaranteed fresh
    prediction.
    """
    cache_key = ("mlb", home_team_id, away_team_id, home_pitcher_id,
                 away_pitcher_id, venue)
    if use_cache:
        cached = _pred_cache_get(cache_key)
        if cached is not None:
            return cached

    result = predict_matchup(
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        home_pitcher_id=home_pitcher_id,
        away_pitcher_id=away_pitcher_id,
        venue=venue,
    )
    if "error" in result:
        return result

    from engine.config import get_flag as _get_flag

    if _get_flag("ENABLE_MLB_MC", False):
        try:
            from engine.mc_mlb_run import run_mlb_mc
            import engine.config as _cfg
            today_s = datetime.now().strftime("%Y-%m-%d")
            result["mc"] = run_mlb_mc(
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                home_pitcher_id=home_pitcher_id,
                away_pitcher_id=away_pitcher_id,
                venue=venue,
                n_sims=int(getattr(_cfg, "MLB_MC_N_SIMS", 100_000)),
                seed=_mc_seed("mlb", today_s, home_team_id, away_team_id,
                              home_pitcher_id, away_pitcher_id),
            )
        except Exception as e:
            logger.warning("MC prediction failed for %s/%s: %s",
                           home_team_id, away_team_id, e, exc_info=True)
            result["mc"] = {"error": str(e)}

    if _get_flag("ENABLE_MLB_GBM", False):
        try:
            from engine.gbm.predict import predict_mlb as _gbm_predict
            from engine.db import get_conn as _gc
            result["gbm"] = _gbm_predict(_gc(), {
                "mlb_game_id": 0,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "home_pitcher_id": home_pitcher_id,
                "away_pitcher_id": away_pitcher_id,
                "venue": venue,
            })
        except Exception as e:
            logger.warning("GBM prediction failed for %s/%s: %s",
                           home_team_id, away_team_id, e, exc_info=True)
            result["gbm"] = {"error": str(e)}

    try:
        from engine.ensemble import ensemble_mlb
        result["ensemble"] = ensemble_mlb(result)
    except Exception as e:
        logger.debug("MLB ensemble blend failed: %s", e)
        result["ensemble"] = {}

    # Backfill the factor-model NRFI/YRFI scalars with the blended
    # ensemble value so every surface shows the same number. The pick
    # engine reads ens["nrfi"] (e.g. 51.2%) while PredictionResults
    # reads first_inning.nrfi (factor-only, e.g. 50.1%); without this
    # backfill the game-detail panel disagreed with the headline pick.
    # Per-team scores_1st stay factor-derived (ensemble doesn't model
    # them) so only the top-line scalars are rewritten.
    ens = result.get("ensemble") or {}
    fi = result.get("first_inning")
    if isinstance(fi, dict) and ens.get("nrfi") is not None:
        nrfi_val = float(ens["nrfi"])
        fi["nrfi"] = nrfi_val
        fi["yrfi"] = 1.0 - nrfi_val

    if use_cache:
        _pred_cache_put(cache_key, result)
    return result


def _predict_nhl_full(home_key: str, away_key: str,
                     use_cache: bool = True) -> dict | None:
    """Run factor + MC + GBM for NHL so generate_nhl_picks_with_context
    can blend via ensemble_nhl. Same caching shape as MLB."""
    cache_key = ("nhl", home_key, away_key)
    if use_cache:
        cached = _pred_cache_get(cache_key)
        if cached is not None:
            return cached

    from engine.nhl_predict import predict_matchup as _nhl_pm
    result = _nhl_pm(home_key, away_key)
    if not result:
        return None

    from engine.config import get_flag as _get_flag

    if _get_flag("ENABLE_NHL_MC", False, sport="nhl"):
        try:
            from engine.mc_nhl_run import run_nhl_mc
            import engine.config as _cfg
            home_abbr = (result.get("home") or {}).get("abbreviation") or home_key
            away_abbr = (result.get("away") or {}).get("abbreviation") or away_key
            today_s = datetime.now().strftime("%Y-%m-%d")
            result["mc"] = run_nhl_mc(
                home_abbr, away_abbr,
                n_sims=int(getattr(_cfg, "NHL_MC_N_SIMS", 100_000)),
                seed=_mc_seed("nhl", today_s, home_abbr, away_abbr),
            )
        except Exception as e:
            logger.warning("NHL MC failed for %s/%s: %s", home_key, away_key, e)
            result["mc"] = {"error": str(e)}

    if _get_flag("ENABLE_NHL_GBM", False, sport="nhl"):
        try:
            from engine.gbm.predict import predict_nhl as _gbm_predict_nhl
            from engine.nhl_db import get_conn as _nhl_conn
            # nhl_predict.predict_matchup returns {home: {name, abbreviation,
            # record, key}} -- no `id`. Resolve team_id via nhl_teams.abbreviation
            # so the GBM payload has what engine.gbm.features_nhl expects.
            nhl_conn = _nhl_conn()
            home_abbr = (result.get("home") or {}).get("abbreviation") or ""
            away_abbr = (result.get("away") or {}).get("abbreviation") or ""
            home_tid = away_tid = None
            if home_abbr and away_abbr:
                rows = nhl_conn.execute(
                    "SELECT id, abbreviation FROM nhl_teams "
                    "WHERE abbreviation IN (?, ?)",
                    (home_abbr.upper(), away_abbr.upper()),
                ).fetchall()
                by_abbr = {r["abbreviation"]: r["id"] for r in rows}
                home_tid = by_abbr.get(home_abbr.upper())
                away_tid = by_abbr.get(away_abbr.upper())
            if home_tid and away_tid:
                today_s = datetime.now().strftime("%Y-%m-%d")
                result["gbm"] = _gbm_predict_nhl(nhl_conn, {
                    "home_team_id": int(home_tid),
                    "away_team_id": int(away_tid),
                    "date": today_s,
                    "game_type": _nhl_resolve_game_type(
                        nhl_conn, int(home_tid), int(away_tid), today_s,
                    ),
                })
            else:
                # Expose the lookup failure so we don't silently fall back
                # to factor-only again. UI can distinguish "GBM off" from
                # "GBM tried and couldn't resolve team".
                result["gbm"] = {"error": f"team_id lookup failed: "
                                          f"{home_abbr!r}/{away_abbr!r}"}
        except Exception as e:
            logger.warning("NHL GBM failed for %s/%s: %s", home_key, away_key, e)
            result["gbm"] = {"error": str(e)}

    if use_cache:
        _pred_cache_put(cache_key, result)
    return result


def _predict_nba_full(home_abbr: str, away_abbr: str,
                     odds: dict | None = None,
                     use_cache: bool = True) -> dict | None:
    """Run factor + MC + GBM for NBA Q1 so generate_q1_picks can blend
    via ensemble_nba. Cached per (home, away) tuple; odds are pulled
    fresh each time since they change intra-day."""
    # Cache on pred only -- picks are rebuilt each request because they
    # depend on live odds. The factor+MC+GBM chain doesn't need odds so
    # caching it independently keeps the cache hit rate high.
    cache_key = ("nba", home_abbr, away_abbr)
    if use_cache:
        cached = _pred_cache_get(cache_key)
        if cached is not None:
            return cached

    from engine.nba_q1_predict import predict_q1
    odds = odds or {}
    q1_spread = odds.get("q1_spread")
    q1_total = odds.get("q1_total")
    try:
        result = predict_q1(home_abbr, away_abbr,
                            spread=q1_spread, total=q1_total)
    except Exception as e:
        logger.warning("NBA Q1 factor predict failed for %s/%s: %s",
                       home_abbr, away_abbr, e)
        return None
    if not result:
        return None

    from engine.config import get_flag as _get_flag

    if _get_flag("ENABLE_NBA_MC", False, sport="nba"):
        try:
            from engine.mc_nba_run import run_nba_q1_mc
            import engine.config as _cfg
            today_s = datetime.now().strftime("%Y-%m-%d")
            result["mc"] = run_nba_q1_mc(
                home_abbr, away_abbr,
                n_sims=int(getattr(_cfg, "NBA_MC_N_SIMS", 100_000)),
                seed=_mc_seed("nba", today_s, home_abbr, away_abbr),
            )
        except Exception as e:
            logger.warning("NBA MC failed for %s/%s: %s", home_abbr, away_abbr, e)
            result["mc"] = {"error": str(e)}

    if _get_flag("ENABLE_NBA_GBM", False, sport="nba"):
        try:
            from engine.gbm.predict import predict_nba as _gbm_predict_nba
            from engine.nba_db import get_conn as _nba_conn
            # predict_q1 returns flat home_abbr/away_abbr, not a nested
            # home.id -- resolve via nba_teams.abbreviation.
            nba_conn = _nba_conn()
            h_abbr_u = (home_abbr or "").upper()
            a_abbr_u = (away_abbr or "").upper()
            home_tid = away_tid = None
            if h_abbr_u and a_abbr_u:
                rows = nba_conn.execute(
                    "SELECT id, abbreviation FROM nba_teams "
                    "WHERE abbreviation IN (?, ?)",
                    (h_abbr_u, a_abbr_u),
                ).fetchall()
                by_abbr = {r["abbreviation"]: r["id"] for r in rows}
                home_tid = by_abbr.get(h_abbr_u)
                away_tid = by_abbr.get(a_abbr_u)
            if home_tid and away_tid:
                result["gbm"] = _gbm_predict_nba(nba_conn, {
                    "home_team_id": int(home_tid),
                    "away_team_id": int(away_tid),
                    "date": datetime.now().strftime("%Y-%m-%d"),
                })
            else:
                result["gbm"] = {"error": f"team_id lookup failed: "
                                          f"{h_abbr_u!r}/{a_abbr_u!r}"}
        except Exception as e:
            logger.warning("NBA GBM failed for %s/%s: %s", home_abbr, away_abbr, e)
            result["gbm"] = {"error": str(e)}

    if use_cache:
        _pred_cache_put(cache_key, result)
    return result


@app.post("/api/predict")
def api_predict(req: PredictRequest):
    """Run a game prediction."""
    result = _predict_mlb_full(
        home_team_id=req.home_team_id,
        away_team_id=req.away_team_id,
        home_pitcher_id=req.home_pitcher_id,
        away_pitcher_id=req.away_pitcher_id,
        venue=req.venue,
    )
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    # Attach season_context so the GameDetail banner renders when it
    # would render on best-bets cards too. predict_matchup doesn't
    # produce this yet; fall back to the month-based heuristic.
    if result.get("season_context") is None:
        sc = _mlb_season_context(req.home_team_id, req.away_team_id)
        if sc:
            result["season_context"] = sc

    # Expose home_impact / away_impact on the injuries block so the
    # GameDetail Injuries card can render the "X% weaker" summary the
    # NHL version has. predict_matchup itself just stores the raw list.
    inj = result.get("injuries") or {}
    if isinstance(inj, dict):
        if inj.get("home_impact") is None:
            inj["home_impact"] = _compute_injury_impact_pct(inj.get("home") or [])
        if inj.get("away_impact") is None:
            inj["away_impact"] = _compute_injury_impact_pct(inj.get("away") or [])
        result["injuries"] = inj

    # Surface the MLB direction-allow flags so the frontend's
    # PredictionResults / EdgeCallout can suppress recommendations for
    # disabled directions. Without this the UI would still show e.g.
    # 'STRONG Under 8.5' even when MLB_ALLOW_OU_UNDER=False, because
    # the frontend's getBestEdge() recomputes from raw probabilities
    # and bypasses picks.py's filtering. Single source of truth stays
    # in engine.config.
    # Surface flag values through get_flag() so any active runtime
    # overrides (from engine.train --apply) are reflected in the UI.
    from engine.config import get_flag as _get_flag
    result["direction_filters"] = {
        "rl_favorite": bool(_get_flag("MLB_ALLOW_RL_FAVORITE", True)),
        "rl_underdog": bool(_get_flag("MLB_ALLOW_RL_UNDERDOG", True)),
        "nrfi":        bool(_get_flag("MLB_ALLOW_NRFI", True)),
        "yrfi":        bool(_get_flag("MLB_ALLOW_YRFI", True)),
        "ou_over":     bool(_get_flag("MLB_ALLOW_OU_OVER", True)),
        "ou_under":    bool(_get_flag("MLB_ALLOW_OU_UNDER", True)),
    }

    # Surface active runtime overrides so GameDetail can show a notice
    # ("2 markets currently suppressed by data: OU_UNDER, RL_FAVORITE")
    # instead of silently dropping rows. Each entry carries the reason
    # and expiry so the user can inspect via /api/model-overrides.
    try:
        from engine.model_overrides import list_overrides as _list_ov
        result["active_overrides"] = _list_ov("mlb")
    except Exception as e:
        logger.debug("active_overrides lookup failed: %s", e)
        result["active_overrides"] = []

    # Log component probabilities per market so engine.ensemble_auto_tune
    # can grade each signal against actual outcomes once the game ends
    # and produce tuned blend weights. Logging runs unconditionally
    # (even when MC/GBM didn't run -- the missing components are stored
    # as NULL and dropped at tune time if coverage is under 50%).
    try:
        from engine.prediction_log import log_signals as _log_signals
        from engine.db import get_conn as _gc
        mc = result.get("mc") or {}
        gbm = result.get("gbm") or {}
        if "error" in mc: mc = {}
        if "error" in gbm: gbm = {}
        # Look up the mlb_game_id for this matchup on today's slate so
        # the signal row can JOIN against games cleanly later. We don't
        # always have this pre-prediction -- if missing, store 0 so the
        # row isn't orphaned (the daily settle pass can still compute
        # outcomes on games that ARE logged with a real id).
        from datetime import datetime as _dt
        today = _dt.now().strftime("%Y-%m-%d")
        _mlb_id = _gc().execute(
            "SELECT mlb_game_id FROM games "
            "WHERE date = ? AND home_team_id = ? AND away_team_id = ? "
            "LIMIT 1",
            (today, req.home_team_id, req.away_team_id),
        ).fetchone()
        game_id = _mlb_id["mlb_game_id"] if _mlb_id else None

        signals = {}
        # home_win
        factor_hw = (result.get("win_prob") or {}).get("home")
        mc_hw = (mc.get("win_prob") or {}).get("home")
        gbm_hw = gbm.get("home_win")
        if factor_hw is not None or mc_hw is not None or gbm_hw is not None:
            signals["home_win"] = {
                "factor": factor_hw, "mc": mc_hw, "gbm": gbm_hw,
            }
        # total (expected runs)
        factor_t = result.get("total")
        mc_t = (mc.get("expected_runs") or {}).get("total")
        gbm_t = gbm.get("total_runs")
        if factor_t is not None or mc_t is not None or gbm_t is not None:
            signals["total"] = {
                "factor": factor_t, "mc": mc_t, "gbm": gbm_t,
            }
        # NRFI
        factor_n = (result.get("first_inning") or {}).get("nrfi")
        mc_n = (mc.get("nrfi") or {}).get("nrfi")
        gbm_n = gbm.get("nrfi_hit")
        if factor_n is not None or mc_n is not None or gbm_n is not None:
            signals["nrfi"] = {
                "factor": factor_n, "mc": mc_n, "gbm": gbm_n,
            }
        # F5 home_win
        factor_f5 = ((result.get("f5") or {}).get("win_prob") or {}).get("home")
        mc_f5 = ((mc.get("f5") or {}).get("win_prob") or {}).get("home")
        gbm_f5 = gbm.get("f5_home_win")
        if factor_f5 is not None or mc_f5 is not None or gbm_f5 is not None:
            signals["f5_home_win"] = {
                "factor": factor_f5, "mc": mc_f5, "gbm": gbm_f5,
            }
        # F5 total
        factor_f5t = (result.get("f5") or {}).get("total")
        mc_f5t = ((mc.get("f5") or {}).get("expected_runs") or {}).get("total")
        gbm_f5t = gbm.get("f5_total")
        if factor_f5t is not None or mc_f5t is not None or gbm_f5t is not None:
            signals["f5_total"] = {
                "factor": factor_f5t, "mc": mc_f5t, "gbm": gbm_f5t,
            }

        if signals:
            _log_signals("mlb", game_id, today,
                         req.home_team_id, req.away_team_id, signals)
    except Exception as e:
        logger.debug("prediction signal logging failed: %s", e)

    # Run the unified picks engine so GameDetail renders the SAME rows
    # as the Scoreboard best-bet badge and POTD selection. Without this,
    # BettingPicks / PredictionResults reimplement edge logic in JS and
    # drift from engine.picks (hardcoded -120 NRFI, ignored direction
    # filters, no real F5/NRFI DK odds). Single source of truth lives
    # in engine.picks.generate_picks().
    #
    # If best-bets already computed picks for this game (attached to
    # the cached prediction), reuse them so all three surfaces
    # (scoreboard card, game detail, tracker) show the same pick.
    try:
        from engine.picks import (
            generate_picks, get_best_pick, match_odds, fetch_real_odds_for_games,
        )
        from engine.db import get_team_by_id

        # Prefer picks store (single source of truth from best-bets)
        home_team = get_team_by_id(req.home_team_id) or {}
        away_team = get_team_by_id(req.away_team_id) or {}
        h_abbr = home_team.get("abbreviation", "")
        a_abbr = away_team.get("abbreviation", "")

        stored = _picks_store_get("mlb", h_abbr, a_abbr)
        if stored:
            picks = stored["picks"]
            odds = stored["odds"]
        elif result.get("_cached_picks"):
            picks = result["_cached_picks"]
            odds = result.get("_cached_odds", {})
        else:

            odds = _odds_from_scoreboard_cache(h_abbr, a_abbr) if (h_abbr and a_abbr) else {}
            if not odds and h_abbr and a_abbr:
                all_odds = fetch_real_odds_for_games()
                odds = match_odds(h_abbr, a_abbr, all_odds)

            picks = generate_picks(
                home_team_id=req.home_team_id,
                away_team_id=req.away_team_id,
                home_pitcher_id=req.home_pitcher_id,
                away_pitcher_id=req.away_pitcher_id,
                venue=req.venue,
                odds=odds or None,
                pred=result,
            )

        result["picks"] = picks
        # Use recorded tracker pick as canonical best_pick
        predict_matchup_str = f"{a_abbr} @ {h_abbr}"
        predict_date = datetime.now().strftime("%Y-%m-%d")
        recorded = _get_recorded_pick("mlb", predict_matchup_str, predict_date)
        result["best_pick"] = recorded if recorded else get_best_pick(picks)
        result["odds"] = odds
    except Exception as e:
        logger.warning("Unified picks generation failed for %s/%s: %s",
                       req.home_team_id, req.away_team_id, e)
        result["picks"] = []
        result["best_pick"] = None
        result["odds"] = {}

    return result


@app.get("/api/best-bets/progress")
def api_best_bets_progress(sport: str = "mlb"):
    """Spinner support: live progress of an in-flight /api/<sport>/best-bets run.

    Default sport is mlb for back-compat with the original single-sport
    endpoint. Frontend passes ?sport=nhl or ?sport=nba for those mounts.
    """
    snap = _bb_progress_snapshot(sport)
    total = snap.get("total") or 0
    done = snap.get("done") or 0
    snap["pct"] = round(done / total * 100, 1) if total else 0.0
    snap["sport"] = sport
    return snap


@app.get("/api/nhl/best-bets/progress")
def api_nhl_best_bets_progress():
    return api_best_bets_progress(sport="nhl")


@app.get("/api/nba/best-bets/progress")
def api_nba_best_bets_progress():
    return api_best_bets_progress(sport="nba")


@app.get("/api/{sport}/pick-events")
def api_pick_events(sport: str, game_id: str | None = None,
                    hours: int = 24):
    """Recent pick-event log for the BestBets card 📜 breadcrumb.

    Returns the appeared / swapped / pulled / line_shift transitions
    the model has emitted today (or longer via ``hours``). Scope to a
    single matchup with ``game_id`` so each card only loads its own
    thread instead of the whole slate.
    """
    if sport not in ("mlb", "nhl", "nba"):
        return {"error": f"unknown sport: {sport}"}
    from engine.pick_events import list_events
    return list_events(sport, game_id=game_id, hours=hours)


def _bb_predict_one(game: dict, all_odds) -> dict | None:
    """Compute the full prediction + odds payload for a single game.

    Returns a dict with everything /api/best-bets needs (pred, h/a IDs,
    odds, abbreviations) or None if the game should be skipped (live,
    completed, missing IDs, prediction error).
    """
    from engine.picks import match_odds
    home_id = game["home"].get("team_id")
    away_id = game["away"].get("team_id")
    if not home_id or not away_id:
        return None

    state = game["status"].get("state", "pre")
    if state in ("post", "in") or game["status"].get("completed"):
        return None

    home_pid = game.get("home_pitcher") or {}
    away_pid = game.get("away_pitcher") or {}
    try:
        h_pitcher_id = int(home_pid["id"]) if home_pid.get("id") else None
        a_pitcher_id = int(away_pid["id"]) if away_pid.get("id") else None
    except (ValueError, TypeError):
        h_pitcher_id = None
        a_pitcher_id = None

    h_abbr = game["home"]["abbreviation"]
    a_abbr = game["away"]["abbreviation"]
    # Merge HR's full scrape (ML/RL/OU + alt spreads + team totals + F5
    # + derivatives) with whatever the scoreboard attached (HR if HR
    # matched, Odds API/ESPN otherwise). HR wins on overlap so the model
    # always prices against the book it was tuned for; scoreboard fields
    # only fill gaps where HR didn't ship a market for that game.
    matched = match_odds(h_abbr, a_abbr, all_odds) or {}
    game_odds = {**(game.get("odds") or {}), **matched}

    # Bypass the 30-min pred cache for games starting within 1hr —
    # late lineup scratches, weather, line moves all matter most in
    # the final hour. Outside the 1hr window the cache stands so we
    # don't redo full MC + GBM on every refresh of an early-slate game.
    use_cache = not _is_game_imminent(game.get("date"))
    try:
        pred = _predict_mlb_full(
            home_team_id=home_id,
            away_team_id=away_id,
            home_pitcher_id=h_pitcher_id,
            away_pitcher_id=a_pitcher_id,
            venue=game.get("venue"),
            use_cache=use_cache,
        )
    except Exception as e:
        logger.error("  Prediction crashed for %s: %s",
                     game.get("short_name", "?"), e, exc_info=True)
        return None

    if "error" in pred:
        logger.error("  Prediction failed for %s: %s",
                     game.get("short_name", "?"), pred["error"])
        return None

    return {
        "game": game,
        "home_id": home_id, "away_id": away_id,
        "h_pitcher_id": h_pitcher_id, "a_pitcher_id": a_pitcher_id,
        "h_abbr": h_abbr, "a_abbr": a_abbr,
        "odds": game_odds,
        "pred": pred,
    }


@app.get("/api/best-bets")
@_bb_reset_on_exit("mlb")
def api_best_bets():
    """
    Run predictions on all today's games using the unified picks engine.

    Per-game predictions run in parallel (ThreadPoolExecutor) so cold
    dashboard load isn't N x serial MC sims. Progress is published
    via /api/best-bets/progress so the frontend can render a percentage.
    """
    import time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    target_date = datetime.now().strftime("%Y-%m-%d")
    games = _get_scoreboard()

    from engine.picks import generate_picks, get_best_pick, fetch_real_odds_for_games

    all_odds = fetch_real_odds_for_games()

    # Determine the predictable subset up front so the progress total is
    # accurate (don't count games we'll skip).
    predictable = [
        g for g in games
        if g["home"].get("team_id") and g["away"].get("team_id")
        and g["status"].get("state", "pre") not in ("post", "in")
        and not g["status"].get("completed")
    ]

    _bb_progress_set(total=len(predictable), done=0, phase="predicting",
                     started_at=_time.time(), finished_at=None)
    logger.info("Best bets: analyzing %d games (parallel)", len(predictable))

    # Up to 8 workers in parallel: predict_matchup is DB-bound, MC sims
    # are largely numpy (releases the GIL), GBM is XGBoost C++ (releases
    # the GIL). 8 is well-tuned for a typical 10-15 game slate.
    prepped: list[dict] = []
    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="bb-pred") as pool:
        futures = {pool.submit(_bb_predict_one, g, all_odds): g for g in predictable}
        for fut in as_completed(futures):
            try:
                row = fut.result()
            except Exception as e:
                logger.error("  Worker crashed: %s", e, exc_info=True)
                row = None
            _bb_progress_increment()
            if row:
                prepped.append(row)

    _bb_progress_set(phase="building")

    bets = []
    for row in prepped:
        game = row["game"]
        home_id = row["home_id"]
        away_id = row["away_id"]
        h_pitcher_id = row["h_pitcher_id"]
        a_pitcher_id = row["a_pitcher_id"]
        h_abbr = row["h_abbr"]
        a_abbr = row["a_abbr"]
        game_odds = row["odds"]
        pred = row["pred"]

        try:
            picks = generate_picks(
                home_team_id=home_id,
                away_team_id=away_id,
                home_pitcher_id=h_pitcher_id,
                away_pitcher_id=a_pitcher_id,
                venue=game.get("venue"),
                odds=game_odds,
                pred=pred,
            )
        except Exception as e:
            logger.error("  Pick generation failed for %s: %s", game.get("short_name", "?"), e)
            continue

        if not picks:
            continue

        _picks_store_put("mlb", h_abbr, a_abbr, picks, game_odds)

        # Card + POTD should always agree. To make that true, both
        # are computed against CORE picks only — derivatives flow
        # exclusively through `derivative_picks` and the dedicated
        # tracker.
        core_picks = [p for p in picks if p.get("type") not in _MLB_DERIV_TYPES]
        matchup = f"{a_abbr} @ {h_abbr}"
        recorded = _get_recorded_pick("mlb", matchup, target_date)

        # Per-game lock: once the game is within 1 hour of tip-off,
        # freeze the card's best_pick to whatever the tracker recorded
        # (or fall back to current best if nothing recorded). Live
        # in-game odds/score swings can't keep flipping the headline —
        # what the user saw at lock time is what they bet.
        #
        # Pre-game line drift: if HR moved the line enough for the live
        # picker to fall below EDGE_SKIP, fall back to the recorded
        # tracker pick instead of showing "NO PICK" — the card should
        # never disagree with the tracker on whether a game has a play.
        locked = _is_game_locked(game.get("date"))
        live_best = get_best_pick(core_picks) if core_picks else None
        if locked and recorded:
            best = recorded
        elif live_best and (live_best.get("confidence") or "lean") != "skip":
            best = live_best
        elif recorded:
            from engine.config import EDGE_STRONG, EDGE_MODERATE, EDGE_LEAN
            r_edge = float(recorded.get("edge") or 0)
            r_conf = ("strong" if r_edge >= EDGE_STRONG else
                      "moderate" if r_edge >= EDGE_MODERATE else
                      "lean")
            best = {**recorded, "confidence": r_conf}
        else:
            best = live_best
        if not best:
            continue

        # Card-enrich: derive rest / factors / injuries / season_context
        # from the prediction we already computed above.
        win_prob = {}
        rest = {}
        factors = {}
        injuries = {}
        season_context = None
        try:
            if pred and "error" not in pred:
                win_prob = pred.get("win_prob") or {}

                # Rest signal for MLB: travel fatigue multipliers < 1.0 mean
                # the team is fatigued; the healthier side has a rest advantage.
                tr = pred.get("travel") or {}
                home_travel = tr.get("home", 1.0) or 1.0
                away_travel = tr.get("away", 1.0) or 1.0
                rest = {
                    "home_short_rest": home_travel < 0.97,
                    "away_short_rest": away_travel < 0.97,
                    "home_rest_advantage": home_travel > away_travel + 0.02,
                    "away_rest_advantage": away_travel > home_travel + 0.02,
                }

                # Factors surfaced in CardInsight: wRC+ gap, form, park.
                # Pull from prediction sub-dicts + the direct team_record.
                try:
                    from engine.db import get_team_record
                    from engine.mlb_predict import SEASON as _MLB_SEASON
                    h_rec = get_team_record(home_id, _MLB_SEASON) or {}
                    a_rec = get_team_record(away_id, _MLB_SEASON) or {}
                except Exception:
                    h_rec, a_rec = {}, {}
                factors = {
                    "home_wrc_plus": h_rec.get("wrc_plus"),
                    "away_wrc_plus": a_rec.get("wrc_plus"),
                    "home_form": _mlb_form_from_reasoning(pred.get("reasoning"), h_abbr),
                    "away_form": _mlb_form_from_reasoning(pred.get("reasoning"), a_abbr),
                    "park_factor": pred.get("park_factor"),
                    "home_ops": h_rec.get("ops"),
                    "away_ops": a_rec.get("ops"),
                }

                # Injury impact: convert the MLB injury multiplier to a 0-1
                # "strength fraction" matching the NHL card's home_impact
                # semantic (1.0 = healthy, <1 = weaker).
                inj_data = pred.get("injuries") or {}
                injuries = {
                    "home_impact": _compute_injury_impact_pct(inj_data.get("home") or []),
                    "away_impact": _compute_injury_impact_pct(inj_data.get("away") or []),
                    "home_list": inj_data.get("home") or [],
                    "away_list": inj_data.get("away") or [],
                }

                # Season context: prefer anything computed server-side,
                # otherwise fall back to the standings-aware detector.
                season_context = pred.get("season_context") or _mlb_season_context(home_id, away_id)
        except Exception as e:
            logger.debug("  Enrich-best-bet failed for %s/%s: %s", h_abbr, a_abbr, e)

        # all_picks = top 4 CORE picks (no derivatives) plus the 1st
        # INN top-up if outside top 4. Derivatives are routed entirely
        # through `derivative_picks` below; they never appear in
        # all_picks so the card and POTD always agree on the headline.
        all_picks = list(core_picks[:4])
        if not any(p.get("type") == "1st INN" for p in all_picks):
            for p in core_picks[4:]:
                if p.get("type") == "1st INN":
                    all_picks.append(p)
                    break

        derivative_picks = sorted(
            (p for p in picks if p.get("type") in _MLB_DERIV_TYPES),
            key=lambda p: -(p.get("edge") or 0),
        )

        bets.append({
            "game_id": game["id"],
            "matchup": f"{a_abbr} @ {h_abbr}",
            "home": game["home"],
            "away": game["away"],
            "time": game["date"],
            "venue": game.get("venue", ""),
            "best_pick": best,
            "all_picks": all_picks,
            "derivative_picks": derivative_picks,
            "confidence": best.get("confidence", "lean"),
            "win_prob": win_prob,
            "rest": rest,
            "factors": factors,
            "injuries": injuries,
            "season_context": season_context,
            # When this game's picks were generated (ISO UTC). UI uses
            # this to render an "as of HH:MM" tag so users can tell
            # how stale the cards are vs HR's live odds. Combined with
            # the frontend's 5-min auto-refresh, worst-case staleness
            # is bounded; the timestamp makes it visible.
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "recorded_pick": recorded,
            "is_locked": locked,
        })

    bets.sort(key=lambda b: b["best_pick"]["edge"], reverse=True)

    # Reconcile tracker pending picks with the live model. When the
    # line moves or the model swaps, the pending row updates so the
    # tracker dashboard isn't stuck on yesterday's recommendation.
    try:
        from engine.tracker import refresh_pending_for_today
        diff = refresh_pending_for_today(bets, target_date)
        if any(diff.values()):
            logger.info("Tracker pending sync (mlb): %s", diff)
    except Exception as e:
        logger.warning("Tracker pending refresh failed: %s", e)

    # Paper-bet log for derivative markets — separate table so we can
    # evaluate derivative profitability in isolation. Records top 3
    # derivative picks per game with edge >= 4%; idempotent across
    # repeat calls (UNIQUE constraint dedupes; live edge/odds refresh
    # on re-call so the row stays current).
    try:
        from engine.derivative_tracker import record_top_derivatives
        diff = record_top_derivatives("mlb", bets, n_per_game=1,
                                       min_edge=4.0, target_date=target_date)
        if diff.get("inserted") or diff.get("updated"):
            logger.info("Derivative tracker (mlb): %s", diff)
    except Exception as e:
        logger.warning("MLB derivative recorder failed: %s", e)

    # Pick-events breadcrumb log — drives the 📜 badge on BestBets cards.
    # Diffs the current best_pick per game vs the most recent event row
    # and emits appeared / swapped / pulled / line_shift transitions.
    # Idempotent within a poll cycle because state lives in the events
    # table, not in-memory.
    try:
        from engine.pick_events import detect_transitions
        ev = detect_transitions("mlb", [
            {"game_id": b["game_id"], "matchup": b["matchup"],
             "best_pick": b.get("best_pick")}
            for b in bets
        ], date=target_date)
        if any(ev.values()):
            logger.info("Pick events (mlb): %s", ev)
    except Exception as e:
        logger.warning("MLB pick-events failed: %s", e)

    import time as _time
    _bb_progress_set(phase="idle", finished_at=_time.time())
    return bets


def _mlb_form_from_reasoning(reasoning: list | None, abbr: str) -> float | None:
    """Parse the hot/cold form pct out of reasoning strings.

    _build_reasoning produces lines like 'NYY running hot (form +4.5%)'
    so we scrape the matching one. Returns signed fraction (e.g. 0.045)
    or None when form info isn't present.
    """
    if not reasoning:
        return None
    import re
    tag = abbr.upper()
    pat = re.compile(r"(?:form\s*)?([+\-]?\d+(?:\.\d+)?)\s*%")
    for line in reasoning:
        if not isinstance(line, str):
            continue
        if tag not in line:
            continue
        if "form" not in line.lower():
            continue
        m = pat.search(line)
        if m:
            try:
                return float(m.group(1)) / 100.0
            except ValueError:
                pass
    return None


def _mlb_season_context(home_id: int | None = None,
                         away_id: int | None = None) -> dict | None:
    """Return a season-context dict the UI can render as a banner.

    Uses month + (when team IDs provided) standings context:
      - Oct: always "playoffs"
      - Sep: "regular-late" unless one team has <90 games played (early)
      - Aug + either team within 5 GB of a playoff spot: "playoff-race"
      - Else: None (no banner)

    'implications' is a truthy string so the UI gate fires.
    """
    from datetime import datetime
    m = datetime.now().month
    if m == 10:
        return {
            "phase": "playoffs",
            "implications": "Postseason intensity (small sample, high leverage).",
        }
    if m == 9:
        return {
            "phase": "regular-late",
            "implications": "Playoff race: more bullpen usage, tighter lineups.",
        }

    # Standings-aware detection for August: is either team in a playoff race?
    if m == 8 and home_id and away_id:
        try:
            from engine.db import get_team_record
            from engine.mlb_predict import SEASON as _SEASON
            h = get_team_record(home_id, _SEASON) or {}
            a = get_team_record(away_id, _SEASON) or {}
            h_gb = h.get("games_back")
            a_gb = a.get("games_back")
            if (h_gb is not None and h_gb <= 5) or (a_gb is not None and a_gb <= 5):
                return {
                    "phase": "regular-late",
                    "implications": "Playoff race: close to playoff line.",
                }
        except Exception:
            pass
    return None


def _nba_season_context() -> dict | None:
    """Return a season-context banner dict for NBA GameDetail.

    NBA regular season: late October to mid-April.
    Play-in tournament: mid-April.
    Playoffs: mid-April to mid-June.

    Month-based phases:
      - Jun: "finals" intensity (if still playing)
      - Apr-May: "playoffs" / play-in
      - Mar: "late regular-season"
      - Else: None
    """
    from datetime import datetime
    now = datetime.now()
    m, d = now.month, now.day
    if m == 6:
        return {
            "phase": "playoffs",
            "implications": "NBA Finals / late playoffs - highest stakes.",
        }
    if m == 5 or (m == 4 and d >= 14):
        return {
            "phase": "playoffs",
            "implications": "Playoff basketball - adjusted for postseason intensity.",
        }
    if m == 4 and d >= 10:
        return {
            "phase": "regular-late",
            "implications": "Play-in race: seeding + tanking incentives mixed.",
        }
    if m == 3 and d >= 15:
        return {
            "phase": "regular-late",
            "implications": "Late regular season: playoff race, rest management.",
        }
    return None


def _compute_injury_impact_pct(inj_list: list) -> float:
    """Map MLB injury list to a 0-1 impact multiplier matching NHL semantic.

    Uses engine.injuries.compute_mlb_injury_impact conventions: each
    starter out subtracts a small fraction. Returns a strength fraction
    (1.0 = no injuries, 0.85 = ~15% weaker, etc).
    """
    if not inj_list:
        return 1.0
    try:
        from engine.injuries import compute_mlb_injury_impact
        # The function takes (team_id, injuries_list) and returns a
        # multiplier in the ~0.70-1.00 range.
        return float(compute_mlb_injury_impact(0, inj_list) or 1.0)
    except Exception:
        # Fallback heuristic: one player out = ~3% weaker, capped.
        return max(0.70, 1.0 - min(len(inj_list) * 0.03, 0.30))


def _implied(ml: int) -> float:
    """Convert American odds to implied probability."""
    if ml < 0:
        return abs(ml) / (abs(ml) + 100)
    return 100 / (ml + 100)


def _find_ou(ou_lines, vegas_total):
    """Find the O/U entry closest to the Vegas total."""
    vt = float(vegas_total)
    for fmt in [str(vt), f"{vt:.1f}", str(int(vt))]:
        if fmt in ou_lines:
            return ou_lines[fmt]
    # Closest
    best_key = min(ou_lines.keys(), key=lambda k: abs(float(k) - vt), default=None)
    return ou_lines.get(best_key) if best_key else None


@app.get("/api/tracker/history")
def api_pick_history():
    """Return recent main-tracker pick history with results.

    Filters out derivative bet types defensively. The MLB derivative
    settler trampoline (engine.derivative_tracker._settle_mlb_derivatives)
    inserts temp rows into the main `picks` table mid-flight to reuse
    settle_picks(), then deletes them in the finally block. During that
    window — observed via fast tab-switching between Pick Tracker and
    Derivative Tracker — the temp rows leak into this endpoint's
    response. Excluding derivative types here makes the leak invisible
    to the UI even if the trampoline race condition fires.
    """
    conn = get_conn()
    placeholders = ",".join("?" * len(_MLB_DERIV_TYPES))
    picks = conn.execute(
        f"SELECT * FROM picks WHERE bet_type NOT IN ({placeholders}) "
        f"ORDER BY created_at DESC LIMIT 50",
        tuple(_MLB_DERIV_TYPES),
    ).fetchall()
    return [dict(p) for p in picks]


# ── Derivative paper-bet tracker (separate from main pick tracker) ──
# Mirrors the per-sport tracker endpoints. Lets the user evaluate
# Phase 1 derivative profitability in isolation; the main tracker
# stays focused on the primary ML/RL/PL/O/U markets.

@app.get("/api/{sport}/derivative-tracker/history")
def api_derivative_history(sport: str):
    if sport not in ("mlb", "nhl", "nba"):
        raise HTTPException(status_code=400, detail="Unknown sport")
    from engine.derivative_tracker import get_history
    return get_history(sport, limit=200)


@app.get("/api/{sport}/derivative-tracker/summary")
def api_derivative_summary(sport: str):
    if sport not in ("mlb", "nhl", "nba"):
        raise HTTPException(status_code=400, detail="Unknown sport")
    from engine.derivative_tracker import get_summary, settle_derivative_picks
    # Auto-settle any pending whose games have completed before
    # returning summary. Mirrors the main tracker's auto-settle on
    # summary fetch.
    try:
        settle_derivative_picks(sport)
    except Exception as e:
        logger.warning("Derivative auto-settle (%s) failed: %s", sport, e)
    return get_summary(sport)


@app.post("/api/{sport}/derivative-tracker/settle")
def api_derivative_settle(sport: str):
    if sport not in ("mlb", "nhl", "nba"):
        raise HTTPException(status_code=400, detail="Unknown sport")
    from engine.derivative_tracker import settle_derivative_picks
    return settle_derivative_picks(sport)


# ── Player props (Phase 2f-iii) ──────────────────────────────
# Props are heavy (4k+ rows for MLB) and only relevant on the Props
# tab — never load them in the cold best-bets path. The tab fetches
# /api/{sport}/props/today on demand. Today only MLB is wired; NBA
# and NHL get their own scraper extensions in 2h / 2i.

@app.get("/api/{sport}/props/today")
def api_props_today(sport: str):
    """Live HR player-prop lines for ``sport``. Returns dict keyed by
    matchup (``AWAY@HOME``) → list of prop rows. 60s cache TTL inside
    the scraper protects upstream from per-tick repeats."""
    if sport != "mlb":
        # NBA / NHL prop scrapers land in 2h / 2i; until then return
        # empty rather than 404 so the frontend can render a "coming
        # soon" empty state without distinguishing per-sport.
        return {}
    from scrapers.hardrock_props import fetch_mlb_props
    try:
        return fetch_mlb_props()
    except Exception as e:
        logger.warning("props fetch failed: %s", e)
        return {}


@app.get("/api/{sport}/props-tracker/summary")
def api_props_summary(sport: str):
    """Settled prop-pick history + pending list. Mirrors the derivative
    summary endpoint shape so the same PicksTable shell renders both.

    Pending and settled are queried separately so a heavy pending slate
    (250+ props on a busy night) can't crowd out the settled history
    that powers the P/L banner totals.
    """
    if sport not in ("mlb", "nhl", "nba"):
        raise HTTPException(status_code=400, detail="Unknown sport")
    from engine.player_props_tracker import settle_player_props
    from engine.player_props_db import list_picks, _conn_for
    # Auto-settle any pending whose game logs have landed before
    # returning the summary — same pattern as /derivative-tracker/summary.
    try:
        settle_player_props(sport)
    except Exception as e:
        logger.warning("player_props auto-settle (%s) failed: %s", sport, e)

    pending = list_picks(sport, pending_only=True, limit=10_000)
    # Settled: pull a generous window so the banner P/L reflects the
    # full history, not just the newest 200 rows. Use a direct query
    # rather than list_picks(limit=200) which bundles pending in.
    conn = _conn_for(sport)
    settled_rows = conn.execute(
        "SELECT * FROM player_props_picks WHERE result IS NOT NULL "
        "ORDER BY date DESC, id DESC LIMIT 1000"
    ).fetchall()
    finished = [dict(r) for r in settled_rows]
    wins   = sum(1 for p in finished if p["result"] == "W")
    losses = sum(1 for p in finished if p["result"] == "L")
    pushes = sum(1 for p in finished if p["result"] == "P")
    profit = sum(float(p.get("profit") or 0) for p in finished)
    return {
        "wins": wins, "losses": losses, "pushes": pushes,
        "total": wins + losses + pushes,
        "win_pct": round(wins / (wins + losses) * 100, 1) if (wins + losses) else 0.0,
        "profit": round(profit, 2),
        "pending": pending,
        "history": finished,
    }


@app.post("/api/{sport}/props-tracker/settle")
def api_props_settle(sport: str):
    if sport not in ("mlb", "nhl", "nba"):
        raise HTTPException(status_code=400, detail="Unknown sport")
    from engine.player_props_tracker import settle_player_props
    return settle_player_props(sport)


@app.get("/api/mlb/first-inning-tracker/summary")
def api_first_inning_summary():
    """Settled + pending 1st INN (NRFI/YRFI) picks, MLB only.
    Mirrors the derivative-tracker summary shape so the same
    PicksTable shell can render it."""
    from engine.db import get_conn as _mlb_conn
    conn = _mlb_conn()
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM picks WHERE bet_type = '1st INN' "
        "ORDER BY date DESC, id DESC LIMIT 200"
    ).fetchall()]
    pending = [r for r in rows if r.get("result") is None]
    finished = [r for r in rows if r.get("result") is not None]
    wins = sum(1 for r in finished if r["result"] == "W")
    losses = sum(1 for r in finished if r["result"] == "L")
    pushes = sum(1 for r in finished if r["result"] == "P")
    profit = sum(float(r.get("profit") or 0) for r in finished)
    # NRFI vs YRFI breakdown so the frontend can show the
    # direction-specific records the user cares about.
    by_dir: dict[str, dict] = {}
    for r in finished:
        d = "NRFI" if "NRFI" in (r.get("pick") or "").upper() else "YRFI"
        s = by_dir.setdefault(d, {"wins": 0, "losses": 0, "profit": 0.0, "total": 0})
        s["total"] += 1
        if r["result"] == "W":
            s["wins"] += 1
        elif r["result"] == "L":
            s["losses"] += 1
        s["profit"] += float(r.get("profit") or 0)
    for s in by_dir.values():
        s["profit"] = round(s["profit"], 2)
        if s["wins"] + s["losses"] > 0:
            s["win_pct"] = round(s["wins"] / (s["wins"] + s["losses"]) * 100, 1)
        else:
            s["win_pct"] = 0.0
    return {
        "wins": wins, "losses": losses, "pushes": pushes,
        "total": wins + losses + pushes,
        "win_pct": round(wins / (wins + losses) * 100, 1) if (wins + losses) else 0.0,
        "profit": round(profit, 2),
        "pending": pending,
        "history": finished,
        "by_direction": by_dir,
    }


@app.get("/api/mlb/first-inning-pick-of-day")
def api_first_inning_potd():
    """Today's top-edge 1st INN pick (NRFI direction only — YRFI
    disabled per engine.config). Returned shape matches the other
    POTD endpoints so PotdHero can render it directly."""
    from engine.db import get_conn as _mlb_conn
    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")
    conn = _mlb_conn()
    row = conn.execute(
        "SELECT * FROM picks WHERE bet_type = '1st INN' AND date = ? "
        "ORDER BY edge DESC LIMIT 1", (today,),
    ).fetchone()
    if not row:
        return {"message": "No qualifying 1st INN pick today."}
    return dict(row)


@app.get("/api/{sport}/player-props/potd")
def api_player_props_potd(sport: str):
    """Today's player-prop Pick-of-the-Day. Locks on first call;
    subsequent calls return the locked POTD even if the slate
    re-ranks. Returns the full pick row joined with POTD metadata,
    or {message} when no qualifying pick exists today."""
    if sport not in ("mlb", "nhl", "nba"):
        raise HTTPException(status_code=400, detail="Unknown sport")
    from engine.player_props_potd import (
        get_or_create_potd, refresh_potd_for_line_movement,
    )
    # Refresh-on-read: tracks HR line drift on the locked POTD's bet
    # so the displayed line / price matches what the user can place.
    # Same player + bet_type + side stays frozen — only line/odds move.
    try:
        refresh_potd_for_line_movement(sport)
    except Exception as e:
        logger.warning("prop POTD line refresh failed for %s: %s", sport, e)
    potd = get_or_create_potd(sport)
    if not potd:
        return {"message": "No qualifying prop POTD today."}
    return potd


@app.get("/api/{sport}/derivative-pick-of-day")
def api_derivative_potd(sport: str):
    """Today's derivative POTD per sport. Locks on first call (uses
    today's best-bets to select); subsequent calls return the locked
    pick from the per-sport derivative_pick_of_day table."""
    if sport not in ("mlb", "nhl", "nba"):
        raise HTTPException(status_code=400, detail="Unknown sport")
    from engine.derivative_tracker import (
        get_today_derivative_potd, get_or_create_derivative_potd,
    )
    existing = get_today_derivative_potd(sport)
    if existing:
        return existing
    if sport == "mlb":
        bets = api_best_bets()
    elif sport == "nhl":
        bets = api_nhl_best_bets()
    elif sport == "nba":
        bets = api_nba_best_bets()
    else:
        bets = []
    if isinstance(bets, list):
        return get_or_create_derivative_potd(sport, bets) or {
            "message": "No qualifying derivative picks today", "sport": sport,
        }
    return {"error": "Could not generate bets"}


@app.get("/api/teams/{team_id}/profile")
def api_team_profile(team_id: int):
    """Full team profile with stats, recent games, and form."""
    team = get_team_by_id(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")

    record = get_team_record(team_id, SEASON) or {}
    recent = get_recent_games(team_id, 15)
    bp = get_bullpen(team_id, SEASON) or {}

    # PIT stats for deeper analysis
    from engine.pit_stats import compute_team_stats_at_date
    today = datetime.now().strftime("%Y-%m-%d")
    pit = compute_team_stats_at_date(team_id, today, SEASON)

    # Get roster pitchers
    conn = get_conn()
    pitchers = conn.execute("""
        SELECT p.mlb_id, p.name, p.throws,
               ps.era, ps.whip, ps.k_per_9, ps.wins, ps.losses, ps.innings
        FROM players p
        LEFT JOIN pitcher_stats ps ON p.mlb_id = ps.player_id AND ps.season = ?
        WHERE p.team_id = ? AND p.position = 'P' AND p.active = 1
        ORDER BY ps.innings DESC
    """, (SEASON, team_id)).fetchall()

    return {
        "team": team,
        "record": record,
        "pit_stats": pit,
        "bullpen": bp,
        "recent_games": recent,
        "pitchers": [dict(p) for p in pitchers],
    }


@app.post("/api/calibrate")
def api_calibrate(days: int = Query(default=30)):
    """Run model self-calibration on recent games."""
    from engine.calibration import calibrate
    from engine.mlb_predict import reload_weights
    report = calibrate(days=days)
    reload_weights()  # Refresh cached weights
    return report


@app.get("/api/calibration/status")
def api_calibration_status():
    """Return current model weights and calibration info."""
    from engine.calibration import get_calibration_status
    return get_calibration_status()


@app.post("/api/calibrate/teams")
def api_calibrate_teams():
    """Run per-team calibration."""
    from engine.team_calibration import calibrate_teams
    return calibrate_teams()


@app.get("/api/debug/odds")
def api_debug_odds():
    """Test all odds sources."""
    result = {}

    # Test The Odds API
    try:
        from scrapers.odds_api import fetch_odds, _get_api_key, KEY_FILE
        key = _get_api_key()
        result["odds_api"] = {
            "key_found": key is not None,
            "key_file": str(KEY_FILE),
            "key_file_exists": KEY_FILE.exists(),
            "key_preview": key[:8] + "..." if key else None,
        }
        if key:
            odds = fetch_odds()
            result["odds_api"]["games_found"] = len(odds)
            result["odds_api"]["sample"] = dict(list(odds.items())[:1]) if odds else None
    except Exception as e:
        result["odds_api"] = {"error": str(e)}

    # Test ESPN
    try:
        from scrapers.espn_odds import fetch_game_odds
        games = _get_scoreboard()
        if games:
            espn = fetch_game_odds(games[0].get("id"))
            result["espn"] = {"game": games[0].get("short_name", ""), "odds": espn}
    except Exception as e:
        result["espn"] = {"error": str(e)}

    return result


@app.get("/api/debug/teams")
def api_debug_teams():
    """Debug: dump raw team data to see league/division values."""
    conn = get_conn()
    rows = conn.execute("SELECT mlb_id, abbreviation, name, league, division FROM teams").fetchall()
    return [dict(r) for r in rows]


@app.get("/api/backtest")
def api_backtest(days: int = Query(default=0), min_edge: float = Query(default=3),
                 season: int = Query(default=0)):
    """Run model backtest against historical games."""
    from engine.backtest import run_backtest

    yr = season if season > 0 else None

    # If requesting a past season, auto-load the data if not already present
    if yr:
        conn = get_conn()
        game_count = conn.execute(
            "SELECT COUNT(*) as c FROM games WHERE season = ? AND status = 'final'",
            (yr,)
        ).fetchone()["c"]
        if game_count < 100:
            # Need to fetch this season's data first
            logger.info("Loading %d season data for backtest...", yr)
            from scrapers.mlb_stats import fetch_teams, fetch_season_results
            fetch_teams()
            fetch_season_results(season=yr)
            game_count = conn.execute(
                "SELECT COUNT(*) as c FROM games WHERE season = ? AND status = 'final'",
                (yr,)
            ).fetchone()["c"]
            logger.info("Loaded %d games for %d", game_count, yr)

    results = run_backtest(
        season=yr,
        days=days if days > 0 else None,
        min_edge=min_edge,
    )
    # Don't send full game log to frontend (too large)
    results.pop("game_log", None)
    return results


@app.post("/api/tracker/record")
def api_record_picks():
    """Record today's model picks."""
    try:
        from engine.tracker import record_picks
        picks = record_picks()
        return {"recorded": len(picks), "picks": picks}
    except Exception as e:
        logger.error("Record picks failed: %s", e, exc_info=True)
        return {"error": str(e), "recorded": 0}


@app.post("/api/tracker/settle")
def api_settle_picks():
    """Settle completed picks + POTD against final scores."""
    try:
        from engine.tracker import settle_picks
        result = settle_picks()
        # Also settle MLB POTD
        try:
            from engine.pick_of_day import settle_potd
            settle_potd("mlb")
        except Exception as e:
            logger.warning("MLB POTD settle failed: %s", e)
        return result
    except Exception as e:
        logger.error("Settle picks failed: %s", e, exc_info=True)
        return {"error": str(e), "settled": 0}


@app.post("/api/mlb/refresh-lineups")
def api_refresh_mlb_lineups(date: str | None = Query(None),
                             record: bool = Query(True)):
    """Re-check confirmed MLB lineups for `date` (defaults to today) and
    invalidate picks for games whose lineup has changed since the last
    snapshot. Intended to run 2-3h before first pitch via Windows Task
    Scheduler / cron so day-of scratches and late lineup shuffles
    don't strand a stale pick on the card.

    Query params:
      date: YYYY-MM-DD (defaults to today)
      record: when True (default), re-run tracker.record_picks(force=True)
              so the recorded rows reflect the confirmed lineup too.
    """
    try:
        from engine.lineup_refresh import refresh_for_date
        return refresh_for_date(date=date, record_picks=record)
    except Exception as e:
        logger.error("lineup refresh failed: %s", e, exc_info=True)
        return {"error": str(e), "deltas": 0, "invalidated": 0}


@app.get("/api/tracker/summary")
def api_pick_summary():
    """Get running pick totals."""
    from engine.tracker import get_pick_summary
    return get_pick_summary()


@app.get("/api/standings")
def api_standings():
    """Return MLB standings grouped by division."""
    conn = get_conn()

    # Debug: check what's in the DB
    team_count = conn.execute("SELECT COUNT(*) as c FROM teams").fetchone()["c"]
    ts_count = conn.execute("SELECT COUNT(*) as c FROM team_stats").fetchone()["c"]
    with_league = conn.execute(
        "SELECT COUNT(*) as c FROM teams WHERE league IS NOT NULL AND league != ''"
    ).fetchone()["c"]
    sample = conn.execute(
        "SELECT abbreviation, league, division FROM teams LIMIT 3"
    ).fetchall()
    logger.info("Standings: %d teams, %d stats, %d with league. Sample: %s",
                team_count, ts_count, with_league,
                [(dict(r)) for r in sample])

    if team_count == 0:
        return []

    rows = conn.execute("""
        SELECT t.mlb_id, t.name, t.abbreviation, t.league, t.division,
               ts.wins, ts.losses, ts.run_diff, ts.streak,
               ts.last_10_wins, ts.last_10_losses,
               ts.home_wins, ts.home_losses, ts.away_wins, ts.away_losses,
               ts.era, ts.ops, ts.wrc_plus
        FROM teams t
        LEFT JOIN team_stats ts ON t.mlb_id = ts.team_id AND ts.season = ?
        ORDER BY t.league, t.division, ts.wins DESC
    """, (SEASON,)).fetchall()

    logger.info("Standings query returned %d rows", len(rows))
    # Log first few rows to see actual values
    for r in rows[:3]:
        logger.info("  Row: %s league='%s' division='%s' wins=%s",
                    r["abbreviation"], r["league"], r["division"], r["wins"])

    if not rows:
        return []

    divisions = {}
    for r in rows:
        league = (r["league"] or "").strip()
        division = (r["division"] or "").strip()
        if not league or not division:
            logger.warning("  Skipping %s: league='%s' division='%s'",
                          r["abbreviation"], league, division)
            continue
        div_key = f"{league} {division}"
        if div_key not in divisions:
            divisions[div_key] = {
                "league": league,
                "division": division,
                "teams": [],
            }
        w = r["wins"] or 0
        l = r["losses"] or 0
        divisions[div_key]["teams"].append({
            "id": r["mlb_id"],
            "name": r["name"],
            "abbreviation": r["abbreviation"],
            "wins": w,
            "losses": l,
            "pct": f".{int(w / (w + l) * 1000):03d}" if (w + l) > 0 else ".000",
            "run_diff": r["run_diff"] or 0,
            "streak": r["streak"] or "",
            "last_10": f"{r['last_10_wins'] or 0}-{r['last_10_losses'] or 0}",
            "home": f"{r['home_wins'] or 0}-{r['home_losses'] or 0}",
            "away": f"{r['away_wins'] or 0}-{r['away_losses'] or 0}",
            "era": r["era"],
            "ops": r["ops"],
            "wrc_plus": r["wrc_plus"],
        })

    return list(divisions.values())


@app.get("/api/pitcher/{pitcher_id}")
def api_pitcher(pitcher_id: int):
    """Return pitcher stats and recent starts."""
    from engine.db import get_pitcher_recent_starts
    conn = get_conn()

    player = conn.execute(
        "SELECT * FROM players WHERE mlb_id = ?", (pitcher_id,)
    ).fetchone()
    if not player:
        raise HTTPException(status_code=404, detail="Pitcher not found")

    stats = get_pitcher_season(pitcher_id, SEASON)
    recent = get_pitcher_recent_starts(pitcher_id, 5)

    return {
        "player": dict(player),
        "stats": dict(stats) if stats else None,
        "recent_starts": recent,
    }


# ══════════════════════════════════════════════════════════════
#  NHL ENDPOINTS
# ══════════════════════════════════════════════════════════════

# NHL team name → abbreviation (for Odds API matching)
_NHL_TEAM_ABBR = {
    "Anaheim Ducks": "ANA", "Arizona Coyotes": "ARI", "Utah Hockey Club": "UTA", "Utah Mammoth": "UTA",
    "Boston Bruins": "BOS", "Buffalo Sabres": "BUF",
    "Calgary Flames": "CGY", "Carolina Hurricanes": "CAR",
    "Chicago Blackhawks": "CHI", "Colorado Avalanche": "COL",
    "Columbus Blue Jackets": "CBJ", "Dallas Stars": "DAL",
    "Detroit Red Wings": "DET", "Edmonton Oilers": "EDM",
    "Florida Panthers": "FLA", "Los Angeles Kings": "LAK",
    "Minnesota Wild": "MIN", "Montreal Canadiens": "MTL",
    "Nashville Predators": "NSH", "New Jersey Devils": "NJD",
    "New York Islanders": "NYI", "New York Rangers": "NYR",
    "Ottawa Senators": "OTT", "Philadelphia Flyers": "PHI",
    "Pittsburgh Penguins": "PIT", "San Jose Sharks": "SJS",
    "Seattle Kraken": "SEA", "St. Louis Blues": "STL",
    "Tampa Bay Lightning": "TBL", "Toronto Maple Leafs": "TOR",
    "Vancouver Canucks": "VAN", "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH", "Winnipeg Jets": "WPG",
}

_NHL_ABBR_ALTS = {
    "TB": "TBL", "TBL": "TB",
    "NJ": "NJD", "NJD": "NJ",
    "SJ": "SJS", "SJS": "SJ",
    "LA": "LAK", "LAK": "LA",
    "WAS": "WSH", "WSH": "WAS",
    "CLB": "CBJ", "CBJ": "CLB",
    "MON": "MTL", "MTL": "MON",
    "NAS": "NSH", "NSH": "NAS",
    "AZ": "UTA", "UTA": "AZ",
    "UTAH": "UTA", "UTH": "UTA",
}

# Map ESPN abbreviation to team JSON key
_NHL_ESPN_TO_KEY = {}  # populated lazily

def _nhl_espn_to_key() -> dict:
    """Build mapping from ESPN abbreviation to JSON file key."""
    if _NHL_ESPN_TO_KEY:
        return _NHL_ESPN_TO_KEY
    from engine.data import list_teams, load_team
    for t in list_teams("NHL"):
        team = load_team("NHL", t["key"])
        if team:
            abbr = team.get("abbreviation", "")
            if abbr:
                _NHL_ESPN_TO_KEY[abbr] = t["key"]
                # Add alternate abbreviations
                alt = _NHL_ABBR_ALTS.get(abbr)
                if alt:
                    _NHL_ESPN_TO_KEY[alt] = t["key"]
            _NHL_ESPN_TO_KEY[t["key"]] = t["key"]
            # Also map by short name (e.g. "Bruins" -> "bruins")
            short = team.get("short_name", "")
            if short:
                _NHL_ESPN_TO_KEY[short.lower()] = t["key"]
            # Also map by last word of name (e.g. "Mammoth" for Utah Mammoth)
            name = team.get("name", "")
            if name:
                last_word = name.split()[-1].lower()
                if last_word not in _NHL_ESPN_TO_KEY:
                    _NHL_ESPN_TO_KEY[last_word] = t["key"]
    return _NHL_ESPN_TO_KEY


def _nhl_alt_abbr(abbr: str) -> str:
    return _NHL_ABBR_ALTS.get(abbr, abbr)



def _get_nhl_scoreboard(date: str = "") -> list[dict]:
    """Fetch NHL scoreboard from ESPN."""
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    espn_date = target_date.replace("-", "")

    cache_key = f"nhl:{espn_date}"
    now = time.time()
    if cache_key in _scoreboard_cache:
        ts, cached = _scoreboard_cache[cache_key]
        if now - ts < CACHE_TTL:
            return cached

    url = f"{ESPN_BASE}/hockey/nhl/scoreboard?dates={espn_date}"
    logger.info("Fetching NHL scoreboard: %s", url)
    espn_data = _fetch_espn_json(url)

    games = []
    if espn_data:
        events = espn_data.get("events", [])
        logger.info("ESPN NHL returned %d events", len(events))
        games = _parse_nhl_scoreboard(espn_data)

    # Yesterday-still-live carryover. Same rationale as NBA: late
    # games that cross midnight UTC drop off today's slate while
    # they're still in progress.
    if date == "":
        from datetime import timedelta as _td
        yest = (datetime.now() - _td(days=1)).strftime("%Y%m%d")
        try:
            yest_data = _fetch_espn_json(
                f"{ESPN_BASE}/hockey/nhl/scoreboard?dates={yest}")
            if yest_data:
                yest_games = _parse_nhl_scoreboard(yest_data)
                seen_ids = {g.get("id") for g in games}
                for g in yest_games:
                    state = (g.get("status") or {}).get("state", "")
                    if state == "in" and g.get("id") not in seen_ids:
                        games.append(g)
                        logger.info("NHL: carried over live game from "
                                    "yesterday: %s", g.get("id"))
        except Exception as e:
            logger.debug("NHL yesterday-live carryover failed: %s", e)

    # Fallback without date
    if not games and date == "":
        espn_data = _fetch_espn_json(f"{ESPN_BASE}/hockey/nhl/scoreboard")
        if espn_data:
            games = _parse_nhl_scoreboard(espn_data)

    # If still no games, try tomorrow
    if not games and date == "":
        from datetime import timedelta as _td
        tomorrow = (datetime.now() + _td(days=1)).strftime("%Y%m%d")
        espn_data = _fetch_espn_json(f"{ESPN_BASE}/hockey/nhl/scoreboard?dates={tomorrow}")
        if espn_data:
            games = _parse_nhl_scoreboard(espn_data)

    # ── Hard Rock fallback ──────────────────────────────────────
    # When ESPN hasn't posted today's schedule at all (zero events),
    # build minimal scoreboard entries from Hard Rock. Only triggers
    # when ESPN returned nothing — NOT when all games are live/done
    # (that would pull in tomorrow's games from HR).
    if not games and date == "":
        try:
            from scrapers.hardrock_odds import fetch_nhl as _hr_nhl
            hr_odds = _hr_nhl()
            if hr_odds:
                logger.info("NHL: ESPN empty, building %d games from Hard Rock",
                            len(hr_odds))
                for matchup_key, odds_data in hr_odds.items():
                    parts = matchup_key.split("@")
                    if len(parts) != 2:
                        continue
                    a_abbr, h_abbr = parts
                    existing = any(
                        g["home"]["abbreviation"] == h_abbr and
                        g["away"]["abbreviation"] == a_abbr
                        for g in games
                    )
                    if existing:
                        continue
                    games.append({
                        "id": f"hr_{a_abbr}_{h_abbr}",
                        "date": target_date,
                        "home": {
                            "abbreviation": h_abbr,
                            "name": h_abbr,
                            "score": None,
                        },
                        "away": {
                            "abbreviation": a_abbr,
                            "name": a_abbr,
                            "score": None,
                        },
                        "status": {"state": "pre", "detail": "Scheduled"},
                        "odds": odds_data,
                        "source": "hardrock",
                    })
        except Exception as e:
            logger.debug("NHL Hard Rock fallback failed: %s", e)

    # Fetch NHL odds from The Odds API
    try:
        nhl_odds = _fetch_nhl_odds()
        if nhl_odds:
            matched = 0
            for game in games:
                h = game["home"]["abbreviation"]
                a = game["away"]["abbreviation"]
                key = f"{a}@{h}"
                from engine.picks import match_odds as _match
                matched_odds = _match(h, a, nhl_odds)
                if matched_odds:
                    game["odds"] = matched_odds
                    matched += 1
            logger.info("NHL odds: matched %d/%d games", matched, len(games))

            # Store odds snapshots for historical backtesting
            try:
                from engine.odds_history import store_nhl_odds
                odds_rows = []
                for game in games:
                    if game.get("odds"):
                        odds_rows.append({
                            "game_date": target_date,
                            "home_abbr": game["home"]["abbreviation"],
                            "away_abbr": game["away"]["abbreviation"],
                            "odds": game["odds"],
                        })
                if odds_rows:
                    store_nhl_odds(odds_rows)
            except Exception as e:
                logger.debug("Odds history storage failed: %s", e)

            # Compare current odds against tracked opening odds for line movement.
            # If we haven't seen this matchup yet, store the opening snapshot.
            try:
                from engine.line_movement import get_line_movement, track_opening_odds
                for game in games:
                    if not game.get("odds"):
                        continue
                    h_abbr = game["home"]["abbreviation"]
                    a_abbr = game["away"]["abbreviation"]
                    game_date = (game.get("date", "") or "")[:10] or target_date
                    key = f"{game_date}_{a_abbr}@{h_abbr}"
                    movement = get_line_movement("nhl", key, game["odds"])
                    if movement:
                        game["line_movement"] = movement
                    else:
                        track_opening_odds("nhl", key, game["odds"])
            except Exception as e:
                logger.debug("NHL line movement failed: %s", e)
    except Exception as e:
        logger.warning("NHL odds failed: %s", e)

    # Enrich with starting goalies - try DailyFaceoff first, then NHL API
    goalie_count = 0
    try:
        from scrapers.dailyfaceoff import get_starting_goalies
        df_goalies = get_starting_goalies()
        if df_goalies:
            for game in games:
                h = game["home"]["abbreviation"]
                a = game["away"]["abbreviation"]
                for abbr_try, side in [(h, "home_goalie"), (a, "away_goalie")]:
                    for try_abbr in [abbr_try, _nhl_alt_abbr(abbr_try)]:
                        if try_abbr in df_goalies:
                            game[side] = df_goalies[try_abbr]
                            goalie_count += 1
                            break
    except Exception as e:
        logger.debug("DailyFaceoff failed: %s", e)

    # Fallback: NHL API for goalies if DailyFaceoff didn't work
    if goalie_count == 0:
        try:
            nhl_schedule = _fetch_espn_json("https://api-web.nhle.com/v1/score/now")
            if nhl_schedule and nhl_schedule.get("games"):
                for nhl_game in nhl_schedule["games"]:
                    # Extract team abbreviations
                    def _gs(obj):
                        return obj.get("default", "") if isinstance(obj, dict) else str(obj) if obj else ""

                    h_abbr = _gs(nhl_game.get("homeTeam", {}).get("abbrev", ""))
                    a_abbr = _gs(nhl_game.get("awayTeam", {}).get("abbrev", ""))

                    # Match to our scoreboard games
                    for game in games:
                        gh = game["home"]["abbreviation"]
                        ga = game["away"]["abbreviation"]
                        if (gh == h_abbr or _nhl_alt_abbr(gh) == h_abbr) and \
                           (ga == a_abbr or _nhl_alt_abbr(ga) == a_abbr):
                            # Home goalie
                            hg = nhl_game.get("homeTeam", {}).get("goalie", {})
                            if not hg:
                                # Try alternate field names
                                hg = nhl_game.get("homeTeam", {}).get("startingGoalie", {})
                            if hg and hg.get("id"):
                                first = _gs(hg.get("firstName", ""))
                                last = _gs(hg.get("lastName", ""))
                                game["home_goalie"] = {
                                    "name": f"{first} {last}".strip(),
                                    "status": "expected",
                                    "id": hg.get("id"),
                                }

                            # Away goalie
                            ag = nhl_game.get("awayTeam", {}).get("goalie", {})
                            if not ag:
                                ag = nhl_game.get("awayTeam", {}).get("startingGoalie", {})
                            if ag and ag.get("id"):
                                first = _gs(ag.get("firstName", ""))
                                last = _gs(ag.get("lastName", ""))
                                game["away_goalie"] = {
                                    "name": f"{first} {last}".strip(),
                                    "status": "expected",
                                    "id": ag.get("id"),
                                }
                            break
        except Exception as e:
            logger.debug("NHL API goalie fallback failed: %s", e)

    # ── Enrich with playoff series context ──
    try:
        from engine.series_context import infer_series
        from engine.nhl_predict import _is_playoff_window
        if _is_playoff_window():
            for game in games:
                h = game["home"]["abbreviation"]
                a = game["away"]["abbreviation"]
                series = infer_series("nhl", h, a)
                if series.get("in_series"):
                    game["series"] = series
    except Exception as e:
        logger.debug("NHL series enrichment failed: %s", e)

    _scoreboard_cache[cache_key] = (now, games)
    # Evict oldest entries if cache grows too large
    if len(_scoreboard_cache) > MAX_CACHE_ENTRIES:
        oldest = min(_scoreboard_cache, key=lambda k: _scoreboard_cache[k][0])
        del _scoreboard_cache[oldest]
    return games


def _parse_nhl_scoreboard(data: dict) -> list[dict]:
    """Parse ESPN NHL scoreboard into game objects."""
    events = data.get("events", [])
    games = []

    for event in events:
        comps = event.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue

        status = comp.get("status", {})
        status_type = status.get("type", {})

        home_team = None
        away_team = None
        for c in competitors:
            team = c.get("team", {})
            raw_score = c.get("score", "0")
            if isinstance(raw_score, dict):
                score = raw_score.get("displayValue", raw_score.get("value", "0"))
            else:
                score = str(raw_score)

            # Parse record - NHL has W-L-OTL format
            record = ""
            if c.get("records"):
                record = c["records"][0].get("summary", "")

            entry = {
                "espn_id": team.get("id", ""),
                "name": team.get("displayName", team.get("name", "")),
                "abbreviation": team.get("abbreviation", ""),
                "score": score,
                "record": record,
                "logo": "",
                "winner": c.get("winner", False),
            }

            logo = team.get("logo", "")
            if isinstance(logo, str) and logo:
                entry["logo"] = logo
            elif isinstance(logo, dict):
                entry["logo"] = logo.get("href", "")
            else:
                logos = team.get("logos", [])
                if logos and isinstance(logos, list):
                    first = logos[0]
                    entry["logo"] = first.get("href", "") if isinstance(first, dict) else (first if isinstance(first, str) else "")

            if c.get("homeAway") == "home":
                home_team = entry
            else:
                away_team = entry

        if not home_team or not away_team:
            continue

        game = {
            "id": event.get("id", ""),
            "date": event.get("date", ""),
            "name": event.get("name", ""),
            "short_name": event.get("shortName", ""),
            "home": home_team,
            "away": away_team,
            "status": {
                "state": status_type.get("state", "pre"),
                "detail": status_type.get("shortDetail",
                          status_type.get("detail", "")),
                "description": status_type.get("description", ""),
                "completed": status_type.get("completed", False),
                "period": status.get("period", 0),
            },
            "venue": comp.get("venue", {}).get("fullName", ""),
            "broadcast": "",
            "odds": None,
        }

        # Broadcast
        broadcasts = comp.get("broadcasts", [])
        if broadcasts:
            names = []
            for b in broadcasts:
                for n in b.get("names", []):
                    names.append(n)
            game["broadcast"] = ", ".join(names[:2])

        # ESPN inline odds
        odds = comp.get("odds", [])
        if odds:
            o = odds[0]
            home_odds = o.get("homeTeamOdds", {}) or {}
            away_odds = o.get("awayTeamOdds", {}) or {}
            game["odds"] = {
                "spread": o.get("details", ""),
                "over_under": o.get("overUnder"),
                "home_ml": home_odds.get("moneyLine"),
                "away_ml": away_odds.get("moneyLine"),
                "home_spread_odds": home_odds.get("spreadOdds"),
                "away_spread_odds": away_odds.get("spreadOdds"),
                "home_spread_point": home_odds.get("spreadLine") or home_odds.get("line"),
                "away_spread_point": away_odds.get("spreadLine") or away_odds.get("line"),
                "over_odds": o.get("overOdds"),
                "under_odds": o.get("underOdds"),
                "provider": "ESPN",
            }

        games.append(game)

    return games


# NHL odds cache (separate from MLB)
_nhl_odds_cache: dict | None = None
_nhl_odds_cache_time: float = 0

# Negative-result TTL: when The Odds API is 401 / quota-exhausted,
# DON'T retry it on every best-bets tick. Cache the empty dict for
# NHL_EMPTY_CACHE_TTL seconds so we fall through to the Hard Rock / DK
# chain instead of hammering the API + burning wall-clock per request.
NHL_EMPTY_CACHE_TTL = 120


def _fetch_nhl_odds() -> dict:
    """Fetch NHL odds with multi-source fallback:

        1. Hard Rock Bet (preferred primary, free, no quota)
        2. The Odds API (original source, monthly credit-gated)
        3. DraftKings public sportsbook API (free fallback)

    Merges the first non-empty source. Cached for 10 min; 2 min for
    empty results so a 401/exhaustion on Odds API doesn't hammer it.
    """
    global _nhl_odds_cache, _nhl_odds_cache_time

    if _nhl_odds_cache is not None:
        age = time.time() - _nhl_odds_cache_time
        ttl = 600 if _nhl_odds_cache else NHL_EMPTY_CACHE_TTL
        if age < ttl:
            return _nhl_odds_cache

    merged: dict = {}

    # 1. Hard Rock primary
    try:
        from scrapers.hardrock_odds import fetch_nhl as _hr_nhl
        hr = _hr_nhl()
        if hr:
            merged.update(hr)
            logger.info("NHL odds: %d games from Hard Rock", len(hr))
    except Exception as e:
        logger.debug("Hard Rock NHL odds failed: %s", e)

    # 2. The Odds API (only if Hard Rock didn't cover the slate)
    if not merged:
        import os
        from pathlib import Path
        key_file = Path(__file__).resolve().parent.parent / "data" / "odds_api_key.txt"
        api_key = os.environ.get("ODDS_API_KEY") or (key_file.read_text().strip() if key_file.exists() else None)
        if api_key:
            oddsapi_result = _fetch_nhl_odds_oddsapi(api_key)
            if oddsapi_result:
                merged.update(oddsapi_result)
        else:
            logger.debug("NHL: no Odds API key configured, skipping that source")

    # 3. DraftKings public API (ultimate fallback)
    if not merged:
        try:
            from scrapers.nhl_dk_odds import fetch_nhl_dk_odds
            dk = fetch_nhl_dk_odds()
            if dk:
                merged.update(dk)
                logger.info("NHL odds fallback: merged %d DK games", len(dk))
        except Exception as e:
            logger.debug("DK NHL odds fallback failed: %s", e)

    _nhl_odds_cache = merged
    _nhl_odds_cache_time = time.time()
    return merged


def _fetch_nhl_odds_oddsapi(api_key: str) -> dict:
    """Original Odds API path for NHL. Extracted so the multi-source
    `_fetch_nhl_odds` stays readable."""
    url = (f"https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds/"
           f"?apiKey={api_key}"
           f"&regions=us"
           f"&markets=h2h,spreads,totals"
           f"&oddsFormat=american"
           f"&bookmakers=draftkings")

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "NHLPredictionEngine/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            remaining = resp.headers.get("x-requests-remaining", "?")
            logger.info("NHL Odds API: %s requests remaining", remaining)
    except Exception as e:
        logger.warning("NHL Odds API failed: %s", e)
        return {}

    if not data or not isinstance(data, list):
        return {}

    odds_map = {}
    for game in data:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        h_abbr = _NHL_TEAM_ABBR.get(home, home[:3].upper())
        a_abbr = _NHL_TEAM_ABBR.get(away, away[:3].upper())
        key = f"{a_abbr}@{h_abbr}"

        result = {"provider": "DraftKings"}
        bookmakers = game.get("bookmakers", [])
        if not bookmakers:
            continue

        book = bookmakers[0]
        for market in book.get("markets", []):
            mkey = market.get("key", "")
            outcomes = market.get("outcomes", [])

            if mkey == "h2h":
                for o in outcomes:
                    if o.get("name") == home:
                        result["home_ml"] = o.get("price", 0)
                    elif o.get("name") == away:
                        result["away_ml"] = o.get("price", 0)
            elif mkey == "spreads":
                for o in outcomes:
                    if o.get("name") == home:
                        result["home_spread_odds"] = o.get("price", 0)
                        result["home_spread_point"] = o.get("point", 0)
                    elif o.get("name") == away:
                        result["away_spread_odds"] = o.get("price", 0)
                        result["away_spread_point"] = o.get("point", 0)
            elif mkey == "totals":
                for o in outcomes:
                    name = o.get("name", "").lower()
                    if "over" in name:
                        result["over_odds"] = o.get("price", 0)
                        result["over_under"] = o.get("point", 0)
                    elif "under" in name:
                        result["under_odds"] = o.get("price", 0)

        if result.get("home_ml"):
            odds_map[key] = result

    return odds_map


@app.post("/api/nhl/sync")
def api_nhl_sync():
    """Refresh NHL team data from ESPN."""
    try:
        from scrapers.espn import scrape_league
        updated = scrape_league("hockey", "nhl", "NHL")
        # Clear the key cache so it rebuilds
        _NHL_ESPN_TO_KEY.clear()
        return {"status": "ok", "updated": len(updated)}
    except Exception as e:
        logger.error("NHL sync failed: %s", e)
        return {"status": "error", "message": str(e)}


@app.get("/api/nhl/scoreboard")
def api_nhl_scoreboard(date: str = Query(default="")):
    """Return today's NHL games."""
    return _get_nhl_scoreboard(date)


@app.get("/api/nhl/standings")
def api_nhl_standings():
    """Return NHL standings from the official NHL API."""
    # Primary: NHL Stats API (api-web.nhle.com)
    nhl_url = "https://api-web.nhle.com/v1/standings/now"
    data = _fetch_espn_json(nhl_url)  # reuse fetch helper

    if data and data.get("standings"):
        return _parse_nhl_api_standings(data)

    # Fallback: local team JSON files
    logger.warning("NHL API standings failed, using JSON fallback")
    return _nhl_standings_from_json()


def _parse_nhl_api_standings(data: dict) -> list[dict]:
    """Parse standings from api-web.nhle.com/v1/standings/now."""
    divisions = {}

    for entry in data.get("standings", []):
        div = entry.get("divisionName", "Unknown")

        # Team name - use teamCommonName (e.g. "Avalanche") + placeName (e.g. "Colorado")
        # teamName.default often contains the full name already, so avoid doubling
        def _nhl_str(obj):
            if isinstance(obj, dict):
                return obj.get("default", "")
            return str(obj) if obj else ""

        team_abbr = _nhl_str(entry.get("teamAbbrev", ""))
        team_logo = entry.get("teamLogo", "")
        place = _nhl_str(entry.get("placeName", ""))
        common_name = _nhl_str(entry.get("teamCommonName", ""))
        team_name = _nhl_str(entry.get("teamName", ""))

        # Use "Place CommonName" (e.g. "Colorado Avalanche")
        # Fall back to teamName if commonName not available
        if common_name:
            full_name = f"{place} {common_name}".strip()
        elif team_name and place and not team_name.startswith(place):
            full_name = f"{place} {team_name}".strip()
        else:
            full_name = team_name or place

        wins = entry.get("wins", 0)
        losses = entry.get("losses", 0)
        otl = entry.get("otLosses", 0)
        points = entry.get("points", 0)
        gf = entry.get("goalFor", 0)
        ga = entry.get("goalAgainst", 0)
        diff = entry.get("goalDifferential", 0)

        streak_code = entry.get("streakCode", "")
        streak_count = entry.get("streakCount", 0)
        streak = f"{streak_code}{streak_count}" if streak_code else ""

        l10w = entry.get("l10Wins", 0)
        l10l = entry.get("l10Losses", 0)
        l10o = entry.get("l10OtLosses", 0)
        l10 = f"{l10w}-{l10l}-{l10o}"

        hw = entry.get("homeWins", 0)
        hl = entry.get("homeLosses", 0)
        ho = entry.get("homeOtLosses", 0)
        home = f"{hw}-{hl}-{ho}"

        rw = entry.get("roadWins", 0)
        rl = entry.get("roadLosses", 0)
        ro = entry.get("roadOtLosses", 0)
        away = f"{rw}-{rl}-{ro}"

        team_entry = {
            "name": full_name,
            "abbreviation": team_abbr,
            "logo": team_logo,
            "record": f"{wins}-{losses}-{otl}",
            "wins": wins,
            "losses": losses,
            "otl": otl,
            "points": points,
            "gf": gf,
            "ga": ga,
            "diff": diff,
            "streak": streak,
            "home": home,
            "away": away,
            "l10": l10,
        }

        if div not in divisions:
            divisions[div] = {"name": div, "teams": []}
        divisions[div]["teams"].append(team_entry)

    # Sort each division by points
    for div in divisions.values():
        div["teams"].sort(key=lambda t: t["points"], reverse=True)

    return list(divisions.values())


def _nhl_standings_from_json() -> list[dict]:
    """Build NHL standings from local team JSON files as fallback."""
    from engine.data import list_teams, load_team

    # NHL division assignments
    _DIVISIONS = {
        "Atlantic": ["BOS", "BUF", "DET", "FLA", "MTL", "OTT", "TBL", "TB", "TOR"],
        "Metropolitan": ["CAR", "CBJ", "CLB", "NJD", "NJ", "NYI", "NYR", "PHI", "PIT", "WSH", "WAS"],
        "Central": ["CHI", "COL", "DAL", "MIN", "NSH", "NAS", "STL", "UTA", "AZ", "WPG"],
        "Pacific": ["ANA", "CGY", "EDM", "LAK", "LA", "SEA", "SJS", "SJ", "VAN", "VGK"],
    }

    # Reverse lookup: abbr -> division
    abbr_to_div = {}
    for div, abbrs in _DIVISIONS.items():
        for a in abbrs:
            abbr_to_div[a] = div

    divisions = {}
    for t in list_teams("NHL"):
        team = load_team("NHL", t["key"])
        if not team:
            continue

        abbr = team.get("abbreviation", "")
        div = abbr_to_div.get(abbr, "Unknown")

        # Parse record "W-L-OTL"
        record = team.get("record", "")
        parts = record.split("-") if record else []
        wins = int(parts[0]) if len(parts) > 0 and parts[0].isdigit() else 0
        losses = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        otl = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        points = wins * 2 + otl

        entry = {
            "name": team.get("name", t["key"]),
            "abbreviation": abbr,
            "logo": "",
            "record": record,
            "wins": wins,
            "losses": losses,
            "otl": otl,
            "points": points,
            "gf": 0,
            "ga": 0,
            "diff": 0,
            "streak": "",
            "home": "",
            "away": "",
            "l10": "",
        }

        if div not in divisions:
            divisions[div] = {"name": div, "teams": []}
        divisions[div]["teams"].append(entry)

    # Sort teams by points
    for div in divisions.values():
        div["teams"].sort(key=lambda t: t["points"], reverse=True)

    print(f"[NHL STANDINGS] Fallback: {len(divisions)} divisions from JSON", flush=True)
    return list(divisions.values())


@app.get("/api/nhl/predict")
def api_nhl_predict(home: str = Query(...), away: str = Query(...)):
    """
    Run NHL prediction. home/away are team keys (e.g. 'bruins', 'maple_leafs')
    or abbreviations (e.g. 'BOS', 'TOR').
    """
    from engine.nhl_predict import predict_matchup as nhl_predict

    key_map = _nhl_espn_to_key()

    # Resolve to JSON keys
    home_key = key_map.get(home, home.lower())
    away_key = key_map.get(away, away.lower())

    result = nhl_predict(home_key, away_key)
    if not result:
        raise HTTPException(status_code=400, detail=f"Could not predict {away} @ {home}")

    # NHL Monte Carlo shadow prediction (gated on ENABLE_NHL_MC).
    from engine.config import get_flag as _get_flag
    if _get_flag("ENABLE_NHL_MC", False, sport="nhl"):
        try:
            from engine.mc_nhl_run import run_nhl_mc
            import engine.config as _cfg
            home_abbr = (result.get("home") or {}).get("abbreviation") or home
            away_abbr = (result.get("away") or {}).get("abbreviation") or away
            result["mc"] = run_nhl_mc(
                home_abbr, away_abbr,
                n_sims=int(getattr(_cfg, "NHL_MC_N_SIMS", 50_000)),
            )
        except Exception as e:
            logger.warning("NHL MC shadow failed for %s/%s: %s", home, away, e)
            result["mc"] = {"error": str(e)}

    # NHL GBM prediction (gated on ENABLE_NHL_GBM; requires trained
    # artifacts in data/models/nhl_gbm_*_latest.json).
    if _get_flag("ENABLE_NHL_GBM", False, sport="nhl"):
        try:
            from engine.gbm.predict import predict_nhl as _gbm_predict_nhl
            from engine.nhl_db import get_conn as _nhl_conn
            # Same team-id resolution as _predict_nhl_full -- nhl_predict
            # doesn't emit result["home"]["id"], so we look up by
            # abbreviation against nhl_teams.
            nhl_conn = _nhl_conn()
            home_abbr = (result.get("home") or {}).get("abbreviation") or ""
            away_abbr = (result.get("away") or {}).get("abbreviation") or ""
            home_tid = away_tid = None
            if home_abbr and away_abbr:
                rows = nhl_conn.execute(
                    "SELECT id, abbreviation FROM nhl_teams "
                    "WHERE abbreviation IN (?, ?)",
                    (home_abbr.upper(), away_abbr.upper()),
                ).fetchall()
                by_abbr = {r["abbreviation"]: r["id"] for r in rows}
                home_tid = by_abbr.get(home_abbr.upper())
                away_tid = by_abbr.get(away_abbr.upper())
            if home_tid and away_tid:
                today_s = datetime.now().strftime("%Y-%m-%d")
                result["gbm"] = _gbm_predict_nhl(nhl_conn, {
                    "home_team_id": int(home_tid),
                    "away_team_id": int(away_tid),
                    "date": today_s,
                    "game_type": _nhl_resolve_game_type(
                        nhl_conn, int(home_tid), int(away_tid), today_s,
                    ),
                })
            else:
                result["gbm"] = {"error": f"team_id lookup failed: "
                                          f"{home_abbr!r}/{away_abbr!r}"}
        except Exception as e:
            logger.warning("NHL GBM shadow failed for %s/%s: %s", home, away, e)
            result["gbm"] = {"error": str(e)}

    try:
        from engine.ensemble import ensemble_nhl
        result["ensemble"] = ensemble_nhl(result)
    except Exception as e:
        logger.debug("NHL ensemble blend failed: %s", e)

    # Run the same pick generator the Scoreboard uses so GameDetail and
    # the card agree on best_pick / edges. Previously GameDetail
    # reimplemented edge selection client-side (findBestEdge) which used
    # different thresholds + no empirical calibration, so the card
    # could say "PL MTL +1.5 / +14.1%" while the detail computed
    # "Under 6.5 / +12.9%" on the same game.
    try:
        from engine.nhl_picks import generate_nhl_picks_with_context
        # Prefer picks store from best-bets
        home_abbr = (result.get("home") or {}).get("abbreviation") or ""
        away_abbr = (result.get("away") or {}).get("abbreviation") or ""
        stored = _picks_store_get("nhl", home_abbr, away_abbr)
        if stored:
            picks = stored["picks"]
            game_odds = stored["odds"]
        elif result.get("_cached_picks"):
            picks = result["_cached_picks"]
            game_odds = result.get("_cached_odds", {})
        else:
            game_odds = _odds_from_scoreboard_cache(home_abbr, away_abbr, sport="nhl")
            if not game_odds and home_abbr and away_abbr:
                from engine.picks import match_odds as _match
                odds_map = _fetch_nhl_odds()
                game_odds = _match(home_abbr, away_abbr, odds_map)
            picks, _ctx = generate_nhl_picks_with_context(
                home_key, away_key, game_odds, pred=result,
            )
        result["picks"] = picks
        result["best_pick"] = picks[0] if picks else None
        result["odds"] = game_odds
    except Exception as e:
        logger.warning("NHL picks generation in predict failed for %s/%s: %s",
                       home, away, e)
        result["picks"] = []
        result["best_pick"] = None
        result["odds"] = {}

    return result


@app.get("/api/nhl/best-bets")
@_bb_reset_on_exit("nhl")
def api_nhl_best_bets():
    """Run predictions on all today's NHL games and find edges.

    Parallel fan-out so cold load isn't N x serial. Each game's
    factor+MC+GBM chain runs in a worker, then picks are assembled
    serially on the main thread (generate_nhl_picks_with_context reads
    the augmented pred and blends via ensemble_nhl).
    """
    import time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from engine.nhl_picks import generate_nhl_picks_with_context

    games = _get_nhl_scoreboard()
    key_map = _nhl_espn_to_key()

    # Fetch starting goalies from DailyFaceoff once up front.
    df_goalies = {}
    try:
        from scrapers.dailyfaceoff import get_starting_goalies, match_goalie_to_player
        df_goalies = get_starting_goalies()
        if df_goalies:
            logger.info("DailyFaceoff: %d starting goalies loaded", len(df_goalies))
    except Exception as e:
        logger.debug("DailyFaceoff unavailable: %s", e)

    predictable = []
    for game in games:
        state = game["status"].get("state", "pre")
        if state in ("post", "in") or game["status"].get("completed"):
            continue
        h_abbr = game["home"]["abbreviation"]
        a_abbr = game["away"]["abbreviation"]
        h_key = key_map.get(h_abbr)
        a_key = key_map.get(a_abbr)
        if not h_key or not a_key:
            h_name = game["home"]["name"].split()[-1].lower()
            a_name = game["away"]["name"].split()[-1].lower()
            h_key = h_key or key_map.get(h_name, h_name)
            a_key = a_key or key_map.get(a_name, a_name)
        predictable.append({
            "game": game, "h_abbr": h_abbr, "a_abbr": a_abbr,
            "h_key": h_key, "a_key": a_key,
            "odds": game.get("odds"),
        })

    _bb_progress_set("nhl", total=len(predictable), done=0, phase="predicting",
                     started_at=_time.time(), finished_at=None)
    logger.info("NHL best bets: analyzing %d games (parallel)", len(predictable))

    def _predict_one(row):
        try:
            uc = not _is_game_imminent((row.get("game") or {}).get("date"))
            return (row, _predict_nhl_full(row["h_key"], row["a_key"], use_cache=uc))
        except Exception as e:
            logger.error("NHL predict crashed for %s @ %s: %s",
                         row["a_abbr"], row["h_abbr"], e, exc_info=True)
            return (row, None)

    prepped = []
    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="nhl-bb") as pool:
        futures = [pool.submit(_predict_one, r) for r in predictable]
        for fut in as_completed(futures):
            row, pred = fut.result()
            _bb_progress_increment("nhl")
            if pred is not None:
                row["pred"] = pred
                prepped.append(row)

    _bb_progress_set("nhl", phase="building")

    bets = []
    for row in prepped:
        game = row["game"]
        h_abbr = row["h_abbr"]
        a_abbr = row["a_abbr"]
        odds = row["odds"]
        pred = row["pred"]

        home_goalie_name = None
        away_goalie_name = None
        for h_try in [h_abbr, _nhl_alt_abbr(h_abbr)]:
            if h_try in df_goalies:
                home_goalie_name = df_goalies[h_try]["name"]
                break
        for a_try in [a_abbr, _nhl_alt_abbr(a_abbr)]:
            if a_try in df_goalies:
                away_goalie_name = df_goalies[a_try]["name"]
                break

        try:
            picks, ctx = generate_nhl_picks_with_context(
                row["h_key"], row["a_key"], odds, pred=pred,
            )
        except Exception as e:
            logger.error("NHL picks failed for %s @ %s: %s", a_abbr, h_abbr, e)
            continue
        if not picks:
            continue

        # generate_nhl_picks_with_context auto-caches via its internal
        # generate_picks call (if applicable). Also cache here for
        # NHL-specific pick format compatibility.
        pred["_cached_picks"] = picks
        pred["_cached_odds"] = odds
        _picks_store_put("nhl", h_abbr, a_abbr, picks, odds)

        core_picks = [p for p in picks if p.get("type") not in _NHL_DERIV_TYPES]
        nhl_matchup = f"{a_abbr} @ {h_abbr}"
        nhl_target = datetime.now().strftime("%Y-%m-%d")
        recorded = _get_recorded_pick("nhl", nhl_matchup, nhl_target)
        locked = _is_game_locked(game.get("date"))
        if locked and recorded:
            best = recorded
        else:
            best = core_picks[0] if core_picks else None
        if not best:
            continue

        goalie_info = {}
        if home_goalie_name:
            h_gs = df_goalies.get(h_abbr, df_goalies.get(_nhl_alt_abbr(h_abbr), {}))
            goalie_info["home"] = {"name": home_goalie_name, "status": h_gs.get("status", "unconfirmed")}
        if away_goalie_name:
            a_gs = df_goalies.get(a_abbr, df_goalies.get(_nhl_alt_abbr(a_abbr), {}))
            goalie_info["away"] = {"name": away_goalie_name, "status": a_gs.get("status", "unconfirmed")}

        # all_picks = top 4 CORE only (derivatives in their own panel).
        all_picks = list(core_picks[:4])
        derivative_picks = sorted(
            (p for p in picks if p.get("type") in _NHL_DERIV_TYPES),
            key=lambda p: -(p.get("edge") or 0),
        )

        bets.append({
            "game_id": game["id"],
            "matchup": f"{a_abbr} @ {h_abbr}",
            "home": game["home"],
            "away": game["away"],
            "time": game["date"],
            "venue": game.get("venue", ""),
            "goalies": goalie_info,
            "best_pick": best,
            "all_picks": all_picks,
            "derivative_picks": derivative_picks,
            "confidence": best.get("confidence", "lean"),
            "rest": ctx.get("rest", {}),
            "injuries": ctx.get("injuries", {}),
            "win_prob": ctx.get("win_prob", {}),
            "expected_score": ctx.get("expected_score", {}),
            "factors": ctx.get("factors", {}),
            "season_context": ctx.get("season_context", {}),
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "recorded_pick": recorded,
            "is_locked": locked,
        })

    bets.sort(key=lambda b: b["best_pick"]["edge"], reverse=True)

    try:
        from engine.nhl_tracker import refresh_pending_for_today as _nhl_refresh
        nhl_target = datetime.now().strftime("%Y-%m-%d")
        diff = _nhl_refresh(bets, nhl_target)
        if any(diff.values()):
            logger.info("Tracker pending sync (nhl): %s", diff)
    except Exception as e:
        logger.warning("NHL tracker pending refresh failed: %s", e)

    try:
        from engine.derivative_tracker import record_top_derivatives
        diff = record_top_derivatives("nhl", bets, n_per_game=1,
                                       min_edge=4.0,
                                       target_date=datetime.now().strftime("%Y-%m-%d"))
        if diff.get("inserted") or diff.get("updated"):
            logger.info("Derivative tracker (nhl): %s", diff)
    except Exception as e:
        logger.warning("NHL derivative recorder failed: %s", e)

    try:
        from engine.pick_events import detect_transitions
        ev = detect_transitions("nhl", [
            {"game_id": b["game_id"], "matchup": b["matchup"],
             "best_pick": b.get("best_pick")}
            for b in bets
        ], date=datetime.now().strftime("%Y-%m-%d"))
        if any(ev.values()):
            logger.info("Pick events (nhl): %s", ev)
    except Exception as e:
        logger.warning("NHL pick-events failed: %s", e)

    _bb_progress_set("nhl", phase="idle", finished_at=_time.time())
    return bets


@app.get("/api/nhl/tracker/history")
def api_nhl_pick_history():
    """Return recent NHL pick history.

    Filters out derivative bet types — see api_pick_history docstring
    for the trampoline-race rationale. Same pattern applies here.
    """
    from engine.nhl_tracker import _get_nhl_db
    conn = _get_nhl_db()
    placeholders = ",".join("?" * len(_NHL_DERIV_TYPES))
    picks = conn.execute(
        f"SELECT * FROM nhl_picks WHERE bet_type NOT IN ({placeholders}) "
        f"ORDER BY created_at DESC LIMIT 50",
        tuple(_NHL_DERIV_TYPES),
    ).fetchall()
    return [dict(p) for p in picks]


@app.get("/api/nhl/tracker/summary")
def api_nhl_pick_summary():
    """Get NHL running pick totals."""
    from engine.nhl_tracker import get_pick_summary
    return get_pick_summary()


@app.post("/api/nhl/tracker/record")
def api_nhl_record_picks():
    """Record today's NHL picks."""
    try:
        from engine.nhl_tracker import record_picks
        picks = record_picks()
        return {"recorded": len(picks), "picks": picks}
    except Exception as e:
        logger.error("NHL record picks failed: %s", e, exc_info=True)
        return {"error": str(e), "recorded": 0}


@app.post("/api/nhl/tracker/settle")
def api_nhl_settle_picks():
    """Settle completed NHL picks + POTD."""
    try:
        from engine.nhl_tracker import settle_picks
        result = settle_picks()
        try:
            from engine.pick_of_day import settle_potd
            settle_potd("nhl")
        except Exception as e:
            logger.warning("NHL POTD settle failed: %s", e)
        return result
    except Exception as e:
        logger.error("NHL settle picks failed: %s", e, exc_info=True)
        return {"error": str(e), "settled": 0}


@app.get("/api/nhl/backtest")
def api_nhl_backtest(days: int = Query(default=0), min_edge: float = Query(default=3.0),
                     season: int | None = Query(default=None),
                     pit: bool = Query(default=True)):
    """Run NHL backtest on historical games.

    Args:
        pit: If True (default), use point-in-time stats to avoid lookahead
            bias.  If False, use current-season stats (for comparison).
    """
    try:
        # Auto-load historical NHL season if not present
        if season:
            from engine.nhl_db import get_conn as nhl_conn
            conn = nhl_conn()
            # Try both season formats (2025 and 20252026)
            yr = season
            season_ids = [yr]
            if yr < 10000:
                season_ids.append(yr * 10000 + yr + 1)
                season_ids.append((yr - 1) * 10000 + yr)
            placeholders = ",".join("?" for _ in season_ids)
            game_count = conn.execute(
                f"SELECT COUNT(*) FROM nhl_games WHERE status = 'final' AND season IN ({placeholders})",
                season_ids
            ).fetchone()[0]
            if game_count < 50:
                logger.info("Loading NHL %s season data for backtest...", season)
                from scrapers.nhl_api import sync_history
                season_str = f"{yr}{yr+1}" if yr < 10000 else str(yr)
                sync_history(season_str)

        from engine.nhl_backtest import run_nhl_backtest
        return run_nhl_backtest(days=days, min_edge=min_edge, season=season,
                                pit_mode=pit)
    except Exception as e:
        logger.error("NHL backtest failed: %s", e, exc_info=True)
        return {"error": str(e)}


@app.get("/api/accuracy")
def api_accuracy(sport: str = Query(default="mlb")):
    """Get prediction accuracy / calibration data."""
    try:
        from engine.accuracy import compute_calibration
        return compute_calibration(sport=sport)
    except Exception as e:
        logger.error("Accuracy computation failed: %s", e, exc_info=True)
        return {"error": str(e)}


@app.get("/api/line-movement/{sport}/{matchup_key}")
def api_line_movement(sport: str, matchup_key: str):
    """Get line movement for a specific game."""
    try:
        from engine.line_movement import get_line_movement
        from scrapers.odds_api import fetch_odds
        current_odds = {}
        if sport == "mlb":
            all_odds = fetch_odds()
            current_odds = all_odds.get(matchup_key, {})
        return get_line_movement(sport, matchup_key, current_odds) or {"movement": "none"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/nhl/backtest/thresholds")
def api_nhl_backtest_thresholds(days: int = Query(default=0),
                                season: int | None = Query(default=None),
                                pit: bool = Query(default=True)):
    """Run NHL backtest at multiple edge thresholds (1-15%) and compare.

    Returns a list of dicts with bets/win_pct/roi/profit per threshold for
    each bet category (moneyline, over_under, puck_line, best_bet).
    """
    try:
        from engine.nhl_backtest import analyze_edge_thresholds
        return analyze_edge_thresholds(days=days, season=season, pit_mode=pit)
    except Exception as e:
        logger.error("NHL threshold analysis failed: %s", e, exc_info=True)
        return {"error": str(e)}


@app.get("/api/nhl/odds/history")
def api_nhl_odds_history(date: str = Query(default="")):
    """Get stored historical odds."""
    from engine.odds_history import get_historical_odds
    return get_historical_odds(date=date or None)


@app.get("/api/debug/nhl-live-stats")
def api_debug_nhl_live_stats():
    """Debug: show what live team stats are actually being loaded."""
    result = {}
    try:
        from engine.nhl_predict import (
            _fetch_team_summary_stats,
            _ensure_club_stats_loaded,
            _live_stats_cache,
        )

        # Try the raw fetch first
        raw = _fetch_team_summary_stats()
        result["fetch_result_count"] = len(raw)
        # Sample a few teams
        sample_keys = list(raw.keys())[:5]
        result["fetch_sample"] = {k: raw[k] for k in sample_keys}

        # Check if FLA specifically is in the result
        result["fla_from_fetch"] = raw.get("FLA", "NOT FOUND")
        result["bos_from_fetch"] = raw.get("BOS", "NOT FOUND")
        result["cbj_from_fetch"] = raw.get("CBJ", "NOT FOUND")
        result["buf_from_fetch"] = raw.get("BUF", "NOT FOUND")

        # Now force-load and check the merged cache
        _ensure_club_stats_loaded()
        from engine.nhl_predict import _live_stats_cache as cache
        if cache:
            result["cache_fla"] = cache.get("FLA", "NOT FOUND")
            result["cache_bos"] = cache.get("BOS", "NOT FOUND")
        else:
            result["cache"] = "None"
    except Exception as e:
        import traceback
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
    return result


@app.get("/api/debug/nhl-raw-stats")
def api_debug_nhl_raw_stats():
    """Debug: fetch raw NHL stats.rest response to see field names."""
    import json
    import urllib.error
    import urllib.parse
    import urllib.request
    try:
        query = urllib.parse.urlencode({
            "cayenneExp": "seasonId=20252026 and gameTypeId=2"
        })
        url = f"https://api.nhle.com/stats/rest/en/team/summary?{query}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        # Return the first team's full row so we can see field names
        rows = data.get("data", [])
        return {
            "total_teams": len(rows),
            "first_team": rows[0] if rows else None,
            "keys_available": list(rows[0].keys()) if rows else [],
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/api/pick-of-day/{sport}")
def api_pick_of_day(sport: str, view: str = Query(default="q1")):
    """Get today's Pick of the Day for a sport.

    NBA-only ``view`` query param: 'q1' (default) selects from Q1
    markets only, 'full' selects from full-game ML/SPREAD/TOTAL.
    'both' returns ``{q1: ..., full: ...}`` so the dashboard can
    flip between views without a second fetch. MLB/NHL ignore view.
    """
    from engine.pick_of_day import (
        get_or_create_potd, get_today_potd,
        refresh_potd_for_line_movement,
    )

    # Resolve today's bets up front so the line-movement refresh can
    # also invalidate locks where the GameCard's headline has drifted
    # away from the locked bet_type. Without this, the refresh only
    # covers price drift on the same bet_type — the
    # "card says ML, POTD says +1.5 RL" misalignment slips through.
    if sport == "nhl":
        bets = api_nhl_best_bets()
    elif sport == "mlb":
        bets = api_best_bets()
    elif sport == "nba":
        bets = api_nba_best_bets()
    else:
        return {"error": f"Unknown sport: {sport}"}
    bets_list = bets if isinstance(bets, list) else None

    if sport == "nba" and view == "both":
        out = {}
        for v in ("q1", "full"):
            try:
                refresh_potd_for_line_movement(sport, bets_list, view=v)
            except Exception as e:
                logger.warning("POTD line refresh failed for %s/%s: %s", sport, v, e)
            potd = get_today_potd(sport, view=v)
            if not potd and bets_list is not None:
                potd = get_or_create_potd(sport, bets_list, view=v)
            out[v] = potd or None
        return out

    try:
        refresh_potd_for_line_movement(sport, bets_list, view=view)
    except Exception as e:
        logger.warning("POTD line refresh failed for %s: %s", sport, e)

    # Single-view path
    potd = get_today_potd(sport, view=view)
    if potd:
        return potd

    if bets_list is not None:
        potd = get_or_create_potd(sport, bets_list, view=view)
        return potd or {"message": "No qualifying picks today", "sport": sport}
    return {"error": "Could not generate bets"}


@app.get("/api/pick-of-day/{sport}/summary")
def api_potd_summary(sport: str):
    """Get POTD running totals.

    Auto-settles pending POTDs before returning the summary so the
    hero shown on the dashboard reflects finals without requiring the
    user to click "Settle Completed" on the Pick Tracker tab. settle_potd
    is a no-op when nothing is pending, so the cost is a single SQLite
    round-trip on typical calls.
    """
    from engine.pick_of_day import get_potd_summary, settle_potd
    try:
        settle_potd(sport)
    except Exception as e:
        logger.warning("POTD auto-settle failed for %s: %s", sport, e)
    return get_potd_summary(sport)


@app.post("/api/pick-of-day/{sport}/settle")
def api_potd_settle(sport: str):
    """Settle completed POTDs."""
    from engine.pick_of_day import settle_potd
    return settle_potd(sport)


@app.post("/api/pick-of-day/{sport}/recalc-profit")
def api_potd_recalc_profit(sport: str):
    """Rewrite profit on every settled POTD row using the current
    $100-unit formula. Run this once after a unit-sizing change to
    realign rows that were persisted under an older formula (e.g.
    Kelly dollarization against a $5000 bankroll)."""
    from engine.pick_of_day import recalc_potd_profit
    return recalc_potd_profit(sport)


@app.post("/api/calibration/refresh")
def api_calibration_refresh(sport: str | None = None):
    """Rebuild the empirical-calibration table(s) from the current
    tracker data without restarting the server. Useful after a code
    change to the calibration math so the in-memory table gets rebuilt
    in the new shape (e.g. after adding sample-count shrinkage, the
    old 3-tuple rows need refresh to become 4-tuples with real counts).

    No sport -> refresh all three (mlb, nhl, nba).
    """
    from engine.empirical_calibration import refresh_calibration, refresh_all_sports
    if sport:
        return refresh_calibration(sport)
    return refresh_all_sports()


@app.delete("/api/pick-of-day/{sport}")
def api_potd_reset(sport: str):
    """Delete today's POTD so it regenerates on next request."""
    from engine.pick_of_day import _ensure_potd_table, _get_conn
    from datetime import datetime
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    today = datetime.now().strftime("%Y-%m-%d")
    conn.execute("DELETE FROM pick_of_day WHERE date = ?", (today,))
    conn.commit()
    return {"status": "cleared", "date": today, "sport": sport}


# ══════════════════════════════════════════════════════════════
#  NBA Q1 ENDPOINTS
# ══════════════════════════════════════════════════════════════

_nba_scoreboard_cache: dict[str, tuple[float, list]] = {}


def _get_nba_scoreboard(date: str = "") -> list[dict]:
    """Fetch NBA scoreboard from ESPN, enriched with Q1 scores and odds.

    When called with no date (default-today path), also pulls
    yesterday's scoreboard and keeps any live games. Late-night games
    that cross midnight UTC otherwise drop off the dashboard while
    they're still in progress — surfaces as "DEN @ MIN disappeared
    in the 4th quarter at midnight" on the user side.
    """
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    espn_date = target_date.replace("-", "")

    cache_key = f"nba:{espn_date}"
    now = time.time()
    if cache_key in _nba_scoreboard_cache:
        ts, cached = _nba_scoreboard_cache[cache_key]
        if now - ts < CACHE_TTL:
            return cached

    url = f"{ESPN_BASE}/basketball/nba/scoreboard?dates={espn_date}"
    logger.info("Fetching NBA scoreboard: %s", url)
    espn_data = _fetch_espn_json(url)

    games = []
    if espn_data:
        events = espn_data.get("events", [])
        logger.info("ESPN NBA returned %d events", len(events))
        games = _parse_nba_scoreboard(espn_data)

    # Yesterday-still-live carryover. Only on the default-today path —
    # explicit ?date=YYYY-MM-DD lookups stay strict.
    if date == "":
        from datetime import timedelta as _td
        yest = (datetime.now() - _td(days=1)).strftime("%Y%m%d")
        try:
            yest_data = _fetch_espn_json(
                f"{ESPN_BASE}/basketball/nba/scoreboard?dates={yest}")
            if yest_data:
                yest_games = _parse_nba_scoreboard(yest_data)
                seen_ids = {g.get("id") for g in games}
                for g in yest_games:
                    state = (g.get("status") or {}).get("state", "")
                    if state == "in" and g.get("id") not in seen_ids:
                        games.append(g)
                        logger.info("NBA: carried over live game from "
                                    "yesterday: %s", g.get("id"))
        except Exception as e:
            logger.debug("NBA yesterday-live carryover failed: %s", e)

    # If no games today, try tomorrow
    if not games and date == "":
        from datetime import timedelta as _td
        tomorrow = (datetime.now() + _td(days=1)).strftime("%Y%m%d")
        logger.info("No NBA games today, checking tomorrow (%s)", tomorrow)
        espn_data = _fetch_espn_json(f"{ESPN_BASE}/basketball/nba/scoreboard?dates={tomorrow}")
        if espn_data:
            games = _parse_nba_scoreboard(espn_data)
            if games:
                logger.info("Found %d NBA games for tomorrow", len(games))

    # ── Hard Rock fallback (only when ESPN returned zero events) ──
    if not games and date == "":
        try:
            from scrapers.hardrock_odds import fetch_nba as _hr_nba
            hr_odds = _hr_nba()
            if hr_odds:
                logger.info("NBA: ESPN empty, building %d games from Hard Rock",
                            len(hr_odds))
                for matchup_key, odds_data in hr_odds.items():
                    parts = matchup_key.split("@")
                    if len(parts) != 2:
                        continue
                    a_abbr, h_abbr = parts
                    existing = any(
                        g["home"]["abbreviation"] == h_abbr and
                        g["away"]["abbreviation"] == a_abbr
                        for g in games
                    )
                    if existing:
                        continue
                    games.append({
                        "id": f"hr_{a_abbr}_{h_abbr}",
                        "date": target_date,
                        "home": {
                            "abbreviation": h_abbr,
                            "name": h_abbr,
                            "score": None,
                        },
                        "away": {
                            "abbreviation": a_abbr,
                            "name": a_abbr,
                            "score": None,
                        },
                        "status": {"state": "pre", "detail": "Scheduled"},
                        "odds": odds_data,
                        "source": "hardrock",
                    })
        except Exception as e:
            logger.debug("NBA Hard Rock fallback failed: %s", e)

    # Fetch NBA odds from The Odds API
    try:
        nba_odds = _fetch_nba_odds()
        if nba_odds:
            matched = 0
            for game in games:
                h = game["home"]["abbreviation"]
                a = game["away"]["abbreviation"]
                key = f"{a}@{h}"
                from engine.picks import match_odds as _match
                matched_odds = _match(h, a, nba_odds)
                if matched_odds:
                    game["odds"] = matched_odds
                    matched += 1
            logger.info("NBA odds: matched %d/%d games", matched, len(games))
    except Exception as e:
        logger.warning("NBA odds failed: %s", e)

    # Cache
    if len(_nba_scoreboard_cache) >= MAX_CACHE_ENTRIES:
        oldest = min(_nba_scoreboard_cache, key=lambda k: _nba_scoreboard_cache[k][0])
        del _nba_scoreboard_cache[oldest]
    # ── Enrich with playoff series context ──
    try:
        from engine.series_context import infer_series
        from engine.nba_q1_predict import _is_nba_playoffs
        if _is_nba_playoffs():
            for game in games:
                h = game["home"]["abbreviation"]
                a = game["away"]["abbreviation"]
                series = infer_series("nba", h, a)
                if series.get("in_series"):
                    game["series"] = series
    except Exception as e:
        logger.debug("NBA series enrichment failed: %s", e)

    _nba_scoreboard_cache[cache_key] = (now, games)
    return games


def _parse_nba_scoreboard(espn_data: dict) -> list[dict]:
    """Parse ESPN NBA scoreboard response into our standard format with Q1 data."""
    games = []
    for ev in espn_data.get("events", []):
      try:
        comp = ev.get("competitions", [{}])[0]
        teams = comp.get("competitors", [])
        if len(teams) < 2:
            continue

        home_raw = next((t for t in teams if t.get("homeAway") == "home"), teams[0])
        away_raw = next((t for t in teams if t.get("homeAway") == "away"), teams[1])

        def parse_team(raw):
            t = raw.get("team", {})
            if isinstance(t, str):
                t = {"name": t}
            record = ""
            for r in raw.get("records", []):
                if not isinstance(r, dict):
                    continue
                if r.get("type") == "total":
                    record = r.get("summary", "")
                    break
            logo = t.get("logo", "")
            if isinstance(logo, dict):
                logo = logo.get("href", "")
            elif isinstance(logo, list) and logo:
                logo = logo[0].get("href", "") if isinstance(logo[0], dict) else str(logo[0])
            return {
                "name": t.get("displayName", t.get("name", "")),
                "abbreviation": t.get("abbreviation", ""),
                "logo": logo if isinstance(logo, str) else "",
                "record": record,
                "score": str(raw.get("score", "")),
                "winner": raw.get("winner", False),
            }

        home = parse_team(home_raw)
        away = parse_team(away_raw)

        # Parse Q1 scores from linescores
        home_q1 = None
        away_q1 = None
        home_ls = home_raw.get("linescores", [])
        away_ls = away_raw.get("linescores", [])
        if home_ls and len(home_ls) >= 1:
            v = home_ls[0]
            home_q1 = int(v.get("value", 0)) if isinstance(v, dict) else int(v) if str(v).isdigit() else None
        if away_ls and len(away_ls) >= 1:
            v = away_ls[0]
            away_q1 = int(v.get("value", 0)) if isinstance(v, dict) else int(v) if str(v).isdigit() else None

        # Quarter scores for display
        quarters = []
        for i in range(max(len(home_ls), len(away_ls))):
            hval = home_ls[i] if i < len(home_ls) else 0
            aval = away_ls[i] if i < len(away_ls) else 0
            hv = int(hval.get("value", 0)) if isinstance(hval, dict) else int(hval) if str(hval).isdigit() else 0
            av = int(aval.get("value", 0)) if isinstance(aval, dict) else int(aval) if str(aval).isdigit() else 0
            quarters.append({"quarter": i + 1, "home": hv, "away": av})

        status_raw = comp.get("status", {})
        status_type = status_raw.get("type", {})
        state = status_type.get("state", "pre")
        period = status_raw.get("period", 0)

        game = {
            "id": ev.get("id", ""),
            "date": ev.get("date", ""),
            "venue": comp.get("venue", {}).get("fullName", ""),
            "broadcast": "",
            "home": home,
            "away": away,
            "q1": {
                "home": home_q1,
                "away": away_q1,
            },
            "quarters": quarters,
            "status": {
                "state": state,
                "detail": status_type.get("detail", ""),
                "completed": status_type.get("completed", False),
                "period": period,
            },
        }

        # Extract broadcast - names can be strings or dicts
        for bc in comp.get("broadcasts", []):
            if not isinstance(bc, dict):
                continue
            raw_names = bc.get("names", [])
            names = [n.get("shortName", str(n)) if isinstance(n, dict) else str(n) for n in raw_names]
            if names:
                game["broadcast"] = ", ".join(names)
                break

        games.append(game)
      except Exception as e:
        import traceback
        logger.warning("NBA parse event failed: %s\n%s", e, traceback.format_exc())
        continue

    return games


# NBA abbreviation aliases (ESPN vs odds providers)
_NBA_ABBR_MAP = {
    "GS": "GSW", "GSW": "GS",
    "NY": "NYK", "NYK": "NY",
    "SA": "SAS", "SAS": "SA",
    "NO": "NOP", "NOP": "NO",
    "PHX": "PHO", "PHO": "PHX",
    "WSH": "WAS", "WAS": "WSH",
    "BKN": "BRK", "BRK": "BKN",
    "CHA": "CHO", "CHO": "CHA",
}


def _nba_alt_abbr(abbr: str) -> str:
    return _NBA_ABBR_MAP.get(abbr, abbr)


# NBA team name -> abbreviation for odds matching
_NBA_TEAM_ABBRS = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GS", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "LA Clippers": "LAC", "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL", "LA Lakers": "LAL",
    "Memphis Grizzlies": "MEM", "Miami Heat": "MIA", "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN", "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL", "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX", "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC", "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR", "Utah Jazz": "UTA", "Washington Wizards": "WAS",
}


def _fetch_nba_odds() -> dict:
    """Fetch NBA odds (full-game + Q1 markets) from The Odds API via the
    unified fallback chain in scrapers.nba_odds.

    Delegates to fetch_all_nba_odds so the UI scoreboard endpoint picks
    up the same data the tracker uses - including h2h_q1, spreads_q1,
    and totals_q1 markets via the per-event endpoint. Previously this
    was a duplicated bulk-only fetcher that silently dropped Q1 markets,
    which is why the Q1 model picks card kept showing (-110) defaults
    no matter how many times the scrapers were fixed.
    """
    try:
        from scrapers.nba_odds import fetch_all_nba_odds
        return fetch_all_nba_odds() or {}
    except Exception as e:
        logger.warning("NBA odds unified fetch failed: %s", e)
        return {}


@app.post("/api/nba/sync")
def api_nba_sync():
    """Refresh NBA data."""
    try:
        from scrapers.nba_espn import sync_nba
        result = sync_nba()
        return {"status": "ok", "result": result}
    except ImportError:
        return {"error": "NBA sync module (scrapers.nba_espn) not available yet"}
    except Exception as e:
        logger.error("NBA sync failed: %s", e)
        return {"status": "error", "message": str(e)}


@app.get("/api/nba/scoreboard")
def api_nba_scoreboard(date: str = Query(default="")):
    """Return today's NBA games with Q1 scores."""
    try:
        return _get_nba_scoreboard(date)
    except Exception as e:
        logger.error("NBA scoreboard failed: %s", e)
        return []


@app.get("/api/nba/standings")
def api_nba_standings():
    """Return NBA standings by conference/division from ESPN.

    ESPN flattened the legacy site-v2 endpoint ({fullViewLink} only,
    no children) so we hit the apis/v2 path with level=3 to get the
    Conference -> Division -> Teams tree the parser expects. Verified
    2026-04-25 returning 6 divisions x 5 teams across both conferences.
    """
    try:
        url = "https://site.api.espn.com/apis/v2/sports/basketball/nba/standings?level=3"
        data = _fetch_espn_json(url)
        if not data:
            return []
        return _parse_nba_standings(data)
    except Exception as e:
        logger.error("NBA standings failed: %s", e)
        return []


def _parse_nba_standings(data: dict) -> list[dict]:
    """Parse ESPN NBA standings into conference/division structure."""
    divisions = {}

    for child in data.get("children", []):
        conf_name = child.get("name", "")  # "Eastern Conference" etc.
        conf_short = "Eastern" if "east" in conf_name.lower() else "Western"

        for div_data in child.get("children", []):
            div_name = div_data.get("name", "")
            standings = div_data.get("standings", {})
            entries = standings.get("entries", [])

            div_teams = []
            for entry in entries:
                team_info = entry.get("team", {})
                abbr = team_info.get("abbreviation", "")
                name = team_info.get("displayName", team_info.get("name", ""))
                logo = team_info.get("logos", [{}])[0].get("href", "") if team_info.get("logos") else ""

                stats = {}
                for s in entry.get("stats", []):
                    stats[s.get("name", "")] = s.get("value", 0)

                wins = int(stats.get("wins", 0))
                losses = int(stats.get("losses", 0))
                pct = stats.get("winPercent", stats.get("winPct", 0))
                gb = stats.get("gamesBehind", stats.get("GB", "-"))
                streak = stats.get("streak", "")
                # Try common field names for last 10
                l10 = stats.get("record-last10", stats.get("Last10Record", ""))
                home_rec = stats.get("Home", stats.get("home", ""))
                away_rec = stats.get("Road", stats.get("away", stats.get("road", "")))
                ppg = stats.get("avgPointsFor", stats.get("pointsFor", 0))
                papg = stats.get("avgPointsAgainst", stats.get("pointsAgainst", 0))
                diff = stats.get("differential", stats.get("pointDifferential", 0))

                div_teams.append({
                    "name": name,
                    "abbreviation": abbr,
                    "logo": logo,
                    "conference": conf_short,
                    "record": f"{wins}-{losses}",
                    "wins": wins,
                    "losses": losses,
                    "pct": round(pct, 3) if isinstance(pct, float) else pct,
                    "gb": gb,
                    "home": str(home_rec),
                    "away": str(away_rec),
                    "l10": str(l10),
                    "streak": str(streak),
                    "ppg": round(ppg, 1) if isinstance(ppg, float) else ppg,
                    "papg": round(papg, 1) if isinstance(papg, float) else papg,
                    "diff": round(diff, 1) if isinstance(diff, float) else diff,
                })

            # Sort by wins descending
            div_teams.sort(key=lambda t: t["wins"], reverse=True)
            divisions[div_name] = {
                "name": div_name,
                "conference": conf_short,
                "teams": div_teams,
            }

    return list(divisions.values())


@app.get("/api/nba/predict")
def api_nba_predict(home: str = Query(...), away: str = Query(...)):
    """Run NBA Q1 prediction for a specific matchup."""
    try:
        from engine.nba_q1_predict import predict_q1_matchup
    except ImportError:
        return {"error": "NBA Q1 prediction engine not loaded yet"}

    # Pull Q1 markets so spread/total/ML picks can be evaluated against
    # real posted lines instead of being silently skipped. Uses the full
    # Odds-API → DK → ESPN fallback chain so picks work even when the
    # user's plan tier doesn't include Q1 markets.
    # Try the scoreboard cache first -- _get_nba_scoreboard already
    # attached Q1 odds to each game. Only hit fetch_all_nba_odds on
    # cache miss so we don't burn extra Odds API credits per game view.
    odds = _odds_from_scoreboard_cache(home, away, sport="nba")
    if not odds:
        try:
            from scrapers.nba_odds import fetch_all_nba_odds
            from engine.picks import match_odds as _match
            odds_map = fetch_all_nba_odds() or {}
            odds = _match(home, away, odds_map) or {}
        except Exception as e:
            logger.debug("NBA Q1 odds fetch failed in predict endpoint: %s", e)

    try:
        result = predict_q1_matchup(home, away, odds=odds)
        if not result:
            raise HTTPException(status_code=400, detail=f"Could not predict {away} @ {home}")
        # Attach season_context so the GameDetail banner lights up during
        # end-of-regular-season + playoff windows.
        if result.get("season_context") is None:
            sc = _nba_season_context()
            if sc:
                result["season_context"] = sc

        # NBA Q1 Monte Carlo shadow (gated on ENABLE_NBA_MC).
        from engine.config import get_flag as _get_flag
        if _get_flag("ENABLE_NBA_MC", False, sport="nba"):
            try:
                from engine.mc_nba_run import run_nba_q1_mc
                import engine.config as _cfg
                result["mc"] = run_nba_q1_mc(
                    home, away,
                    n_sims=int(getattr(_cfg, "NBA_MC_N_SIMS", 50_000)),
                )
            except Exception as e:
                logger.warning("NBA MC shadow failed for %s/%s: %s", home, away, e)
                result["mc"] = {"error": str(e)}

        # NBA GBM shadow (gated on ENABLE_NBA_GBM). See _predict_nba_full
        # for rationale on the abbreviation-based team-id lookup.
        if _get_flag("ENABLE_NBA_GBM", False, sport="nba"):
            try:
                from engine.gbm.predict import predict_nba as _gbm_predict_nba
                from engine.nba_db import get_conn as _nba_conn
                nba_conn = _nba_conn()
                h_abbr_u = (home or "").upper()
                a_abbr_u = (away or "").upper()
                home_tid = away_tid = None
                if h_abbr_u and a_abbr_u:
                    rows = nba_conn.execute(
                        "SELECT id, abbreviation FROM nba_teams "
                        "WHERE abbreviation IN (?, ?)",
                        (h_abbr_u, a_abbr_u),
                    ).fetchall()
                    by_abbr = {r["abbreviation"]: r["id"] for r in rows}
                    home_tid = by_abbr.get(h_abbr_u)
                    away_tid = by_abbr.get(a_abbr_u)
                if home_tid and away_tid:
                    result["gbm"] = _gbm_predict_nba(nba_conn, {
                        "home_team_id": int(home_tid),
                        "away_team_id": int(away_tid),
                        "date": datetime.now().strftime("%Y-%m-%d"),
                    })
                else:
                    result["gbm"] = {"error": f"team_id lookup failed: "
                                              f"{h_abbr_u!r}/{a_abbr_u!r}"}
            except Exception as e:
                logger.warning("NBA GBM shadow failed for %s/%s: %s", home, away, e)
                result["gbm"] = {"error": str(e)}

        # Phase 2k: full-game prediction + picks alongside Q1.
        try:
            from engine.nba_predict import predict_full as _predict_full
            from engine.mc_nba_run import run_nba_full_mc as _run_full_mc
            from engine.nba_picks import generate_full_picks as _gen_full_picks
            full_factor = _predict_full(
                home, away,
                spread=odds.get("home_spread_point"),
                total=odds.get("over_under"),
            )
            try:
                full_mc = _run_full_mc(home, away, n_sims=20_000)
            except Exception:
                full_mc = None
            full_picks = _gen_full_picks(
                home, away, odds=odds,
                pred={**result, "full": full_factor, "mc_full": full_mc or {}},
            )
            from engine.config import EDGE_STRONG, EDGE_MODERATE, EDGE_LEAN, EDGE_SKIP
            for p in full_picks:
                e = p.get("edge", 0)
                if e >= EDGE_STRONG:    p["confidence"] = "strong"
                elif e >= EDGE_MODERATE: p["confidence"] = "moderate"
                elif e >= EDGE_LEAN:     p["confidence"] = "lean"
                else:                    p["confidence"] = "skip"
                if e < EDGE_SKIP:        p["confidence"] = "skip"
            result["full"] = full_factor
            result["full_picks"] = full_picks
        except Exception as e:
            logger.warning("NBA full-game prediction in /predict failed for %s/%s: %s",
                           home, away, e)

        try:
            from engine.ensemble import ensemble_nba
            result["ensemble"] = ensemble_nba(result)
        except Exception as e:
            logger.debug("NBA ensemble blend failed: %s", e)

        # Same picks-attach pattern as NHL -- keep GameDetail consistent
        # with the scoreboard card's best_pick / edges instead of
        # reimplementing edge selection client-side. generate_q1_picks
        # accepts pred= so we reuse the factor + MC + GBM work we
        # already did above instead of re-running predict_q1.
        try:
            # Prefer picks store from best-bets
            stored = _picks_store_get("nba", home, away)
            if stored:
                picks = stored["picks"]
                odds = stored["odds"]
            elif result.get("_cached_picks"):
                picks = result["_cached_picks"]
                odds = result.get("_cached_odds", odds)
            else:
                from engine.nba_picks import generate_q1_picks
                picks = generate_q1_picks(home, away, odds=odds, pred=result)
            # Merge full-game picks into the unified picks list so the
            # detail page sees both Q1 + full picks under result["picks"].
            full_picks_local = result.get("full_picks") or []
            if full_picks_local:
                # De-dupe by (type, pick) so any overlap with cached picks
                # doesn't double-list.
                seen = {(p.get("type"), p.get("pick")) for p in picks}
                for fp in full_picks_local:
                    if (fp.get("type"), fp.get("pick")) not in seen:
                        picks.append(fp)
                picks = sorted(picks, key=lambda p: -(p.get("edge") or 0))
            result["picks"] = picks
            result["best_pick"] = picks[0] if picks else None
            result["odds"] = odds
        except Exception as e:
            logger.warning("NBA Q1 picks generation in predict failed for %s/%s: %s",
                           home, away, e)
            result["picks"] = []
            result["best_pick"] = None
            result["odds"] = odds

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("NBA predict failed: %s", e)
        return {"error": str(e)}


@app.get("/api/nba/best-bets")
@_bb_reset_on_exit("nba")
def api_nba_best_bets():
    """Generate Q1 spread picks for all today's NBA games.

    Parallel fan-out so dashboard cold load isn't N x serial. Each game
    runs factor + MC + GBM in a worker, then predict_q1_matchup reuses
    the augmented pred for pick generation (blending via ensemble_nba).
    """
    import time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    try:
        from engine.nba_q1_predict import predict_q1_matchup
    except ImportError:
        return []

    nba_odds_map = {}
    try:
        from scrapers.nba_odds import fetch_all_nba_odds
        nba_odds_map = fetch_all_nba_odds()
    except Exception as e:
        logger.debug("NBA odds fetch failed: %s", e)

    games = _get_nba_scoreboard()

    from engine.picks import match_odds as _match_nba
    predictable = []
    for game in games:
        state = game["status"].get("state", "pre")
        if state in ("post", "in") or game["status"].get("completed"):
            continue
        h_abbr = game["home"]["abbreviation"]
        a_abbr = game["away"]["abbreviation"]
        odds = dict(game.get("odds") or {})
        # Route the supplementary map through match_odds so the Hard Rock
        # q1_alt_spreads (and anything else with home/away-keyed fields)
        # are aligned to our schedule's home/away before merging. The
        # raw key lookup we used before both missed reversed-key matches
        # and merged unswapped sides on top of already-swapped scoreboard
        # odds — surfaced as Spurs +1.5 @ +130 when the book actually
        # had Spurs -1.5 @ -170.
        market_odds = _match_nba(h_abbr, a_abbr, nba_odds_map) or {}
        for k, v in market_odds.items():
            if k not in odds or odds.get(k) is None:
                odds[k] = v
        predictable.append({
            "game": game, "h_abbr": h_abbr, "a_abbr": a_abbr, "odds": odds,
        })

    _bb_progress_set("nba", total=len(predictable), done=0, phase="predicting",
                     started_at=_time.time(), finished_at=None)
    logger.info("NBA best bets: analyzing %d games (parallel)", len(predictable))

    def _predict_one(row):
        try:
            uc = not _is_game_imminent((row.get("game") or {}).get("date"))
            return (row, _predict_nba_full(row["h_abbr"], row["a_abbr"],
                                              row["odds"], use_cache=uc))
        except Exception as e:
            logger.error("NBA predict crashed for %s @ %s: %s",
                         row["a_abbr"], row["h_abbr"], e, exc_info=True)
            return (row, None)

    prepped = []
    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="nba-bb") as pool:
        futures = [pool.submit(_predict_one, r) for r in predictable]
        for fut in as_completed(futures):
            row, pred_full = fut.result()
            _bb_progress_increment("nba")
            if pred_full is not None:
                row["pred_full"] = pred_full
                prepped.append(row)

    _bb_progress_set("nba", phase="building")

    bets = []
    for row in prepped:
        game = row["game"]
        h_abbr = row["h_abbr"]
        a_abbr = row["a_abbr"]
        odds = row["odds"]
        pred_full = row["pred_full"]

        try:
            # predict_q1_matchup wraps factor pred with picks + confidence
            # tags + CI band. Passing pred_full (which carries mc/gbm)
            # makes generate_q1_picks blend all three signals through
            # ensemble_nba instead of re-running predict_q1 factor-only.
            pred = predict_q1_matchup(h_abbr, a_abbr, odds=odds, pred=pred_full)
        except Exception as e:
            logger.error("NBA Q1 prediction failed for %s @ %s: %s", a_abbr, h_abbr, e)
            continue

        if not pred:
            continue

        picks = pred.get("picks", [])
        if not picks:
            continue

        # ── Phase 2k: full-game predictions + picks ──
        # Run the full-game factor model + MC alongside Q1 so the
        # ensemble blender has all three signals. Append full-game
        # picks to the same list; the picks_store + GameCard layers
        # consume them transparently.
        full_picks: list = []
        try:
            from engine.nba_predict import predict_full
            from engine.mc_nba_run import run_nba_full_mc
            from engine.nba_picks import generate_full_picks
            full_factor = predict_full(
                h_abbr, a_abbr,
                spread=odds.get("home_spread_point"),
                total=odds.get("over_under"),
            )
            try:
                full_mc = run_nba_full_mc(h_abbr, a_abbr, n_sims=20_000)
            except Exception as mc_err:
                logger.debug("NBA full-game MC failed for %s/%s: %s",
                             h_abbr, a_abbr, mc_err)
                full_mc = None
            # Compose pred with full-game side-block so ensemble_nba
            # blends factor + MC + GBM for ML/total/margin.
            ensemble_input = {
                **pred,
                "full": full_factor,
                "mc_full": full_mc or {},
            }
            full_picks = generate_full_picks(
                h_abbr, a_abbr, odds=odds, pred=ensemble_input)
            # Tag confidence on full-game picks the same way Q1 does so
            # the GameCard "skip" gating works consistently.
            from engine.config import EDGE_STRONG, EDGE_MODERATE, EDGE_LEAN, EDGE_SKIP
            for p in full_picks:
                e = p.get("edge", 0)
                if e >= EDGE_STRONG:    p["confidence"] = "strong"
                elif e >= EDGE_MODERATE: p["confidence"] = "moderate"
                elif e >= EDGE_LEAN:     p["confidence"] = "lean"
                else:                    p["confidence"] = "skip"
                if e < EDGE_SKIP:        p["confidence"] = "skip"
            picks = picks + full_picks
            pred["full"] = full_factor
            pred["full_picks"] = full_picks
        except Exception as e:
            logger.warning("NBA full-game prediction failed for %s/%s: %s",
                           h_abbr, a_abbr, e)

        # Cache picks on prediction for predict endpoint consistency
        pred_full["_cached_picks"] = picks
        pred_full["_cached_odds"] = odds
        _picks_store_put("nba", h_abbr, a_abbr, picks, odds)

        # Card + POTD agree by construction: both built from CORE
        # picks only. Derivatives flow through `derivative_picks` /
        # the dedicated tracker. Re-sort here because the merged list
        # contains both Q1 and full-game picks (each sub-picker sorts
        # internally, but the union doesn't carry the order through).
        # Headline selection prefers primary markets; ALT picks only
        # surface as the headline when no primary clears 6%+ edge.
        # Without that gate the GameCard would routinely headline a
        # +60% alt-line bet that's exactly the longshot calibration
        # trap player props got burned by.
        core_picks_all = sorted(
            (p for p in picks if p.get("type") not in _NBA_DERIV_TYPES),
            key=lambda p: -(p.get("edge") or 0),
        )
        _ALT_TYPES = {"ALT SPREAD", "ALT TOTAL"}
        _primary_picks = [p for p in core_picks_all
                           if p.get("type") not in _ALT_TYPES
                           and (p.get("edge") or 0) >= 6.0]
        # core_picks order: primary picks first (>=6% edge), then
        # everything else by edge. The card's best_pick = core_picks[0]
        # so primary-market picks always win the headline when they
        # exist, ALT only surfaces when no primary cleared 6%+.
        _alt_or_low = [p for p in core_picks_all if p not in _primary_picks]
        core_picks = _primary_picks + _alt_or_low
        nba_matchup = f"{a_abbr} @ {h_abbr}"
        nba_target = datetime.now().strftime("%Y-%m-%d")
        recorded = _get_recorded_pick("nba", nba_matchup, nba_target)
        locked = _is_game_locked(game.get("date"))
        if locked and recorded:
            best = recorded
        else:
            best = core_picks[0] if core_picks else None
        if not best:
            continue

        # Surface Q1 roster/injury info as a unified "injuries" field so
        # the scoreboard CardInsight can render shorthanded warnings the
        # same way it does for MLB.
        _factors = pred.get("factors") or {}
        _home_roster = _factors.get("home_roster") or {}
        _away_roster = _factors.get("away_roster") or {}

        def _nba_roster_impact(roster: dict) -> float:
            # Convert Q1 delta (pts) to a 0-1 strength multiplier. Q1
            # avg ~29 pts/team, so -6 Q1 pts = ~20% weaker.
            delta = roster.get("q1_delta") or 0
            if delta >= 0:
                return 1.0
            return max(0.70, 1.0 + (delta / 29.0))

        injuries_payload = {
            "home_impact": _nba_roster_impact(_home_roster),
            "away_impact": _nba_roster_impact(_away_roster),
            "home_list": _home_roster.get("out_players") or [],
            "away_list": _away_roster.get("out_players") or [],
        }

        # all_picks = top 4 CORE only (derivatives in their own panel).
        all_picks = list(core_picks[:4])
        derivative_picks = sorted(
            (p for p in picks if p.get("type") in _NBA_DERIV_TYPES),
            key=lambda p: -(p.get("edge") or 0),
        )

        # Phase 2k: per-view best picks so the GameCard toggle can swap
        # between Q1 and Full headlines without re-fetching. Each view
        # picks its highest-edge non-skip representative from the
        # respective category set, with primary-market preference for
        # Full (alt lines only headline if no primary clears 6%).
        Q1_TYPES = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
        FULL_PRIMARY = {"ML", "SPREAD", "TOTAL"}
        FULL_ALT = {"ALT SPREAD", "ALT TOTAL"}
        def _top(types_set):
            cands = [p for p in core_picks
                     if p.get("type") in types_set
                     and (p.get("confidence") or "lean") != "skip"]
            return cands[0] if cands else None
        best_q1 = _top(Q1_TYPES)
        best_full_primary = _top(FULL_PRIMARY)
        best_full_alt = _top(FULL_ALT)
        # Headline rule: primary if it cleared 6%; otherwise alt; else
        # the highest-edge primary even at 4-6% so the user still sees
        # a pick rather than nothing.
        if best_full_primary and (best_full_primary.get("edge") or 0) >= 6.0:
            best_full = best_full_primary
        elif best_full_alt:
            best_full = best_full_alt
        else:
            best_full = best_full_primary

        bets.append({
            "game_id": game["id"],
            "matchup": f"{a_abbr} @ {h_abbr}",
            "home": game["home"],
            "away": game["away"],
            "time": game["date"],
            "venue": game.get("venue", ""),
            "best_pick": best,
            "best_pick_q1": best_q1,
            "best_pick_full": best_full,
            "all_picks": all_picks,
            "derivative_picks": derivative_picks,
            "confidence": best.get("confidence", "lean"),
            "win_prob": pred.get("win_prob", {}),
            "expected_q1_score": pred.get("expected_q1_score", {}),
            "factors": pred.get("factors", {}),
            "rest": pred.get("rest", {}),
            "injuries": injuries_payload,
            "season_context": pred.get("season_context") or _nba_season_context(),
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "recorded_pick": recorded,
            "is_locked": locked,
            "full": pred.get("full"),
            "full_picks": pred.get("full_picks") or [],
        })

    bets.sort(key=lambda b: b["best_pick"].get("edge", 0), reverse=True)

    try:
        from engine.nba_tracker import refresh_pending_for_today as _nba_refresh
        nba_target = datetime.now().strftime("%Y-%m-%d")
        diff = _nba_refresh(bets, nba_target)
        if any(diff.values()):
            logger.info("Tracker pending sync (nba): %s", diff)
    except Exception as e:
        logger.warning("NBA tracker pending refresh failed: %s", e)

    try:
        from engine.derivative_tracker import record_top_derivatives
        diff = record_top_derivatives("nba", bets, n_per_game=1,
                                       min_edge=4.0,
                                       target_date=datetime.now().strftime("%Y-%m-%d"))
        if diff.get("inserted") or diff.get("updated"):
            logger.info("Derivative tracker (nba): %s", diff)
    except Exception as e:
        logger.warning("NBA derivative recorder failed: %s", e)

    try:
        from engine.pick_events import detect_transitions
        ev = detect_transitions("nba", [
            {"game_id": b["game_id"], "matchup": b["matchup"],
             "best_pick": b.get("best_pick")}
            for b in bets
        ], date=datetime.now().strftime("%Y-%m-%d"))
        if any(ev.values()):
            logger.info("Pick events (nba): %s", ev)
    except Exception as e:
        logger.warning("NBA pick-events failed: %s", e)

    _bb_progress_set("nba", phase="idle", finished_at=_time.time())
    return bets


@app.get("/api/nba/tracker/history")
def api_nba_pick_history():
    """Return recent NBA pick history.

    Filters out derivative bet types — see api_pick_history docstring
    for the trampoline-race rationale. Same pattern applies here.
    """
    try:
        from engine.nba_tracker import get_nba_pick_history
        rows = get_nba_pick_history() or []
        return [r for r in rows if r.get("bet_type") not in _NBA_DERIV_TYPES]
    except ImportError:
        return []
    except Exception as e:
        logger.error("NBA pick history failed: %s", e)
        return []


@app.get("/api/nba/tracker/summary")
def api_nba_pick_summary():
    """Get NBA running pick totals."""
    try:
        from engine.nba_tracker import get_nba_pick_summary
        return get_nba_pick_summary()
    except ImportError:
        return {"overall": {"total": 0, "wins": 0, "losses": 0, "profit": 0, "win_pct": 0}, "by_type": {}}
    except Exception as e:
        logger.error("NBA pick summary failed: %s", e)
        return {"overall": {"total": 0, "wins": 0, "losses": 0, "profit": 0, "win_pct": 0}, "by_type": {}}


@app.post("/api/nba/tracker/record")
def api_nba_record_picks():
    """Record today's NBA picks."""
    try:
        from engine.nba_tracker import record_nba_picks
        picks = record_nba_picks()
        return {"recorded": len(picks), "picks": picks}
    except ImportError:
        return {"error": "NBA tracker module not loaded yet", "recorded": 0}
    except Exception as e:
        logger.error("NBA record picks failed: %s", e, exc_info=True)
        return {"error": str(e), "recorded": 0}


@app.post("/api/nba/tracker/settle")
def api_nba_settle_picks():
    """Settle completed NBA picks + POTD."""
    try:
        from engine.nba_tracker import settle_nba_picks
        result = settle_nba_picks()
        # Parity with MLB + NHL -- also settle NBA POTDs so the POTD
        # hero record doesn't drift behind the Pick Tracker record.
        try:
            from engine.pick_of_day import settle_potd
            settle_potd("nba")
        except Exception as e:
            logger.warning("NBA POTD settle failed: %s", e)
        return result
    except ImportError:
        return {"error": "NBA tracker module not loaded yet", "settled": 0}
    except Exception as e:
        logger.error("NBA settle picks failed: %s", e, exc_info=True)
        return {"error": str(e), "settled": 0}


@app.get("/api/nba/backtest")
def api_nba_backtest(days: int = Query(default=0), min_edge: float = Query(default=3.0),
                     season: int | None = Query(default=None)):
    """Run NBA Q1 backtest on historical games."""
    try:
        from engine.nba_backtest import run_nba_backtest
        return run_nba_backtest(days=days, min_edge=min_edge, season=season)
    except ImportError:
        return {"error": "NBA backtest module not loaded yet"}
    except Exception as e:
        logger.error("NBA backtest failed: %s", e, exc_info=True)
        return {"error": str(e)}


@app.get("/api/nba/scoreboard/debug")
def api_nba_scoreboard_debug():
    """Debug: show raw ESPN NBA scoreboard response."""
    import json
    target_date = datetime.now().strftime("%Y%m%d")
    url = f"{ESPN_BASE}/basketball/nba/scoreboard?dates={target_date}"
    data = _fetch_espn_json(url)
    if not data:
        # Try without date
        data = _fetch_espn_json(f"{ESPN_BASE}/basketball/nba/scoreboard")
    if not data:
        return {"error": "ESPN returned no data", "url": url}
    events = data.get("events", [])
    result = {
        "url": url,
        "date": target_date,
        "top_keys": list(data.keys()),
        "event_count": len(events),
    }
    if events:
        ev = events[0]
        result["first_event_name"] = ev.get("name", "?")
        result["first_event_date"] = ev.get("date", "?")
        comps = ev.get("competitions", [{}])
        if comps:
            teams = comps[0].get("competitors", [])
            result["first_event_teams"] = len(teams)
            if teams:
                result["first_team_keys"] = list(teams[0].keys())[:10]
    else:
        # Check if there's a day/league info
        result["day"] = data.get("day", data.get("leagues", "?"))
    return result
