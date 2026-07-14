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

from fastapi import FastAPI, HTTPException, Query, Request, Response, Body
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from engine.db import (
    get_conn, get_all_teams, get_team_by_id, get_team_by_abbr,
    get_today_games, get_team_record, get_pitcher_season,
    get_bullpen, get_recent_games,
)
from engine.mlb_predict import predict_matchup

from .routes_tennis import router as tennis_router
from .routes_nba import router as nba_router
from .routes_nhl import router as nhl_router
from .routes_mlb import router as mlb_router
from .routes_props import router as props_router
from .routes_potd import router as potd_router
from .routes_basketball import router as basketball_router
from .routes_hockey import router as hockey_router
from .routes_motorsports import router as motorsports_router
from .routes_golf import router as golf_router
from .routes_soccer import router as soccer_router
from .routes_football import router as football_router
from .routes_baseball import router as baseball_router
from .routes_v32 import router as v32_router
from .routes_health import router as health_router
from .routes_picks_unified import router as picks_unified_router
from .routes_bet_queue import router as bet_queue_router
from .routes_queue_placer import router as queue_placer_router
from ._tz import et_today_str, et_now, et_month

logger = logging.getLogger(__name__)

SEASON = et_now().year
SERVER_STARTED_AT = time.time()

app = FastAPI(title="MLB Prediction Engine")

# CORS_ORIGINS env var lets a developer testing on a phone (e.g.,
# http://192.168.1.42:5173) add their LAN IP without editing this file.
# Comma-separated origins. Default keeps the localhost-only baseline.
_DEFAULT_CORS = "http://localhost:5173,http://127.0.0.1:5173"
_cors_origins = [
    o.strip() for o in os.getenv("CORS_ORIGINS", _DEFAULT_CORS).split(",")
    if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Sport routers split out of this file 2026-05-02 to start breaking the
# 5800+ line monolith into per-sport modules.
app.include_router(tennis_router)
app.include_router(nba_router)
app.include_router(nhl_router)
app.include_router(mlb_router)
app.include_router(props_router)
app.include_router(potd_router)
app.include_router(basketball_router)
app.include_router(hockey_router)
app.include_router(motorsports_router)
app.include_router(golf_router)
app.include_router(soccer_router)
app.include_router(football_router)
app.include_router(baseball_router)
app.include_router(v32_router)
app.include_router(health_router)
# Unified picks API — cross-sport read endpoints fed by the
# engine.picks_unified layer. Phase 2 of the unification work.
app.include_router(picks_unified_router)
# Auto-eligible bet queue — cross-sport recommendation stream. Filters
# today's picks to those in historically profitable cells (n>=50 +
# stake-weighted ROI>=5%). Read-only; no actual bet placement.
app.include_router(bet_queue_router)
# Queue auto-placer routes (live-fire kill switch, placement log,
# manual sweep trigger, relay health probe). Default posture is
# DRY-RUN — real bets require AUTO_BET_LIVE=1 env + /live-fire/on
# flag, and every attempt is logged to data/queue_placer/placements.db.
app.include_router(queue_placer_router)


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

from ._espn import ESPN_BASE, _fetch_espn_json  # noqa: F401
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


# ── Endpoints ───────────────────────────────────────────────

@app.post("/api/_client_error")
async def api_client_error(req: Request):
    """Receiver for the frontend ErrorBoundary's fire-and-forget report.
    Logs the error to the backend so client crashes end up in server
    logs without depending on the user opening devtools. Always returns
    204 — never blocks the boundary's recovery path."""
    try:
        body = await req.json()
    except Exception:
        body = {}
    logger.warning(
        "frontend ErrorBoundary: %s :: %s",
        body.get("message", "<no-message>"),
        (body.get("componentStack") or "").splitlines()[0:1],
    )
    return Response(status_code=204)


@app.get("/api/{sport}/live-state")
def api_live_state(sport: str, game_id: str | None = Query(default=None)):
    """Phase 3a debug endpoint — returns whatever the live worker has
    written for the requested sport. Used during development to verify
    the worker is healthy + the parsed state shape matches what the
    predictor in 3b will read.

    `?game_id=...` returns a single game's state; without it returns
    every active row for the sport.
    """
    if sport not in ("nba", "nhl"):
        raise HTTPException(status_code=400, detail="live engine only covers NBA + NHL")
    from engine.live._store import get_state, list_active, worker_heartbeat
    if game_id:
        s = get_state(sport, game_id)
        if not s:
            return {"sport": sport, "game_id": game_id,
                    "state": None, "reason": "stale or missing"}
        return {"sport": sport, "game_id": game_id, "state": s}
    return {
        "sport": sport,
        "active_games": list_active(sport),
        "worker": worker_heartbeat(sport),
    }


@app.get("/api/{sport}/live-picks")
def api_live_picks_active(sport: str):
    """Phase 3b read endpoint — runs the live picker against every
    in-progress game in the live store and returns the surfaced edges.

    These are *candidate* picks (the engine's current recommendations)
    — they're not locked to the tracker until the user explicitly
    POSTs to /live-picks/record. The frontend live tab polls this
    every 15s (NBA) / 30s (NHL) and the user clicks lock on the ones
    they want frozen.

    Returns every active game even when no picks clear the edge floor
    so the frontend can render "live but no edges right now" per game
    instead of mistaking 'no qualifying picks' for 'worker is down'.
    """
    if sport not in ("nba", "nhl", "wnba", "ncaam", "afl"):
        raise HTTPException(
            status_code=400,
            detail="live engine only covers NBA/NHL/WNBA/NCAAM/AFL",
        )
    from engine.live import get_live_picks, list_active
    out: list[dict] = []
    for row in list_active(sport):
        gid = row["game_id"]
        # NBA/NHL run the continuous-live picker; WNBA/NCAAM/AFL only
        # emit at intermission boundaries via the predictor module, so
        # the active-state endpoint surfaces empty picks for them and
        # the user picks up live edges via the /live-picks/history tab
        # once an intermission lands.
        picks = (get_live_picks(sport, gid)
                  if sport in ("nba", "nhl") else [])
        state = row["state"] or {}
        # Include every active game, even if picks=[]. The
        # LiveGameCard component already renders an empty-picks
        # placeholder ("No edges above floor right now") so the live
        # state stays visible even during cold-start / smoothing
        # windows where no edge clears the floor.
        out.append({
            "game_id": gid,
            "matchup": state.get("matchup"),
            "state": state.get("status"),
            "home": state.get("home"),    # {abbr, score, name}
            "away": state.get("away"),
            "linescores": state.get("linescores"),
            "picks": picks,
        })
    return {"sport": sport, "games": out}


@app.post("/api/{sport}/live-picks/record")
def api_live_picks_record(sport: str, payload: dict = Body(...)):
    """Lock one live pick into the live tracker. Body is a single pick
    dict in the same shape `engine.live.get_live_picks` returns
    (sport / game_id / matchup / bet_type / pick / odds / model_prob /
    edge_pct / snapshot{...}).

    Returns the row id; frontend uses this to dedupe rapid double-
    clicks. Idempotency is the caller's responsibility — same edge
    surfaces every poll, and we'll happily insert two rows if the user
    clicks twice.
    """
    if sport == "wnba":
        # WNBA predictor port pending — record endpoint returns 503 so
        # the lock button shows a clear "not yet" rather than a 400.
        raise HTTPException(
            status_code=503,
            detail="WNBA live picks not online yet — pending predictor port",
        )
    if sport not in ("nba", "nhl"):
        raise HTTPException(status_code=400, detail="live tracker only covers NBA + NHL")
    from engine.live_tracker import record_live_pick
    try:
        new_id = record_live_pick(sport, payload)
    except Exception as e:
        logger.warning("live-picks record failed: %s", e)
        raise HTTPException(status_code=400, detail=str(e))
    return {"id": new_id, "sport": sport}


@app.get("/api/{sport}/live-picks/history")
def api_live_picks_history(sport: str, limit: int = 200):
    """Tracker view — pending + settled live picks for a sport, most-
    recent first. Each row carries snapshot fields (period / clock /
    score / remaining_s) so the UI can render the lock-time context."""
    if sport not in ("nba", "nhl", "wnba", "ncaam", "afl"):
        raise HTTPException(
            status_code=400,
            detail="live tracker only covers NBA/NHL/WNBA/NCAAM/AFL",
        )
    from engine.live_tracker import list_history
    return {"sport": sport, "rows": list_history(sport, limit=limit)}


@app.post("/api/{sport}/live-picks/settle")
def api_live_picks_settle(sport: str):
    """Manually trigger settlement. The live worker calls this on a
    cadence (every 30-60s) but exposing it lets ops force a sweep.
    Returns ``{settled, wins, losses, pushes, pending_remaining}``."""
    if sport not in ("nba", "nhl", "wnba", "ncaam", "afl"):
        raise HTTPException(
            status_code=400,
            detail="live tracker only covers NBA/NHL/WNBA/NCAAM/AFL",
        )
    from engine.live_tracker import settle_live_picks
    return settle_live_picks(sport)


@app.get("/api/_health")
def api_health():
    """Deep-check health endpoint — exercises every dependency the
    prediction pipeline needs (per-sport DBs, today's schedule rows,
    calibration tables, picks_cache, GBM artifacts, stale POTD locks)
    and returns a structured payload.

    HTTP status: 200 on ok or warn (degraded but online — monitors
    shouldn't flap on stale-data), 503 on fail (a check is hard
    failing). The `status` field still carries the granular signal.

    See backend/health.py for the per-check details.
    """
    from ..health import run_health_checks
    payload = run_health_checks()
    # Hard failures (any check with status=fail) elevate to 503 so
    # standard HTTP monitors can alert without JSON parsing.
    if payload.get("status") == "fail":
        return JSONResponse(content=payload, status_code=503)
    return payload


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
    """Health check covering DB connectivity, sync recency, and per-sport
    row counts. Designed to be polled by Task Scheduler / external
    monitors -- a 'degraded' status indicates a sport's data is stale
    enough that picks may be wrong. (Odds API credit-balance check was
    removed 2026-05-11 along with the Odds API integration itself.)"""
    from engine.db import DB_PATH as MLB_DB

    payload: dict = {
        "status": "ok",
        "uptime_seconds": int(time.time() - SERVER_STARTED_AT),
        "started_at": datetime.fromtimestamp(SERVER_STARTED_AT, timezone.utc).isoformat(),
        "sports": {},
    }
    degraded_reasons: list[str] = []

    def _sport_status(label: str, db_path, picks_table: str | None,
                       conn_factory, teams_table: str = "teams",
                       games_table: str = "games") -> dict:
        try:
            conn = conn_factory()
            teams = conn.execute(
                f"SELECT COUNT(*) AS c FROM {teams_table}"
            ).fetchone()["c"]
            # `updated_at` may not exist on per-sport games tables (NHL
            # uses `created_at`, NBA varies). Try updated_at first;
            # fall back to date if missing so the check still runs.
            try:
                last_game_row = conn.execute(
                    f"SELECT MAX(updated_at) AS t FROM {games_table}"
                ).fetchone()
            except Exception:
                last_game_row = conn.execute(
                    f"SELECT MAX(date) AS t FROM {games_table}"
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
        payload["sports"]["nhl"] = _sport_status(
            "nhl", NHL_DB, "nhl_picks", _nhl_conn,
            teams_table="nhl_teams", games_table="nhl_games",
        )
    except Exception:
        pass
    try:
        from engine.nba_db import get_conn as _nba_conn, DB_PATH as NBA_DB
        payload["sports"]["nba"] = _sport_status(
            "nba", NBA_DB, "nba_picks", _nba_conn,
            teams_table="nba_teams", games_table="nba_games",
        )
    except Exception:
        pass

    # Odds API was retired 2026-05-11 — user dropped the subscription
    # after HR (curl_cffi TLS fix) became reliable as the primary source.

    if degraded_reasons:
        payload["status"] = "degraded"
        payload["degraded_reasons"] = degraded_reasons

    # Promote actual failures (any sport with ok=False) to a real HTTP
    # error code so standard monitors can alert without parsing JSON.
    # Degraded-but-online stays 200 so monitors don't flap on stale-data.
    has_fail = any(s.get("ok") is False for s in payload["sports"].values())
    if has_fail:
        payload["status"] = "fail"
        return JSONResponse(content=payload, status_code=503)

    return payload


# /api/teams + /api/teams/{team_id} migrated to routes_mlb.py (#317).


def _get_scoreboard(date: str = "") -> list[dict]:
    """Core scoreboard logic - reusable by other endpoints."""
    target_date = date or et_today_str()
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
        yest = (et_now() - _td(days=1)).strftime("%Y%m%d")
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
        tomorrow = (et_now() + _td(days=1)).strftime("%Y%m%d")
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

    # The-Odds-API fallback removed 2026-05-11; HR is the primary source.

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


# /api/scoreboard migrated to routes_mlb.py (#317).


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


# PredictRequest model migrated to routes_mlb.py (#317).


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
    today = et_today_str()
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
    today = et_today_str()
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
    today = et_today_str()
    cutoff = (et_now() - timedelta(days=7)).strftime("%Y-%m-%d")
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
            # Matchup-abbr drift: ESPN scoreboard uses ARI/CHW/WSH/ATH
            # while tracker rows were saved with HR's AZ/CWS/WAS/OAK. Try
            # every alias permutation of the (away, home) pair before
            # giving up. Without this, AZ@COL stays in the tracker but
            # never resurfaces on the card because matchup mismatches.
            from engine.abbr import aliases_for
            sep = " @ " if " @ " in matchup else "@"
            parts = matchup.split(sep)
            if len(parts) == 2:
                away, home = parts[0].strip(), parts[1].strip()
                away_aliases = aliases_for(away, sport)
                home_aliases = aliases_for(home, sport)
                for a in away_aliases:
                    for h in home_aliases:
                        alt = f"{a} @ {h}"
                        if alt == matchup:
                            continue
                        row = conn.execute(
                            f"SELECT bet_type, pick, model_prob, edge, odds "
                            f"FROM {table} "
                            f"WHERE date = ? AND matchup = ? "
                            f"AND result IS NULL "
                            f"ORDER BY edge DESC LIMIT 1",
                            (date, alt),
                        ).fetchone()
                        if row:
                            break
                    if row:
                        break

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

        # Derive stake_units from (prob, edge) at read time. The picks
        # table doesn't persist this column (it's a pure function of
        # the two values it already stores), but the card's EdgeBadge
        # needs it to render the unit-size pill. Without it, every
        # locked / pre-locked recorded pick renders without a stake
        # badge — visible most often on MLB ALT O/U because that's
        # the bet_type users notice the missing pill on.
        try:
            from engine._pick_helpers import stake_units_for
            stake_units = stake_units_for(row["model_prob"] or 0.0, edge)
        except Exception:
            stake_units = None
        return {
            "type": row["bet_type"],
            "pick": row["pick"],
            "prob": row["model_prob"],
            "edge": edge,
            "odds": row["odds"],
            "confidence": conf,
            "stake_units": stake_units,
            "from_tracker": True,
        }
    except Exception as e:
        logger.debug("Could not read recorded pick for %s %s: %s", sport, matchup, e)
        return None


# Best-bets progress + single-flight infra lives in ._bestbets so per-sport
# router modules (routes_nba, routes_nhl) can import the decorator at module
# load time without a circular dependency on this package.
from ._bestbets import (  # noqa: F401
    _BB_PROGRESS, _BB_PROGRESS_LOCK,
    _bb_progress_set, _bb_progress_increment, _bb_progress_snapshot,
    _bb_single_flight, _bb_reset_on_exit,
)


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


def _pred_cache_clear_sport(sport: str) -> int:
    """Drop every cached prediction for ``sport``. Returns the number
    of entries removed.

    Called after lineup / injury / goalie refreshes detect a roster
    delta. The cache key includes pitcher IDs but NOT the lineup
    snapshot, so a same-pitcher prediction with a changed lineup would
    otherwise keep returning the stale morning blob until the 30-min
    TTL expires. Whole-sport clear is cheap (≤15 games re-predict on
    next request — same as a cold slate) and removes the precision
    burden of mapping team_ids → cache keys per sport."""
    with _PRED_CACHE_LOCK:
        keys = [k for k in _PRED_CACHE if k and k[0] == sport]
        for k in keys:
            _PRED_CACHE.pop(k, None)
    return len(keys)


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
            today_s = et_today_str()
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
                "date": et_today_str(),
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
        log_meta = {
            "date": et_today_str(),
            "game_id": f"{et_today_str()}_{home_team_id}_{away_team_id}",
        }
        result["ensemble"] = ensemble_mlb(result, log_meta=log_meta)
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
            today_s = et_today_str()
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
                today_s = et_today_str()
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

    _log_signals_nhl_safe(home_key, away_key, result)

    if use_cache:
        _pred_cache_put(cache_key, result)
    return result


def _log_signals_nhl_safe(home_key: str, away_key: str,
                          result: dict | None) -> None:
    """Persist NHL factor/MC/GBM probabilities for ``home_win`` and
    ``total`` markets so engine.ensemble_auto_tune has data to retune
    from. Mirrors the MLB block in ``api_predict``. Wrapped in a
    blanket try/except — signal logging is best-effort, never blocks
    the prediction path.

    Lookups: result['win_prob']['home'] (factor),
    result['mc']['win_prob']['home'] (MC), result['gbm']['home_win']
    (GBM). Total: result['total'], mc.expected_goals.total,
    gbm.total_goals.
    """
    if not result:
        return
    try:
        from engine.prediction_log import log_signals as _log_signals
        from engine.nhl_db import get_conn as _nhl_conn
        from datetime import datetime as _dt
        today = _dt.now().strftime("%Y-%m-%d")
        home_abbr = (result.get("home") or {}).get("abbreviation") or home_key
        away_abbr = (result.get("away") or {}).get("abbreviation") or away_key
        # Resolve game_id + team_ids from the schedule. Missing matches
        # land with None values; log_signals stores them and the
        # tuner just skips orphaned rows at JOIN time.
        conn = _nhl_conn()
        rows = conn.execute(
            "SELECT id, abbreviation FROM nhl_teams WHERE abbreviation IN (?, ?)",
            (home_abbr.upper() if home_abbr else "",
             away_abbr.upper() if away_abbr else "")
        ).fetchall()
        by_abbr = {r["abbreviation"]: r["id"] for r in rows}
        home_tid = by_abbr.get((home_abbr or "").upper())
        away_tid = by_abbr.get((away_abbr or "").upper())
        game_row = conn.execute(
            "SELECT game_id FROM nhl_games "
            "WHERE date = ? AND home_team_id = ? AND away_team_id = ? LIMIT 1",
            (today, home_tid, away_tid),
        ).fetchone() if (home_tid and away_tid) else None
        game_id = game_row["game_id"] if game_row else None

        mc = result.get("mc") or {}
        gbm = result.get("gbm") or {}
        if "error" in mc:
            mc = {}
        if "error" in gbm:
            gbm = {}

        signals: dict[str, dict] = {}
        # home_win
        factor_hw = (result.get("win_prob") or {}).get("home")
        mc_hw = (mc.get("win_prob") or {}).get("home")
        gbm_hw = gbm.get("home_win")
        if factor_hw is not None or mc_hw is not None or gbm_hw is not None:
            signals["home_win"] = {"factor": factor_hw, "mc": mc_hw,
                                    "gbm": gbm_hw}
        # total (expected goals)
        factor_t = result.get("total")
        mc_t = (mc.get("expected_goals") or {}).get("total")
        gbm_t = gbm.get("total_goals")
        if factor_t is not None or mc_t is not None or gbm_t is not None:
            signals["total"] = {"factor": factor_t, "mc": mc_t,
                                 "gbm": gbm_t}

        if signals:
            _log_signals("nhl", game_id, today,
                         home_tid, away_tid, signals)
    except Exception as e:
        logger.debug("NHL signal logging failed: %s", e)


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
            today_s = et_today_str()
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
                    "date": et_today_str(),
                })
            else:
                result["gbm"] = {"error": f"team_id lookup failed: "
                                          f"{h_abbr_u!r}/{a_abbr_u!r}"}
        except Exception as e:
            logger.warning("NBA GBM failed for %s/%s: %s", home_abbr, away_abbr, e)
            result["gbm"] = {"error": str(e)}

    _log_signals_nba_safe(home_abbr, away_abbr, result)

    if use_cache:
        _pred_cache_put(cache_key, result)
    return result


def _log_signals_nba_safe(home_abbr: str, away_abbr: str,
                           result: dict | None) -> None:
    """Persist NBA Q1 factor/MC/GBM probabilities for ``q1_home_win``
    and ``q1_total`` so engine.ensemble_auto_tune has data to retune
    NBA weights from. Same wrapping pattern as the NHL/MLB versions —
    blanket try/except so a missing schedule row never blocks the
    prediction path."""
    if not result:
        return
    try:
        from engine.prediction_log import log_signals as _log_signals
        from engine.nba_db import get_conn as _nba_conn
        from datetime import datetime as _dt
        today = _dt.now().strftime("%Y-%m-%d")
        h_abbr_u = (home_abbr or "").upper()
        a_abbr_u = (away_abbr or "").upper()

        conn = _nba_conn()
        rows = conn.execute(
            "SELECT id, abbreviation FROM nba_teams WHERE abbreviation IN (?, ?)",
            (h_abbr_u, a_abbr_u),
        ).fetchall()
        by_abbr = {r["abbreviation"]: r["id"] for r in rows}
        home_tid = by_abbr.get(h_abbr_u)
        away_tid = by_abbr.get(a_abbr_u)
        game_row = conn.execute(
            "SELECT game_id FROM nba_games "
            "WHERE date = ? AND home_team_id = ? AND away_team_id = ? LIMIT 1",
            (today, home_tid, away_tid),
        ).fetchone() if (home_tid and away_tid) else None
        game_id = game_row["game_id"] if game_row else None

        mc = result.get("mc") or {}
        gbm = result.get("gbm") or {}
        if "error" in mc:
            mc = {}
        if "error" in gbm:
            gbm = {}

        signals: dict[str, dict] = {}
        # q1_home_win — factor exposes q1_ml_home directly
        factor_hw = result.get("q1_ml_home")
        mc_hw = (mc.get("win_prob") or {}).get("home")
        gbm_hw = gbm.get("q1_home_win")
        if factor_hw is not None or mc_hw is not None or gbm_hw is not None:
            signals["q1_home_win"] = {"factor": factor_hw, "mc": mc_hw,
                                       "gbm": gbm_hw}
        # q1_total — predicted_total from factor, MC expected_points.total,
        # GBM q1_total_points
        factor_t = result.get("predicted_total")
        mc_t = (mc.get("expected_points") or {}).get("total")
        gbm_t = gbm.get("q1_total_points")
        if factor_t is not None or mc_t is not None or gbm_t is not None:
            signals["q1_total"] = {"factor": factor_t, "mc": mc_t,
                                    "gbm": gbm_t}

        if signals:
            _log_signals("nba", game_id, today,
                         home_tid, away_tid, signals)
    except Exception as e:
        logger.debug("NBA signal logging failed: %s", e)


# /api/predict migrated to routes_mlb.py (#317).


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
                    hours: int = 24, scope: str | None = None):
    """Recent pick-event log for the BestBets card 📜 breadcrumb.

    Returns the appeared / swapped / pulled / line_shift transitions
    the model has emitted today (or longer via ``hours``). Scope to a
    single matchup with ``game_id`` so each card only loads its own
    thread instead of the whole slate.

    ``scope`` filters by family ("q1" / "full" — NBA only). When the
    NBA card is rendered on the Full view the frontend passes
    scope="full" so the popover shows only Full-family transitions
    (and vice versa for Q1).
    """
    if sport not in ("mlb", "nhl", "nba"):
        return {"error": f"unknown sport: {sport}"}
    from engine.pick_events import list_events
    return list_events(sport, game_id=game_id, hours=hours, scope=scope)


# /api/best-bets + _bb_predict_one migrated to routes_mlb.py (#317).


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
    m = et_month()
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
    now = et_now()
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


# /api/tracker/history migrated to routes_mlb.py (#317).


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




# /api/teams/{team_id}/profile + /api/calibrate migrated to routes_mlb.py (#317).


@app.get("/api/calibration/status")
def api_calibration_status():
    """Return current model weights and calibration info."""
    from engine.calibration import get_calibration_status
    return get_calibration_status()


@app.get("/api/calibration/reliability/{sport}")
def api_calibration_reliability(sport: str, days: int = 0,
                                 buckets: int = 10):
    """Reliability diagram data for a sport. Powers the Calibration
    tab — see engine/calibration_diagnostics.py.

    Returns per-(bet_type) bucket arrays + overall + headline metrics
    (Brier, ECE) so the dashboard can render reliability curves
    without further computation.
    """
    if sport.lower() not in ("mlb", "nhl", "nba", "tennis"):
        raise HTTPException(status_code=400,
                             detail=f"Unknown sport: {sport}")
    from engine.calibration_diagnostics import reliability_for_sport
    return reliability_for_sport(sport.lower(), days=days, n_buckets=buckets)


@app.get("/api/calibration/reliability")
def api_calibration_reliability_all(days: int = 0, buckets: int = 10):
    """All-sports reliability data in one call."""
    from engine.calibration_diagnostics import reliability_all_sports
    return reliability_all_sports(days=days, n_buckets=buckets)


@app.get("/api/provenance/{sport}")
def api_provenance_list(sport: str,
                         date_from: str | None = None,
                         date_to: str | None = None,
                         bet_type: str | None = None,
                         accepted: str | None = None,
                         limit: int = 100):
    """List recent pick-provenance rows. ``accepted`` accepts
    'true' / 'false' strings for URL-friendly filtering."""
    from engine.pick_provenance import query as _query
    accepted_filter: bool | None = None
    if accepted is not None:
        accepted_filter = accepted.lower() in ("true", "1", "yes")
    return {
        "sport": sport,
        "rows": _query(sport,
                        date_from=date_from, date_to=date_to,
                        bet_type=bet_type,
                        accepted=accepted_filter,
                        limit=limit),
    }


@app.get("/api/provenance/{sport}/{date}/{pick_key}")
def api_provenance_get(sport: str, date: str, pick_key: str):
    """Fetch full decoded trace for one pick. Powers a 'why this
    pick' drill-down in the UI."""
    from engine.pick_provenance import get as _get
    out = _get(sport, date, pick_key)
    if not out:
        raise HTTPException(status_code=404, detail="provenance not found")
    return out


# /api/calibrate/teams + /api/debug/odds + /api/debug/teams + /api/backtest
# + /api/tracker/record + /api/tracker/settle + /api/mlb/refresh-lineups
# migrated to routes_mlb.py (#317).


@app.post("/api/nhl/refresh-goalies")
def api_refresh_nhl_goalies(date: str | None = Query(None),
                             record: bool = Query(True)):
    """Re-check confirmed NHL goalie starters for `date` (defaults to
    today) and invalidate picks for games whose announced starter has
    changed since the snapshot. Goalie change is the single biggest
    in-day swing factor for NHL win probability (5-8% per game), so
    this should fire every 15-30 min through the afternoon."""
    try:
        from engine.nhl_goalie_refresh import refresh_for_date
        result = refresh_for_date(date=date, record_picks=record)
        if (result.get("invalidated") or 0) > 0:
            cleared = _pred_cache_clear_sport("nhl")
            result["pred_cache_cleared"] = cleared
        return result
    except Exception as e:
        logger.error("NHL goalie refresh failed: %s", e, exc_info=True)
        return {"error": str(e), "deltas": 0, "invalidated": 0}


@app.post("/api/nba/refresh-injuries")
def api_refresh_nba_injuries(date: str | None = Query(None),
                              record: bool = Query(True)):
    """Re-check NBA OUT-player sets for `date` (defaults to today) and
    invalidate picks for games whose OUT roster has changed. Targets
    day-of load-management decisions and late injury report drops.

    Also backstops the daily schedule sync: before the injury refresh
    runs, hit ESPN's scoreboard for the date so any games the cron
    missed (laptop off / sync.bat skipped) land in ``nba_games``.
    Without this, the live intermission predictor fires on games that
    have no canonical row (observed 2026-05-13: CLE@DET game_id missing
    from nba_games even though live picks were emitted)."""
    try:
        from engine.sports.nba.db import get_conn as _nba_conn
        from datetime import datetime
        target = date or datetime.now().strftime("%Y-%m-%d")
        # ESPN scoreboard takes YYYYMMDD (no dashes).
        try:
            from scrapers.nba_espn import fetch_scoreboard as _nba_scoreboard
            yyyymmdd = target.replace("-", "")
            _nba_scoreboard(date=yyyymmdd)
        except Exception as exc:
            logger.warning("NBA scoreboard backstop failed for %s: %s",
                           target, exc)
        from engine.nba_injury_refresh import refresh_for_date
        result = refresh_for_date(date=date, record_picks=record)
        if (result.get("invalidated") or 0) > 0:
            cleared = _pred_cache_clear_sport("nba")
            result["pred_cache_cleared"] = cleared
        return result
    except Exception as e:
        logger.error("NBA injury refresh failed: %s", e, exc_info=True)
        return {"error": str(e), "deltas": 0, "invalidated": 0}


# /api/tracker/summary migrated to routes_mlb.py (#317).


# /api/standings + /api/pitcher/{pitcher_id} migrated to routes_mlb.py (#317).


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
    target_date = date or et_today_str()
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
        yest = (et_now() - _td(days=1)).strftime("%Y%m%d")
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
        tomorrow = (et_now() + _td(days=1)).strftime("%Y%m%d")
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

# Negative-result TTL: when upstream returns empty, DON'T retry on every
# best-bets tick. Cache the empty dict for NHL_EMPTY_CACHE_TTL seconds so
# transient upstream failures don't hammer the source.
NHL_EMPTY_CACHE_TTL = 120


def _fetch_nhl_odds() -> dict:
    """Fetch NHL odds from Hard Rock Bet (FL operator). Cached for
    10 min; 2 min for empty results so a transient upstream failure
    doesn't hammer it."""
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

    _nhl_odds_cache = merged
    _nhl_odds_cache_time = time.time()
    return merged


# _fetch_nhl_odds_oddsapi removed 2026-05-11 along with the rest of
# the The-Odds-API integration.


# /api/accuracy migrated to routes_mlb.py (#317).


@app.get("/api/line-movement/{sport}/{matchup_key}")
def api_line_movement(sport: str, matchup_key: str):
    """Get line movement for a specific game."""
    try:
        from engine.line_movement import get_line_movement
        current_odds = {}
        if sport == "mlb":
            from engine.picks import fetch_real_odds_for_games
            all_odds = fetch_real_odds_for_games()
            current_odds = all_odds.get(matchup_key, {})
        return get_line_movement(sport, matchup_key, current_odds) or {"movement": "none"}
    except Exception as e:
        return {"error": str(e)}





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
    target_date = date or et_today_str()
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
        yest = (et_now() - _td(days=1)).strftime("%Y%m%d")
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
        tomorrow = (et_now() + _td(days=1)).strftime("%Y%m%d")
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

    # Fetch NBA odds via the unified chain (Hard Rock → ESPN).
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
    """Fetch NBA odds (full-game + Q1 markets) via the unified fallback
    chain in scrapers.nba_odds (Hard Rock → ESPN).

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




# Tennis routes + their constants now live in routes_tennis.py.


