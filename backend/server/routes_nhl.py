"""NHL API routes.

Extracted 2026-05-02 from backend/server/__init__.py as part of the
ongoing per-sport router split (#157, #161). Mounted in __init__.py via
``app.include_router(nhl_router)``.

Shared helpers come from ``._espn`` (ESPN_BASE / fetch) and
``._bestbets`` (single-flight + progress decorator). Heavy NHL helpers
(_predict_nhl_full, _get_nhl_scoreboard, _fetch_nhl_odds, etc.) are
still in __init__.py and accessed via lazy proxies — same pattern as
routes_nba.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from ._espn import ESPN_BASE, _fetch_espn_json
from ._bestbets import (
    _bb_progress_set, _bb_progress_increment, _bb_progress_snapshot,
    _bb_reset_on_exit,
)
from ._tz import et_today_str, et_now
# Hot-path imports — moved up from inside per-request handlers to save
# the per-call module-table lookup cost in the NHL best-bets pipeline.
from engine.picks import (
    match_odds as _match_picks,
    get_best_pick as _get_best_pick_shared,
)

logger = logging.getLogger(__name__)
router = APIRouter()


# --- Cold-start self-warm (Austin 2026-10-03) ---------------------------------
# The NHL board endpoint is heavy on a COLD hit (first request after a backend
# restart): it loads the model + computes predictions for the full slate, so the
# first caller otherwise eats ~3-4 min while everything warms. (The goalie
# refresh in the piggyback is only ~4s — not the cost.) To keep real callers off
# that cold path, a module-level daemon thread self-hits /api/nhl/best-bets once
# shortly after startup, warming the model + caches before anyone opens the NHL
# tab. Module-level on purpose — @app.on_event never fires here (see memory).
# Fully fail-safe: any error just means the first real caller warms it instead.
def _nhl_coldstart_selfwarm():
    import time as _t
    import urllib.request as _u
    _t.sleep(25)  # let uvicorn bind + the app finish importing
    try:
        _u.urlopen("http://127.0.0.1:8000/api/nhl/best-bets", timeout=330).read()
        logger.info("NHL cold-start self-warm: board warmed")
    except Exception as _e:
        logger.warning("NHL cold-start self-warm skipped: %s", _e)


try:
    import threading as _threading
    _threading.Thread(target=_nhl_coldstart_selfwarm, daemon=True,
                       name="nhl-coldstart-selfwarm").start()
except Exception:
    pass


# Lazy proxies for helpers that still live in backend/server/__init__.py.
# Resolved on first call so importing this module FROM __init__.py
# doesn't trigger a circular import.
def _lazy(name):
    def proxy(*args, **kwargs):
        from backend import server as _srv
        return getattr(_srv, name)(*args, **kwargs)
    proxy.__name__ = name
    return proxy

_get_nhl_scoreboard = _lazy("_get_nhl_scoreboard")
_fetch_nhl_odds = _lazy("_fetch_nhl_odds")
_nhl_alt_abbr = _lazy("_nhl_alt_abbr")
_nhl_espn_to_key = _lazy("_nhl_espn_to_key")
_nhl_resolve_game_type = _lazy("_nhl_resolve_game_type")
_predict_nhl_full = _lazy("_predict_nhl_full")
_odds_from_scoreboard_cache = _lazy("_odds_from_scoreboard_cache")
_get_nhl_db = _lazy("_get_nhl_db")
_pred_cache_clear_sport = _lazy("_pred_cache_clear_sport")
_is_game_imminent = _lazy("_is_game_imminent")
_is_game_locked = _lazy("_is_game_locked")
_get_recorded_pick = _lazy("_get_recorded_pick")
_picks_store_get = _lazy("_picks_store_get")
_picks_store_put = _lazy("_picks_store_put")


class _LazyConst:
    """Bare-name lookup proxy for collections in __init__.py.
    See routes_nba._LazyConst for the rationale (PEP 562 doesn't
    catch bare-name lookups inside the module's own functions).
    Delegates arbitrary attribute access (``.clear()``, ``.update()``)
    to the resolved object so mutable cache dicts also work."""
    def __init__(self, name: str):
        object.__setattr__(self, "_name", name)
        object.__setattr__(self, "_resolved", None)

    def _get(self):
        if self._resolved is None:
            from backend import server as _srv
            object.__setattr__(self, "_resolved", getattr(_srv, self._name))
        return self._resolved

    def __contains__(self, item):
        return item in self._get()

    def __iter__(self):
        return iter(self._get())

    def __len__(self):
        return len(self._get())

    def __getattr__(self, attr):
        return getattr(self._get(), attr)


_NHL_DERIV_TYPES = _LazyConst("_NHL_DERIV_TYPES")

# Pick Authority shadow lane (MintyBot 2026-10-26, Austin "make sure NHL is up to snuff"): a
# second pricing pass on the ratings model's own probability (W=1) runs at most every
# _NHL_SHADOW_EVERY_S per game -- the recorder keeps each market's first sighting anyway.
_NHL_SHADOW_EVERY_S = 1800
_NHL_SHADOW_LAST: dict = {}
_NHL_SHADOW_CANDS: dict = {}   # game_id -> last shadow-lane candidates: the card's model lean (2026-10-26)


def _nhl_is_p1(p: dict) -> bool:
    return str((p or {}).get("pick") or "").startswith("P1 ")
_NHL_ESPN_TO_KEY = _LazyConst("_NHL_ESPN_TO_KEY")


# ── PAPER picks ──────────────────────────────────────────────────────
# The stacked-calibration paper helpers (_nhl_cal_prob / _nhl_paper_picks / _nhl_p1_paper_picks)
# were retired 2026-10-26: every NHL lean (card, P1 view, paper tracker, POTD) is now the Pick
# Authority's select_pick over candidates priced off the ratings model and anchored once
# (engine.sports.nhl._market_anchor). See _compute_nhl_best_bets.


@router.post("/api/nhl/sync")
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


@router.get("/api/nhl/scoreboard")
def api_nhl_scoreboard(date: str = Query(default="")):
    """Return today's NHL games."""
    games = _get_nhl_scoreboard(date)
    # Attach the RECORDED paper lean (carries the settled W/L) to each game so
    # live/final cards keep showing the pick + result like WNBA. best-bets is
    # pre-game only, so it drops live/final games (Austin 2026-09-29).
    if isinstance(games, list):
        try:
            from engine.nhl_tracker._summary import recorded_nhl_paper_pick as _rpp
            for _g in games:
                # Scheduled (state 'pre'): LOCK the card to the recorded (held) first lean so it
                # stops flipping as the line moves (Austin 2026-10-06) -- the line moving our way
                # is a CLV win, not a reason to re-pick. Live/final: fill the recorded pick only
                # when best-bets dropped the game (prior behavior).
                if isinstance(_g, dict):  # ALWAYS lock to best recorded pick (full-game + P1), held live/final -> card==tracker (Austin 2026-10-17)
                    _gid = _g.get("id")
                    if _gid is not None:
                        # _g["date"] is the raw ESPN scoreboard field, a UTC
                        # ISO timestamp (e.g. '2026-10-02T01:00Z' for a 9pm
                        # ET game) -- NOT the plain 'YYYY-MM-DD' nhl_picks.date
                        # uses. Convert via _et_game_date so the lookup scopes
                        # to the correct ET calendar date instead of silently
                        # never matching (Austin 2026-10-01 bugfix, paired with
                        # the recorded_nhl_paper_pick date guard).
                        _p = _rpp(_gid, date=_et_game_date(_g.get("date")) or None)
                        if not _p and (_g.get("status") or {}).get("state") in ("in", "post"):
                            # no recorded pick: the lean the card held pre-game (Austin 2026-10-27)
                            from engine.selection import lean_lock as _lean_lock
                            _p = _lean_lock.held_with_result("nhl", "nhl", str(_gid))
                        if _p:
                            if (_g.get("status") or {}).get("state") in ("in", "post"):
                                # best-bets (the pre-game graduated flag) drops started games (Austin 2026-10-29)
                                from engine.selection import lean_lock as _lean_lock
                                _lean_lock.flag_graduation("nhl", "nhl", _p)
                            _g["paper_pick"] = _p
        except Exception:
            pass
    # Board window (Austin 2026-10-01): the default call (no explicit `date` --
    # i.e. the NHL board the frontend polls) rides _get_nhl_scoreboard's
    # NHL_LOOKAHEAD_DAYS lookahead (default 7) so best-bets / HR-priced cards can
    # see a week out, but the BOARD itself should only show today + tomorrow.
    # Filter post-hoc here rather than shrinking the shared helper's lookahead so
    # _compute_nhl_best_bets() (which also calls _get_nhl_scoreboard(lookahead=True))
    # keeps its existing window. Compare each game's ET calendar date, not a bare
    # UTC slice -- an evening ET game's UTC timestamp has already rolled to the
    # next day (e.g. '2026-10-02T01:00Z' for a 9pm ET game on Oct 1), so [:10]
    # (or a naive date() compare) lands a day off from the ET boundary Austin
    # actually means. A currently-live game is always kept regardless of its
    # date so the yesterday-still-live carryover a few lines up in
    # _get_nhl_scoreboard doesn't get filtered right back out.
    if date == "" and isinstance(games, list):
        from datetime import timedelta as _td
        _today = et_today_str()
        _tomorrow = (et_now() + _td(days=1)).strftime("%Y-%m-%d")
        games = [
            _g for _g in games
            if not isinstance(_g, dict)
            or (_g.get("status") or {}).get("state") == "in"
            or _et_game_date(_g.get("date")) in ("", _today, _tomorrow)
        ]
    return games


@router.get("/api/nhl/standings")
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


@router.get("/api/nhl/predict")
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

    # NHL Monte Carlo shadow prediction (gated on ENABLE_NHL_MC). Not run under the ratings
    # model (2026-10-26): it prices every market from its own exact score grid.
    from engine.config import get_flag as _get_flag
    _nhl_ratings_on = result.get("model") == "ratings"
    if _get_flag("ENABLE_NHL_MC", False, sport="nhl") and not _nhl_ratings_on:
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
    if _get_flag("ENABLE_NHL_GBM", False, sport="nhl") and not _nhl_ratings_on:
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
                result["gbm"] = {"error": f"team_id lookup failed: "
                                          f"{home_abbr!r}/{away_abbr!r}"}
        except Exception as e:
            logger.warning("NHL GBM shadow failed for %s/%s: %s", home, away, e)
            result["gbm"] = {"error": str(e)}

    if not _nhl_ratings_on:
        try:
            from engine.ensemble import ensemble_nhl
            log_meta = {
                "date": et_today_str(),
                "game_id": f"{et_today_str()}_{home}_{away}",
            }
            result["ensemble"] = ensemble_nhl(result, log_meta=log_meta)
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
                odds_map = _fetch_nhl_odds()
                game_odds = _match_picks(home_abbr, away_abbr, odds_map)
            picks, _ctx = generate_nhl_picks_with_context(
                home_key, away_key, game_odds, pred=result,
            )
        result["picks"] = picks
        # Same Pick Authority selection as the best-bets card (2026-10-26): proven market only.
        try:
            from engine.selection import select_pick as _authority_select
            result["best_pick"] = _authority_select("nhl", "nhl", [p for p in (picks or []) if not _nhl_is_p1(p)])
        except Exception:
            result["best_pick"] = None
        result["odds"] = game_odds
    except Exception as e:
        logger.warning("NHL picks generation in predict failed for %s/%s: %s",
                       home, away, e)
        result["picks"] = []
        result["best_pick"] = None
        result["odds"] = {}

    return result


_NHL_BB_CACHE: dict = {}
_NHL_BB_TTL_S = 60.0

# Persisted best-bets cache: serve the last-known slate instantly on a cold
# process (SWR then refreshes it), instead of a synchronous full recompute on
# the first request after a restart. MintyBot 2026-10-15.
from ._bestbets import persist_bb as _persist_bb, load_bb as _load_bb  # noqa: E402
try:
    import time as _t_rh
    _bb_rh, _ = _load_bb("nhl")
    if _bb_rh is not None:
        _NHL_BB_CACHE["nhl"] = (_t_rh.monotonic() - _NHL_BB_TTL_S - 1.0, _bb_rh)
except Exception:
    pass
_NHL_BB_REFRESHING: set = set()


@router.get("/api/nhl/best-bets")
def api_nhl_best_bets():
    """Served from a short-TTL cache + background refresh (stale-while-
    revalidate) so the heavy per-game predict never blocks a dashboard load
    after the first (Austin 2026-09-29 perf). Paper recording still runs
    inside the compute (cold inline, then in the bg refresh)."""
    import threading
    import time as _t
    ck = "nhl"
    cached = _NHL_BB_CACHE.get(ck)
    if cached is not None:
        built, resp = cached
        if (_t.monotonic() - built) >= _NHL_BB_TTL_S and ck not in _NHL_BB_REFRESHING:
            _NHL_BB_REFRESHING.add(ck)

            def _bg():
                try:
                    r = _compute_nhl_best_bets()
                    _NHL_BB_CACHE[ck] = (_t.monotonic(), r)
                    _persist_bb("nhl", r)
                except Exception as e:
                    logger.warning("[nhl] bg best-bets refresh failed: %s", e)
                finally:
                    _NHL_BB_REFRESHING.discard(ck)

            threading.Thread(target=_bg, daemon=True).start()
        return resp
    resp = _compute_nhl_best_bets()
    _NHL_BB_CACHE[ck] = (_t.monotonic(), resp)
    _persist_bb("nhl", resp)
    return resp


def _et_game_date(iso_time: str | None) -> str:
    """ET calendar date (YYYY-MM-DD) for a game's ISO start time.

    A bet's ``time`` is the ESPN event date -- a UTC ISO string (e.g.
    '2026-09-30T02:00Z' for a 10pm ET game), so a naive ``[:10]`` slice
    lands on the WRONG (next) day for evening games. Convert to US/Eastern
    first. Mirrors engine.derivative_tracker._bet_date. A plain
    'YYYY-MM-DD' (HR-fallback rows) is already ET -> returned as-is; an
    empty/unparseable value returns "" (callers treat "" as "cannot tell"
    -> today, never as a future game, so a dateless row is never dropped)."""
    if not iso_time or not isinstance(iso_time, str) or len(iso_time) < 10:
        return ""
    if "T" not in iso_time:
        return iso_time[:10]
    try:
        try:
            from zoneinfo import ZoneInfo
            _et = ZoneInfo("America/New_York")
        except Exception:
            _et = None
        dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
        if _et is not None and dt.tzinfo is not None:
            dt = dt.astimezone(_et)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return iso_time[:10]


def _nhl_final_display_row(game: dict) -> dict:
    """A DISPLAY-ONLY best-bets row for a TODAY game that has already gone
    live/final (MintyBot 2026-10-12, Bug 1). Carries the game + its status +
    the recorded paper pick so the slate shows the full night incl. finished
    games with their result — but deliberately NO paper_picks/paper_picks_p1,
    so the client-side POTD hero recompute can never surface a started/finished
    game (the Bug 2 'reverts to FLA -1.5 (FINAL)' guard). It is never predicted
    or recorded (it's not in `today_bets`)."""
    h = game.get("home", {}) or {}
    a = game.get("away", {}) or {}
    recorded = None
    try:
        from engine.nhl_tracker._summary import recorded_nhl_paper_pick as _rpp
        recorded = _rpp(game.get("id"), date=_et_game_date(game.get("date")) or None)
    except Exception:
        recorded = None
    return {
        "game_id": game.get("id"),
        "matchup": f"{a.get('abbreviation', '')} @ {h.get('abbreviation', '')}",
        "home": h,
        "away": a,
        "time": game.get("date"),
        "venue": game.get("venue", ""),
        "status": game.get("status", {}),
        "odds": game.get("odds"),
        "best_pick": recorded,
        "best_pick_full": recorded,
        "best_pick_p1": None,
        "best_derivative": None,
        "all_picks": [],
        "derivative_picks": [],
        "paper_picks": [],
        "paper_picks_p1": [],
        "final": True,
    }


_NHL_EV_CACHE = {"ts": 0.0, "map": {}}

def _nhl_extra_evidence_map(conn):
    """{bet_type: (settled_n, days_tracked)} for the EXTRA (period/derivative) markets,
    across nhl_derivative_picks + nhl_p1_paper_picks. Cached 300s so the gate costs one
    GROUP BY per window, not a COUNT per candidate per game (the tracker is 300 games)."""
    import time as _t
    if _NHL_EV_CACHE["map"] and (_t.monotonic() - _NHL_EV_CACHE["ts"]) < 300:
        return _NHL_EV_CACHE["map"]
    m = {}
    try:
        for row in conn.execute(
                "SELECT bet_type, COUNT(*) n, MIN(date) mn, MAX(date) mx FROM ("
                "  SELECT bet_type,date,result FROM nhl_derivative_picks"
                "  UNION ALL SELECT bet_type,date,result FROM nhl_p1_paper_picks"
                ") WHERE result IS NOT NULL AND result != 'V' GROUP BY bet_type"):
            n = (row["n"] or 0)
            days = 0
            if row["mn"] and row["mx"]:
                try:
                    days = int((conn.execute("SELECT julianday(?)-julianday(?)",
                                (row["mx"], row["mn"])).fetchone()[0]) or 0)
                except Exception:
                    days = 0
            m[row["bet_type"]] = (n, days)
    except Exception:
        return {}
    _NHL_EV_CACHE["ts"] = _t.monotonic(); _NHL_EV_CACHE["map"] = m
    return m


def _nhl_market_scope(bet_type, pick):
    """Market bucket for the tracker chip, keyed on the BET not the source table.
    nhl_picks holds the card's headline best in ANY market, so P1/period picks can
    live there -- classify by bet_type/pick text, not by which table it came from
    (fixes P1 picks in nhl_picks showing as 'Full Game'; MintyBot 2026-10-15)."""
    bt = (bet_type or "").strip().upper()
    pk = (pick or "").strip().upper()
    if bt in ("PL", "ML", "O/U"):
        return "Full Game"
    if pk.startswith("P1") or bt.startswith("P1"):
        return "P1"
    return "Period / Team Total"


_LAST_NHL_AUTO_SETTLE = [0.0]
def _maybe_auto_settle_nhl():
    """Throttled, non-blocking settle so NHL picks grade shortly after games go
    final. There is no standalone settle cron -- grading only ran on the ~3AM job,
    so tonight's finals piled up pending (MintyBot 2026-10-15). Fires on a tracker
    view, at most once per 10 min, in a daemon thread."""
    import time as _t, threading as _th
    now = _t.monotonic()
    if now - _LAST_NHL_AUTO_SETTLE[0] < 600.0:
        return
    _LAST_NHL_AUTO_SETTLE[0] = now
    def _run():
        try:
            from engine.nhl_tracker import settle_picks as _sp
            _sp()
        except Exception as _e:
            logger.warning("auto-settle nhl (full game) failed: %s", _e)
        try:
            from engine.derivative_tracker import settle_derivative_picks as _sd, NHL_P1_PAPER_TABLE as _p1t
            _sd("nhl", table=_p1t)
        except Exception as _e:
            logger.warning("auto-settle nhl (p1) failed: %s", _e)
        try:
            from engine.pick_of_day import settle_potd as _spotd
            _spotd("nhl")
        except Exception as _e:
            logger.warning("auto-settle nhl (potd) failed: %s", _e)
    _th.Thread(target=_run, daemon=True).start()


def _nhl_merged_recorded_best(game_id, date=None, conn=None, ev_map=None):
    """The game's single BEST recorded pick = the merged CALIBRATED best across all game
    markets (core full-game + period/team-total derivatives + P1), GATED: core full-game
    always eligible, an EXTRA market may only win once it has EARNED a track record
    (>=60 settled / >=30 days, engine.calibration_gate.headline_eligible) AND clears the
    +EV floor -- matching the NHL paper-POTD gate (Austin 2026-10-12/15). Player
    props/goals excluded (own feed). Pass `conn`/`ev_map` to share them across many games
    (the tracker). Chip-shaped + profit/stake_units + _scope for the tracker."""
    import sqlite3 as _sq
    from engine.nhl_tracker import _get_nhl_db
    from engine.calibration_gate import cal_edge as _ce, headline_eligible as _he
    if conn is None:
        try:
            conn = _get_nhl_db(); conn.row_factory = _sq.Row
        except Exception:
            return None
    if ev_map is None:
        ev_map = _nhl_extra_evidence_map(conn)
    dq = " AND date = ?" if date else ""
    args = (str(game_id), date) if date else (str(game_id),)
    cands = []
    try:
        for r in conn.execute(
                "SELECT bet_type,pick,odds,edge,model_prob,result,profit,stake_units,closing_odds "
                "FROM nhl_picks WHERE game_id=?" + dq + " AND COALESCE(is_shadow,0)=1 "
                "AND (result IS NULL OR result != 'V') AND ABS(edge)<=20.0", args):
            d = dict(r); d["_scope"] = _nhl_market_scope(d.get("bet_type"), d.get("pick")); d["_extra"] = False; cands.append(d)
        for tbl, lbl in (("nhl_derivative_picks", "Period / Team Total"),
                         ("nhl_p1_paper_picks", "P1")):
            for r in conn.execute(
                    "SELECT bet_type,pick,odds,edge,model_prob,result,profit,stake_units,closing_odds "
                    "FROM " + tbl + " WHERE game_id=?" + dq + " "
                    "AND (result IS NULL OR result != 'V')", args):
                d = dict(r); d["_scope"] = _nhl_market_scope(d.get("bet_type"), d.get("pick")); d["_extra"] = True; cands.append(d)
    except Exception:
        return None
    if not cands:
        return None
    scored = []
    for p in cands:
        ce = _ce("nhl", p.get("bet_type"), p.get("model_prob"), p.get("odds"),
                 pick_text=p.get("pick"), raw_edge=p.get("edge"))
        n = days = None
        if p["_extra"]:
            n, days = ev_map.get(p["bet_type"], (0, 0))
        eligible = _he(ce, p["_extra"], n, days)
        scored.append((eligible, ce if ce is not None else -999.0, p))
    pool = [sc for sc in scored if sc[0]]
    if not pool:
        return None
    b = max(pool, key=lambda sc: sc[1])[2]
    st = b.get("stake_units"); st = 1.0 if st is None else st
    return {"type": b["bet_type"], "market": b["bet_type"], "pick": b["pick"],
            "odds": b["odds"], "edge": b["edge"], "prob": b["model_prob"],
            "result": b["result"], "paper": True, "_scope": b["_scope"], "closing_odds": b.get("closing_odds"),
            "profit": round(b["profit"] * st, 2) if b.get("profit") is not None else None,
            "stake_units": st}

@_bb_reset_on_exit("nhl")
def _compute_nhl_best_bets():
    """Run predictions on all today's NHL games and find edges.

    Parallel fan-out so cold load isn't N x serial. Each game's
    factor+MC+GBM chain runs in a worker, then picks are assembled
    serially on the main thread (generate_nhl_picks_with_context reads
    the augmented pred and blends via ensemble_nhl).
    """
    import time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from engine.nhl_picks import generate_nhl_picks_with_context

    # TONIGHT'S SLATE ONLY (MintyBot 2026-10-12, Bug 1 fix): best-bets must be
    # TODAY's ET games, not a multi-day board. A 2026-09-29 change had flipped this
    # to the 7-day lookahead (lookahead=True) "so every future board game gets a
    # lean" — but that leaked tomorrow's games (e.g. MIN@WPG on 10/13) into tonight's
    # slate and made the POTD candidate pool span days. Back to lookahead=False (the
    # documented contract in routes_nhl_extra._get_nhl_scoreboard): fetch TODAY only
    # (+ ESPN's yesterday-still-live carryover). The returned `bets` is then pinned
    # to the ET-today slate below, and today's already-final/live games are re-added
    # as DISPLAY rows so the slate shows the whole night (incl. an afternoon FINAL),
    # not just the pre-game remainder. Recording was already TODAY-only (`today_bets`),
    # so it is unchanged. Trade-off: tomorrow's board cards no longer carry a
    # speculative best-bets lean badge (they still render from the scoreboard board).
    games = _get_nhl_scoreboard(lookahead=False)
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

    _today_ds = et_today_str()
    predictable = []
    final_display = []  # today's already live/final games -> display-only rows (Bug 1)
    for game in games:
        state = game["status"].get("state", "pre")
        if state in ("post", "in") or game["status"].get("completed"):
            # Keep TODAY's live/final games on the slate as display-only rows so
            # best-bets shows the whole night (with results), but never predict or
            # record them. Non-today (e.g. a yesterday-live carryover) is dropped.
            if _et_game_date(game.get("date")) in ("", _today_ds):
                final_display.append(_nhl_final_display_row(game))
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
            g_date = (row.get("game") or {}).get("date")
            uc = not _is_game_imminent(g_date)
            # g_date threads THIS game's date into the b2b/rest check so a
            # 7-day-lookahead game is judged against its own night, not
            # wall-clock today (2026-09-30 fix -- see _check_back_to_back).
            return (row, _predict_nhl_full(row["h_key"], row["a_key"],
                                            use_cache=uc, game_date=g_date))
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
    shadow_by_game: dict = {}   # game_id -> Pick Authority shadow-lane candidates (W=1)
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
        # Paper (shadow) leans come from the model prediction + odds,
        # independent of the real edge floor (Austin 2026-09-29 "it's paper").
        # Compute them BEFORE the skip so a game with a paper lean but no
        # floor-clearing REAL pick still renders a card — opening night, the
        # real gates suppress everything, but the model still has a lean.
        # Pick Authority (MintyBot 2026-10-26): the card/P1 leans come from select_pick over
        # the PA-anchored candidates below -- the old stacked-calibration paper helpers
        # (_nhl_paper_picks / _nhl_p1_paper_picks) are retired. Every predicted game renders;
        # NO-PLAY = no pick (an unproven market defers to the line, ~0 edge).
        if pred.get("model") != "ratings" and not picks:
            continue
        _gid = game.get("id")
        if _gid is not None and (time.time() - _NHL_SHADOW_LAST.get(str(_gid), 0.0)
                                 >= _NHL_SHADOW_EVERY_S):
            # Shadow lane: every candidate priced on the ratings model's own number (W=1) and
            # mirrored to picks_unified is_shadow=1 below, so unproven markets build a record.
            try:
                from engine.sports.nhl._market_anchor import shadow_mode as _nhl_shadow
                with _nhl_shadow():
                    _sh, _ = generate_nhl_picks_with_context(
                        row["h_key"], row["a_key"], odds, pred=pred)
                shadow_by_game[str(_gid)] = _sh or []
                _NHL_SHADOW_CANDS[str(_gid)] = _sh or []
                _NHL_SHADOW_LAST[str(_gid)] = time.time()
            except Exception as e:
                logger.debug("NHL shadow pass failed for %s @ %s: %s", a_abbr, h_abbr, e)

        # generate_nhl_picks_with_context auto-caches via its internal
        # generate_picks call (if applicable). Also cache here for
        # NHL-specific pick format compatibility.
        pred["_cached_picks"] = picks
        pred["_cached_odds"] = odds
        _picks_store_put("nhl", h_abbr, a_abbr, picks, odds)

        core_picks = [p for p in picks if p.get("type") not in _NHL_DERIV_TYPES]
        nhl_matchup = f"{a_abbr} @ {h_abbr}"
        nhl_target = et_today_str()
        recorded = _get_recorded_pick("nhl", nhl_matchup, nhl_target)
        locked = _is_game_locked(game.get("date"))
        # Hold the card PAPER headline to the recorded (held) first lean —
        # mirror the scoreboard (~L383): best-bets shipped the LIVE paper lean,
        # so once the line moved a game card flipped to a higher-edge market
        # (Under 5.5) while the POTD + recorded held the first lean (DAL +1.5).
        # The line moving our way is a CLV win, not a re-pick. (Austin 2026-10-14
        # card/POTD inconsistency.)
        # Pick Authority (Workstream #1): the held lean, run through the shared
        # selector (calibrated edge + confidence) IS this game's selected pick.
        try:
            from engine.selection import apply_selected as _apply_selected
            _merged_rb = _nhl_merged_recorded_best(
                game.get("id"), date=_et_game_date(game.get("date")) or nhl_target)
            _rec_chip = _apply_selected("nhl", _merged_rb)
            # Recorded picks already carry their calibrated/anchored edge (e.g. the
            # market-blended P1 at +6.6). apply_selected re-derives via cal_edge, which
            # DOUBLE-calibrates the already-calibrated prob (empirical cal for NHL Period
            # Total INFLATES: 0.63->0.76), turning +6.6 into +19.8. Keep the recorded
            # edge + re-tag confidence. (MintyBot 2026-10-18, Austin "anchor it is".)
            if _rec_chip and _merged_rb and _merged_rb.get("edge") is not None:
                _rec_chip["edge"] = _merged_rb["edge"]
                try:
                    from engine.config import EDGE_STRONG as _ES, EDGE_MODERATE as _EM
                    _re2 = float(_rec_chip["edge"] or 0)
                    _rec_chip["confidence"] = ("strong" if _re2 >= _ES else "moderate" if _re2 >= _EM else "lean")
                except Exception:
                    pass
        except Exception:
            _rec_chip = None
        # Pick Authority over ALL of each view's candidates (MintyBot 2026-10-26): it used to
        # gate only the single top lean, so an unproven top pick hid a proven one beneath it.
        # A held (recorded) lean stays its view's pick -- the line moving our way is a CLV
        # win, not a re-pick (Austin 2026-10-14) -- while its market is still proven.
        _full_cands = [p for p in picks if not _nhl_is_p1(p)]
        _p1_cands = [p for p in picks if _nhl_is_p1(p)]
        _held_p1 = bool(_rec_chip) and _nhl_is_p1(_rec_chip)
        try:
            from engine.selection import select_pick as _authority_select
            best = _authority_select("nhl", "nhl", [_rec_chip] if (_rec_chip and not _held_p1)
                                     else _full_cands)
            best_pick_p1 = _authority_select("nhl", "nhl", [_rec_chip] if _held_p1 else _p1_cands)
        except Exception:
            best = best_pick_p1 = None
        paper_leans = [best] if best else []
        paper_picks_p1 = [best_pick_p1] if best_pick_p1 else []
        try:
            from engine.selection import card_lean as _card_lean
            _nhl_card_lean = _card_lean("nhl", "nhl", [p for p in _NHL_SHADOW_CANDS.get(str(game.get("id")), [])
                                                      if not _nhl_is_p1(p)] or _full_cands, best)  # card's best lean (Austin 2026-10-26): graduated pick, else the ungraduated lean -- display only
        except Exception:
            _nhl_card_lean = None
        try:   # held per game (Austin 2026-10-27): the first lean is kept, and carried through live + final
            from engine.selection import lean_lock as _lean_lock
            _nhl_card_lean = _lean_lock.hold("nhl", str(game.get("id")), _nhl_card_lean)
        except Exception:
            pass

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
        # Secondary derivative badge for the card. GameCard renders
        # `bet.best_derivative` as a subordinate chip beneath the main
        # bet when present; without it the deriv pick stays invisible
        # on the card even though it's in `derivative_picks`. Same
        # pattern as MLB's `best_derivative` field.
        # Extra NHL markets (reg/period 3-way, period spread, goal-1, first-goal)
        # are UNPROVEN -> shadow-only; keep them out of the secondary chip so a
        # raw/overstated lean never surfaces (they still shadow-track for graduation).
        # Pick Authority: the chip shows only a PROVEN derivative market (none until a cell
        # graduates on the ratings model's record).
        try:
            from engine.selection import select_pick as _authority_select
            best_derivative = _authority_select(
                "nhl", "nhl", [p for p in derivative_picks if not _nhl_is_p1(p)])
        except Exception:
            best_derivative = None

        # Display-only win-prob calibration (MintyBot 2026-10-22): the card's H2H
        # win-% BARS are the RAW model prob (~8pp hot in the 60-70 band). Map them
        # through the model's own ML track record (isotonic over calibration_samples,
        # mirrored so an away favorite calibrates like a home favorite, NO-FLIP). On a
        # COPY (cal_winprob returns a NEW dict) so the pick objects, the ctx/pred
        # win_prob the paper leans read, and the raw signal log stay untouched.
        # Passthrough until the hockey/nhl map is fitted.
        # Not under the ratings model (2026-10-26): that map was fit on the old model's record
        # and squeezes every game into .42-.58; the ratings probabilities are calibrated as-is
        # (holdout moneyline log-loss = the closing line's).
        try:
            if ctx.get("model") == "ratings":
                _disp_wp = ctx.get("win_prob", {})
            else:
                from ._display_cal import cal_winprob as _cal_winprob
                _disp_wp = _cal_winprob("hockey/nhl", ctx.get("win_prob", {}))
        except Exception:
            _disp_wp = ctx.get("win_prob", {})
        # Card % = what the Pick Authority bets with (model blended with the de-vigged ML, 2026-10-26).
        try:
            from ._display_cal import blend_card_wp as _blend_card_wp
            _disp_wp = _blend_card_wp("nhl", "nhl", _disp_wp, (odds or {}).get("home_ml"), (odds or {}).get("away_ml"))
        except Exception:
            pass
        bets.append({
            "game_id": game["id"],
            "matchup": f"{a_abbr} @ {h_abbr}",
            "home": game["home"],
            "away": game["away"],
            "time": game["date"],
            "venue": game.get("venue", ""),
            "goalies": goalie_info,
            "odds": odds,  # both-sides board (OddsGrid reads this; 2026-10-11)
            "best_pick": best,
            "best_pick_full": best,
            "best_pick_p1": best_pick_p1,
            "best_derivative": best_derivative,
            "all_picks": all_picks,
            "derivative_picks": derivative_picks,
            "confidence": (best or {}).get("confidence", "lean"),
            "rest": ctx.get("rest", {}),
            "injuries": ctx.get("injuries", {}),
            "win_prob": _disp_wp,
            "expected_score": ctx.get("expected_score", {}),
            "factors": ctx.get("factors", {}),
            "season_context": ctx.get("season_context", {}),
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "recorded_pick": recorded,
            "is_locked": locked,
            # Calibrated PAPER (shadow) leans — best per full-game family,
            # bypassing the bet floor. Recorded is_shadow=1 in the piggyback
            # below (Austin 2026-09-22).
            "paper_picks": paper_leans,
            "paper_picks_p1": paper_picks_p1,
            "card_lean": _nhl_card_lean,
        })

    # Sort by main pick edge desc; derivative-only nights (best_pick=None)
    # sink to the bottom, ordered between themselves by their P1 edge so
    # the strongest derivative play still bubbles up first.
    def _sort_key(b: dict) -> tuple[int, float]:
        main = b.get("best_pick")
        if main:
            return (1, float(main.get("edge") or 0))
        deriv = b.get("best_pick_p1") or b.get("best_derivative") or {}
        return (0, float(deriv.get("edge") or 0))
    bets.sort(key=_sort_key, reverse=True)

    # DISPLAY vs RECORD split (2026-09-29 every-game leans): the board shows
    # every scheduled game (7-day lookahead), but the tracker / paper / POTD /
    # pick-event writers below stay TODAY-ONLY so future-day games are never
    # recorded misdated under today's date. `today_bets` = games whose ET
    # start-date is today (undated HR-fallback rows -> "" -> treated as today,
    # never dropped). Every recorder is thus fed exactly the today-only slate
    # it saw before the lookahead change; only the returned `bets` is the full
    # board.
    _today = et_today_str()
    today_bets = [b for b in bets if _et_game_date(b.get("time")) in ("", _today)]

    try:
        from engine.nhl_tracker import refresh_pending_for_today as _nhl_refresh
        nhl_target = et_today_str()
        diff = _nhl_refresh(today_bets, nhl_target)
        if any(diff.values()):
            logger.info("Tracker pending sync (nhl): %s", diff)
    except Exception as e:
        logger.warning("NHL tracker pending refresh failed: %s", e)

    # ACTIVE/STAKED recording DISABLED (Austin 2026-10-03): the NHL tracker is
    # STRICTLY PAPER now. record_picks() wrote is_shadow=0 staked rows (the old
    # "Active Picks" tracker); it is intentionally no longer called. Only the
    # is_shadow=1 calibrated PAPER lean is recorded (below), deduped to the
    # single best lean per game, and the leftover is_shadow=0 rows are voided in
    # that same pass so the tracker mirrors the card — one paper pick per game.

    # Piggyback CALIBRATED PAPER (shadow) recording — the model's best
    # calibrated lean per full-game family, logged is_shadow=1 as a forward
    # test. Kickoff-locked + per-family-locked inside record_paper; rides
    # the same settle + closing-odds capture as the real rows. Austin
    # 2026-09-22.
    try:
        from engine.nhl_tracker import record_paper as _nhl_record_paper
        _paper_n = 0
        for b in today_bets:
            if b.get("is_locked"):
                continue
            # Pick Authority graduation lane (Austin 2026-10-18 "capture all markets"):
            # mirror the FULL generated slate (core + period/team-total derivatives)
            # to picks_unified as is_shadow=1 so un-headlined NHL markets accumulate a
            # settled record and can graduate. Once-per-game (deduped).
            try:
                from engine.picks_unified import record_shadow_slate as _rss_nhl
                _rss_nhl("nhl", "nhl", b.get("game_id"), b.get("matchup") or "",
                         shadow_by_game.get(str(b.get("game_id"))) or [])
            except Exception:
                pass
            pp = b.get("paper_picks") or []
            if not pp:
                continue
            # STRICTLY PAPER, ONE PER GAME (Austin 2026-10-03): record the single
            # best-edge PAPER lean — what the card shows. record_paper holds a
            # PER-GAME lock (one is_shadow=1 row per game, locked at first record),
            # so repeated/concurrent hits converge to one pick per game with NO
            # per-hit voiding — an earlier per-hit dedup emptied games when a pick
            # briefly dropped out of the board, and raced across concurrent hits.
            # The active layer is gone at the SOURCE: record_picks is disabled above
            # and the goalie refresh below runs record_picks=False, so nothing
            # writes is_shadow=0 anymore; the few leftover active rows settle out.
            _best_pp = max(pp, key=lambda x: (x.get("edge") if x.get("edge") is not None else -999.0))
            if (_best_pp.get("edge") or 0.0) <= 0:
                continue
            _r = _nhl_record_paper(b.get("matchup") or "", b.get("game_id"),
                                   [_best_pp], date=nhl_target)
            _paper_n += _r.get("recorded", 0)
        if _paper_n:
            logger.info("NHL paper (api hit): recorded=%d shadow for %s",
                        _paper_n, nhl_target)
    except Exception as e:
        logger.warning("NHL paper record (api hit) failed: %s", e)

    # Piggyback closing-odds capture on every games-endpoint hit. The
    # nightly sync runs at 8am/5pm but HR often hasn't published the
    # current night's lines at sync time, leaving every settled pick
    # without closing_odds (NHL CLV coverage was 1.1% pre-fix). Fetch
    # is HR-cached so this is effectively free when the cache is warm.
    # Most-recent-pre-game-stamp wins inside capture_closing_odds.
    try:
        from engine.nhl_tracker import capture_closing_odds as _nhl_cap
        n = _nhl_cap()
        if n:
            logger.info("NHL closing-odds capture (api hit): %d", n)
    except Exception as e:
        logger.warning("NHL closing-odds capture (api hit) failed: %s", e)

    # Piggyback NHL goalie refresh — same throttle pattern as NBA
    # injuries (2 min). Goalie scratches are the biggest in-day NHL
    # WP swing factor (5-8% per game). Refresh diffs announced
    # starters vs the morning snapshot; on delta, invalidates picks +
    # POTD and clears pred cache so the next /predict sees the new
    # state.
    _now_ts = _time.time()
    last_g = globals().get("_NHL_GOALIE_REFRESH_LAST", 0.0)
    if _now_ts - last_g >= 120:
        globals()["_NHL_GOALIE_REFRESH_LAST"] = _now_ts
        try:
            from engine.nhl_goalie_refresh import refresh_for_date
            # record_picks=False (Austin 2026-10-03): strictly-paper tracker — the
            # goalie refresh must NOT re-record is_shadow=0 active rows (it was the
            # remaining active-layer leak after record_picks() was disabled). It
            # still diffs goalies + invalidates picks/POTD + clears the pred cache;
            # the calibrated PAPER lean is re-recorded by record_paper above.
            g_result = refresh_for_date(date=nhl_target, record_picks=False)
            if (g_result.get("invalidated") or 0) > 0:
                cleared = _pred_cache_clear_sport("nhl")
                logger.info("NHL goalie refresh (api hit): "
                            "deltas=%d invalidated=%d cleared=%d",
                            g_result.get("deltas", 0),
                            g_result.get("invalidated", 0),
                            cleared)
        except Exception as e:
            logger.warning("NHL goalie refresh (api hit) failed: %s", e)

    try:
        from engine.derivative_tracker import record_top_derivatives
        diff = record_top_derivatives("nhl", today_bets, n_per_game=1,
                                       min_edge=4.0,
                                       target_date=et_today_str())
        if diff.get("inserted") or diff.get("updated"):
            logger.info("Derivative tracker (nhl): %s", diff)
    except Exception as e:
        logger.warning("NHL derivative recorder failed: %s", e)

    # P1 paper tracker (Austin 2026-10-01 P1 tab/tracker consistency pass):
    # records each game's single P1 lean -- best_pick_p1, falling back to
    # paper_picks_p1 -- into the isolated nhl_p1_paper_picks table, one
    # row per game at any positive edge. Same two-tier fallback the P1
    # card itself falls back to (see BetsView.jsx's `chosen`/`paper_pick`
    # and _p1_lean_for_bet's docstring) -- as of 2026-10-01, best_pick_p1
    # is empty slate-wide (HR hasn't posted period-market odds yet this
    # early in the season), so the cards -- and now the tracker -- are
    # currently running on the paper_picks_p1 tier. Deliberately separate
    # from the record_top_derivatives call above: that recorder keeps
    # only the single top-edge derivative per game across every period +
    # Team Total combined (so a real P1 edge can lose the slot to an
    # unrelated pick), floors at 4.0 (tuned for the Derivatives tab's
    # aggregate, not the P1 tab), and never reads paper_picks_p1 at all.
    # See record_p1_paper_picks's docstring.
    try:
        from engine.derivative_tracker import record_p1_paper_picks
        p1_diff = record_p1_paper_picks(today_bets, target_date=et_today_str())
        if p1_diff.get("inserted") or p1_diff.get("updated"):
            logger.info("P1 paper tracker (nhl): %s", p1_diff)
    except Exception as e:
        logger.warning("NHL P1 paper recorder failed: %s", e)

    # Lock today's cross-market PAPER POTD AFTER both the full-game (above) and
    # P1 (just now) shadow pools are recorded, so the day's first lock already
    # sees both markets and picks the best cross-market lean — not only whichever
    # full-game pick recorded first (Bug 2, MintyBot 2026-10-12). Idempotent +
    # started/finished-aware inside record_paper_potd.
    try:
        from engine.nhl_tracker import record_paper_potd as _nhl_paper_potd
        # final_display = today's already live/final games (ESPN state in/post),
        # so a POTD can never lock onto a started game even before it settles.
        _started_ids = {r.get("game_id") for r in final_display if r.get("game_id")}
        _nhl_paper_potd(et_today_str(), started_ids=_started_ids)
    except Exception as e:
        logger.warning("NHL paper POTD lock (api hit) failed: %s", e)

    # NHL player-props generation moved OFF this piggyback path 2026-10-01
    # (Austin: decouple from best-bets, it was GIL-contending with the
    # ~109s cold best-bets compute). engine.sports.nhl.prop_picks.
    # generate_picks() now runs on its own Windows scheduled task
    # (NhlPropsGen, every 3h ~9am-9pm ET via scripts\nhl_props_gen.py) with
    # a season-aware adaptive lookback instead of the old flat 365. This
    # endpoint stays a pure read of whatever that job already recorded --
    # see routes_props.py's api_nfl_player_props_today, nhl branch.

    # Piggyback P1 POTD lock (Austin 2026-09-22 perf fix): select + lock the
    # first-period POTD from the bets we JUST generated — reuses the same
    # derivative_picks, no fresh generation. This makes /api/pick-of-day/nhl
    # ?view=p1 a pure fast read (locked row when a pick exists; memoized
    # "no pick today" otherwise) instead of re-running best-bets (~20s) on
    # every P1-tab poll.
    try:
        from engine.derivative_tracker import get_or_create_p1_potd
        get_or_create_p1_potd("nhl", today_bets)
    except Exception as e:
        logger.warning("NHL P1 POTD lock (api hit) failed: %s", e)

    try:
        from engine.pick_events import detect_transitions
        ev = detect_transitions("nhl", [
            {"game_id": b["game_id"], "matchup": b["matchup"],
             "best_pick": b.get("best_pick")}
            for b in today_bets
        ], date=et_today_str())
        if any(ev.values()):
            logger.info("Pick events (nhl): %s", ev)
    except Exception as e:
        logger.warning("NHL pick-events failed: %s", e)

    # Pin the returned board to TONIGHT's ET slate (Bug 1): drop any stray
    # non-today row, then re-add today's already-final/live games as display rows
    # so the slate == tonight's full set (incl. finished games with their result).
    # Recording above already ran on `today_bets` (pre-game only); this only shapes
    # the RETURNED value.
    _today_ret = et_today_str()
    bets = [b for b in bets if _et_game_date(b.get("time")) in ("", _today_ret)]
    bets.extend(final_display)

    _bb_progress_set("nhl", phase="idle", finished_at=_time.time())
    return bets


@router.get("/api/nhl/tracker/history")
def api_nhl_pick_history():
    """Return recent NHL pick history.

    Filters out derivative bet types — see api_pick_history docstring
    for the trampoline-race rationale. Same pattern applies here.
    """
    from engine.nhl_tracker import _get_nhl_db
    conn = _get_nhl_db()
    placeholders = ",".join("?" * len(_NHL_DERIV_TYPES))
    # REAL rows only (is_shadow=0) for the Active/History tables — paper
    # rides in the separate `paper` object below (Austin 2026-09-22).
    picks = conn.execute(
        f"SELECT * FROM nhl_picks WHERE bet_type NOT IN ({placeholders}) "
        f"AND COALESCE(is_shadow,0)=0 "
        f"ORDER BY created_at DESC LIMIT 50",
        tuple(_NHL_DERIV_TYPES),
    ).fetchall()
    rows = []
    for p in picks:
        d = dict(p)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        rows.append(d)
    # Calibrated PAPER (shadow) block — mirrors routes_football's
    # /tracker/history `paper` object so PickHistory's Paper section renders.
    # summary + rows are isolated from real via is_shadow; potd_record is the
    # full-game core POTD's running record. Settlement + CLV happen on the
    # shared settle/cron (no is_shadow filter), so this is a pure read.
    paper = {"summary": {}, "rows": [], "potd_record": {}}
    try:
        from engine.nhl_tracker import paper_summary, list_paper_history
        paper["summary"] = paper_summary()
        paper["rows"] = list_paper_history(limit=50)
    except Exception as e:
        logger.warning("NHL paper summary/history failed: %s", e)
    try:
        from engine.pick_of_day import get_potd_summary
        paper["potd_record"] = get_potd_summary("nhl")
    except Exception as e:
        logger.debug("NHL POTD record fetch failed: %s", e)
    return {"rows": rows, "paper": paper}


@router.get("/api/nhl/tracker/unified")
def api_nhl_tracker_unified(limit: int = 300):
    """ONE NHL tracker = the card's 'Best' per game (the merged CALIBRATED best across
    all game markets: full game + period/team-total + P1). Mirrors the card exactly
    (Austin 2026-10-15, the tracker merge). Player props/goals are excluded -- their own
    tab. One row per game (the merged best), via _nhl_merged_recorded_best (the same
    selector the card's held lean uses). Read-only; per-market endpoints untouched."""
    _maybe_auto_settle_nhl()
    import sqlite3 as _sq
    from engine.nhl_tracker import _get_nhl_db
    conn = _get_nhl_db()
    conn.row_factory = _sq.Row
    try:
        gids = [r[0] for r in conn.execute(
            "SELECT game_id, MAX(c) mc FROM ("
            "  SELECT game_id, MAX(created_at) c FROM nhl_picks WHERE COALESCE(is_shadow,0)=1 GROUP BY game_id"
            "  UNION ALL SELECT game_id, MAX(created_at) c FROM nhl_derivative_picks GROUP BY game_id"
            "  UNION ALL SELECT game_id, MAX(created_at) c FROM nhl_p1_paper_picks GROUP BY game_id"
            ") GROUP BY game_id ORDER BY mc DESC LIMIT ?", (limit,))]
    except Exception as e:
        logger.warning("unified tracker game list failed: %s", e)
        gids = []
    evm = _nhl_extra_evidence_map(conn)
    rows = []
    for gid in gids:
        best = _nhl_merged_recorded_best(gid, conn=conn, ev_map=evm)
        if not best:
            continue
        meta = (conn.execute("SELECT matchup,date FROM nhl_picks WHERE game_id=? LIMIT 1", (str(gid),)).fetchone()
                or conn.execute("SELECT matchup,date FROM nhl_derivative_picks WHERE game_id=? LIMIT 1", (str(gid),)).fetchone()
                or conn.execute("SELECT matchup,date FROM nhl_p1_paper_picks WHERE game_id=? LIMIT 1", (str(gid),)).fetchone())
        rows.append({
            "game_id": gid,
            "date": meta["date"] if meta else None,
            "matchup": meta["matchup"] if meta else None,
            "market": best.get("_scope"),
            "bet_type": best["type"], "pick": best["pick"], "odds": best["odds"],
            "edge": best["edge"], "result": best["result"], "profit": best.get("profit"),
            "model_prob": best.get("prob"), "closing_odds": best.get("closing_odds"), "stake_units": best.get("stake_units"),
        })

    def _sum(rs):
        w = sum(1 for r in rs if r["result"] == "W")
        l = sum(1 for r in rs if r["result"] == "L")
        pu = sum(1 for r in rs if r["result"] in ("P", "PUSH"))
        pend = sum(1 for r in rs if not r["result"])
        prof = round(sum((r["profit"] or 0) for r in rs if r["result"]), 2)
        dec = w + l
        return {"n": len(rs), "wins": w, "losses": l, "pushes": pu, "pending": pend,
                "profit": prof, "win_pct": round(100.0 * w / dec, 1) if dec else 0.0}

    by_market = {}
    for r in rows:
        by_market.setdefault(r["market"] or "?", []).append(r)
    return {
        "rows": rows,
        "summary": {"all": _sum(rows)},
        "by_market": {m: _sum(rs) for m, rs in by_market.items()},
    }


# ── NHL P1 paper tracker (Austin 2026-10-01 P1 tab/tracker consistency) ──
# Reads engine.derivative_tracker's isolated nhl_p1_paper_picks table
# (one row per game, sourced from best_pick_p1 falling back to
# paper_picks_p1 -- the same two-tier pick the P1 card itself falls back
# to). Deliberately separate from both /api/nhl/derivative-tracker/*
# (the generic, 4.0-floor-gated, shared-across-periods Derivatives tab)
# and /api/nhl/tracker/history's `paper` block (full-GAME is_shadow=1
# picks) -- see record_p1_paper_picks's docstring for why a third store
# was needed instead of reusing either.
@router.get("/api/nhl/p1-tracker/summary")
def api_nhl_p1_tracker_summary():
    from engine.derivative_tracker import (
        get_summary, settle_derivative_picks, NHL_P1_PAPER_TABLE,
    )
    # Auto-settle pending rows before reporting, mirroring the generic
    # derivative-tracker summary endpoint's auto-settle-on-fetch.
    try:
        settle_derivative_picks("nhl", table=NHL_P1_PAPER_TABLE)
    except Exception as e:
        logger.warning("P1 paper auto-settle failed: %s", e)
    return get_summary("nhl", table=NHL_P1_PAPER_TABLE)


@router.get("/api/nhl/p1-tracker/history")
def api_nhl_p1_tracker_history():
    from engine.derivative_tracker import get_history, NHL_P1_PAPER_TABLE
    return get_history("nhl", table=NHL_P1_PAPER_TABLE, limit=200)


@router.post("/api/nhl/p1-tracker/settle")
def api_nhl_p1_tracker_settle():
    from engine.derivative_tracker import settle_derivative_picks, NHL_P1_PAPER_TABLE
    return settle_derivative_picks("nhl", table=NHL_P1_PAPER_TABLE)


@router.get("/api/nhl/tracker/paper")
def api_nhl_paper_shadow():
    """NHL PAPER-pick shadow (full-game scope) in MLB's /api/tracker/paper
    shape: {full: {summary, rows, pending, potd, potd_record}}. Backed by the
    calibrated is_shadow=1 rows + the paper POTD. Fully isolated from the real
    picks / real P/L. Pure reads + a cheap idempotent POTD lock — no heavy
    slate generation (settlement rides the shared settle cron)."""
    from engine.nhl_tracker import (
        paper_summary, list_paper_history, record_paper_potd,
        get_paper_potd, paper_potd_summary,
    )
    _sd = et_today_str()
    try:
        record_paper_potd(_sd)   # lock today's best paper lean (idempotent)
    except Exception as e:
        logger.debug("NHL paper POTD lock (endpoint) failed: %s", e)
    try:
        summ = paper_summary()
        full = {
            "summary": summ,
            "rows": list_paper_history(limit=100),
            "pending": summ.get("pending", 0),
            "potd": get_paper_potd(_sd),
            "potd_record": paper_potd_summary(),
        }
    except Exception as e:
        logger.warning("NHL paper shadow endpoint failed: %s", e)
        full = {"summary": {}, "rows": [], "pending": 0,
                "potd": None, "potd_record": {}}
    return {"full": full}


@router.get("/api/nhl/tracker/summary")
def api_nhl_pick_summary():
    """Get NHL running pick totals."""
    from engine.nhl_tracker import get_pick_summary
    return get_pick_summary()


@router.post("/api/nhl/tracker/record")
def api_nhl_record_picks():
    """Record today's NHL picks."""
    try:
        from engine.nhl_tracker import record_picks
        picks = record_picks()
        return {"recorded": len(picks), "picks": picks}
    except Exception as e:
        logger.error("NHL record picks failed: %s", e, exc_info=True)
        return {"error": str(e), "recorded": 0}


@router.post("/api/nhl/tracker/settle")
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


@router.get("/api/nhl/backtest")
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
        from ._bestbets import cached_backtest
        return cached_backtest(
            f"backtest_nhl_{season or 0}_{days}_{min_edge:g}_{int(pit)}",
            lambda: run_nhl_backtest(days=days, min_edge=min_edge, season=season,
                                     pit_mode=pit))
    except Exception as e:
        logger.error("NHL backtest failed: %s", e, exc_info=True)
        return {"error": str(e)}
@router.get("/api/nhl/backtest/thresholds")
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


@router.get("/api/nhl/odds/history")
def api_nhl_odds_history(date: str = Query(default="")):
    """Get stored historical odds."""
    from engine.odds_history import get_historical_odds
    return get_historical_odds(date=date or None)


@router.get("/api/debug/nhl-live-stats")
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


@router.get("/api/debug/nhl-raw-stats")
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

