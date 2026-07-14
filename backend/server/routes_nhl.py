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
from ._tz import et_today_str
# Hot-path imports — moved up from inside per-request handlers to save
# the per-call module-table lookup cost in the NHL best-bets pipeline.
from engine.picks import (
    match_odds as _match_picks,
    get_best_pick as _get_best_pick_shared,
)

logger = logging.getLogger(__name__)
router = APIRouter()


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
_NHL_ESPN_TO_KEY = _LazyConst("_NHL_ESPN_TO_KEY")


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
    return _get_nhl_scoreboard(date)


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
        result["best_pick"] = picks[0] if picks else None
        result["odds"] = game_odds
    except Exception as e:
        logger.warning("NHL picks generation in predict failed for %s/%s: %s",
                       home, away, e)
        result["picks"] = []
        result["best_pick"] = None
        result["odds"] = {}

    return result


@router.get("/api/nhl/best-bets")
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
        nhl_target = et_today_str()
        recorded = _get_recorded_pick("nhl", nhl_matchup, nhl_target)
        locked = _is_game_locked(game.get("date"))
        # Use the highest-edge non-skip core pick for the card headline.
        # `core_picks[0]` alone showed "no pick" whenever the top-by-EV
        # entry happened to be tagged 'skip' — even when a strong pick
        # (e.g. PL VGK -1.5) sat at index 1. Skipping the skip-tagged
        # picks here matches the MLB pattern (engine.picks.get_best_pick).
        live_best = _get_best_pick_shared(core_picks) if core_picks else None
        if locked and recorded:
            best = recorded
        elif live_best:
            best = live_best
        elif recorded:
            # Card should never disagree with the tracker on whether a
            # game has a play. If line drift dropped the live pick below
            # any threshold, fall back to the recorded pick (re-tagged).
            from engine.config import EDGE_STRONG, EDGE_MODERATE
            r_edge = float(recorded.get("edge") or 0)
            r_conf = ("strong" if r_edge >= EDGE_STRONG else
                      "moderate" if r_edge >= EDGE_MODERATE else
                      "lean")
            best = {**recorded, "confidence": r_conf}
        else:
            best = None
        # A "derivative-only" night (CAR @ MTL 2026-05-27 with only
        # Period Total + Period DNB above gates and no core ML/spread/
        # total) used to drop the whole game here, taking the P1 tab
        # card down with it. Keep the game in the response when ANY
        # derivative pick exists so the P1 tab still renders the
        # period play even without a core-card headline.
        has_deriv = any(p.get("type") in _NHL_DERIV_TYPES for p in picks)
        if not best and not has_deriv:
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
        # P1 best pick — highest-edge first-period derivative. The
        # Period-* bet_types ("Period DNB", "Period Total", "Period BTS")
        # span P1/P2/P3 of a game; the period is encoded in the pick
        # TEXT ("P1 Over 1.5", "P2 Winner", "P3 BTS Yes"). Filter on
        # type AND pick-text prefix "P1" so the bets card surfaces the
        # first-period pick specifically rather than whichever period
        # happens to have the highest edge.
        _PERIOD_TYPES = {"Period DNB", "Period Total", "Period BTS"}
        p1_candidates = [
            p for p in derivative_picks
            if p.get("type") in _PERIOD_TYPES
                and str(p.get("pick") or "").startswith("P1 ")
                and (p.get("edge") or 0) > 0
        ]
        # Stake-aware tiebreak (same logic as NBA Q1 family selection).
        # Within a 1pp edge band, prefer the higher-stake recommendation —
        # a 5.1% edge on -110 chalk gets 0u while a 4.9% edge on +money
        # gets 0.5u. Cards should surface the actionable bet, not the
        # cosmetically-bigger edge.
        if p1_candidates:
            top_edge = max(p.get("edge", 0) or 0 for p in p1_candidates)
            near_top = [
                p for p in p1_candidates
                if (p.get("edge", 0) or 0) >= (top_edge - 1.0)
            ]
            best_pick_p1 = max(
                near_top,
                key=lambda p: (
                    (p.get("stake_units") or 0),
                    (p.get("edge") or 0),
                ),
            )
        else:
            best_pick_p1 = None
        # Secondary derivative badge for the card. GameCard renders
        # `bet.best_derivative` as a subordinate chip beneath the main
        # bet when present; without it the deriv pick stays invisible
        # on the card even though it's in `derivative_picks`. Same
        # pattern as MLB's `best_derivative` field.
        best_derivative = next(
            (p for p in derivative_picks
             if (p.get("confidence") or "lean") != "skip"),
            None,
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
            "best_pick_full": best,
            "best_pick_p1": best_pick_p1,
            "best_derivative": best_derivative,
            "all_picks": all_picks,
            "derivative_picks": derivative_picks,
            "confidence": (best or {}).get("confidence", "lean"),
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

    try:
        from engine.nhl_tracker import refresh_pending_for_today as _nhl_refresh
        nhl_target = et_today_str()
        diff = _nhl_refresh(bets, nhl_target)
        if any(diff.values()):
            logger.info("Tracker pending sync (nhl): %s", diff)
    except Exception as e:
        logger.warning("NHL tracker pending refresh failed: %s", e)

    # Piggyback record_picks — see MLB twin for rationale.
    try:
        from engine.nhl_tracker import record_picks as _nhl_record
        new_recorded = _nhl_record(date=nhl_target)
        if new_recorded:
            logger.info("Tracker recorded (nhl api hit): %d new pick(s) for %s",
                        len(new_recorded), nhl_target)
    except Exception as e:
        logger.warning("NHL tracker record_picks (api hit) failed: %s", e)

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
            g_result = refresh_for_date(date=nhl_target, record_picks=True)
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
        diff = record_top_derivatives("nhl", bets, n_per_game=1,
                                       min_edge=4.0,
                                       target_date=et_today_str())
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
        ], date=et_today_str())
        if any(ev.values()):
            logger.info("Pick events (nhl): %s", ev)
    except Exception as e:
        logger.warning("NHL pick-events failed: %s", e)

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
    picks = conn.execute(
        f"SELECT * FROM nhl_picks WHERE bet_type NOT IN ({placeholders}) "
        f"ORDER BY created_at DESC LIMIT 50",
        tuple(_NHL_DERIV_TYPES),
    ).fetchall()
    return [dict(p) for p in picks]


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
        return run_nhl_backtest(days=days, min_edge=min_edge, season=season,
                                pit_mode=pit)
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

