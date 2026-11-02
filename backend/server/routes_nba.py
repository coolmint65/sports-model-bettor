"""NBA API routes.

Extracted 2026-05-02 from backend/server/__init__.py as part of the
ongoing per-sport router split (#157). Mounted in __init__.py via
``app.include_router(nba_router)``.

Shared helpers (ESPN_BASE, _fetch_espn_json) live in ``._espn`` to avoid
a circular import with the FastAPI app instance.
"""
from __future__ import annotations

import logging
import re
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
# the per-call module-table lookup (~30-50ms across multiple per-game
# worker calls).
from engine.picks import (
    match_odds as _match_picks,
    get_best_pick as _get_best_pick_shared,
)
# Shared PAPER-pick engine (unification Phase 2): picks the model's best lean
# per full-game family out of the ungated picks list, tagged paper=True.
from engine.paper_picks import select_paper_leans

logger = logging.getLogger(__name__)
router = APIRouter()

# Pick Authority shadow-lane pricing pass: at most once per game per this many seconds
# (see api_nba_best_bets). game_id -> last pass time.
_NBA_SHADOW_EVERY_S = 1800
_NBA_SHADOW_LAST: dict = {}
_NBA_SHADOW_CANDS: dict = {}   # game_id -> last shadow-lane candidates: the card's model lean (2026-10-26)


# Lazy proxies for helpers that still live in backend/server/__init__.py.
# Resolved on first call so that importing this module FROM __init__.py
# doesn't trigger a circular import. Once those helpers move into a
# dedicated _nba_helpers.py the proxies can be replaced with direct imports.
def _lazy(name):
    def proxy(*args, **kwargs):
        from backend import server as _srv
        return getattr(_srv, name)(*args, **kwargs)
    proxy.__name__ = name
    return proxy

_get_nba_scoreboard = _lazy("_get_nba_scoreboard")
_odds_from_scoreboard_cache = _lazy("_odds_from_scoreboard_cache")
_nba_season_context = _lazy("_nba_season_context")
_predict_nba_full = _lazy("_predict_nba_full")
_pred_cache_clear_sport = _lazy("_pred_cache_clear_sport")
_is_game_imminent = _lazy("_is_game_imminent")
_is_game_locked = _lazy("_is_game_locked")
_get_recorded_pick = _lazy("_get_recorded_pick")
_picks_store_get = _lazy("_picks_store_get")
_picks_store_put = _lazy("_picks_store_put")


class _LazyConst:
    """Proxy for a constant collection that lives in __init__.py.

    PEP 562 module __getattr__ only catches ``module.attr`` lookups
    from outside the module, NOT bare-name lookups inside the
    module's own function bodies. So `if x not in _NBA_DERIV_TYPES`
    inside a route handler raises NameError unless _NBA_DERIV_TYPES
    is bound at module level. This proxy binds an iterable shim that
    resolves on first use (after __init__.py finishes loading and the
    real constant exists) and delegates arbitrary attribute access
    (``.clear()``, ``.update()`` on mutable caches) to the resolved
    object.
    """
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


_NBA_DERIV_TYPES = _LazyConst("_NBA_DERIV_TYPES")


@router.post("/api/nba/sync")
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


@router.get("/api/nba/scoreboard")
def api_nba_scoreboard(date: str = Query(default="")):
    """Return today's NBA games with Q1 scores."""
    try:
        games = _get_nba_scoreboard(date, lookahead=True)
    except Exception as e:
        logger.error("NBA scoreboard failed: %s", e)
        return []
    # Attach the RECORDED paper lean (carries the settled W/L) so live/final
    # cards keep showing the pick + result like WNBA (Austin 2026-09-29).
    if isinstance(games, list):
        try:
            from engine.nba_tracker._paper import recorded_nba_paper_pick as _rpp
            for _g in games:
                if isinstance(_g, dict) and not _g.get("paper_pick"):
                    _gid = _g.get("id")
                    if _gid is not None:
                        _p = _rpp(_gid)
                        if _p:
                            _g["paper_pick"] = _p
        except Exception:
            pass
    # Card lean lock (Austin 2026-10-27 "they should stay on the cards when the game is live"): best-bets
    # skips started games, so a live/final card shows the lean it held pre-game (+ its W/L once settled).
    if isinstance(games, list):
        try:
            from engine.selection import lean_lock as _lean_lock
            for _g in games:
                if (isinstance(_g, dict) and not _g.get("paper_pick")
                        and (_g.get("status") or {}).get("state") in ("in", "post")):
                    _p = _lean_lock.held_with_result("nba", "nba", _g.get("id"))
                    if _p:
                        _g["paper_pick"] = _p
            # best-bets (the pre-game source of the graduated flag) skips started games: stamp the live/final
            # card's pick with its cell's current graduation (Austin 2026-10-29)
            for _g in games:
                if isinstance(_g, dict) and (_g.get("status") or {}).get("state") in ("in", "post"):
                    _lean_lock.flag_graduation("nba", "nba", _g.get("paper_pick"))
        except Exception:
            pass
    return games


@router.get("/api/nba/standings")
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


@router.get("/api/nba/predict")
def api_nba_predict(home: str = Query(...), away: str = Query(...)):
    """Run NBA Q1 prediction for a specific matchup."""
    try:
        from engine.nba_q1_predict import predict_q1_matchup
    except ImportError:
        return {"error": "NBA Q1 prediction engine not loaded yet"}

    # Pull Q1 markets so spread/total/ML picks can be evaluated against
    # real posted lines instead of being silently skipped. Uses the
    # Hard Rock → ESPN fallback chain.
    # Try the scoreboard cache first -- _get_nba_scoreboard already
    # attached Q1 odds to each game. Only hit fetch_all_nba_odds on
    # cache miss.
    odds = _odds_from_scoreboard_cache(home, away, sport="nba")
    if not odds:
        try:
            from scrapers.nba_odds import fetch_all_nba_odds
            odds_map = fetch_all_nba_odds() or {}
            odds = _match_picks(home, away, odds_map) or {}
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
                        "date": et_today_str(),
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
            from engine._pick_helpers import tag_confidence
            tag_confidence(full_picks)
            result["full"] = full_factor
            result["full_picks"] = full_picks
        except Exception as e:
            logger.warning("NBA full-game prediction in /predict failed for %s/%s: %s",
                           home, away, e)

        try:
            from engine.ensemble import ensemble_nba
            log_meta = {
                "date": et_today_str(),
                "game_id": f"{et_today_str()}_{home}_{away}",
            }
            result["ensemble"] = ensemble_nba(result, log_meta=log_meta)
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
            # best_pick = highest-edge Q1-only pick. The Q1 detail panel
            # reads result["best_pick"]; if we let it pick from the
            # merged Q1+Full list it'll surface a full-game ALT SPREAD
            # (line 1.5 / +130 etc.) under the Q1 header — confirmed
            # bug 2026-04-29 with LAL@HOU showing "HOU +1.5 @ +130" on
            # the Q1 panel because the merged list put the full-game
            # ALT SPREAD on top by edge. Full panel finds its own best
            # from full_picks separately.
            _Q1_TYPES = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
            q1_only = [p for p in picks if p.get("type") in _Q1_TYPES]
            result["best_pick"] = _get_best_pick_shared(q1_only) if q1_only else None  # calibrated select (2026-10-13)
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


def _blend_card_wp(sport, league, wp, home_odds, away_odds):
    """Card % = the Pick Authority's market blend (backend.server._display_cal.blend_card_wp)."""
    try:
        from ._display_cal import blend_card_wp
        return blend_card_wp(sport, league, wp, home_odds, away_odds)
    except Exception:
        return wp


def _nba_card_win_prob(pred):
    """Card win-% bar probs (Austin 2026-10-25 "NBA missing the percentage bars").
    The NBA pred never carries a win_prob dict (NHL/MLB do): generate_full_picks writes
    the ENSEMBLE home-win blend into pred["full"]["ml_home"] instead, so the old
    pred.get("win_prob", {}) emitted {} and GameCard (win_prob.home != null) hid the bar
    on every card. Shape it from that ensemble prob — the same number the ML pick logic
    uses (BasketballPanel rule: bar == what picks use, never the factor-only margin)."""
    wp = pred.get("win_prob") or {}
    if wp.get("home") is not None:
        return wp
    full = pred.get("full") or {}
    mlh = full.get("ml_home")
    if mlh is None:
        return {}
    mla = full.get("ml_away")
    return {"home": float(mlh),
            "away": float(mla) if mla is not None else 1.0 - float(mlh)}


_NBA_BB_CACHE: dict = {}
_NBA_BB_TTL_S = 60.0

# Stale-while-revalidate + persisted cache, same as NHL/MLB (speed pass 2026-10-28). NBA had
# neither: every /best-bets AND every /pick-of-day/nba call ran the full per-game slate
# synchronously on the request thread (90 s+ timeouts on a 10-game night).
from ._bestbets import persist_bb as _persist_bb, load_bb as _load_bb  # noqa: E402
try:
    _bb_rh, _ = _load_bb("nba")
    if _bb_rh is not None:
        _NBA_BB_CACHE["nba"] = (time.monotonic() - _NBA_BB_TTL_S - 1.0, _bb_rh)
except Exception:
    pass
_NBA_BB_REFRESHING: set = set()


@router.get("/api/nba/best-bets")
def api_nba_best_bets():
    """Served from a short-TTL cache + background refresh so the heavy slate never blocks a
    dashboard load after the first. Recording still runs inside the compute (bg refresh)."""
    import threading
    ck = "nba"
    cached = _NBA_BB_CACHE.get(ck)
    if cached is not None:
        built, resp = cached
        if (time.monotonic() - built) >= _NBA_BB_TTL_S and ck not in _NBA_BB_REFRESHING:
            _NBA_BB_REFRESHING.add(ck)

            def _bg():
                try:
                    r = _compute_nba_best_bets()
                    _NBA_BB_CACHE[ck] = (time.monotonic(), r)
                    _persist_bb("nba", r)
                except Exception as e:
                    logger.warning("[nba] bg best-bets refresh failed: %s", e)
                finally:
                    _NBA_BB_REFRESHING.discard(ck)

            threading.Thread(target=_bg, daemon=True).start()
        return resp
    resp = _compute_nba_best_bets()
    _NBA_BB_CACHE[ck] = (time.monotonic(), resp)
    _persist_bb("nba", resp)
    return resp


@_bb_reset_on_exit("nba")
def _compute_nba_best_bets():
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

    games = _get_nba_scoreboard()  # today-only: cards get the full window from /scoreboard; best-bets stays fast

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
        market_odds = _match_picks(h_abbr, a_abbr, nba_odds_map) or {}
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
    shadow_by_game: dict = {}   # game_id -> Pick Authority shadow-lane candidates (W=1)
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

        # Q1 may legitimately be empty when HR doesn't ship a Q1 spread/total
        # for this game (LAL @ OKC 2026-05-05: q1_home_spread_point + q1_total
        # both None, so Q1 picks list is empty). Don't bail here — full-game
        # picks are generated below and the union is what feeds the card.
        picks = list(pred.get("picks", []))

        # ── Phase 2k: full-game predictions + picks ──
        # Run the full-game factor model + MC alongside Q1 so the
        # ensemble blender has all three signals. Append full-game
        # picks to the same list; the picks_store + GameCard layers
        # consume them transparently.
        full_picks: list = []
        _ens_input = None
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
            _ens_input = ensemble_input
            # Log the pre-tip ensemble legs (factor / MC / GBM / blend) per game to
            # data/ensemble_log.db (UPSERT -> the last pre-tip snapshot) so the win-%
            # bar can be calibrated on real results (2026-10-26; only Q1 was logged).
            try:
                from engine.ensemble import ensemble_nba as _ens_log
                _ens_log(ensemble_input, log_meta={"date": et_today_str(), "game_id": game["id"]})
            except Exception:
                pass
            # Tag confidence on full-game picks the same way Q1 does so
            # the GameCard "skip" gating works consistently.
            from engine._pick_helpers import tag_confidence
            tag_confidence(full_picks)
            picks = picks + full_picks
            pred["full"] = full_factor
            pred["full_picks"] = full_picks
        except Exception as e:
            logger.warning("NBA full-game prediction failed for %s/%s: %s",
                           h_abbr, a_abbr, e)

        # Pick Authority shadow lane (MintyBot 2026-10-26, Austin "NBA for PA"): price every
        # candidate again on the MODEL's own probability (W=1, shadow_mode) and mirror ALL of
        # them -- not the card's top 4 that survived the old stacked calibration -- to
        # picks_unified is_shadow=1, so an unproven market builds its record and graduates on
        # its own (NFL's design). The headline/card picks above stay market-anchored. The
        # second pricing pass doubles a refresh's cost, so it runs at most every
        # _NBA_SHADOW_EVERY_S per game: the recorder keeps only each market's first sighting
        # anyway, and a later pass still catches markets HR posts near tip (Q1, halves).
        if time.time() - _NBA_SHADOW_LAST.get(game["id"], 0.0) >= _NBA_SHADOW_EVERY_S:
            try:
                from engine.sports.nba._market_anchor import shadow_mode as _nba_shadow
                from engine.nba_picks import generate_q1_picks as _nba_gq1
                with _nba_shadow():
                    _sh = list(_nba_gq1(h_abbr, a_abbr, odds, None, pred=pred) or [])
                    if _ens_input is not None:
                        from engine.nba_picks import generate_full_picks as _nba_gfull
                        _sh += list(_nba_gfull(h_abbr, a_abbr, odds=odds, pred=_ens_input) or [])
                shadow_by_game[game["id"]] = _sh
                _NBA_SHADOW_CANDS[game["id"]] = _sh
                _NBA_SHADOW_LAST[game["id"]] = time.time()
            except Exception as e:
                logger.debug("NBA shadow pass failed for %s @ %s: %s", a_abbr, h_abbr, e)

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
        nba_target = et_today_str()
        recorded = _get_recorded_pick("nba", nba_matchup, nba_target)
        locked = _is_game_locked(game.get("date"))
        # Use the highest-edge non-skip core pick — same fix as NHL/MLB.
        # Otherwise a 'skip'-tagged top entry (e.g. low-edge ML) hides a
        # strong pick like a Q1_TOTAL or SPREAD sitting at index 1.
        live_best = _get_best_pick_shared(core_picks) if core_picks else None
        if locked and recorded:
            best = recorded
        elif live_best:
            best = live_best
        elif recorded:
            from engine.config import EDGE_STRONG, EDGE_MODERATE
            r_edge = float(recorded.get("edge") or 0)
            r_conf = ("strong" if r_edge >= EDGE_STRONG else
                      "moderate" if r_edge >= EDGE_MODERATE else
                      "lean")
            best = {**recorded, "confidence": r_conf}
        else:
            best = None
        # No `if not best: continue` any more (2026-10-26): under the Pick Authority most
        # games have NO qualifying pick (an unproven market defers to the line, ~0 edge), and
        # skipping them hid the whole card. Every predicted game renders; NO-PLAY = no pick.

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
        # between Q1 and Full headlines without re-fetching.
        Q1_TYPES = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
        Q1_DERIV_TYPES = {"Q1 Team Total", "Q1 Total O/E"}
        # Pick Authority (Austin 2026-10-18 "apply this to NBA"; fixed 2026-10-26): each
        # view's headline = select_pick over ALL of that view's candidates -- the best
        # PROVEN market (edge_proven) by edge x prob, else NO-PLAY. It used to gate only the
        # view's single top-edge pick, so an unproven top pick hid a proven one beneath it.
        # Each view's card == its hero (best_pick_full==paper_picks[0],
        # best_pick_q1==paper_picks_q1[0]); main best_pick = the stronger of the two. The
        # game already cleared the no-pick guard above, so a NO-PLAY game still renders.
        _q1_view = Q1_TYPES | Q1_DERIV_TYPES
        _q1_cands = [p for p in picks if p.get("type") in _q1_view]
        _full_cands = [p for p in picks if p.get("type") not in _q1_view]
        try:
            from engine.selection import select_pick as _authority_select
            best_full = _authority_select("nba", "nba", _full_cands)
            best_q1 = _authority_select("nba", "nba", _q1_cands)
        except Exception:
            best_full = best_q1 = None
        _mains = [x for x in (best_full, best_q1) if x]
        best = (max(_mains, key=lambda p: float(p.get("edge") or 0) * float(p.get("prob") or p.get("model_prob") or 0))
                if _mains else None)
        # Card == tracker (Austin 2026-11-01 "one pick, read everywhere"): the headline HOLDS the game's
        # RECORDED authority pick -- the FIRST non-void nba_paper_picks row for the game, exactly what
        # /api/nba/tracker/unified grades -- at its entry price, instead of re-deriving a live pick every
        # refresh as lines move. Live selection still feeds best_full/best_q1 so the recorder keeps
        # working; it only headlines until a pick is recorded (the recorder writes that headline first).
        try:
            _rec_best = _nba_recorded_authority_best(game["id"])
        except Exception:
            _rec_best = None
        if _rec_best:
            from engine.config import EDGE_STRONG, EDGE_MODERATE
            _re = float(_rec_best.get("edge") or 0)
            best = {**_rec_best, "model_prob": _rec_best.get("prob"), "held": True,
                    "confidence": ("strong" if _re >= EDGE_STRONG else "moderate" if _re >= EDGE_MODERATE else "lean")}
        _nba_paper_full = [best_full] if best_full else []
        _nba_paper_q1 = [best_q1] if best_q1 else []
        try:
            from engine.selection import card_lean as _card_lean
            if _rec_best:   # held graduated headline -> the pill shows exactly it (2026-11-01; was best_full)
                _nba_card_lean = _card_lean("nba", "nba", [], best)
            else:
                _nba_card_lean = _card_lean("nba", "nba", [p for p in _NBA_SHADOW_CANDS.get(game["id"], [])
                                                          if p.get("type") not in _q1_view] or _full_cands, best)  # card's best lean (Austin 2026-10-26): graduated headline, else the ungraduated lean -- display only
        except Exception:
            _nba_card_lean = None
        try:   # held per game (Austin 2026-10-27): the first lean is kept, and carried through live + final
            from engine.selection import lean_lock as _lean_lock
            _nba_card_lean = _lean_lock.hold("nba", game["id"], _nba_card_lean)
        except Exception:
            pass

        bets.append({
            "game_id": game["id"],
            "lookahead": bool(game.get("lookahead")),
            "matchup": f"{a_abbr} @ {h_abbr}",
            "home": game["home"],
            "away": game["away"],
            "time": game["date"],
            "venue": game.get("venue", ""),
            "odds": odds,  # both-sides board (OddsGrid reads this; 2026-10-11)
            "best_pick": best,
            "best_pick_q1": best_q1,
            "best_pick_full": best_full,
            "all_picks": all_picks,
            "derivative_picks": derivative_picks,
            "confidence": (best or {}).get("confidence", "lean"),
            "win_prob": _blend_card_wp("nba", "nba", _nba_card_win_prob(pred),
                                       (odds or {}).get("home_ml"), (odds or {}).get("away_ml")),
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
            # PAPER (shadow) layer: the model's raw ML/SPREAD/TOTAL directional
            # lean (real-bet gate bypassed), recorded stake-free in the SEPARATE
            # nba_paper_picks table by the piggyback below — never nba_picks.
            "paper_picks": _nba_paper_full,
            "card_lean": _nba_card_lean,
            "paper_picks_q1": _nba_paper_q1,
        })

    bets.sort(key=lambda b: (b["best_pick"] or {}).get("edge", 0), reverse=True)

    try:
        from engine.nba_tracker import refresh_pending_for_today as _nba_refresh
        nba_target = et_today_str()
        # TODAY-only (Austin 2026-10-03): lookahead put future games in `bets`
        # for display; pending-sync must ignore them so future picks are not
        # misdated under today.
        _bets_today = [b for b in bets if not b.get("lookahead")]
        diff = _nba_refresh(_bets_today, nba_target)
        if any(diff.values()):
            logger.info("Tracker pending sync (nba): %s", diff)
    except Exception as e:
        logger.warning("NBA tracker pending refresh failed: %s", e)

    # Piggyback record_picks — see MLB twin for rationale (cron skips
    # left MLB tracker empty for 4 days starting 2026-04-29 because
    # only the cron called record_picks).
    try:
        # Paper-only (Austin 2026-10-04 "gone entirely"): NBA no longer records
        # ACTIVE picks (nba_picks). The paper layer (nba_paper_picks) is untouched
        # and drives the Tracker tab. Re-enable via
        # apply_override("nba", "ENABLE_NBA_ACTIVE_RECORDING", True).
        from engine.config import get_flag
        if get_flag("ENABLE_NBA_ACTIVE_RECORDING", False, sport="nba"):
            from engine.nba_tracker import record_picks as _nba_record
            new_recorded = _nba_record(date=nba_target)
            if new_recorded:
                logger.info("Tracker recorded (nba api hit): %d new pick(s) for %s",
                            len(new_recorded), nba_target)
    except Exception as e:
        logger.warning("NBA tracker record_picks (api hit) failed: %s", e)

    # See NHL twin in api_nhl_bets — piggyback CLV capture on every
    # games-endpoint hit. NBA Q1 closing odds are particularly tricky:
    # HR drops the Q1 market the moment Q1 ends, and the once-daily
    # sync rarely catches the line right at tip-off. API-driven capture
    # closes that gap for both Q1 and Full markets.
    try:
        from engine.nba_tracker import capture_closing_odds as _nba_cap
        n = _nba_cap()
        if n:
            logger.info("NBA closing-odds capture (api hit): %d", n)
    except Exception as e:
        logger.warning("NBA closing-odds capture (api hit) failed: %s", e)

    # PAPER shadow layer (Austin 2026-09-28, unification Phase 2): record the
    # model's raw directional lean per full-game family as stake-free rows in
    # the SEPARATE nba_paper_picks table (never nba_picks), then CLV-capture +
    # grade them with the real core. Fully isolated from the real record;
    # kickoff-locked (skip started games so a post-tip lean isn't recorded).
    try:
        from engine.nba_tracker import (
            record_paper as _nba_paper_rec,
            settle_paper as _nba_paper_settle,
            capture_closing_odds_paper as _nba_paper_cap,
        )
        _ptarget = et_today_str()
        _pn = 0
        # ONE paper lean per view (Austin 2026-10-03, mirroring NHL/WNBA): the
        # single max-edge full-game lean + the single max-edge Q1 lean, each a
        # live mirror in its own family group (record_paper swaps/clears stale
        # rows within the group). Was: all 3 full-game leans, no Q1.
        from engine.nba_tracker._paper import Q1_VIEW_TYPES as _q1_view_types
        def _one(lst):
            lst = [p for p in (lst or []) if p.get("edge") is not None]
            return [max(lst, key=lambda x: x.get("edge"))] if lst else []
        for _b in bets:
            if _b.get("lookahead"):
                continue  # TODAY-only: do not record future lookahead games
            if _b.get("is_locked"):
                continue  # kickoff-lock
            # Pick Authority graduation lane (Austin 2026-10-18 "capture all markets"):
            # mirror the FULL generated slate (full + Q1 + derivatives: Q1 team total,
            # odd/even, alts) to picks_unified is_shadow=1 so un-headlined NBA markets
            # accumulate a settled record and can graduate. Once-per-game (deduped).
            try:
                from engine.picks_unified import record_shadow_slate as _rss_nba
                _rss_nba("nba", "nba", _b.get("game_id"), _b.get("matchup") or "",
                         shadow_by_game.get(_b.get("game_id")) or [])
            except Exception:
                pass
            _mu = _b.get("matchup") or ""
            _gid = _b.get("game_id")
            # FIRST PICK WINS (2026-11-01): record_paper writes each view's pick ONCE and never
            # rewrites it. The headline's view goes FIRST so the game's earliest row IS the card's
            # headline (_nba_recorded_authority_best = earliest non-void row).
            _views = [("full", _one(_b.get("paper_picks"))), ("q1", _one(_b.get("paper_picks_q1")))]
            _hb = _b.get("best_pick") or {}
            if _hb and _hb.get("type") in _q1_view_types:
                _views.reverse()
            for _sc, _pl in _views:
                _pn += _nba_paper_rec(_mu, _gid, _pl, target_date=_ptarget,
                                      scope=_sc).get("recorded", 0)
        if _pn:
            logger.info("NBA PAPER (api hit): recorded=%d shadow for %s",
                        _pn, _ptarget)
        try:
            _nba_paper_cap()
        except Exception as _e:
            logger.debug("NBA paper closing capture failed: %s", _e)
        try:
            _nba_paper_settle()
        except Exception as _e:
            logger.debug("NBA paper settle failed: %s", _e)
    except Exception as e:
        logger.warning("NBA paper piggyback (api hit) failed: %s", e)

    # Piggyback NBA injury refresh — same shape as the CLV piggyback.
    # nba_injury_refresh diffs the current OUT player set per game
    # against the snapshot it last stored; on a delta it invalidates
    # picks_cache + POTD + re-records picks with force=True so the
    # next predict consult sees the new injury state. Throttled at
    # 2 minutes per process so a burst of dashboard hits doesn't
    # hammer ESPN. The nba_injury_refresh internals also rely on a
    # 30-min in-process injury cache, so the actual ESPN fetch only
    # fires every half hour even if this throttle is shorter.
    try:
        global _NBA_INJ_REFRESH_LAST  # type: ignore[name-defined]
    except NameError:  # first call in this process
        pass
    _now_ts = _time.time()
    last_ts = globals().get("_NBA_INJ_REFRESH_LAST", 0.0)
    if _now_ts - last_ts >= 120:
        globals()["_NBA_INJ_REFRESH_LAST"] = _now_ts
        try:
            from engine.nba_injury_refresh import refresh_for_date
            inj_result = refresh_for_date(date=nba_target,
                                          record_picks=True)
            if (inj_result.get("invalidated") or 0) > 0:
                cleared = _pred_cache_clear_sport("nba")
                logger.info("NBA injury refresh (api hit): "
                            "deltas=%d invalidated=%d cleared=%d",
                            inj_result.get("deltas", 0),
                            inj_result.get("invalidated", 0),
                            cleared)
        except Exception as e:
            logger.warning("NBA injury refresh (api hit) failed: %s", e)

    try:
        from engine.derivative_tracker import record_top_derivatives
        diff = record_top_derivatives("nba", bets, n_per_game=1,
                                       min_edge=4.0,
                                       target_date=et_today_str())
        if diff.get("inserted") or diff.get("updated"):
            logger.info("Derivative tracker (nba): %s", diff)
    except Exception as e:
        logger.warning("NBA derivative recorder failed: %s", e)

    try:
        from engine.pick_events import detect_transitions
        target = et_today_str()
        # Track Q1 and Full transitions independently so each card's
        # popover surfaces only its own family's history. Without the
        # split, the global best_pick stream mixed Q1 swaps into Full
        # cards (and vice versa) — the chip showed Full ALT SPREAD
        # while the modal listed Q1 transitions.
        ev_full = detect_transitions("nba", [
            {"game_id": b["game_id"], "matchup": b["matchup"],
             "best_pick": b.get("best_pick_full")}
            for b in bets
        ], date=target, scope="full")
        ev_q1 = detect_transitions("nba", [
            {"game_id": b["game_id"], "matchup": b["matchup"],
             "best_pick": b.get("best_pick_q1")}
            for b in bets
        ], date=target, scope="q1")
        if any(ev_full.values()) or any(ev_q1.values()):
            logger.info("Pick events (nba) full=%s q1=%s", ev_full, ev_q1)
    except Exception as e:
        logger.warning("NBA pick-events failed: %s", e)

    _bb_progress_set("nba", phase="idle", finished_at=_time.time())
    return bets


# -- NBA unified tracker (MintyBot 2026-10-22, NHL "tracker merge" -> NBA) --
# The card's single BEST per game = the merged CALIBRATED best across all game
# markets (Full Game core + Q1 + Q1 team-total/derivative), GATED like NHL:
# core full-game always eligible; an EXTRA (Q1 / quarter) market may only
# HEADLINE once it has EARNED a track record (>=60 settled / >=30 days,
# engine.calibration_gate.headline_eligible) AND clears the +EV floor. PAPER /
# display only -- reads the isolated nba_paper_picks (full + Q1 leans) +
# nba_derivative_picks (Q1 team totals / odd-even); NEVER the real nba_picks.
# Forked from routes_nhl._nhl_merged_recorded_best / api_nhl_tracker_unified.

_NBA_EV_CACHE = {"ts": 0.0, "map": {}}

def _nba_extra_evidence_map(conn):
    """{bet_type: (settled_n, days_tracked)} for the EXTRA (Q1 / quarter /
    team-total) markets across nba_paper_picks (its Q1_* rows) +
    nba_derivative_picks. Cached 300s so the gate costs one GROUP BY per
    window, not a COUNT per candidate per game. Mirror of
    _nhl_extra_evidence_map. (Full-game bet_types are also counted here but
    never looked up -- only _extra picks consult the map.)"""
    import time as _t
    if _NBA_EV_CACHE["map"] and (_t.monotonic() - _NBA_EV_CACHE["ts"]) < 300:
        return _NBA_EV_CACHE["map"]
    m = {}
    try:
        for row in conn.execute(
                "SELECT bet_type, COUNT(*) n, MIN(date) mn, MAX(date) mx FROM ("
                "  SELECT bet_type,date,result FROM nba_paper_picks"
                "  UNION ALL SELECT bet_type,date,result FROM nba_derivative_picks"
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
    _NBA_EV_CACHE["ts"] = _t.monotonic(); _NBA_EV_CACHE["map"] = m
    return m


def _nba_market_scope(bet_type, pick):
    """Market bucket for the tracker chip, keyed on the BET not the source
    table -- nba_paper_picks holds BOTH the full-game core lean AND the Q1
    lean, so a Q1 pick must never show as 'Full Game'. NBA is Q1-centric:
    Full Game (ML/SPREAD/TOTAL/ALT) vs Q1 (Q1_ML/Q1_SPREAD/Q1_TOTAL) vs
    Q1 Team Total (the Q1 derivatives: team total, odd/even)."""
    bt = (bet_type or "").strip().upper()
    if bt in ("ML", "SPREAD", "TOTAL", "ALT SPREAD", "ALT TOTAL",
              "ALT_SPREAD", "ALT_TOTAL"):
        return "Full Game"
    if bt in ("Q1_ML", "Q1_SPREAD", "Q1_TOTAL"):
        return "Q1"
    if bt[:3] in ("H1_", "H2_", "Q2_", "Q3_", "Q4_"):
        return "Period"   # halves / later quarters (were mislabelled "Q1 Team Total", 2026-11-01)
    return "Q1 Team Total"


def _nba_flat_profit(result, odds):
    """Flat $100-stake P/L from (result, American odds) -- the EXACT convention
    engine.nba_tracker._settle.settle_picks already uses (win: +odds when
    positive else 100/abs(odds)*100; loss: -$100; push/void: $0). Paper rows
    record stake_units=0, so the NHL tracker's profit*stake_units would read a
    useless $0 -- grade every row flat $100 instead (Austin 2026-10-22)."""
    r = (result or "").strip().upper()
    if not r:
        return None  # pending -- excluded from the P/L sum
    if r == "W":
        try:
            o = float(odds if odds is not None else -110)
        except (TypeError, ValueError):
            o = -110.0
        if o == 0:
            return 0.0
        return round(o, 2) if o > 0 else round(100.0 / abs(o) * 100.0, 2)
    if r == "L":
        return -100.0
    return 0.0  # P / PUSH / V


_LAST_NBA_AUTO_SETTLE = [0.0]
def _maybe_auto_settle_nba():
    """Throttled, non-blocking settle so NBA paper + Q1 picks grade shortly
    after games go final. Fires on a tracker view, at most once per 10 min, in
    a daemon thread. Mirror of _maybe_auto_settle_nhl. engine.nba_db.get_conn
    is thread-local (WAL), so the daemon thread gets its own connection."""
    import time as _t, threading as _th
    now = _t.monotonic()
    if now - _LAST_NBA_AUTO_SETTLE[0] < 600.0:
        return
    _LAST_NBA_AUTO_SETTLE[0] = now
    def _run():
        try:
            from engine.nba_tracker import settle_paper as _sp
            _sp()
        except Exception as _e:
            logger.warning("auto-settle nba (paper full+q1) failed: %s", _e)
        try:
            from engine.derivative_tracker import settle_derivative_picks as _sd
            _sd("nba")
        except Exception as _e:
            logger.warning("auto-settle nba (derivatives) failed: %s", _e)
        try:
            from engine.pick_of_day import settle_potd as _spotd
            _spotd("nba")
        except Exception as _e:
            logger.warning("auto-settle nba (potd) failed: %s", _e)
    _th.Thread(target=_run, daemon=True).start()


def _nba_merged_recorded_best(game_id, date=None, conn=None, ev_map=None):
    """The game's single BEST recorded PAPER pick = the merged CALIBRATED best
    across all game markets (Full Game core + Q1 + Q1 team-total/derivative),
    GATED: core full-game always eligible; an EXTRA (Q1 / quarter) market may
    only win once it has EARNED a track record (headline_eligible) AND clears
    the +EV floor. Reads nba_paper_picks (full + Q1 leans) + nba_derivative_picks
    (Q1 derivatives); NEVER the real nba_picks. Chip-shaped + flat-$100 profit +
    _scope. Pass conn/ev_map to share them across many games (the tracker).
    Mirror of _nhl_merged_recorded_best."""
    import sqlite3 as _sq
    from engine.nba_db import get_conn
    from engine.calibration_gate import cal_edge as _ce, headline_eligible as _he
    if conn is None:
        try:
            conn = get_conn(); conn.row_factory = _sq.Row
        except Exception:
            return None
    if ev_map is None:
        ev_map = _nba_extra_evidence_map(conn)
    dq = " AND date = ?" if date else ""
    args = (str(game_id), date) if date else (str(game_id),)
    cands = []
    try:
        # Full-game core + Q1 primary leans both live in nba_paper_picks; the
        # ABS(edge)<=20 guard drops a blown-up calibration edge (mirror NHL).
        for r in conn.execute(
                "SELECT bet_type,pick,odds,edge,model_prob,result,profit,stake_units,closing_odds "
                "FROM nba_paper_picks WHERE game_id=?" + dq + " "
                "AND (result IS NULL OR result != 'V') AND ABS(COALESCE(edge,0))<=20.0", args):
            d = dict(r); d["_scope"] = _nba_market_scope(d.get("bet_type"), d.get("pick"))
            d["_extra"] = (d["_scope"] != "Full Game"); cands.append(d)
        # Q1 derivatives (team total / odd-even) -- always EXTRA.
        for r in conn.execute(
                "SELECT bet_type,pick,odds,edge,model_prob,result,profit,stake_units,closing_odds "
                "FROM nba_derivative_picks WHERE game_id=?" + dq + " "
                "AND (result IS NULL OR result != 'V')", args):
            d = dict(r); d["_scope"] = _nba_market_scope(d.get("bet_type"), d.get("pick"))
            d["_extra"] = True; cands.append(d)
    except Exception:
        return None
    if not cands:
        return None
    scored = []
    for p in cands:
        ce = _ce("nba", p.get("bet_type"), p.get("model_prob"), p.get("odds"),
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
    return {"type": b["bet_type"], "market": b["bet_type"], "pick": b["pick"],
            "odds": b["odds"], "edge": b["edge"], "prob": b["model_prob"],
            "result": b["result"], "paper": True, "_scope": b["_scope"],
            "closing_odds": b.get("closing_odds"),
            "profit": _nba_flat_profit(b["result"], b["odds"]),
            "stake_units": 1.0}


def _nba_recorded_authority_best(game_id, date=None, conn=None):
    """The game's RECORDED AUTHORITY pick -- FIRST PICK WINS ("one pick, read everywhere", Austin
    2026-11-01): the EARLIEST non-void nba_paper_picks row for the game. Shared by the card hold
    (_compute_nba_best_bets `best`) and /api/nba/tracker/unified, so the two can never disagree.

    Why earliest and not max(best Full row, best Q1 row) by edge*prob (the 2026-10-23 rule): the paper
    piggyback records one row per view (Full + Q1), and the two views' first picks usually land at
    DIFFERENT times (e.g. Full at 06:15, Q1 at 08:02). Max-of-two let the later view's row DISPLACE
    the pick the card had already headlined and held -- exactly the flip the rule forbids. Under
    first-pick-wins the first row recorded for a game is the headline the card showed at that moment
    (record_paper never rewrites a row, and the piggyback writes the headline's view first when both
    views record in the same pass), so the earliest row IS the held headline; the other view's row is
    kept as that view's evidence only. NO cal_edge / headline_eligible regate and NO edge guard.
    NEVER the real nba_picks. Chip-shaped + flat-$100 profit + _scope (tracker row shape unchanged).
    Pass conn to share it across many games."""
    import sqlite3 as _sq
    from engine.nba_db import get_conn
    if conn is None:
        try:
            conn = get_conn(); conn.row_factory = _sq.Row
        except Exception:
            return None
    dq = " AND date = ?" if date else ""
    args = (str(game_id), date) if date else (str(game_id),)
    try:
        r = conn.execute(
            "SELECT bet_type,pick,odds,edge,model_prob,result,closing_odds "
            "FROM nba_paper_picks WHERE game_id=?" + dq + " "
            "AND (result IS NULL OR result != 'V') ORDER BY id ASC LIMIT 1", args).fetchone()
    except Exception:
        return None
    if not r:
        return None
    b = dict(r)
    return {"type": b["bet_type"], "market": b["bet_type"], "pick": b["pick"],
            "odds": b["odds"], "edge": b["edge"], "prob": b["model_prob"],
            "result": b["result"], "paper": True,
            "_scope": _nba_market_scope(b.get("bet_type"), b.get("pick")),
            "closing_odds": b.get("closing_odds"),
            "profit": _nba_flat_profit(b["result"], b["odds"]),
            "stake_units": 1.0}


@router.get("/api/nba/tracker/unified")
def api_nba_tracker_unified(limit: int = 300):
    """ONE NBA tracker = EXACTLY the card's headline `best_pick` per game, read
    back from the recorded authority rows in nba_paper_picks (the per-view
    best_full / best_q1 the card records post select_pick / Pick-Authority gate);
    one row per game = the FIRST recorded authority row (first pick wins, 2026-11-01),
    the SAME function the card holds. Pick Authority -- "one pick, read everywhere" (Austin
    2026-10-23): was a cal_edge/headline-gated re-derivation across
    nba_paper_picks + nba_derivative_picks (_nba_merged_recorded_best) that
    surfaced a DIFFERENT pick than the card. NEVER the real nba_picks (NBA active
    recording is off; the authority lives in the isolated nba_paper_picks). Via
    _nba_recorded_authority_best; P/L graded FLAT $100. Read-only; per-market
    endpoints untouched."""
    _maybe_auto_settle_nba()
    import sqlite3 as _sq
    from engine.nba_db import get_conn
    conn = get_conn()
    conn.row_factory = _sq.Row
    try:
        gids = [r[0] for r in conn.execute(
            "SELECT game_id, MAX(created_at) c FROM nba_paper_picks "
            "GROUP BY game_id ORDER BY c DESC LIMIT ?", (limit,))]
    except Exception as e:
        logger.warning("nba unified tracker game list failed: %s", e)
        gids = []
    rows = []
    for gid in gids:
        # Pick Authority: show EXACTLY the card's headline (best_pick), read back
        # from the recorded authority rows in nba_paper_picks -- NOT the old
        # cal_edge/headline-gated re-derivation across the paper+derivative pools
        # (_nba_merged_recorded_best), which surfaced a DIFFERENT pick than the
        # card headlined ("one pick, read everywhere", Austin 2026-10-23).
        best = _nba_recorded_authority_best(gid, conn=conn)
        if not best:
            continue
        meta = conn.execute("SELECT matchup,date FROM nba_paper_picks WHERE game_id=? LIMIT 1", (str(gid),)).fetchone()
        rows.append({
            "game_id": gid,
            "date": meta["date"] if meta else None,
            "matchup": meta["matchup"] if meta else None,
            "market": best.get("_scope"),
            "bet_type": best["type"], "pick": best["pick"], "odds": best["odds"],
            "edge": best["edge"], "result": best["result"], "profit": best.get("profit"),
            "model_prob": best.get("prob"), "closing_odds": best.get("closing_odds"),
            "stake_units": best.get("stake_units"),
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


@router.get("/api/nba/tracker/history")
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


@router.get("/api/nba/tracker/summary")
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


@router.post("/api/nba/tracker/record")
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


@router.post("/api/nba/tracker/settle")
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
        try:
            from engine.nba_tracker import settle_paper as _nba_paper_settle
            _nba_paper_settle()
        except Exception as e:
            logger.warning("NBA paper settle failed: %s", e)
        return result
    except ImportError:
        return {"error": "NBA tracker module not loaded yet", "settled": 0}
    except Exception as e:
        logger.error("NBA settle picks failed: %s", e, exc_info=True)
        return {"error": str(e), "settled": 0}


@router.get("/api/nba/tracker/paper")
def api_nba_paper_shadow():
    """NBA PAPER-pick shadow tracker — the model's raw directional leans
    recorded stake-free in the SEPARATE nba_paper_picks table, graded + CLV'd
    by the same core. Fully isolated from the real (nba_picks) record. Mirrors
    /api/nhl/tracker/paper so the shared PickHistory paper panel can render it."""
    try:
        from engine.nba_tracker._paper import (
            paper_summary, list_paper_history,
            record_nba_paper_potd, get_nba_paper_potd, nba_paper_potd_summary,
        )
        # Lock today's paper POTD per view (idempotent per date+scope = game-day
        # freeze) so the hero shows a STABLE pick, not a live recompute. 2026-10-03.
        for _sc in ("full", "q1"):
            try:
                record_nba_paper_potd(scope=_sc)
            except Exception as _e:
                logger.debug("NBA paper POTD lock (%s) failed: %s", _sc, _e)
        return {
            "full": {"potd": get_nba_paper_potd(scope="full"),
                     "potd_record": nba_paper_potd_summary(scope="full")},
            "q1": {"potd": get_nba_paper_potd(scope="q1"),
                   "potd_record": nba_paper_potd_summary(scope="q1")},
            "summary": paper_summary(), "rows": list_paper_history(limit=50)}
    except Exception as e:
        logger.error("NBA paper shadow failed: %s", e)
        return {"summary": {}, "rows": []}


@router.get("/api/nba/backtest")
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


@router.get("/api/nba/scoreboard/debug")
def api_nba_scoreboard_debug():
    """Debug: show raw ESPN NBA scoreboard response."""
    import json
    target_date = et_today_str("%Y%m%d")
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
