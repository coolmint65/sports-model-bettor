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

logger = logging.getLogger(__name__)
router = APIRouter()


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
        return _get_nba_scoreboard(date)
    except Exception as e:
        logger.error("NBA scoreboard failed: %s", e)
        return []


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
            result["best_pick"] = q1_only[0] if q1_only else None
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


@router.get("/api/nba/best-bets")
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
            from engine._pick_helpers import tag_confidence
            tag_confidence(full_picks)
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
        Q1_DERIV_TYPES = {"Q1 Team Total", "Q1 Total O/E"}
        FULL_PRIMARY = {"ML", "SPREAD", "TOTAL"}
        FULL_ALT = {"ALT SPREAD", "ALT TOTAL"}
        def _top(types_set, source=core_picks):
            cands = [p for p in source
                     if p.get("type") in types_set
                     and (p.get("confidence") or "lean") != "skip"]
            return cands[0] if cands else None
        best_q1 = _top(Q1_TYPES)
        # Q1 derivative fallback: if HR ships only team-total / O-E for
        # Q1 (no primary Q1 ML/spread/total clears the gates), let the
        # derivative fill the Q1 toggle slot. derivative_picks is
        # already sorted highest-edge first.
        if best_q1 is None:
            best_q1 = _top(Q1_DERIV_TYPES, source=derivative_picks)
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
        nba_target = et_today_str()
        diff = _nba_refresh(bets, nba_target)
        if any(diff.values()):
            logger.info("Tracker pending sync (nba): %s", diff)
    except Exception as e:
        logger.warning("NBA tracker pending refresh failed: %s", e)

    # Piggyback record_picks — see MLB twin for rationale (cron skips
    # left MLB tracker empty for 4 days starting 2026-04-29 because
    # only the cron called record_picks).
    try:
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
        return result
    except ImportError:
        return {"error": "NBA tracker module not loaded yet", "settled": 0}
    except Exception as e:
        logger.error("NBA settle picks failed: %s", e, exc_info=True)
        return {"error": str(e), "settled": 0}


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
