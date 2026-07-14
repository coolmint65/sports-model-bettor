"""MLB API routes — extracted from backend/server/__init__.py.

Stage-2 of #317 (MLB monolith extraction). 17 of 19 MLB endpoints now
live here:
    /api/teams, /api/teams/{team_id}, /api/teams/{team_id}/profile
    /api/standings
    /api/pitcher/{pitcher_id}
    /api/scoreboard
    /api/tracker/history, /api/tracker/record, /api/tracker/settle,
        /api/tracker/summary
    /api/calibrate, /api/calibrate/teams
    /api/debug/odds, /api/debug/teams
    /api/backtest
    /api/mlb/refresh-lineups
    /api/accuracy

The two remaining (``/api/predict``, ``/api/best-bets``) stay in
__init__.py because each pulls on a chain of MLB-specific helpers
(_predict_mlb_full, _picks_store_*, _get_scoreboard, _enrich_games,
_resolve_abbr, _alt_abbr, _safe_game_pk, _parse_espn_scoreboard,
_compute_injury_impact_pct, _mlb_form_from_reasoning, _mlb_season_context,
_implied, _find_ou, _mc_seed, PredictRequest model). Migrating those
safely requires extracting the helpers first — separate sprint.

Helper access from this module follows the same lazy-proxy pattern
``routes_nba.py`` / ``routes_nhl.py`` use, so the shared helpers
don't need to move yet either.

Mounted from __init__.py via ``app.include_router(mlb_router)``.
"""
from __future__ import annotations

import logging

from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from engine.db import (
    get_conn, get_all_teams, get_team_by_id, get_team_record,
    get_pitcher_season, get_bullpen, get_recent_games,
)
# Hot-path imports — moved up from inside per-request handlers. The
# best-bets pipeline fans these out to 8 worker threads which each
# previously paid the import-table lookup cost (~50-80ms per cold req).
from engine.picks import (
    generate_picks, get_best_pick, match_odds, fetch_real_odds_for_games,
)

from ._tz import et_now, et_today_str
from ._bestbets import (
    _bb_progress_set, _bb_progress_increment, _bb_reset_on_exit,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _season() -> int:
    """Current MLB season year in ET. Recomputed each request so the
    January-1 boundary doesn't snap to last year on a UTC server."""
    return et_now().year


# ── Lazy proxies for helpers that still live in __init__.py ──
def _lazy(name):
    def proxy(*args, **kwargs):
        from backend import server as _srv
        return getattr(_srv, name)(*args, **kwargs)
    proxy.__name__ = name
    return proxy


_get_scoreboard = _lazy("_get_scoreboard")
_pred_cache_clear_sport = _lazy("_pred_cache_clear_sport")
_predict_mlb_full = _lazy("_predict_mlb_full")
_picks_store_get = _lazy("_picks_store_get")
_picks_store_put = _lazy("_picks_store_put")
_odds_from_scoreboard_cache = _lazy("_odds_from_scoreboard_cache")
_get_recorded_pick = _lazy("_get_recorded_pick")
_is_game_locked = _lazy("_is_game_locked")
_is_game_imminent = _lazy("_is_game_imminent")
_mlb_season_context = _lazy("_mlb_season_context")
_compute_injury_impact_pct = _lazy("_compute_injury_impact_pct")
_mlb_form_from_reasoning = _lazy("_mlb_form_from_reasoning")


class _LazyConst:
    """Proxy for a constant collection that lives in __init__.py.

    Mirrors routes_nba.py's _LazyConst — bare-name lookups inside
    handler bodies need the symbol bound at module load, but the real
    constant only exists after __init__.py finishes initialising."""
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


_MLB_DERIV_TYPES = _LazyConst("_MLB_DERIV_TYPES")


# ── Teams ─────────────────────────────────────────────────────

@router.get("/api/teams")
def api_teams():
    """Return all 30 MLB teams."""
    teams = get_all_teams()
    season = _season()
    result = []
    for t in teams:
        record = get_team_record(t["mlb_id"], season)
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
            "last_10": (
                f"{record.get('last_10_wins', 0)}-"
                f"{record.get('last_10_losses', 0)}"
            ) if record else "",
            "run_diff": record.get("run_diff", 0) if record else 0,
        })
    return result


@router.get("/api/teams/{team_id}")
def api_team_detail(team_id: int):
    """Return full team data with stats."""
    team = get_team_by_id(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")
    season = _season()
    record = get_team_record(team_id, season) or {}
    bp = get_bullpen(team_id, season) or {}
    recent = get_recent_games(team_id, 10)
    return {
        "team": team,
        "record": record,
        "bullpen": bp,
        "recent_games": recent,
    }


@router.get("/api/teams/{team_id}/profile")
def api_team_profile(team_id: int):
    """Full team profile with stats, recent games, and pitcher roster."""
    team = get_team_by_id(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")
    season = _season()
    record = get_team_record(team_id, season) or {}
    recent = get_recent_games(team_id, 15)
    bp = get_bullpen(team_id, season) or {}

    from engine.pit_stats import compute_team_stats_at_date
    today = et_today_str()
    pit = compute_team_stats_at_date(team_id, today, season)

    conn = get_conn()
    pitchers = conn.execute("""
        SELECT p.mlb_id, p.name, p.throws,
               ps.era, ps.whip, ps.k_per_9, ps.wins, ps.losses, ps.innings
        FROM players p
        LEFT JOIN pitcher_stats ps ON p.mlb_id = ps.player_id AND ps.season = ?
        WHERE p.team_id = ? AND p.position = 'P' AND p.active = 1
        ORDER BY ps.innings DESC
    """, (season, team_id)).fetchall()

    return {
        "team": team,
        "record": record,
        "pit_stats": pit,
        "bullpen": bp,
        "recent_games": recent,
        "pitchers": [dict(p) for p in pitchers],
    }


# ── Standings ─────────────────────────────────────────────────

@router.get("/api/standings")
def api_standings():
    """MLB standings grouped by division."""
    conn = get_conn()
    season = _season()

    team_count = conn.execute(
        "SELECT COUNT(*) as c FROM teams"
    ).fetchone()["c"]
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
    """, (season,)).fetchall()
    if not rows:
        return []

    divisions: dict[str, dict] = {}
    for r in rows:
        league = (r["league"] or "").strip()
        division = (r["division"] or "").strip()
        if not league or not division:
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


# ── Pitcher ───────────────────────────────────────────────────

@router.get("/api/pitcher/{pitcher_id}")
def api_pitcher(pitcher_id: int):
    """Pitcher stats + recent starts."""
    from engine.db import get_pitcher_recent_starts
    conn = get_conn()
    season = _season()
    player = conn.execute(
        "SELECT * FROM players WHERE mlb_id = ?", (pitcher_id,)
    ).fetchone()
    if not player:
        raise HTTPException(status_code=404, detail="Pitcher not found")
    stats = get_pitcher_season(pitcher_id, season)
    recent = get_pitcher_recent_starts(pitcher_id, 5)
    return {
        "player": dict(player),
        "stats": dict(stats) if stats else None,
        "recent_starts": recent,
    }


# ── Scoreboard ────────────────────────────────────────────────

@router.get("/api/scoreboard")
def api_scoreboard(date: str = Query(default="")):
    """Today's MLB games. Heavy work happens in __init__.py:_get_scoreboard."""
    return _get_scoreboard(date)


# ── Tracker (pick history / record / settle / summary) ────────

@router.get("/api/tracker/history")
def api_pick_history():
    """Recent main-tracker pick history with results.

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
    deriv_types = list(_MLB_DERIV_TYPES)
    placeholders = ",".join("?" * len(deriv_types))
    picks = conn.execute(
        f"SELECT * FROM picks WHERE bet_type NOT IN ({placeholders}) "
        f"ORDER BY created_at DESC LIMIT 50",
        tuple(deriv_types),
    ).fetchall()
    return [dict(p) for p in picks]


@router.post("/api/tracker/record")
def api_record_picks():
    """Record today's model picks."""
    try:
        from engine.tracker import record_picks
        picks = record_picks()
        return {"recorded": len(picks), "picks": picks}
    except Exception as e:
        logger.error("Record picks failed: %s", e, exc_info=True)
        return {"error": str(e), "recorded": 0}


@router.post("/api/tracker/settle")
def api_settle_picks():
    """Settle completed picks + POTD against final scores."""
    try:
        from engine.tracker import settle_picks
        result = settle_picks()
        try:
            from engine.pick_of_day import settle_potd
            settle_potd("mlb")
        except Exception as e:
            logger.warning("MLB POTD settle failed: %s", e)
        return result
    except Exception as e:
        logger.error("Settle picks failed: %s", e, exc_info=True)
        return {"error": str(e), "settled": 0}


@router.get("/api/tracker/summary")
def api_pick_summary():
    """Running pick totals."""
    from engine.tracker import get_pick_summary
    return get_pick_summary()


# ── Calibration (MLB-side) ────────────────────────────────────

@router.post("/api/calibrate")
def api_calibrate(days: int = Query(default=30)):
    """Run model self-calibration on recent games."""
    from engine.calibration import calibrate
    from engine.mlb_predict import reload_weights
    report = calibrate(days=days)
    reload_weights()  # Refresh cached weights
    return report


@router.post("/api/calibrate/teams")
def api_calibrate_teams():
    """Run per-team calibration."""
    from engine.team_calibration import calibrate_teams
    return calibrate_teams()


# ── Backtest ──────────────────────────────────────────────────

@router.get("/api/backtest")
def api_backtest(days: int = Query(default=0), min_edge: float = Query(default=3),
                  season: int = Query(default=0)):
    """Run model backtest against historical games."""
    from engine.backtest import run_backtest

    yr = season if season > 0 else None

    # Auto-load historical season data if requesting a past season we
    # haven't ingested yet. 100-game threshold = "this season has barely
    # any data" — fetch it before running the backtest.
    if yr:
        conn = get_conn()
        game_count = conn.execute(
            "SELECT COUNT(*) as c FROM games WHERE season = ? AND status = 'final'",
            (yr,)
        ).fetchone()["c"]
        if game_count < 100:
            logger.info("Loading %d season data for backtest...", yr)
            from scrapers.mlb_stats import fetch_teams, fetch_season_results
            fetch_teams()
            fetch_season_results(season=yr)

    results = run_backtest(
        season=yr,
        days=days if days > 0 else None,
        min_edge=min_edge,
    )
    results.pop("game_log", None)  # too large for the wire
    return results


# ── Lineup refresh (MLB) ──────────────────────────────────────

@router.post("/api/mlb/refresh-lineups")
def api_refresh_mlb_lineups(date: str | None = Query(None),
                              record: bool = Query(True)):
    """Re-check confirmed MLB lineups for `date` and invalidate picks
    whose lineup has changed. Day-of scratches and late lineup shuffles
    otherwise leave a stale pick on the card."""
    try:
        from engine.lineup_refresh import refresh_for_date
        result = refresh_for_date(date=date, record_picks=record)
        # Drop the in-process pred cache when picks_cache rows were
        # invalidated so the next best-bets call recomputes against the
        # confirmed lineup instead of returning the stale blob until TTL
        # expires.
        if (result.get("invalidated") or 0) > 0:
            cleared = _pred_cache_clear_sport("mlb")
            result["pred_cache_cleared"] = cleared
        return result
    except Exception as e:
        logger.error("lineup refresh failed: %s", e, exc_info=True)
        return {"error": str(e), "deltas": 0, "invalidated": 0}


# ── Debug ─────────────────────────────────────────────────────

@router.get("/api/debug/odds")
def api_debug_odds():
    """Test all odds sources."""
    result: dict = {}

    try:
        from scrapers.espn_odds import fetch_game_odds
        games = _get_scoreboard()
        if games:
            espn = fetch_game_odds(games[0].get("id"))
            result["espn"] = {
                "game": games[0].get("short_name", ""),
                "odds": espn,
            }
    except Exception as e:
        result["espn"] = {"error": str(e)}

    return result


@router.get("/api/debug/teams")
def api_debug_teams():
    """Dump raw team data to inspect league/division values."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT mlb_id, abbreviation, name, league, division FROM teams"
    ).fetchall()
    return [dict(r) for r in rows]


# ── Accuracy ──────────────────────────────────────────────────
# Cross-sport in spirit (sport= query param) but defaults to MLB and
# shipped with the MLB router historically.

@router.get("/api/accuracy")
def api_accuracy(sport: str = Query(default="mlb")):
    """Get prediction accuracy / calibration data."""
    try:
        from engine.accuracy import compute_calibration
        return compute_calibration(sport=sport)
    except Exception as e:
        logger.error("Accuracy computation failed: %s", e, exc_info=True)
        return {"error": str(e)}


# ── Predict ───────────────────────────────────────────────────

class PredictRequest(BaseModel):
    home_team_id: int
    away_team_id: int
    home_pitcher_id: int | None = None
    away_pitcher_id: int | None = None
    venue: str | None = None


@router.post("/api/predict")
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
    # would render on best-bets cards too.
    if result.get("season_context") is None:
        sc = _mlb_season_context(req.home_team_id, req.away_team_id)
        if sc:
            result["season_context"] = sc

    # Expose home_impact / away_impact on the injuries block so the
    # GameDetail Injuries card can render the "X% weaker" summary the
    # NHL version has.
    inj = result.get("injuries") or {}
    if isinstance(inj, dict):
        if inj.get("home_impact") is None:
            inj["home_impact"] = _compute_injury_impact_pct(inj.get("home") or [])
        if inj.get("away_impact") is None:
            inj["away_impact"] = _compute_injury_impact_pct(inj.get("away") or [])
        result["injuries"] = inj

    # Surface MLB direction-allow flags so the frontend's
    # PredictionResults / EdgeCallout can suppress recommendations for
    # disabled directions.
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
    # instead of silently dropping rows.
    try:
        from engine.model_overrides import list_overrides as _list_ov
        result["active_overrides"] = _list_ov("mlb")
    except Exception as e:
        logger.debug("active_overrides lookup failed: %s", e)
        result["active_overrides"] = []

    # Log component probabilities per market so engine.ensemble_auto_tune
    # can grade each signal against actual outcomes once the game ends
    # and produce tuned blend weights.
    try:
        from engine.prediction_log import log_signals as _log_signals
        from engine.db import get_conn as _gc
        mc = result.get("mc") or {}
        gbm = result.get("gbm") or {}
        if "error" in mc: mc = {}
        if "error" in gbm: gbm = {}
        # Look up the mlb_game_id for this matchup on today's slate so
        # the signal row can JOIN against games cleanly later.
        today = et_today_str()
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
    # as the Scoreboard best-bet badge and POTD selection.
    try:
        from engine.db import get_team_by_id as _get_team_by_id

        # Prefer picks store (single source of truth from best-bets)
        home_team = _get_team_by_id(req.home_team_id) or {}
        away_team = _get_team_by_id(req.away_team_id) or {}
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
        predict_date = et_today_str()
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


# ── Best Bets ─────────────────────────────────────────────────

def _bb_predict_one(game: dict, all_odds) -> dict | None:
    """Compute the full prediction + odds payload for a single game.

    Returns a dict with everything /api/best-bets needs (pred, h/a IDs,
    odds, abbreviations) or None if the game should be skipped (live,
    completed, missing IDs, prediction error).
    """
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
    # Merge HR's full scrape with whatever the scoreboard attached.
    matched = match_odds(h_abbr, a_abbr, all_odds) or {}
    game_odds = {**(game.get("odds") or {}), **matched}

    # Bypass the 30-min pred cache for games starting within 1hr — late
    # lineup scratches, weather, and line moves all matter most in the
    # final hour. Outside the 1hr window the cache stands.
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


@router.get("/api/best-bets")
@_bb_reset_on_exit("mlb")
def api_best_bets():
    """Run predictions on all today's games using the unified picks engine.

    Per-game predictions run in parallel (ThreadPoolExecutor) so cold
    dashboard load isn't N x serial MC sims. Progress is published via
    /api/best-bets/progress so the frontend can render a percentage."""
    import time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    target_date = et_today_str()
    games = _get_scoreboard()

    all_odds = fetch_real_odds_for_games()

    predictable = [
        g for g in games
        if g["home"].get("team_id") and g["away"].get("team_id")
        and g["status"].get("state", "pre") not in ("post", "in")
        and not g["status"].get("completed")
    ]

    _bb_progress_set(total=len(predictable), done=0, phase="predicting",
                      started_at=_time.time(), finished_at=None)
    logger.info("Best bets: analyzing %d games (parallel)", len(predictable))

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
            logger.error("  Pick generation failed for %s: %s",
                         game.get("short_name", "?"), e)
            continue

        # Don't drop the card when picks is empty: if the live picker
        # filtered out a previously-recorded pick (HR moved the line past
        # an edge ceiling, calibration shifted, etc.), the tracker's
        # recorded pick should still surface on the card. Only drop when
        # nothing was ever recorded for the matchup.
        matchup = f"{a_abbr} @ {h_abbr}"
        recorded_check = _get_recorded_pick("mlb", matchup, target_date)
        if not picks and not recorded_check:
            continue

        _picks_store_put("mlb", h_abbr, a_abbr, picks, game_odds)

        # Card + POTD must always agree on which pick is "best". To
        # guarantee that, both compute against CORE picks only —
        # derivatives flow exclusively through `derivative_picks` and
        # the dedicated tracker.
        deriv_types = list(_MLB_DERIV_TYPES)
        core_picks = [p for p in picks if p.get("type") not in deriv_types]
        # matchup already computed above for the empty-picks fallback;
        # reuse recorded_check so we don't query twice.
        recorded = recorded_check

        # Per-game lock: once the game is within 1 hour of tip-off,
        # freeze the card's best_pick to whatever the tracker recorded.
        # Pre-game line drift: if HR moved the line enough for the live
        # picker to fall below EDGE_SKIP, fall back to the recorded
        # tracker pick instead of showing "NO PICK".
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
        # Top derivative pick for the secondary card badge.
        deriv_sorted = sorted(
            (p for p in picks if p.get("type") in deriv_types
                and (p.get("confidence") or "lean") != "skip"),
            key=lambda p: -(p.get("edge") or 0),
        )
        best_derivative = deriv_sorted[0] if deriv_sorted else None
        if not best and not best_derivative:
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
                tr = pred.get("travel") or {}
                home_travel = tr.get("home", 1.0) or 1.0
                away_travel = tr.get("away", 1.0) or 1.0
                rest = {
                    "home_short_rest": home_travel < 0.97,
                    "away_short_rest": away_travel < 0.97,
                    "home_rest_advantage": home_travel > away_travel + 0.02,
                    "away_rest_advantage": away_travel > home_travel + 0.02,
                }
                try:
                    from engine.db import get_team_record as _gtr
                    from engine.mlb_predict import SEASON as _MLB_SEASON
                    h_rec = _gtr(home_id, _MLB_SEASON) or {}
                    a_rec = _gtr(away_id, _MLB_SEASON) or {}
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
                inj_data = pred.get("injuries") or {}
                injuries = {
                    "home_impact": _compute_injury_impact_pct(inj_data.get("home") or []),
                    "away_impact": _compute_injury_impact_pct(inj_data.get("away") or []),
                    "home_list": inj_data.get("home") or [],
                    "away_list": inj_data.get("away") or [],
                }
                season_context = pred.get("season_context") or _mlb_season_context(home_id, away_id)
        except Exception as e:
            logger.debug("  Enrich-best-bet failed for %s/%s: %s",
                         h_abbr, a_abbr, e)

        # all_picks = top 4 CORE picks (no derivatives) plus the 1st INN
        # top-up if outside top 4.
        all_picks = list(core_picks[:4])
        if not any(p.get("type") == "1st INN" for p in all_picks):
            for p in core_picks[4:]:
                if p.get("type") == "1st INN":
                    all_picks.append(p)
                    break

        derivative_picks = sorted(
            (p for p in picks if p.get("type") in deriv_types),
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
            "best_derivative": best_derivative,
            "all_picks": all_picks,
            "derivative_picks": derivative_picks,
            "confidence": (best.get("confidence", "lean") if best else "skip"),
            "win_prob": win_prob,
            "rest": rest,
            "factors": factors,
            "injuries": injuries,
            "season_context": season_context,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "recorded_pick": recorded,
            "is_locked": locked,
        })

    # Sort by best edge — cores rank above derivatives.
    def _sort_key(b):
        bp = b.get("best_pick")
        if bp:
            return (1, bp.get("edge", 0))
        bd = b.get("best_derivative")
        return (0, (bd or {}).get("edge", 0))
    bets.sort(key=_sort_key, reverse=True)

    # Reconcile tracker pending picks with the live model.
    try:
        from engine.tracker import refresh_pending_for_today
        diff = refresh_pending_for_today(bets, target_date)
        if any(diff.values()):
            logger.info("Tracker pending sync (mlb): %s", diff)
    except Exception as e:
        logger.warning("Tracker pending refresh failed: %s", e)

    # Piggyback record_picks on every /api/best-bets call so the
    # tracker stays current without depending on the cron alone.
    try:
        from engine.tracker import record_picks
        new_recorded = record_picks(date=target_date)
        if new_recorded:
            logger.info("Tracker recorded (mlb api hit): %d new pick(s) for %s",
                        len(new_recorded), target_date)
    except Exception as e:
        logger.warning("Tracker record_picks (mlb api hit) failed: %s", e)

    # Piggyback CLV capture on every games-endpoint hit so pending
    # picks get their closing odds stamped close to game time, not
    # from the once-or-twice-daily sync.
    try:
        from engine.tracker import capture_closing_odds as _mlb_cap
        n = _mlb_cap()
        if n:
            logger.info("MLB closing-odds capture (api hit): %d", n)
    except Exception as e:
        logger.warning("MLB closing-odds capture (api hit) failed: %s", e)

    # Paper-bet log for derivative markets — separate table so we can
    # evaluate derivative profitability in isolation.
    try:
        from engine.derivative_tracker import record_top_derivatives
        diff = record_top_derivatives("mlb", bets, n_per_game=1,
                                       min_edge=4.0, target_date=target_date)
        if diff.get("inserted") or diff.get("updated"):
            logger.info("Derivative tracker (mlb): %s", diff)
    except Exception as e:
        logger.warning("MLB derivative recorder failed: %s", e)

    # Pick-events breadcrumb log — drives the 📜 badge on BestBets cards.
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

    _bb_progress_set(phase="idle", finished_at=_time.time())
    return bets
