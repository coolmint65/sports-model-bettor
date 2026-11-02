"""Pick-of-the-Day routes (cross-sport).

Extracted 2026-05-02 from backend/server/__init__.py (#164). Covers the
public POTD lifecycle: get/create today's POTD, summary, settle,
recalc-profit, calibration refresh, delete.

Heavy callsites (api_best_bets / api_nhl_best_bets / api_nba_best_bets)
still live in __init__.py and are reached via lazy proxies. Tennis
slate fetch goes through the tennis router via lazy proxy too.
"""
from __future__ import annotations

import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from ._tz import et_today_str

logger = logging.getLogger(__name__)
router = APIRouter()

import time as _time

# POTD cold-load throttle (MintyBot 2026-10-15): /api/pick-of-day/{sport}
# regenerates the full live slate per request (api_nhl_best_bets ~20s, etc.).
# PotdPanel polls it repeatedly and, since the handler is a sync def, each poll
# holds a threadpool worker for the whole regen -> a few sports x a few pollers
# exhaust the pool and wedge the entire API (HTTP 000). This in-process throttle
# lets repeated polls serve the already-locked POTD row (a fast single-row read)
# instead of recomputing; the regen + line-movement refresh still runs, but at
# most once per window per (sport,view,tour).
_POTD_REFRESH_TS: dict = {}
_POTD_THROTTLE_SECS = 300

def _potd_fresh(key) -> bool:
    ts = _POTD_REFRESH_TS.get(key)
    return ts is not None and (_time.monotonic() - ts) < _POTD_THROTTLE_SECS

def _potd_mark(key) -> None:
    _POTD_REFRESH_TS[key] = _time.monotonic()


def _lazy(name, module="backend.server"):
    """Lazy-resolved proxy. ``module`` defaults to the package itself;
    routers extracted in #161/#164 expose their api functions from
    their own modules and need explicit module pointers."""
    def proxy(*args, **kwargs):
        import importlib
        mod = importlib.import_module(module)
        return getattr(mod, name)(*args, **kwargs)
    proxy.__name__ = name
    return proxy

api_best_bets = _lazy("api_best_bets", "backend.server.routes_mlb")
api_nhl_best_bets = _lazy("api_nhl_best_bets", "backend.server.routes_nhl")
api_nba_best_bets = _lazy("api_nba_best_bets", "backend.server.routes_nba")


def api_tennis_scheduled(*args, **kwargs):
    """Lazy bridge to the tennis router's slate endpoint. Imported on
    first call so loading routes_potd at app startup doesn't pull
    routes_tennis transitively (it would, but explicit avoids a cycle
    confusion when reading the module top)."""
    from .routes_tennis import api_tennis_scheduled as _real
    return _real(*args, **kwargs)


def _football_game_date(league: str, game_id) -> str | None:
    """Actual game date (YYYY-MM-DD) for a football POTD, so the cross-sport
    hero can filter to TODAY's slate. Football POTDs are forward-looking (the
    best UPCOMING pick), so an NFL POTD shown on a Tue is really a Sun game and
    must never headline the overall hero as if it were today. MintyBot 2026-10-06."""
    import os, sqlite3
    try:
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        p = os.path.join(root, "data", "football", f"{league}.db")
        if not os.path.exists(p):
            return None
        con = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        try:
            row = con.execute("SELECT date FROM games WHERE game_id=?", (str(game_id),)).fetchone()
        finally:
            con.close()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def _football_team_meta(league: str, game_id):
    """(home_logo, away_logo, home_name, away_name) for a football POTD. CFB has
    hundreds of teams and ESPN keys its logos by numeric id, so the frontend can
    build neither the logo URL nor the full name from the abbr — it relies on the
    payload (frontend teamLogo.js / PotdHero). Mirrors _football_game_date's
    read-only game-id lookup. MintyBot 2026-10-16."""
    import os, sqlite3
    try:
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        p = os.path.join(root, "data", "football", f"{league}.db")
        if not os.path.exists(p):
            return None, None, None, None
        con = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        # CFB reads better as the school name (location: "App State" / "Coastal
        # Carolina") than the full "School Mascot" name; NFL/UFL keep the full
        # name. Column is a fixed literal, not user input. (Austin 2026-10-16)
        _namecol = "location" if league == "cfb" else "name"
        try:
            row = con.execute(
                f"SELECT ht.logo_url, at.logo_url, ht.{_namecol}, at.{_namecol} FROM games g "
                "LEFT JOIN teams ht ON ht.id = g.home_team_id "
                "LEFT JOIN teams at ON at.id = g.away_team_id "
                "WHERE g.game_id=?", (str(game_id),)).fetchone()
        finally:
            con.close()
        return (row[0], row[1], row[2], row[3]) if row else (None, None, None, None)
    except Exception:
        return None, None, None, None


def _paper_potd_fallback(sport: str, tour: str | None = None):
    """Surface the sport tab's PAPER POTD on the cross-sport page for sports whose
    best-bets feed is empty (MLB rejects on calibration shrinkage; WNBA often has no
    clean best-bets edge). Returns the SAME pick the tab shows. Austin 2026-10-07.

    Tennis (PickMint tennis parity Phase 2/3, Austin 2026-10-10): tennis is
    100% on probation, so its best-bets feed is ALWAYS empty here — this is
    the only path that ever gives tennis a Pick of the Day, cross-sport page
    included. ``tour`` scopes it to ATP or WTA (defaults to 'atp', same
    default the main dispatcher below uses when the caller hasn't picked one)."""
    try:
        if sport == "mlb":
            from engine.mlb_paper_shadow import get_locked_potd as _glp, record_potd as _rp
            from engine.pick_of_day._select import et_today_str as _td
            _d = _td()
            try:
                _rp("full", _d)
            except Exception:
                pass
            return _glp("full", _d)
        if sport == "wnba":
            from engine.basketball._paper_potd import get_paper_potd as _gpp, record_paper_potd as _rpp
            try:
                _rpp("wnba", scope="full")
            except Exception:
                pass
            return _gpp("wnba", scope="full")
        if sport == "tennis":
            from engine.tennis_paper_potd import (
                get_tennis_paper_potd as _gtp, record_tennis_paper_potd as _rtp,
            )
            _tour = tour or "atp"
            try:
                _rtp(_tour)
            except Exception:
                pass
            return _gtp(_tour)
    except Exception as _e:
        logger.warning("paper POTD fallback failed for %s: %s", sport, _e)
    return None


@router.get("/api/pick-of-day/{sport}")
def api_pick_of_day(sport: str, view: str = Query(default="q1"),
                     tour: str | None = Query(default=None)):
    """Get today's Pick of the Day for a sport.

    NBA-only ``view`` query param: 'q1' (default) selects from Q1
    markets only, 'full' selects from full-game ML/SPREAD/TOTAL.
    'both' returns ``{q1: ..., full: ...}`` so the dashboard can
    flip between views without a second fetch. MLB/NHL ignore view.

    Tennis-only ``tour`` query param: 'atp' or 'wta' scopes both the
    bet candidates pulled from the tennis slate AND the per-tour POTD
    row (ATP and WTA can coexist on the same date). Defaults to 'atp'
    when sport=='tennis' and the caller hasn't specified — keeps
    legacy/CLI callers working.
    """
    from engine.pick_of_day import (
        get_or_create_potd, get_today_potd,
        refresh_potd_for_line_movement,
    )

    # EPL -> cross-sport POTD page (Austin 2026-10-12). Soccer already has a
    # full POTD at /api/soccer/{league}/potd via the SAME pick_of_day engine the
    # other POTD sports use; bridge it so PotdPanel's uniform
    # /api/pick-of-day/epl fetch works. eng_premier = the EPL league key. Returns
    # {message} during the international break / until HR posts the odds board,
    # which PotdPanel filters out like any other quiet sport.
    if sport == "epl":
        from backend.server.routes_soccer import api_soccer_potd
        return api_soccer_potd("eng_premier")

    # NHL P1 view (Austin 2026-09-22): a REAL first-period Pick-of-the-Day,
    # not the full-game POTD. Sourced from the P1-scoped derivative POTD
    # (Period* markets whose pick text starts "P1 "), locked in its own
    # sibling table + settled via the derivative tracker. Short-circuits
    # before the full-game POTD machinery below (which ignores view for NHL).
    if sport == "nhl" and view == "p1":
        from engine.derivative_tracker import (
            get_today_p1_potd, get_or_create_p1_potd, p1_potd_known_empty,
        )
        _no_pick = {"message": "No qualifying P1 pick today",
                    "sport": "nhl", "view": "p1"}
        # Fast paths first — a locked P1 pick, or a memoized "no pick today"
        # from an earlier generation (best-bets piggyback or a prior hit).
        # Only fall through to the ~20s api_nhl_best_bets() regeneration on a
        # genuine cold cache (e.g. right after a restart, before best-bets has
        # run). This stops the P1 tab hero spinning 20s on every poll.
        existing = get_today_p1_potd("nhl")
        if existing:
            return existing
        if p1_potd_known_empty("nhl"):
            return _no_pick
        bets = api_nhl_best_bets()
        if isinstance(bets, list):
            return get_or_create_p1_potd("nhl", bets) or _no_pick
        return {"error": "Could not generate bets"}

    # Cold-load fast path (MintyBot 2026-10-15): for the slate-regen sports with no
    # slate-derived finalization (nhl/mlb/tennis -- nba is preseason-gated + has a
    # both-view, wnba needs the slate for game_date), serve the locked POTD row
    # without the ~20s regen when it was refreshed within the throttle window. Kills
    # the per-poll recompute that was starving the sync-def threadpool.
    _tour_key = "atp" if (sport == "tennis" and not tour) else tour
    _ckey = (sport, view, _tour_key) if sport in ("nhl", "mlb", "tennis") else None
    if _ckey and view != "both" and _potd_fresh(_ckey):
        _cached = get_today_potd(sport, view=view, tour=_tour_key)
        if _cached:
            return _cached

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
        # Preseason gate (Austin 2026-10-07): NBA preseason POTDs carry
        # uncalibrated/fake edges (no preseason history to calibrate against), so
        # a preseason game must never headline the cross-sport POTD hero or lock
        # into the overall record. The 2026 regular season starts 2026-10-20;
        # gate every NBA POTD before then. Auto-ungates on that date (no manual
        # flip). nba_games is the legacy schema with no game_type column, so the
        # preseason test is date-based -- bump _NBA_REG_SEASON_START each season.
        _NBA_REG_SEASON_START = "2026-10-20"
        if et_today_str() < _NBA_REG_SEASON_START:
            return {"message": "NBA preseason - POTD gated until the regular season (Oct 20)",
                    "sport": "nba", "view": view, "preseason": True}
        bets = api_nba_best_bets()
    elif sport == "wnba":
        # WNBA full-game POTD: pull from the SAME calibrated slate the card uses
        # (api_basketball_today) so POTD can never surface a pick the card doesn't.
        # Added 2026-09-16 (MintyBot): the dispatch had no wnba branch, so
        # /api/pick-of-day/wnba errored → refresh_potd_for_line_movement never ran →
        # a pick locked early at ~04:00 stayed stale after the line moved and the
        # edge vanished (phantom "Mystics" vs the card's no-pick). This branch feeds
        # the current calibrated best_picks so the refresh + recompute self-correct.
        from backend.server.routes_basketball import api_basketball_today
        bets = []
        try:
            _slate = api_basketball_today("wnba")
            _wgames = (_slate.get("games") or []) if isinstance(_slate, dict) else []
        except Exception as _e:
            logger.warning("POTD wnba slate fetch failed: %s", _e)
            _wgames = []
        # game_id -> actual game date (YYYY-MM-DD), so the cross-sport panel can
        # hide this pick until its own game day the same way football's game_date
        # does. api_basketball_today FALLS FORWARD up to 7 days when the league
        # has no games today (or all are final) -- e.g. a WNBA Finals gap day --
        # and that `date` differs from et_today_str() without anything in the
        # locked POTD row saying so otherwise. MintyBot 2026-10-10 (PotdPanel
        # card-gate fix; same bug class as the NFL forward-looking POTD).
        _wnba_game_dates = {str(_g.get("game_id")): _g.get("date")
                            for _g in _wgames if _g.get("game_id") is not None and _g.get("date")}
        for _g in _wgames:
            if _g.get("status") != "scheduled":
                continue
            _bp = _g.get("best_pick")
            if not _bp:
                continue
            _bpn = dict(_bp)
            if "prob" not in _bpn and "model_prob" in _bpn:
                _bpn["prob"] = _bpn["model_prob"]
            _ha = _g.get("home_abbr") or "?"
            _aa = _g.get("away_abbr") or "?"
            bets.append({
                "game_id": _g.get("game_id"),
                "matchup": f"{_aa} @ {_ha}",
                "best_pick": _bpn,
                "home": {"name": _g.get("home_name") or _ha, "abbreviation": _ha},
                "away": {"name": _g.get("away_name") or _aa, "abbreviation": _aa},
                "time": _g.get("start_time") or "",
                "venue": "",
                "status": {"state": "pre"},
            })
    elif sport == "tennis":
        # Pull tennis matches from /api/tennis/scheduled (which now
        # embeds picks/odds/best_pick) and reshape to the team-sport
        # bet dict select_potd expects. Tennis picks ship with
        # `model_prob`; alias it to `prob` so the scorer's lookup
        # keys land. Synthetic home/away objects keep the matchup
        # validator happy — tennis doesn't have a real home/away.
        # Tour scoping: pass `tour` through so the slate fetch only
        # returns ATP or WTA matches; per-tour POTDs coexist for the
        # same date. Default to atp when caller didn't specify.
        if not tour:
            tour = "atp"
        try:
            tennis_payload = api_tennis_scheduled(date=None, days=2,
                                                    tour=tour, refresh=False)
            tennis_matches = tennis_payload.get("matches", []) if isinstance(tennis_payload, dict) else []
        except Exception as e:
            logger.warning("POTD tennis fetch failed: %s", e)
            tennis_matches = []
        bets = []
        for m in tennis_matches:
            bp = m.get("best_pick")
            if not bp:
                continue
            # Re-key model_prob → prob for the picker's expectation
            bp_norm = dict(bp)
            if "prob" not in bp_norm and "model_prob" in bp_norm:
                bp_norm["prob"] = bp_norm["model_prob"]
            # Last-name pseudo-abbreviations let the matchup validator
            # in select_potd's pick-name check work. Tennis pick text
            # uses the full player name ("Jannik Sinner"); the
            # validator splits matchup on " @ " and confirms one of
            # the resulting tokens appears in the pick text. With
            # "Sinner @ Fils" as the matchup, "Sinner" lands in
            # "Jannik Sinner" and the validator passes.
            p1 = m.get("p1_name") or ""
            p2 = m.get("p2_name") or ""
            p1_abbr = (p1.split(" ")[-1] if p1 else "?")
            p2_abbr = (p2.split(" ")[-1] if p2 else "?")
            bets.append({
                "game_id": m.get("match_id"),
                "matchup": f"{p1_abbr} @ {p2_abbr}",
                "best_pick": bp_norm,
                "home": {"name": p2, "abbreviation": p2_abbr},
                "away": {"name": p1, "abbreviation": p1_abbr},
                "time": m.get("start_time") or "",
                "venue": m.get("tournament") or "",
                "status": {"state": (m.get("status") or "pre")},
            })
        bets_list = bets
    elif sport in ("nfl", "cfb", "ufl"):
        # Football POTD is generated + locked by the football pipeline
        # (engine.football / pick_of_day); this endpoint just returns the
        # stored row — the /summary sibling already reads it, and there is
        # no bets_list to regenerate from here. view is inert for football
        # (only NBA has a _full table). unify Phase 1 (MintyBot 2026-09-28).
        potd = get_today_potd(sport, view=view)
        if isinstance(potd, dict) and potd.get("game_id"):
            _gd = _football_game_date(sport, potd.get("game_id"))
            if _gd:
                potd = {**potd, "game_date": _gd}
            # Team logos + full names in the payload — CFB can't be built from the
            # abbr (ESPN keys logos by numeric id; hundreds of teams). MintyBot 2026-10-16.
            _hl, _al, _hn, _an = _football_team_meta(sport, potd.get("game_id"))
            if _hl or _al:
                potd = {**potd, "home_logo": _hl, "away_logo": _al}
            if _hn or _an:
                potd = {**potd, "home_name": _hn, "away_name": _an}
        return potd or {"message": "No qualifying picks today", "sport": sport}
    else:
        return {"error": f"Unknown sport: {sport}"}
    if sport != "tennis":
        bets_list = bets if isinstance(bets, list) else None

    # Pick Authority league-segmentation (Austin 2026-10-22): edge_proven keys on
    # (sport, league, bet_type) now, not just (sport, bet_type). This dispatcher's
    # own "sport" is a page key that for wnba/tennis isn't the picks_unified sport
    # column -- resolve both explicitly rather than guessing inside select_potd.
    _ep_db_sport = "basketball" if sport == "wnba" else sport
    _ep_league = tour if (sport == "tennis" and tour) else sport

    if sport == "nba" and view == "both":
        out = {}
        for v in ("q1", "full"):
            try:
                refresh_potd_for_line_movement(sport, bets_list, view=v)
            except Exception as e:
                logger.warning("POTD line refresh failed for %s/%s: %s", sport, v, e)
            potd = get_today_potd(sport, view=v)
            if not potd and bets_list is not None:
                potd = get_or_create_potd(sport, bets_list, view=v,
                                          db_sport=_ep_db_sport, league=_ep_league)
            out[v] = potd or None
        return out

    try:
        refresh_potd_for_line_movement(sport, bets_list, view=view)
    except Exception as e:
        logger.warning("POTD line refresh failed for %s: %s", sport, e)
    if _ckey:
        _potd_mark(_ckey)

    # WNBA-only, no-op for every other sport: stamp game_date onto the returned
    # row from the slate-fetch lookup built above, same purpose as football's
    # _football_game_date. Guarded on sport == "wnba" so _wnba_game_dates (only
    # ever assigned in the wnba branch above) is never referenced otherwise.
    def _attach_wnba_game_date(p):
        if sport == "wnba" and isinstance(p, dict) and p.get("game_id") and not p.get("game_date"):
            _gd = _wnba_game_dates.get(str(p.get("game_id")))
            if _gd:
                return {**p, "game_date": _gd}
        return p

    # Single-view path. Tennis adds a `tour` dimension so ATP and WTA
    # POTDs coexist; non-tennis ignores tour.
    potd = get_today_potd(sport, view=view, tour=tour)
    if potd:
        return _attach_wnba_game_date(potd)

    if bets_list is not None:
        potd = get_or_create_potd(sport, bets_list, view=view, tour=tour,
                                  db_sport=_ep_db_sport, league=_ep_league)
        if potd:
            return _attach_wnba_game_date(potd)
        return _paper_potd_fallback(sport, tour=tour) or {"message": "No qualifying picks today", "sport": sport}
    return _paper_potd_fallback(sport, tour=tour) or {"error": "Could not generate bets"}


@router.get("/api/pick-of-day/{sport}/summary")
def api_potd_summary(sport: str, scope: str | None = Query(None),
                     view: str | None = Query(None)):
    """Get POTD running totals.

    Auto-settles pending POTDs before returning the summary so the
    hero shown on the dashboard reflects finals without requiring the
    user to click "Settle Completed" on the Pick Tracker tab. settle_potd
    is a no-op when nothing is pending, so the cost is a single SQLite
    round-trip on typical calls.

    NHL ``view=p1`` (Austin 2026-09-22): return the P1 derivative POTD's
    running record instead of the full-game POTD's, so the P1 tab hero
    shows its own W/L/profit. Backfills settled results from the deriv
    tracker first (the P1 lock table's own result columns lag)."""
    if sport == "nhl" and view == "p1":
        from engine.derivative_tracker import (
            get_p1_potd_summary, backfill_p1_potd_results,
        )
        try:
            backfill_p1_potd_results("nhl")
        except Exception as e:
            logger.warning("P1 POTD backfill failed: %s", e)
        try:
            return get_p1_potd_summary("nhl")
        except Exception:
            return {"total": 0, "wins": 0, "losses": 0, "pushes": 0,
                    "pending": 0, "profit": 0, "win_pct": 0.0, "roi": 0.0}
    from engine.pick_of_day import get_potd_summary, settle_potd
    try:
        settle_potd(sport)
    except Exception as e:
        logger.warning("POTD auto-settle failed for %s: %s", sport, e)
    try:
        return get_potd_summary(sport, scope=scope)
    except ValueError:
        # Unknown / not-yet-registered sport key -> empty record, not a 500.
        return {"total": 0, "wins": 0, "losses": 0, "pushes": 0, "pending": 0,
                "profit": 0, "win_pct": 0.0, "roi": 0.0, "recent": []}


@router.post("/api/pick-of-day/{sport}/settle")
def api_potd_settle(sport: str):
    """Settle completed POTDs."""
    from engine.pick_of_day import settle_potd
    return settle_potd(sport)


@router.post("/api/pick-of-day/{sport}/recalc-profit")
def api_potd_recalc_profit(sport: str):
    """Rewrite profit on every settled POTD row using the current
    $100-unit formula. Run this once after a unit-sizing change to
    realign rows that were persisted under an older formula (e.g.
    Kelly dollarization against a $5000 bankroll)."""
    from engine.pick_of_day import recalc_potd_profit
    return recalc_potd_profit(sport)


@router.post("/api/calibration/refresh")
def api_calibration_refresh(sport: str | None = None):
    """Rebuild the empirical + blend calibration tables from the
    current tracker data without restarting the server.

    Reloads both layers in lockstep — the running server caches the
    blend curve in-process, so a fit_sport via CLI doesn't propagate
    until the next process restart unless we call reload() here too.
    Without the blend reload, raising MIN_SAMPLES (or any other math
    change) silently keeps the stale Platt curve in memory and the
    new behavior never kicks in.

    No sport -> refresh all three (mlb, nhl, nba).
    """
    from engine.empirical_calibration import (
        refresh_calibration, refresh_all_sports,
    )
    from engine import blend_calibration as _bc
    if sport:
        out = refresh_calibration(sport)
        try:
            _bc.fit_sport(sport)
            _bc.reload()
        except Exception as e:
            logger.warning("blend_cal reload (%s) failed: %s", sport, e)
        return out
    out = refresh_all_sports()
    try:
        _bc.fit_all_sports()
        _bc.reload()
    except Exception as e:
        logger.warning("blend_cal reload (all) failed: %s", e)
    return out


@router.delete("/api/pick-of-day/{sport}")
def api_potd_reset(sport: str):
    """Delete today's POTD so it regenerates on next request."""
    from engine.pick_of_day import _ensure_potd_table, _get_conn
    from datetime import datetime
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    today = et_today_str()
    conn.execute("DELETE FROM pick_of_day WHERE date = ?", (today,))
    conn.commit()
    return {"status": "cleared", "date": today, "sport": sport}


# Overall POTD (cross-sport) - Austin 2026-10-06: the single best POTD across ALL sports
# has its OWN record (daily max-edge pick's W/L + odds-based $1 P/L), not any one league's.
# Distinct path (/api/potd-overall/...) so /pick-of-day/{sport} doesn't catch sport="overall".
import os as _os, sqlite3 as _sq
_POTD_DBS = {"mlb": "mlb.db", "nba": "nba.db", "nhl": "nhl.db", "tennis": "tennis.db",
             "wnba": "basketball/wnba.db", "nfl": "football/nfl.db", "cfb": "football/cfb.db"}
_DATA_DIR = _os.path.join(_os.path.dirname(__file__), "..", "..", "data")

def _odds_profit(o):
    o = float(o)
    return o / 100.0 if o > 0 else 100.0 / abs(o)

@router.get("/api/potd-overall/summary")
def api_overall_potd_summary() -> dict:
    """Record of the daily OVERALL best POTD (max edge across all sports)."""
    by_date = {}
    by_date_clv = {}
    for sport, rel in _POTD_DBS.items():
        p = _os.path.join(_DATA_DIR, rel)
        if not _os.path.exists(p):
            continue
        try:
            c = _sq.connect("file:" + p + "?mode=ro", uri=True)
            rows = c.execute("SELECT date, edge, result, odds, closing_odds FROM pick_of_day "
                             "WHERE edge IS NOT NULL AND odds IS NOT NULL").fetchall()
            c.close()
        except Exception:
            continue
        for date, edge, result, odds, closing in rows:
            # Skip NBA preseason (2026-10-01..2026-10-19): uncalibrated fake edges
            # must not pollute the overall hero record. June playoffs (pre-October)
            # and Oct-20+ regular season are unaffected. (Austin 2026-10-07)
            if sport == "nba" and "2026-10-01" <= str(date) < "2026-10-20":
                continue
            e = float(edge or 0)
            # W/L + profit: settled heroes only (the record stays unchanged).
            if result in ("W", "L"):
                if date not in by_date or e > by_date[date][0]:
                    by_date[date] = (e, result, odds)
            # CLV: max-edge hero per date WITH a captured closing line (settled
            # OR pending - CLV is known at the close, before the result).
            if closing is not None:
                if date not in by_date_clv or e > by_date_clv[date][0]:
                    by_date_clv[date] = (e, odds, closing)
    wins = sum(1 for v in by_date.values() if v[1] == "W")
    losses = sum(1 for v in by_date.values() if v[1] == "L")
    profit = sum((_odds_profit(v[2]) if v[1] == "W" else -1.0) for v in by_date.values())
    total = wins + losses
    # CLV% = impliedProb(closing) - impliedProb(entry), averaged over heroes with
    # a captured close. Forward-only: accrues as new POTDs close + settle.
    def _imp(o):
        o = float(o)
        return (abs(o) / (abs(o) + 100.0)) if o < 0 else (100.0 / (o + 100.0))
    clv_pts = [100.0 * (_imp(cl) - _imp(en)) for (_e, en, cl) in by_date_clv.values()]
    clv_n = len(clv_pts)
    clv_avg = round(sum(clv_pts) / clv_n, 2) if clv_n else None
    return {"wins": wins, "losses": losses, "profit": round(profit, 2), "total": total,
            "win_pct": round(100 * wins / total, 1) if total else 0.0,
            "roi": round(100 * profit / total, 1) if total else 0.0,
            "clv_avg": clv_avg, "clv_n": clv_n}
