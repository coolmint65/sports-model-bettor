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
    else:
        return {"error": f"Unknown sport: {sport}"}
    if sport != "tennis":
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

    # Single-view path. Tennis adds a `tour` dimension so ATP and WTA
    # POTDs coexist; non-tennis ignores tour.
    potd = get_today_potd(sport, view=view, tour=tour)
    if potd:
        return potd

    if bets_list is not None:
        potd = get_or_create_potd(sport, bets_list, view=view, tour=tour)
        return potd or {"message": "No qualifying picks today", "sport": sport}
    return {"error": "Could not generate bets"}


@router.get("/api/pick-of-day/{sport}/summary")
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
