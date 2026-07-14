"""Player-props + derivative-tracker + first-inning + POTD-derivative routes.

Extracted 2026-05-02 from backend/server/__init__.py (#164). The block
covers Phase 2f-iii (player props), the derivative tracker / POTD
endpoints, and MLB first-inning POTD because they share the same
heavy-load characteristics: each fetches a per-sport snapshot on demand
and writes back to its own per-sport tracker.

Heavy callsites (api_*_best_bets) are still in __init__.py and accessed
via lazy proxies — same pattern as routes_nba / routes_nhl.
"""
from __future__ import annotations

import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from ._tz import et_today_str

logger = logging.getLogger(__name__)
router = APIRouter()


def _lazy(name, module="backend.server"):
    """Lazy-resolved proxy. ``module`` defaults to the package itself
    (where the older callsites still live); per-sport routers were
    extracted in #161/#164, so cross-router callers must point at the
    new home (``backend.server.routes_nba`` etc.)."""
    def proxy(*args, **kwargs):
        import importlib
        mod = importlib.import_module(module)
        return getattr(mod, name)(*args, **kwargs)
    proxy.__name__ = name
    return proxy

# Best-bets functions called by the derivative + POTD endpoints to
# materialize today's slate before deriving the props/derivative picks.
api_best_bets = _lazy("api_best_bets")
api_nhl_best_bets = _lazy("api_nhl_best_bets", "backend.server.routes_nhl")
api_nba_best_bets = _lazy("api_nba_best_bets", "backend.server.routes_nba")


# ── Player props (Phase 2f-iii) ──────────────────────────────
# Props are heavy (4k+ rows for MLB) and only relevant on the Props
# tab — never load them in the cold best-bets path. The tab fetches
# /api/{sport}/props/today on demand. Today only MLB is wired; NBA
# and NHL get their own scraper extensions in 2h / 2i.

@router.get("/api/{sport}/props/today")
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


@router.get("/api/{sport}/props-tracker/summary")
def api_props_summary(sport: str):
    """Settled prop-pick history + pending list. Mirrors the derivative
    summary endpoint shape so the same PicksTable shell renders both.

    Pending and settled are queried separately so a heavy pending slate
    (250+ props on a busy night) can't crowd out the settled history
    that powers the P/L banner totals.
    """
    if sport not in ("mlb", "nhl", "nba", "wnba"):
        raise HTTPException(status_code=400, detail="Unknown sport")
    from engine.player_props_tracker import settle_player_props
    from engine.player_props_db import list_picks, _conn_for
    # Auto-settle any pending whose game logs have landed before
    # returning the summary — same pattern as /derivative-tracker/summary.
    try:
        settle_player_props(sport)
    except Exception as e:
        logger.warning("player_props auto-settle (%s) failed: %s", sport, e)

    # Show only picks dated up to today (user's local). The picker
    # writes tomorrow's slate ahead of time so the model has it ready
    # at midnight, but the user doesn't want to see those picks in
    # the tracker until the day actually flips. Surfaced 2026-04-28.
    today_local = et_today_str()
    pending_all = list_picks(sport, pending_only=True, limit=10_000)
    pending = [p for p in pending_all if (p.get("date") or "") <= today_local]

    # Display cap: even if stale picks linger between sync runs (the
    # picker enforces top-3 at gen-time but doesn't re-void from
    # previous sessions), surface only the top 3 per (date) by edge.
    # Mirrors TOP_N_PER_SPORT in the prop pickers — same number, same
    # rationale (user directive: 1-3 highest-conviction per sport per
    # night). The DB stays as-is so settle still works on every row.
    DISPLAY_CAP_PER_DATE = 3
    by_date: dict[str, list] = {}
    for p in pending:
        by_date.setdefault(p.get("date") or "", []).append(p)
    capped: list = []
    for d, picks_for_date in by_date.items():
        ranked = sorted(picks_for_date,
                        key=lambda p: float(p.get("edge") or 0.0),
                        reverse=True)
        capped.extend(ranked[:DISPLAY_CAP_PER_DATE])
    pending = capped
    # Settled: pull a generous window so the banner P/L reflects the
    # full history, not just the newest 200 rows. Use a direct query
    # rather than list_picks(limit=200) which bundles pending in.
    conn = _conn_for(sport)
    settled_rows = conn.execute(
        "SELECT * FROM player_props_picks WHERE result IS NOT NULL "
        "ORDER BY date DESC, id DESC LIMIT 1000"
    ).fetchall()
    # Stake-weighted profit + ROI. Each row contributes profit*stake;
    # ROI denom is sum(stake_units) for W/L. NULL → 1.0u legacy default.
    finished = []
    profit = 0.0
    stake_settled = 0.0
    for r in settled_rows:
        d = dict(r)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
            profit += d["profit"]
        if d.get("result") in ("W", "L"):
            stake_settled += stake_u
        finished.append(d)
    wins   = sum(1 for p in finished if p["result"] == "W")
    losses = sum(1 for p in finished if p["result"] == "L")
    pushes = sum(1 for p in finished if p["result"] == "P")
    return {
        "wins": wins, "losses": losses, "pushes": pushes,
        "total": wins + losses + pushes,
        "win_pct": round(wins / (wins + losses) * 100, 1) if (wins + losses) else 0.0,
        "profit": round(profit, 2),
        "roi": round(profit / stake_settled, 1) if stake_settled else 0.0,
        "pending": pending,
        "history": finished,
    }


@router.post("/api/{sport}/props-tracker/settle")
def api_props_settle(sport: str):
    if sport not in ("mlb", "nhl", "nba", "wnba"):
        raise HTTPException(status_code=400, detail="Unknown sport")
    from engine.player_props_tracker import settle_player_props
    return settle_player_props(sport)


@router.get("/api/mlb/first-inning-tracker/summary")
def api_first_inning_summary():
    """Settled + pending 1st INN (NRFI/YRFI) picks, MLB only.
    Mirrors the derivative-tracker summary shape so the same
    PicksTable shell can render it."""
    from engine.db import get_conn as _mlb_conn
    conn = _mlb_conn()
    raw_rows = [dict(r) for r in conn.execute(
        "SELECT * FROM picks WHERE bet_type = '1st INN' "
        "ORDER BY date DESC, id DESC LIMIT 200"
    ).fetchall()]
    # Stake-weighted profit (mirrors per-sport trackers). Project
    # per-row profit before splitting pending/settled so the table
    # column matches the totals.
    rows = []
    for r in raw_rows:
        stake_u = r["stake_units"] if r.get("stake_units") is not None else 1.0
        if r.get("profit") is not None:
            r["profit"] = round(r["profit"] * stake_u, 2)
        rows.append(r)
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


@router.get("/api/mlb/first-inning-pick-of-day")
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


@router.get("/api/{sport}/player-props/potd")
def api_player_props_potd(sport: str):
    """Today's player-prop Pick-of-the-Day. Locks on first call;
    subsequent calls return the locked POTD even if the slate
    re-ranks. Returns the full pick row joined with POTD metadata,
    or {message} when no qualifying pick exists today."""
    if sport not in ("mlb", "nhl", "nba", "wnba"):
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


@router.get("/api/{sport}/derivative-pick-of-day/summary")
def api_derivative_potd_summary(sport: str):
    """Aggregate W/L/profit across the derivative POTD lock table.
    Same shape as /api/pick-of-day/{sport}/summary so the frontend
    PotdHero can show a record block on the derivative POTD card
    consistent with the core POTD."""
    if sport not in ("mlb", "nhl", "nba", "wnba"):
        raise HTTPException(status_code=400, detail="Unknown sport")
    from engine.derivative_tracker import get_derivative_potd_summary
    return get_derivative_potd_summary(sport)


@router.get("/api/{sport}/player-props/potd/summary")
def api_player_props_potd_summary(sport: str):
    """Aggregate W/L/profit across the prop POTD lock table joined to
    the props picks for results. Lets the prop POTD card show its
    record block consistent with core POTD."""
    if sport not in ("mlb", "nhl", "nba", "wnba"):
        raise HTTPException(status_code=400, detail="Unknown sport")
    from engine.player_props_potd import get_potd_summary
    return get_potd_summary(sport)


@router.get("/api/{sport}/derivative-pick-of-day")
def api_derivative_potd(sport: str):
    """Today's derivative POTD per sport. Locks on first call (uses
    today's best-bets to select); subsequent calls return the locked
    pick from the per-sport derivative_pick_of_day table."""
    if sport not in ("mlb", "nhl", "nba", "wnba"):
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
