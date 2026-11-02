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


_L10_CACHE: dict = {}
_L10_TTL_S = 600.0


def _last10_form(sport, player_id, bet_type, line, pick_or_side):
    """10-min memo over _last10_form_uncached (speed pass 2026-10-28): the summary route asks
    for every pending AND settled pick (~360 on NBA) on each load, one log query + 10 JSON
    parses apiece, ~1 s a request. New games land at most a few times a day."""
    import time as _t
    key = (sport, player_id, bet_type, line, pick_or_side)
    hit = _L10_CACHE.get(key)
    now = _t.monotonic()
    if hit is not None and now - hit[0] < _L10_TTL_S:
        return hit[1]
    val = _last10_form_uncached(sport, player_id, bet_type, line, pick_or_side)
    if len(_L10_CACHE) > 20000:
        _L10_CACHE.clear()
    _L10_CACHE[key] = (now, val)
    return val


def _last10_form_uncached(sport, player_id, bet_type, line, pick_or_side):
    """Last up-to-10 games of recent form for a player-prop pick, feeding
    the frontend sparkline + hit rate. Shape is FROZEN (the FE depends on it):

        {"values": [oldest..newest], "hits": <int>, "n": <int>}

    ``values`` are the player's values for THIS pick's stat over their most
    recent up-to-10 games they actually played, ordered oldest->newest.
    ``hits`` counts how many would have WON the pick (Over/Yes: value > line;
    Under/No: value < line; value == line is a push, NOT a hit). Reuses the
    tracker's stat-key map, composite handling and win rule, and mirrors the
    NHL toi_min>0 "played" filter. NEVER raises -- any missing map / logs /
    stat yields {"values": [], "hits": 0, "n": 0}."""
    empty = {"values": [], "hits": 0, "n": 0}
    try:
        if player_id is None:
            return empty
        import json
        from engine.player_props_db import _conn_for
        from engine.player_props_tracker import (
            _stat_key_for, _determine_outcome, _COMPOSITE_KEYS,
        )
        stat_key = _stat_key_for(sport, bet_type or "")
        if not stat_key:
            return empty
        # Side: prefer an explicit Over/Under/Yes/No, else parse it out of
        # the pick text ("Skubal Over 7.5"). A yes/no "occurred" market
        # (Anytime Goal/TD -- pick is NOT a numeric "Over/Under N") is graded
        # separately below (value >= 1), so only numeric O/U needs a line.
        side = (pick_or_side or "").strip().lower()
        if side not in ("over", "under", "yes", "no"):
            if "under" in side:
                side = "under"
            elif "over" in side:
                side = "over"
        numeric_ou = side in ("over", "under")
        line_f = None
        if numeric_ou:
            if line is None:
                return empty
            try:
                line_f = float(line)
            except (TypeError, ValueError):
                return empty
        if sport == "nfl" and stat_key in ("pass_yds", "rush_yds", "rec_yds", "receptions"):
            # NFL yards/receptions live in player_prop_logs (nflverse, keyed by
            # espn_id; zero rows for snaps-without-touches), not player_game_logs.
            from engine.sports.nfl.prop_model import last10 as _nfl_last10
            values = _nfl_last10(int(player_id), stat_key)
            hits = (sum(1 for v in values if _determine_outcome(line_f, side, v) == "W")
                    if numeric_ou else 0)
            return {"values": values, "hits": int(hits), "n": len(values)}
        components = _COMPOSITE_KEYS.get(stat_key)
        conn = _conn_for(sport)
        rows = conn.execute(
            "SELECT stats_json FROM player_game_logs "
            "WHERE player_id = ? ORDER BY date DESC, id DESC LIMIT 50",
            (int(player_id),),
        ).fetchall()
        newest_first = []
        for r in rows:
            if len(newest_first) >= 10:
                break
            try:
                stats = json.loads(r["stats_json"] or "{}")
            except (ValueError, TypeError):
                continue
            # NHL "played" filter (mirror nhl.player_mc._player_observations):
            # the player must have taken the ice in this game.
            if sport == "nhl" and not (stats.get("toi_min", 0) > 0):
                continue
            if components:
                # Composite stat (NBA PRA = pts+reb+ast, NHL Points = g+a):
                # require every base component, like _lookup_actual.
                total = 0.0
                ok = True
                for c in components:
                    cv = stats.get(c)
                    if cv is None:
                        ok = False
                        break
                    try:
                        total += float(cv)
                    except (TypeError, ValueError):
                        ok = False
                        break
                if not ok:
                    continue
                newest_first.append(total)
            else:
                raw = stats.get(stat_key)
                if raw is None:
                    continue
                try:
                    newest_first.append(float(raw))
                except (TypeError, ValueError):
                    continue
        values = list(reversed(newest_first))  # oldest -> newest
        if numeric_ou:
            # numeric Over/Under N -- unchanged tracker win rule.
            hits = sum(1 for v in values
                       if _determine_outcome(line_f, side, v) == "W")
        elif side == "no":
            # yes/no "occurred" market, No side: hit = it did NOT occur.
            hits = sum(1 for v in values if v < 1)
        else:
            # yes/no "occurred" market (Anytime Goal/TD), Yes / default:
            # hit = the event happened at least once that game (value >= 1).
            hits = sum(1 for v in values if v >= 1)
        return {"values": values, "hits": int(hits), "n": len(values)}
    except Exception:
        return empty


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
api_best_bets = _lazy("api_best_bets", "backend.server.routes_mlb")   # moved out of the package (#161)
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


_PROPS_SETTLE_LAST: dict = {}
_PROPS_SETTLE_EVERY_S = 120.0


def _kick_props_settle(sport, settle):
    import threading
    import time as _t
    now = _t.monotonic()
    if now - _PROPS_SETTLE_LAST.get(sport, -1e9) < _PROPS_SETTLE_EVERY_S:
        return
    _PROPS_SETTLE_LAST[sport] = now

    def _bg():
        try:
            settle(sport)
        except Exception as e:
            logger.warning("player_props auto-settle (%s) failed: %s", sport, e)

    threading.Thread(target=_bg, daemon=True).start()


# Record reset (Austin 2026-10-29, "these records are from before the rework"): the Player Props
# tracker counts only picks from the current model on. NBA = props v2 (shipped 10-26; the
# April-May rows are the old picker). NHL = the 10-26 evening rework (edge fix, early card lock,
# weekly blend refit). Older rows stay in the table untouched -- only the tracker view skips them.
_PROPS_SINCE = {"nba": "2026-10-26", "nhl": "2026-10-27"}


# Suggested stake per pick (the card's stake chip). Lives in engine.player_props_stake so the
# props POTD shows + grades the same stake (2026-11-01); tiers + rationale documented there.
from engine.player_props_stake import stake_tier as _stake_tier  # noqa: E402


@router.get("/api/{sport}/props-tracker/summary")
def api_props_summary(sport: str):
    """Settled prop-pick history + pending list. Mirrors the derivative
    summary endpoint shape so the same PicksTable shell renders both.

    Pending and settled are queried separately so a heavy pending slate
    (250+ props on a busy night) can't crowd out the settled history
    that powers the P/L banner totals.
    """
    if sport not in ("mlb", "nhl", "nba", "wnba", "nfl"):
        raise HTTPException(status_code=400, detail="Unknown sport")
    from engine.player_props_tracker import settle_player_props
    from engine.player_props_db import list_picks, _conn_for
    # Auto-settle any pending whose game logs have landed -- same pattern as
    # /derivative-tracker/summary, but in the background and at most every 2 min per sport
    # (speed pass 2026-10-28: inline it added ~4-5 s to every load, mostly an ESPN fetch).
    # The worker's settle loop grades these too; this just keeps a fresh result close.
    _kick_props_settle(sport, settle_player_props)

    # Show only picks dated up to today (user's local). The picker
    # writes tomorrow's slate ahead of time so the model has it ready
    # at midnight, but the user doesn't want to see those picks in
    # the tracker until the day actually flips. Surfaced 2026-04-28.
    # NHL and NBA (10-29) are the exception: their cards lock the first time HR prices a
    # game, usually the evening before (Austin 2026-10-26, "lock them as
    # soon as"), so the early line is only worth anything if the locked
    # pick shows right away. The card shows the game date on those.
    today_local = et_today_str()
    pending_all = list_picks(sport, pending_only=True, limit=10_000)
    # Anytime Goal/TD have their OWN tab — keep them out of the Player Props
    # tab's slate + history (they share the player_props_picks table). (Austin 2026-09-29)
    _ANYTIME_BT = ("Anytime Goal", "Anytime TD")
    since = _PROPS_SINCE.get(sport, "")
    pending = [p for p in pending_all
               if ((p.get("date") or "") <= today_local or sport in ("nhl", "nba"))
               and (p.get("date") or "") >= since
               and (p.get("bet_type") not in _ANYTIME_BT)]

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
    # Attach last-10 recent form (sparkline + hit rate) to each pending pick.
    for _p in pending:
        _p["stake"] = _stake_tier(sport, _p)
        _p["l10"] = _last10_form(sport, _p.get("player_id"), _p.get("bet_type"),
                                 _p.get("line"), _p.get("side") or _p.get("pick"))
    # Settled: pull a generous window so the banner P/L reflects the
    # full history, not just the newest 200 rows. Use a direct query
    # rather than list_picks(limit=200) which bundles pending in.
    conn = _conn_for(sport)
    settled_rows = conn.execute(
        "SELECT * FROM player_props_picks WHERE result IS NOT NULL "
        "AND bet_type NOT IN ('Anytime Goal', 'Anytime TD') AND date >= ? "
        "ORDER BY date DESC, id DESC LIMIT 1000", (since,)
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
        d["l10"] = _last10_form(sport, d.get("player_id"), d.get("bet_type"),
                               d.get("line"), d.get("side") or d.get("pick"))
        finished.append(d)
    wins   = sum(1 for p in finished if p["result"] == "W")
    losses = sum(1 for p in finished if p["result"] == "L")
    pushes = sum(1 for p in finished if p["result"] == "P")
    card_gate = None
    if sport == "nhl":          # NHL card gate (MintyBot 2026-11-01): why the card is empty / what's proven
        try:
            from engine.sports.nhl.card_gate import summary as _gate_summary
            card_gate = _gate_summary()
        except Exception as e:
            card_gate = {"error": str(e)}
    elif sport in ("nba", "nfl"):   # eval-log gate (MintyBot 2026-11-02): shadow record of every graded main line
        try:
            from importlib import import_module as _imp_mod
            card_gate = dict(_imp_mod(f"engine.sports.{sport}.prop_picks").card_gate(), kind="eval_log")
        except Exception as e:
            card_gate = {"error": str(e)}
    return {
        "card_gate": card_gate,
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
    if sport not in ("mlb", "nhl", "nba", "wnba", "nfl"):
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
    if sport not in ("mlb", "nhl", "nba", "wnba", "nfl"):
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
    potd["l10"] = _last10_form(sport, potd.get("player_id"), potd.get("bet_type"),
                               potd.get("line"), potd.get("side") or potd.get("pick"))
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
    if sport not in ("mlb", "nhl", "nba", "wnba", "nfl"):
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


@router.get("/api/{sport}/player-props/today")
def api_nfl_player_props_today(sport: str):
    """NFL player props (passing/rushing/receiving yards + receptions) priced
    vs Hard Rock's alt-line thresholds (PAPER). NHL player props (Skater SOG /
    Goals / Assists, Goalie Saves) ride a separate generator with a
    record-then-read contract -- see the nhl branch below. Other sports
    return an empty coming-soon slate."""
    if sport == "nhl":
        # engine.sports.nhl.prop_picks.generate_picks() is RECORD-ONLY: it
        # fetches HR's live NHL prop board, scores it, and INSERTs today's
        # rows into player_props_picks as a side effect, returning a bare
        # {evaluated, picked, ...} counts dict -- not a {picks:[...]} display
        # payload like NFL's generate_picks(). Generation itself is piggybacked
        # onto the NHL best-bets refresh cycle (routes_nhl.py, near
        # record_top_derivatives) so this GET stays a cheap read: just pull
        # back whatever's already been recorded for today via the generic
        # (sport-parameterized) player_props_db reader. (Austin 2026-10-01)
        try:
            from engine.player_props_db import list_picks
            raw = list_picks("nhl", date=et_today_str())
            # player_props_picks is apparently ALSO written by the Anytime
            # Goal pipeline (bet_type='Anytime Goal', confidence NULL/'skip'
            # for every row observed 2026-10-01 -- it has its own tab/feed,
            # NHLAnytimeGoalView + /api/nhl/anytime-goal/today, so those rows
            # don't belong on the PLAYER PROPS tab even when actionable).
            # Mirrors the confidence!='skip' filter used elsewhere in this
            # codebase (e.g. best_derivative in routes_nhl.py) to keep
            # evaluated-but-rejected rows off the display. Confirm whether
            # Anytime Goal rows can ever carry a non-skip confidence before
            # relying on this filter alone to fully separate the two tabs.
            picks = [p for p in raw if p.get("confidence") not in (None, "skip")]
            return {"sport": "nhl", "picks": picks}
        except Exception as e:
            logger.warning("nhl player-props read failed: %s", e)
            return {"sport": "nhl", "picks": [], "error": str(e)}
    if sport != "nfl":
        return {"sport": sport, "picks": [],
                "message": "Player-props model is NFL-only in v1."}
    try:
        from engine.sports.nfl.prop_picks import generate_picks  # Phase 2: shared-model picker
        return generate_picks()
    except Exception as e:
        logger.warning("nfl player-props slate failed: %s", e)
        return {"sport": sport, "picks": [], "error": str(e)}
