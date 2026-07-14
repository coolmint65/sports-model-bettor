"""
Live data worker — Phase 3a entry point.

Runs as a separate process from the FastAPI server. Polls ESPN
scoreboard for NBA + NHL, normalizes each in-progress game's state,
and writes to `engine.live._store` (SQLite-backed shared cache).

Cadence per spec:
    NBA: 15s
    NHL: 30s

Two staggered loops to honor the per-sport cadence without bloating
the polling burden — NBA polls every 15s, NHL every 30s. Both share
one main thread; we just track per-sport `next_due_at` timestamps.

Failure handling:
    - ESPN returns None → keep last-known state (we don't overwrite
      with empty), log at warning level.
    - One sport's parse throws → don't kill the loop; isolate per-sport.
    - Single game's parse throws → don't kill that sport's tick;
      isolate per-game inside _state.fetch_states.

Run separately:
    python -m services.live_worker.main

Stop:
    Ctrl+C — KeyboardInterrupt is caught, store-flush is unnecessary
    (writes are durable per upsert).
"""

from __future__ import annotations
import json
import logging
import os
import signal
import time
import urllib.error
import urllib.request
from typing import Any

from engine.live._state import fetch_states
from engine.live._odds import fetch_live_odds
from engine.live._pbp import fetch_plays
from engine.live._store import (
    upsert_state, upsert_pbp, purge_stale, purge_old_pbp, ensure_table,
    get_pbp, get_state,
)
from engine.live._intermissions import (
    detect_and_fire as detect_intermission,
    list_unconsumed as list_unconsumed_intermissions,
    purge_old as purge_old_intermissions,
)
from engine.live_tracker import settle_live_picks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] live_worker: %(message)s",
)
logger = logging.getLogger("live_worker")

# Phase 5a (Level 2.5): tightened from 15s/30s to 5s/10s. ESPN's own
# scoreboard cadence is ~10-15s during games, so polling 3x faster
# means we surface state changes within one ESPN refresh instead of
# missing the previous one. ESPN's CDN tolerates this comfortably —
# we're under the ~1 req/sec floor most public APIs accept.
#
# HR live odds get hit on the same tick (see _poll_sport). If HR
# starts throwing 429s, decouple by adding a separate scheduler
# entry for fetch_live_odds at a slower cadence.
_POLL_INTERVAL_S = {"nba": 5, "nhl": 10, "wnba": 10, "ncaam": 10, "afl": 10}

# Garbage-collect rows older than this so completed games don't pile
# up in the cache. 30 min is generous — gives the API time to read
# final-state rows before they vanish.
_PURGE_AFTER_S = 1800
_PURGE_EVERY_S = 300

# Auto-settle cadence for locked live picks. The settler is a no-op
# when nothing's resolvable (cheap), so running on a tight 60s loop
# is fine — picks land in the tracker UI almost the moment a quarter
# / period closes. Manual /live-picks/settle endpoint stays as an
# escape hatch.
_SETTLE_EVERY_S = 60

# Phase 5b PBP poll cadence. Plays accumulate steadily — we don't
# need 5s freshness on a play that already happened — but we do need
# the period's plays settled BEFORE the intermission predictor fires.
# 30s is the sweet spot: lighter than state polling, fast enough
# that a buzzer-beater is captured well before halftime ends.
#
# Tracked separately from state polling because the summary endpoint
# is heavier (per-game fetch) and we want to throttle it independently
# of the all-games-in-one scoreboard call.
_PBP_INTERVAL_S = {"nba": 30, "nhl": 30, "wnba": 30, "ncaam": 30, "afl": 30}

# Roster refresh cadence. NHL goalies confirm ~2h before puck drop;
# MLB lineups confirm 2-3h before first pitch; NBA load-management
# decisions can drop within an hour of tip-off. 15 min keeps us within
# one cycle of any announcement while staying cheap — each refresh
# is a no-op when no rosters changed.
#
# Hits the backend HTTP endpoints rather than calling refresh_for_date
# directly so the in-process _PRED_CACHE clear runs in the backend
# process where the cache actually lives. live_worker and backend are
# separate processes; calling the engine module here would drop the
# picks_cache table rows but leave the backend's stale predictions
# in memory until TTL.
_REFRESH_EVERY_S = 900

# Closing-odds capture cadence. Per-minute heartbeat per sport while
# any uncaptured pending pick exists for today — see
# _has_pending_today / _capture_closing_odds. A pure cron schedule
# misses the narrow window between pick generation and HR dropping
# the prematch market (NHL/NBA staggered tips, West-coast late games),
# which is why CLV coverage bottomed out at 32% / 8% / 1% by 2026-04-30
# even with the cron running. The worker's per-minute beat catches
# every game inside HR's prematch window.
_CLV_CAPTURE_INTERVAL_S = 60
_BACKEND_BASE = os.environ.get("LIVE_WORKER_BACKEND",
                                "http://127.0.0.1:8000")
_REFRESH_PATHS = {
    "mlb": "/api/mlb/refresh-lineups",
    "nhl": "/api/nhl/refresh-goalies",
    "nba": "/api/nba/refresh-injuries",
}
# Skip the very first refresh tick — backend isn't always warm yet
# when live_worker starts (start.bat staggers them by ~8s but uvicorn
# --reload can take longer). 60s after start gives enough headroom
# without losing meaningful day-of timing.
_REFRESH_STARTUP_DELAY_S = 60

# Calibration refit — A8 event-driven with 24h safety floor. The
# worker checks the events log every hour; per-sport, it skips the
# refit if a `refit` event already exists within the floor window.
# Drift-fired refits (engine.drift_detector.consume_unactioned_signals)
# count toward the floor, so a healthy sport just hits the floor
# cadence and a drifting sport refits sooner — no double work.
#
# Was: unconditional hourly refit per sport. Cron-floor is materially
# the same for cost (refit is ~2-5s per sport, idempotent), but the
# semantics are now "refit when needed, otherwise prove we don't need
# to" instead of "refit on a clock and hope it's enough."
_CALIBRATION_CHECK_EVERY_S = 3600
_CALIBRATION_FLOOR_HOURS = 24
_CALIBRATION_SPORTS = ("mlb", "nhl", "nba", "tennis", "wnba", "ncaam", "afl")

# Closing-line prior refit — daily safety floor. Same A8 pattern: the
# loop ticks daily, but the floor check skips when a recent refit
# event covers us. Sample-shape stability is on the order of weeks, so
# the floor is plenty.
_PRIOR_REFIT_CHECK_EVERY_S = 86_400
_PRIOR_REFIT_FLOOR_HOURS = 24

# Player-prop shrinkage refit — daily floor. Same A8 pattern. The
# multipliers in data/prop_shrinkage.json damp prop probabilities to
# match realised settle distributions. Sample size grows by ~tens of
# stat-rows per day; floor of 24h is plenty.
_PROP_CALIBRATION_CHECK_EVERY_S = 86_400
_PROP_CALIBRATION_FLOOR_HOURS = 24
_PROP_CALIBRATION_SPORTS = ("mlb", "nba", "nhl", "wnba")

# National-team Elo refresh — daily floor. eloratings.net updates after
# every international match; a 24h cadence catches every meaningful
# rating shift during a tournament window (World Cup matchdays especially)
# without burning bandwidth between matchdays. Refreshing fifa_internationals
# automatically benefits any soccer league that sources its Elo from it
# (currently just the WC predictor — see engine/soccer/_predict.py line ~200).
_ELO_REFRESH_EVERY_S = 86_400

# Daily player-props generation floor. The sync_*.bat scripts call
# {sport}_prop_picks.generate_picks(date=tomorrow) on a cron — when
# cron skips (laptop off / scheduler hiccup), the slate has no props
# the next morning. Worker backstops with a 24h floor and emits a
# `refit` event per sport so the floor recognises a successful run.
_PLAYER_PROPS_CHECK_EVERY_S = 21_600   # check every 6h
_PLAYER_PROPS_FLOOR_HOURS = 24

# Framework-league results refresh — hourly. Today's slate for
# basketball-framework (WNBA / NCAAM / Euroleague / etc) and hockey-
# framework (AHL / PWHL) leagues won't flip from scheduled → final
# without an upstream pull. NHL / NBA are covered by the dedicated
# ESPN polling loops higher up; this hook handles everything else.
# Skipped for off-season leagues (no work to do).
_FRAMEWORK_REFRESH_EVERY_S = 3600

# Tennis schedule refresh — every 5 min. Tennis isn't part of the live
# poller (NBA/NHL only), so completed matches stay showing as 'in' until
# someone hits the API with ?refresh=true OR the API-side stale-live
# auto-refresh fires. The worker takes ownership so user-facing UX is
# correct even when nobody's looking. Bounded cost: one ESPN tennis
# scoreboard fetch per 5 min.
_TENNIS_REFRESH_EVERY_S = 300

# Event-log sync — every 30 min. A1 architecture: every pick + settle
# from the per-sport tables is mirrored into the canonical events log
# (data/events.db). engine.events_backfill is idempotent on (sport,
# pick_id), so re-running just emits whatever's new since the last call.
# 30 min cadence is plenty — the events log is eventually-consistent
# with legacy tables, never the source of a live decision yet.
_EVENT_LOG_SYNC_EVERY_S = 1800

# Drift detection — every 1h. A3 closes the feedback loop:
#   1. Scan settled decisions per (sport, bet_type), compare recent
#      Brier + hit-rate to prior. Cells that drift past threshold emit
#      drift_signal events.
#   2. Consume drift_signal events → trigger out-of-cycle calibration
#      refit + write a ``refit`` event so the cooldown applies.
# Two passes per hour (scan + consume) is plenty — drift moves on the
# order of weeks, but the loop being event-driven means we never miss
# a real signal.
_DRIFT_SCAN_EVERY_S = 3600

# Bulletproof Phase 2 — 24/7 emission cadences for previously
# lazy-on-click lanes. Tuned to balance freshness vs HR/ESPN load.
# Prematch picks rerun every 5 min so model updates + line moves get
# the picks tracker fresh between major odds shifts. Tennis runs at
# 15 min (more matches, less odds movement per match). Halftime drain
# is per-minute since matches transit HT in seconds and we need to
# catch the next-minute window.
_PREMATCH_PICKS_EVERY_S = 300        # NBA/MLB/NHL `/api/{sport}/best-bets`
_TENNIS_PICKS_EVERY_S = 900           # `/api/tennis/scheduled` walks the slate
_TENNIS_SETTLE_EVERY_S = 300          # grade resolved matches
_SOCCER_HT_DRAIN_EVERY_S = 90         # fire_halftime_picks per in-season league
                                       # — halftime is 15 min, 90s cadence still
                                       # catches HT well within window while
                                       # halving per-league HR call rate
_FOOTBALL_SLATE_EVERY_S = 1800        # api_football_today per league (30m)
_BASEBALL_SLATE_EVERY_S = 1800        # api_baseball_today per league (30m)
_MOTORSPORTS_SETTLE_EVERY_S = 600     # settle_race for finalized races (10m)


_running = True


def _handle_signal(signum: int, _frame: Any) -> None:
    global _running
    logger.info("live_worker received signal %s — shutting down", signum)
    _running = False


def _poll_sport(sport: str) -> int:
    """Fetch and persist the live state + live odds for one sport.
    Returns the number of games written.

    Two upstream calls per tick:
      1. ESPN scoreboard → game state (score, period, clock)
      2. HR live-markets → odds dict per matchup

    They're correlated by AWAY@HOME matchup string. State is the
    authoritative game list (ESPN is reliable, never empty during
    games); odds is best-effort overlay (HR may be stale or empty
    if session expired).

    Per-sport try/except so one outage doesn't kill the loop.
    """
    try:
        states = fetch_states(sport)
    except Exception as e:
        logger.warning("fetch_states(%s) crashed: %s", sport, e)
        return 0

    if not states:
        return 0

    # HR live odds keyed by "AWAY@HOME" — best-effort, may be empty.
    try:
        odds_map = fetch_live_odds(sport)
    except Exception as e:
        logger.warning("fetch_live_odds(%s) crashed: %s", sport, e)
        odds_map = {}

    written = 0
    for s in states:
        try:
            # Attach odds onto state if HR shipped them. The store
            # holds one blob per game; merging keeps the predictor
            # in 3b reading from a single dict.
            key = f"{s['away']['abbr']}@{s['home']['abbr']}"
            game_odds = odds_map.get(key)
            if game_odds:
                s = {**s, "odds": game_odds}
            upsert_state(sport, s["game_id"], s)
            written += 1
        except Exception as e:
            logger.warning("live upsert failed for %s/%s: %s",
                           sport, s.get("game_id"), e)
    if written:
        logger.info("polled %s — %d game(s) written, %d with HR odds",
                    sport, written, sum(1 for s in states
                                         if odds_map.get(
                                             f"{s['away']['abbr']}@{s['home']['abbr']}")))
    return written


def _trigger_intermission_predict(event: dict) -> None:
    """Fan out to the per-sport intermission predictor that consumes
    the just-fired event and writes new picks for the upcoming
    period(s). Per-sport modules live in engine.live (5h NHL,
    5i NBA) — this stub routes by sport so the worker doesn't need
    to know the predictor internals.

    Wrapped in a blanket try at the call site — predictor failure
    must not block the polling loop, intermission rows stay
    unconsumed and a later worker run can retry.
    """
    sport = event["sport"]
    if sport == "nhl":
        from engine.live._nhl_period_predict import run_for_event
        run_for_event(event)
    elif sport in ("nba", "wnba", "ncaam", "afl"):
        # Shared basketball-framework intermission predictor. NCAAM
        # has a single halftime intermission; NBA/WNBA/AFL have three
        # (Q1/Q2/Q3 ends).
        from engine.live._nba_intermission_predict import run_for_event
        run_for_event(event)


def _poll_pbp_sport(sport: str) -> int:
    """Pull play-by-play for every active game in ``sport`` and
    persist new plays. Returns the count of plays inserted across all
    games in this tick.

    Reads game ids from the freshest live_state rows so we only hit
    ESPN's summary endpoint for games that are actually in-progress.
    Failed fetches per game are isolated — one game's outage doesn't
    skip the rest of the slate.
    """
    try:
        from engine.live._store import list_active
        rows = list_active(sport)
    except Exception as e:
        logger.warning("pbp poll(%s): list_active failed: %s", sport, e)
        return 0
    if not rows:
        return 0
    inserted_total = 0
    for r in rows:
        state = r.get("state") or {}
        status = (state.get("status") or {}).get("state") or ""
        if status != "in":
            # Skip pre/post — pre has no plays yet; post is settled
            # and the existing rows don't change.
            continue
        gid = r.get("game_id") or ""
        if not gid:
            continue
        try:
            plays = fetch_plays(sport, gid)
        except Exception as e:
            logger.debug("fetch_plays(%s/%s) crashed: %s", sport, gid, e)
            continue
        if not plays:
            continue
        try:
            inserted = upsert_pbp(sport, gid, plays)
        except Exception as e:
            logger.warning("upsert_pbp(%s/%s) crashed: %s", sport, gid, e)
            continue
        inserted_total += inserted
        if inserted:
            logger.debug("%s pbp game=%s ingested %d new play(s)",
                         sport, gid, inserted)
        # Fire intermission event when this poll caught a period_end.
        # The detector dedupes per (sport, game, period) so re-running
        # within the same intermission window is a no-op.
        try:
            event = detect_intermission(sport, gid, state, plays)
        except Exception as e:
            logger.debug("intermission detect(%s/%s) crashed: %s",
                         sport, gid, e)
            event = None
        if event:
            logger.info("INTERMISSION %s game=%s period=%s kind=%s",
                        sport, gid, event["period"], event["kind"])
            try:
                _trigger_intermission_predict(event)
            except Exception as e:
                logger.warning("intermission trigger %s game=%s failed: %s",
                               sport, gid, e)
    if inserted_total:
        logger.info("polled %s pbp — %d new play(s) across %d game(s)",
                    sport, inserted_total, len(rows))

    # Drain unconsumed intermissions for this sport. The fresh-fire
    # dispatch above only runs ONCE per (game, period); if that first
    # call returned 0 picks (e.g. HR live odds hadn't populated at the
    # exact period-end tick), the row stays NULL.consumed_at forever
    # because `detect_intermission` is idempotent and won't re-emit.
    # OKC @ SA 2026-05-28 hit this — Q1 + halftime both detected at
    # ~01:05-01:07 UTC but no picks ever surfaced. Retry by re-running
    # the predictor against any unconsumed row on every tick — the
    # predictor self-marks consumed only when emit > 0.
    try:
        pending = list_unconsumed_intermissions(sport)
    except Exception as e:
        logger.debug("list_unconsumed_intermissions(%s) crashed: %s",
                      sport, e)
        pending = []
    for ev in pending:
        try:
            _trigger_intermission_predict(ev)
        except Exception as e:
            logger.warning("intermission retry %s game=%s P%s failed: %s",
                            sport, ev.get("game_id"),
                            ev.get("period"), e)

    return inserted_total


def _post_backend(path: str, timeout_s: float = 30.0) -> dict | None:
    """Fire a POST against the backend and return the parsed JSON
    response. Returns None on any failure — callers log + carry on.

    No body is sent because every refresh endpoint reads its inputs
    from query string defaults. ``Content-Length: 0`` is set explicitly
    so the request is well-formed even on stricter HTTP stacks."""
    url = _BACKEND_BASE.rstrip("/") + path
    req = urllib.request.Request(url, method="POST",
                                  headers={"Content-Length": "0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read()
    except urllib.error.URLError as e:
        logger.debug("backend POST %s failed: %s", path, e)
        return None
    except Exception as e:
        logger.warning("backend POST %s crashed: %s", path, e)
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


def _has_pending_today_hockey(league: str) -> bool:
    """Per-league hockey-framework version of _has_pending_today.
    Reads from the theScore-backed per-league DB so a single league's
    pending pick triggers only its own HR fetch."""
    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")
    try:
        mod = __import__(f"engine.sports.{league}.db",
                          fromlist=["get_conn"])
        conn = mod.get_conn()
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM picks "
            "WHERE date = ? AND result IS NULL "
            "AND closing_odds IS NULL",
            (today,),
        ).fetchone()
    except Exception:
        return False
    return bool(row and (row["n"] or 0) > 0)


def _has_pending_today_basketball(league: str) -> bool:
    """Per-league basketball-framework version of _has_pending_today.
    NBA goes through the dedicated nba_picks path; every other framework
    league (wnba/ncaam/afl/euroleague/germany_bbl/...) routes through
    here so a single league's open pick triggers its own HR fetch
    without dragging in the whole basketball registry."""
    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")
    try:
        from engine.basketball._db import get_conn as _bb_conn
        conn = _bb_conn(league)
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM picks "
            "WHERE date = ? AND result IS NULL "
            "AND closing_odds IS NULL",
            (today,),
        ).fetchone()
    except Exception:
        return False
    return bool(row and (row["n"] or 0) > 0)


def _has_pending_today_soccer(league: str) -> bool:
    """Soccer twin of _has_pending_today — keyed by club league rather
    than top-level sport. Same threshold (pending + closing_odds NULL
    + today's date) so HR fetches stay cheap when nothing needs CLV."""
    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")
    try:
        from engine.soccer._db import get_conn as _soc_conn
        conn = _soc_conn(league)
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM picks "
            "WHERE date = ? AND result IS NULL "
            "AND closing_odds IS NULL",
            (today,),
        ).fetchone()
    except Exception:
        return False
    return bool(row and (row["n"] or 0) > 0)


def _has_pending_today(sport: str) -> bool:
    """True if there are any pending picks for today in ``sport``.

    Trigger for closing-odds capture. Live worker can't read pre-game
    state from the live store (engine.live._state filters out `pre`
    events), so we use pending-picks-for-today as the trigger instead.
    Cheap: one indexed COUNT per minute. Capture itself is idempotent
    so over-firing has no cost.
    """
    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")
    try:
        if sport == "nhl":
            from engine.nhl_tracker._helpers import _get_nhl_db
            conn = _get_nhl_db()
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM nhl_picks "
                "WHERE date = ? AND result IS NULL "
                "AND closing_odds IS NULL",
                (today,),
            ).fetchone()
        elif sport == "nba":
            from engine.nba_db import get_conn
            conn = get_conn()
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM nba_picks "
                "WHERE date = ? AND result IS NULL "
                "AND closing_odds IS NULL",
                (today,),
            ).fetchone()
        elif sport == "mlb":
            from engine.db import get_conn
            conn = get_conn()
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM picks "
                "WHERE date = ? AND result IS NULL",
                (today,),
            ).fetchone()
        elif sport in ("wnba", "ncaam", "afl"):
            from engine.basketball._db import get_conn as _bb_conn
            conn = _bb_conn(sport)
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM picks "
                "WHERE date = ? AND result IS NULL "
                "AND closing_odds IS NULL",
                (today,),
            ).fetchone()
        else:
            return False
    except Exception:
        return False
    return bool(row and (row["n"] or 0) > 0)


def _capture_closing_odds(sport: str) -> int:
    """Per-sport capture wrapper. Returns rows updated. Idempotent —
    the underlying capture only writes when ``closing_odds`` is NULL
    or has changed.
    """
    try:
        if sport == "nhl":
            from engine.nhl_tracker import capture_closing_odds as _cap
        elif sport == "nba":
            from engine.nba_tracker import capture_closing_odds as _cap
        elif sport == "mlb":
            from engine.tracker import capture_closing_odds as _cap
        elif sport in ("wnba", "ncaam", "afl"):
            # Handled by the dedicated basketball-framework CLV loop
            # (next_bb_clv_at) which iterates active_leagues() — keeps
            # wnba/ncaam/afl from double-firing CLV via this NBA-flavor
            # block AND the universal basketball loop.
            return 0
        else:
            return 0
        n = _cap()
        if n:
            logger.info("closing-odds capture %s: %d row(s)", sport, n)
        return n
    except Exception as e:
        logger.warning("closing-odds capture %s failed: %s", sport, e)
        return 0


def _run_calibration_refresh() -> None:
    """A8 safety-floor refit. Per-sport, skip if a refit event landed
    within the floor window — drift-fired or prior cron-floor counts.
    When the floor fires, write a `refit` event so the next tick skips."""
    try:
        from engine.empirical_calibration import refresh_calibration
        from engine import events as _events
    except Exception as e:
        logger.warning("calibration refresh import failed: %s", e)
        return
    for sport in _CALIBRATION_SPORTS:
        try:
            if not _events.needs_refit(
                "empirical_calibration", sport,
                max_age_h=_CALIBRATION_FLOOR_HOURS,
            ):
                continue
            result = refresh_calibration(sport)
            n = result.get("n_settled") if isinstance(result, dict) else None
            _events.write_refit(
                sport=sport,
                component="empirical_calibration",
                summary={"n_settled": n,
                          "floor_hours": _CALIBRATION_FLOOR_HOURS},
                triggered_by="cron_floor",
            )
            if n:
                logger.info("calibration floor-refit %s: n_settled=%d",
                            sport, n)
        except Exception as e:
            logger.warning("calibration refresh %s failed: %s", sport, e)


def _run_tennis_refresh() -> None:
    """Refresh today's + tomorrow's tennis schedule. Flips 'in'-status
    matches to 'post' when ESPN reports completion. Quiet on success."""
    try:
        from datetime import datetime
        from engine.tennis_schedule import ingest_schedule_window
        today = datetime.now().strftime("%Y-%m-%d")
        result = ingest_schedule_window(today, days=2)
        # Only log when something material happened.
        summaries = result.get("summaries") or []
        n_inserted = sum(s.get("inserted", 0) for s in summaries)
        if n_inserted:
            logger.info("tennis schedule refresh: %d match(es) ingested", n_inserted)
    except Exception as e:
        logger.warning("tennis schedule refresh failed: %s", e)


# ── Bulletproof Phase 2 helpers (24/7 emission) ────────────────

def _run_prematch_picks(sport: str) -> None:
    """Force NBA/MLB/NHL prematch picks to fire on a worker cadence
    instead of waiting for the user to open the best-bets card.

    Hits the public route so all the side-effects (`record_picks`,
    `picks_store_put`, closing-line capture) trigger the same way as
    a user request would. POSTs to /api/{sport}/record-picks where it
    exists; otherwise GET /api/{sport}/best-bets (which piggybacks
    `record_picks` in its handler).
    """
    path_map = {
        "nba": "/api/nba/best-bets",
        "mlb": "/api/mlb/best-bets",
        "nhl": "/api/nhl/best-bets",
    }
    path = path_map.get(sport)
    if not path:
        return
    try:
        _get_backend(path)
    except Exception as e:
        logger.debug("prematch picks %s failed: %s", sport, e)


def _run_football_slate() -> None:
    """Walk every football league registered in the framework, fire
    its `today` route. The route's internal pipeline (ingest → predict
    → odds → picks → record → settle) handles everything. Previously
    UFL only emitted picks when the user opened the page; this lifts
    that gate."""
    try:
        from engine.football._config import LEAGUE_REGISTRY as _FB_REG
        from backend.server.routes_football import (
            api_football_today as _fb_today,
        )
    except Exception as e:
        logger.debug("football slate import skipped: %s", e)
        return
    from datetime import datetime
    today = datetime.now()
    for key, cfg in (_FB_REG or {}).items():
        months = cfg.get("season_months") or ()
        if months and today.month not in months:
            continue
        try:
            slate = _fb_today(key)
            picks = slate.get("picks") or []
            if picks:
                logger.info("football slate %s: %d game(s), %d pick(s)",
                            key, len(slate.get("games") or []), len(picks))
        except Exception as e:
            logger.warning("football slate %s failed: %s", key, e)


def _run_baseball_framework_slate() -> None:
    """Walk every baseball framework league (college today; others
    later) and fire its `today` route. Same gap-closure pattern as
    football — slate refresh + picks + settle land via the route."""
    try:
        from engine.baseball._config import LEAGUE_REGISTRY as _BB_REG
        from backend.server.routes_baseball import (
            api_baseball_today as _bb_today,
        )
    except Exception as e:
        logger.debug("baseball framework slate import skipped: %s", e)
        return
    from datetime import datetime
    today = datetime.now()
    for key, cfg in (_BB_REG or {}).items():
        months = cfg.get("season_months") or ()
        if months and today.month not in months:
            continue
        try:
            slate = _bb_today(key)
            picks = slate.get("picks") or []
            if picks:
                logger.info("baseball-framework slate %s: %d pick(s)",
                            key, len(picks))
        except Exception as e:
            logger.warning("baseball-framework slate %s failed: %s",
                            key, e)


def _run_tennis_picks() -> None:
    """Force tennis picks generation by hitting the slate route. The
    handler's per-match loop runs the predictor + emits picks via
    `record_tennis_pick` for every today/tomorrow match — currently
    only fires when the user opens the tennis tab."""
    try:
        _get_backend("/api/tennis/scheduled")
    except Exception as e:
        logger.debug("tennis picks emit failed: %s", e)


def _run_tennis_settle() -> None:
    """Drive `settle_tennis_picks` on the worker. Previously settle
    only ran on user navigation; pending picks rotted (caused 17
    stuck picks 2026-05-28 before perf fix landed)."""
    try:
        from engine.tennis_tracker import settle_tennis_picks
        r = settle_tennis_picks()
        if isinstance(r, dict) and (r.get("settled") or 0) > 0:
            logger.info("tennis settle: %s", r)
    except Exception as e:
        logger.warning("tennis settle failed: %s", e)


def _run_soccer_halftime_drain() -> None:
    """Fire `fire_halftime_picks(league)` for every in-season soccer
    league. The function internally checks for matches with HT scores
    recorded but no final yet, runs the halftime-adjusted predictor,
    emits picks against HR live odds. UPSERT-keyed so per-minute
    re-runs are no-ops when the live state hasn't changed."""
    try:
        from engine.soccer._config import active_leagues as _soc_active
        from engine.soccer._live_picks import fire_halftime_picks
    except Exception as e:
        logger.debug("soccer halftime drain import skipped: %s", e)
        return
    for league in _soc_active():
        try:
            r = fire_halftime_picks(league)
            stored = (r.get("stored") if isinstance(r, dict) else 0) or 0
            if stored:
                logger.info("soccer halftime %s: %d pick(s) stored",
                            league, stored)
        except Exception as e:
            logger.debug("soccer halftime %s failed: %s", league, e)


def _run_motorsports_settle() -> None:
    """Walk every motorsports series; for any race that's finalized
    with pending picks, call `settle_race`. The framework slate route
    only refreshes upcoming races — it doesn't grade past ones, which
    left F1 picks pending after race finals."""
    try:
        from engine.motorsports import SERIES_REGISTRY as _MS_REG
        from engine.motorsports._db import get_conn as _ms_conn
        from engine.motorsports._picks import settle_race
    except Exception as e:
        logger.debug("motorsports settle import skipped: %s", e)
        return
    for series, cfg in (_MS_REG or {}).items():
        if (cfg.get("status") == "pending_data"
                or cfg.get("data_source") == "pending"):
            continue
        try:
            conn = _ms_conn(series)
            # Race finalization status label: F1 (Ergast) writes 'complete',
            # NASCAR/IndyCar (ESPN core) also writes 'complete'. Earlier
            # this filter used 'final' which never matched, so NASCAR
            # picks sat pending after real completed races (Chicago
            # 2026-07-05 stuck 4 days before manual regrade). Also fall
            # back to race_date-in-the-past for edges where the ingest
            # hasn't updated status yet — settle_race is a no-op when
            # results are missing so this is safe.
            rows = conn.execute(
                "SELECT DISTINCT p.race_id FROM picks p "
                "JOIN races r ON r.race_id = p.race_id "
                "WHERE p.result IS NULL "
                "  AND (r.status IN ('complete', 'final') "
                "       OR (r.race_date IS NOT NULL "
                "           AND r.race_date < date('now')))"
            ).fetchall()
            for r in rows:
                res = settle_race(series, r["race_id"])
                if isinstance(res, dict) and (res.get("settled") or 0) > 0:
                    logger.info("motorsports settle %s/%s: %s",
                                series, r["race_id"], res)
        except Exception as e:
            logger.debug("motorsports settle %s failed: %s", series, e)


def _get_backend(path: str, timeout_s: float = 30.0) -> dict | None:
    """GET against the backend (mirrors `_post_backend`). Used by the
    24/7 picks-emit lanes that hit a GET route to trigger the route's
    internal record_picks side-effect."""
    url = _BACKEND_BASE.rstrip("/") + path
    req = urllib.request.Request(url, method="GET",
                                  headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as r:
            body = r.read()
        try:
            return json.loads(body) if body else None
        except Exception:
            return None
    except Exception as e:
        logger.debug("_get_backend(%s) failed: %s", path, e)
        return None


def _run_prop_calibration_refit() -> None:
    """A8 safety-floor refit for player-prop shrinkage multipliers."""
    try:
        from engine.player_props_calibration import refresh_shrinkage
        from engine import events as _events
    except Exception as e:
        logger.warning("prop calibration import failed: %s", e)
        return
    for sport in _PROP_CALIBRATION_SPORTS:
        try:
            if not _events.needs_refit(
                "prop_shrinkage", sport,
                max_age_h=_PROP_CALIBRATION_FLOOR_HOURS,
            ):
                continue
            res = refresh_shrinkage(sport)
            recs = res.get("recs") if isinstance(res, dict) else {}
            adjusted = sum(1 for v in (recs or {}).values()
                           if isinstance(v, dict) and v.get("shrink", 1.0) < 1.0)
            _events.write_refit(
                sport=sport,
                component="prop_shrinkage",
                summary={"adjusted_stats": adjusted,
                          "floor_hours": _PROP_CALIBRATION_FLOOR_HOURS},
                triggered_by="cron_floor",
            )
            if adjusted:
                logger.info("prop calibration floor-refit %s: %d stat(s) shrunk",
                            sport, adjusted)
        except Exception as e:
            logger.warning("prop calibration %s failed: %s", sport, e)


def _run_player_props_generation() -> None:
    """A8 pattern for player-prop picks: skip per-sport when a refit
    event covers the floor window, otherwise generate today's slate.
    Mirrors the calibration / prior / shrinkage loops — keeps props
    flowing even when the cron sync scripts haven't run.

    Was a real bug 2026-05-04/05: 5 days without MLB props because the
    cron skipped. Worker now backstops with a 24h floor."""
    try:
        from engine import events as _events
        from datetime import datetime
    except Exception as e:
        logger.warning("player props generator import failed: %s", e)
        return
    today = datetime.now().strftime("%Y-%m-%d")
    # WNBA's prop picker lives under the basketball framework rather
    # than engine.{sport}_prop_picks — special-case the import path.
    _PROP_PICKER_PATHS = {
        "mlb":  ("engine.mlb_prop_picks", "generate_picks"),
        "nba":  ("engine.nba_prop_picks", "generate_picks"),
        "nhl":  ("engine.nhl_prop_picks", "generate_picks"),
        "wnba": ("engine.basketball._wnba_prop_picks", "generate_picks"),
    }
    for sport in ("mlb", "nba", "nhl", "wnba"):
        try:
            # Gate per calendar-day, not per 24h. Yesterday's 8pm refit
            # would lock today's 4pm slate out under a pure 24h floor;
            # but picks are date-scoped — we need a fresh pass once per
            # slate-day. Skip only if today's date already has a refit
            # entry. Fall back to the time-based floor when the prior
            # event has no 'date' field (legacy events pre-2026-05-14).
            last = _events.latest_refit("player_props_picks", sport)
            last_payload = (last or {}).get("payload") or {}
            last_date = (last_payload.get("summary") or {}).get("date")
            if last_date == today:
                continue
            if last_date is None and not _events.needs_refit(
                "player_props_picks", sport,
                max_age_h=_PLAYER_PROPS_FLOOR_HOURS,
            ):
                continue
            mod_path, fn_name = _PROP_PICKER_PATHS[sport]
            mod = __import__(mod_path, fromlist=[fn_name])
            res = getattr(mod, fn_name)(date=today)
            picked = res.get("picked", 0) if isinstance(res, dict) else 0
            _events.write_refit(
                sport=sport,
                component="player_props_picks",
                summary={"picked": picked,
                          "evaluated": (res or {}).get("evaluated"),
                          "floor_hours": _PLAYER_PROPS_FLOOR_HOURS,
                          "date": today},
                triggered_by="cron_floor",
            )
            if picked:
                logger.info("props floor-gen %s: %d picked", sport, picked)
        except Exception as e:
            logger.warning("props gen %s failed: %s", sport, e)


def _run_framework_results_refresh() -> None:
    """Tick every in-season framework league through its full slate
    pipeline: ingest → predict → pick → record → settle.

    Previously only ingest + settle ran here — picks were emitted
    exclusively when a user hit /api/{sport}/{league}/today. Cold leagues
    (CBA / NZ NBL / PWHL / niche euros) accumulated no settled picks, so
    calibration never converged for them. Per directive: always picking,
    even if nobody's watching tonight.

    Implementation: call each sport's slate route directly. api_*_today
    handlers internally do debounced ingest + slate build + predict +
    HR odds + picks + tracker record + settle. Calling them here flips
    the trigger from user-activity to clock — pick rows land in the
    per-league tables on the hourly tick and the cron-fed calibration
    pipeline sees the fresh sample."""
    from datetime import datetime
    today = datetime.now()

    # Basketball framework — WNBA / NCAAM / Euroleague / RealGM tier
    # (AFL / CBA / NZ NBL / Argentina / Brazil etc).
    try:
        from engine.basketball import LEAGUE_REGISTRY as _BB_REG
        from backend.server.routes_basketball import (
            api_basketball_today as _bb_today,
        )
    except Exception as e:
        logger.debug("framework refresh import (basketball) skipped: %s", e)
        _BB_REG = {}
        _bb_today = None
    for key, cfg in (_BB_REG or {}).items():
        if key == "nba":
            continue   # dedicated NBA loop covers picks via /refresh-injuries
        months = cfg.get("season_months") or ()
        if today.month not in months:
            continue
        if not _bb_today:
            continue
        try:
            slate = _bb_today(key)
            games = slate.get("games") or []
            n_pick = sum(1 for g in games if g.get("best_pick"))
            if n_pick:
                logger.info("framework slate %s: %d game(s), %d pick(s)",
                            key, len(games), n_pick)
        except Exception as e:
            logger.warning("framework slate %s failed: %s", key, e)
        # Settle pending picks for this league. The slate route only
        # ingests results — without an explicit settle, pending picks
        # rotted until the user viewed the tracker tab. Tracker route
        # was the only place settle_picks fired, which meant a quiet
        # tab on a small league = picks stranded forever even after
        # the games finalized in the DB.
        try:
            from engine.basketball import settle_picks as _bb_settle
            r = _bb_settle(key)
            n_set = (r.get("settled") if isinstance(r, dict) else 0) or 0
            if n_set:
                logger.info("framework settle %s: %d", key, n_set)
        except Exception as e:
            logger.debug("framework settle %s failed: %s", key, e)
        # Logo backfill — idempotent, no-op when every team already has
        # a logo. Picks up new teams that just got ingested via the
        # slate call above. Throttled by the missing-logo check inside
        # the backfill function so off-season leagues skip cheaply.
        try:
            from engine.basketball._logo_backfill import backfill_league as _logo_bf
            res = _logo_bf(key)
            if isinstance(res, dict) and res.get("updated"):
                logger.info("framework logos %s: %d new", key, res["updated"])
        except Exception as e:
            logger.debug("logo backfill %s skipped: %s", key, e)

    # Basketball calibration refresh — each league's walk-forward
    # (engine.basketball._walkforward) seeds ML+SPREAD+TOTAL and the
    # Q1_* triplet for leagues with quarter/half score history.
    # Floor-gated so we don't replay thousands of games every hour.
    try:
        from engine.basketball import _walkforward as _bb_wf
        from engine.framework_calibration import invalidate as _fc_invalidate
        from engine import events as _events
    except Exception as e:
        logger.debug("basketball calibration refresh skipped: %s", e)
        _bb_wf = None
    if _bb_wf is not None:
        for key, cfg in (_BB_REG or {}).items():
            if key == "nba":
                continue
            months = cfg.get("season_months") or ()
            if today.month not in months:
                continue
            try:
                if not _events.needs_refit(
                    "basketball_calibration", key,
                    max_age_h=_CALIBRATION_FLOOR_HOURS,
                ):
                    continue
                res = _bb_wf.seed_calibration(key)
                summary = (res or {}).get("summary") or {}
                n = summary.get("n_processed")
                _events.write_refit(
                    sport=key,
                    component="basketball_calibration",
                    summary={"n_processed": n,
                              "floor_hours": _CALIBRATION_FLOOR_HOURS},
                    triggered_by="cron_floor",
                )
                _fc_invalidate(key)
                if n:
                    logger.info("basketball calibration floor-refit %s: "
                                "n=%d", key, n)
            except Exception as e:
                logger.warning("basketball calibration refresh %s failed: %s",
                               key, e)

    # Hockey framework — AHL / PWHL / AIHL / NZIHL.
    try:
        from engine.hockey import HOCKEY_LEAGUE_REGISTRY as _HK_REG
        from backend.server.routes_hockey import (
            api_hockey_today as _hk_today,
        )
    except Exception as e:
        logger.debug("framework refresh import (hockey) skipped: %s", e)
        _HK_REG = {}
        _hk_today = None
    # Per-league ingest refresh — most leagues fetch lazily on first
    # /today hit, but the SofaScore-backed leagues need a periodic
    # refresh to bring tomorrow's events in before the slate route is
    # called. Idempotent; no-op when SofaScore returns the same blob.
    _INGEST_PATHS = {
        "aihl":  ("engine.sports.aihl.ingest",  "refresh"),
        "nzihl": ("engine.sports.nzihl.ingest", "refresh"),
    }
    for key, cfg in (_HK_REG or {}).items():
        if key == "nhl":
            continue
        months = cfg.get("season_months") or ()
        if today.month not in months:
            continue
        if key in _INGEST_PATHS:
            mod_path, fn_name = _INGEST_PATHS[key]
            try:
                mod = __import__(mod_path, fromlist=[fn_name])
                r = getattr(mod, fn_name)()
                if r and (r.get("events") or 0):
                    logger.info("hockey ingest %s: %s", key, r)
            except Exception as e:
                logger.warning("hockey ingest %s failed: %s", key, e)
        if not _hk_today:
            continue
        try:
            slate = _hk_today(key)
            games = slate.get("games") or []
            n_pick = sum(1 for g in games if g.get("best_pick"))
            if n_pick:
                logger.info("framework slate %s: %d game(s), %d pick(s)",
                            key, len(games), n_pick)
        except Exception as e:
            logger.warning("framework slate %s failed: %s", key, e)
        # Settle pending hockey-framework picks (AHL/PWHL/AIHL/NZIHL).
        # Slate route only ingests; settlement otherwise only fires
        # when the user opens the tracker tab — picks rotted on
        # quiet leagues. Same pattern as basketball framework above.
        try:
            from engine.hockey._tracker import settle_pending as _hk_settle
            r = _hk_settle(key)
            n_set = (r.get("settled") if isinstance(r, dict) else 0) or 0
            if n_set:
                logger.info("hockey settle %s: %d", key, n_set)
        except Exception as e:
            logger.debug("hockey settle %s failed: %s", key, e)

    # Hockey calibration refresh — each league has its own walk-forward
    # ML+SPREAD+TOTAL bucket calibrator (engine.hockey._calibrate). Floor-
    # gated like the team-sports calibrators so we don't replay 5k games
    # every tick. New AHL/PWHL/AIHL/NZIHL finals feed back into the
    # bucket aggregates within 24h of settlement.
    try:
        from engine.hockey import _calibrate as _hk_cal
        from engine.framework_calibration import invalidate as _fc_invalidate
        from engine import events as _events
    except Exception as e:
        logger.debug("hockey calibration refresh skipped: %s", e)
        _hk_cal = None
    if _hk_cal is not None:
        for key, cfg in (_HK_REG or {}).items():
            if key == "nhl":
                continue
            try:
                if not _events.needs_refit(
                    "hockey_calibration", key,
                    max_age_h=_CALIBRATION_FLOOR_HOURS,
                ):
                    continue
                res = _hk_cal.seed_calibration(key)
                n = res.get("n_games") if res else None
                _events.write_refit(
                    sport=key,
                    component="hockey_calibration",
                    summary={"n_games": n,
                              "floor_hours": _CALIBRATION_FLOOR_HOURS},
                    triggered_by="cron_floor",
                )
                _fc_invalidate(key)
                if n:
                    logger.info("hockey calibration floor-refit %s: "
                                "games=%d", key, n)
            except Exception as e:
                logger.warning("hockey calibration refresh %s failed: %s",
                               key, e)

    # Soccer framework — 14 club competitions + WC + internationals pool.
    # The /today route does ingest + predict + odds + picks + record so
    # each tick keeps every active league fresh.
    try:
        from engine.soccer import LEAGUE_REGISTRY as _SOC_REG, active_leagues as _soc_active
        from backend.server.routes_soccer import (
            api_soccer_today as _soc_today,
        )
    except Exception as e:
        logger.debug("framework refresh import (soccer) skipped: %s", e)
        _SOC_REG = {}
        _soc_active = lambda: []
        _soc_today = None
    if _soc_today:
        for key in _soc_active():
            cfg = (_SOC_REG or {}).get(key) or {}
            # Skip the catch-all internationals pool from the routine slate
            # tick — it has no HR comp_id and emits no picks. The
            # standalone backfill path keeps it fresh.
            if not cfg.get("hr_comp_id"):
                continue
            try:
                slate = _soc_today(key)
                n_pick = len(slate.get("picks") or [])
                if n_pick:
                    logger.info("framework slate soccer:%s: %d pick(s)",
                                key, n_pick)
            except Exception as e:
                logger.warning("framework slate soccer:%s failed: %s", key, e)
            # Settle soccer-framework picks after the slate ingest —
            # same gap as basketball/hockey: settle only fired when
            # the user opened the tracker tab; quiet leagues' pending
            # picks rotted past their game dates.
            try:
                from engine.soccer._tracker import settle_picks as _soc_settle
                r = _soc_settle(key)
                n_set = (r.get("settled") if isinstance(r, dict) else 0) or 0
                if n_set:
                    logger.info("soccer settle %s: %d", key, n_set)
            except Exception as e:
                logger.debug("soccer settle %s failed: %s", key, e)

    # Soccer calibration refresh — each league has its own walk-forward
    # bet-type bucket calibrator (engine.soccer._walkforward). Floor-
    # gated so we don't replay the holdout window every tick. New
    # finals feed back into the bucket aggregates within 24h.
    try:
        from engine.soccer import _walkforward as _soc_wf
        from engine.framework_calibration import invalidate as _fc_invalidate
        from engine import events as _events
    except Exception as e:
        logger.debug("soccer calibration refresh skipped: %s", e)
        _soc_wf = None
    if _soc_wf is not None:
        for key in _soc_active():
            cfg = (_SOC_REG or {}).get(key) or {}
            if not cfg.get("hr_comp_id"):
                # internationals pool / unmapped leagues — no picks fire
                # here so calibration refresh would be wasted IO.
                continue
            try:
                if not _events.needs_refit(
                    "soccer_calibration", key,
                    max_age_h=_CALIBRATION_FLOOR_HOURS,
                ):
                    continue
                res = _soc_wf.seed_calibration(key)
                n = res.get("n_processed") if res else None
                _events.write_refit(
                    sport=key,
                    component="soccer_calibration",
                    summary={"n_processed": n,
                              "floor_hours": _CALIBRATION_FLOOR_HOURS},
                    triggered_by="cron_floor",
                )
                _fc_invalidate(key)
                if n:
                    logger.info("soccer calibration floor-refit %s: "
                                "n=%d", key, n)
            except Exception as e:
                logger.warning("soccer calibration refresh %s failed: %s",
                               key, e)

    # Soccer constants refresh — the Dixon-Coles lambda scaffolding
    # (avg_home_goals/avg_away_goals/home_advantage/h1_share). Distinct
    # from the calibration buckets above: this is the *model* prior, not
    # the post-hoc shrinkage. World Cup ran for 3 weeks on a stale 2024
    # internationals fit (avg goals 11% low) because nothing was refitting
    # constants once new finals settled. Now lives on the same floor.
    try:
        from engine.soccer import _calibrate as _soc_const_cal
    except Exception as e:
        logger.debug("soccer constants refresh skipped: %s", e)
        _soc_const_cal = None
    if _soc_const_cal is not None:
        for key in _soc_active():
            cfg = (_SOC_REG or {}).get(key) or {}
            if not cfg.get("hr_comp_id"):
                continue
            try:
                if not _events.needs_refit(
                    "soccer_constants", key,
                    max_age_h=_CALIBRATION_FLOOR_HOURS,
                ):
                    continue
                # min_matches=60 lets mid-tournament refits (WC, Copa)
                # land their first true-data fit before the bracket ends;
                # larger leagues blow past it on day one.
                res = _soc_const_cal.fit(key, min_matches=60)
                _events.write_refit(
                    sport=key,
                    component="soccer_constants",
                    summary={"n_fitted": res.get("fitted_n"),
                              "h1_n": res.get("h1_fitted_n"),
                              "floor_hours": _CALIBRATION_FLOOR_HOURS},
                    triggered_by="cron_floor",
                )
                logger.info("soccer constants floor-refit %s: n=%s "
                            "avg_home=%.2f avg_away=%.2f h1_share=%s",
                            key, res.get("fitted_n"),
                            res.get("avg_home_goals", 0.0),
                            res.get("avg_away_goals", 0.0),
                            res.get("h1_share"))
            except ValueError:
                # not enough finals yet — try again next floor tick
                continue
            except Exception as e:
                logger.warning("soccer constants refresh %s failed: %s",
                               key, e)

    # Motorsports — F1 (Ergast-backed). Series-scoped so we walk
    # SERIES_REGISTRY instead of a league registry. Today route handles
    # ingest + predict + odds + picks + record.
    try:
        from engine.motorsports import SERIES_REGISTRY as _MS_REG
        from backend.server.routes_motorsports import (
            api_motorsports_today as _ms_today,
        )
    except Exception as e:
        logger.debug("framework refresh import (motorsports) skipped: %s", e)
        _MS_REG = {}
        _ms_today = None
    for key, cfg in (_MS_REG or {}).items():
        if not _ms_today:
            break
        # Skip series whose ingest pipeline isn't wired yet (IndyCar /
        # NASCAR). They surface in the sidebar with a "soon" tag but
        # the worker has no data source to pull from.
        if (cfg.get("status") == "pending_data"
                or cfg.get("data_source") == "pending"):
            continue
        try:
            slate = _ms_today(key)
            picks = slate.get("picks_summary") or {}
            n_pick = (picks.get("winner") or 0) + (picks.get("podium") or 0)
            if n_pick:
                logger.info("framework slate %s: %d pick(s)", key, n_pick)
        except Exception as e:
            logger.warning("framework slate %s failed: %s", key, e)

    # Motorsports calibration refresh — F1 uses its own per-series
    # walk-forward calibrator (engine.motorsports._calibration), not the
    # team-sports empirical_calibration that _CALIBRATION_SPORTS covers.
    # Refit lazily: skip when a recent refit event landed within the
    # floor window so we don't re-replay 50 races every hour.
    try:
        from engine.motorsports import _calibration as _ms_cal
        from engine import events as _events
    except Exception as e:
        logger.debug("motorsports calibration refresh skipped: %s", e)
        _ms_cal = None
    if _ms_cal is not None:
        for key, cfg in (_MS_REG or {}).items():
            # Same pending-data skip as the slate loop above.
            if (cfg.get("status") == "pending_data"
                    or cfg.get("data_source") == "pending"):
                continue
            try:
                if not _events.needs_refit(
                    "motorsports_calibration", key,
                    max_age_h=_CALIBRATION_FLOOR_HOURS,
                ):
                    continue
                res = _ms_cal.refresh(key)
                n = (res.get("backfill") or {}).get("rows") if res else None
                _events.write_refit(
                    sport=key,
                    component="motorsports_calibration",
                    summary={"n_rows": n,
                              "floor_hours": _CALIBRATION_FLOOR_HOURS},
                    triggered_by="cron_floor",
                )
                if n:
                    logger.info("motorsports calibration floor-refit %s: "
                                "rows=%d", key, n)
            except Exception as e:
                logger.warning("motorsports calibration refresh %s failed: %s",
                               key, e)

    # Golf — TOUR_REGISTRY-scoped. Slate route does ingest + predict +
    # odds + picks + record + settle. Skip tours whose ESPN endpoint
    # is off-season (no scheduled events).
    try:
        from engine.golf import TOUR_REGISTRY as _GOLF_REG
        from backend.server.routes_golf import (
            api_golf_today as _golf_today,
        )
    except Exception as e:
        logger.debug("framework refresh import (golf) skipped: %s", e)
        _GOLF_REG = {}
        _golf_today = None
    # Refresh external skill priors (OWGR + DataGolf SG) ahead of slate
    # so the predictor consumes the freshest data. OWGR updates weekly
    # (Monday) but is cheap to refetch; DataGolf SG updates inter-round
    # (~4× per tournament week). The ingest helpers no-op gracefully
    # when scrapers / network are unavailable, so any failure here
    # leaves the predictor on its score-to-par-only baseline.
    try:
        from engine.golf._external_ingest import (
            refresh_owgr as _golf_owgr_refresh,
            refresh_datagolf_sg as _golf_sg_refresh,
        )
    except Exception as e:
        logger.debug("framework refresh import (golf priors) skipped: %s", e)
        _golf_owgr_refresh = None
        _golf_sg_refresh = None
    for key in (_GOLF_REG or {}):
        if _golf_owgr_refresh:
            try:
                r = _golf_owgr_refresh(key)
                if r and r.get("matched"):
                    logger.info("framework owgr golf:%s matched=%d skipped=%d",
                                key, r.get("matched"), r.get("skipped"))
            except Exception as e:
                logger.warning("owgr golf:%s failed: %s", key, e)
        if _golf_sg_refresh:
            try:
                r = _golf_sg_refresh(key)
                if r and r.get("matched"):
                    logger.info("framework dg-sg golf:%s event=%r matched=%d",
                                key, r.get("event"), r.get("matched"))
            except Exception as e:
                logger.warning("dg-sg golf:%s failed: %s", key, e)
    # Pre-hydrate fields for upcoming tournaments from HR's WINNER
    # market. ESPN doesn't post a tournament's field until 24-48h
    # before start; HR posts outright odds days earlier. Without this
    # step the slate route renders an empty field + no picks for any
    # tournament that HR has odds for but ESPN hasn't filed yet
    # (CJ Cup Byron Nelson 2026-05-18 was the precedent).
    #
    # Idempotent: ``ingest_field_from_hr`` uses INSERT OR IGNORE so
    # re-running on a hydrated tournament is a no-op. Cheap (~1 HR
    # fetch per tour per tick).
    try:
        from engine.golf._ingest import ingest_field_from_hr as _golf_hr_field
        from engine.golf._db import get_conn as _golf_conn
        from datetime import datetime as _dt, timedelta as _td
    except Exception as e:
        logger.debug("framework refresh import (golf hr-field) skipped: %s", e)
        _golf_hr_field = None
    if _golf_hr_field is not None:
        # Look ahead 10 days for scheduled tournaments missing a field.
        # Beyond that HR usually doesn't have outright markets yet.
        today_iso = _dt.utcnow().strftime("%Y-%m-%d")
        horizon = (_dt.utcnow() + _td(days=10)).strftime("%Y-%m-%d")
        for key in (_GOLF_REG or {}):
            try:
                conn = _golf_conn(key)
                rows = conn.execute("""
                    SELECT t.id, t.name
                    FROM tournaments t
                    LEFT JOIN field_entries f ON f.tournament_id = t.id
                    WHERE t.status = 'scheduled'
                      AND t.start_date >= ?
                      AND t.start_date <= ?
                    GROUP BY t.id, t.name
                    HAVING COUNT(f.player_id) = 0
                """, (today_iso, horizon)).fetchall()
                for r in rows:
                    res = _golf_hr_field(key, r["id"])
                    n = res.get("rows", 0) if isinstance(res, dict) else 0
                    if n:
                        logger.info("framework hr-field golf:%s tournament=%r "
                                    "hydrated %d players",
                                    key, r["name"], n)
            except Exception as e:
                logger.warning("hr-field hydrate golf:%s failed: %s", key, e)

    for key in (_GOLF_REG or {}):
        if not _golf_today:
            break
        try:
            slate = _golf_today(key)
            summary = slate.get("picks_summary") or {}
            n_pick = sum(summary.values())
            if n_pick:
                logger.info("framework slate golf:%s: %d pick(s) %s",
                            key, n_pick, summary)
        except Exception as e:
            logger.warning("framework slate golf:%s failed: %s", key, e)


def _run_drift_scan() -> None:
    """Scan + consume drift_signal events. Two-pass:
       (1) detect cells that drifted past threshold and emit signals;
       (2) consume unactioned signals — fire calibration refit, log
       refit event so cooldown applies."""
    try:
        from engine import drift_detector
    except Exception as e:
        logger.warning("drift detector import failed: %s", e)
        return
    try:
        detections = drift_detector.scan()
        if detections:
            logger.info("drift detector: %d cell(s) crossed threshold",
                        len(detections))
        consume = drift_detector.consume_unactioned_signals()
        if consume.get("actioned"):
            logger.info("drift consume: actioned=%d skipped=%d",
                        consume["actioned"], consume["skipped"])
    except Exception as e:
        logger.warning("drift scan/consume failed: %s", e)


def _run_event_log_sync() -> None:
    """Mirror new picks + settles from per-sport tables into the
    canonical events log. Idempotent — only emits events for pick_ids
    not already in the events table.

    Foundation of the A1 architecture: calibration / edge_floors /
    reliability eventually become projections of this log instead of
    independently-mutated tables. Today the projections are read-only
    (engine.events_materialize); legacy tables remain authoritative
    until parity proves out post-A2."""
    try:
        from engine.events_backfill import backfill_all
    except Exception as e:
        logger.warning("event log sync import failed: %s", e)
        return
    try:
        results = backfill_all()
        new_events = sum(
            r.get("decision_emitted", 0) + r.get("settle_emitted", 0)
            for r in results.values()
        )
        if new_events:
            logger.info("event log sync: %d new events", new_events)
    except Exception as e:
        logger.warning("event log sync failed: %s", e)


def _run_prior_refit() -> None:
    """A8 safety-floor refit for closing-line prior weights. fit_all_sports
    is sport-cross by design; we gate on a single 'all' bucket in the
    events log so the floor check is one row, not one per sport."""
    try:
        from engine.closing_line_prior import fit_all_sports
        from engine import events as _events
    except Exception as e:
        logger.warning("closing-line prior refit import failed: %s", e)
        return
    if not _events.needs_refit(
        "closing_line_prior", "all",
        max_age_h=_PRIOR_REFIT_FLOOR_HOURS,
    ):
        return
    try:
        result = fit_all_sports()
        active = sum(1 for r in result.values()
                     if r.get("fitted") and r["fitted"])
        _events.write_refit(
            sport="all",
            component="closing_line_prior",
            summary={"active_sports": active,
                      "floor_hours": _PRIOR_REFIT_FLOOR_HOURS},
            triggered_by="cron_floor",
        )
        logger.info("closing-line prior floor-refit: %d sport(s) with fitted weights",
                    active)
    except Exception as e:
        logger.warning("closing-line prior refit failed: %s", e)


def _run_refresh(sport: str) -> None:
    """Trigger a single sport's roster refresh in the backend process.
    Safe to invoke when nothing changed — the underlying refresh is
    idempotent and quick when no deltas land."""
    path = _REFRESH_PATHS.get(sport)
    if not path:
        return
    result = _post_backend(path)
    if result is None:
        return
    deltas = result.get("deltas") or 0
    invalidated = result.get("invalidated") or 0
    if deltas or invalidated:
        logger.info(
            "%s refresh: %d delta(s), %d invalidated, "
            "pred_cache_cleared=%s, re_recorded=%s",
            sport, deltas, invalidated,
            result.get("pred_cache_cleared", 0),
            result.get("re_recorded", False),
        )


def main() -> None:
    """Main loop — staggered per-sport polling, runs until signaled."""
    ensure_table()

    # Trap SIGINT/SIGTERM so a clean Ctrl+C doesn't leave a half-written
    # row.
    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    next_due_at = {sport: 0.0 for sport in _POLL_INTERVAL_S}
    next_pbp_at = {sport: 0.0 for sport in _PBP_INTERVAL_S}
    next_purge_at = 0.0
    next_settle_at = 0.0
    # Stagger the first refresh past startup so the backend is warm.
    next_refresh_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    # Queue auto-placer cadence. Sweeps the bet queue every N seconds
    # and either dry-runs (log-only, default) or POSTs to the Beelink
    # relay (live). Kill switch is env AUTO_BET_LIVE=1 + on-disk flag;
    # sweep_and_place() reads both and picks the mode. Cheap when the
    # queue is empty — one build_queue() call → 0 picks → early exit.
    try:
        from engine.queue_placer import (
            sweep_and_place as _placer_sweep,
            poll_pending_placements as _placer_poll,
            PLACER_INTERVAL_S as _PLACER_INTERVAL_S,
        )
        _placer_available = True
    except Exception as _e:
        logger.debug("queue placer import skipped: %s", _e)
        _placer_available = False
        _PLACER_INTERVAL_S = 60
    next_placer_at = time.monotonic() + 30  # small startup delay
    # Per-sport closing-odds capture timers. Worker fires capture only
    # when an imminent tip is detected (see _games_imminent), throttled
    # at _CLV_CAPTURE_INTERVAL_S so a long pre-tip window doesn't burn
    # HR fetches.
    next_clv_at = {sport: 0.0 for sport in _POLL_INTERVAL_S}
    # Soccer CLV — separate timer per club league. No live state polling
    # for soccer (no ESPN scoreboard wired) so soccer doesn't sit in
    # _POLL_INTERVAL_S; the closing-odds capture still benefits from the
    # worker's heartbeat the same way NBA/NHL do.
    try:
        from engine.soccer._config import active_leagues as _soccer_active
        _soccer_leagues = list(_soccer_active())
    except Exception:
        _soccer_leagues = []
    next_soccer_clv_at = {league: 0.0 for league in _soccer_leagues}
    # Basketball framework CLV — every active in-season league except
    # NBA (which has its own dedicated capture path). wnba/ncaam/afl
    # are also handled here through the basketball._clv adapter rather
    # than the per-sport block at _capture_closing_odds, so they don't
    # double-fire.
    try:
        from engine.basketball._config import active_leagues as _bb_active
        _bb_leagues = [s for s in _bb_active() if s != "nba"]
    except Exception:
        _bb_leagues = []
    next_bb_clv_at = {league: 0.0 for league in _bb_leagues}
    # Hockey framework CLV — AHL/PWHL/AIHL/NZIHL. Each league has its
    # own theScore-backed DB so the loop fans out per-league.
    _hk_leagues = ("ahl", "pwhl", "aihl", "nzihl")
    next_hk_clv_at = {league: 0.0 for league in _hk_leagues}
    # Tennis CLV — single bucket (no per-tournament split); HR ships
    # one tennis tree containing all ATP+WTA events together.
    next_tennis_clv_at = 0.0
    # Golf CLV — single bucket fan-out across active tours. Each tour
    # only fetches HR when one of its picks lacks closing odds.
    next_golf_clv_at = 0.0
    # Motorsports CLV — F1 today; IndyCar/NASCAR slot in once their
    # frameworks register a series config.
    next_motorsports_clv_at = 0.0
    # Worker takes ownership of calibration + closing-line prior refits
    # so cron skips don't degrade the model. Stagger the first refit
    # past startup like the roster refresh — backend warmup buffer.
    next_calibration_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    next_prior_refit_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    next_prop_calibration_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    # Elo refresh fires shortly after startup so the tournament window
    # is always within 24h of the latest eloratings.net snapshot.
    next_elo_refresh_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    next_props_gen_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    next_framework_refresh_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    next_tennis_refresh_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    next_event_log_sync_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    next_drift_scan_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S

    # Bulletproof Phase 2 — 24/7 emission timers.
    next_prematch_at = {sport: (time.monotonic() + _REFRESH_STARTUP_DELAY_S)
                         for sport in ("mlb", "nba", "nhl")}
    next_tennis_picks_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    next_tennis_settle_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    next_soccer_ht_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    next_football_slate_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    next_baseball_fw_slate_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S
    next_motorsports_settle_at = time.monotonic() + _REFRESH_STARTUP_DELAY_S

    logger.info("live_worker started — NBA state %ss / pbp %ss, "
                "NHL state %ss / pbp %ss, settle every %ss, "
                "roster refresh every %ss",
                _POLL_INTERVAL_S["nba"], _PBP_INTERVAL_S["nba"],
                _POLL_INTERVAL_S["nhl"], _PBP_INTERVAL_S["nhl"],
                _SETTLE_EVERY_S, _REFRESH_EVERY_S)

    while _running:
        now = time.monotonic()

        for sport, interval in _POLL_INTERVAL_S.items():
            if now >= next_due_at[sport]:
                _poll_sport(sport)
                next_due_at[sport] = time.monotonic() + interval

        for sport, interval in _PBP_INTERVAL_S.items():
            if now >= next_pbp_at[sport]:
                try:
                    _poll_pbp_sport(sport)
                except Exception as e:
                    logger.warning("pbp poll(%s) crashed: %s", sport, e)
                next_pbp_at[sport] = time.monotonic() + interval

        if now >= next_settle_at:
            for sport in _POLL_INTERVAL_S:
                try:
                    res = settle_live_picks(sport)
                    if res.get("settled", 0) > 0:
                        logger.info(
                            "settled %d live %s pick(s) (W=%d L=%d P=%d, "
                            "%d still pending)",
                            res["settled"], sport, res["wins"],
                            res["losses"], res["pushes"],
                            res["pending_remaining"],
                        )
                except Exception as e:
                    logger.warning("live settle(%s) failed: %s", sport, e)

            # Prematch + POTD settle. Was historically gated to the
            # user clicking the "Settle Completed" button on the tracker
            # tab — so NBA/NHL/MLB prematch picks + POTD rows piled up
            # pending whenever the user didn't visit the page. Worker
            # now runs all three settlers + POTD every minute. Idempotent;
            # only touches rows still pending at call time.
            for _sport in ("mlb", "nba", "nhl"):
                try:
                    if _sport == "mlb":
                        from engine.tracker import settle_picks as _pp_settle
                        r = _pp_settle()
                    elif _sport == "nba":
                        from engine.nba_tracker import settle_nba_picks as _pp_settle
                        r = _pp_settle()
                    elif _sport == "nhl":
                        from engine.nhl_tracker import settle_picks as _pp_settle
                        r = _pp_settle()
                    if isinstance(r, dict) and (r.get("settled") or 0) > 0:
                        logger.info("prematch settle %s: %s", _sport, r)
                except Exception as e:
                    logger.warning("prematch settle(%s) failed: %s",
                                   _sport, e)
                # Derivative settle — lives in a separate table
                # (`derivative_picks`) and was previously only triggered
                # by the UI's "Settle Completed" button. Without it,
                # F5 Team Total / Team Total / Inning Total etc. piled
                # up pending after the game ended (user flagged 2026-
                # 05-17). Same per-minute cadence as prematch settle.
                try:
                    from engine.derivative_tracker import settle_derivative_picks
                    r = settle_derivative_picks(_sport)
                    if isinstance(r, dict) and (r.get("settled") or 0) > 0:
                        logger.info("derivative settle %s: %s", _sport, r)
                except Exception as e:
                    logger.warning("derivative settle(%s) failed: %s",
                                   _sport, e)
                try:
                    from engine.pick_of_day import settle_potd
                    r = settle_potd(_sport)
                    if isinstance(r, dict) and (r.get("settled") or 0) > 0:
                        logger.info("POTD settle %s: %s", _sport, r)
                except Exception as e:
                    logger.warning("POTD settle(%s) failed: %s", _sport, e)

            next_settle_at = time.monotonic() + _SETTLE_EVERY_S

        if now >= next_purge_at:
            try:
                deleted = purge_stale(_PURGE_AFTER_S)
                if deleted:
                    logger.info("purged %d stale live_state row(s)", deleted)
            except Exception as e:
                logger.warning("live_state purge failed: %s", e)
            try:
                deleted_pbp = purge_old_pbp(_PURGE_AFTER_S)
                if deleted_pbp:
                    logger.info("purged %d stale live_pbp row(s)",
                                deleted_pbp)
            except Exception as e:
                logger.warning("live_pbp purge failed: %s", e)
            try:
                # Intermissions stay around 24h so the day's events are
                # inspectable in case of predictor debugging, but
                # anything older than that is just clutter.
                deleted_int = purge_old_intermissions(24 * 3600)
                if deleted_int:
                    logger.info("purged %d stale live_intermissions row(s)",
                                deleted_int)
            except Exception as e:
                logger.warning("live_intermissions purge failed: %s", e)
            next_purge_at = time.monotonic() + _PURGE_EVERY_S

        if now >= next_refresh_at:
            for sport in _REFRESH_PATHS:
                _run_refresh(sport)
            next_refresh_at = time.monotonic() + _REFRESH_EVERY_S

        if _placer_available and now >= next_placer_at:
            try:
                r = _placer_sweep()   # dry_run=None → follows kill switch
                if r.get("attempted", 0) > 0:
                    logger.info("queue placer sweep: %s", r)
                # Immediately poll for any queued/submitted placements
                # so terminal state (placed / rejected) lands within
                # one worker tick of the relay ACK.
                pr = _placer_poll()
                if pr.get("refreshed", 0) > 0:
                    logger.info("queue placer poll: %s", pr)
            except Exception as _e:
                logger.warning("queue placer sweep failed: %s", _e)
            next_placer_at = time.monotonic() + _PLACER_INTERVAL_S

        # Closing-odds capture — fire per-sport every minute while any
        # uncaptured pending pick exists for today. The cron path also
        # runs capture at sync time, but the cron's ~1/day cadence
        # missed the narrow window before HR drops the market. The
        # worker's per-minute heartbeat catches every tip across the
        # full CLV stack: NBA/NHL/MLB/WNBA/NCAAM/AFL (this loop) plus
        # tennis/soccer/golf/motorsports (separate loops below).
        # NHL CLV was 1.1% pre-fix (#166).
        for sport in _POLL_INTERVAL_S:
            if now >= next_clv_at[sport] and _has_pending_today(sport):
                _capture_closing_odds(sport)
                next_clv_at[sport] = time.monotonic() + _CLV_CAPTURE_INTERVAL_S

        # Motorsports CLV — F1 (single-series for now). Cheap fetch
        # since HR ships one race at a time.
        if now >= next_motorsports_clv_at:
            try:
                from engine.motorsports._db import get_conn as _mc
                pending = _mc("f1").execute(
                    "SELECT COUNT(*) FROM picks "
                    "WHERE result IS NULL AND closing_odds IS NULL"
                ).fetchone()[0]
                if pending:
                    from engine.motorsports._clv import capture_all as _ms_cap
                    res = _ms_cap()
                    captured = sum(int(v or 0) for v in res.values())
                    if captured:
                        logger.info("motorsports CLV: %s -> %d row(s)",
                                    dict(res), captured)
            except Exception as e:
                logger.warning("motorsports CLV crashed: %s", e)
            next_motorsports_clv_at = time.monotonic() + _CLV_CAPTURE_INTERVAL_S

        # Golf CLV — runs across all 3 tours when any one has a
        # pending pick without closing odds. HR golf is per-tournament.
        if now >= next_golf_clv_at:
            try:
                from engine.golf._db import get_conn as _gc
                pending_total = 0
                for _t in ("pga", "lpga", "kornferry"):
                    try:
                        c = _gc(_t)
                        pending_total += c.execute(
                            "SELECT COUNT(*) FROM picks "
                            "WHERE result IS NULL AND closing_odds IS NULL"
                        ).fetchone()[0]
                    except Exception:
                        continue
                if pending_total:
                    from engine.golf._clv import capture_all as _golf_capture
                    res = _golf_capture()
                    total_captured = sum(int(v or 0) for v in res.values())
                    if total_captured:
                        logger.info("golf CLV: %s -> %d row(s) captured",
                                    dict(res), total_captured)
            except Exception as e:
                logger.warning("golf CLV crashed: %s", e)
            next_golf_clv_at = time.monotonic() + _CLV_CAPTURE_INTERVAL_S

        # Basketball framework CLV — per-league fan-out. wnba/ncaam/afl
        # are caught here via the basketball._clv adapter instead of the
        # per-sport block at the top of _capture_closing_odds, plus every
        # newly-promoted minor league (germany_bbl/greece_a1/etc) is in
        # scope without further wiring.
        for league in _bb_leagues:
            if now >= next_bb_clv_at[league] \
                    and _has_pending_today_basketball(league):
                try:
                    from engine.basketball._clv import (
                        capture_closing_odds as _bb_capture,
                    )
                    n = _bb_capture(league)
                    if n:
                        logger.info(
                            "basketball CLV %s: %d row(s) captured",
                            league, n,
                        )
                except Exception as e:
                    logger.warning("basketball CLV(%s) crashed: %s",
                                   league, e)
                next_bb_clv_at[league] = (
                    time.monotonic() + _CLV_CAPTURE_INTERVAL_S)

        # Hockey framework CLV — per-league. Same fan-out shape as
        # the basketball framework; uses engine.hockey._clv with the
        # flat-key extractor (home_ml/away_ml/home_pl_odds/etc).
        for league in _hk_leagues:
            if now >= next_hk_clv_at[league] \
                    and _has_pending_today_hockey(league):
                try:
                    from engine.hockey._clv import (
                        capture_closing_odds as _hk_capture,
                    )
                    n = _hk_capture(league)
                    if n:
                        logger.info("hockey CLV %s: %d row(s) captured",
                                    league, n)
                except Exception as e:
                    logger.warning("hockey CLV(%s) crashed: %s",
                                   league, e)
                next_hk_clv_at[league] = (
                    time.monotonic() + _CLV_CAPTURE_INTERVAL_S)

        # Tennis CLV — single bucket, runs whenever a pending pick
        # lacks closing odds. HR tennis tree is one fetch for all events.
        if now >= next_tennis_clv_at:
            try:
                from engine.tennis_db import get_conn as _tc
                pending = _tc().execute(
                    "SELECT COUNT(*) FROM tennis_picks "
                    "WHERE result IS NULL AND closing_odds IS NULL"
                ).fetchone()[0]
                if pending:
                    from engine.tennis_clv import (
                        capture_closing_odds as _tennis_capture,
                    )
                    n = _tennis_capture()
                    if n:
                        logger.info("tennis CLV: %d row(s) captured", n)
            except Exception as e:
                logger.warning("tennis CLV crashed: %s", e)
            next_tennis_clv_at = time.monotonic() + _CLV_CAPTURE_INTERVAL_S

        # Soccer CLV — per-league. _has_pending_today_soccer + capture
        # use the soccer framework's CLV adapter; one HR fetch per
        # league so high-traffic leagues (Premier League / Champions
        # League) aren't gated on smaller markets' fetch cadence.
        for league in _soccer_leagues:
            if now >= next_soccer_clv_at[league] \
                    and _has_pending_today_soccer(league):
                try:
                    from engine.soccer._clv import (
                        capture_closing_odds as _soccer_capture,
                    )
                    n = _soccer_capture(league)
                    if n:
                        logger.info("soccer CLV %s: %d row(s) captured",
                                    league, n)
                except Exception as e:
                    logger.warning("soccer CLV(%s) crashed: %s",
                                   league, e)
                next_soccer_clv_at[league] = (
                    time.monotonic() + _CLV_CAPTURE_INTERVAL_S)

        # A8 calibration floor check — fires only when no recent refit
        # event covers the floor window.
        if now >= next_calibration_at:
            _run_calibration_refresh()
            next_calibration_at = time.monotonic() + _CALIBRATION_CHECK_EVERY_S

        # A8 closing-line prior floor check.
        if now >= next_prior_refit_at:
            _run_prior_refit()
            next_prior_refit_at = time.monotonic() + _PRIOR_REFIT_CHECK_EVERY_S

        # A8 player-prop shrinkage floor check.
        if now >= next_prop_calibration_at:
            _run_prop_calibration_refit()
            next_prop_calibration_at = (time.monotonic()
                                          + _PROP_CALIBRATION_CHECK_EVERY_S)

        # National-team Elo refresh from eloratings.net. Updates the
        # fifa_internationals.team_elo source-of-truth so soccer
        # leagues that read from it (WC, friendlies) see ratings that
        # reflect every result since yesterday's tick. Cheap-fail —
        # network blip just logs and retries next day.
        if now >= next_elo_refresh_at:
            try:
                from engine.soccer._elo_seed import seed as _elo_seed
                res = _elo_seed("fifa_internationals")
                logger.info(
                    "[elo_refresh] fifa_internationals updated=%d "
                    "missing_team=%d missing_rating=%d",
                    res.get("updated", 0),
                    len(res.get("missing_team", [])),
                    len(res.get("missing_rating", [])),
                )
            except Exception as e:
                logger.warning("[elo_refresh] failed: %s", e)
            next_elo_refresh_at = time.monotonic() + _ELO_REFRESH_EVERY_S

        # A8 player-prop generation floor check (per sport, 24h floor).
        if now >= next_props_gen_at:
            _run_player_props_generation()
            next_props_gen_at = (time.monotonic()
                                   + _PLAYER_PROPS_CHECK_EVERY_S)

        # Framework-league results refresh — hourly. Refreshes today's
        # slate for in-season WNBA/NCAAM/Euroleague/AHL/PWHL etc.
        if now >= next_framework_refresh_at:
            _run_framework_results_refresh()
            next_framework_refresh_at = (time.monotonic()
                                           + _FRAMEWORK_REFRESH_EVERY_S)

        # Tennis schedule refresh — flips completed matches to 'post'.
        if now >= next_tennis_refresh_at:
            _run_tennis_refresh()
            next_tennis_refresh_at = time.monotonic() + _TENNIS_REFRESH_EVERY_S

        # Event log sync — mirror legacy picks tables into events.db
        # so the canonical log keeps up with new picks + settles.
        if now >= next_event_log_sync_at:
            _run_event_log_sync()
            next_event_log_sync_at = time.monotonic() + _EVENT_LOG_SYNC_EVERY_S

        # Drift scan + consume — A3 self-healing loop.
        if now >= next_drift_scan_at:
            _run_drift_scan()
            next_drift_scan_at = time.monotonic() + _DRIFT_SCAN_EVERY_S

        # ── Bulletproof Phase 2 — 24/7 emission ──
        # Prematch picks for the Big-3 — was waiting on user clicks.
        for sport, due in list(next_prematch_at.items()):
            if now >= due:
                _run_prematch_picks(sport)
                next_prematch_at[sport] = (time.monotonic()
                                             + _PREMATCH_PICKS_EVERY_S)
        # Tennis picks emission (15m) + settle (5m).
        if now >= next_tennis_picks_at:
            _run_tennis_picks()
            next_tennis_picks_at = time.monotonic() + _TENNIS_PICKS_EVERY_S
        if now >= next_tennis_settle_at:
            _run_tennis_settle()
            next_tennis_settle_at = (time.monotonic()
                                       + _TENNIS_SETTLE_EVERY_S)
        # Soccer halftime drain — per-minute.
        if now >= next_soccer_ht_at:
            _run_soccer_halftime_drain()
            next_soccer_ht_at = (time.monotonic()
                                   + _SOCCER_HT_DRAIN_EVERY_S)
        # Football slate (UFL today) — 30m.
        if now >= next_football_slate_at:
            _run_football_slate()
            next_football_slate_at = (time.monotonic()
                                        + _FOOTBALL_SLATE_EVERY_S)
        # Baseball framework slate (college today) — 30m.
        if now >= next_baseball_fw_slate_at:
            _run_baseball_framework_slate()
            next_baseball_fw_slate_at = (time.monotonic()
                                           + _BASEBALL_SLATE_EVERY_S)
        # Motorsports settle — finalized races whose picks haven't graded.
        if now >= next_motorsports_settle_at:
            _run_motorsports_settle()
            next_motorsports_settle_at = (time.monotonic()
                                            + _MOTORSPORTS_SETTLE_EVERY_S)

        # Sleep until the next due tick across all timers. Cap at 1s
        # so the signal handler responds fast on Ctrl+C.
        upcoming = (list(next_due_at.values())
                    + list(next_pbp_at.values())
                    + list(next_clv_at.values())
                    + list(next_prematch_at.values())
                    + [next_settle_at, next_purge_at, next_refresh_at,
                       next_calibration_at, next_prior_refit_at,
                       next_prop_calibration_at,
                       next_tennis_refresh_at,
                       next_event_log_sync_at,
                       next_drift_scan_at,
                       next_props_gen_at,
                       next_framework_refresh_at,
                       next_tennis_picks_at, next_tennis_settle_at,
                       next_soccer_ht_at,
                       next_football_slate_at,
                       next_baseball_fw_slate_at,
                       next_motorsports_settle_at])
        sleep_s = max(0.5, min(1.0, min(upcoming) - time.monotonic()))
        time.sleep(sleep_s)

    logger.info("live_worker exited cleanly")


if __name__ == "__main__":
    main()
