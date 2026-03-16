"""
Real-time live updates via WebSocket and background odds scheduler.

Provides:
- WebSocket connection manager for broadcasting updates to all clients
- Background scheduler that automatically syncs odds at smart intervals
- Change detection so updates only push when data actually changes
"""

import asyncio
import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

from fastapi import WebSocket, WebSocketDisconnect

from app.utils import serialize_utc_datetime
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.constants import GAME_FINAL_STATUSES
from app.database import get_session_context, get_write_session_context
from app.models.game import Game

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# WebSocket Connection Manager
# ---------------------------------------------------------------------------

class ConnectionManager:
    """Manages WebSocket connections and broadcasts updates."""

    def __init__(self):
        self._connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._connections.add(ws)
        logger.info("WebSocket client connected (%d total)", len(self._connections))

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self._connections.discard(ws)
        logger.info("WebSocket client disconnected (%d remaining)", len(self._connections))

    @property
    def client_count(self) -> int:
        return len(self._connections)

    async def broadcast(self, message: Dict[str, Any]):
        """Send a JSON message to all connected clients."""
        if not self._connections:
            return
        payload = json.dumps(message)
        stale: List[WebSocket] = []
        async with self._lock:
            for ws in self._connections:
                try:
                    await ws.send_text(payload)
                except Exception:
                    stale.append(ws)
            for ws in stale:
                self._connections.discard(ws)


manager = ConnectionManager()


# ---------------------------------------------------------------------------
# Odds snapshot for change detection
# ---------------------------------------------------------------------------

_snapshot_lock = asyncio.Lock()
_last_odds_snapshot: Dict[int, Dict[str, Any]] = {}


def _snapshot_game_odds(game: Game) -> Dict[str, Any]:
    """Extract odds fields from a Game into a comparable dict."""
    return {
        "home_moneyline": game.home_moneyline,
        "away_moneyline": game.away_moneyline,
        "over_under_line": game.over_under_line,
        "over_price": game.over_price,
        "under_price": game.under_price,
        "home_spread_line": game.home_spread_line,
        "away_spread_line": game.away_spread_line,
        "home_spread_price": game.home_spread_price,
        "away_spread_price": game.away_spread_price,
    }


def _odds_changed_unlocked(game_id: int, current: Dict[str, Any]) -> bool:
    """Check if odds changed from last known snapshot.

    MUST be called while holding ``_snapshot_lock``.
    """
    previous = _last_odds_snapshot.get(game_id)
    if previous is None:
        return True
    return previous != current


# ---------------------------------------------------------------------------
# Background odds sync
# ---------------------------------------------------------------------------

_scheduler_task: Optional[asyncio.Task] = None
_scheduler_running = False


async def _sync_odds_and_broadcast(skip_alternates: bool = True):
    """Fetch latest odds, detect changes, broadcast.

    This is the fast path: odds-only, no prediction regeneration.
    Predictions are regenerated separately on a slower cadence.

    When ``skip_alternates`` is True (the default for fast cycles),
    per-event alternate line API calls are skipped and cached data is
    used instead, dramatically reducing Odds API credit consumption.

    Uses two separate sessions:
    1. Sync session — fetches odds, writes to DB, COMMITS immediately
       so that the REST API serves fresh data to polling clients.
    2. Read session — re-queries committed data for change detection
       and WebSocket broadcast.
    """
    global _last_odds_snapshot

    try:
        # Phase 1: Sync odds and COMMIT to DB immediately.
        # This ensures the /schedule/live endpoint returns fresh data
        # even before we broadcast via WebSocket.
        # Uses get_write_session_context() to hold the global write lock
        # for the entire transaction, preventing SQLite "database is locked".
        matched = []
        async with get_write_session_context() as session:
            today = date.today()

            # Check if there are any non-final games to sync
            games_result = await session.execute(
                select(func.count(Game.id)).where(
                    Game.date == today,
                    ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                )
            )
            game_count = games_result.scalar() or 0

            if not game_count:
                logger.debug("Odds sync: no non-final games today")
                return

            # Odds-only sync (no prediction regen) — force=True to
            # bypass the service-layer throttle since the scheduler
            # already controls pacing.
            from app.services.odds import sync_odds
            matched = await sync_odds(
                session, force=True, skip_alternates=skip_alternates,
            )
            # Session commits on exit via get_session_context()

        if not matched:
            logger.debug("Odds sync: no games matched from sportsbooks")
            return

        logger.info("Odds sync committed: %d games updated", len(matched))

        # Phase 2: Re-query committed data, detect changes, broadcast.
        async with get_session_context() as session:
            today = date.today()
            games_result = await session.execute(
                select(Game)
                .options(selectinload(Game.home_team), selectinload(Game.away_team))
                .where(
                    Game.date == today,
                    ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                )
            )
            games = games_result.scalars().all()

            # Detect changes and build update payload
            changed_games: List[Dict[str, Any]] = []
            async with _snapshot_lock:
                for game in games:
                    current = _snapshot_game_odds(game)
                    if _odds_changed_unlocked(game.id, current):
                        _last_odds_snapshot[game.id] = current
                        changed_games.append({
                            "game_id": game.id,
                            "home_abbrev": game.home_team.abbreviation if game.home_team else "",
                            "away_abbrev": game.away_team.abbreviation if game.away_team else "",
                            "status": game.status,
                            "odds": {
                                **current,
                                "odds_updated_at": serialize_utc_datetime(game.odds_updated_at),
                            },
                        })

            if not changed_games:
                return

            # Broadcast to all WebSocket clients
            if manager.client_count > 0:
                await manager.broadcast({
                    "type": "odds_update",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "changed_games": changed_games,
                })
                logger.info(
                    "Broadcast odds update: %d games changed, clients=%d",
                    len(changed_games), manager.client_count,
                )

    except Exception as exc:
        logger.error("Background odds sync failed: %s", exc, exc_info=True)


async def _sync_confirmed_starters():
    """Fetch confirmed starting goalies from the NHL API.

    Runs every 15 minutes when there are games today. The NHL typically
    confirms starters 1-3 hours before puck drop, so frequent checks
    during the pregame window catch these confirmations early.
    """
    try:
        async with get_session_context() as session:
            from app.scrapers.starter_scraper import sync_confirmed_starters
            starters = await sync_confirmed_starters(session)
            confirmed = [s for s in starters if s["confirmed"]]
            if confirmed:
                logger.info(
                    "Confirmed starters: %s",
                    ", ".join(f"{s['goalie_name']} ({s['team_abbrev']})" for s in confirmed),
                )
    except Exception as exc:
        logger.error("Starter sync failed: %s", exc, exc_info=True)


async def _sync_player_props():
    """Fetch and persist player prop odds for today's games.

    Runs every 30 minutes whenever there are games scheduled today,
    regardless of how far away game time is. Uses per-event Odds API
    calls (5 credits per game).
    """
    try:
        async with get_write_session_context() as session:
            from app.services.odds import sync_player_props
            count = await sync_player_props(session)
            if count:
                logger.info("Player props sync: %d lines updated", count)
    except Exception as exc:
        logger.error("Player props sync failed: %s", exc, exc_info=True)


async def _regenerate_predictions():
    """Regenerate predictions for today's non-final games.

    Runs on a slower cadence than odds sync since predictions don't
    need to update every 30 seconds.
    """
    try:
        async with get_write_session_context() as session:
            from datetime import date as date_type

            from sqlalchemy import delete as sa_delete

            from app.analytics.predictions import PredictionManager
            from app.models.prediction import Prediction

            today = date_type.today()

            async with session.begin_nested():
                non_final_ids = select(Game.id).where(
                    Game.date == today,
                    ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                )
                # Only delete live-phase predictions; preserve prematch
                # so the /schedule/today top_pick (bet tracker) persists
                # through the entire game lifecycle.
                await session.execute(
                    sa_delete(Prediction).where(
                        Prediction.game_id.in_(non_final_ids),
                        Prediction.phase != "prematch",
                    )
                )
                await session.flush()
                pm = PredictionManager()
                bets = await pm.get_best_bets(session)
                pred_count = len(bets) if bets else 0

            logger.info("Predictions regenerated: %d bets", pred_count)

            # Notify clients that predictions were updated
            if pred_count > 0 and manager.client_count > 0:
                await manager.broadcast({
                    "type": "predictions_update",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

    except Exception as exc:
        logger.error("Prediction regeneration failed: %s", exc, exc_info=True)


async def _settle_bets():
    """Auto-settle predictions and tracked bets for completed games.

    Runs after each scheduler cycle so bets are graded promptly
    when a game goes final.
    """
    try:
        from app.services.settlement import settle_completed_games

        async with get_write_session_context() as session:
            result = await settle_completed_games(session)

        total = result["predictions_graded"] + result["tracked_bets_settled"]
        if total > 0 and manager.client_count > 0:
            await manager.broadcast({
                "type": "settlements_update",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "predictions_graded": result["predictions_graded"],
                "tracked_bets_settled": result["tracked_bets_settled"],
            })
    except Exception as exc:
        logger.error("Auto-settlement failed: %s", exc, exc_info=True)


async def _check_auto_retrain():
    """Check if the ML model should be retrained and retrain if needed.

    Runs on a slow cadence (weekly / after enough new games). The
    actual decision logic lives in auto_retrain.py — this is just
    the scheduler wrapper that handles session management and logging.
    """
    try:
        from app.analytics.auto_retrain import auto_retrain_if_needed

        async with get_write_session_context() as session:
            result = await auto_retrain_if_needed(session)

        if result.get("retrained"):
            if result.get("improved"):
                logger.info(
                    "Auto-retrain: model improved (MAE %.4f -> %.4f, %d games)",
                    result.get("old_mae") or 0.0,
                    result["new_mae"],
                    result["games_used"],
                )
            else:
                logger.info(
                    "Auto-retrain: new model not better, kept old (MAE %.4f vs %.4f)",
                    result["new_mae"],
                    result.get("old_mae") or 0.0,
                )
        else:
            logger.debug("Auto-retrain: skipped — %s", result.get("reason", "unknown"))
    except Exception as exc:
        logger.error("Auto-retrain check failed: %s", exc, exc_info=True)


async def _run_full_data_sync():
    """Run a full data sync (schedule, teams, rosters, odds, injuries, predictions).

    Delegates to the same pipeline as the manual sync button, but runs
    automatically in the background. Also refreshes injury data.
    """
    try:
        from app.api.data import _run_full_sync, _sync_state
        if _sync_state.get("running"):
            logger.debug("Full sync already running, skipping")
            return
        await _run_full_sync()
        logger.info("Periodic full data sync completed")

        # Sync injury reports
        try:
            from app.scrapers.injury_scraper import fetch_injury_reports
            async with get_write_session_context() as session:
                count = await fetch_injury_reports(session)
                logger.info("Injury sync: %d records updated", count)
        except Exception as inj_exc:
            logger.error("Injury sync failed: %s", inj_exc)

        # Sync MoneyPuck 5v5 possession data
        try:
            from app.scrapers.moneypuck import sync_moneypuck_ev_stats
            async with get_write_session_context() as session:
                count = await sync_moneypuck_ev_stats(session)
                logger.info("MoneyPuck 5v5 sync: %d teams updated", count)
        except Exception as mp_exc:
            logger.error("MoneyPuck sync failed: %s", mp_exc)

        # Sync ESPN team stats (PP%, PK%, shots, faceoffs)
        try:
            from app.scrapers.espn import ESPNScraper
            espn = ESPNScraper()
            async with get_write_session_context() as session:
                count = await espn.sync_team_stats(session)
                logger.info("ESPN stats sync: %d teams updated", count)
            await espn.close()
        except Exception as espn_exc:
            logger.error("ESPN stats sync failed: %s", espn_exc)

    except Exception as exc:
        logger.error("Periodic full data sync failed: %s", exc, exc_info=True)


async def _scheduler_loop():
    """Adaptive scheduler: fast odds when live, slower predictions & full sync.

    Timing strategy (optimised for Odds API credit conservation):
    - Live games: odds every 60s (main lines only, alternates cached)
    - Pregame (within 2h of first game): odds every 120s
    - Idle (no games within 2h): odds every 10min
    - Off days (no games scheduled): no odds sync at all
    - Alternate lines: refreshed every 30min via full sync
    - Full data sync: every 30min, runs in a separate task

    Credit budget math (assuming 1 bulk API call per sync):
    - Live (~3h window): 3×60 = 180 syncs × 1 call = 180 credits
    - Pregame (~4h): 4×30 = 120 syncs × 1 call = 120 credits
    - Idle: negligible (~6/hr × remaining hours)
    - Alt-line refresh: ~8 calls every 60min = ~8/hr
    - Daily budget target: ~200-400 credits/game day (20K plan = ~650/day)
    """
    global _scheduler_running

    LIVE_INTERVAL = 120      # 120 seconds when games are live (was 60)
    PREGAME_INTERVAL = 300   # 5 minutes for pregame odds (was 120)
    IDLE_INTERVAL = 900      # 15 minutes when no games within 2h (was 600)
    OFF_DAY_INTERVAL = 3600  # 60 minutes on off days (was 1800)
    PREGAME_WINDOW_HOURS = 2 # Start syncing 2h before first game
    PRED_REGEN_INTERVAL = 300  # 5 minutes between prediction regenerations
    FULL_SYNC_INTERVAL = 3600  # 60 minutes for full data refresh (was 1800)
    ALT_REFRESH_INTERVAL = 3600  # 60 min — refresh alternate lines (was 1800)
    PROPS_SYNC_INTERVAL = 3600   # 60 min — sync player props (was 1800)
    STARTER_SYNC_INTERVAL = 900  # 15 min — check confirmed starters
    RETRAIN_CHECK_INTERVAL = 86400  # 24h — check if ML model needs retraining

    _scheduler_running = True
    logger.info("Live odds scheduler started (credit-optimised)")

    loop = asyncio.get_event_loop()

    # Launch the full data sync as a BACKGROUND task so it never blocks
    # the fast odds polling loop.  Previous behaviour was to await the
    # full sync here — if the H2H historical sync took 30-60+ minutes,
    # the odds loop wouldn't start until it finished, leaving live odds
    # stale the entire time.
    _full_sync_task: Optional[asyncio.Task] = asyncio.create_task(
        _run_full_data_sync()
    )
    last_full_sync = loop.time()
    last_pred_regen = loop.time()
    last_alt_refresh = 0.0  # force alt refresh on first sync
    last_props_sync = 0.0   # force props sync on first cycle with games
    last_starter_sync = 0.0  # force starter sync on first cycle with games
    last_retrain_check = 0.0  # force retrain check on first idle cycle
    _iteration = 0

    # Brief pause to let the full sync populate today's schedule
    # before we start querying for games.
    await asyncio.sleep(5)

    while _scheduler_running:
        try:
            _iteration += 1
            cycle_start = loop.time()

            # Determine current state: live / pregame / idle / off-day
            interval = OFF_DAY_INTERVAL
            live_count = 0
            upcoming_count = 0
            games_within_window = False
            try:
                async with get_session_context() as session:
                    today = date.today()

                    live_result = await session.execute(
                        select(func.count(Game.id)).where(
                            Game.date == today,
                            func.lower(Game.status).in_(("in_progress", "live")),
                        )
                    )
                    live_count = live_result.scalar() or 0

                    if live_count > 0:
                        interval = LIVE_INTERVAL
                    else:
                        # Check for upcoming (non-final) games today
                        upcoming_result = await session.execute(
                            select(func.count(Game.id)).where(
                                Game.date == today,
                                ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                            )
                        )
                        upcoming_count = upcoming_result.scalar() or 0

                        if upcoming_count > 0:
                            # Check if any game starts within the pregame window
                            now_utc = datetime.now(timezone.utc)
                            window_cutoff = now_utc + timedelta(
                                hours=PREGAME_WINDOW_HOURS
                            )
                            near_result = await session.execute(
                                select(func.count(Game.id)).where(
                                    Game.date == today,
                                    ~func.lower(Game.status).in_(
                                        GAME_FINAL_STATUSES
                                    ),
                                    Game.start_time <= window_cutoff,
                                )
                            )
                            near_count = near_result.scalar() or 0
                            if near_count > 0:
                                games_within_window = True
                                interval = PREGAME_INTERVAL
                            else:
                                # Games today but not for a while — idle
                                interval = IDLE_INTERVAL
                        # else: no games today → OFF_DAY_INTERVAL
            except Exception as exc:
                logger.warning("Scheduler interval check failed: %s", exc)
                interval = IDLE_INTERVAL  # safe fallback

            # Decide whether to sync odds at all
            should_sync = live_count > 0 or games_within_window
            has_games_today = live_count > 0 or upcoming_count > 0

            # On idle/off-day, skip odds sync entirely to save credits.
            # The full data sync (every 30min) still runs and will pick
            # up schedule changes.
            if should_sync:
                # Decide whether this cycle should also refresh alt lines.
                # Alt lines are refreshed every ALT_REFRESH_INTERVAL or on
                # the first sync of the session.
                now = loop.time()
                need_alt_refresh = (
                    now - last_alt_refresh >= ALT_REFRESH_INTERVAL
                )
                await _sync_odds_and_broadcast(
                    skip_alternates=not need_alt_refresh,
                )
                if need_alt_refresh:
                    last_alt_refresh = now
                    logger.info("Alt-line cache refreshed")

            # Sync player props:
            # - Always on first cycle (last_props_sync == 0) so picks are
            #   available immediately after startup
            # - When games exist but none are live (prematch-only market)
            # - Never mid-game to conserve Odds API credits
            first_cycle = last_props_sync == 0.0
            if first_cycle or (has_games_today and live_count == 0):
                now = loop.time()
                if now - last_props_sync >= PROPS_SYNC_INTERVAL:
                    await _sync_player_props()
                    last_props_sync = now
                if now - last_starter_sync >= STARTER_SYNC_INTERVAL:
                    await _sync_confirmed_starters()
                    last_starter_sync = now

            # Heartbeat log every 10 iterations (or every iteration
            # when games are live) for observability.
            if live_count > 0 or _iteration % 10 == 0:
                state = (
                    "LIVE" if live_count > 0
                    else "PREGAME" if games_within_window
                    else "IDLE" if upcoming_count > 0
                    else "OFF_DAY"
                )
                logger.info(
                    "Scheduler heartbeat: iter=%d, interval=%ds, state=%s, "
                    "live=%d, upcoming=%d, clients=%d",
                    _iteration, interval, state, live_count,
                    upcoming_count, manager.client_count,
                )

            # Regenerate predictions on a slower cadence
            now = loop.time()
            if now - last_pred_regen >= PRED_REGEN_INTERVAL:
                await _regenerate_predictions()
                last_pred_regen = now

            # Auto-settle bets for any games that just went final.
            # Runs every cycle (lightweight — only queries for unsettled).
            await _settle_bets()

            # Auto-retrain ML model check — runs daily (or after enough
            # new settled games). The actual retrain is CPU-bound but
            # short (~10-30s), so we run it inline on a slow cadence.
            now = loop.time()
            if now - last_retrain_check >= RETRAIN_CHECK_INTERVAL:
                await _check_auto_retrain()
                last_retrain_check = now

            # Periodic full data sync — run as background task so it
            # never blocks the fast odds loop
            now = loop.time()
            if now - last_full_sync >= FULL_SYNC_INTERVAL:
                if _full_sync_task is None or _full_sync_task.done():
                    _full_sync_task = asyncio.create_task(_run_full_data_sync())
                last_full_sync = now

            # Sleep only the remaining time in the interval, accounting
            # for how long the sync took
            elapsed = loop.time() - cycle_start
            sleep_time = max(0, interval - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Scheduler loop error: %s", exc, exc_info=True)
            await asyncio.sleep(60)

    logger.info("Live odds scheduler stopped")


async def start_scheduler():
    """Start the background odds scheduler."""
    global _scheduler_task
    if _scheduler_task is not None and not _scheduler_task.done():
        return
    _scheduler_task = asyncio.create_task(_scheduler_loop())


async def stop_scheduler():
    """Stop the background odds scheduler."""
    global _scheduler_running, _scheduler_task
    _scheduler_running = False
    if _scheduler_task is not None:
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass
        _scheduler_task = None


def scheduler_status() -> Dict[str, Any]:
    """Return scheduler health info for the /health endpoint."""
    task_alive = _scheduler_task is not None and not _scheduler_task.done()
    return {
        "scheduler_running": _scheduler_running,
        "scheduler_task_alive": task_alive,
        "websocket_clients": manager.client_count,
    }


async def ensure_scheduler_alive():
    """Restart the scheduler if it died.  Called from the health endpoint."""
    global _scheduler_task
    if _scheduler_running and (_scheduler_task is None or _scheduler_task.done()):
        logger.warning("Scheduler task died — restarting automatically")
        _scheduler_task = asyncio.create_task(_scheduler_loop())


# ---------------------------------------------------------------------------
# WebSocket endpoint handler
# ---------------------------------------------------------------------------

async def websocket_handler(ws: WebSocket):
    """Handle a WebSocket connection for live updates."""
    await manager.connect(ws)
    try:
        # Send initial state on connect
        try:
            async with get_session_context() as session:
                today = date.today()
                games_result = await session.execute(
                    select(Game)
                    .options(selectinload(Game.home_team), selectinload(Game.away_team))
                    .where(
                        Game.date == today,
                        ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                    )
                )
                games = games_result.scalars().all()

                initial_games = []
                async with _snapshot_lock:
                    for g in games:
                        snap = _snapshot_game_odds(g)
                        _last_odds_snapshot[g.id] = snap
                        initial_games.append({
                            "game_id": g.id,
                            "home_abbrev": g.home_team.abbreviation if g.home_team else "",
                            "away_abbrev": g.away_team.abbreviation if g.away_team else "",
                            "status": g.status,
                            "home_score": g.home_score,
                            "away_score": g.away_score,
                            "period": g.period,
                            "clock": g.clock,
                            "odds": {
                                **snap,
                                "odds_updated_at": serialize_utc_datetime(g.odds_updated_at),
                            },
                        })

                await ws.send_text(json.dumps({
                    "type": "initial_state",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "games": initial_games,
                }))
        except WebSocketDisconnect:
            return
        except Exception as exc:
            logger.warning("Failed to send initial state: %s", exc)

        # Keep connection alive; listen for client messages (ping/pong)
        while True:
            try:
                msg = await ws.receive_text()
                if msg == "ping":
                    await ws.send_text(json.dumps({"type": "pong"}))
            except (WebSocketDisconnect, RuntimeError):
                break
    finally:
        await manager.disconnect(ws)
