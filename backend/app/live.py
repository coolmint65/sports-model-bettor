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
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Set

from fastapi import WebSocket, WebSocketDisconnect

from app.utils import serialize_utc_datetime
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.constants import GAME_FINAL_STATUSES
from app.database import get_session_context
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


def _odds_changed(game_id: int, current: Dict[str, Any]) -> bool:
    """Check if odds changed from last known snapshot."""
    previous = _last_odds_snapshot.get(game_id)
    if previous is None:
        return True
    return previous != current


# ---------------------------------------------------------------------------
# Background odds sync
# ---------------------------------------------------------------------------

_scheduler_task: Optional[asyncio.Task] = None
_scheduler_running = False


async def _sync_odds_and_broadcast():
    """Fetch latest odds, detect changes, broadcast.

    This is the fast path: odds-only, no prediction regeneration.
    Predictions are regenerated separately on a slower cadence.

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
        matched = []
        async with get_session_context() as session:
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
            matched = await sync_odds(session, force=True)
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
                    if _odds_changed(game.id, current):
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


async def _regenerate_predictions():
    """Regenerate predictions for today's non-final games.

    Runs on a slower cadence than odds sync since predictions don't
    need to update every 30 seconds.
    """
    try:
        async with get_session_context() as session:
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
                await session.execute(
                    sa_delete(Prediction).where(
                        Prediction.game_id.in_(non_final_ids)
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


async def _run_full_data_sync():
    """Run a full data sync (schedule, teams, rosters, odds, predictions).

    Delegates to the same pipeline as the manual sync button, but runs
    automatically in the background.
    """
    try:
        from app.api.data import _run_full_sync, _sync_state
        if _sync_state.get("running"):
            logger.debug("Full sync already running, skipping")
            return
        await _run_full_sync()
        logger.info("Periodic full data sync completed")
    except Exception as exc:
        logger.error("Periodic full data sync failed: %s", exc, exc_info=True)


async def _scheduler_loop():
    """Adaptive scheduler: fast odds when live, slower predictions & full sync.

    Timing strategy:
    - Live games: odds every 30s, predictions every 5min
    - Pregame: odds every 90s, predictions every 5min
    - Idle: odds every 5min, predictions every 10min
    - Full data sync: every 30min, runs in a separate task so it never
      blocks the odds loop
    """
    global _scheduler_running

    LIVE_INTERVAL = 30       # 30 seconds when games are live
    PREGAME_INTERVAL = 90    # 90 seconds for pregame odds
    IDLE_INTERVAL = 300      # 5 minutes when nothing happening
    PRED_REGEN_INTERVAL = 300  # 5 minutes between prediction regenerations
    FULL_SYNC_INTERVAL = 1800  # 30 minutes for full data refresh

    _scheduler_running = True
    logger.info("Live odds scheduler started")

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
    _iteration = 0

    # Brief pause to let the full sync populate today's schedule
    # before we start querying for games.
    await asyncio.sleep(5)

    while _scheduler_running:
        try:
            _iteration += 1
            cycle_start = loop.time()

            # Determine current interval based on game state
            interval = IDLE_INTERVAL
            live_count = 0
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
                        upcoming_result = await session.execute(
                            select(func.count(Game.id)).where(
                                Game.date == today,
                                ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                            )
                        )
                        upcoming = upcoming_result.scalar() or 0
                        if upcoming > 0:
                            interval = PREGAME_INTERVAL
            except Exception as exc:
                logger.warning("Scheduler interval check failed: %s", exc)

            # Always sync odds — the interval already adjusts pacing.
            # Even in "idle" mode, keep syncing so pregame odds stay
            # fresh for any games that appear.
            await _sync_odds_and_broadcast()

            # Heartbeat log every 10 iterations (or every iteration
            # when games are live) for observability.
            if live_count > 0 or _iteration % 10 == 0:
                logger.info(
                    "Scheduler heartbeat: iter=%d, interval=%ds, "
                    "live=%d, clients=%d",
                    _iteration, interval, live_count,
                    manager.client_count,
                )

            # Regenerate predictions on a slower cadence
            now = loop.time()
            if now - last_pred_regen >= PRED_REGEN_INTERVAL:
                await _regenerate_predictions()
                last_pred_regen = now

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
        except Exception as exc:
            logger.warning("Failed to send initial state: %s", exc)

        # Keep connection alive; listen for client messages (ping/pong)
        while True:
            try:
                msg = await ws.receive_text()
                if msg == "ping":
                    await ws.send_text(json.dumps({"type": "pong"}))
            except WebSocketDisconnect:
                break
    finally:
        await manager.disconnect(ws)
