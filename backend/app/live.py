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
    """Fetch latest odds via service layer, detect changes, broadcast."""
    global _last_odds_snapshot

    try:
        async with get_session_context() as session:
            today = date.today()

            # Snapshot pre-sync state
            games_result = await session.execute(
                select(Game)
                .options(selectinload(Game.home_team), selectinload(Game.away_team))
                .where(
                    Game.date == today,
                    ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                )
            )
            games = games_result.scalars().all()

            if not games:
                return

            for g in games:
                _snapshot_game_odds(g)  # populate pre-sync snapshots

            # Use service layer for sync + prediction regen
            from app.services.odds import sync_odds_and_regenerate
            matched, pred_count = await sync_odds_and_regenerate(session)

            if not matched:
                return

            # Re-query to get updated values
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
                                "odds_updated_at": game.odds_updated_at.isoformat() if game.odds_updated_at else None,
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
                    "predictions_updated": pred_count > 0,
                })
                logger.info(
                    "Broadcast odds update: %d games changed, predictions=%d, clients=%d",
                    len(changed_games), pred_count, manager.client_count,
                )

    except Exception as exc:
        logger.error("Background odds sync failed: %s", exc, exc_info=True)


async def _scheduler_loop():
    """Adaptive scheduler: fast when live, moderate for pregame, idle otherwise."""
    global _scheduler_running

    LIVE_INTERVAL = 30       # 30 seconds when games are live
    PREGAME_INTERVAL = 120   # 2 minutes for pregame odds
    IDLE_INTERVAL = 300      # 5 minutes when nothing happening

    _scheduler_running = True
    logger.info("Live odds scheduler started")

    while _scheduler_running:
        try:
            # Determine current interval based on game state
            interval = IDLE_INTERVAL
            try:
                async with get_session_context() as session:
                    today = date.today()

                    live_result = await session.execute(
                        select(func.count(Game.id)).where(
                            func.lower(Game.status).in_(("in_progress", "live"))
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

            # Only sync if there are clients connected or games are live
            has_clients = manager.client_count > 0
            has_live = interval == LIVE_INTERVAL

            if has_clients or has_live:
                await _sync_odds_and_broadcast()

            await asyncio.sleep(interval)

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
                                "odds_updated_at": g.odds_updated_at.isoformat() if g.odds_updated_at else None,
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
