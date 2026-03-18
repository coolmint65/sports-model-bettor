"""
FastAPI application entry point.

Configures the app with CORS middleware, registers all API routers,
and wires up startup/shutdown lifecycle events for database initialization.
"""

import logging
import logging.handlers
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.config import DATA_DIR, settings
from app.database import close_db, init_db
from app.live import (
    ensure_scheduler_alive,
    manager,
    scheduler_status,
    start_scheduler,
    stop_scheduler,
    websocket_handler,
)

# Configure root logger so all app.* module loggers output to console and file
_log_fmt = logging.Formatter(
    "%(asctime)s %(levelname)-8s [%(name)s] %(message)s", datefmt="%H:%M:%S"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)

# Rotating file handler: 5 MB per file, keep last 3 rotations.
# On Windows the backup file may be locked by another process, so we
# subclass to swallow PermissionError during rollover instead of crashing.
class _SafeRotatingFileHandler(logging.handlers.RotatingFileHandler):
    def doRollover(self):
        try:
            super().doRollover()
        except PermissionError:
            # Another process holds the log file open (common on Windows).
            # Continue writing to the current file rather than crashing.
            pass

DATA_DIR.mkdir(parents=True, exist_ok=True)
_file_handler = _SafeRotatingFileHandler(
    DATA_DIR / "app.log", maxBytes=5 * 1024 * 1024, backupCount=3
)
_file_handler.setFormatter(_log_fmt)
_file_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(_file_handler)

# Suppress noisy SQLAlchemy engine logs (SQL statements) so app-level
# diagnostic messages (odds sync, predictions, etc.) are visible.
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

# Suppress per-request httpx logs (HTTP Request: GET ... 200 OK) that
# flood the console during odds/roster sync.
logging.getLogger("httpx").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings.db_dir.mkdir(parents=True, exist_ok=True)
    await init_db()
    await start_scheduler()
    yield
    await stop_scheduler()
    from app.cache import close_cache_db
    await close_cache_db()
    await close_db()


class HealthResponse(BaseModel):
    status: str
    app_name: str
    version: str
    sport: str
    timestamp: str
    scheduler_running: bool = False
    scheduler_task_alive: bool = False
    websocket_clients: int = 0


def create_app() -> FastAPI:
    application = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description="Sports betting model API with NHL predictions and analytics.",
        lifespan=lifespan,
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Health check
    @application.get("/health", response_model=HealthResponse, tags=["health"])
    async def health_check():
        # Auto-restart scheduler if it died
        await ensure_scheduler_alive()
        sched = scheduler_status()
        return HealthResponse(
            status="healthy",
            app_name=settings.app_name,
            version=settings.app_version,
            sport=settings.default_sport,
            timestamp=datetime.now(timezone.utc).isoformat(),
            scheduler_running=sched["scheduler_running"],
            scheduler_task_alive=sched["scheduler_task_alive"],
            websocket_clients=sched["websocket_clients"],
        )

    # Register all API routers from individual modules
    from app.api import all_routers

    for router in all_routers:
        application.include_router(router)

    # Also register the basic CRUD routes
    from app.api.routes import (
        games_router,
        health_router,
        players_router,
        predictions_router,
        teams_router,
    )

    application.include_router(teams_router)
    application.include_router(games_router)
    application.include_router(players_router)
    application.include_router(predictions_router)
    application.include_router(health_router)

    # Client error reporting endpoint
    client_error_logger = logging.getLogger("client_errors")

    @application.post("/api/client-error", tags=["health"])
    async def report_client_error(request: Request):
        try:
            body = await request.json()
            client_error_logger.error(
                "Frontend error: %s | url=%s | stack=%s",
                body.get("error", "unknown"),
                body.get("url", ""),
                (body.get("stack") or "")[:500],
            )
        except Exception:
            pass
        return {"ok": True}

    # WebSocket endpoint for live updates
    from fastapi import WebSocket as WS

    @application.websocket("/ws/live")
    async def ws_live(ws: WS):
        await websocket_handler(ws)

    # Static files
    static_dir = Path(__file__).resolve().parent.parent / "static"
    if static_dir.is_dir():
        application.mount(
            "/static", StaticFiles(directory=str(static_dir)), name="static"
        )

    return application


app = create_app()
