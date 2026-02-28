"""
FastAPI application entry point.

Configures the app with CORS middleware, registers all API routers,
and wires up startup/shutdown lifecycle events for database initialization.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.config import settings
from app.database import close_db, init_db

# Configure root logger so all app.* module loggers output to console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
# Suppress noisy SQLAlchemy engine logs (SQL statements) so app-level
# diagnostic messages (odds sync, predictions, etc.) are visible.
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings.db_dir.mkdir(parents=True, exist_ok=True)
    await init_db()
    yield
    await close_db()


class HealthResponse(BaseModel):
    status: str
    app_name: str
    version: str
    sport: str
    timestamp: str


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
        return HealthResponse(
            status="healthy",
            app_name=settings.app_name,
            version=settings.app_version,
            sport=settings.default_sport,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    # Register all API routers from individual modules
    from app.api import all_routers

    for router in all_routers:
        application.include_router(router)

    # Also register the basic CRUD routes
    from app.api.routes import (
        games_router,
        players_router,
        predictions_router,
        teams_router,
    )

    application.include_router(teams_router)
    application.include_router(games_router)
    application.include_router(players_router)
    application.include_router(predictions_router)

    # Static files
    static_dir = Path(__file__).resolve().parent.parent / "static"
    if static_dir.is_dir():
        application.mount(
            "/static", StaticFiles(directory=str(static_dir)), name="static"
        )

    return application


app = create_app()
