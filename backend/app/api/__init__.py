"""
FastAPI API routers for the sports betting application.

Collects all route modules and exposes them for inclusion in the
main FastAPI application. Import ``all_routers`` to register every
router at once, or import individual routers by name.

Usage in main.py::

    from app.api import all_routers

    for router in all_routers:
        app.include_router(router)
"""

from app.api.data import router as data_router
from app.api.games import router as games_router
from app.api.injuries import router as injuries_router
from app.api.matchups import router as matchups_router
from app.api.ml import router as ml_router
from app.api.model_config import router as model_config_router
from app.api.predictions import router as predictions_router
from app.api.schedule import router as schedule_router
from app.api.stats import router as stats_router
from app.api.parlays import router as parlays_router
from app.api.player_props import router as player_props_router

all_routers = [
    schedule_router,
    games_router,
    predictions_router,
    stats_router,
    data_router,
    injuries_router,
    matchups_router,
    model_config_router,
    ml_router,
    player_props_router,
    parlays_router,
]

__all__ = [
    "all_routers",
    "schedule_router",
    "games_router",
    "predictions_router",
    "stats_router",
    "data_router",
    "injuries_router",
    "matchups_router",
    "model_config_router",
    "ml_router",
    "player_props_router",
    "parlays_router",
]
