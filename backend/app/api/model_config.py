"""
API endpoints for viewing and updating model configuration.

Allows runtime inspection and adjustment of all prediction model
parameters — weights, thresholds, and tuning constants — without
requiring a code deployment.
"""

import logging
from typing import Any, Dict

from fastapi import APIRouter
from pydantic import ValidationError

from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/config", tags=["config"])


@router.get("/model")
async def get_model_config() -> Dict[str, Any]:
    """Get current model configuration values.

    Returns all tunable parameters organized by category:
    model constants, injury settings, matchup settings, and
    prediction thresholds.
    """
    return {
        "model": settings.model.model_dump(),
        "injury": settings.injury.model_dump(),
        "matchup": settings.matchup.model_dump(),
        "prediction_thresholds": {
            "min_confidence": settings.min_confidence,
            "min_edge": settings.min_edge,
            "best_bet_edge": settings.best_bet_edge,
            "best_bet_max_favorite": settings.best_bet_max_favorite,
            "best_bet_max_implied": settings.best_bet_max_implied,
        },
    }


@router.put("/model")
async def update_model_config(
    updates: Dict[str, Any],
) -> Dict[str, Any]:
    """Update model configuration values at runtime.

    Accepts a partial dict of values to update. Only specified keys
    are changed; others keep their current values.

    The updates dict can have top-level keys:
    - "model": ModelConfig fields
    - "injury": InjuryConfig fields
    - "matchup": MatchupConfig fields
    - "thresholds": prediction threshold fields

    Example:
        {
            "model": {
                "home_ice_advantage": 0.18,
                "h2h_factor": 0.12
            },
            "injury": {
                "max_injury_reduction": 0.25
            }
        }

    Returns the full updated configuration.
    """
    changed = []

    if "model" in updates and isinstance(updates["model"], dict):
        for key, value in updates["model"].items():
            if hasattr(settings.model, key):
                old_val = getattr(settings.model, key)
                setattr(settings.model, key, value)
                changed.append(f"model.{key}: {old_val} -> {value}")
                logger.info("Config updated: model.%s = %s (was %s)", key, value, old_val)

    if "injury" in updates and isinstance(updates["injury"], dict):
        for key, value in updates["injury"].items():
            if hasattr(settings.injury, key):
                old_val = getattr(settings.injury, key)
                setattr(settings.injury, key, value)
                changed.append(f"injury.{key}: {old_val} -> {value}")
                logger.info("Config updated: injury.%s = %s (was %s)", key, value, old_val)

    if "matchup" in updates and isinstance(updates["matchup"], dict):
        for key, value in updates["matchup"].items():
            if hasattr(settings.matchup, key):
                old_val = getattr(settings.matchup, key)
                setattr(settings.matchup, key, value)
                changed.append(f"matchup.{key}: {old_val} -> {value}")
                logger.info("Config updated: matchup.%s = %s (was %s)", key, value, old_val)

    if "thresholds" in updates and isinstance(updates["thresholds"], dict):
        for key in ["min_confidence", "min_edge", "best_bet_edge",
                     "best_bet_max_favorite", "best_bet_max_implied"]:
            if key in updates["thresholds"]:
                old_val = getattr(settings, key)
                setattr(settings, key, updates["thresholds"][key])
                changed.append(f"thresholds.{key}: {old_val} -> {updates['thresholds'][key]}")
                logger.info("Config updated: %s = %s (was %s)",
                            key, updates["thresholds"][key], old_val)

    return {
        "status": "updated",
        "changes": changed,
        "config": await get_model_config(),
    }


@router.post("/model/reset")
async def reset_model_config() -> Dict[str, Any]:
    """Reset all model configuration to default values."""
    from app.config import ModelConfig, InjuryConfig, MatchupConfig

    settings.model = ModelConfig()
    settings.injury = InjuryConfig()
    settings.matchup = MatchupConfig()

    logger.info("Model configuration reset to defaults")

    return {
        "status": "reset",
        "config": await get_model_config(),
    }


@router.get("/model/backtest")
async def backtest_current(
    days_back: int = 90,
    limit: int = 200,
) -> Dict[str, Any]:
    """Run a backtest with the current model parameters.

    Evaluates predictions against historical completed games and reports
    hit rate, ROI, and log-loss metrics.
    """
    from app.analytics.backtest import run_backtest_api
    from app.database import get_session_context

    async with get_session_context() as db:
        return await run_backtest_api(db, days_back=days_back, limit=limit)


@router.post("/model/grid-search")
async def grid_search(
    days_back: int = 90,
    limit: int = 200,
    quick: bool = True,
) -> Dict[str, Any]:
    """Run a grid search to find optimal parameters.

    Tests combinations of key model parameters against historical data.
    Set quick=false for a more comprehensive (but slower) search.

    Returns the top 5 parameter combinations ranked by log-loss.
    """
    from app.analytics.backtest import run_grid_search_api
    from app.database import get_session_context

    async with get_session_context() as db:
        return await run_grid_search_api(
            db, days_back=days_back, limit=limit, quick=quick
        )
