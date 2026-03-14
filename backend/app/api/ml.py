"""
API endpoints for the ML model layer.

Provides endpoints to trigger training, check model status, and
inspect feature importance.
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session

router = APIRouter(prefix="/api/ml", tags=["ml"])


@router.post("/train")
async def train_model(
    days_back: int = Query(default=365, ge=30, le=730, description="Days of history to use"),
    limit: int = Query(default=2000, ge=50, le=5000, description="Max games to train on"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Train the ML model from historical game data.

    Builds feature vectors from completed games, trains gradient boosted
    tree models for home/away xG, and saves the result to disk.
    """
    from app.analytics.ml_training import train_ml_model

    result = await train_ml_model(session, days_back=days_back, limit=limit)
    return result


@router.get("/status")
async def model_status() -> Dict[str, Any]:
    """Get the current ML model status, training metrics, and feature info."""
    from app.analytics.ml_model import MLModel
    from app.config import settings

    model = MLModel()
    loaded = model.load(settings.model.ml_model_path)

    if not loaded:
        return {
            "is_trained": False,
            "model_path": settings.model.ml_model_path,
            "blend_weight": settings.model.ml_blend_weight,
            "message": "No trained model found. Use POST /api/ml/train to train one.",
        }

    status = model.status()
    status["model_path"] = settings.model.ml_model_path
    status["blend_weight"] = settings.model.ml_blend_weight
    return status


@router.get("/features")
async def feature_info() -> Dict[str, Any]:
    """List all features used by the ML model."""
    from app.analytics.ml_features import get_feature_names

    names = get_feature_names()
    return {
        "feature_count": len(names),
        "features": names,
    }


@router.get("/backtest")
async def ml_backtest_comparison(
    days_back: int = Query(default=90, ge=14, le=365, description="Days of history"),
    limit: int = Query(default=200, ge=20, le=1000, description="Max games"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Compare Poisson-only vs ML-blended vs ML-heavy backtests.

    Runs three backtests on the same games and returns side-by-side
    metrics so you can evaluate the ML model's impact.
    """
    from app.analytics.backtest import run_ml_comparison_api

    return await run_ml_comparison_api(session, days_back=days_back, limit=limit)
