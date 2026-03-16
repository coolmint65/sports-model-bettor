"""
Auto-retrain feedback loop for the ML model.

Periodically checks whether enough new settled games have accumulated
since the last training run. If so, retrains the model, compares
cross-validated metrics against the current model, and only promotes
the new model if it improves on the old one.

This closes the feedback loop: outcomes flow back into the model
so it continuously learns from its mistakes.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.constants import GAME_FINAL_STATUSES
from app.models.game import Game

logger = logging.getLogger(__name__)

# Retrain triggers.
MIN_NEW_GAMES_THRESHOLD = 50
MAX_DAYS_BETWEEN_TRAINS = 14


async def should_retrain(db: AsyncSession) -> bool:
    """Decide whether the ML model should be retrained.

    Returns True when either:
    - 50+ new settled games exist since the last training, OR
    - 14+ days have elapsed since the last training.

    If no model exists on disk yet, always returns True (initial train).
    """
    from app.analytics.ml_model import MLModel

    model = MLModel()
    model_path = settings.model.ml_model_path
    loaded = model.load(model_path)

    if not loaded:
        logger.info("No existing ML model found — retrain needed")
        return True

    # Determine how many settled games exist.
    total_settled = await _count_settled_games(db)

    # How many games the current model was trained on.
    games_used = 0
    if model.metrics:
        games_used = model.metrics.n_samples

    new_games = total_settled - games_used
    if new_games >= MIN_NEW_GAMES_THRESHOLD:
        logger.info(
            "Retrain trigger: %d new settled games since last train (%d total, %d used)",
            new_games, total_settled, games_used,
        )
        return True

    # Check time since last training.
    if model.trained_at:
        try:
            trained_dt = datetime.fromisoformat(
                model.trained_at.replace("Z", "+00:00")
            )
            days_since = (datetime.now(timezone.utc) - trained_dt).days
            if days_since >= MAX_DAYS_BETWEEN_TRAINS:
                logger.info(
                    "Retrain trigger: %d days since last train (threshold=%d)",
                    days_since, MAX_DAYS_BETWEEN_TRAINS,
                )
                return True
        except (ValueError, TypeError):
            # Can't parse trained_at — treat as stale.
            logger.warning("Could not parse trained_at='%s', triggering retrain", model.trained_at)
            return True

    return False


async def auto_retrain_if_needed(db: AsyncSession) -> Dict[str, Any]:
    """Check retrain conditions and retrain the ML model if warranted.

    If the new model has better (lower) cross-validated MAE than the
    old one, it replaces the saved model. Otherwise the old model is
    kept and the new one is discarded.

    Returns:
        Status dict with keys: retrained, improved, old_mae, new_mae,
        games_used — or retrained=False with a reason string.
    """
    if not await should_retrain(db):
        return {"retrained": False, "reason": "Not enough new data"}

    from app.analytics.ml_model import MLModel
    from app.analytics.ml_training import build_training_data

    model_path = settings.model.ml_model_path

    # Load the current model to capture its metrics for comparison.
    old_model = MLModel()
    has_old = old_model.load(model_path)
    old_cv_mae = None
    if has_old and old_model.metrics:
        old_cv_mae = (
            old_model.metrics.cv_home_xg_mae + old_model.metrics.cv_away_xg_mae
        ) / 2.0

    # Build training data from all available settled games.
    X, y_home, y_away, n_skipped = await build_training_data(
        db, days_back=365, limit=2000,
    )

    if X.size == 0:
        return {
            "retrained": False,
            "reason": "No training data available",
        }

    min_games = settings.model.ml_min_training_games
    if X.shape[0] < min_games:
        return {
            "retrained": False,
            "reason": f"Insufficient data: {X.shape[0]} games (need {min_games})",
        }

    # Train a new model.
    from app.analytics.ml_features import get_feature_names

    new_model = MLModel()
    feature_names = get_feature_names()
    metrics = new_model.train(X, y_home, y_away, feature_names)

    new_cv_mae = (metrics.cv_home_xg_mae + metrics.cv_away_xg_mae) / 2.0
    games_used = int(X.shape[0])

    # Compare: promote only if the new model is strictly better.
    improved = True
    if old_cv_mae is not None:
        improved = new_cv_mae < old_cv_mae

    if improved:
        new_model.save(model_path)
        logger.info(
            "Auto-retrain: new model saved (CV MAE %.4f -> %.4f, %d games)",
            old_cv_mae or 0.0, new_cv_mae, games_used,
        )
    else:
        logger.info(
            "Auto-retrain: new model discarded (CV MAE %.4f >= %.4f, %d games)",
            new_cv_mae, old_cv_mae, games_used,
        )

    return {
        "retrained": True,
        "improved": improved,
        "old_mae": round(old_cv_mae, 4) if old_cv_mae is not None else None,
        "new_mae": round(new_cv_mae, 4),
        "games_used": games_used,
    }


async def _count_settled_games(db: AsyncSession) -> int:
    """Count how many completed games with scores exist in the database."""
    stmt = select(func.count(Game.id)).where(
        and_(
            func.lower(Game.status).in_(GAME_FINAL_STATUSES),
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
        )
    )
    result = await db.execute(stmt)
    return result.scalar() or 0
