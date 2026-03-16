"""
Training data builder and training pipeline for the ML model.

Builds a training dataset from completed games by extracting features
and actual outcomes, then trains the ML model on that data.

Usage:
    python -m app.analytics.ml_training                # train with defaults
    python -m app.analytics.ml_training --days 180     # last 180 days
    python -m app.analytics.ml_training --limit 500    # max 500 games
"""

import asyncio
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from app.analytics.features import FeatureEngine
from app.analytics.ml_features import flatten_features, get_feature_names
from app.analytics.ml_model import MLModel
from app.config import settings
from app.constants import GAME_FINAL_STATUSES

logger = logging.getLogger(__name__)


async def build_training_data(
    db,
    days_back: int = 365,
    limit: int = 2000,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, int]:
    """Build feature matrix and labels from completed games.

    For each completed game with scores, extracts the full feature set
    and records actual home/away goals as labels.

    Args:
        db: AsyncSession for database access.
        days_back: How far back to look for completed games.
        limit: Maximum number of games to include.

    Returns:
        (X, y_home, y_away, n_skipped) tuple where:
        - X is the feature matrix (n_samples, n_features)
        - y_home is home goals scored
        - y_away is away goals scored
        - n_skipped is how many games were skipped due to errors
    """
    from sqlalchemy import and_, func, select
    from app.models.game import Game

    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)

    stmt = (
        select(Game)
        .where(
            and_(
                func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                Game.date >= start_date,
                Game.date <= end_date,
                Game.home_score.isnot(None),
                Game.away_score.isnot(None),
            )
        )
        .order_by(Game.date.desc())
        .limit(limit)
    )
    result = await db.execute(stmt)
    games = list(result.scalars().all())

    if not games:
        logger.warning("No completed games found for training data")
        return np.array([]), np.array([]), np.array([]), 0

    logger.info("Building training data from %d completed games", len(games))

    feature_engine = FeatureEngine()
    feature_names = get_feature_names()
    n_features = len(feature_names)

    rows: List[np.ndarray] = []
    home_goals: List[float] = []
    away_goals: List[float] = []
    n_skipped = 0

    for i, game in enumerate(games):
        try:
            features = await feature_engine.build_game_features(db, game.id)
            flat = flatten_features(features)

            row = np.array(
                [flat.get(name, float("nan")) for name in feature_names],
                dtype=np.float64,
            )
            rows.append(row)
            home_goals.append(float(game.home_score))
            away_goals.append(float(game.away_score))

            if (i + 1) % 50 == 0:
                logger.info("  Processed %d/%d games", i + 1, len(games))
        except Exception as e:
            logger.debug("Skipping game %d: %s", game.id, e)
            n_skipped += 1
            continue

    if not rows:
        return np.array([]), np.array([]), np.array([]), n_skipped

    X = np.vstack(rows)
    y_home = np.array(home_goals, dtype=np.float64)
    y_away = np.array(away_goals, dtype=np.float64)

    logger.info(
        "Training data ready: %d samples, %d features, %d skipped",
        X.shape[0], X.shape[1], n_skipped,
    )

    return X, y_home, y_away, n_skipped


async def train_ml_model(
    db,
    days_back: int = 365,
    limit: int = 2000,
    save_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Full training pipeline: build data, train, save.

    Args:
        db: AsyncSession for database access.
        days_back: How far back to look for training data.
        limit: Maximum number of games.
        save_path: Where to save the model (defaults to config setting).

    Returns:
        Dict with training results and metrics.
    """
    if save_path is None:
        save_path = settings.model.ml_model_path

    # Build training data
    X, y_home, y_away, n_skipped = await build_training_data(db, days_back, limit)

    if X.size == 0:
        return {
            "status": "error",
            "message": "No training data available",
            "games_skipped": n_skipped,
        }

    min_games = settings.model.ml_min_training_games
    if X.shape[0] < min_games:
        return {
            "status": "error",
            "message": f"Insufficient training data: {X.shape[0]} games (need {min_games})",
            "games_found": X.shape[0],
            "games_skipped": n_skipped,
        }

    # Train
    model = MLModel()
    feature_names = get_feature_names()
    metrics = model.train(X, y_home, y_away, feature_names)

    # Save
    model.save(save_path)

    return {
        "status": "success",
        "games_used": X.shape[0],
        "games_skipped": n_skipped,
        "model_path": save_path,
        "metrics": metrics.to_dict(),
    }


# ------------------------------------------------------------------ #
#  CLI entry point                                                     #
# ------------------------------------------------------------------ #

async def _main():
    """CLI entry point for training the ML model."""
    import json
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    from app.database import get_session_context, init_db

    await init_db()

    days_back = 365
    limit = 2000

    # Parse simple CLI args
    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--days" and i + 1 < len(args):
            days_back = int(args[i + 1])
        elif arg == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])

    print(f"Training ML model (days_back={days_back}, limit={limit})...")

    async with get_session_context() as db:
        result = await train_ml_model(db, days_back=days_back, limit=limit)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(_main())
