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
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from app.analytics.features import FeatureEngine
from app.analytics.ml_features import flatten_features, get_feature_names
from app.analytics.ml_model import MLModel
from app.config import settings, DATA_DIR
from app.constants import GAME_FINAL_STATUSES
from app.database import async_session_factory

logger = logging.getLogger(__name__)

# Disk cache for training data — avoids re-extracting features for 1000+
# games on every server restart.  Stored as a compressed .npz file
# alongside the ML model.
_TRAINING_CACHE_PATH = DATA_DIR / "ml_training_cache.npz"


def _load_training_cache() -> Optional[Dict[str, np.ndarray]]:
    """Load cached training data from disk.  Returns None if missing or corrupt."""
    if not _TRAINING_CACHE_PATH.exists():
        return None
    try:
        data = np.load(_TRAINING_CACHE_PATH, allow_pickle=False)
        required = {"X", "y_home", "y_away", "game_ids"}
        if not required.issubset(data.files):
            logger.info("Training cache missing keys, will rebuild")
            return None
        n_features_expected = len(get_feature_names())
        if data["X"].shape[1] != n_features_expected:
            logger.info(
                "Training cache feature count changed (%d -> %d), will rebuild",
                data["X"].shape[1], n_features_expected,
            )
            return None
        return {k: data[k] for k in data.files}
    except Exception as exc:
        logger.warning("Failed to load training cache: %s", exc)
        return None


def _save_training_cache(
    X: np.ndarray,
    y_home: np.ndarray,
    y_away: np.ndarray,
    game_ids: np.ndarray,
) -> None:
    """Persist training data to disk for fast restart."""
    try:
        np.savez_compressed(
            _TRAINING_CACHE_PATH,
            X=X, y_home=y_home, y_away=y_away, game_ids=game_ids,
        )
        logger.info(
            "Training cache saved: %d samples (%s)",
            X.shape[0], _TRAINING_CACHE_PATH,
        )
    except Exception as exc:
        logger.warning("Failed to save training cache: %s", exc)


async def build_training_data(
    db,
    days_back: int = 365,
    limit: int = 2000,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, int]:
    """Build feature matrix and labels from completed games.

    Uses a disk cache to avoid re-extracting features for games that
    were already processed.  Only new games (not in the cache) go
    through the expensive feature extraction pipeline.

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

    # --- Try to reuse cached training data --- #
    cache = _load_training_cache()
    cached_ids: set = set()
    cached_rows: List[np.ndarray] = []
    cached_home: List[float] = []
    cached_away: List[float] = []

    current_game_ids = {g.id for g in games}

    if cache is not None:
        cached_id_arr = cache["game_ids"]
        # Keep only cached entries that are still in the current query window
        for i, gid in enumerate(cached_id_arr):
            gid_int = int(gid)
            if gid_int in current_game_ids:
                cached_ids.add(gid_int)
                cached_rows.append(cache["X"][i])
                cached_home.append(float(cache["y_home"][i]))
                cached_away.append(float(cache["y_away"][i]))

    # Filter to only games that need feature extraction
    new_games = [g for g in games if g.id not in cached_ids]

    if not new_games:
        logger.info(
            "Training data fully cached: %d samples, 0 new games",
            len(cached_rows),
        )
        X = np.vstack(cached_rows) if cached_rows else np.array([])
        y_home = np.array(cached_home, dtype=np.float64)
        y_away = np.array(cached_away, dtype=np.float64)
        return X, y_home, y_away, 0

    logger.info(
        "Building training data: %d cached, %d new (of %d total)",
        len(cached_rows), len(new_games), len(games),
    )

    feature_names = get_feature_names()

    # --- Process new games concurrently in batches --- #
    BATCH_SIZE = 10

    async def _process_game(game):
        """Build features for a single game using its own DB session."""
        async with async_session_factory() as session:
            feature_engine = FeatureEngine()
            features = await feature_engine.build_game_features(session, game.id)
            flat = flatten_features(features)
            row = np.array(
                [flat.get(name, float("nan")) for name in feature_names],
                dtype=np.float64,
            )
            return game.id, row, float(game.home_score), float(game.away_score)

    new_rows: List[np.ndarray] = []
    new_home: List[float] = []
    new_away: List[float] = []
    new_ids: List[int] = []
    n_skipped = 0

    for batch_start in range(0, len(new_games), BATCH_SIZE):
        batch = new_games[batch_start : batch_start + BATCH_SIZE]
        tasks = [_process_game(g) for g in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for game, result_item in zip(batch, results):
            if isinstance(result_item, Exception):
                logger.debug("Skipping game %d: %s", game.id, result_item)
                n_skipped += 1
            else:
                gid, row, h_score, a_score = result_item
                new_rows.append(row)
                new_home.append(h_score)
                new_away.append(a_score)
                new_ids.append(gid)

        processed = min(batch_start + BATCH_SIZE, len(new_games))
        if processed % 50 < BATCH_SIZE or processed == len(new_games):
            logger.info("  Processed %d/%d new games", processed, len(new_games))

    # Merge cached + new
    all_rows = cached_rows + new_rows
    all_home = cached_home + new_home
    all_away = cached_away + new_away
    all_ids = list(cached_ids) + new_ids

    if not all_rows:
        return np.array([]), np.array([]), np.array([]), n_skipped

    X = np.vstack(all_rows)
    y_home = np.array(all_home, dtype=np.float64)
    y_away = np.array(all_away, dtype=np.float64)
    game_id_arr = np.array(all_ids, dtype=np.int64)

    # Save merged data to disk for next startup
    _save_training_cache(X, y_home, y_away, game_id_arr)

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
