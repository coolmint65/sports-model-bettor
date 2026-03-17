"""
ML model layer for xG prediction using gradient boosted trees.

Trains HistGradientBoostingRegressor models to predict home/away expected
goals from the flattened feature vector. Designed to augment (not replace)
the existing Poisson model via a configurable blend weight.

Key design choices:
- HistGradientBoosting handles NaN natively (no imputation needed)
- No feature scaling required (tree-based)
- Separate models for home_xg and away_xg (different distributions)
- Persistence via joblib for fast load/save
"""

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np

from app.analytics.ml_features import (
    features_to_array,
    flatten_features,
    get_feature_names,
)

logger = logging.getLogger(__name__)


@dataclass
class TrainingMetrics:
    """Metrics from a training run."""
    n_samples: int = 0
    home_xg_mae: float = 0.0
    away_xg_mae: float = 0.0
    home_xg_rmse: float = 0.0
    away_xg_rmse: float = 0.0
    cv_home_xg_mae: float = 0.0
    cv_away_xg_mae: float = 0.0
    train_time_seconds: float = 0.0
    feature_count: int = 0
    top_features_home: List[Dict[str, Any]] = field(default_factory=list)
    top_features_away: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "n_samples": self.n_samples,
            "home_xg_mae": round(self.home_xg_mae, 4),
            "away_xg_mae": round(self.away_xg_mae, 4),
            "home_xg_rmse": round(self.home_xg_rmse, 4),
            "away_xg_rmse": round(self.away_xg_rmse, 4),
            "cv_home_xg_mae": round(self.cv_home_xg_mae, 4),
            "cv_away_xg_mae": round(self.cv_away_xg_mae, 4),
            "train_time_seconds": round(self.train_time_seconds, 2),
            "feature_count": self.feature_count,
            "top_features_home": self.top_features_home[:10],
            "top_features_away": self.top_features_away[:10],
        }


class MLModel:
    """Gradient boosting model for xG prediction.

    Trains two separate HistGradientBoostingRegressor models: one for
    home expected goals and one for away expected goals.
    """

    def __init__(self) -> None:
        self.home_model = None
        self.away_model = None
        self.is_trained: bool = False
        self.feature_names: List[str] = []
        self.metrics: Optional[TrainingMetrics] = None
        self.trained_at: Optional[str] = None

    def train(
        self,
        X: np.ndarray,
        y_home: np.ndarray,
        y_away: np.ndarray,
        feature_names: Optional[List[str]] = None,
    ) -> TrainingMetrics:
        """Train home and away xG models on the provided data.

        Args:
            X: Feature matrix (n_samples, n_features). NaN values are OK.
            y_home: Home goals scored (n_samples,).
            y_away: Away goals scored (n_samples,).
            feature_names: Optional list of feature names for importance tracking.

        Returns:
            TrainingMetrics with in-sample and cross-validated metrics.
        """
        from sklearn.ensemble import HistGradientBoostingRegressor
        from sklearn.model_selection import cross_val_score

        start = time.time()
        n_samples = X.shape[0]
        self.feature_names = feature_names or get_feature_names()

        logger.info("Training ML model on %d samples, %d features", n_samples, X.shape[1])

        # Hyperparameters tuned for small-to-medium dataset (~200-2000 games)
        params = {
            "max_iter": 200,
            "max_depth": 5,
            "learning_rate": 0.05,
            "min_samples_leaf": 10,
            "l2_regularization": 1.0,
            "max_bins": 128,
            "random_state": 42,
        }

        # Train home xG model
        self.home_model = HistGradientBoostingRegressor(**params)
        self.home_model.fit(X, y_home)

        # Train away xG model
        self.away_model = HistGradientBoostingRegressor(**params)
        self.away_model.fit(X, y_away)

        # In-sample metrics
        home_pred = self.home_model.predict(X)
        away_pred = self.away_model.predict(X)

        metrics = TrainingMetrics(
            n_samples=n_samples,
            home_xg_mae=float(np.mean(np.abs(home_pred - y_home))),
            away_xg_mae=float(np.mean(np.abs(away_pred - y_away))),
            home_xg_rmse=float(np.sqrt(np.mean((home_pred - y_home) ** 2))),
            away_xg_rmse=float(np.sqrt(np.mean((away_pred - y_away) ** 2))),
            feature_count=X.shape[1],
        )

        # Cross-validated MAE (5-fold if enough data, 3-fold otherwise)
        n_folds = 5 if n_samples >= 100 else 3
        if n_samples >= 30:
            cv_home = cross_val_score(
                HistGradientBoostingRegressor(**params),
                X, y_home, cv=n_folds, scoring="neg_mean_absolute_error",
            )
            cv_away = cross_val_score(
                HistGradientBoostingRegressor(**params),
                X, y_away, cv=n_folds, scoring="neg_mean_absolute_error",
            )
            metrics.cv_home_xg_mae = float(-cv_home.mean())
            metrics.cv_away_xg_mae = float(-cv_away.mean())

        # Feature importance (only available in sklearn >= 1.4)
        if self.feature_names and hasattr(self.home_model, "feature_importances_"):
            metrics.top_features_home = self._top_features(
                self.home_model.feature_importances_
            )
            metrics.top_features_away = self._top_features(
                self.away_model.feature_importances_
            )

        elapsed = time.time() - start
        metrics.train_time_seconds = elapsed

        self.is_trained = True
        self.metrics = metrics
        self.trained_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        logger.info(
            "Training complete in %.1fs. CV MAE: home=%.3f, away=%.3f",
            elapsed, metrics.cv_home_xg_mae, metrics.cv_away_xg_mae,
        )

        return metrics

    def predict_xg(self, features: Dict[str, Any]) -> Tuple[float, float]:
        """Predict home_xg and away_xg from a raw (nested) feature dict.

        Args:
            features: The nested feature dict from build_game_features().

        Returns:
            (home_xg, away_xg) tuple. Values are clamped to [0.5, 6.0].

        Raises:
            RuntimeError: If the model hasn't been trained yet.
        """
        if not self.is_trained:
            raise RuntimeError("ML model has not been trained")

        flat = flatten_features(features)
        X = features_to_array(flat).reshape(1, -1)

        home_xg = float(self.home_model.predict(X)[0])
        away_xg = float(self.away_model.predict(X)[0])

        # Clamp to reasonable range
        home_xg = max(0.5, min(6.0, home_xg))
        away_xg = max(0.5, min(6.0, away_xg))

        return round(home_xg, 3), round(away_xg, 3)

    def predict_xg_from_flat(self, flat: Dict[str, float]) -> Tuple[float, float]:
        """Predict from an already-flattened feature dict (avoids double-flatten)."""
        if not self.is_trained:
            raise RuntimeError("ML model has not been trained")

        X = features_to_array(flat).reshape(1, -1)
        home_xg = float(self.home_model.predict(X)[0])
        away_xg = float(self.away_model.predict(X)[0])
        home_xg = max(0.5, min(6.0, home_xg))
        away_xg = max(0.5, min(6.0, away_xg))
        return round(home_xg, 3), round(away_xg, 3)

    def save(self, path: str) -> None:
        """Persist the trained model to disk."""
        if not self.is_trained:
            raise RuntimeError("Cannot save untrained model")

        filepath = Path(path)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "home_model": self.home_model,
            "away_model": self.away_model,
            "feature_names": self.feature_names,
            "metrics": self.metrics.to_dict() if self.metrics else None,
            "trained_at": self.trained_at,
        }
        joblib.dump(payload, filepath)
        logger.info("ML model saved to %s", filepath)

    def load(self, path: str) -> bool:
        """Load a trained model from disk.

        Returns True if loaded successfully, False if file doesn't exist.
        """
        filepath = Path(path)
        if not filepath.exists():
            logger.info("No ML model file at %s", filepath)
            return False

        try:
            payload = joblib.load(filepath)
            self.home_model = payload["home_model"]
            self.away_model = payload["away_model"]
            self.feature_names = payload.get("feature_names", [])
            self.trained_at = payload.get("trained_at")
            self.is_trained = True

            # Restore metrics if available
            metrics_dict = payload.get("metrics")
            if metrics_dict:
                self.metrics = TrainingMetrics(**{
                    k: v for k, v in metrics_dict.items()
                    if k in TrainingMetrics.__dataclass_fields__
                })

            logger.info("ML model loaded from %s (trained %s)", filepath, self.trained_at)
            return True
        except Exception as e:
            logger.error("Failed to load ML model from %s: %s", filepath, e)
            return False

    def status(self) -> Dict[str, Any]:
        """Return model status info for the API."""
        return {
            "is_trained": self.is_trained,
            "trained_at": self.trained_at,
            "feature_count": len(self.feature_names),
            "metrics": self.metrics.to_dict() if self.metrics else None,
        }

    def _top_features(self, importances: np.ndarray, n: int = 15) -> List[Dict[str, Any]]:
        """Get top N features by importance."""
        indices = np.argsort(importances)[::-1][:n]
        result = []
        for idx in indices:
            name = self.feature_names[idx] if idx < len(self.feature_names) else f"feature_{idx}"
            result.append({
                "name": name,
                "importance": round(float(importances[idx]), 4),
            })
        return result
