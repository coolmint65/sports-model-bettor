"""
NHL prop GBM loader (Phase 2k-i for hockey).

Same lazy-load pattern as engine.mlb_prop_gbm. Skater goals (g) and
assists (a) shipped with massive lift (+36% / +27% MAE reduction).
Other stats fall back to rolling-mean μ.
"""

from __future__ import annotations

import logging
from pathlib import Path
import numpy as np

from .nhl_prop_features import feature_cols, extract_features

logger = logging.getLogger(__name__)
_MODEL_DIR = Path(__file__).resolve().parent.parent / "data" / "models"
_SHIPPED_STATS = {"g", "a"}
_model_cache: dict[str, object] = {}


def _model_path(stat_key: str) -> Path:
    return _MODEL_DIR / f"nhl_prop_{stat_key}_xgb_latest.json"


def _load_model(stat_key: str):
    if stat_key in _model_cache:
        return _model_cache[stat_key]
    path = _model_path(stat_key)
    if not path.exists():
        _model_cache[stat_key] = None
        return None
    try:
        import xgboost as xgb
        m = xgb.XGBRegressor()
        m.load_model(str(path))
        _model_cache[stat_key] = m
        return m
    except Exception as e:
        logger.warning("failed to load NHL GBM for %s: %s", stat_key, e)
        _model_cache[stat_key] = None
        return None


def has_model(stat_key: str) -> bool:
    return stat_key in _SHIPPED_STATS and _model_path(stat_key).exists()


def predict_mu(stat_key: str, player_id: int, game_id: str,
               fallback_mu: float | None = None) -> float | None:
    if not has_model(stat_key):
        return fallback_mu
    m = _load_model(stat_key)
    if m is None:
        return fallback_mu
    feats = extract_features(int(player_id), str(game_id), stat_key)
    if not feats:
        return fallback_mu
    cols = feature_cols(stat_key)
    X = np.asarray(
        [[feats.get(c) if feats.get(c) is not None else np.nan for c in cols]],
        dtype=float,
    )
    try:
        return max(0.0, float(m.predict(X)[0]))
    except Exception as e:
        logger.warning("NHL GBM predict failed for %s p=%s: %s",
                       stat_key, player_id, e)
        return fallback_mu


__all__ = ["predict_mu", "has_model"]
