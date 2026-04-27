"""
GBM prediction loader for MLB prop μ (Phase 2k-i live).

Wraps the XGBoost models trained by ``engine.mlb_props_train_all``
so the MC sampler can call ``predict_mu(stat_key, player_id, game_pk)``
and get a feature-aware μ for stats that survived the ship gate.

Stats that didn't survive (k_p, bb_p, outs, h_allowed, h, tb, k_b)
return None — the sampler falls back to rolling-mean μ for those,
preserving the calibration we already have.

Models are loaded lazily on first call and cached in-process so the
per-game predict cost is just feature extraction + 1 forward pass.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np

from .mlb_prop_features_v2 import feature_cols, extract_features

logger = logging.getLogger(__name__)

_MODEL_DIR = Path(__file__).resolve().parent.parent / "data" / "models"

# Stats with shipped GBM models — must match the persisted files in
# data/models/mlb_prop_{stat}_xgb_latest.json. Anything not in this
# set falls back to rolling-mean μ in the sampler.
_SHIPPED_STATS = {"er", "hr", "rbi", "r", "sb", "bb_b"}


_model_cache: dict[str, object] = {}
_meta_cache: dict[str, dict] = {}


def _model_path(stat_key: str) -> Path:
    return _MODEL_DIR / f"mlb_prop_{stat_key}_xgb_latest.json"


def _meta_path(stat_key: str) -> Path:
    return _MODEL_DIR / f"mlb_prop_{stat_key}_xgb_latest.meta.json"


def _load_model(stat_key: str):
    """Lazy-load + cache the XGBoost model for one stat. Returns None
    if the model file doesn't exist."""
    if stat_key in _model_cache:
        return _model_cache[stat_key]
    path = _model_path(stat_key)
    if not path.exists():
        _model_cache[stat_key] = None
        return None
    try:
        import xgboost as xgb
        model = xgb.XGBRegressor()
        model.load_model(str(path))
        _model_cache[stat_key] = model
        meta = json.loads(_meta_path(stat_key).read_text()) if _meta_path(stat_key).exists() else {}
        _meta_cache[stat_key] = meta
        return model
    except Exception as e:
        logger.warning("failed to load GBM for %s: %s", stat_key, e)
        _model_cache[stat_key] = None
        return None


def has_model(stat_key: str) -> bool:
    """True iff a shipped GBM exists for this stat."""
    return stat_key in _SHIPPED_STATS and _model_path(stat_key).exists()


def predict_mu(stat_key: str, player_id: int, game_pk: str,
               fallback_mu: float | None = None) -> float | None:
    """Returns the GBM-predicted μ for this (player, game, stat) or
    ``fallback_mu`` when no model is available or feature extraction
    fails. Negative predictions are clipped to 0 (sampler can't sample
    a negative-mean count distribution)."""
    if not has_model(stat_key):
        return fallback_mu
    model = _load_model(stat_key)
    if model is None:
        return fallback_mu
    feats = extract_features(int(player_id), str(game_pk), stat_key)
    if not feats:
        return fallback_mu
    cols = feature_cols(stat_key)
    X = np.asarray(
        [[feats.get(c) if feats.get(c) is not None else np.nan for c in cols]],
        dtype=float,
    )
    try:
        pred = float(model.predict(X)[0])
        return max(0.0, pred)
    except Exception as e:
        logger.warning("GBM predict failed for %s p=%s: %s",
                       stat_key, player_id, e)
        return fallback_mu


def shipped_stats() -> set[str]:
    return set(_SHIPPED_STATS)


__all__ = ["predict_mu", "has_model", "shipped_stats"]
