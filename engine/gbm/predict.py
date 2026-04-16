"""
GBM inference -- load latest trained model and produce predictions.

Called from /api/predict to produce the GBM signal alongside MC + the
factor model. Output is a dict of predicted probabilities / expected
values keyed by target (home_win, nrfi_hit, f5_home_win, total_runs,
f5_total).

Caches loaded models in-process so the per-request cost is a feature
lookup + tree traversal (< 1 ms).
"""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_MODELS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "models"

# (sport, target) -> (model, meta); populated lazily and refreshed when
# the "latest" artifact changes mtime.
_CACHE: dict[tuple[str, str], dict] = {}
_LOCK = threading.Lock()


def _load_latest(sport: str, target: str):
    """Load the latest GBM artifact for a (sport, target). Returns dict
    with keys {model, meta, mtime} or None when the artifact is missing."""
    art = _MODELS_DIR / f"{sport}_gbm_{target}_latest.json"
    meta_path = _MODELS_DIR / f"{sport}_gbm_{target}_latest.meta.json"
    if not art.exists():
        return None
    mtime = art.stat().st_mtime

    with _LOCK:
        cached = _CACHE.get((sport, target))
        if cached and cached["mtime"] == mtime:
            return cached
        try:
            import xgboost as xgb
        except ImportError:
            logger.debug("xgboost not installed -- GBM disabled")
            return None
        try:
            meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
            task = meta.get("task", "classification")
            if task == "classification":
                model = xgb.XGBClassifier()
            else:
                model = xgb.XGBRegressor()
            model.load_model(str(art))
            entry = {"model": model, "meta": meta, "mtime": mtime}
            _CACHE[(sport, target)] = entry
            return entry
        except Exception as e:
            logger.warning("Failed to load GBM %s/%s: %s", sport, target, e)
            return None


def predict_mlb(conn, game_payload: dict) -> dict[str, Any]:
    """Run all MLB GBM targets on one matchup.

    game_payload is the same dict shape we hand to extract_mlb_features
    (home_team_id, away_team_id, home_pitcher_id, away_pitcher_id, date,
    venue). Returns a dict keyed by target name with either a float
    probability (classification) or a float expected value (regression).
    """
    try:
        import pandas as pd
    except ImportError:
        return {"error": "pandas not installed"}

    from .features import extract_mlb_features, FEATURE_NAMES
    features = extract_mlb_features(conn, game_payload)
    if not features:
        return {"error": "feature extraction failed"}

    X = pd.DataFrame([{k: features[k] for k in FEATURE_NAMES}])

    out: dict[str, Any] = {}
    for target_name, _ in _MLB_TARGETS:
        entry = _load_latest("mlb", target_name)
        if entry is None:
            continue
        model = entry["model"]
        meta = entry["meta"]
        try:
            if meta.get("task") == "classification":
                out[target_name] = round(float(model.predict_proba(X)[0, 1]), 4)
            else:
                out[target_name] = round(float(model.predict(X)[0]), 3)
        except Exception as e:
            out[target_name] = {"error": str(e)}

    if out:
        out["model_trained_at"] = max(
            (e.get("trained_at", "") for e in [_load_latest("mlb", t[0])["meta"]
                                               for t in _MLB_TARGETS
                                               if _load_latest("mlb", t[0])]),
            default="",
        )
    return out


# Keep in sync with engine.gbm.train.SPORT_TARGETS["mlb"]
_MLB_TARGETS = [
    ("home_win",     "classification"),
    ("nrfi_hit",     "classification"),
    ("f5_home_win",  "classification"),
    ("total_runs",   "regression"),
    ("f5_total",     "regression"),
]


def is_available(sport: str = "mlb") -> bool:
    """True if at least one latest-artifact exists for this sport."""
    if sport == "mlb":
        return any(
            (_MODELS_DIR / f"mlb_gbm_{t[0]}_latest.json").exists()
            for t in _MLB_TARGETS
        )
    return False
