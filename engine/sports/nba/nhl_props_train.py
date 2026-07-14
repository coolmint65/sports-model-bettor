"""
NBA + NHL prop GBM trainer + ship/kill decider.

Mirrors engine.mlb_props_train_all but routes to per-sport feature
modules. Same 10% MAE-reduction ship gate.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit

SHIP_GATE_MAE_REDUCTION = 0.10
_MODEL_DIR = Path(__file__).resolve().parents[3] / "data" / "models"


def _to_xy(rows, cols):
    X = np.asarray(
        [[r.features.get(c, np.nan) if r.features.get(c) is not None
          else np.nan for c in cols] for r in rows],
        dtype=float,
    )
    y = np.asarray([r.target for r in rows], dtype=float)
    return X, y


def _train_xgb(X, y):
    import xgboost as xgb
    return xgb.XGBRegressor(
        objective="reg:absoluteerror",
        max_depth=4, learning_rate=0.05, n_estimators=300,
        subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
        verbosity=0,
    ).fit(X, y)


def _cv(rows, cols, n_splits=5):
    rows = sorted(rows, key=lambda r: r.date)
    X, y = _to_xy(rows, cols)
    base_idx = cols.index("rolling_30d")
    folds = []
    for fold, (tr, te) in enumerate(TimeSeriesSplit(n_splits=n_splits).split(X)):
        if len(te) < 20:
            continue
        baseline = X[te, base_idx]
        baseline = np.where(np.isnan(baseline), np.nanmean(y[tr]), baseline)
        bm = mean_absolute_error(y[te], baseline)
        m = _train_xgb(X[tr], y[tr])
        gm = mean_absolute_error(y[te], m.predict(X[te]))
        folds.append({"baseline_mae": float(bm), "gbm_mae": float(gm),
                      "reduction_pct": float((bm - gm) / bm * 100) if bm > 0 else 0.0})
    if not folds:
        return {"folds": [], "mean_baseline_mae": None, "mean_gbm_mae": None,
                "mean_reduction_pct": 0.0}
    return {
        "folds": folds,
        "mean_baseline_mae": float(np.mean([f["baseline_mae"] for f in folds])),
        "mean_gbm_mae": float(np.mean([f["gbm_mae"] for f in folds])),
        "mean_reduction_pct": float(np.mean([f["reduction_pct"] for f in folds])),
    }


def train_stat(sport: str, stat_key: str, *, lookback_days: int = 180,
                min_rows: int = 100) -> dict:
    if sport == "nba":
        from .prop_features import feature_cols, build_training_set
    elif sport == "nhl":
        from ...nhl_prop_features import feature_cols, build_training_set
    else:
        raise ValueError(f"unsupported sport: {sport}")
    rows = build_training_set(stat_key, lookback_days=lookback_days)
    if len(rows) < min_rows:
        return {"sport": sport, "stat": stat_key, "n_rows": len(rows),
                "decision": "kill", "reason": f"insufficient data (n={len(rows)})"}
    cols = feature_cols(stat_key)
    cv = _cv(rows, cols)
    decision = ("ship" if (cv["mean_reduction_pct"] or 0) >= SHIP_GATE_MAE_REDUCTION * 100
                else "kill")
    out = {
        "sport": sport, "stat": stat_key, "n_rows": len(rows),
        "n_features": len(cols),
        "baseline_mae": cv["mean_baseline_mae"],
        "gbm_mae": cv["mean_gbm_mae"],
        "reduction_pct": cv["mean_reduction_pct"],
        "decision": decision,
    }
    if decision == "ship":
        rs = sorted(rows, key=lambda r: r.date)
        X, y = _to_xy(rs, cols)
        model = _train_xgb(X, y)
        _MODEL_DIR.mkdir(parents=True, exist_ok=True)
        model_path = _MODEL_DIR / f"{sport}_prop_{stat_key}_xgb_latest.json"
        meta_path = _MODEL_DIR / f"{sport}_prop_{stat_key}_xgb_latest.meta.json"
        model.save_model(str(model_path))
        meta_path.write_text(json.dumps({
            "sport": sport, "stat": stat_key, "feature_cols": cols,
            "n_rows": len(rows),
            "cv_baseline_mae": cv["mean_baseline_mae"],
            "cv_gbm_mae": cv["mean_gbm_mae"],
            "reduction_pct": cv["mean_reduction_pct"],
            "trained_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }, indent=2))
        out["model_path"] = str(model_path)
    return out


def train_all_nba(*, lookback_days: int = 180) -> list[dict]:
    return [train_stat("nba", s, lookback_days=lookback_days)
            for s in ["pts", "reb", "ast", "tpm", "ftm", "to", "stl", "blk"]]


def train_all_nhl(*, lookback_days: int = 180) -> list[dict]:
    return [train_stat("nhl", s, lookback_days=lookback_days)
            for s in ["g", "a", "sog", "hits", "blocks",
                      "saves", "shots_against", "ga"]]


__all__ = ["train_stat", "train_all_nba", "train_all_nhl"]
