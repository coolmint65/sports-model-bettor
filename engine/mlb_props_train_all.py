"""
Multi-stat MLB prop GBM trainer + ship/kill decider.

Runs ``train_and_decide`` on every MLB stat the picker tracks,
produces a leaderboard of which stats benefit from per-game features
above the rolling-mean baseline. Per-stat decisions because some
stats almost certainly carry feature signal (Batter HR with park +
opp SP HR/9) while others are noise-dominated (Batter SB).
"""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit

from .mlb_prop_features_v2 import (
    feature_cols, build_training_set, TrainingRow,
)

SHIP_GATE_MAE_REDUCTION = 0.10
_MODEL_DIR = Path(__file__).resolve().parent.parent / "data" / "models"


def _to_xy(rows: list[TrainingRow], cols: list[str]):
    X = np.asarray(
        [[r.features.get(c, np.nan) if r.features.get(c) is not None
          else np.nan for c in cols] for r in rows],
        dtype=float,
    )
    y = np.asarray([r.target for r in rows], dtype=float)
    return X, y


def _train_xgb(X: np.ndarray, y: np.ndarray):
    import xgboost as xgb
    return xgb.XGBRegressor(
        objective="reg:absoluteerror",
        max_depth=4, learning_rate=0.05, n_estimators=300,
        subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
        verbosity=0,
    ).fit(X, y)


def _baseline_mae_idx(cols: list[str]) -> int:
    return cols.index("rolling_30d")


def cross_validate(rows: list[TrainingRow], cols: list[str],
                    n_splits: int = 5) -> dict:
    rows = sorted(rows, key=lambda r: r.date)
    X, y = _to_xy(rows, cols)
    base_idx = _baseline_mae_idx(cols)
    fold_results = []
    for fold, (tr, te) in enumerate(TimeSeriesSplit(n_splits=n_splits).split(X)):
        if len(te) < 20:
            continue
        baseline_preds = X[te, base_idx]
        # Replace NaN baselines with overall train mean (rare; very
        # early rows where rolling_30d had no history).
        baseline_preds = np.where(np.isnan(baseline_preds),
                                   np.nanmean(y[tr]), baseline_preds)
        baseline_mae = mean_absolute_error(y[te], baseline_preds)
        model = _train_xgb(X[tr], y[tr])
        gbm_preds = model.predict(X[te])
        gbm_mae = mean_absolute_error(y[te], gbm_preds)
        fold_results.append({
            "n_test": int(len(te)),
            "baseline_mae": float(baseline_mae),
            "gbm_mae": float(gbm_mae),
            "reduction_pct": float((baseline_mae - gbm_mae) / baseline_mae * 100)
                            if baseline_mae > 0 else 0.0,
        })
    if not fold_results:
        return {"folds": [], "mean_baseline_mae": None, "mean_gbm_mae": None,
                "mean_reduction_pct": 0.0}
    return {
        "folds": fold_results,
        "mean_baseline_mae": float(np.mean([f["baseline_mae"] for f in fold_results])),
        "mean_gbm_mae": float(np.mean([f["gbm_mae"] for f in fold_results])),
        "mean_reduction_pct": float(np.mean([f["reduction_pct"] for f in fold_results])),
    }


def train_stat(stat_key: str, *, lookback_days: int = 180,
                min_rows: int = 100) -> dict:
    rows = build_training_set(stat_key, lookback_days=lookback_days)
    if len(rows) < min_rows:
        return {"stat": stat_key, "n_rows": len(rows),
                "decision": "kill",
                "reason": f"insufficient data (n={len(rows)} < {min_rows})"}
    cols = feature_cols(stat_key)
    cv = cross_validate(rows, cols)
    decision = ("ship" if (cv["mean_reduction_pct"] or 0) >= SHIP_GATE_MAE_REDUCTION * 100
                else "kill")
    out = {
        "stat": stat_key,
        "n_rows": len(rows),
        "n_features": len(cols),
        "baseline_mae": cv["mean_baseline_mae"],
        "gbm_mae": cv["mean_gbm_mae"],
        "reduction_pct": cv["mean_reduction_pct"],
        "decision": decision,
        "folds": cv["folds"],
    }
    if decision == "ship":
        # Train final on all data + persist.
        rs = sorted(rows, key=lambda r: r.date)
        X, y = _to_xy(rs, cols)
        model = _train_xgb(X, y)
        _MODEL_DIR.mkdir(parents=True, exist_ok=True)
        model_path = _MODEL_DIR / f"mlb_prop_{stat_key}_xgb_latest.json"
        meta_path = _MODEL_DIR / f"mlb_prop_{stat_key}_xgb_latest.meta.json"
        model.save_model(str(model_path))
        meta_path.write_text(json.dumps({
            "stat": stat_key,
            "feature_cols": cols,
            "n_rows": len(rows),
            "cv_baseline_mae": cv["mean_baseline_mae"],
            "cv_gbm_mae": cv["mean_gbm_mae"],
            "reduction_pct": cv["mean_reduction_pct"],
            "trained_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }, indent=2))
        out["model_path"] = str(model_path)
    return out


def train_all_mlb(*, lookback_days: int = 180) -> list[dict]:
    """Run train_stat across every MLB prop stat the picker tracks."""
    stats = ["k_p", "bb_p", "outs", "er", "h_allowed",
             "hr", "h", "tb", "rbi", "r", "sb", "bb_b", "k_b"]
    return [train_stat(s, lookback_days=lookback_days) for s in stats]


__all__ = ["train_stat", "train_all_mlb"]
