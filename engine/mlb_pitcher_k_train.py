"""
MLB pitcher Ks GBM training + validation (Phase 2k-i).

Trains an XGBoost regressor to predict per-game pitcher Ks from the
features in ``engine.mlb_prop_features``. Compares MAE against the
naive baseline (just-use-rolling-30d-mean) using time-series-aware
cross-validation. Reports per-feature importance and ablation
deltas so we can kill features that don't move the needle.

Decision gate: ship the GBM only when it beats baseline by ≥10%
MAE reduction. If the gain is marginal, the rolling mean stays as
the picker's μ — the same "factors=noise" discipline used on the
MLB cap-relaxation backtest earlier in the project.
"""

from __future__ import annotations

import json
import logging
import math
import time
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit

from .mlb_prop_features import FEATURE_COLS, build_training_set, TrainingRow

logger = logging.getLogger(__name__)

# Decision threshold: GBM must reduce MAE by at least this fraction
# vs the rolling-mean baseline to be worth shipping. 10% is what we
# committed to in the conversation (factors=noise discipline).
SHIP_GATE_MAE_REDUCTION = 0.10

_MODEL_DIR = Path(__file__).resolve().parent.parent / "data" / "models"
_MODEL_PATH = _MODEL_DIR / "mlb_pitcher_k_xgb_latest.json"
_META_PATH = _MODEL_DIR / "mlb_pitcher_k_xgb_latest.meta.json"


def _to_xy(rows: list[TrainingRow]) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """Convert training rows to (X, y, dates). Missing features are
    set to NaN so XGBoost's default missing-value handling kicks in
    rather than us imputing badly."""
    X_list = []
    y_list = []
    dates = []
    for r in rows:
        row = []
        for col in FEATURE_COLS:
            v = r.features.get(col)
            row.append(float(v) if v is not None else np.nan)
        X_list.append(row)
        y_list.append(r.target_k)
        dates.append(r.date)
    return (np.asarray(X_list, dtype=float),
            np.asarray(y_list, dtype=float),
            dates)


def _baseline_mae(rows: list[TrainingRow]) -> float:
    """MAE of "just use rolling_k_30d as the prediction" — what the
    picker uses today. The number the GBM has to beat."""
    preds = []
    actuals = []
    for r in rows:
        rk = r.features.get("rolling_k_30d")
        if rk is None:
            continue
        preds.append(float(rk))
        actuals.append(r.target_k)
    if not preds:
        return float("inf")
    return mean_absolute_error(actuals, preds)


def _train_xgb(X: np.ndarray, y: np.ndarray, **xgb_params) -> Any:
    import xgboost as xgb
    params = {
        "objective": "reg:absoluteerror",  # match the MAE eval metric
        "max_depth": 4,
        "learning_rate": 0.05,
        "n_estimators": 300,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 5,
        "verbosity": 0,
    }
    params.update(xgb_params)
    model = xgb.XGBRegressor(**params)
    # XGBoost handles NaN natively when missing=np.nan (the default
    # since v1.0). No need to impute.
    model.fit(X, y)
    return model


def cross_validate(rows: list[TrainingRow], n_splits: int = 5) -> dict:
    """Time-series CV — earlier rows train, later rows test. Mirrors
    how the live picker would have used historical data on each
    target date."""
    rows = sorted(rows, key=lambda r: r.date)
    X, y, dates = _to_xy(rows)
    tscv = TimeSeriesSplit(n_splits=n_splits)
    fold_results = []
    for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
        if len(test_idx) < 20:
            continue
        baseline_preds = X[test_idx, FEATURE_COLS.index("rolling_k_30d")]
        baseline_mae = mean_absolute_error(y[test_idx], baseline_preds)
        model = _train_xgb(X[train_idx], y[train_idx])
        gbm_preds = model.predict(X[test_idx])
        gbm_mae = mean_absolute_error(y[test_idx], gbm_preds)
        fold_results.append({
            "fold": fold,
            "n_train": int(len(train_idx)),
            "n_test": int(len(test_idx)),
            "baseline_mae": float(baseline_mae),
            "gbm_mae": float(gbm_mae),
            "reduction_pct": float((baseline_mae - gbm_mae) / baseline_mae * 100),
        })
    if not fold_results:
        return {"folds": [], "mean_baseline_mae": None,
                "mean_gbm_mae": None, "mean_reduction_pct": 0.0}
    return {
        "folds": fold_results,
        "mean_baseline_mae": float(np.mean([f["baseline_mae"] for f in fold_results])),
        "mean_gbm_mae": float(np.mean([f["gbm_mae"] for f in fold_results])),
        "mean_reduction_pct": float(np.mean([f["reduction_pct"] for f in fold_results])),
    }


def feature_ablation(rows: list[TrainingRow]) -> dict:
    """Drop each feature one at a time, retrain, measure CV MAE
    delta. Negative delta means the feature was holding things up;
    positive (or near-zero) means the feature is noise."""
    rows = sorted(rows, key=lambda r: r.date)
    X, y, _ = _to_xy(rows)
    full_cv = cross_validate(rows, n_splits=4)
    full_mae = full_cv["mean_gbm_mae"]
    out = []
    for i, col in enumerate(FEATURE_COLS):
        ablated = X.copy()
        ablated[:, i] = np.nan  # drop feature
        # Run the same CV but on ablated X.
        from sklearn.model_selection import TimeSeriesSplit
        tscv = TimeSeriesSplit(n_splits=4)
        maes = []
        for train_idx, test_idx in tscv.split(ablated):
            if len(test_idx) < 20:
                continue
            model = _train_xgb(ablated[train_idx], y[train_idx])
            preds = model.predict(ablated[test_idx])
            maes.append(mean_absolute_error(y[test_idx], preds))
        ablated_mae = float(np.mean(maes)) if maes else float("nan")
        delta = ablated_mae - full_mae if not math.isnan(ablated_mae) else float("nan")
        out.append({
            "feature": col,
            "ablated_mae": ablated_mae,
            "delta": delta,
            "verdict": "useful" if delta > 0.02 else "noise",
        })
    out.sort(key=lambda r: -r["delta"])
    return {"full_mae": full_mae, "ablation": out}


def train_and_decide(*, lookback_days: int = 30) -> dict:
    """Top-level: builds dataset, runs CV, decides ship vs kill,
    persists model + meta when shipping. Returns the full report."""
    t0 = time.time()
    rows = build_training_set(lookback_days=lookback_days)
    if len(rows) < 100:
        return {"decision": "kill",
                "reason": f"insufficient data (n={len(rows)})"}
    baseline_mae = _baseline_mae(rows)
    cv = cross_validate(rows)
    abl = feature_ablation(rows)
    elapsed = time.time() - t0
    decision = ("ship" if cv["mean_reduction_pct"] >= SHIP_GATE_MAE_REDUCTION * 100
                else "kill")
    report = {
        "n_train_rows": len(rows),
        "baseline_mae": baseline_mae,
        "cv": cv,
        "ablation": abl,
        "decision": decision,
        "ship_gate_pct": SHIP_GATE_MAE_REDUCTION * 100,
        "elapsed_s": elapsed,
    }
    if decision == "ship":
        # Train final model on the full data set + persist.
        X, y, _ = _to_xy(sorted(rows, key=lambda r: r.date))
        model = _train_xgb(X, y)
        _MODEL_DIR.mkdir(parents=True, exist_ok=True)
        model.save_model(str(_MODEL_PATH))
        _META_PATH.write_text(json.dumps({
            "feature_cols": FEATURE_COLS,
            "n_rows": len(rows),
            "cv_mae": cv["mean_gbm_mae"],
            "baseline_mae": baseline_mae,
            "reduction_pct": cv["mean_reduction_pct"],
            "trained_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }, indent=2))
        report["model_path"] = str(_MODEL_PATH)
    return report


__all__ = ["train_and_decide", "cross_validate", "feature_ablation"]
