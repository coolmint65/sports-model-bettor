"""
MLB 1st-inning per-team scoring GBM (Phase 2k-iii).

Trains a binary classifier predicting "did this team score in
the 1st inning of this game?" and reports per-feature ablation
to honor the factors=noise discipline.

Decision gate: ship the GBM only when it beats the legacy
2-signal model (pitcher scoreless% + team score%) by ≥10% on
log-loss in time-series CV. Otherwise the legacy heuristic
stays.

Outputs (when ship gate passes):
    data/models/mlb_1st_inn_p_scores_xgb_latest.json
    data/models/mlb_1st_inn_p_scores_xgb_latest.meta.json
"""

from __future__ import annotations

import json
import math
import time
from pathlib import Path

import numpy as np
from sklearn.metrics import log_loss
from sklearn.model_selection import TimeSeriesSplit

from .mlb_first_inning_features import (
    feature_cols, build_training_set, TrainingRow,
)

SHIP_GATE_LOG_LOSS_REDUCTION = 0.10
_MODEL_DIR = Path(__file__).resolve().parent.parent / "data" / "models"
_MODEL_PATH = _MODEL_DIR / "mlb_1st_inn_p_scores_xgb_latest.json"
_META_PATH = _MODEL_DIR / "mlb_1st_inn_p_scores_xgb_latest.meta.json"


def _to_xy(rows, cols):
    X = np.asarray(
        [[r.features.get(c, np.nan) if r.features.get(c) is not None
          else np.nan for c in cols] for r in rows],
        dtype=float,
    )
    y = np.asarray([r.target for r in rows], dtype=int)
    return X, y


def _train_xgb(X, y):
    import xgboost as xgb
    return xgb.XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        max_depth=4, learning_rate=0.05, n_estimators=400,
        subsample=0.8, colsample_bytree=0.8, min_child_weight=10,
        verbosity=0,
    ).fit(X, y)


def _baseline_pred(rows: list[TrainingRow]) -> np.ndarray:
    """Replicates the legacy model: blend of pitcher scoreless% and
    team score%. Used as the 'beat me' baseline. NaN safety: when
    both signals missing, falls back to overall base rate."""
    preds = []
    base_rate = sum(r.target for r in rows) / len(rows) if rows else 0.5
    for r in rows:
        sp = r.features.get("opp_sp_first_inning_scoreless")
        team = r.features.get("team_first_inning_score_pct")
        # Pitcher predicts P(NOT score); team predicts P(score). Convert
        # both to P(score) and average — what the legacy model does.
        signals = []
        if sp is not None:
            signals.append(1.0 - float(sp))
        if team is not None:
            signals.append(float(team))
        if signals:
            preds.append(sum(signals) / len(signals))
        else:
            preds.append(base_rate)
    return np.clip(np.asarray(preds), 1e-6, 1 - 1e-6)


def cross_validate(rows: list[TrainingRow], cols: list[str],
                    n_splits: int = 5) -> dict:
    rows = sorted(rows, key=lambda r: r.date)
    X, y = _to_xy(rows, cols)
    folds = []
    for fold, (tr, te) in enumerate(TimeSeriesSplit(n_splits=n_splits).split(X)):
        if len(te) < 100:
            continue
        baseline_preds = _baseline_pred([rows[i] for i in te])
        base_ll = log_loss(y[te], baseline_preds, labels=[0, 1])
        model = _train_xgb(X[tr], y[tr])
        gbm_preds = model.predict_proba(X[te])[:, 1]
        gbm_ll = log_loss(y[te], gbm_preds, labels=[0, 1])
        folds.append({
            "n_test": int(len(te)),
            "baseline_logloss": float(base_ll),
            "gbm_logloss": float(gbm_ll),
            "reduction_pct": float((base_ll - gbm_ll) / base_ll * 100),
        })
    if not folds:
        return {"folds": [], "mean_baseline_ll": None, "mean_gbm_ll": None,
                "mean_reduction_pct": 0.0}
    return {
        "folds": folds,
        "mean_baseline_ll": float(np.mean([f["baseline_logloss"] for f in folds])),
        "mean_gbm_ll": float(np.mean([f["gbm_logloss"] for f in folds])),
        "mean_reduction_pct": float(np.mean([f["reduction_pct"] for f in folds])),
    }


def feature_ablation(rows: list[TrainingRow]) -> dict:
    """Drop each feature one at a time, retrain, measure CV log-loss
    delta. Positive delta = feature is useful; near-zero = noise."""
    rows = sorted(rows, key=lambda r: r.date)
    cols = feature_cols()
    X, y = _to_xy(rows, cols)
    full = cross_validate(rows, cols, n_splits=4)
    full_ll = full["mean_gbm_ll"]
    out = []
    for i, col in enumerate(cols):
        ablated_X = X.copy()
        ablated_X[:, i] = np.nan
        tscv = TimeSeriesSplit(n_splits=4)
        lls = []
        for tr, te in tscv.split(ablated_X):
            if len(te) < 100:
                continue
            model = _train_xgb(ablated_X[tr], y[tr])
            preds = model.predict_proba(ablated_X[te])[:, 1]
            lls.append(log_loss(y[te], preds, labels=[0, 1]))
        ab_ll = float(np.mean(lls)) if lls else float("nan")
        delta = ab_ll - full_ll if not math.isnan(ab_ll) else float("nan")
        out.append({
            "feature": col,
            "ablated_logloss": ab_ll,
            "delta": delta,
            "verdict": "useful" if delta > 0.001 else "noise",
        })
    out.sort(key=lambda r: -r["delta"])
    return {"full_ll": full_ll, "ablation": out}


def train_and_decide(*, lookback_days: int = 1100) -> dict:
    t0 = time.time()
    rows = build_training_set(lookback_days=lookback_days)
    if len(rows) < 500:
        return {"decision": "kill",
                "reason": f"insufficient data (n={len(rows)})"}
    cols = feature_cols()
    cv = cross_validate(rows, cols)
    abl = feature_ablation(rows)
    decision = ("ship" if cv["mean_reduction_pct"] >= SHIP_GATE_LOG_LOSS_REDUCTION * 100
                else "kill")
    report = {
        "n_train_rows": len(rows),
        "base_rate_score_pct": round(100 * sum(r.target for r in rows) / len(rows), 1),
        "cv": cv,
        "ablation": abl,
        "decision": decision,
        "ship_gate_pct": SHIP_GATE_LOG_LOSS_REDUCTION * 100,
        "elapsed_s": time.time() - t0,
    }
    if decision == "ship":
        rs = sorted(rows, key=lambda r: r.date)
        X, y = _to_xy(rs, cols)
        model = _train_xgb(X, y)
        _MODEL_DIR.mkdir(parents=True, exist_ok=True)
        model.save_model(str(_MODEL_PATH))
        _META_PATH.write_text(json.dumps({
            "feature_cols": cols,
            "n_rows": len(rows),
            "cv_baseline_logloss": cv["mean_baseline_ll"],
            "cv_gbm_logloss": cv["mean_gbm_ll"],
            "reduction_pct": cv["mean_reduction_pct"],
            "trained_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }, indent=2))
        report["model_path"] = str(_MODEL_PATH)
    return report


__all__ = ["train_and_decide", "cross_validate", "feature_ablation"]
