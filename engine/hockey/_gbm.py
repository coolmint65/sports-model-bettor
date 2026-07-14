"""GBM training + inference for the hockey framework (AHL/PWHL/etc).

Mirrors `engine.basketball._gbm` shape — three targets per league
(home_win classification, margin regression, total_goals regression),
chronological time-split holdout, per-process booster cache. The
basketball framework can't be reused as-is because hockey scoring
behaviour differs (Poisson-ish goals vs Gaussian-ish points), so the
hyperparams + targets are tuned for the hockey distribution.

Models persist to ``data/hockey/<league>_gbm/<target>.json``. Callers
get inference via ``predict(league, target, features)``.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from . import HOCKEY_LEAGUE_REGISTRY
from ._features import extract_features, extract_targets, feature_columns

logger = logging.getLogger(__name__)


_MODELS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "hockey"


TARGETS: list[tuple[str, str]] = [
    ("home_win", "classification"),
    ("margin", "regression"),
    ("total_goals", "regression"),
]


def _model_path(league: str, target: str) -> Path:
    return _MODELS_DIR / f"{league}_gbm" / f"{target}.json"


def _load_dataset(league: str) -> tuple[list, dict, list, list]:
    mod = __import__(f"engine.sports.{league}.db", fromlist=["get_conn"])
    conn = mod.get_conn()
    rows = conn.execute(
        "SELECT id, date, season, home_team_id, away_team_id, "
        "       home_score, away_score, status "
        "FROM games WHERE status = 'final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date"
    ).fetchall()
    cols = feature_columns()
    X: list[list[float]] = []
    y: dict[str, list[float]] = {t: [] for t, _ in TARGETS}
    dates: list[str] = []
    for r in rows:
        g = dict(r)
        feats = extract_features(league, g)
        targets = extract_targets(g)
        if not feats or not targets:
            continue
        X.append([float(feats.get(c, 0.0)) for c in cols])
        for t, _ in TARGETS:
            y[t].append(float(targets[t]))
        dates.append(g["date"])
    return X, y, dates, cols


def _split_chronological(X, y_dict, dates, test_frac=0.20):
    n = len(X)
    if n == 0:
        return {"X_train": [], "X_test": [], "y_train": {}, "y_test": {},
                 "n_train": 0, "n_test": 0, "test_start_date": None}
    cut = int(n * (1 - test_frac))
    return {
        "X_train": X[:cut],
        "X_test": X[cut:],
        "y_train": {t: ys[:cut] for t, ys in y_dict.items()},
        "y_test": {t: ys[cut:] for t, ys in y_dict.items()},
        "n_train": cut,
        "n_test": n - cut,
        "test_start_date": dates[cut] if cut < n else None,
    }


def train(league: str, *, test_frac: float = 0.20) -> dict:
    """Train all three targets for ``league``. Persists each model to
    ``data/hockey/<league>_gbm/<target>.json``."""
    if league not in HOCKEY_LEAGUE_REGISTRY:
        raise KeyError(f"Unknown hockey league {league!r}")
    if league == "nhl":
        raise ValueError("NHL has its own GBM path in engine.gbm.predict")
    try:
        import xgboost as xgb
    except ImportError as e:
        raise RuntimeError("XGBoost not installed") from e

    X, y, dates, cols = _load_dataset(league)
    if len(X) < 200:
        raise ValueError(
            f"Only {len(X)} labeled games for {league!r} (need >= 200); "
            f"backfill more or skip GBM for this league"
        )

    split = _split_chronological(X, y, dates, test_frac=test_frac)
    out_dir = _MODELS_DIR / f"{league}_gbm"
    out_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "league": league,
        "n_train": split["n_train"],
        "n_test": split["n_test"],
        "test_start_date": split["test_start_date"],
        "feature_columns": cols,
        "models": {},
    }

    for target, task in TARGETS:
        params = _params_for(task)
        dtrain = xgb.DMatrix(split["X_train"],
                              label=split["y_train"][target],
                              feature_names=cols)
        dtest = xgb.DMatrix(split["X_test"],
                             label=split["y_test"][target],
                             feature_names=cols)
        booster = xgb.train(
            params, dtrain,
            num_boost_round=300,
            evals=[(dtrain, "train"), (dtest, "test")],
            verbose_eval=False,
            early_stopping_rounds=20,
        )
        pred_test = booster.predict(dtest)
        metrics = _metrics(task, split["y_test"][target], list(pred_test))
        summary["models"][target] = metrics
        booster.save_model(str(_model_path(league, target)))
        logger.info("[%s gbm] %s trained: %s", league, target, metrics)

    (_MODELS_DIR / f"{league}_gbm" / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8",
    )
    return summary


def _metrics(task: str, y_true: list[float], y_pred: list[float]) -> dict:
    n = len(y_true)
    if n == 0:
        return {"n": 0}
    if task == "regression":
        sse = sum((a - b) ** 2 for a, b in zip(y_true, y_pred))
        mae = sum(abs(a - b) for a, b in zip(y_true, y_pred)) / n
        return {"n": n, "rmse": float(round((sse / n) ** 0.5, 3)),
                 "mae": float(round(mae, 3))}
    import math
    correct = sum(1 for yt, yp in zip(y_true, y_pred)
                   if (yp >= 0.5) == (yt >= 0.5))
    eps = 1e-7
    ll = -sum(
        yt * math.log(max(min(yp, 1 - eps), eps))
        + (1 - yt) * math.log(max(min(1 - yp, 1 - eps), eps))
        for yt, yp in zip(y_true, y_pred)
    ) / n
    return {"n": n, "accuracy": round(correct / n, 3),
             "logloss": round(ll, 3)}


def _params_for(task: str) -> dict:
    base = {
        "max_depth": 4,
        "eta": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 8,
        "reg_lambda": 1.0,
        "tree_method": "hist",
        "verbosity": 0,
    }
    if task == "classification":
        base["objective"] = "binary:logistic"
        base["eval_metric"] = "logloss"
    else:
        base["objective"] = "reg:squarederror"
        base["eval_metric"] = "rmse"
    return base


_BOOSTER_CACHE: dict[tuple[str, str], Any] = {}


def predict(league: str, target: str, features: dict[str, float]) -> float | None:
    cache_key = (league, target)
    booster = _BOOSTER_CACHE.get(cache_key)
    if booster is None:
        path = _model_path(league, target)
        if not path.exists():
            return None
        try:
            import xgboost as xgb
            booster = xgb.Booster()
            booster.load_model(str(path))
            _BOOSTER_CACHE[cache_key] = booster
        except Exception as e:
            logger.warning("load %s/%s failed: %s", league, target, e)
            return None
    cols = feature_columns()
    import xgboost as xgb
    row = [[float(features.get(c, 0.0)) for c in cols]]
    d = xgb.DMatrix(row, feature_names=cols)
    pred = booster.predict(d)
    return float(pred[0]) if len(pred) else None


def reset_cache() -> None:
    _BOOSTER_CACHE.clear()


def is_available(league: str) -> bool:
    """True iff all three targets for ``league`` have trained models on disk."""
    return all(_model_path(league, t).exists() for t, _ in TARGETS)


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.hockey._gbm")
    ap.add_argument("league", choices=("ahl", "pwhl"))
    ap.add_argument("--test-frac", type=float, default=0.20)
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    summary = train(args.league, test_frac=args.test_frac)
    print(json.dumps({
        "n_train": summary["n_train"],
        "n_test": summary["n_test"],
        "test_start_date": summary["test_start_date"],
        "models": summary["models"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
