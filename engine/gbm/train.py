"""
GBM training pipeline.

Fits gradient-boosted tree models (XGBoost by default, or whatever
scikit-compatible estimator is configured) on historical game
outcomes, one model per target, per sport. Versioned artifacts land
in data/models/<sport>_gbm_<target>_<YYYYMMDD>.json.

The backend loads the newest artifact at inference time (see
engine.gbm.predict). Weekly retraining via Task Scheduler keeps the
model fresh as rosters / rotations change.

Usage:
    python -m engine.gbm.train mlb                 # train all MLB targets
    python -m engine.gbm.train mlb --target home_win
    python -m engine.gbm.train mlb --days 365      # train on last 365 days
    python -m engine.gbm.train mlb --dry-run       # show data stats, no fit
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
MODELS_DIR = ROOT / "data" / "models"

SPORT_TARGETS = {
    "mlb": [
        ("home_win",     "classification"),
        ("nrfi_hit",     "classification"),
        ("f5_home_win",  "classification"),
        ("total_runs",   "regression"),
        ("f5_total",     "regression"),
    ],
}


def train_sport(sport: str, target: str | None = None,
                 days: int | None = None,
                 dry_run: bool = False) -> dict:
    """Train one GBM per target for a sport. Returns a summary dict.

    Data is split into train / validation chronologically (last 15% as
    validation). Early-stopping kicks in on validation logloss / RMSE
    so we don't over-fit when historical data is thin.
    """
    if sport not in SPORT_TARGETS:
        return {"error": f"Unsupported sport: {sport}"}

    try:
        import numpy as np
        import pandas as pd
        import xgboost as xgb
    except ImportError as e:
        return {"error": f"GBM deps not installed: {e}"}

    logger.info("Loading historical %s games...", sport)
    games_df, targets_df = _load_dataset(sport, days=days)
    if games_df is None or games_df.empty:
        return {"error": "No historical games found"}

    targets_to_train = [t for t in SPORT_TARGETS[sport] if target is None or t[0] == target]

    report: dict[str, Any] = {
        "sport": sport,
        "trained_at": datetime.now().isoformat(timespec="seconds"),
        "n_games": len(games_df),
        "date_range": (
            str(games_df["_date"].min()),
            str(games_df["_date"].max()),
        ),
        "targets": {},
    }

    if dry_run:
        logger.info("Dry run: %d games, %d candidate targets", len(games_df),
                    len(targets_to_train))
        report["targets"] = {name: "skipped (dry run)" for name, _ in targets_to_train}
        return report

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    from .features import FEATURE_NAMES
    X_full = games_df[FEATURE_NAMES].astype(float)

    # Chronological split: last 15% is validation
    n = len(X_full)
    split = int(n * 0.85)

    today_tag = datetime.now().strftime("%Y%m%d")

    for target_name, task in targets_to_train:
        if target_name not in targets_df.columns:
            report["targets"][target_name] = {"skipped": "no column"}
            continue
        y_all = targets_df[target_name]
        mask = y_all.notna()
        if mask.sum() < 500:
            report["targets"][target_name] = {
                "skipped": f"only {int(mask.sum())} samples",
            }
            continue

        X = X_full.loc[mask]
        y = y_all.loc[mask]

        n2 = len(y)
        split2 = int(n2 * 0.85)
        X_train, X_val = X.iloc[:split2], X.iloc[split2:]
        y_train, y_val = y.iloc[:split2], y.iloc[split2:]

        params = _params_for(task)
        t0 = time.time()

        if task == "classification":
            model = xgb.XGBClassifier(**params)
        else:
            model = xgb.XGBRegressor(**params)

        try:
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                verbose=False,
            )
        except Exception as e:
            report["targets"][target_name] = {"error": str(e)}
            continue

        elapsed = time.time() - t0

        # Compute simple metric
        if task == "classification":
            val_pred_proba = model.predict_proba(X_val)[:, 1]
            val_pred = (val_pred_proba >= 0.5).astype(int)
            acc = float((val_pred == y_val.to_numpy()).mean())
            # Brier score is a proper scoring rule for calibration
            brier = float(((val_pred_proba - y_val.to_numpy()) ** 2).mean())
            metric = {"accuracy": round(acc, 4), "brier": round(brier, 4)}
        else:
            val_pred = model.predict(X_val)
            rmse = float(np.sqrt(((val_pred - y_val.to_numpy()) ** 2).mean()))
            metric = {"rmse": round(rmse, 4)}

        # Save versioned + latest-symlink-equivalent (plain copy)
        dated = MODELS_DIR / f"{sport}_gbm_{target_name}_{today_tag}.json"
        latest = MODELS_DIR / f"{sport}_gbm_{target_name}_latest.json"
        model.save_model(str(dated))
        model.save_model(str(latest))

        # Persist metadata alongside
        meta = {
            "sport": sport,
            "target": target_name,
            "task": task,
            "n_train": len(y_train),
            "n_val": len(y_val),
            "feature_names": FEATURE_NAMES,
            "trained_at": datetime.now().isoformat(timespec="seconds"),
            "metric": metric,
            "elapsed_sec": round(elapsed, 2),
        }
        (MODELS_DIR / f"{sport}_gbm_{target_name}_{today_tag}.meta.json").write_text(
            json.dumps(meta, indent=2)
        )
        (MODELS_DIR / f"{sport}_gbm_{target_name}_latest.meta.json").write_text(
            json.dumps(meta, indent=2)
        )

        report["targets"][target_name] = meta
        logger.info("Trained %s %s -> %s", sport, target_name, metric)

    return report


def _load_dataset(sport: str, days: int | None = None):
    """Pull completed games from the DB and extract features + targets.

    Returns (features_df, targets_df) indexed by row. Any game where
    feature extraction returns None is dropped.
    """
    try:
        import pandas as pd
    except ImportError:
        return None, None

    if sport == "mlb":
        from engine.db import get_conn
        conn = get_conn()
        q = (
            "SELECT mlb_game_id, date, home_team_id, away_team_id, "
            "       home_pitcher_id, away_pitcher_id, venue, "
            "       home_score, away_score, home_linescore, away_linescore, "
            "       weather_temp, weather_wind, umpire "
            "FROM games WHERE status = 'final'"
        )
        params: tuple = ()
        if days:
            cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            q += " AND date >= ?"
            params = (cutoff,)
        q += " ORDER BY date ASC"
        rows = [dict(r) for r in conn.execute(q, params).fetchall()]

        from .features import extract_mlb_features, extract_target, FEATURE_NAMES

        feats = []
        targets = []
        for g in rows:
            f = extract_mlb_features(conn, g)
            if not f:
                continue
            t = extract_target(g)
            if not t:
                continue
            f["_date"] = g["date"]
            feats.append(f)
            targets.append(t)

        if not feats:
            return None, None
        df_f = pd.DataFrame(feats)
        df_t = pd.DataFrame(targets)
        return df_f, df_t
    return None, None


def _params_for(task: str) -> dict:
    """Default XGBoost params -- tuned for our data-scale regime.

    Small enough to train in seconds on ~8000 games and avoid overfitting
    when historical data is thinner. No hyperparameter search -- the
    defaults are strong and search adds a lot of complexity for marginal
    gain at this scale.
    """
    common = dict(
        n_estimators=400,
        learning_rate=0.05,
        max_depth=4,
        min_child_weight=5,
        subsample=0.85,
        colsample_bytree=0.85,
        tree_method="hist",
        random_state=42,
        early_stopping_rounds=30,
    )
    if task == "classification":
        common.update(dict(
            objective="binary:logistic",
            eval_metric="logloss",
        ))
    else:
        common.update(dict(
            objective="reg:squarederror",
            eval_metric="rmse",
        ))
    return common


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sport", choices=sorted(SPORT_TARGETS.keys()))
    parser.add_argument("--target", default=None)
    parser.add_argument("--days", type=int, default=None,
                        help="Only train on games in the last N days")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(levelname)-7s %(message)s")

    r = train_sport(args.sport, target=args.target, days=args.days,
                    dry_run=args.dry_run)
    print(json.dumps(r, indent=2, default=str))
    return 0 if "error" not in r else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
