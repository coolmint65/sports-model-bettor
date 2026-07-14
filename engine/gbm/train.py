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
    "nhl": [
        ("home_win",        "classification"),
        ("total_goals",     "regression"),
        ("p1_home_win",     "classification"),
        ("p1_total_goals",  "regression"),
        # Puck-line cover: 1 if home wins by 2+ goals. Pure classification —
        # NHL spreads are virtually always ±1.5 so a regression on
        # margin would be over-specified. Added 2026-05-18 for V3.1
        # market-feature validation on NHL spreads.
        ("home_pl_cover",   "classification"),
    ],
    "nba": [
        ("home_win",         "classification"),
        ("total_points",     "regression"),
        ("margin",           "regression"),
        ("q1_home_win",      "classification"),
        ("q1_total_points",  "regression"),
        ("q1_margin",        "regression"),
    ],
    # Phase 6.5 — Tennis. Targets at the (tour, target) level so
    # ATP / WTA fit independent models. Tour selection happens via
    # a feature-row 'tour' filter at training time, then the model
    # artifact name carries the tour: tennis_atp_p1_win_<date>.json
    "tennis_atp": [
        ("p1_win",         "classification"),
        ("total_games",    "regression"),
        ("straight_sets",  "classification"),
    ],
    "tennis_wta": [
        ("p1_win",         "classification"),
        ("total_games",    "regression"),
        ("straight_sets",  "classification"),
    ],
    # Phase 5e — Live GBM. State-at-time-T → final outcome. Trained
    # on the historical_pbp corpus (1.97M NBA / 1.36M NHL plays).
    # One model per (sport, target). Ensembles with the analytical
    # 5h/5i predictors at inference time via engine.gbm.predict.
    "nba_live": [
        ("final_total_points", "regression"),
        ("final_margin",       "regression"),
        ("home_final_win",     "classification"),
    ],
    "nhl_live": [
        ("final_total_points", "regression"),
        ("final_margin",       "regression"),
        ("home_final_win",     "classification"),
    ],
    # WNBA live — same shape as NBA live. Backfilled from historical_pbp
    # 2026-05-15 via engine.backfill_pbp; build_live_dataset_nba already
    # works on the historical_pbp table sport-agnostically once we
    # parametrize the team-table source.
    "wnba_live": [
        ("final_total_points", "regression"),
        ("final_margin",       "regression"),
        ("home_final_win",     "classification"),
    ],
    # NCAAM live — 2 × 20-min halves so the model effectively learns
    # H1-end → final state. Single intermission per game means fewer
    # state samples per game than NBA/WNBA (1 vs 3) but the 5000-game
    # backfill more than compensates.
    "ncaam_live": [
        ("final_total_points", "regression"),
        ("final_margin",       "regression"),
        ("home_final_win",     "classification"),
    ],
    # AFL live — 4 × 20-min quarters. ESPN ships ~50 plays per game
    # (only goals/behinds/period-end are tracked). The feature shape
    # reuses NBA_LIVE_FEATURE_NAMES; "made_fg"/"missed_fg" map to
    # AFL's "goal kicked"/"behind kicked"/"miss" play types via the
    # generic shooting_play flag.
    "afl_live": [
        ("final_total_points", "regression"),
        ("final_margin",       "regression"),
        ("home_final_win",     "classification"),
    ],
}


def train_sport(sport: str, target: str | None = None,
                 days: int | None = None,
                 dry_run: bool = False,
                 time_decay_half_life_days: int | None = None) -> dict:
    """Train one GBM per target for a sport. Returns a summary dict.

    Data is split into train / validation chronologically (last 15% as
    validation). Early-stopping kicks in on validation logloss / RMSE
    so we don't over-fit when historical data is thin.

    ``time_decay_half_life_days`` (opt-in) — when set, training rows
    get a sample weight ``0.5 ** (days_old / half_life)`` where
    days_old is measured from the most-recent match in the training
    set. A 730-day half-life weighting (2 years) means a 6-year-old
    match contributes ~13% as much as today's; a 2-year-old match
    contributes 50%. This counters the "career-long average drowns
    recent dominance" failure mode that surfaced in tennis (Sinner-
    Zverev 2026-05-03 case: GBM trained on the full 25-year corpus
    said Zverev 74% on clay; recent reality is the opposite).
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
    feature_names = _feature_names_for(sport)
    X_full = games_df[feature_names].astype(float)

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

        # Per-row sample weights — opt-in time decay. Anchor the
        # half-life clock to the most-recent training match so an
        # older corpus still gets sensible weights (a half-life of
        # 730 days against a 2024 cutoff puts 2018 matches at ~13%).
        sample_weight = None
        if time_decay_half_life_days:
            train_dates = pd.to_datetime(games_df.loc[mask, "_date"]
                                         .iloc[:split2])
            anchor = train_dates.max()
            days_old = (anchor - train_dates).dt.days.clip(lower=0)
            half_life = float(time_decay_half_life_days)
            sample_weight = 0.5 ** (days_old.to_numpy() / half_life)

        params = _params_for(task)
        t0 = time.time()

        if task == "classification":
            model = xgb.XGBClassifier(**params)
        else:
            model = xgb.XGBRegressor(**params)

        try:
            fit_kwargs = {
                "eval_set": [(X_val, y_val)],
                "verbose": False,
            }
            if sample_weight is not None:
                fit_kwargs["sample_weight"] = sample_weight
            model.fit(X_train, y_train, **fit_kwargs)
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
            "feature_names": feature_names,
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

    if sport == "nhl":
        from engine.nhl_db import get_conn as _nhl_conn
        conn = _nhl_conn()
        q = (
            "SELECT game_id, date, home_team_id, away_team_id, "
            "       home_score, away_score, "
            "       home_p1, away_p1, home_p2, away_p2, home_p3, away_p3, "
            "       home_shots, away_shots, "
            "       home_pp_goals, home_pp_opps, away_pp_goals, away_pp_opps, "
            "       home_faceoff_pct, away_faceoff_pct, season, game_type "
            "FROM nhl_games WHERE status = 'final' "
            "  AND home_score IS NOT NULL AND away_score IS NOT NULL"
        )
        params = ()
        if days:
            cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            q += " AND date >= ?"
            params = (cutoff,)
        q += " ORDER BY date ASC"
        rows = [dict(r) for r in conn.execute(q, params).fetchall()]

        from .features_nhl import extract_nhl_features, extract_nhl_target

        feats = []
        targets = []
        for g in rows:
            f = extract_nhl_features(conn, g)
            if not f:
                continue
            t = extract_nhl_target(g)
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

    if sport == "nba":
        from engine.nba_db import get_conn as _nba_conn
        conn = _nba_conn()
        q = (
            "SELECT game_id, date, home_team_id, away_team_id, "
            "       home_score, away_score, "
            "       home_q1, away_q1, home_q2, away_q2, "
            "       home_q3, away_q3, home_q4, away_q4, "
            "       home_pace, away_pace, season "
            "FROM nba_games WHERE status = 'final' "
            "  AND home_score IS NOT NULL AND away_score IS NOT NULL"
        )
        params = ()
        if days:
            cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            q += " AND date >= ?"
            params = (cutoff,)
        q += " ORDER BY date ASC"
        rows = [dict(r) for r in conn.execute(q, params).fetchall()]

        from .features_nba import extract_nba_features, extract_nba_target

        feats = []
        targets = []
        for g in rows:
            f = extract_nba_features(conn, g)
            if not f:
                continue
            t = extract_nba_target(g)
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

    # Phase 5e Live GBM — features built from historical_pbp via
    # engine.gbm.features_live. One feature row per (game, period_end)
    # so games contribute multiple training rows (different states,
    # same final outcome).
    if sport in ("nba_live", "nhl_live", "wnba_live", "ncaam_live", "afl_live"):
        from .features_live import (
            build_live_dataset_nba, build_live_dataset_nhl,
            build_live_dataset_wnba, build_live_dataset_ncaam,
            build_live_dataset_afl,
        )
        if sport == "nba_live":
            df_f, df_t = build_live_dataset_nba(
                limit=int(days) if days else None
            )
        elif sport == "wnba_live":
            df_f, df_t = build_live_dataset_wnba(
                limit=int(days) if days else None
            )
        elif sport == "ncaam_live":
            df_f, df_t = build_live_dataset_ncaam(
                limit=int(days) if days else None
            )
        elif sport == "afl_live":
            df_f, df_t = build_live_dataset_afl(
                limit=int(days) if days else None
            )
        else:
            df_f, df_t = build_live_dataset_nhl(
                limit=int(days) if days else None
            )
        if df_f is None or df_f.empty:
            return None, None
        # Synth a dummy _date col since features_live emits empty
        # placeholders. The chronological split in train_sport sorts
        # by row order which is already chronological from the games
        # query.
        df_f["_date"] = ""
        return df_f, df_t

    # Tennis — sport key carries the tour suffix ("tennis_atp" /
    # "tennis_wta") so MODELS_DIR artifacts are unambiguous and
    # training filters cleanly.
    if sport in ("tennis_atp", "tennis_wta"):
        tour = sport.split("_", 1)[1]
        from engine.tennis_db import get_conn as _tennis_conn, ensure_tables
        ensure_tables()
        conn = _tennis_conn()
        q = (
            "SELECT * FROM tennis_matches "
            "WHERE tour = ? AND winner_id IS NOT NULL "
            "  AND loser_id IS NOT NULL AND tourney_date IS NOT NULL"
        )
        params = (tour,)
        if days:
            cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            q += " AND tourney_date >= ?"
            params = (tour, cutoff)
        q += " ORDER BY tourney_date ASC, match_id ASC"
        rows = [dict(r) for r in conn.execute(q, params).fetchall()]

        from .features_tennis import (
            PlayerHistory, extract_match_features, extract_match_target,
        )
        from engine.tennis_elo import rating_for as _elo_for

        # Cache Elo lookups during the chronological walk so we don't
        # re-query SQLite for each player. The 'all' + per-surface
        # ratings are LIVE values (computed once over full history) —
        # for true point-in-time training we'd retrain Elo per row
        # but the variance is small enough that this is acceptable
        # for MVP. Future improvement: emit per-month Elo snapshots
        # and look up by (player, month-before-match).
        elo_cache: dict = {}
        def _elo_lookup(t: str, pid: int, surface: str) -> dict | None:
            key = (t, int(pid), surface)
            if key not in elo_cache:
                elo_cache[key] = _elo_for(t, pid, surface=surface,
                                           fallback_to_all=False)
            return elo_cache[key]

        history = PlayerHistory()
        feats: list[dict] = []
        targets: list[dict] = []
        for m in rows:
            t = extract_match_target(m)
            if not t:
                continue
            f = extract_match_features(history, m, _elo_lookup)
            if not f:
                continue
            f["_date"] = m.get("tourney_date")
            feats.append(f)
            targets.append(t)

        if not feats:
            return None, None
        df_f = pd.DataFrame(feats)
        df_t = pd.DataFrame(targets)
        return df_f, df_t

    return None, None


def _feature_names_for(sport: str) -> list[str]:
    """Return the feature-name list for the given sport."""
    if sport == "mlb":
        from .features import FEATURE_NAMES
        return FEATURE_NAMES
    if sport == "nhl":
        from .features_nhl import NHL_FEATURE_NAMES
        return NHL_FEATURE_NAMES
    if sport == "nba":
        from .features_nba import NBA_FEATURE_NAMES
        return NBA_FEATURE_NAMES
    if sport in ("tennis_atp", "tennis_wta"):
        from .features_tennis import FEATURE_NAMES as _T
        return _T
    if sport in ("nba_live", "wnba_live", "ncaam_live", "afl_live"):
        # WNBA + NCAAM + AFL reuse the NBA live feature set 1:1 — same
        # ESPN PBP, same period-end signal, same scoring play schema.
        # AFL "made_fg"/"missed_fg" map to goals/behinds via the
        # generic shooting_play flag in features_live._nba_state_to_features.
        from .features_live import NBA_LIVE_FEATURE_NAMES
        return NBA_LIVE_FEATURE_NAMES
    if sport == "nhl_live":
        from .features_live import NHL_LIVE_FEATURE_NAMES
        return NHL_LIVE_FEATURE_NAMES
    raise ValueError(f"Unknown sport: {sport}")


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
    parser.add_argument("--time-decay-half-life-days", type=int, default=None,
                        help="Apply time-decay sample weights with this "
                             "half-life (recent matches weighted more). "
                             "Tennis defaults to 730 (2 years) when omitted "
                             "to counter career-long surface-specialization "
                             "bias against current form.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(levelname)-7s %(message)s")

    # Tennis-specific default: 2-year half-life if the caller didn't
    # specify. Player skill / surface dominance shifts within ~2 years,
    # so older matches should contribute fractionally. Other sports
    # default to no decay (legacy behavior).
    half_life = args.time_decay_half_life_days
    if half_life is None and args.sport.startswith("tennis_"):
        half_life = 730

    r = train_sport(args.sport, target=args.target, days=args.days,
                    dry_run=args.dry_run,
                    time_decay_half_life_days=half_life)
    print(json.dumps(r, indent=2, default=str))
    return 0 if "error" not in r else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
