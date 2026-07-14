"""Tennis distribution-market predictor (GBM regression, Stage 0.8).

Replaces the failed serve-MC simulator for total_games / total_sets /
set_spread style markets with a gradient-boosted regression on
match-level features.

Why GBM instead of serve-MC
---------------------------
Serve-MC failed Stage 0 + 0.5 + 0.7 (Bradley-Terry) for total_games:
RMSE 9.68 vs naive predict-the-mean RMSE 6.10. Pure serve-stat
simulation overpredicts games systematically because match-aggregate
serve rates can't extract the matchup-specific skill gap from the
observed data.

GBM regression sidesteps the simulation entirely — feeds match-level
features into a tree model that learns the empirical mapping to
total_games. Easily beats predict-the-mean as long as feature
correlations with total_games exist (they do — surface, best_of,
elo_gap, server quality gap all matter).

Features (per match, all PIT-correct)
-------------------------------------
- best_of                 (3 or 5)
- surface                 (one-hot: Hard/Clay/Grass)
- elo_gap                 abs(p1_elo - p2_elo) on surface
- p1_elo, p2_elo          (per surface)
- p1_serve_rate_raw       career serve win rate before match
- p2_serve_rate_raw
- p1_serve_rate_surface   surface-specific serve rate before match
- p2_serve_rate_surface
- p1_return_rate_raw, p2_return_rate_raw
- avg_serve_rate          mean of both player serve rates
- serve_rate_gap          abs(p1 - p2) serve rate
- tour                    one-hot atp/wta

Target: total_games (regression).

Persistence: trained model saved to data/models/tennis_dist_gbm_<target>_<YYYYMMDD>.json
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime
from pathlib import Path
from typing import Iterable
from ._tz import et_today_str

logger = logging.getLogger(__name__)


_MODELS_DIR = Path(__file__).resolve().parent.parent / "data" / "models"

_FEATURE_NAMES = [
    "best_of",
    "is_clay", "is_grass", "is_hard",
    "is_atp",
    "elo_gap", "elo_max",
    "p1_serve_raw", "p2_serve_raw",
    "p1_serve_surface", "p2_serve_surface",
    "avg_serve_rate", "serve_rate_gap",
]


def _surface_serve_prior(surface: str) -> float:
    return {"Hard": 0.608, "Clay": 0.591, "Grass": 0.631, "Carpet": 0.634}.get(
        surface, 0.60,
    )


def _player_serve_rate_quick(conn, tour: str, player_id: int,
                              surface: str | None,
                              cutoff_date: str) -> float:
    """Quick career serve rate before cutoff. Surface-filtered if given."""
    args: list = [tour, player_id, player_id]
    where = ("WHERE tour = ? AND (winner_id = ? OR loser_id = ?) "
             "AND tourney_date < ? "
             "AND w_svpt IS NOT NULL AND l_svpt IS NOT NULL")
    args.append(cutoff_date)
    if surface:
        where += " AND surface = ?"
        args.append(surface)
    row = conn.execute(
        f"SELECT SUM(CASE WHEN winner_id=? THEN w_1stWon+w_2ndWon "
        f"             ELSE l_1stWon+l_2ndWon END) AS won, "
        f"      SUM(CASE WHEN winner_id=? THEN w_svpt ELSE l_svpt END) AS pts "
        f"FROM tennis_matches {where}",
        [player_id, player_id] + args,
    ).fetchone()
    if row and row["pts"] and row["pts"] > 50:
        return row["won"] / row["pts"]
    return _surface_serve_prior(surface or "Hard")


def _build_feature_row(conn, match: dict) -> list[float]:
    """Construct one feature row for a match using PIT data only."""
    surface = match["surface"] or "Hard"
    tour = match["tour"]
    cutoff = match["tourney_date"]

    # Elo (PIT) — pull from tennis_elo if present
    p1_elo = p2_elo = 1500.0
    try:
        rows = conn.execute(
            "SELECT player_id, rating FROM tennis_elo "
            "WHERE tour = ? AND surface = ? AND player_id IN (?, ?) "
            "  AND date < ? "
            "ORDER BY date DESC",
            (tour, surface, match["p1_id"], match["p2_id"], cutoff),
        ).fetchall()
        seen = set()
        for r in rows:
            if r["player_id"] in seen:
                continue
            seen.add(r["player_id"])
            if r["player_id"] == match["p1_id"]:
                p1_elo = float(r["rating"])
            else:
                p2_elo = float(r["rating"])
            if len(seen) == 2:
                break
    except Exception:
        pass

    p1_serve_raw = _player_serve_rate_quick(conn, tour, match["p1_id"],
                                             None, cutoff)
    p2_serve_raw = _player_serve_rate_quick(conn, tour, match["p2_id"],
                                             None, cutoff)
    p1_serve_surf = _player_serve_rate_quick(conn, tour, match["p1_id"],
                                              surface, cutoff)
    p2_serve_surf = _player_serve_rate_quick(conn, tour, match["p2_id"],
                                              surface, cutoff)

    return [
        float(match["best_of"]),
        1.0 if surface == "Clay" else 0.0,
        1.0 if surface == "Grass" else 0.0,
        1.0 if surface == "Hard" else 0.0,
        1.0 if tour == "atp" else 0.0,
        abs(p1_elo - p2_elo),
        max(p1_elo, p2_elo),
        p1_serve_raw, p2_serve_raw,
        p1_serve_surf, p2_serve_surf,
        (p1_serve_surf + p2_serve_surf) / 2.0,
        abs(p1_serve_surf - p2_serve_surf),
    ]


# ── Training ────────────────────────────────────────────────────

def _parse_score_total_games(score: str | None) -> int | None:
    if not score:
        return None
    import re as _re
    total = 0
    found = False
    for m in _re.finditer(r"(\d+)-(\d+)", score):
        total += int(m.group(1)) + int(m.group(2))
        found = True
    return total if found else None


def _load_training_rows(tour_filter: str | None,
                         min_date: str | None,
                         max_date: str | None) -> list[dict]:
    """Pull every settled match with the data we need to build a row.
    Returns list of dicts ready for _build_feature_row + a target."""
    from .tennis_db import get_conn
    conn = get_conn()
    where = ["w_svpt IS NOT NULL", "l_svpt IS NOT NULL",
             "score IS NOT NULL", "score != ''",
             "surface IN ('Hard','Clay','Grass')",
             "best_of IN (3, 5)"]
    args: list = []
    if tour_filter:
        where.append("tour = ?")
        args.append(tour_filter)
    if min_date:
        where.append("tourney_date >= ?")
        args.append(min_date)
    if max_date:
        where.append("tourney_date <= ?")
        args.append(max_date)
    rows = conn.execute(
        f"SELECT tour, match_id, tourney_date, surface, best_of, "
        f"       winner_id, loser_id, score "
        f"FROM tennis_matches WHERE {' AND '.join(where)} "
        f"ORDER BY tourney_date",
        args,
    ).fetchall()
    out = []
    for r in rows:
        # Use winner_id/loser_id arbitrarily as p1/p2 — features are
        # symmetric in the relevant dimensions (gap, sum, surface).
        # Target is total_games (matchup-symmetric anyway).
        total = _parse_score_total_games(r["score"])
        if total is None or total < 12 or total > 80:
            continue
        out.append({
            "tour": r["tour"],
            "match_id": r["match_id"],
            "tourney_date": r["tourney_date"],
            "surface": r["surface"],
            "best_of": int(r["best_of"]),
            "p1_id": r["winner_id"],
            "p2_id": r["loser_id"],
            "total_games": total,
        })
    return out


def fit(target: str = "total_games",
        train_min_date: str = "2018-01-01",
        train_max_date: str | None = None,
        sample_limit: int | None = 30000) -> dict:
    """Fit a GBM on (features → target). Saves under data/models/.

    sample_limit: cap for fit speed during iteration (None = use all).
    Default 30000 keeps fit under ~1 minute even with on-the-fly feature
    construction.
    """
    try:
        import xgboost as xgb
    except ImportError:
        raise RuntimeError("xgboost required — install or change to "
                           "sklearn.GradientBoostingRegressor here")

    from .tennis_db import get_conn
    conn = get_conn()

    matches = _load_training_rows(None, train_min_date, train_max_date)
    if not matches:
        return {"error": "no training rows"}
    if sample_limit and len(matches) > sample_limit:
        # Even-sample across the date range so train + holdout cover same era
        step = len(matches) // sample_limit
        matches = matches[::step][:sample_limit]
    logger.info("Building features for %d matches (target=%s)",
                len(matches), target)

    X = []
    y = []
    for i, m in enumerate(matches):
        if i % 5000 == 0 and i:
            logger.info("  features: %d/%d", i, len(matches))
        try:
            X.append(_build_feature_row(conn, m))
            y.append(float(m[target]))
        except Exception as exc:
            logger.debug("feature row failed for %s: %s", m["match_id"], exc)
            continue

    if not X:
        return {"error": "no usable feature rows"}

    # 80/20 chronological split (X built in date order)
    n = len(X)
    cut = int(n * 0.8)
    X_train, y_train = X[:cut], y[:cut]
    X_val, y_val = X[cut:], y[cut:]

    model = xgb.XGBRegressor(
        n_estimators=400,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        early_stopping_rounds=20,
        eval_metric="rmse",
    )
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

    preds = model.predict(X_val)
    sse = sum((p - y) ** 2 for p, y in zip(preds, y_val))
    val_rmse = math.sqrt(sse / len(y_val))

    # Naive baseline for context
    train_mean = sum(y_train) / len(y_train)
    naive_sse = sum((train_mean - y) ** 2 for y in y_val)
    naive_rmse = math.sqrt(naive_sse / len(y_val))

    # Persist
    _MODELS_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    model_path = _MODELS_DIR / f"tennis_dist_gbm_{target}_{today}.json"
    model.save_model(str(model_path))
    latest_path = _MODELS_DIR / f"tennis_dist_gbm_{target}_latest.json"
    model.save_model(str(latest_path))

    meta = {
        "target": target,
        "n_train": len(X_train),
        "n_val": len(X_val),
        "val_rmse": round(val_rmse, 4),
        "naive_predict_mean_rmse": round(naive_rmse, 4),
        "improvement_vs_naive": round(naive_rmse - val_rmse, 4),
        "feature_names": _FEATURE_NAMES,
        "trained_at": datetime.now().isoformat(timespec="seconds"),
    }
    meta_path = _MODELS_DIR / f"tennis_dist_gbm_{target}_{today}.meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    (_MODELS_DIR / f"tennis_dist_gbm_{target}_latest.meta.json").write_text(
        json.dumps(meta, indent=2), encoding="utf-8",
    )
    logger.info("Saved %s (val_rmse=%.3f vs naive %.3f, delta=%+.3f)",
                model_path.name, val_rmse, naive_rmse,
                naive_rmse - val_rmse)
    return meta


# ── Predict ────────────────────────────────────────────────────

_MODEL_CACHE: dict = {}


def predict_total_games(tour: str, p1_id: int, p2_id: int, *,
                         surface: str, best_of: int,
                         cutoff_date: str | None) -> float | None:
    """Predict total games for a single match. Returns None when no
    model exists or features can't be built."""
    try:
        import xgboost as xgb
    except ImportError:
        return None
    if "total_games" not in _MODEL_CACHE:
        latest = _MODELS_DIR / "tennis_dist_gbm_total_games_latest.json"
        if not latest.exists():
            return None
        m = xgb.XGBRegressor()
        m.load_model(str(latest))
        _MODEL_CACHE["total_games"] = m
    model = _MODEL_CACHE["total_games"]

    from .tennis_db import get_conn
    conn = get_conn()
    match = {
        "tour": tour, "p1_id": p1_id, "p2_id": p2_id,
        "surface": surface, "best_of": best_of,
        "tourney_date": cutoff_date or et_today_str(),
    }
    try:
        x = _build_feature_row(conn, match)
    except Exception:
        return None
    pred = model.predict([x])
    return float(pred[0])


# ── CLI ────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="Tennis distribution GBM")
    p.add_argument("--fit", action="store_true",
                   help="Train the total_games regressor")
    p.add_argument("--min-date", default="2018-01-01",
                   help="Minimum training match date")
    p.add_argument("--limit", type=int, default=30000,
                   help="Sample cap for fit speed")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    if args.fit:
        result = fit(target="total_games",
                     train_min_date=args.min_date,
                     sample_limit=args.limit)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
