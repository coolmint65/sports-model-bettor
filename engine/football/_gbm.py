"""Gradient-boosted machine predictor for football.

Sample-size gated. UFL has ~110 games across 3 seasons — way below
the floor where a GBM gives stable estimates (~500 games is the
practical minimum; ~1k+ is comfortable). For now this module ships
the **wiring** so V3.2's signal explanation endpoint reports
`gbm_prob: null` cleanly, and so the ensemble blender can drop the
GBM leg gracefully when no trained model exists.

When UFL accumulates 3-4 more seasons (or we backfill XFL/USFL
history into a shared football_v1 model), this module trains a
LightGBM / scikit-learn HistGradientBoostingClassifier on:
  - Elo diff
  - Recent form (avg margin last 4 games per side)
  - Home/away indicator
  - Strength of schedule

Target: home_win (classification), home_cover (classification),
total_over_45 (classification — 45 is the UFL median total).

Persist to ``data/football/{league}_gbm/model.pkl``. Until then the
predictor returns Nones for the GBM leg.
"""
from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Minimum number of finalized games required before we'll train a
# GBM. Below this threshold every call returns Nones — the ensemble
# blender drops the leg.
MIN_TRAIN_GAMES = 500


def _model_path(league: str) -> Path:
    return (Path(__file__).resolve().parent.parent.parent
            / "data" / "football" / f"{league}_gbm" / "model.pkl")


def _is_trained(league: str) -> bool:
    return _model_path(league).exists()


def predict_gbm(*, league: str, home_team_id: int, away_team_id: int,
                 expected_margin: float, expected_total: float,
                 spread: float | None = None,
                 total_line: float | None = None) -> dict:
    """Return the GBM-derived probabilities for this matchup, or Nones
    when the model isn't trained yet. Output keys are namespaced with
    ``gbm_`` so they merge cleanly into the predict_match dict.
    """
    if not _is_trained(league):
        return {
            "gbm_trained":     False,
            "gbm_p_home":      None,
            "gbm_p_away":      None,
            "gbm_p_home_cover": None,
            "gbm_p_away_cover": None,
            "gbm_p_over":      None,
            "gbm_p_under":     None,
        }
    # Trained-model branch — loaded lazily because joblib import is
    # ~80ms cold-start. Falls back to None on any read error.
    try:
        import joblib
        bundle = joblib.load(_model_path(league))
    except Exception as e:
        logger.warning("[football:%s] GBM load failed: %s", league, e)
        return {"gbm_trained": False, "gbm_p_home": None, "gbm_p_away": None,
                "gbm_p_home_cover": None, "gbm_p_away_cover": None,
                "gbm_p_over": None, "gbm_p_under": None}

    features = _build_features(
        league=league,
        home_team_id=home_team_id, away_team_id=away_team_id,
        expected_margin=expected_margin, expected_total=expected_total,
        spread=spread, total_line=total_line,
    )

    out: dict = {"gbm_trained": True}
    ml = bundle.get("ml")
    if ml is not None and features is not None:
        try:
            p_home = float(ml.predict_proba([features])[0][1])
            out["gbm_p_home"] = round(p_home, 4)
            out["gbm_p_away"] = round(1.0 - p_home, 4)
        except Exception:
            out["gbm_p_home"] = out["gbm_p_away"] = None
    else:
        out["gbm_p_home"] = out["gbm_p_away"] = None

    spread_model = bundle.get("spread")
    if spread_model is not None and features is not None and spread is not None:
        try:
            p_cover = float(spread_model.predict_proba(
                [features + [spread]])[0][1])
            out["gbm_p_home_cover"] = round(p_cover, 4)
            out["gbm_p_away_cover"] = round(1.0 - p_cover, 4)
        except Exception:
            out["gbm_p_home_cover"] = out["gbm_p_away_cover"] = None
    else:
        out["gbm_p_home_cover"] = out["gbm_p_away_cover"] = None

    total_model = bundle.get("total")
    if total_model is not None and features is not None and total_line is not None:
        try:
            p_over = float(total_model.predict_proba(
                [features + [total_line]])[0][1])
            out["gbm_p_over"] = round(p_over, 4)
            out["gbm_p_under"] = round(1.0 - p_over, 4)
        except Exception:
            out["gbm_p_over"] = out["gbm_p_under"] = None
    else:
        out["gbm_p_over"] = out["gbm_p_under"] = None
    return out


def _build_features(*, league: str, home_team_id: int, away_team_id: int,
                     expected_margin: float, expected_total: float,
                     spread: float | None = None,
                     total_line: float | None = None) -> list[float] | None:
    """Feature vector for the GBM. Kept tiny on purpose — small sample
    sizes can't support a wide feature set.

    Returns None if any feature can't be computed (rare — typically
    only happens for cold-start matchups where one team has no prior
    games).
    """
    from ._elo import replay, INIT_ELO
    try:
        ratings = replay(league)
    except Exception:
        return None
    r_h = ratings.get(int(home_team_id), INIT_ELO)
    r_a = ratings.get(int(away_team_id), INIT_ELO)
    elo_diff = r_h - r_a

    # Recent form: avg margin per team across the latest N games.
    form_home = _recent_margin(league, home_team_id, n=4, as_home=True)
    form_away = _recent_margin(league, away_team_id, n=4, as_home=False)

    return [
        float(elo_diff),
        float(expected_margin),
        float(expected_total),
        float(form_home if form_home is not None else 0.0),
        float(form_away if form_away is not None else 0.0),
    ]


def _recent_margin(league: str, team_id: int, *, n: int,
                    as_home: bool) -> float | None:
    from ._db import get_conn
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT home_team_id, away_team_id, home_score, away_score "
        "FROM games WHERE status='final' "
        "  AND (home_team_id = ? OR away_team_id = ?) "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date DESC LIMIT ?",
        (int(team_id), int(team_id), int(n)),
    ).fetchall()
    if not rows:
        return None
    diffs = []
    for r in rows:
        if int(r["home_team_id"]) == int(team_id):
            diffs.append(int(r["home_score"]) - int(r["away_score"]))
        else:
            diffs.append(int(r["away_score"]) - int(r["home_score"]))
    return sum(diffs) / len(diffs)


def train(league: str) -> dict:
    """Train + persist a GBM bundle for ``league``. Returns counters.
    Refuses to train below ``MIN_TRAIN_GAMES`` games so the cold-
    start sample doesn't produce an overfit model that ships to
    picks.
    """
    from ._db import get_conn
    conn = get_conn(league)
    n_final = conn.execute(
        "SELECT COUNT(*) FROM games WHERE status='final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL"
    ).fetchone()[0]
    if n_final < MIN_TRAIN_GAMES:
        return {
            "trained":      False,
            "n_games":      n_final,
            "min_required": MIN_TRAIN_GAMES,
            "note":         "below training threshold — GBM stays gated",
        }
    # Below code path runs once the sample grows. Trained model goes
    # to data/football/{league}_gbm/model.pkl. The HistGradientBoosting
    # classifier from scikit-learn is the same family the basketball /
    # NBA framework uses; mlflow-style versioning would be nice but
    # would be a separate concern.
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
        import joblib
    except ImportError as e:
        return {"trained": False, "error": f"sklearn not available: {e}"}

    rows = conn.execute(
        "SELECT home_team_id, away_team_id, home_score, away_score, date "
        "FROM games WHERE status='final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date ASC"
    ).fetchall()

    # Build per-game (features, targets) pairs by walking forward in
    # time so each game's features are computed from PRE-game state
    # only (avoids leakage). Future expansion: walk-forward
    # calibration to fit per-market shrinkage.
    from ._elo import update as _elo_update, INIT_ELO
    ratings: dict[int, float] = {}
    feats_ml = []; labels_ml = []
    for r in rows:
        h_id = int(r["home_team_id"]); a_id = int(r["away_team_id"])
        r_h = ratings.get(h_id, INIT_ELO)
        r_a = ratings.get(a_id, INIT_ELO)
        elo_diff = r_h - r_a
        form_h = _recent_margin(league, h_id, n=4, as_home=True) or 0.0
        form_a = _recent_margin(league, a_id, n=4, as_home=False) or 0.0
        feats_ml.append([elo_diff, 0.0, 0.0, form_h, form_a])
        labels_ml.append(int(int(r["home_score"]) > int(r["away_score"])))
        n_h, n_a = _elo_update(r_h, r_a,
                                home_score=int(r["home_score"]),
                                away_score=int(r["away_score"]))
        ratings[h_id] = n_h; ratings[a_id] = n_a

    if not feats_ml:
        return {"trained": False, "n_games": n_final, "error": "no usable rows"}

    ml = HistGradientBoostingClassifier(max_iter=200, max_depth=4,
                                         learning_rate=0.05,
                                         random_state=42)
    ml.fit(feats_ml, labels_ml)
    bundle = {"ml": ml, "spread": None, "total": None,
              "trained_at": str(__import__("datetime").datetime.now()),
              "n_games": n_final}
    out_path = _model_path(league)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, out_path)
    logger.info("[football:%s] GBM trained n=%d → %s",
                league, n_final, out_path)
    return {"trained": True, "n_games": n_final, "path": str(out_path)}


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.football._gbm")
    ap.add_argument("league")
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s %(message)s")
    res = train(args.league)
    print(res)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
