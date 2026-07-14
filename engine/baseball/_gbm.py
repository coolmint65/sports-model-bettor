"""Baseball GBM predictor — Elo + pitcher-rotation features.

Train/inference symmetry: at training time we recompute expected_margin
from the point-in-time Elo state, NOT 0.0 (the v1 bug). The GBM now
learns to weight the Elo signal directly alongside team-rotation
pitcher stats.

Pitcher features: per team, average ERA/K9/BB9 over the team's last 5
starts BEFORE the game date (from ``pitcher_starts`` ingest). ESPN only
ships pitching detail on ~26% of NCAA games so each team also carries
a ``has_pitcher_data`` flag — the GBM learns when to lean on rotation
features vs fall back to Elo + form. Probable-starter feeds don't
exist for college baseball, so we stay at the team-rotation level
rather than the matchup-specific starter level.
"""
from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

MIN_TRAIN_GAMES = 500

# Feature ordering must stay stable — the trained model's column index
# is positional. Adding/reordering invalidates the persisted bundle.
FEATURE_NAMES = [
    "elo_diff",
    "expected_margin",
    "form_h",
    "form_a",
    "home_rot_era",
    "away_rot_era",
    "home_rot_k9",
    "away_rot_k9",
    "home_rot_bb9",
    "away_rot_bb9",
    "home_has_pitch",
    "away_has_pitch",
]

# Sentinel defaults when pitcher data is missing. NCAA league averages
# from the 2026 sample (mean of available rotation aggregates). Picked
# so the GBM's "no data" branch sees a neutral center rather than 0.0.
_LEAGUE_AVG_ERA = 5.5
_LEAGUE_AVG_K9  = 8.5
_LEAGUE_AVG_BB9 = 4.0


def _model_path(league: str) -> Path:
    return (Path(__file__).resolve().parent.parent.parent
            / "data" / "baseball" / f"{league}_gbm" / "model.pkl")


def _is_trained(league: str) -> bool:
    return _model_path(league).exists()


def predict_gbm(*, league: str, home_team_id: int, away_team_id: int,
                 expected_margin: float, expected_total: float,
                 spread: float | None = None,
                 total_line: float | None = None,
                 as_of_date: str | None = None) -> dict:
    if not _is_trained(league):
        return {
            "gbm_trained":      False,
            "gbm_p_home":       None,
            "gbm_p_away":       None,
            "gbm_p_home_cover": None,
            "gbm_p_away_cover": None,
            "gbm_p_over":       None,
            "gbm_p_under":      None,
        }
    try:
        import joblib
        bundle = joblib.load(_model_path(league))
    except Exception as e:
        logger.warning("[baseball:%s] GBM load failed: %s", league, e)
        return {"gbm_trained": False, "gbm_p_home": None, "gbm_p_away": None,
                "gbm_p_home_cover": None, "gbm_p_away_cover": None,
                "gbm_p_over": None, "gbm_p_under": None}

    features = _build_features(
        league=league,
        home_team_id=home_team_id, away_team_id=away_team_id,
        expected_margin=expected_margin,
        as_of_date=as_of_date,
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
    out["gbm_p_home_cover"] = out["gbm_p_away_cover"] = None
    out["gbm_p_over"] = out["gbm_p_under"] = None
    return out


def _build_features(*, league: str, home_team_id: int, away_team_id: int,
                     expected_margin: float,
                     as_of_date: str | None = None) -> list[float] | None:
    from ._elo import replay, INIT_ELO
    try:
        ratings = replay(league)
    except Exception:
        return None
    r_h = ratings.get(int(home_team_id), INIT_ELO)
    r_a = ratings.get(int(away_team_id), INIT_ELO)
    form_h = _recent_margin(league, home_team_id, n=5, as_of_date=as_of_date)
    form_a = _recent_margin(league, away_team_id, n=5, as_of_date=as_of_date)
    h_era, h_k9, h_bb9, h_has = _rotation_stats(league, home_team_id,
                                                  as_of_date=as_of_date)
    a_era, a_k9, a_bb9, a_has = _rotation_stats(league, away_team_id,
                                                  as_of_date=as_of_date)
    return [
        float(r_h - r_a),
        float(expected_margin),
        float(form_h if form_h is not None else 0.0),
        float(form_a if form_a is not None else 0.0),
        float(h_era), float(a_era),
        float(h_k9),  float(a_k9),
        float(h_bb9), float(a_bb9),
        float(h_has), float(a_has),
    ]


def _recent_margin(league: str, team_id: int, *, n: int,
                    as_of_date: str | None = None) -> float | None:
    """Average margin from team's last N games before ``as_of_date``.
    ``as_of_date`` None == open-ended (inference uses live state)."""
    from ._db import get_conn
    conn = get_conn(league)
    if as_of_date:
        rows = conn.execute(
            "SELECT home_team_id, away_team_id, home_score, away_score "
            "FROM games WHERE status='final' "
            "  AND (home_team_id = ? OR away_team_id = ?) "
            "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
            "  AND date < ? "
            "ORDER BY date DESC LIMIT ?",
            (int(team_id), int(team_id), as_of_date, int(n)),
        ).fetchall()
    else:
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


def _rotation_stats(league: str, team_id: int, *,
                     as_of_date: str | None = None, n: int = 5
                     ) -> tuple[float, float, float, int]:
    """Return (era, k9, bb9, has_data) over team's last ``n`` starter
    appearances before ``as_of_date``. ``has_data`` is 1 when we found
    at least one start, 0 otherwise (caller still gets league-avg
    sentinels so the GBM sees a stable feature shape)."""
    from ._db import get_conn
    conn = get_conn(league)
    if as_of_date:
        rows = conn.execute(
            "SELECT ip_outs, earned_runs, strikeouts, walks "
            "FROM pitcher_starts "
            "WHERE team_id = ? AND is_starter = 1 "
            "  AND game_date < ? "
            "  AND ip_outs IS NOT NULL AND ip_outs > 0 "
            "ORDER BY game_date DESC LIMIT ?",
            (int(team_id), as_of_date, int(n)),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT ip_outs, earned_runs, strikeouts, walks "
            "FROM pitcher_starts "
            "WHERE team_id = ? AND is_starter = 1 "
            "  AND ip_outs IS NOT NULL AND ip_outs > 0 "
            "ORDER BY game_date DESC LIMIT ?",
            (int(team_id), int(n)),
        ).fetchall()
    if not rows:
        return _LEAGUE_AVG_ERA, _LEAGUE_AVG_K9, _LEAGUE_AVG_BB9, 0
    total_outs = sum(int(r["ip_outs"] or 0) for r in rows)
    if total_outs <= 0:
        return _LEAGUE_AVG_ERA, _LEAGUE_AVG_K9, _LEAGUE_AVG_BB9, 0
    total_er = sum(int(r["earned_runs"] or 0) for r in rows)
    total_k  = sum(int(r["strikeouts"] or 0) for r in rows)
    total_bb = sum(int(r["walks"] or 0) for r in rows)
    ip = total_outs / 3.0
    era = (total_er * 9.0) / ip if ip > 0 else _LEAGUE_AVG_ERA
    k9  = (total_k  * 9.0) / ip if ip > 0 else _LEAGUE_AVG_K9
    bb9 = (total_bb * 9.0) / ip if ip > 0 else _LEAGUE_AVG_BB9
    return era, k9, bb9, 1


def train(league: str) -> dict:
    from ._db import get_conn
    conn = get_conn(league)
    n_final = conn.execute(
        "SELECT COUNT(*) FROM games WHERE status='final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL"
    ).fetchone()[0]
    if n_final < MIN_TRAIN_GAMES:
        return {"trained": False, "n_games": n_final,
                "min_required": MIN_TRAIN_GAMES,
                "note": "below training threshold"}
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
        from sklearn.metrics import brier_score_loss
        import joblib
    except ImportError as e:
        return {"trained": False, "error": f"sklearn not available: {e}"}

    from ._elo import update as _elo_update, INIT_ELO
    from . import get_league_config
    cfg = get_league_config(league)
    hfa = cfg.get("home_advantage_elo") or 30.0
    elo_per_run = 100.0

    rows = conn.execute(
        "SELECT home_team_id, away_team_id, home_score, away_score, date "
        "FROM games WHERE status='final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date ASC"
    ).fetchall()

    # Walk-forward Elo replay — point-in-time ratings + rotation stats
    # for each game. Both expected_margin and pitcher rotation features
    # are computed from the state BEFORE the game.
    ratings: dict[int, float] = {}
    feats = []
    labels = []
    n_with_pitch = 0
    for r in rows:
        h_id = int(r["home_team_id"]); a_id = int(r["away_team_id"])
        r_h = ratings.get(h_id, INIT_ELO); r_a = ratings.get(a_id, INIT_ELO)
        elo_diff = r_h - r_a
        expected_margin = (elo_diff + hfa) / elo_per_run
        date = r["date"]
        form_h = _recent_margin(league, h_id, n=5, as_of_date=date) or 0.0
        form_a = _recent_margin(league, a_id, n=5, as_of_date=date) or 0.0
        h_era, h_k9, h_bb9, h_has = _rotation_stats(league, h_id,
                                                      as_of_date=date)
        a_era, a_k9, a_bb9, a_has = _rotation_stats(league, a_id,
                                                      as_of_date=date)
        if h_has or a_has:
            n_with_pitch += 1
        feats.append([elo_diff, expected_margin, form_h, form_a,
                       h_era, a_era, h_k9, a_k9, h_bb9, a_bb9,
                       float(h_has), float(a_has)])
        labels.append(int(int(r["home_score"]) > int(r["away_score"])))
        n_h, n_a = _elo_update(r_h, r_a,
                                home_score=int(r["home_score"]),
                                away_score=int(r["away_score"]))
        ratings[h_id] = n_h; ratings[a_id] = n_a

    if not feats:
        return {"trained": False, "n_games": n_final, "error": "no rows"}

    # Chronological 85/15 split — final stretch is validation so we
    # measure on data the model never trained on.
    n = len(feats)
    split = int(n * 0.85)
    X_train, X_val = feats[:split], feats[split:]
    y_train, y_val = labels[:split], labels[split:]

    ml = HistGradientBoostingClassifier(max_iter=300, max_depth=4,
                                         learning_rate=0.05,
                                         random_state=42)
    ml.fit(X_train, y_train)

    val_pred = ml.predict_proba(X_val)[:, 1]
    val_brier = float(brier_score_loss(y_val, val_pred))
    val_acc = float(sum(1 for p, y in zip(val_pred, y_val)
                         if (p >= 0.5) == bool(y)) / len(y_val))

    bundle = {
        "ml": ml,
        "feature_names": FEATURE_NAMES,
        "trained_at": str(__import__("datetime").datetime.now()),
        "n_games": n_final,
        "n_train": len(X_train),
        "n_val": len(X_val),
        "n_with_pitch": n_with_pitch,
        "brier": round(val_brier, 4),
        "accuracy": round(val_acc, 4),
    }
    out_path = _model_path(league)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, out_path)
    logger.info("[baseball:%s] GBM trained n=%d (pitch=%d) brier=%.4f acc=%.4f → %s",
                league, n_final, n_with_pitch, val_brier, val_acc, out_path)
    return {"trained": True, "n_games": n_final,
            "n_with_pitch": n_with_pitch,
            "brier": round(val_brier, 4),
            "accuracy": round(val_acc, 4),
            "path": str(out_path)}


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.baseball._gbm")
    ap.add_argument("league")
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s %(message)s")
    res = train(args.league)
    print(res)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
