"""
GBM inference -- load latest trained model and produce predictions.

Called from /api/predict to produce the GBM signal alongside MC + the
factor model. Output is a dict of predicted probabilities / expected
values keyed by target (home_win, nrfi_hit, f5_home_win, total_runs,
f5_total).

Caches loaded models in-process so the per-request cost is a feature
lookup + tree traversal (< 1 ms).
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any
from .._tz import et_today_str

logger = logging.getLogger(__name__)

_MODELS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "models"

# (sport, target) -> (model, meta); populated lazily and refreshed when
# the "latest" artifact changes mtime.
_CACHE: dict[tuple[str, str], dict] = {}
_LOCK = threading.Lock()


def _load_latest(sport: str, target: str):
    """Load the latest GBM artifact for a (sport, target). Returns dict
    with keys {model, meta, mtime} or None when the artifact is missing."""
    art = _MODELS_DIR / f"{sport}_gbm_{target}_latest.json"
    meta_path = _MODELS_DIR / f"{sport}_gbm_{target}_latest.meta.json"
    if not art.exists():
        return None
    mtime = art.stat().st_mtime

    with _LOCK:
        cached = _CACHE.get((sport, target))
        if cached and cached["mtime"] == mtime:
            return cached
        try:
            import xgboost as xgb
        except ImportError:
            logger.debug("xgboost not installed -- GBM disabled")
            return None
        try:
            meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
            task = meta.get("task", "classification")
            if task == "classification":
                model = xgb.XGBClassifier()
            else:
                model = xgb.XGBRegressor()
            model.load_model(str(art))
            entry = {"model": model, "meta": meta, "mtime": mtime}
            _CACHE[(sport, target)] = entry
            return entry
        except Exception as e:
            logger.warning("Failed to load GBM %s/%s: %s", sport, target, e)
            return None


def predict_mlb(conn, game_payload: dict) -> dict[str, Any]:
    """Run all MLB GBM targets on one matchup.

    game_payload is the same dict shape we hand to extract_mlb_features
    (home_team_id, away_team_id, home_pitcher_id, away_pitcher_id, date,
    venue). Returns a dict keyed by target name with either a float
    probability (classification) or a float expected value (regression).
    """
    try:
        import pandas as pd
    except ImportError:
        return {"error": "pandas not installed"}

    from .features import extract_mlb_features, FEATURE_NAMES
    features = extract_mlb_features(conn, game_payload)
    if not features:
        return {"error": "feature extraction failed"}

    X = pd.DataFrame([{k: features[k] for k in FEATURE_NAMES}])

    out: dict[str, Any] = {}
    for target_name, _ in _MLB_TARGETS:
        entry = _load_latest("mlb", target_name)
        if entry is None:
            continue
        model = entry["model"]
        meta = entry["meta"]
        try:
            if meta.get("task") == "classification":
                out[target_name] = round(float(model.predict_proba(X)[0, 1]), 4)
            else:
                out[target_name] = round(float(model.predict(X)[0]), 3)
        except Exception as e:
            out[target_name] = {"error": str(e)}

    if out:
        out["model_trained_at"] = max(
            (e.get("trained_at", "") for e in [_load_latest("mlb", t[0])["meta"]
                                               for t in _MLB_TARGETS
                                               if _load_latest("mlb", t[0])]),
            default="",
        )
    return out


# Keep in sync with engine.gbm.train.SPORT_TARGETS["mlb"]
_MLB_TARGETS = [
    ("home_win",     "classification"),
    ("nrfi_hit",     "classification"),
    ("f5_home_win",  "classification"),
    ("total_runs",   "regression"),
    ("f5_total",     "regression"),
]

_NHL_TARGETS = [
    ("home_win",        "classification"),
    ("total_goals",     "regression"),
    ("p1_home_win",     "classification"),
    ("p1_total_goals",  "regression"),
]

_NBA_TARGETS = [
    ("home_win",         "classification"),
    ("total_points",     "regression"),
    ("margin",           "regression"),
    ("q1_home_win",      "classification"),
    ("q1_total_points",  "regression"),
    ("q1_margin",        "regression"),
]


def predict_nhl(conn, game_payload: dict) -> dict[str, Any]:
    """Run all NHL GBM targets on one matchup. Mirrors predict_mlb.

    game_payload keys: home_team_id, away_team_id, date, season, game_type.
    The conn must be the NHL DB connection (engine.nhl_db.get_conn()).
    """
    try:
        import pandas as pd
    except ImportError:
        return {"error": "pandas not installed"}

    from .features_nhl import extract_nhl_features, NHL_FEATURE_NAMES
    features = extract_nhl_features(conn, game_payload)
    if not features:
        return {"error": "feature extraction failed"}

    X = pd.DataFrame([{k: features[k] for k in NHL_FEATURE_NAMES}])

    out: dict[str, Any] = {}
    for target_name, _ in _NHL_TARGETS:
        entry = _load_latest("nhl", target_name)
        if entry is None:
            continue
        model = entry["model"]
        meta = entry["meta"]
        try:
            if meta.get("task") == "classification":
                out[target_name] = round(float(model.predict_proba(X)[0, 1]), 4)
            else:
                out[target_name] = round(float(model.predict(X)[0]), 3)
        except Exception as e:
            out[target_name] = {"error": str(e)}

    return out


def predict_nba(conn, game_payload: dict) -> dict[str, Any]:
    """Run all NBA GBM targets on one matchup. Mirrors predict_mlb.

    game_payload keys: home_team_id, away_team_id, date, season.
    The conn must be the NBA DB connection (engine.nba_db.get_conn()).
    """
    try:
        import pandas as pd
    except ImportError:
        return {"error": "pandas not installed"}

    from .features_nba import extract_nba_features, NBA_FEATURE_NAMES
    features = extract_nba_features(conn, game_payload)
    if not features:
        return {"error": "feature extraction failed"}

    X = pd.DataFrame([{k: features[k] for k in NBA_FEATURE_NAMES}])

    out: dict[str, Any] = {}
    for target_name, _ in _NBA_TARGETS:
        entry = _load_latest("nba", target_name)
        if entry is None:
            continue
        model = entry["model"]
        meta = entry["meta"]
        try:
            if meta.get("task") == "classification":
                out[target_name] = round(float(model.predict_proba(X)[0, 1]), 4)
            else:
                out[target_name] = round(float(model.predict(X)[0]), 3)
        except Exception as e:
            out[target_name] = {"error": str(e)}

    return out


_LIVE_TARGETS = [
    ("final_total_points", "regression"),
    ("final_margin",       "regression"),
    ("home_final_win",     "classification"),
]


def predict_live(sport: str, state: dict) -> dict[str, Any]:
    """Phase 5e — predict (final_total, final_margin, home_final_win)
    from a current-state dict.

    ``state`` must include the same keys ``features_live`` builds:
    period_ended, home_score_so_far, away_score_so_far,
    score_total_so_far, score_margin_so_far, plus the per-sport
    pace/shot/foul running totals.

    Sport key is 'nba_live' or 'nhl_live' for model loading.
    """
    try:
        import pandas as pd
    except ImportError:
        return {"error": "pandas not installed"}
    sport = (sport or "").lower()
    if sport not in ("nba", "nhl", "wnba", "ncaam", "afl"):
        return {"error": f"unsupported sport: {sport!r}"}
    sport_key = f"{sport}_live"
    from .features_live import (
        NBA_LIVE_FEATURE_NAMES, NHL_LIVE_FEATURE_NAMES,
        _LIVE_DEFAULTS_NBA, _LIVE_DEFAULTS_NHL,
        _nba_state_to_features, _nhl_state_to_features,
    )
    # WNBA + NCAAM + AFL share NBA's feature shape — same ESPN PBP,
    # same period-end signal. Game length differs per sport so the
    # converter reads ``state["sport"]`` to pick the right period
    # minutes (10/12/20 vs 4-quarter / 2-half totals).
    if sport in ("nba", "wnba", "ncaam", "afl"):
        feature_names = NBA_LIVE_FEATURE_NAMES
        converter = _nba_state_to_features
    else:
        feature_names = NHL_LIVE_FEATURE_NAMES
        converter = _nhl_state_to_features
    # Stamp the sport into state so the converter's timing math
    # matches the training distribution.
    if "sport" not in state:
        state = {**state, "sport": sport}
    features = converter(state, prematch=None)
    X = pd.DataFrame([{k: features[k] for k in feature_names}])
    out: dict[str, Any] = {}
    for target_name, _ in _LIVE_TARGETS:
        entry = _load_latest(sport_key, target_name)
        if entry is None:
            continue
        model = entry["model"]
        meta = entry["meta"]
        try:
            if meta.get("task") == "classification":
                out[target_name] = round(float(model.predict_proba(X)[0, 1]), 4)
            else:
                out[target_name] = round(float(model.predict(X)[0]), 3)
        except Exception as e:
            out[target_name] = {"error": str(e)}
    return out


_TENNIS_TARGETS = [
    ("p1_win",         "classification"),
    ("total_games",    "regression"),
    ("straight_sets",  "classification"),
]


def predict_tennis(tour: str, p1_id: int, p2_id: int, *,
                    surface: str = "Hard", best_of: int = 3,
                    date: str | None = None) -> dict[str, Any]:
    """Run all Tennis GBM targets on one matchup. tour='atp'|'wta'.

    Builds the feature row from current-state Elo + cached
    PlayerHistory rolled forward through every prior match in the DB.
    For inference we accept the small staleness — features include
    rolling stats up to the most recent match + current Elo.
    """
    try:
        import pandas as pd
    except ImportError:
        return {"error": "pandas not installed"}

    if tour not in ("atp", "wta"):
        return {"error": f"unknown tour: {tour!r}"}

    sport_key = f"tennis_{tour}"
    from .features_tennis import (
        FEATURE_NAMES, PlayerHistory, extract_match_features,
    )
    from engine.tennis_db import get_conn
    from engine.tennis_elo import rating_for as _elo_for

    # Build a synthetic match row to feed extract_match_features.
    # Walk-forward history is built lazily here from the most recent
    # 20 matches per player (much cheaper than rebuilding the full
    # 25-year buffer at inference time).
    conn = get_conn()
    history = PlayerHistory()
    for pid in (p1_id, p2_id):
        rows = conn.execute(
            "SELECT * FROM tennis_matches "
            "WHERE tour = ? AND (winner_id = ? OR loser_id = ?) "
            "  AND tourney_date IS NOT NULL "
            "ORDER BY tourney_date DESC, match_id DESC LIMIT 25",
            (tour, int(pid), int(pid)),
        ).fetchall()
        # Replay these in chronological order to populate the buffer.
        for r in reversed(rows):
            d = dict(r)
            won = (int(d["winner_id"]) == int(pid))
            surf = d.get("surface") or "Hard"
            history.record(
                tour, int(pid), surf, d["tourney_date"],
                won=won,
                svpt=(d.get("w_svpt") if won else d.get("l_svpt")),
                first_in=(d.get("w_1stIn") if won else d.get("l_1stIn")),
                first_won=(d.get("w_1stWon") if won else d.get("l_1stWon")),
                second_won=(d.get("w_2ndWon") if won else d.get("l_2ndWon")),
                bp_saved=(d.get("w_bpSaved") if won else d.get("l_bpSaved")),
                bp_faced=(d.get("w_bpFaced") if won else d.get("l_bpFaced")),
            )

    today = date or et_today_str()
    fake = {
        "tour": tour,
        "match_id": f"PREDICT-{p1_id}-{p2_id}-{today}",
        "tourney_date": today,
        "tourney_level": "M",  # neutral default; tier doesn't move
                                 # the GBM features we computed
        "surface": surface,
        "best_of": best_of,
        "winner_id": p1_id, "loser_id": p2_id,  # _assign_p1 will dice
        "winner_hand": None, "loser_hand": None,
        "winner_age": None, "loser_age": None,
    }

    def _elo_lookup(t, pid, surf):
        return _elo_for(t, pid, surface=surf, fallback_to_all=False)

    features = extract_match_features(history, fake, _elo_lookup)
    if not features:
        return {"error": "feature extraction failed"}

    # extract_match_features randomises p1/p2 by match_id hash. To get
    # a P(actual_p1_wins) we need to know which side ended up as
    # in-row p1. The match_id we synthesised hashes deterministically
    # so we can reconstruct.
    p1_was_winner = (features.get("p1_win") if False else None)
    # Hash check matching _assign_p1 logic
    from .features_tennis import _assign_p1
    feat_p1, _feat_p2 = _assign_p1(fake)
    p1_is_actual = (feat_p1 == p1_id)

    X = pd.DataFrame([{k: features[k] for k in FEATURE_NAMES}])
    out: dict[str, Any] = {}
    for target_name, _ in _TENNIS_TARGETS:
        entry = _load_latest(sport_key, target_name)
        if entry is None:
            continue
        model = entry["model"]
        meta = entry["meta"]
        try:
            if meta.get("task") == "classification":
                p_class1 = float(model.predict_proba(X)[0, 1])
                # Flip when row p1 isn't actual p1_id (so output is
                # always in actual-p1 frame).
                if target_name == "p1_win":
                    out[target_name] = round(
                        p_class1 if p1_is_actual else 1.0 - p_class1, 4)
                else:
                    out[target_name] = round(p_class1, 4)
            else:
                out[target_name] = round(float(model.predict(X)[0]), 3)
        except Exception as e:
            out[target_name] = {"error": str(e)}
    return out


def is_available(sport: str = "mlb") -> bool:
    """True if at least one latest-artifact exists for this sport."""
    targets = {
        "mlb": _MLB_TARGETS, "nhl": _NHL_TARGETS, "nba": _NBA_TARGETS,
        "tennis_atp": _TENNIS_TARGETS, "tennis_wta": _TENNIS_TARGETS,
        "nba_live": _LIVE_TARGETS, "nhl_live": _LIVE_TARGETS,
        "wnba_live": _LIVE_TARGETS, "ncaam_live": _LIVE_TARGETS,
        "afl_live": _LIVE_TARGETS,
    }.get(sport, [])
    return any(
        (_MODELS_DIR / f"{sport}_gbm_{t[0]}_latest.json").exists()
        for t in targets
    )
