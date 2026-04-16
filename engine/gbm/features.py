"""
Feature extraction for the GBM training pipeline.

Given a completed game, extract a feature vector that reflects WHAT THE
MODEL WOULD HAVE KNOWN BEFORE THE GAME STARTED. Point-in-time
discipline is critical -- using season-end stats as features for a
mid-season game is textbook data leakage.

The feature set mirrors what the factor model already uses, so the
GBM trains on the same inputs but learns non-linear interactions the
multiplicative factor stack can't express. Additional raw signals
(exact pitcher ERA/FIP, team wRC+, park run factor, etc.) are passed
in because GBMs can weight them independently.

Returns a dict mapping feature name -> value, which callers turn into
a pandas DataFrame.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)


# Feature-name -> default value when the underlying data is missing.
# GBMs handle NaN natively but stable defaults keep training stable
# across sparse-feature regions.
_DEFAULTS = {
    "home_runs_pg": 4.5,
    "away_runs_pg": 4.5,
    "home_runs_allowed_pg": 4.5,
    "away_runs_allowed_pg": 4.5,
    "home_wrc_plus": 100.0,
    "away_wrc_plus": 100.0,
    "home_ops": 0.720,
    "away_ops": 0.720,
    "home_sp_era": 4.10,
    "away_sp_era": 4.10,
    "home_sp_fip": 4.10,
    "away_sp_fip": 4.10,
    "home_sp_k_pct": 0.225,
    "away_sp_k_pct": 0.225,
    "home_sp_bb_pct": 0.085,
    "away_sp_bb_pct": 0.085,
    "home_sp_whip": 1.30,
    "away_sp_whip": 1.30,
    "home_sp_games_started": 0,
    "away_sp_games_started": 0,
    "home_bullpen_era": 4.20,
    "away_bullpen_era": 4.20,
    "park_run_factor": 1.0,
    "days_into_season": 30,
    "is_playoff": 0,
    "home_form_last_10_pct": 0.500,
    "away_form_last_10_pct": 0.500,
}


def extract_mlb_features(conn, game: dict) -> dict[str, float] | None:
    """Build the feature dict for one completed MLB game.

    Uses point-in-time stats computed AS OF game.date so the feature
    values are what the model would have seen at prediction time.
    Returns None when critical inputs (team IDs, date) are missing so
    the trainer can skip the row cleanly.
    """
    date = game.get("date")
    home_id = game.get("home_team_id")
    away_id = game.get("away_team_id")
    if not (date and home_id and away_id):
        return None

    features: dict[str, float] = dict(_DEFAULTS)

    # Point-in-time team stats
    try:
        from engine.pit_stats import compute_team_stats_at_date
        home_stats = compute_team_stats_at_date(home_id, date, _season_of(date)) or {}
        away_stats = compute_team_stats_at_date(away_id, date, _season_of(date)) or {}
        features["home_runs_pg"] = _num(home_stats.get("runs_pg"), 4.5)
        features["away_runs_pg"] = _num(away_stats.get("runs_pg"), 4.5)
        features["home_runs_allowed_pg"] = _num(home_stats.get("runs_allowed_pg"), 4.5)
        features["away_runs_allowed_pg"] = _num(away_stats.get("runs_allowed_pg"), 4.5)
    except Exception as e:
        logger.debug("team PIT stats failed for game %s: %s", game.get("mlb_game_id"), e)

    # Pitcher PIT stats
    try:
        from engine.pit_stats import compute_pitcher_stats_at_date
        season = _season_of(date)
        if game.get("home_pitcher_id"):
            sp = compute_pitcher_stats_at_date(game["home_pitcher_id"], date, season) or {}
            features["home_sp_era"] = _num(sp.get("era"), 4.10)
            features["home_sp_fip"] = _num(sp.get("fip"), 4.10)
            features["home_sp_k_pct"] = _num(sp.get("k_pct"), 0.225)
            features["home_sp_bb_pct"] = _num(sp.get("bb_pct"), 0.085)
            features["home_sp_whip"] = _num(sp.get("whip"), 1.30)
            features["home_sp_games_started"] = _num(sp.get("games_started"), 0)
        if game.get("away_pitcher_id"):
            sp = compute_pitcher_stats_at_date(game["away_pitcher_id"], date, season) or {}
            features["away_sp_era"] = _num(sp.get("era"), 4.10)
            features["away_sp_fip"] = _num(sp.get("fip"), 4.10)
            features["away_sp_k_pct"] = _num(sp.get("k_pct"), 0.225)
            features["away_sp_bb_pct"] = _num(sp.get("bb_pct"), 0.085)
            features["away_sp_whip"] = _num(sp.get("whip"), 1.30)
            features["away_sp_games_started"] = _num(sp.get("games_started"), 0)
    except Exception as e:
        logger.debug("pitcher PIT stats failed for game %s: %s", game.get("mlb_game_id"), e)

    # Park factor (venue-based)
    try:
        from engine.db import get_park_factor
        park = get_park_factor(game.get("venue") or "") or {}
        features["park_run_factor"] = _num(park.get("run_factor"), 1.0)
    except Exception:
        pass

    # Season-phase features
    features["days_into_season"] = _days_into_season(date)
    features["is_playoff"] = 1 if _is_postseason(date) else 0

    # Recent form (last 10 team games, point-in-time)
    try:
        from engine.db import get_recent_games
        features["home_form_last_10_pct"] = _form_pct(
            get_recent_games(home_id, date, n=10) or []
        )
        features["away_form_last_10_pct"] = _form_pct(
            get_recent_games(away_id, date, n=10) or []
        )
    except Exception:
        pass

    return features


def extract_target(game: dict) -> dict[str, Any]:
    """Extract the outcome targets we want GBM to predict.

    home_win is the primary target; total_runs is the O/U target;
    home_margin is useful for RL-style regression; nrfi_hit and
    f5_home_win target the inning-specific markets when linescore data
    is available.
    """
    import json
    hs = game.get("home_score")
    as_ = game.get("away_score")
    if hs is None or as_ is None:
        return {}
    out: dict[str, Any] = {
        "home_win": int(hs > as_),
        "total_runs": hs + as_,
        "home_margin": hs - as_,
    }
    try:
        h_ls = json.loads(game.get("home_linescore") or "[]")
        a_ls = json.loads(game.get("away_linescore") or "[]")
        if h_ls and a_ls:
            out["nrfi_hit"] = int(h_ls[0] == 0 and a_ls[0] == 0)
            if len(h_ls) >= 5 and len(a_ls) >= 5:
                f5h = sum(h_ls[:5])
                f5a = sum(a_ls[:5])
                if f5h != f5a:
                    out["f5_home_win"] = int(f5h > f5a)
                    out["f5_total"] = f5h + f5a
    except Exception:
        pass
    return out


# ── Helpers ────────────────────────────────────────────────────

def _num(v, default: float) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    if x != x:
        return default
    return x


def _season_of(date_str: str) -> int:
    try:
        return int(str(date_str)[:4])
    except Exception:
        return datetime.now().year


def _days_into_season(date_str: str) -> int:
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
    except Exception:
        return 0
    opener = datetime(d.year, 3, 26)
    return max(0, (d - opener).days)


def _is_postseason(date_str: str) -> bool:
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
    except Exception:
        return False
    if d.month == 10:
        return True
    if d.month == 11 and d.day <= 7:
        return True
    return False


def _form_pct(recent_games: list) -> float:
    if not recent_games:
        return 0.500
    wins = sum(1 for g in recent_games if g.get("won"))
    return wins / len(recent_games)


FEATURE_NAMES = sorted(_DEFAULTS.keys())
