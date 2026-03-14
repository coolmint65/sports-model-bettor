"""
Feature flattening for ML model consumption.

Converts the nested feature dictionary from FeatureEngine.build_game_features()
into a flat numeric vector suitable for scikit-learn models. Handles missing
values gracefully (HistGradientBoosting accepts NaN natively).
"""

import math
from typing import Any, Dict, List, Optional

import numpy as np


# Ordered list of feature names produced by flatten_features().
# Models trained on one version of this list can validate compatibility.
FEATURE_NAMES: List[str] = []

# Populated at module load time by _build_feature_names().
_INITIALIZED = False


def _safe_float(val: Any, default: float = float("nan")) -> float:
    """Convert a value to float, returning NaN for None/missing."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _extract_form(features: Dict[str, Any], prefix: str, window: str) -> Dict[str, float]:
    """Extract form features for a team/window combo."""
    key = f"{prefix}_form_{window}"
    form = features.get(key, {})
    tag = f"{prefix}_form{window}"
    return {
        f"{tag}_win_rate": _safe_float(form.get("win_rate")),
        f"{tag}_avg_gf": _safe_float(form.get("avg_goals_for")),
        f"{tag}_avg_ga": _safe_float(form.get("avg_goals_against")),
        f"{tag}_avg_total": _safe_float(form.get("avg_total_goals")),
        f"{tag}_pdo": _safe_float(form.get("pdo")),
        f"{tag}_shooting_pct": _safe_float(form.get("shooting_pct")),
        f"{tag}_save_pct": _safe_float(form.get("save_pct")),
        f"{tag}_momentum_gf": _safe_float(form.get("momentum_avg_gf")),
        f"{tag}_games": _safe_float(form.get("games_found", 0)),
    }


def _extract_season(features: Dict[str, Any], prefix: str) -> Dict[str, float]:
    """Extract season stats for a team."""
    key = f"{prefix}_season"
    season = features.get(key, {})
    tag = f"{prefix}_season"
    return {
        f"{tag}_gf_pg": _safe_float(season.get("goals_for_pg")),
        f"{tag}_ga_pg": _safe_float(season.get("goals_against_pg")),
        f"{tag}_pp_pct": _safe_float(season.get("pp_pct")),
        f"{tag}_pk_pct": _safe_float(season.get("pk_pct")),
        f"{tag}_shots_for_pg": _safe_float(season.get("shots_for_pg")),
        f"{tag}_shots_against_pg": _safe_float(season.get("shots_against_pg")),
        f"{tag}_faceoff_pct": _safe_float(season.get("faceoff_pct")),
        f"{tag}_win_pct": _safe_float(season.get("win_pct")),
    }


def _extract_splits(features: Dict[str, Any], prefix: str) -> Dict[str, float]:
    """Extract home/away split features."""
    key = f"{prefix}_splits"
    splits = features.get(key, {})
    tag = f"{prefix}_splits"
    return {
        f"{tag}_win_rate": _safe_float(splits.get("win_rate")),
        f"{tag}_avg_gf": _safe_float(splits.get("avg_goals_for")),
        f"{tag}_avg_ga": _safe_float(splits.get("avg_goals_against")),
        f"{tag}_games": _safe_float(splits.get("games_found", 0)),
    }


def _extract_goalie(features: Dict[str, Any], prefix: str) -> Dict[str, float]:
    """Extract goalie features."""
    key = f"{prefix}_goalie"
    goalie = features.get(key, {})
    tag = f"{prefix}_goalie"
    return {
        f"{tag}_season_sv_pct": _safe_float(goalie.get("season_save_pct")),
        f"{tag}_season_gaa": _safe_float(goalie.get("season_gaa")),
        f"{tag}_last5_sv_pct": _safe_float(goalie.get("last5_save_pct")),
        f"{tag}_last5_gaa": _safe_float(goalie.get("last5_gaa")),
        f"{tag}_last10_sv_pct": _safe_float(goalie.get("last10_save_pct")),
        f"{tag}_last10_gaa": _safe_float(goalie.get("last10_gaa")),
        f"{tag}_games_started": _safe_float(goalie.get("games_started_season", 0)),
        f"{tag}_consecutive_starts": _safe_float(goalie.get("consecutive_starts", 0)),
    }


def _extract_skaters(features: Dict[str, Any], prefix: str) -> Dict[str, float]:
    """Extract skater talent features."""
    key = f"{prefix}_skaters"
    skaters = features.get(key, {})
    tag = f"{prefix}_skaters"
    return {
        f"{tag}_top6_fwd_ppg": _safe_float(skaters.get("top6_fwd_ppg")),
        f"{tag}_top4_def_ppg": _safe_float(skaters.get("top4_def_ppg")),
        f"{tag}_star_ppg": _safe_float(skaters.get("star_ppg")),
        f"{tag}_team_ppg": _safe_float(skaters.get("team_skater_ppg")),
    }


def _extract_lineup(features: Dict[str, Any], prefix: str) -> Dict[str, float]:
    """Extract lineup status features."""
    key = f"{prefix}_lineup"
    lineup = features.get(key, {})
    tag = f"{prefix}_lineup"
    return {
        f"{tag}_missing_count": _safe_float(lineup.get("missing_count", 0)),
        f"{tag}_missing_ppg": _safe_float(lineup.get("missing_points_per_game", 0)),
        f"{tag}_strength": _safe_float(lineup.get("lineup_strength", 1.0)),
    }


def _extract_injuries(features: Dict[str, Any], prefix: str) -> Dict[str, float]:
    """Extract injury impact features."""
    key = f"{prefix}_injuries"
    injuries = features.get(key, {})
    tag = f"{prefix}_injuries"
    return {
        f"{tag}_xg_reduction": _safe_float(injuries.get("xg_reduction", 0)),
        f"{tag}_missing_ppg": _safe_float(injuries.get("total_missing_ppg", 0)),
        f"{tag}_injured_count": _safe_float(injuries.get("injured_count", 0)),
        f"{tag}_goalie_injured": 1.0 if injuries.get("goalie_injured") else 0.0,
    }


def _extract_schedule(features: Dict[str, Any], prefix: str) -> Dict[str, float]:
    """Extract schedule context features."""
    key = f"{prefix}_schedule"
    sched = features.get(key, {})
    tag = f"{prefix}_schedule"
    return {
        f"{tag}_b2b": 1.0 if sched.get("is_back_to_back") else 0.0,
        f"{tag}_rest_days": _safe_float(sched.get("days_rest", 1)),
        f"{tag}_games_last_7": _safe_float(sched.get("games_last_7", 0)),
        f"{tag}_road_games": _safe_float(sched.get("consecutive_road_games", 0)),
        f"{tag}_lookahead": 1.0 if sched.get("is_lookahead") else 0.0,
        f"{tag}_letdown": 1.0 if sched.get("is_letdown") else 0.0,
        f"{tag}_travel_disadvantage": 1.0 if sched.get("is_travel_disadvantage") else 0.0,
    }


def _extract_special_teams(features: Dict[str, Any], prefix: str) -> Dict[str, float]:
    """Extract special teams features."""
    key = f"{prefix}_special_teams"
    st = features.get(key, {})
    tag = f"{prefix}_special"
    return {
        f"{tag}_pp_pct": _safe_float(st.get("pp_pct")),
        f"{tag}_pk_pct": _safe_float(st.get("pk_pct")),
    }


def _extract_periods(features: Dict[str, Any], prefix: str) -> Dict[str, float]:
    """Extract period scoring features."""
    key = f"{prefix}_periods"
    periods = features.get(key, {})
    tag = f"{prefix}_periods"
    return {
        f"{tag}_p1_for": _safe_float(periods.get("avg_p1_for")),
        f"{tag}_p2_for": _safe_float(periods.get("avg_p2_for")),
        f"{tag}_p3_for": _safe_float(periods.get("avg_p3_for")),
        f"{tag}_p1_against": _safe_float(periods.get("avg_p1_against")),
        f"{tag}_p2_against": _safe_float(periods.get("avg_p2_against")),
        f"{tag}_p3_against": _safe_float(periods.get("avg_p3_against")),
    }


def _extract_ot(features: Dict[str, Any], prefix: str) -> Dict[str, float]:
    """Extract overtime tendency features."""
    key = f"{prefix}_ot"
    ot = features.get(key, {})
    tag = f"{prefix}_ot"
    return {
        f"{tag}_pct": _safe_float(ot.get("ot_pct")),
        f"{tag}_win_rate": _safe_float(ot.get("ot_win_rate")),
    }


def _extract_advanced(features: Dict[str, Any], prefix: str) -> Dict[str, float]:
    """Extract advanced NHL metrics (Corsi-proxy, shot quality, PDO)."""
    key = f"{prefix}_advanced"
    adv = features.get(key, {})
    tag = f"{prefix}_advanced"
    return {
        f"{tag}_cf_pct": _safe_float(adv.get("corsi_for_pct")),
        f"{tag}_cf_per60": _safe_float(adv.get("corsi_for_per60")),
        f"{tag}_ca_per60": _safe_float(adv.get("corsi_against_per60")),
        f"{tag}_shot_share": _safe_float(adv.get("shot_share")),
        f"{tag}_shooting_pct": _safe_float(adv.get("shooting_pct")),
        f"{tag}_save_pct": _safe_float(adv.get("team_save_pct")),
        f"{tag}_pdo": _safe_float(adv.get("pdo")),
        f"{tag}_hd_proxy": _safe_float(adv.get("high_danger_proxy")),
        f"{tag}_xgf_share": _safe_float(adv.get("xgf_share")),
        f"{tag}_blocks_for": _safe_float(adv.get("avg_blocks_for")),
        f"{tag}_blocks_against": _safe_float(adv.get("avg_blocks_against")),
        f"{tag}_games": _safe_float(adv.get("games_found", 0)),
    }


def flatten_features(features: Dict[str, Any]) -> Dict[str, float]:
    """
    Convert a nested feature dict from build_game_features() into a flat
    numeric dict suitable for ML model input.

    Returns a dict mapping feature names to float values. Missing data
    is represented as float('nan') which HistGradientBoosting handles natively.
    """
    flat: Dict[str, float] = {}

    for prefix in ("home", "away"):
        # Recent form (5-game and 10-game windows)
        flat.update(_extract_form(features, prefix, "5"))
        flat.update(_extract_form(features, prefix, "10"))

        # Season stats
        flat.update(_extract_season(features, prefix))

        # Home/away splits
        flat.update(_extract_splits(features, prefix))

        # Goalie
        flat.update(_extract_goalie(features, prefix))

        # Skater talent
        flat.update(_extract_skaters(features, prefix))

        # Lineup status
        flat.update(_extract_lineup(features, prefix))

        # Injuries
        flat.update(_extract_injuries(features, prefix))

        # Schedule
        flat.update(_extract_schedule(features, prefix))

        # Special teams
        flat.update(_extract_special_teams(features, prefix))

        # Period scoring
        flat.update(_extract_periods(features, prefix))

        # OT tendency
        flat.update(_extract_ot(features, prefix))

        # Advanced metrics (Corsi-proxy, shot quality)
        flat.update(_extract_advanced(features, prefix))

    # --- Head-to-head ---
    h2h = features.get("h2h", {})
    flat["h2h_home_win_rate"] = _safe_float(h2h.get("team1_win_rate"))
    flat["h2h_away_win_rate"] = _safe_float(h2h.get("team2_win_rate"))
    flat["h2h_avg_total"] = _safe_float(h2h.get("avg_total_goals"))
    flat["h2h_home_avg_goals"] = _safe_float(h2h.get("team1_avg_goals"))
    flat["h2h_away_avg_goals"] = _safe_float(h2h.get("team2_avg_goals"))
    flat["h2h_games"] = _safe_float(h2h.get("games_found", 0))

    # --- Team matchup ---
    tm = features.get("team_matchup", {})
    flat["matchup_avg_total"] = _safe_float(tm.get("avg_total_goals"))
    flat["matchup_games"] = _safe_float(tm.get("games_found", 0))

    # --- Player matchups ---
    home_pm = features.get("home_player_matchup", {})
    away_pm = features.get("away_player_matchup", {})
    flat["home_player_matchup_boost"] = _safe_float(home_pm.get("matchup_boost", 0))
    flat["away_player_matchup_boost"] = _safe_float(away_pm.get("matchup_boost", 0))

    # --- Penalty discipline ---
    for prefix in ("home", "away"):
        disc = features.get(f"{prefix}_discipline", {})
        flat[f"{prefix}_discipline_avg_pim"] = _safe_float(disc.get("avg_pim_per_game"))
        flat[f"{prefix}_discipline_rating"] = _safe_float(disc.get("discipline_rating"))

    # --- Close-game record ---
    for prefix in ("home", "away"):
        cr = features.get(f"{prefix}_close_record", {})
        flat[f"{prefix}_close_game_wr"] = _safe_float(cr.get("close_game_win_rate"))
        flat[f"{prefix}_scoring_first_rate"] = _safe_float(cr.get("scoring_first_rate"))

    # --- Game context booleans ---
    flat["is_divisional"] = 1.0 if features.get("is_divisional") else 0.0
    flat["is_cross_conference"] = 1.0 if features.get("is_cross_conference") else 0.0

    # --- Derived differential features ---
    # These help the model learn relative strength without needing to compute
    # feature interactions itself.
    flat["diff_form5_gf"] = flat.get("home_form5_avg_gf", 0) - flat.get("away_form5_avg_gf", 0)
    flat["diff_form5_ga"] = flat.get("home_form5_avg_ga", 0) - flat.get("away_form5_avg_ga", 0)
    flat["diff_form5_win_rate"] = flat.get("home_form5_win_rate", 0) - flat.get("away_form5_win_rate", 0)
    flat["diff_season_gf"] = flat.get("home_season_gf_pg", 0) - flat.get("away_season_gf_pg", 0)
    flat["diff_season_ga"] = flat.get("home_season_ga_pg", 0) - flat.get("away_season_ga_pg", 0)
    flat["diff_goalie_sv_pct"] = flat.get("home_goalie_last5_sv_pct", 0) - flat.get("away_goalie_last5_sv_pct", 0)
    flat["diff_rest_days"] = flat.get("home_schedule_rest_days", 0) - flat.get("away_schedule_rest_days", 0)
    flat["diff_lineup_strength"] = flat.get("home_lineup_strength", 0) - flat.get("away_lineup_strength", 0)
    flat["diff_pp_pct"] = flat.get("home_special_pp_pct", 0) - flat.get("away_special_pp_pct", 0)
    flat["diff_pk_pct"] = flat.get("home_special_pk_pct", 0) - flat.get("away_special_pk_pct", 0)
    flat["diff_injury_impact"] = flat.get("home_injuries_xg_reduction", 0) - flat.get("away_injuries_xg_reduction", 0)

    # Discipline and clutch differentials
    flat["diff_discipline_rating"] = flat.get("home_discipline_rating", 0) - flat.get("away_discipline_rating", 0)
    flat["diff_close_game_wr"] = flat.get("home_close_game_wr", 0) - flat.get("away_close_game_wr", 0)
    flat["diff_scoring_first"] = flat.get("home_scoring_first_rate", 0) - flat.get("away_scoring_first_rate", 0)

    # Advanced metrics differentials
    flat["diff_corsi_pct"] = flat.get("home_advanced_cf_pct", 0) - flat.get("away_advanced_cf_pct", 0)
    flat["diff_shot_share"] = flat.get("home_advanced_shot_share", 0) - flat.get("away_advanced_shot_share", 0)
    flat["diff_shooting_pct"] = flat.get("home_advanced_shooting_pct", 0) - flat.get("away_advanced_shooting_pct", 0)
    flat["diff_pdo"] = flat.get("home_advanced_pdo", 0) - flat.get("away_advanced_pdo", 0)
    flat["diff_hd_proxy"] = flat.get("home_advanced_hd_proxy", 0) - flat.get("away_advanced_hd_proxy", 0)

    return flat


def get_feature_names() -> List[str]:
    """Return the ordered list of feature names produced by flatten_features().

    Generates from a dummy feature dict on first call and caches the result.
    This must be called after any changes to the feature set to refresh the cache.
    """
    global FEATURE_NAMES, _INITIALIZED
    if not _INITIALIZED:
        # Build from an empty feature dict to get all keys in order.
        dummy = flatten_features({})
        FEATURE_NAMES = list(dummy.keys())
        _INITIALIZED = True
    return FEATURE_NAMES


def reset_feature_cache() -> None:
    """Force re-computation of the feature name list on next call."""
    global _INITIALIZED
    _INITIALIZED = False


def features_to_array(flat: Dict[str, float]) -> np.ndarray:
    """Convert a flat feature dict to a 1D numpy array in canonical order."""
    names = get_feature_names()
    return np.array([flat.get(name, float("nan")) for name in names], dtype=np.float64)
