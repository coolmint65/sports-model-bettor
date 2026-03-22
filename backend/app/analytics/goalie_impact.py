"""
Goalie confirmation impact analysis.

Runs predictions twice — once with the expected starter, once with the
backup — and measures the delta. If the pick flips on a goalie change,
the bet is fragile and should be flagged.

Also supports auto-rerunning predictions when goalies are confirmed
(typically 10-11am ET on game day).
"""

import logging
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def analyze_goalie_sensitivity(
    features: Dict[str, Any],
    model,
    sport: str = "nhl",
) -> Dict[str, Any]:
    """Analyze how sensitive predictions are to goalie changes.

    Runs the model with confirmed starters and again with backup
    scenarios to measure prediction fragility.

    Args:
        features: Full game features dict.
        model: BettingModel instance.
        sport: Sport identifier (only NHL has goalie sensitivity).

    Returns:
        Dict with sensitivity analysis results.
    """
    if sport != "nhl":
        return {"sport": sport, "applicable": False}

    home_goalie = features.get("home_goalie", {})
    away_goalie = features.get("away_goalie", {})

    home_starter_conf = features.get("home_starter_status", {}).get(
        "starter_confidence", 0.9
    )
    away_starter_conf = features.get("away_starter_status", {}).get(
        "starter_confidence", 0.9
    )

    # Run with current starters
    base_predictions = _run_predictions_sync(model, features)

    # Scenarios to test
    scenarios = []

    # Scenario 1: Home backup starts instead
    if home_goalie.get("goalie_name"):
        home_backup_features = _swap_goalie_to_backup(features, "home")
        home_backup_preds = _run_predictions_sync(model, home_backup_features)
        scenarios.append({
            "scenario": "home_backup_starts",
            "description": f"If {home_goalie.get('goalie_name', 'home starter')} doesn't start",
            "predictions": home_backup_preds,
        })

    # Scenario 2: Away backup starts instead
    if away_goalie.get("goalie_name"):
        away_backup_features = _swap_goalie_to_backup(features, "away")
        away_backup_preds = _run_predictions_sync(model, away_backup_features)
        scenarios.append({
            "scenario": "away_backup_starts",
            "description": f"If {away_goalie.get('goalie_name', 'away starter')} doesn't start",
            "predictions": away_backup_preds,
        })

    # Analyze deltas
    ml_base = _find_prediction(base_predictions, "ml")
    fragile_picks = []
    pick_deltas = []

    for scenario in scenarios:
        ml_scenario = _find_prediction(scenario["predictions"], "ml")

        if ml_base and ml_scenario:
            conf_delta = abs(ml_base["confidence"] - ml_scenario["confidence"])
            pick_flipped = ml_base.get("prediction") != ml_scenario.get("prediction")

            delta_info = {
                "scenario": scenario["scenario"],
                "description": scenario["description"],
                "base_pick": ml_base.get("prediction"),
                "base_confidence": ml_base.get("confidence"),
                "scenario_pick": ml_scenario.get("prediction"),
                "scenario_confidence": ml_scenario.get("confidence"),
                "confidence_delta": round(conf_delta, 4),
                "pick_flipped": pick_flipped,
            }
            pick_deltas.append(delta_info)

            if pick_flipped:
                fragile_picks.append(delta_info)

    # Compute fragility score (0-1)
    # 0 = rock solid regardless of goalie
    # 1 = pick completely depends on goalie
    max_delta = max(
        (d["confidence_delta"] for d in pick_deltas), default=0
    )
    any_flipped = len(fragile_picks) > 0

    if any_flipped:
        fragility = min(1.0, 0.6 + max_delta)
    else:
        fragility = min(0.5, max_delta * 2)

    # Starter uncertainty penalty
    uncertainty = 0.0
    if home_starter_conf < 0.65:
        uncertainty += 0.15
    if away_starter_conf < 0.65:
        uncertainty += 0.10

    return {
        "applicable": True,
        "home_goalie": home_goalie.get("goalie_name"),
        "away_goalie": away_goalie.get("goalie_name"),
        "home_starter_confidence": home_starter_conf,
        "away_starter_confidence": away_starter_conf,
        "base_ml_pick": ml_base.get("prediction") if ml_base else None,
        "base_ml_confidence": ml_base.get("confidence") if ml_base else None,
        "pick_deltas": pick_deltas,
        "fragile_picks": fragile_picks,
        "fragility_score": round(fragility, 3),
        "starter_uncertainty": round(uncertainty, 3),
        "recommendation": _goalie_recommendation(fragility, uncertainty, home_starter_conf, away_starter_conf),
    }


def _swap_goalie_to_backup(features: Dict[str, Any], side: str) -> Dict[str, Any]:
    """Create a feature set simulating a backup goalie starting.

    Replaces the starter's stats with degraded backup-tier stats.
    """
    modified = deepcopy(features)
    goalie_key = f"{side}_goalie"
    goalie = modified.get(goalie_key, {})

    if not goalie:
        return modified

    # Simulate backup goalie: worse save%, less experience
    backup_goalie = {
        **goalie,
        "goalie_name": f"{goalie.get('goalie_name', 'Starter')} (BACKUP)",
        "season_sv_pct": min(goalie.get("season_sv_pct", 0.905), 0.900),
        "recent_sv_pct": min(goalie.get("recent_sv_pct", 0.905), 0.895),
        "tier": "backup",
        "games_started": max(5, (goalie.get("games_started", 30)) // 3),
        "starter_confidence": 0.4,
    }
    modified[goalie_key] = backup_goalie

    # Also update starter status
    status_key = f"{side}_starter_status"
    modified[status_key] = {
        **modified.get(status_key, {}),
        "starter_confidence": 0.4,
    }

    return modified


def _run_predictions_sync(model, features: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Run model predictions synchronously (for comparison purposes).

    Calls the model's synchronous xG calculation and prediction methods.
    """
    try:
        # Use the internal method directly to avoid async
        home_xg, away_xg = model._calc_expected_goals(features)
        preds = model._generate_predictions(features, home_xg, away_xg)
        return preds
    except Exception as e:
        logger.warning("Failed to run goalie scenario predictions: %s", e)
        return []


def _find_prediction(predictions: List[Dict], bet_type: str) -> Optional[Dict]:
    """Find a specific bet type in a list of predictions."""
    for pred in predictions:
        if pred.get("bet_type") == bet_type:
            return pred
    return None


def _goalie_recommendation(
    fragility: float,
    uncertainty: float,
    home_conf: float,
    away_conf: float,
) -> str:
    """Generate a recommendation based on goalie analysis."""
    if fragility > 0.6:
        return "wait_for_confirmation"
    if fragility > 0.3 and (home_conf < 0.65 or away_conf < 0.65):
        return "wait_for_confirmation"
    if uncertainty > 0.15:
        return "reduce_size"
    if fragility > 0.2:
        return "monitor"
    return "proceed"
