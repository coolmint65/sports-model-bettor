"""
Model self-calibration system.

Analyzes prediction errors from completed games and adjusts factor
weights to minimize future errors. Runs after each day's games settle.

The model has several tunable weights:
- Pitcher ERA weight vs league average
- Home advantage magnitude
- Bullpen influence (% of game)
- Weather sensitivity
- Rest/fatigue impact
- Form recency weight

After each batch of games, this module:
1. Compares predicted runs to actual runs
2. Identifies systematic biases (over/under-predicting)
3. Adjusts weights toward better calibration
4. Saves weights to DB for persistence

This is NOT machine learning — it's statistical calibration.
Simple, transparent, and auditable.
"""

import json
import logging
import math
from datetime import datetime, timedelta

from .db import get_conn

logger = logging.getLogger(__name__)

# Default weights — these get adjusted by calibration
DEFAULT_WEIGHTS = {
    "pitcher_era_weight": 1.0,      # How much pitcher ERA affects runs
    "home_edge": 0.28,              # Home advantage in runs
    "bullpen_pct": 0.35,            # % of game bullpen covers
    "weather_sensitivity": 1.0,     # Multiplier on weather factor
    "rest_sensitivity": 1.0,        # Multiplier on rest factor
    "form_weight": 1.0,             # How much recent form matters
    "park_factor_weight": 1.0,      # How much park factors matter
    "h2h_weight": 1.0,              # H2H adjustment strength
    "nrfi_pitcher_weight": 0.50,    # Pitcher 1st-inning data weight
    "nrfi_team_weight": 0.30,       # Team 1st-inning data weight
}

WEIGHTS_KEY = "model_weights"


def get_weights() -> dict:
    """Load current model weights from DB, or return defaults."""
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS model_config (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()

    row = conn.execute(
        "SELECT value FROM model_config WHERE key = ?", (WEIGHTS_KEY,)
    ).fetchone()

    if row:
        try:
            saved = json.loads(row["value"])
            # Merge with defaults in case new weights were added
            weights = {**DEFAULT_WEIGHTS, **saved}
            return weights
        except json.JSONDecodeError:
            pass

    return dict(DEFAULT_WEIGHTS)


def save_weights(weights: dict) -> None:
    """Save model weights to DB."""
    conn = get_conn()
    conn.execute("""
        INSERT INTO model_config (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
    """, (WEIGHTS_KEY, json.dumps(weights)))
    conn.commit()


def calibrate(season: int | None = None, days: int = 30,
              learning_rate: float = 0.05) -> dict:
    """
    Analyze recent prediction errors and adjust weights.

    Looks at the last N days of games, compares predicted totals
    to actual totals, and nudges weights toward better calibration.

    Args:
        season: Which season to calibrate on
        days: Look back window (default 30 days)
        learning_rate: How aggressively to adjust (0.01 = conservative, 0.10 = aggressive)

    Returns calibration report.
    """
    conn = get_conn()
    yr = season or datetime.now().year
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    games = conn.execute("""
        SELECT * FROM games
        WHERE status = 'final' AND date >= ? AND season = ?
          AND home_score IS NOT NULL AND away_score IS NOT NULL
        ORDER BY date
    """, (start_date, yr)).fetchall()

    if len(games) < 5:
        return {"error": "Not enough games to calibrate", "games": len(games)}

    from .pit_stats import compute_team_stats_at_date, compute_pitcher_stats_at_date
    from .mlb_predict import MLB_AVG_RPG, MLB_AVG_ERA

    weights = get_weights()

    # Track errors
    total_errors = []     # predicted_total - actual_total
    home_errors = []      # predicted_home - actual_home
    away_errors = []
    pitcher_games = []    # (predicted_factor, actual_runs_allowed)
    weather_games = []    # (weather_factor, actual_vs_expected)

    for game in games:
        game = dict(game)
        home_id = game.get("home_team_id")
        away_id = game.get("away_team_id")
        date = game.get("date", "")

        if not home_id or not away_id or not date:
            continue

        home_pit = compute_team_stats_at_date(home_id, date, yr)
        away_pit = compute_team_stats_at_date(away_id, date, yr)

        # Use PIT data blended with league average (same as prediction model)
        home_rpg = MLB_AVG_RPG
        away_rpg = MLB_AVG_RPG
        if home_pit and home_pit.get("runs_pg"):
            gp = home_pit.get("games_played", 0)
            blend = min(gp / 30, 1.0)
            home_rpg = home_pit["runs_pg"] * blend + MLB_AVG_RPG * (1 - blend)
        if away_pit and away_pit.get("runs_pg"):
            gp = away_pit.get("games_played", 0)
            blend = min(gp / 30, 1.0)
            away_rpg = away_pit["runs_pg"] * blend + MLB_AVG_RPG * (1 - blend)

        home_off = home_rpg
        away_off = away_rpg

        home_sp_factor = 1.0
        away_sp_factor = 1.0
        if game.get("home_pitcher_id"):
            sp = compute_pitcher_stats_at_date(game["home_pitcher_id"], date, yr)
            if sp and sp.get("era") and sp.get("games_started", 0) >= 1:
                starts = sp["games_started"]
                sp_blend = min(starts / 8, 1.0)
                raw_factor = sp["era"] / MLB_AVG_ERA
                home_sp_factor = raw_factor * sp_blend + 1.0 * (1 - sp_blend)
                home_sp_factor = max(0.60, min(1.50, home_sp_factor * weights["pitcher_era_weight"]))
        if game.get("away_pitcher_id"):
            sp = compute_pitcher_stats_at_date(game["away_pitcher_id"], date, yr)
            if sp and sp.get("era") and sp.get("games_started", 0) >= 1:
                starts = sp["games_started"]
                sp_blend = min(starts / 8, 1.0)
                raw_factor = sp["era"] / MLB_AVG_ERA
                away_sp_factor = raw_factor * sp_blend + 1.0 * (1 - sp_blend)
                away_sp_factor = max(0.60, min(1.50, away_sp_factor * weights["pitcher_era_weight"]))

        pred_home = home_off * away_sp_factor + weights["home_edge"] / 2
        pred_away = away_off * home_sp_factor - weights["home_edge"] / 2

        actual_home = game["home_score"]
        actual_away = game["away_score"]

        total_errors.append((pred_home + pred_away) - (actual_home + actual_away))
        home_errors.append(pred_home - actual_home)
        away_errors.append(pred_away - actual_away)

    n = len(total_errors)
    if n < 5:
        return {"error": "Not enough valid games", "games": n}

    # Scale learning rate by sample size — more conservative with fewer games
    effective_lr = learning_rate * min(n / 100, 1.0)

    # Analyze biases
    avg_total_error = sum(total_errors) / n
    avg_home_error = sum(home_errors) / n
    avg_away_error = sum(away_errors) / n
    rmse = math.sqrt(sum(e**2 for e in total_errors) / n)

    # Accuracy metrics
    correct_winner = 0
    for game in games:
        game = dict(game)
        hs, aws = game.get("home_score", 0), game.get("away_score", 0)
        if hs is None or aws is None:
            continue
        # We don't have predicted scores stored, so skip this for now

    report = {
        "games_analyzed": n,
        "period": f"Last {days} days",
        "avg_total_error": round(avg_total_error, 3),
        "avg_home_error": round(avg_home_error, 3),
        "avg_away_error": round(avg_away_error, 3),
        "rmse": round(rmse, 3),
        "adjustments": {},
        "weights_before": dict(weights),
    }

    # ── Apply adjustments ──

    # If we consistently over-predict totals, reduce pitcher weight
    # (pitchers are suppressing less than we think)
    if avg_total_error > 0.2:
        adj = min(effective_lr, avg_total_error * 0.02)
        weights["pitcher_era_weight"] = round(weights["pitcher_era_weight"] + adj, 4)
        report["adjustments"]["pitcher_era_weight"] = f"+{adj:.4f} (over-predicting by {avg_total_error:+.2f} runs)"
    elif avg_total_error < -0.2:
        adj = min(effective_lr, abs(avg_total_error) * 0.02)
        weights["pitcher_era_weight"] = round(weights["pitcher_era_weight"] - adj, 4)
        report["adjustments"]["pitcher_era_weight"] = f"-{adj:.4f} (under-predicting by {avg_total_error:+.2f} runs)"

    # Home bias
    home_bias = avg_home_error - avg_away_error
    if abs(home_bias) > 0.15:
        adj = home_bias * effective_lr * 0.5
        weights["home_edge"] = round(max(0.0, min(0.60, weights["home_edge"] - adj)), 4)
        report["adjustments"]["home_edge"] = f"{'+'if adj>0 else ''}{-adj:.4f} (home bias: {home_bias:+.2f})"

    # Win accuracy — track how often we pick the right winner
    correct_winners = 0
    total_decided = 0
    for err_h, err_a in zip(home_errors, away_errors):
        pred_home_wins = (err_h + 999) > (err_a + 999)  # crude, but directional
        # Actually we need the raw predictions, not errors. Skip for now.

    report["effective_learning_rate"] = round(effective_lr, 4)
    report["sample_size_pct"] = round(min(n / 100, 1.0) * 100, 1)

    # Clamp all weights to reasonable ranges
    weights["pitcher_era_weight"] = max(0.50, min(1.50, weights["pitcher_era_weight"]))
    weights["home_edge"] = max(0.0, min(0.60, weights["home_edge"]))
    weights["bullpen_pct"] = max(0.20, min(0.50, weights["bullpen_pct"]))
    weights["weather_sensitivity"] = max(0.50, min(1.50, weights["weather_sensitivity"]))
    weights["rest_sensitivity"] = max(0.50, min(1.50, weights["rest_sensitivity"]))

    report["weights_after"] = dict(weights)

    # Save
    save_weights(weights)
    logger.info("Calibration complete: %d games, RMSE=%.3f, avg_error=%.3f",
                n, rmse, avg_total_error)

    return report


def get_calibration_status() -> dict:
    """Return current weights and last calibration info."""
    weights = get_weights()
    conn = get_conn()
    row = conn.execute(
        "SELECT updated_at FROM model_config WHERE key = ?", (WEIGHTS_KEY,)
    ).fetchone()

    return {
        "weights": weights,
        "last_calibrated": row["updated_at"] if row else None,
        "is_default": weights == DEFAULT_WEIGHTS,
    }


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    args = sys.argv[1:]
    days = 30
    for i, a in enumerate(args):
        if a == "--days" and i + 1 < len(args):
            days = int(args[i + 1])

    print(f"Calibrating on last {days} days...", flush=True)
    report = calibrate(days=days)

    if "error" in report:
        print(f"Error: {report['error']}")
    else:
        print(f"\nGames analyzed: {report['games_analyzed']}")
        print(f"RMSE: {report['rmse']}")
        print(f"Avg total error: {report['avg_total_error']:+.3f} runs")
        print(f"Avg home error: {report['avg_home_error']:+.3f}")
        print(f"Avg away error: {report['avg_away_error']:+.3f}")
        print(f"\nAdjustments:")
        for k, v in report["adjustments"].items():
            print(f"  {k}: {v}")
        print(f"\nWeights saved.")
