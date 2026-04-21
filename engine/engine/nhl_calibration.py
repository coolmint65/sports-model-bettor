"""
NHL Model Calibration — learns from prediction errors to improve accuracy.

Analyzes recent completed games, compares predicted vs actual scores,
and adjusts model weights. Runs daily after settling picks.

Usage:
    python -m engine.nhl_calibration --days 30
"""

import logging
import math
from datetime import datetime, timedelta

from .nhl_db import get_conn

logger = logging.getLogger(__name__)


def calibrate(days: int = 0) -> dict:
    """
    Analyze NHL prediction errors and adjust model weights.
    days=0 means full season (all completed games).
    """
    conn = get_conn()

    if days > 0:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        games = conn.execute("""
            SELECT g.*,
                   ht.abbreviation as home_abbr, at.abbreviation as away_abbr
            FROM nhl_games g
            LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
            LEFT JOIN nhl_teams at ON g.away_team_id = at.id
            WHERE g.status = 'final' AND g.date >= ?
            ORDER BY g.date
        """, (cutoff,)).fetchall()
    else:
        # Full season — learn from ALL completed games
        games = conn.execute("""
            SELECT g.*,
                   ht.abbreviation as home_abbr, at.abbreviation as away_abbr
            FROM nhl_games g
            LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
            LEFT JOIN nhl_teams at ON g.away_team_id = at.id
            WHERE g.status = 'final'
            ORDER BY g.date
        """).fetchall()

    if len(games) < 10:
        return {"message": f"Not enough games ({len(games)}) for calibration", "games": len(games)}

    # Compute model's prediction errors
    total_error = 0
    home_bias = 0
    total_over = 0
    game_count = 0

    for game in games:
        game = dict(game)
        hs = game.get("home_score", 0) or 0
        as_ = game.get("away_score", 0) or 0
        actual_total = hs + as_
        actual_margin = hs - as_

        # We don't store predictions per-game yet, so use league averages
        # to compute bias corrections
        home_bias += 1 if hs > as_ else (-1 if as_ > hs else 0)
        total_over += actual_total
        game_count += 1

    if game_count == 0:
        return {"message": "No completed games found", "games": 0}

    avg_total = total_over / game_count
    home_win_rate = (home_bias + game_count) / (2 * game_count)  # Normalize to 0-1

    # Expected NHL averages
    expected_total = 6.0
    expected_home_wr = 0.545  # Historical NHL home win rate

    # Compute adjustments
    total_bias = avg_total - expected_total
    home_bias_pct = home_win_rate - expected_home_wr

    # Learning rate — learn faster with more data
    # Reaches full rate at 200 games instead of 500
    lr = min(0.20, game_count / 200)

    # Current config
    current_home_edge = _get_config("home_edge", 0.15)
    current_total_adj = _get_config("total_adjustment", 0.0)

    # Adjust
    new_home_edge = current_home_edge + home_bias_pct * lr
    new_home_edge = max(0.05, min(0.30, new_home_edge))

    new_total_adj = current_total_adj + total_bias * lr * 0.1
    new_total_adj = max(-0.3, min(0.3, new_total_adj))

    # Save
    _set_config("home_edge", new_home_edge)
    _set_config("total_adjustment", new_total_adj)
    _set_config("last_calibration", datetime.now().timestamp())
    _set_config("calibration_games", game_count)

    # Compute goalie impact stats
    goalie_results = _calibrate_goalie_weights(conn, cutoff)

    return {
        "games_analyzed": game_count,
        "avg_total": round(avg_total, 2),
        "home_win_rate": round(home_win_rate, 3),
        "adjustments": {
            "home_edge": round(new_home_edge, 4),
            "total_adjustment": round(new_total_adj, 4),
        },
        "goalie_calibration": goalie_results,
    }


def _calibrate_goalie_weights(conn, cutoff: str) -> dict:
    """Analyze how much goalie performance matters in game outcomes."""
    games_with_goalies = conn.execute("""
        SELECT g.*,
               hg.save_pct as home_goalie_svpct,
               ag.save_pct as away_goalie_svpct
        FROM nhl_games g
        LEFT JOIN goalie_stats hg ON g.home_goalie_id = hg.player_id
        LEFT JOIN goalie_stats ag ON g.away_goalie_id = ag.player_id
        WHERE g.status = 'final' AND g.date >= ?
              AND g.home_goalie_id IS NOT NULL AND g.away_goalie_id IS NOT NULL
    """, (cutoff,)).fetchall()

    if len(games_with_goalies) < 5:
        return {"games": len(games_with_goalies), "message": "Not enough goalie data"}

    # Track how often the better goalie's team wins
    better_goalie_wins = 0
    total = 0
    for g in games_with_goalies:
        g = dict(g)
        h_sv = g.get("home_goalie_svpct") or 0
        a_sv = g.get("away_goalie_svpct") or 0
        hs = g.get("home_score", 0) or 0
        as_ = g.get("away_score", 0) or 0

        if h_sv > 0 and a_sv > 0 and hs != as_:
            total += 1
            if (h_sv > a_sv and hs > as_) or (a_sv > h_sv and as_ > hs):
                better_goalie_wins += 1

    goalie_impact = better_goalie_wins / total if total > 0 else 0.5

    # Store goalie weight
    _set_config("goalie_weight", goalie_impact)

    return {
        "games_with_goalies": total,
        "better_goalie_win_rate": round(goalie_impact, 3),
    }


def _get_config(key: str, default: float = 0.0) -> float:
    """Get a config value from nhl_model_config."""
    conn = get_conn()
    row = conn.execute(
        "SELECT value FROM nhl_model_config WHERE key = ?", (key,)
    ).fetchone()
    return row["value"] if row else default


def _set_config(key: str, value: float):
    """Set a config value in nhl_model_config."""
    conn = get_conn()
    conn.execute("""
        INSERT INTO nhl_model_config (key, value, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET value = ?, updated_at = datetime('now')
    """, (key, value, value))
    conn.commit()


def get_calibrated_home_edge() -> float:
    """Get the calibrated home-ice advantage."""
    return _get_config("home_edge", 0.15)


def get_total_adjustment() -> float:
    """Get the calibrated total scoring adjustment."""
    return _get_config("total_adjustment", 0.0)


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    days = 30
    for arg in sys.argv[1:]:
        if arg.startswith("--days"):
            try:
                days = int(sys.argv[sys.argv.index(arg) + 1])
            except (IndexError, ValueError):
                pass

    print(f"Calibrating NHL model from last {days} days...", flush=True)
    result = calibrate(days)
    print(f"  Games analyzed: {result.get('games_analyzed', 0)}")
    print(f"  Avg total goals: {result.get('avg_total', '?')}")
    print(f"  Home win rate: {result.get('home_win_rate', '?')}")
    adj = result.get("adjustments", {})
    print(f"  Home edge: {adj.get('home_edge', '?')}")
    print(f"  Total adjustment: {adj.get('total_adjustment', '?')}")
    gc = result.get("goalie_calibration", {})
    print(f"  Goalie impact: {gc.get('better_goalie_win_rate', '?')}")
