"""
Umpire tendency analysis for MLB predictions.

Fetches home plate umpire data from the MLB Stats API and computes
run adjustments based on historical umpire tendencies (zone size,
K/BB rates, runs per game).
"""

import logging
from datetime import datetime

import requests

from .db import get_conn

logger = logging.getLogger(__name__)

MLB_AVG_RPG_TOTAL = 9.0  # League-average total runs per game (both teams)


def get_umpire_for_game(game_pk: int) -> dict | None:
    """
    Fetch the home plate umpire for a game from the MLB Stats API.

    Returns dict with 'id' and 'name', or None if unavailable.
    """
    url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("Failed to fetch boxscore for game %s: %s", game_pk, e)
        return None

    officials = data.get("officials", [])
    for official in officials:
        job = official.get("officialType", "")
        if job == "Home Plate":
            person = official.get("official", {})
            ump_id = person.get("id")
            ump_name = person.get("fullName")
            if ump_id and ump_name:
                return {"id": ump_id, "name": ump_name}

    return None


def get_umpire_tendencies(umpire_name: str) -> dict | None:
    """
    Look up stored tendencies for an umpire from the DB.

    Returns dict with rpg, k_pct, bb_pct, run_factor, games, etc.
    or None if not found.
    """
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM umpires WHERE name = ?", (umpire_name,)
    ).fetchone()
    if not row:
        return None
    return dict(row)


def compute_umpire_adjustment(umpire_name: str) -> float:
    """
    Return a run adjustment multiplier based on the umpire's historical
    runs/game relative to league average.

    > 1.0 means umpire tends to allow more runs (bigger zone = fewer Ks,
    or smaller zone = more walks/runs).
    < 1.0 means umpire tends to suppress runs.

    Capped at ±5% (0.95 to 1.05).
    """
    tendencies = get_umpire_tendencies(umpire_name)
    if not tendencies:
        return 1.0

    rpg = tendencies.get("rpg")
    games = tendencies.get("games", 0)
    run_factor = tendencies.get("run_factor")

    # If we have a pre-computed run_factor, use it directly
    if run_factor is not None and games >= 10:
        return max(0.95, min(1.05, run_factor))

    # Otherwise compute from rpg vs league average
    if rpg is None or rpg <= 0 or games < 10:
        return 1.0

    factor = rpg / MLB_AVG_RPG_TOTAL
    return max(0.95, min(1.05, factor))


def update_umpire_stats(season: int | None = None) -> int:
    """
    Iterate through completed games in the DB, find the HP umpire for each,
    and compute their avg runs/game, K rate, BB rate. Store in the umpires table.

    Returns the number of umpires updated.
    """
    conn = get_conn()
    yr = season or datetime.now().year

    # Get all final games with an umpire recorded
    games = conn.execute("""
        SELECT mlb_game_id, home_score, away_score, umpire
        FROM games
        WHERE season = ? AND status = 'final' AND umpire IS NOT NULL
          AND home_score IS NOT NULL AND away_score IS NOT NULL
    """, (yr,)).fetchall()

    if not games:
        logger.info("No completed games with umpire data for season %s", yr)
        return 0

    # Aggregate stats per umpire
    ump_stats: dict[str, dict] = {}
    for g in games:
        name = g["umpire"]
        if not name:
            continue
        if name not in ump_stats:
            ump_stats[name] = {
                "games": 0,
                "total_runs": 0,
            }
        ump_stats[name]["games"] += 1
        ump_stats[name]["total_runs"] += (g["home_score"] or 0) + (g["away_score"] or 0)

    # Also try to fetch umpire data from the API for games without umpire info
    # (skip for now — only process games that already have umpire stored)

    updated = 0
    for name, stats in ump_stats.items():
        if stats["games"] < 1:
            continue

        rpg = stats["total_runs"] / stats["games"]
        run_factor = rpg / MLB_AVG_RPG_TOTAL

        conn.execute("""
            INSERT INTO umpires (name, games, rpg, run_factor, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(name) DO UPDATE SET
                games = excluded.games,
                rpg = excluded.rpg,
                run_factor = excluded.run_factor,
                updated_at = excluded.updated_at
        """, (name, stats["games"], round(rpg, 2), round(run_factor, 4)))
        updated += 1

    conn.commit()
    logger.info("Updated %d umpires for season %d", updated, yr)
    return updated
