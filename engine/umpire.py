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
    Rebuild umpire tendency aggregates from final games.

    Writes per-season rows to umpire_season_stats(name, season, ...) so
    training-time feature extraction can do strict point-in-time lookup
    (use season S-1 stats for a game in season S, avoiding look-ahead
    leak). Also refreshes the all-time `umpires` rollup, which inference
    today reads through compute_umpire_adjustment().

    Args:
        season: If provided, rebuild only that season's rows. If None,
            rebuild every season present in the games table.

    Returns total (name, season) rows written.
    """
    conn = get_conn()
    if season is None:
        seasons = [r["season"] for r in conn.execute(
            "SELECT DISTINCT season FROM games "
            "WHERE umpire IS NOT NULL AND status = 'final' AND season IS NOT NULL "
            "ORDER BY season"
        ).fetchall()]
    else:
        seasons = [season]

    if not seasons:
        logger.info("update_umpire_stats: no seasons with umpire data")
        return 0

    total_rows = 0
    for yr in seasons:
        total_rows += _rebuild_season(conn, yr)

    _rebuild_all_time_rollup(conn)
    conn.commit()
    logger.info("update_umpire_stats: wrote %d (name, season) rows across %d seasons",
                total_rows, len(seasons))
    return total_rows


def _rebuild_season(conn, yr: int) -> int:
    """Aggregate one season's games into umpire_season_stats."""
    games = conn.execute("""
        SELECT umpire, home_score, away_score
        FROM games
        WHERE season = ? AND status = 'final' AND umpire IS NOT NULL
          AND home_score IS NOT NULL AND away_score IS NOT NULL
    """, (yr,)).fetchall()
    if not games:
        return 0

    ump_stats: dict[str, dict] = {}
    for g in games:
        name = g["umpire"]
        if not name:
            continue
        bucket = ump_stats.setdefault(name, {"games": 0, "total_runs": 0})
        bucket["games"] += 1
        bucket["total_runs"] += (g["home_score"] or 0) + (g["away_score"] or 0)

    # Wipe prior rows for this season so re-runs are idempotent (e.g. if
    # a game's umpire was corrected after a prior aggregate, stale entries
    # for that umpire in the season would linger otherwise).
    conn.execute("DELETE FROM umpire_season_stats WHERE season = ?", (yr,))

    rows_written = 0
    for name, s in ump_stats.items():
        if s["games"] < 1:
            continue
        rpg = s["total_runs"] / s["games"]
        run_factor = rpg / MLB_AVG_RPG_TOTAL
        conn.execute("""
            INSERT INTO umpire_season_stats (name, season, games, rpg, run_factor)
            VALUES (?, ?, ?, ?, ?)
        """, (name, yr, s["games"], round(rpg, 2), round(run_factor, 4)))
        rows_written += 1

    logger.info("  season %d: %d umpires, %d games", yr, rows_written, len(games))
    return rows_written


def _rebuild_all_time_rollup(conn) -> int:
    """Recompute the `umpires` table as career aggregates across seasons.

    The all-time rollup backs inference-time `compute_umpire_adjustment()`
    and the legacy factor-model path. It sums games / total_runs across
    all seasons in umpire_season_stats so a single DB row reflects an
    umpire's full observed career in our data.
    """
    rows = conn.execute(
        "SELECT name, SUM(games) AS games, SUM(games * rpg) AS total_runs "
        "FROM umpire_season_stats GROUP BY name"
    ).fetchall()
    if not rows:
        return 0

    conn.execute("DELETE FROM umpires")
    for r in rows:
        games_n = r["games"] or 0
        tot_runs = r["total_runs"] or 0.0
        if games_n < 1:
            continue
        rpg = tot_runs / games_n
        run_factor = rpg / MLB_AVG_RPG_TOTAL
        conn.execute("""
            INSERT INTO umpires (name, games, rpg, run_factor)
            VALUES (?, ?, ?, ?)
        """, (r["name"], games_n, round(rpg, 2), round(run_factor, 4)))

    return len(rows)
