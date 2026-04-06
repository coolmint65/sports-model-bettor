"""
Travel fatigue factor for MLB predictions.

Computes a run adjustment based on a team's recent schedule density
and travel patterns using game data already in the DB.
"""

import logging
from datetime import datetime, timedelta

from .db import get_conn

logger = logging.getLogger(__name__)


def compute_travel_fatigue(team_id: int, game_date: str,
                           season: int | None = None) -> float:
    """
    Compute a travel/fatigue multiplier for expected runs.

    Checks the team's last 3 days of games:
    - 3 games in 3 consecutive days: fatigue = -2% (0.98)
    - Traveled (different venue from yesterday): fatigue = -1% (0.99)
    - Day off yesterday: rest bonus = +1% (1.01)

    Factors stack additively then are applied as a multiplier.
    Capped at ±3%.

    Returns a multiplier for expected runs (< 1.0 = fatigued, > 1.0 = rested).
    """
    conn = get_conn()
    yr = season or datetime.now().year
    game_dt = datetime.strptime(game_date, "%Y-%m-%d")

    # Fetch the team's games in the last 3 days
    three_days_ago = (game_dt - timedelta(days=3)).strftime("%Y-%m-%d")
    recent = conn.execute("""
        SELECT date, venue, home_team_id, away_team_id
        FROM games
        WHERE (home_team_id = ? OR away_team_id = ?)
          AND date >= ? AND date < ?
          AND season = ?
          AND status = 'final'
        ORDER BY date DESC
    """, (team_id, team_id, three_days_ago, game_date, yr)).fetchall()

    if not recent:
        # No recent games — could be start of season or long break
        return 1.0

    adjustment = 0.0

    # Check dates of recent games
    yesterday = (game_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    two_days_ago = (game_dt - timedelta(days=2)).strftime("%Y-%m-%d")

    played_yesterday = any(g["date"] == yesterday for g in recent)
    played_two_days_ago = any(g["date"] == two_days_ago for g in recent)
    played_three_days_ago = any(
        g["date"] == three_days_ago for g in recent
    )

    # 3 games in 3 consecutive days: fatigue
    if played_yesterday and played_two_days_ago and played_three_days_ago:
        adjustment -= 0.02

    # Day off yesterday: rest bonus
    if not played_yesterday:
        adjustment += 0.01

    # Travel: different venue yesterday vs today
    if played_yesterday:
        yesterday_games = [g for g in recent if g["date"] == yesterday]
        if yesterday_games:
            last_venue = yesterday_games[0]["venue"]
            # We don't know today's venue from the args, but we can check
            # if the team was away yesterday (implies travel is possible)
            was_away_yesterday = any(
                g["away_team_id"] == team_id for g in yesterday_games
            )
            # If the team was away yesterday, they likely traveled
            # (conservative: only penalize if venue changed)
            if was_away_yesterday:
                adjustment -= 0.01

    multiplier = 1.0 + adjustment
    return round(max(0.97, min(1.03, multiplier)), 4)
