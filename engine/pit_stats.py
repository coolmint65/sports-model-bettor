"""
Point-in-time stats computation.

For accurate backtesting, we need to know what each team's stats
were ON THE DATE of a game, not their end-of-season stats.

This module computes cumulative team stats from game results up to
any given date, eliminating lookahead bias in backtesting.
"""

import json
import logging
from datetime import datetime

from .db import get_conn

logger = logging.getLogger(__name__)


def compute_team_stats_at_date(team_id: int, date: str, season: int) -> dict:
    """
    Compute a team's cumulative stats from all games BEFORE the given date.

    Returns dict matching the team_stats schema: runs_pg, wins, losses,
    home_wins, home_losses, away_wins, away_losses, run_diff, etc.
    """
    conn = get_conn()

    games = conn.execute("""
        SELECT home_team_id, away_team_id, home_score, away_score,
               home_linescore, away_linescore, venue
        FROM games
        WHERE (home_team_id = ? OR away_team_id = ?)
          AND date < ? AND season = ? AND status = 'final'
          AND home_score IS NOT NULL AND away_score IS NOT NULL
        ORDER BY date
    """, (team_id, team_id, date, season)).fetchall()

    if not games:
        return {}

    total_runs_for = 0
    total_runs_against = 0
    wins = 0
    losses = 0
    home_wins = 0
    home_losses = 0
    away_wins = 0
    away_losses = 0
    games_played = 0
    first_inning_scoreless = 0
    first_inning_games = 0

    # Track last 10 for form
    last_10_results = []

    for g in games:
        g = dict(g)
        is_home = g["home_team_id"] == team_id
        team_score = g["home_score"] if is_home else g["away_score"]
        opp_score = g["away_score"] if is_home else g["home_score"]

        games_played += 1
        total_runs_for += team_score
        total_runs_against += opp_score

        won = team_score > opp_score
        if won:
            wins += 1
            if is_home:
                home_wins += 1
            else:
                away_wins += 1
        else:
            losses += 1
            if is_home:
                home_losses += 1
            else:
                away_losses += 1

        last_10_results.append(1 if won else 0)
        if len(last_10_results) > 10:
            last_10_results.pop(0)

        # First inning data from linescore
        home_ls = g.get("home_linescore")
        away_ls = g.get("away_linescore")
        if home_ls and away_ls:
            try:
                h_innings = json.loads(home_ls)
                a_innings = json.loads(away_ls)
                if len(h_innings) > 0 and len(a_innings) > 0:
                    first_inning_games += 1
                    if h_innings[0] == 0 and a_innings[0] == 0:
                        first_inning_scoreless += 1
            except (json.JSONDecodeError, IndexError):
                pass

    if games_played == 0:
        return {}

    runs_pg = round(total_runs_for / games_played, 2)
    runs_against_pg = round(total_runs_against / games_played, 2)
    run_diff = total_runs_for - total_runs_against

    # Streak
    streak_count = 0
    streak_type = ""
    for r in reversed(last_10_results):
        if streak_count == 0:
            streak_type = "W" if r else "L"
            streak_count = 1
        elif (r and streak_type == "W") or (not r and streak_type == "L"):
            streak_count += 1
        else:
            break

    l10_wins = sum(last_10_results)
    l10_losses = len(last_10_results) - l10_wins

    nrfi_rate = round(first_inning_scoreless / first_inning_games, 3) if first_inning_games > 0 else None

    return {
        "runs_pg": runs_pg,
        "runs_against_pg": runs_against_pg,
        "wins": wins,
        "losses": losses,
        "run_diff": run_diff,
        "home_wins": home_wins,
        "home_losses": home_losses,
        "away_wins": away_wins,
        "away_losses": away_losses,
        "last_10_wins": l10_wins,
        "last_10_losses": l10_losses,
        "streak": f"{streak_type}{streak_count}",
        "games_played": games_played,
        "nrfi_rate": nrfi_rate,
        "first_inning_games": first_inning_games,
    }


def compute_pitcher_stats_at_date(pitcher_id: int, date: str, season: int) -> dict:
    """
    Compute a pitcher's cumulative stats from games they started BEFORE
    the given date. Uses game results + linescore data.

    Returns basic pitcher stats: games, wins, losses, estimated ERA.
    """
    conn = get_conn()

    # Games where this pitcher was the starter
    games = conn.execute("""
        SELECT g.home_team_id, g.away_team_id, g.home_score, g.away_score,
               g.home_pitcher_id, g.away_pitcher_id,
               g.home_linescore, g.away_linescore,
               g.winning_pitcher, g.losing_pitcher
        FROM games g
        WHERE (g.home_pitcher_id = ? OR g.away_pitcher_id = ?)
          AND g.date < ? AND g.season = ? AND g.status = 'final'
        ORDER BY g.date
    """, (pitcher_id, pitcher_id, date, season)).fetchall()

    if not games:
        return {}

    starts = 0
    wins = 0
    losses = 0
    total_runs_allowed = 0
    total_first_inning_runs = 0
    first_inning_starts = 0

    for g in games:
        g = dict(g)
        starts += 1

        is_home_sp = g["home_pitcher_id"] == pitcher_id

        # Did they get the W or L?
        if g["winning_pitcher"] == pitcher_id:
            wins += 1
        elif g["losing_pitcher"] == pitcher_id:
            losses += 1

        # Estimate runs allowed: starter typically responsible for ~60% of team's runs allowed
        if is_home_sp:
            opp_score = g["away_score"] or 0
            opp_ls = g.get("away_linescore")
        else:
            opp_score = g["home_score"] or 0
            opp_ls = g.get("home_linescore")

        # Starter gets charged with ~60% of opponent runs
        total_runs_allowed += opp_score * 0.60

        # First inning runs against
        if opp_ls:
            try:
                opp_innings = json.loads(opp_ls)
                if len(opp_innings) > 0:
                    first_inning_starts += 1
                    total_first_inning_runs += opp_innings[0]
            except (json.JSONDecodeError, IndexError):
                pass

    if starts == 0:
        return {}

    # Estimate ERA: (runs_allowed / starts) * 9 / ~5.5 IP per start
    estimated_innings = starts * 5.5
    era = round((total_runs_allowed / estimated_innings) * 9, 2) if estimated_innings > 0 else None

    first_inning_era = None
    if first_inning_starts > 0:
        first_inning_era = round((total_first_inning_runs / first_inning_starts) * 9, 2)

    return {
        "games_started": starts,
        "wins": wins,
        "losses": losses,
        "era": era,
        "estimated_innings": round(estimated_innings, 1),
        "runs_per_start": round(total_runs_allowed / starts, 2),
        "first_inning_era": first_inning_era,
        "first_inning_runs_per_start": round(total_first_inning_runs / first_inning_starts, 2) if first_inning_starts > 0 else None,
    }
