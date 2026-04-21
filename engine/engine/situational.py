"""
Situational adjustments for MLB predictions.

Factors that modify expected runs based on game-day context:
- Weather (temperature, wind)
- Team rest / travel fatigue
- Pitcher days rest
- Lineup strength (when confirmed lineups available)
- Platoon advantage (L/R matchups)
"""

import json
import logging
import math
from datetime import datetime, timedelta

from .db import get_conn

logger = logging.getLogger(__name__)


# ── Weather Impact ──────────────────────────────────────────

def weather_factor(temp: float | None, wind: str | None,
                   venue: str | None = None) -> float:
    """
    Adjust runs based on weather conditions.

    Temperature: Every 10F above 70 adds ~0.5% to run scoring.
    Below 50F suppresses scoring significantly.
    Wind: "out" boosts HR/runs, "in" suppresses.

    Returns multiplier (1.0 = neutral).
    """
    factor = 1.0

    # Temperature adjustment
    if temp is not None:
        if temp > 90:
            factor *= 1.04      # Hot = more runs
        elif temp > 80:
            factor *= 1.02
        elif temp > 70:
            factor *= 1.00      # Neutral
        elif temp > 60:
            factor *= 0.99
        elif temp > 50:
            factor *= 0.97      # Cool = fewer runs
        else:
            factor *= 0.95      # Cold = significantly fewer

    # Wind adjustment (parse wind string from MLB API)
    if wind and isinstance(wind, str):
        wind_lower = wind.lower()
        # Try to extract wind speed
        speed = 0
        for part in wind.split():
            try:
                speed = int(part)
                break
            except ValueError:
                continue

        if speed >= 10:
            if "out" in wind_lower or "left" in wind_lower or "right" in wind_lower:
                factor *= 1.03 + (speed - 10) * 0.003  # Wind out = more runs
            elif "in" in wind_lower:
                factor *= 0.97 - (speed - 10) * 0.003  # Wind in = fewer runs

    # Dome/roof venues are always neutral regardless of weather
    dome_venues = {"Tropicana Field", "Minute Maid Park", "T-Mobile Park",
                   "loanDepot park", "Globe Life Field", "Chase Field",
                   "Rogers Centre", "American Family Field"}
    if venue and venue in dome_venues:
        factor = 1.0

    return round(max(0.90, min(1.10, factor)), 4)


# ── Rest / Travel Fatigue ───────────────────────────────────

def rest_fatigue_factor(team_id: int, game_date: str,
                        season: int | None = None) -> float:
    """
    Adjust for team fatigue based on recent schedule density and travel.

    Factors:
    - Games in last 3 days (4 games in 3 days = tired)
    - Road games (travel wears teams down)
    - Day game after night game (worst scenario)

    Returns multiplier (1.0 = neutral, <1 = fatigued).
    """
    conn = get_conn()
    yr = season or datetime.now().year

    three_days_ago = (datetime.strptime(game_date, "%Y-%m-%d") - timedelta(days=3)).strftime("%Y-%m-%d")

    recent = conn.execute("""
        SELECT date, home_team_id, away_team_id, day_night, venue
        FROM games
        WHERE (home_team_id = ? OR away_team_id = ?)
          AND date >= ? AND date < ? AND season = ?
          AND status = 'final'
        ORDER BY date DESC
    """, (team_id, team_id, three_days_ago, game_date, yr)).fetchall()

    if not recent:
        return 1.0

    games_3d = len(recent)
    road_games = sum(1 for g in recent if g["away_team_id"] == team_id)

    factor = 1.0

    # Heavy schedule
    if games_3d >= 4:
        factor *= 0.97      # 4 games in 3 days = fatigued
    elif games_3d >= 3:
        factor *= 0.99

    # Mostly road
    if road_games >= 3:
        factor *= 0.98       # Heavy travel

    # Day game after night game (check most recent)
    if len(recent) >= 1:
        last_game = dict(recent[0])
        if last_game.get("day_night") == "night":
            # If today is a day game and yesterday was night...
            # We'd need today's day_night to check, but estimate from game count
            if games_3d >= 3:
                factor *= 0.98  # Extra fatigue

    return round(max(0.93, min(1.02, factor)), 4)


# ── Pitcher Days Rest ───────────────────────────────────────

def pitcher_rest_factor(pitcher_id: int, game_date: str,
                        season: int | None = None) -> float:
    """
    Adjust for pitcher rest.

    Standard rest: 5 days between starts.
    Short rest (4 days): slight negative.
    Extra rest (6+ days): slight positive (fresh arm).
    Very long rest (10+): slight negative (rusty).

    Returns multiplier (1.0 = normal rest).
    """
    conn = get_conn()
    yr = season or datetime.now().year

    # Find most recent start before this game
    last_start = conn.execute("""
        SELECT date FROM games
        WHERE (home_pitcher_id = ? OR away_pitcher_id = ?)
          AND date < ? AND season = ? AND status = 'final'
        ORDER BY date DESC
        LIMIT 1
    """, (pitcher_id, pitcher_id, game_date, yr)).fetchone()

    if not last_start:
        return 1.0  # First start of season or no data

    last_date = datetime.strptime(last_start["date"], "%Y-%m-%d")
    game_dt = datetime.strptime(game_date, "%Y-%m-%d")
    days_rest = (game_dt - last_date).days

    if days_rest <= 3:
        return 1.06      # Very short rest = bad for pitcher (opponent scores more)
    elif days_rest == 4:
        return 1.03       # Short rest
    elif days_rest == 5:
        return 1.0        # Normal
    elif days_rest <= 7:
        return 0.98       # Extra rest = good
    elif days_rest <= 10:
        return 1.0        # Extended but not rusty
    else:
        return 1.02       # Very long layoff = rust


# ── Lineup Strength ─────────────────────────────────────────

def lineup_strength_factor(lineup: list[dict] | None) -> float:
    """
    Adjust offense based on confirmed lineup quality.

    Uses the lineup's average OPS to compare against league average.
    Missing key batters significantly weaken a lineup.

    Returns multiplier (1.0 = average lineup).
    """
    if not lineup or len(lineup) < 5:
        return 1.0  # No lineup data, assume average

    # Average OPS of the lineup
    ops_values = [p.get("season_ops") for p in lineup if p.get("season_ops")]
    if len(ops_values) < 3:
        return 1.0

    avg_ops = sum(ops_values) / len(ops_values)
    league_avg_ops = 0.720

    # Scale: .720 OPS = 1.0, .800 = ~1.05, .650 = ~0.95
    factor = avg_ops / league_avg_ops

    return round(max(0.85, min(1.15, factor)), 4)


# ── Platoon Advantage ───────────────────────────────────────

def platoon_factor(lineup: list[dict] | None, pitcher_throws: str | None) -> float:
    """
    Adjust for platoon (L/R) matchup advantage.

    Batters hit significantly better against opposite-hand pitchers:
    - RHB vs LHP: ~15-20 OPS points higher
    - LHB vs RHP: ~15-20 OPS points higher
    - Same-side matchups favor the pitcher

    Returns multiplier (>1 = lineup has platoon advantage).
    """
    if not lineup or not pitcher_throws:
        return 1.0

    pitcher_hand = pitcher_throws.upper()
    if pitcher_hand not in ("L", "R"):
        return 1.0

    # Count batters with platoon advantage
    advantage = 0
    disadvantage = 0
    total = 0

    for batter in lineup:
        bats = (batter.get("bats") or "").upper()
        if not bats or bats == "S":  # Switch hitters are neutral
            continue
        total += 1
        if (bats == "R" and pitcher_hand == "L") or (bats == "L" and pitcher_hand == "R"):
            advantage += 1
        else:
            disadvantage += 1

    if total == 0:
        return 1.0

    # More batters with advantage = offense boost
    advantage_pct = advantage / total
    if advantage_pct > 0.60:
        return 1.03      # Strong platoon advantage
    elif advantage_pct > 0.45:
        return 1.01       # Slight advantage
    elif advantage_pct < 0.30:
        return 0.97       # Pitcher has platoon advantage
    else:
        return 1.0


# ── Combined situational adjustment ─────────────────────────

def compute_all_adjustments(home_team_id: int, away_team_id: int,
                            home_pitcher_id: int | None,
                            away_pitcher_id: int | None,
                            game_date: str,
                            venue: str | None = None,
                            weather_temp: float | None = None,
                            weather_wind: str | None = None,
                            home_lineup: list | None = None,
                            away_lineup: list | None = None,
                            home_pitcher_throws: str | None = None,
                            away_pitcher_throws: str | None = None,
                            season: int | None = None) -> dict:
    """
    Compute all situational adjustments for a game.

    Returns dict with individual factors and combined multipliers
    for home and away expected runs.
    """
    yr = season or datetime.now().year

    # Weather
    wx = weather_factor(weather_temp, weather_wind, venue)

    # Rest/fatigue
    home_rest = rest_fatigue_factor(home_team_id, game_date, yr)
    away_rest = rest_fatigue_factor(away_team_id, game_date, yr)

    # Pitcher rest
    home_sp_rest = pitcher_rest_factor(home_pitcher_id, game_date, yr) if home_pitcher_id else 1.0
    away_sp_rest = pitcher_rest_factor(away_pitcher_id, game_date, yr) if away_pitcher_id else 1.0

    # Lineup strength
    home_lineup_str = lineup_strength_factor(home_lineup)
    away_lineup_str = lineup_strength_factor(away_lineup)

    # Platoon
    home_platoon = platoon_factor(home_lineup, away_pitcher_throws)
    away_platoon = platoon_factor(away_lineup, home_pitcher_throws)

    # Combined: home scoring multiplier
    home_mult = wx * home_rest * away_sp_rest * home_lineup_str * home_platoon
    away_mult = wx * away_rest * home_sp_rest * away_lineup_str * away_platoon

    return {
        "weather": wx,
        "home_rest": home_rest,
        "away_rest": away_rest,
        "home_sp_rest": home_sp_rest,
        "away_sp_rest": away_sp_rest,
        "home_lineup_str": home_lineup_str,
        "away_lineup_str": away_lineup_str,
        "home_platoon": home_platoon,
        "away_platoon": away_platoon,
        "home_multiplier": round(home_mult, 4),
        "away_multiplier": round(away_mult, 4),
    }
