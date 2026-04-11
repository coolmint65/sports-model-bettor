"""
Granular NHL prediction factors computed from the nhl_games table.

Six factors:
  1. Schedule Fatigue Score
  2. Travel Distance
  3. Goalie Workload
  4. PP/PK L10 Trends (Special Teams)
  5. Penalty Minutes Tendency
  6. Shooting Percentage + Regression

Every function accepts (team_id, game_date) so it works for both live
predictions and historical backtesting.

CLI:
    python -m engine.nhl_granular --team TBL --date 2026-04-12
"""

import argparse
import json
import logging
import math
from datetime import datetime, timedelta

try:
    from .nhl_db import get_conn, get_nhl_team_by_abbr, get_team_games_in_range
except ImportError:
    from engine.nhl_db import get_conn, get_nhl_team_by_abbr, get_team_games_in_range

logger = logging.getLogger(__name__)


def _resolve_team_id(team) -> int | None:
    """Accept team_id (int) or abbreviation (str) and return the numeric ID."""
    if isinstance(team, int):
        return team
    if isinstance(team, str):
        t = get_nhl_team_by_abbr(team)
        return t["id"] if t else None
    return None


# ── Constants ───────────────────────────────────────────────

LEAGUE_AVG_SHOOTING_PCT = 0.10  # ~10 % historical average

# NHL arena coordinates (lat, lon)
NHL_VENUES: dict[str, tuple[float, float]] = {
    "ANA": (33.8078, -117.8765),
    "BOS": (42.3662, -71.0621),
    "BUF": (42.8750, -78.8764),
    "CGY": (51.0375, -114.0519),
    "CAR": (35.8033, -78.7220),
    "CHI": (41.8807, -87.6742),
    "COL": (39.7487, -105.0077),
    "CBJ": (39.9691, -83.0060),
    "DAL": (32.7904, -96.8103),
    "DET": (42.3411, -83.0552),
    "EDM": (53.5461, -113.4938),
    "FLA": (26.1584, -80.3256),
    "LAK": (34.0430, -118.2673),
    "MIN": (44.9448, -93.1010),
    "MTL": (45.4960, -73.5693),
    "NSH": (36.1592, -86.7785),
    "NJD": (40.7335, -74.1712),
    "NYI": (40.6826, -73.9754),
    "NYR": (40.7505, -73.9934),
    "OTT": (45.2969, -75.9269),
    "PHI": (39.9012, -75.1720),
    "PIT": (40.4396, -79.9892),
    "SJS": (37.3328, -121.9013),
    "SEA": (47.6206, -122.3540),
    "STL": (38.6267, -90.2025),
    "TBL": (27.9426, -82.4520),
    "TOR": (43.6435, -79.3791),
    "UTA": (40.7683, -111.9011),
    "VAN": (49.2778, -123.1089),
    "VGK": (36.1029, -115.1784),
    "WPG": (49.8928, -97.1436),
    "WSH": (38.8981, -77.0209),
}

CROSS_COUNTRY_KM = 3000  # A single leg >= this counts as cross-country


# ── Helpers ─────────────────────────────────────────────────


def _parse_date(d: str) -> datetime:
    """Parse an ISO date string to a datetime (date only)."""
    return datetime.strptime(d[:10], "%Y-%m-%d")


def _team_venue_abbr(game: dict, team_id: int) -> str:
    """Return the arena abbreviation where *team_id* played this game.

    If team was home  -> home_abbr (that venue).
    If team was away  -> home_abbr (the opponent's venue where they traveled).
    """
    return game["home_abbr"]


def _is_away(game: dict, team_id: int) -> bool:
    return game["away_team_id"] == team_id


def _team_goals(game: dict, team_id: int) -> int | None:
    if game["home_team_id"] == team_id:
        return game.get("home_score")
    return game.get("away_score")


def _team_shots(game: dict, team_id: int) -> int | None:
    if game["home_team_id"] == team_id:
        return game.get("home_shots")
    return game.get("away_shots")


def _opp_shots(game: dict, team_id: int) -> int | None:
    """Shots that this team's goalie faced (opponent's shots)."""
    if game["home_team_id"] == team_id:
        return game.get("away_shots")
    return game.get("home_shots")


def _team_pp(game: dict, team_id: int) -> tuple[int | None, int | None]:
    """Return (pp_goals, pp_opps) for team_id in this game."""
    if game["home_team_id"] == team_id:
        return game.get("home_pp_goals"), game.get("home_pp_opps")
    return game.get("away_pp_goals"), game.get("away_pp_opps")


def _opp_pp(game: dict, team_id: int) -> tuple[int | None, int | None]:
    """Return (pp_goals, pp_opps) for the opponent in this game."""
    if game["home_team_id"] == team_id:
        return game.get("away_pp_goals"), game.get("away_pp_opps")
    return game.get("home_pp_goals"), game.get("home_pp_opps")


def _team_goalie_id(game: dict, team_id: int) -> int | None:
    if game["home_team_id"] == team_id:
        return game.get("home_goalie_id")
    return game.get("away_goalie_id")


# ── Factor 1: Schedule Fatigue ──────────────────────────────


def compute_schedule_fatigue(team, game_date: str) -> dict:
    """Compute fatigue based on games in last 7 days + travel.

    Args:
        team: team_id (int) or abbreviation (str)
    """
    team_id = _resolve_team_id(team)
    if team_id is None:
        return {"fatigue_score": 1.0, "fatigue_label": "light", "games_in_7_days": 0,
                "games_in_4_days": 0, "days_since_last_game": None, "is_road_trip": False, "road_trip_games": 0}
    gd = _parse_date(game_date)
    start_7 = (gd - timedelta(days=7)).strftime("%Y-%m-%d")
    start_4 = (gd - timedelta(days=4)).strftime("%Y-%m-%d")
    end = (gd - timedelta(days=1)).strftime("%Y-%m-%d")

    games_7d = get_team_games_in_range(team_id, start_7, end)
    games_4d = [g for g in games_7d if g["date"] >= start_4]

    # Days since last game
    days_since_last: int | None = None
    if games_7d:
        last_date = _parse_date(games_7d[-1]["date"])
        days_since_last = (gd - last_date).days

    # Road trip detection: consecutive away games leading into this date
    # Walk backwards from the most recent game
    road_trip_games = 0
    is_road_trip = False
    for g in reversed(games_7d):
        if _is_away(g, team_id):
            road_trip_games += 1
        else:
            break
    if road_trip_games >= 2:
        is_road_trip = True

    # Compute fatigue score (1.0 = fully rested, 0.7 = exhausted)
    score = 1.0

    n7 = len(games_7d)
    n4 = len(games_4d)

    # Games in 7 days penalty
    if n7 >= 4:
        score -= 0.12
    elif n7 >= 3:
        score -= 0.06
    elif n7 >= 2:
        score -= 0.02

    # Games in 4 days penalty (additional compression)
    if n4 >= 3:
        score -= 0.10
    elif n4 >= 2:
        score -= 0.04

    # Back-to-back (played yesterday)
    if days_since_last == 1:
        score -= 0.05

    # Road trip penalty
    if road_trip_games >= 4:
        score -= 0.06
    elif road_trip_games >= 3:
        score -= 0.04
    elif road_trip_games >= 2:
        score -= 0.02

    # Rest bonus: 3+ days off
    if days_since_last is not None and days_since_last >= 3:
        score += 0.02

    # Clamp
    score = max(0.70, min(1.0, score))

    # Label
    if score >= 0.95:
        label = "light"
    elif score >= 0.88:
        label = "moderate"
    elif score >= 0.80:
        label = "heavy"
    else:
        label = "extreme"

    return {
        "games_in_7_days": n7,
        "games_in_4_days": n4,
        "days_since_last_game": days_since_last,
        "is_road_trip": is_road_trip,
        "road_trip_games": road_trip_games,
        "fatigue_score": round(score, 4),
        "fatigue_label": label,
    }


# ── Factor 2: Travel Distance ──────────────────────────────


def compute_travel_distance(from_abbr: str, to_abbr: str) -> float:
    """Haversine distance in km between two NHL venues.

    Returns 0.0 if either abbreviation is unknown.
    """
    c1 = NHL_VENUES.get(from_abbr)
    c2 = NHL_VENUES.get(to_abbr)
    if not c1 or not c2:
        return 0.0

    lat1, lon1 = math.radians(c1[0]), math.radians(c1[1])
    lat2, lon2 = math.radians(c2[0]), math.radians(c2[1])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    earth_radius_km = 6371.0
    return round(c * earth_radius_km, 1)


def get_recent_travel(team, game_date: str) -> dict:
    """Compute total travel distance in last 7 days.

    Reconstructs the team's travel itinerary by looking at the arenas
    of consecutive games and summing haversine distances between them.

    Returns:
        {
            "total_km": float,
            "legs": int,
            "cross_country": bool,  # any single leg >= 3000 km
        }
    """
    team_id = _resolve_team_id(team)
    if team_id is None:
        return {"total_km": 0.0, "legs": 0, "cross_country": False}
    gd = _parse_date(game_date)
    start = (gd - timedelta(days=7)).strftime("%Y-%m-%d")
    end = (gd - timedelta(days=1)).strftime("%Y-%m-%d")

    games = get_team_games_in_range(team_id, start, end)

    if not games:
        return {"total_km": 0.0, "legs": 0, "cross_country": False}

    # Build venue sequence: for each game, the arena is the home team's venue
    venues: list[str] = []
    for g in games:
        venues.append(g["home_abbr"])

    total_km = 0.0
    legs = 0
    cross_country = False

    for i in range(1, len(venues)):
        dist = compute_travel_distance(venues[i - 1], venues[i])
        if dist > 0:
            total_km += dist
            legs += 1
            if dist >= CROSS_COUNTRY_KM:
                cross_country = True

    return {
        "total_km": round(total_km, 1),
        "legs": legs,
        "cross_country": cross_country,
    }


# ── Factor 3: Goalie Workload ──────────────────────────────


def compute_goalie_workload(team_or_abbr, game_date: str, goalie_name: str | None = None) -> dict:
    """Compute recent goalie workload from nhl_games.

    Args:
        team_or_abbr: team abbreviation (str) or team_id (int)
        game_date: ISO date string
        goalie_name: optional goalie name; if omitted, uses the most recent starter

    Returns:
        {
            "starts_7d": int,
            "starts_14d": int,
            "avg_shots_recent": float or None,
            "workload_factor": float,   # 1.0 = fresh, 0.85 = heavy workload
            "is_backup": bool,
        }
    """
    neutral = {
        "starts_7d": 0,
        "starts_14d": 0,
        "avg_shots_recent": None,
        "workload_factor": 1.0,
        "is_backup": False,
    }

    team_id = _resolve_team_id(team_or_abbr)
    if team_id is None:
        return neutral

    conn = get_conn()

    goalie_id = None
    if goalie_name:
        # Try exact name match first, then LIKE for partial
        row = conn.execute(
            "SELECT id FROM nhl_players WHERE name = ? AND team_id = ? AND position = 'G'",
            (goalie_name, team_id),
        ).fetchone()
        if not row:
            row = conn.execute(
                "SELECT id FROM nhl_players WHERE name LIKE ? AND team_id = ? AND position = 'G'",
                (f"%{goalie_name}%", team_id),
            ).fetchone()
        if row:
            goalie_id = row["id"]

    if goalie_id is None:
        # Find the most recent starter for this team
        gd = _parse_date(game_date)
        recent_end = (gd - timedelta(days=1)).strftime("%Y-%m-%d")
        recent_start = (gd - timedelta(days=30)).strftime("%Y-%m-%d")
        row = conn.execute("""
            SELECT CASE WHEN home_team_id = ? THEN home_goalie_id ELSE away_goalie_id END AS gid
            FROM nhl_games
            WHERE (home_team_id = ? OR away_team_id = ?) AND status = 'final'
              AND date >= ? AND date <= ?
            ORDER BY date DESC LIMIT 1
        """, (team_id, team_id, team_id, recent_start, recent_end)).fetchone()
        if row and row["gid"]:
            goalie_id = row["gid"]

    if goalie_id is None:
        return neutral

    goalie_id = row["id"]
    gd = _parse_date(game_date)

    start_14 = (gd - timedelta(days=14)).strftime("%Y-%m-%d")
    start_7 = (gd - timedelta(days=7)).strftime("%Y-%m-%d")
    end = (gd - timedelta(days=1)).strftime("%Y-%m-%d")

    # All starts in 14-day window
    games_14d = conn.execute("""
        SELECT g.date, g.home_team_id, g.away_team_id,
               g.home_shots, g.away_shots,
               g.home_goalie_id, g.away_goalie_id
        FROM nhl_games g
        WHERE (g.home_goalie_id = ? OR g.away_goalie_id = ?)
          AND g.date >= ? AND g.date <= ?
          AND g.status = 'final'
        ORDER BY g.date ASC
    """, (goalie_id, goalie_id, start_14, end)).fetchall()

    starts_14d = len(games_14d)
    starts_7d = sum(1 for g in games_14d if g["date"] >= start_7)

    # Average shots faced in these starts
    shots_faced: list[int] = []
    for g in games_14d:
        if g["home_goalie_id"] == goalie_id and g["away_shots"] is not None:
            shots_faced.append(g["away_shots"])
        elif g["away_goalie_id"] == goalie_id and g["home_shots"] is not None:
            shots_faced.append(g["home_shots"])

    avg_shots = round(sum(shots_faced) / len(shots_faced), 1) if shots_faced else None

    # Is backup?  Check how many total team games in last 14 days
    team_games_14d = get_team_games_in_range(team_id, start_14, end)
    team_game_count = len(team_games_14d)
    is_backup = False
    if team_game_count >= 4 and starts_14d <= team_game_count * 0.35:
        is_backup = True

    # Workload factor: 1.0 = fresh, down to 0.85 for heavy usage
    factor = 1.0
    if starts_7d >= 4:
        factor -= 0.12
    elif starts_7d >= 3:
        factor -= 0.07
    elif starts_7d >= 2:
        factor -= 0.03

    # Extra penalty for heavy shot volume recently
    if avg_shots is not None and avg_shots > 35:
        factor -= 0.02

    factor = max(0.85, min(1.0, factor))

    return {
        "starts_7d": starts_7d,
        "starts_14d": starts_14d,
        "avg_shots_recent": avg_shots,
        "workload_factor": round(factor, 4),
        "is_backup": is_backup,
    }


# ── Factor 4: PP/PK L10 Trends ─────────────────────────────


def compute_special_teams_trend(team, game_date: str, window: int = 10) -> dict:
    """Compute recent PP% and PK% from last N games.

    Args:
        team: team_id (int) or abbreviation (str)
    """
    neutral = {
        "pp_l10": 0.20,
        "pp_season": 0.20,
        "pp_trend": "normal",
        "pp_trend_adj": 0.0,
        "pk_l10": 0.80,
        "pk_season": 0.80,
        "pk_trend": "normal",
    }
    team_id = _resolve_team_id(team)
    if team_id is None:
        return neutral

    conn = get_conn()

    # All finished games for this team before game_date, this season
    # We derive season from the game_date (Sept+ = current year start)
    gd = _parse_date(game_date)
    season_start = gd.year if gd.month >= 9 else gd.year - 1
    season = int(f"{season_start}{season_start + 1}")

    # Fetch all games this season up to (not including) game_date
    season_start_date = f"{season_start}-09-01"
    end_date = (gd - timedelta(days=1)).strftime("%Y-%m-%d")

    all_games = get_team_games_in_range(team_id, season_start_date, end_date)
    if not all_games:
        return neutral

    # Accumulate PP and PK data
    def _accum(games: list[dict]) -> tuple[int, int, int, int]:
        """Return (pp_goals, pp_opps, pk_goals_against, pk_opps_against)."""
        pp_g, pp_o, pk_ga, pk_oa = 0, 0, 0, 0
        for g in games:
            t_ppg, t_ppo = _team_pp(g, team_id)
            o_ppg, o_ppo = _opp_pp(g, team_id)
            if t_ppg is not None and t_ppo is not None:
                pp_g += t_ppg
                pp_o += t_ppo
            if o_ppg is not None and o_ppo is not None:
                pk_ga += o_ppg
                pk_oa += o_ppo
        return pp_g, pp_o, pk_ga, pk_oa

    # Season totals
    s_ppg, s_ppo, s_pkga, s_pkoa = _accum(all_games)
    pp_season = s_ppg / s_ppo if s_ppo > 0 else 0.20
    pk_season = 1.0 - (s_pkga / s_pkoa) if s_pkoa > 0 else 0.80

    # Last N games
    recent = all_games[-window:]
    r_ppg, r_ppo, r_pkga, r_pkoa = _accum(recent)
    pp_l10 = r_ppg / r_ppo if r_ppo > 0 else pp_season
    pk_l10 = 1.0 - (r_pkga / r_pkoa) if r_pkoa > 0 else pk_season

    # Trends
    if pp_l10 >= 0.25:
        pp_trend = "hot"
    elif pp_l10 < 0.15:
        pp_trend = "cold"
    else:
        pp_trend = "normal"

    if pk_l10 >= 0.85:
        pk_trend = "hot"
    elif pk_l10 < 0.75:
        pk_trend = "cold"
    else:
        pk_trend = "normal"

    # pp_trend_adj: how much to adjust xG based on PP trending up/down
    # Positive = PP is hot (expect more goals), negative = cold
    pp_trend_adj = (pp_l10 - pp_season) * 0.5 if pp_season > 0 else 0.0
    pp_trend_adj = max(-0.10, min(0.10, pp_trend_adj))

    return {
        "pp_l10": round(pp_l10, 4),
        "pp_season": round(pp_season, 4),
        "pp_trend": pp_trend,
        "pp_trend_adj": round(pp_trend_adj, 4),
        "pk_l10": round(pk_l10, 4),
        "pk_season": round(pk_season, 4),
        "pk_trend": pk_trend,
    }


# ── Factor 5: Penalty Tendency ──────────────────────────────


def compute_penalty_tendency(team, game_date: str) -> dict:
    """How undisciplined is this team?

    Args:
        team: team_id (int) or abbreviation (str)
    """
    neutral = {
        "pim_per_game": 8.0,
        "pp_chances_given": 3.0,
        "discipline_rating": "average",
    }
    team_id = _resolve_team_id(team)
    if team_id is None:
        return neutral

    gd = _parse_date(game_date)
    season_start = gd.year if gd.month >= 9 else gd.year - 1
    season_start_date = f"{season_start}-09-01"
    end_date = (gd - timedelta(days=1)).strftime("%Y-%m-%d")

    all_games = get_team_games_in_range(team_id, season_start_date, end_date)
    if not all_games:
        return neutral

    total_opp_pp_opps = 0
    counted = 0
    for g in all_games:
        _, opp_ppo = _opp_pp(g, team_id)
        if opp_ppo is not None:
            total_opp_pp_opps += opp_ppo
            counted += 1

    if counted == 0:
        return neutral

    avg_pp_given = total_opp_pp_opps / counted
    # Estimate PIM: each PP opp is roughly a 2-minute minor
    avg_pim = avg_pp_given * 2.0

    # Rating thresholds based on average PP chances given per game
    if avg_pp_given <= 2.5:
        rating = "elite"
    elif avg_pp_given <= 3.0:
        rating = "good"
    elif avg_pp_given <= 3.5:
        rating = "average"
    elif avg_pp_given <= 4.2:
        rating = "poor"
    else:
        rating = "undisciplined"

    return {
        "pim_per_game": round(avg_pim, 1),
        "pp_chances_given": round(avg_pp_given, 2),
        "discipline_rating": rating,
    }


# ── Factor 6: Shooting % + Regression ──────────────────────


def compute_shooting_regression(team, game_date: str) -> dict:
    """Detect unsustainable shooting percentages.

    Args:
        team: team_id (int) or abbreviation (str)
    """
    neutral = {
        "shooting_pct": LEAGUE_AVG_SHOOTING_PCT,
        "league_avg": LEAGUE_AVG_SHOOTING_PCT,
        "regression_factor": 1.0,
        "save_pct_regression": 1.0,
    }
    team_id = _resolve_team_id(team)
    if team_id is None:
        return neutral

    gd = _parse_date(game_date)
    season_start = gd.year if gd.month >= 9 else gd.year - 1
    season_start_date = f"{season_start}-09-01"
    end_date = (gd - timedelta(days=1)).strftime("%Y-%m-%d")

    all_games = get_team_games_in_range(team_id, season_start_date, end_date)
    if not all_games:
        return neutral

    total_goals = 0
    total_shots = 0
    total_opp_goals = 0
    total_opp_shots = 0

    for g in all_games:
        goals = _team_goals(g, team_id)
        shots = _team_shots(g, team_id)
        opp_goals = _team_goals(g, g["away_team_id"] if g["home_team_id"] == team_id else g["home_team_id"])
        opp_shots = _opp_shots(g, team_id)

        if goals is not None and shots is not None and shots > 0:
            total_goals += goals
            total_shots += shots
        if opp_goals is not None and opp_shots is not None and opp_shots > 0:
            total_opp_goals += opp_goals
            total_opp_shots += opp_shots

    if total_shots == 0:
        return neutral

    shooting_pct = total_goals / total_shots

    # Compute league average dynamically from all teams this season
    conn = get_conn()
    league_row = conn.execute("""
        SELECT COALESCE(SUM(home_score) + SUM(away_score), 0) AS lg_goals,
               COALESCE(SUM(home_shots) + SUM(away_shots), 0) AS lg_shots
        FROM nhl_games
        WHERE season = ? AND status = 'final'
          AND home_shots IS NOT NULL AND away_shots IS NOT NULL
          AND date < ?
    """, (int(f"{season_start}{season_start + 1}"), game_date)).fetchone()

    if league_row and league_row["lg_shots"] and league_row["lg_shots"] > 0:
        league_avg = league_row["lg_goals"] / league_row["lg_shots"]
    else:
        league_avg = LEAGUE_AVG_SHOOTING_PCT

    # Regression factor:
    # If team shoots at 12%, league avg 10%, they are 2% above.
    # We pull them ~50% toward the mean as a regression estimate.
    deviation = shooting_pct - league_avg
    regression_strength = 0.5  # How much to regress (50%)
    regressed_pct = shooting_pct - (deviation * regression_strength)
    regression_factor = regressed_pct / shooting_pct if shooting_pct > 0 else 1.0

    # Save percentage regression (opponent goalies facing this team)
    # save_pct = 1 - (opp_goals / opp_shots)
    if total_opp_shots > 0:
        opp_save_pct = 1.0 - (total_opp_goals / total_opp_shots)
        league_save_pct = 1.0 - league_avg
        opp_dev = opp_save_pct - league_save_pct
        regressed_save = opp_save_pct - (opp_dev * regression_strength)
        # Factor > 1.0 means opponent goalies have been unusually good (they'll
        # regress toward worse), so this team should score more.
        save_pct_regression = regressed_save / opp_save_pct if opp_save_pct > 0 else 1.0
    else:
        save_pct_regression = 1.0

    return {
        "shooting_pct": round(shooting_pct, 4),
        "league_avg": round(league_avg, 4),
        "regression_factor": round(regression_factor, 4),
        "save_pct_regression": round(save_pct_regression, 4),
    }


# ── Aggregate convenience ──────────────────────────────────


def compute_all_factors(team, game_date: str,
                        goalie_name: str | None = None) -> dict:
    """Compute all six granular factors for a team on a date.

    Args:
        team: team abbreviation (str) or team_id (int)
    """
    result: dict = {}
    result["fatigue"] = compute_schedule_fatigue(team, game_date)
    result["travel"] = get_recent_travel(team, game_date)
    result["special_teams"] = compute_special_teams_trend(team, game_date)
    result["penalties"] = compute_penalty_tendency(team, game_date)
    result["shooting"] = compute_shooting_regression(team, game_date)
    result["goalie"] = compute_goalie_workload(team, game_date, goalie_name)
    return result


# ── CLI ─────────────────────────────────────────────────────


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Compute NHL granular prediction factors for a team/date."
    )
    parser.add_argument("--team", required=True, help="Team abbreviation (e.g. TBL)")
    parser.add_argument("--date", required=True, help="Game date YYYY-MM-DD")
    parser.add_argument("--goalie", default=None, help="Starting goalie name (optional)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    team_row = get_nhl_team_by_abbr(args.team)
    if not team_row:
        print(f"ERROR: Team '{args.team}' not found in database.")
        raise SystemExit(1)

    print(f"\n{'=' * 60}")
    print(f"  NHL Granular Factors: {team_row['name']} ({args.team})")
    print(f"  Game Date: {args.date}")
    print(f"{'=' * 60}\n")

    factors = compute_all_factors(args.team, args.date, args.goalie)

    for section, data in factors.items():
        print(f"--- {section.upper()} ---")
        print(json.dumps(data, indent=2))
        print()


if __name__ == "__main__":
    _cli()
