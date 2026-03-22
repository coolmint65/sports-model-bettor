"""
NHL and NBA venue locations, travel distance/timezone calculations,
and team-specific home court advantages.

Provides great-circle distance (Haversine) and timezone delta
computations for all NHL and NBA teams to support graduated travel
fatigue adjustments in the prediction model.
"""

import math
from typing import Any, Dict, Tuple

from app.config import settings

_mc = settings.model

# (latitude, longitude, utc_offset_hours)
# UTC offsets use standard time (not DST).
NHL_VENUES: Dict[str, Tuple[float, float, int]] = {
    "ANA": (33.8078, -117.8765, -8),   # Anaheim, Pacific
    "BOS": (42.3662, -71.0621, -5),    # Boston, Eastern
    "BUF": (42.8750, -78.8764, -5),    # Buffalo, Eastern
    "CGY": (51.0375, -114.0519, -7),   # Calgary, Mountain
    "CAR": (35.8033, -78.7220, -5),    # Carolina, Eastern
    "CHI": (41.8807, -87.6742, -6),    # Chicago, Central
    "COL": (39.7486, -105.0075, -7),   # Colorado, Mountain
    "CBJ": (39.9693, -83.0060, -5),    # Columbus, Eastern
    "DAL": (32.7905, -96.8103, -6),    # Dallas, Central
    "DET": (42.3411, -83.0554, -5),    # Detroit, Eastern
    "EDM": (53.5461, -113.4938, -7),   # Edmonton, Mountain
    "FLA": (26.1584, -80.3256, -5),    # Florida, Eastern
    "LAK": (34.0430, -118.2673, -8),   # LA Kings, Pacific
    "MIN": (44.9447, -93.1011, -6),    # Minnesota, Central
    "MTL": (45.4961, -73.5693, -5),    # Montreal, Eastern
    "NSH": (36.1592, -86.7785, -6),    # Nashville, Central
    "NJD": (40.7334, -74.1712, -5),    # New Jersey, Eastern
    "NYI": (40.6826, -73.9754, -5),    # NY Islanders (UBS Arena area), Eastern
    "NYR": (40.7505, -73.9934, -5),    # NY Rangers, Eastern
    "OTT": (45.2969, -75.9272, -5),    # Ottawa, Eastern
    "PHI": (39.9012, -75.1720, -5),    # Philadelphia, Eastern
    "PIT": (40.4393, -79.9894, -5),    # Pittsburgh, Eastern
    "SJS": (37.3328, -121.9013, -8),   # San Jose, Pacific
    "SEA": (47.6221, -122.3541, -8),   # Seattle, Pacific
    "STL": (38.6268, -90.2027, -6),    # St. Louis, Central
    "TBL": (27.9425, -82.4519, -5),    # Tampa Bay, Eastern
    "TOR": (43.6435, -79.3791, -5),    # Toronto, Eastern
    "UTA": (40.7683, -111.9011, -7),   # Utah, Mountain
    "VAN": (49.2778, -123.1089, -8),   # Vancouver, Pacific
    "VGK": (36.1029, -115.1785, -8),   # Vegas, Pacific
    "WPG": (49.8928, -97.1436, -6),    # Winnipeg, Central
    "WSH": (38.8981, -77.0209, -5),    # Washington, Eastern
}

# NBA venue locations: (latitude, longitude, utc_offset_hours)
NBA_VENUES: Dict[str, Tuple[float, float, int]] = {
    "ATL": (33.7573, -84.3963, -5),    # Atlanta Hawks, Eastern
    "BOS": (42.3662, -71.0622, -5),    # Boston Celtics, Eastern
    "BKN": (40.6826, -73.9754, -5),    # Brooklyn Nets, Eastern
    "CHA": (35.2251, -80.8392, -5),    # Charlotte Hornets, Eastern
    "CHI": (41.8807, -87.6742, -6),    # Chicago Bulls, Central
    "CLE": (41.4965, -81.6882, -5),    # Cleveland Cavaliers, Eastern
    "DAL": (32.7905, -96.8103, -6),    # Dallas Mavericks, Central
    "DEN": (39.7486, -105.0075, -7),   # Denver Nuggets, Mountain (5,280 ft altitude!)
    "DET": (42.3411, -83.0554, -5),    # Detroit Pistons, Eastern
    "GSW": (37.7680, -122.3877, -8),   # Golden State Warriors, Pacific
    "HOU": (29.7508, -95.3621, -6),    # Houston Rockets, Central
    "IND": (39.7640, -86.1555, -5),    # Indiana Pacers, Eastern
    "LAC": (34.0430, -118.2673, -8),   # LA Clippers, Pacific
    "LAL": (34.0430, -118.2673, -8),   # LA Lakers, Pacific
    "MEM": (35.1382, -90.0506, -6),    # Memphis Grizzlies, Central
    "MIA": (25.7814, -80.1870, -5),    # Miami Heat, Eastern
    "MIL": (43.0451, -87.9174, -6),    # Milwaukee Bucks, Central
    "MIN": (44.9795, -93.2761, -6),    # Minnesota Timberwolves, Central
    "NOP": (29.9490, -90.0821, -6),    # New Orleans Pelicans, Central
    "NYK": (40.7505, -73.9934, -5),    # New York Knicks, Eastern
    "OKC": (35.4634, -97.5151, -6),    # Oklahoma City Thunder, Central
    "ORL": (28.5392, -81.3839, -5),    # Orlando Magic, Eastern
    "PHI": (39.9012, -75.1720, -5),    # Philadelphia 76ers, Eastern
    "PHX": (33.4457, -112.0712, -7),   # Phoenix Suns, Mountain (no DST)
    "POR": (45.5316, -122.6668, -8),   # Portland Trail Blazers, Pacific
    "SAC": (38.5802, -121.4997, -8),   # Sacramento Kings, Pacific
    "SAS": (29.4271, -98.4375, -6),    # San Antonio Spurs, Central
    "TOR": (43.6435, -79.3791, -5),    # Toronto Raptors, Eastern
    "UTA": (40.7683, -111.9011, -7),   # Utah Jazz, Mountain (4,226 ft altitude)
    "WAS": (38.8981, -77.0209, -5),    # Washington Wizards, Eastern
}

# Team-specific home court advantages (points added to home team xP).
# Based on historical home/away splits (2019-2025 averages).
# Key factors: altitude (DEN, UTA), crowd intensity, arena acoustics,
# travel burden on visitors. Default is 2.5 if team not listed.
NBA_HOME_COURT_ADVANTAGES: Dict[str, float] = {
    "DEN": 4.5,   # Altitude (5,280 ft) is the biggest HCA in the NBA
    "UTA": 3.8,   # Altitude (4,226 ft) + passionate crowd
    "MIA": 3.5,   # Heat culture, intense crowd
    "BOS": 3.3,   # Historic arena, passionate fanbase
    "GSW": 3.2,   # Chase Center, loud crowds
    "PHX": 3.0,   # Desert heat + travel burden for visitors
    "MIL": 3.0,   # Fiserv Forum, Bucks culture
    "CLE": 2.8,
    "NYK": 2.8,   # MSG energy
    "MEM": 2.7,
    "MIN": 2.7,
    "OKC": 2.7,
    "DAL": 2.5,
    "IND": 2.5,
    "PHI": 2.5,
    "SAS": 2.5,
    "TOR": 2.5,
    "LAL": 2.3,
    "LAC": 2.3,
    "CHI": 2.3,
    "HOU": 2.3,
    "POR": 2.5,
    "SAC": 2.5,
    "NOP": 2.3,
    "ATL": 2.3,
    "DET": 2.0,
    "ORL": 2.0,
    "CHA": 2.0,
    "WAS": 2.0,
    "BKN": 2.0,
}

# Teams at significant altitude (elevation in feet) — visitors lose ~2%
# VO2max per 1000ft above 3000ft, measurably impacting 2nd half performance.
NBA_ALTITUDE_TEAMS: Dict[str, int] = {
    "DEN": 5280,
    "UTA": 4226,
}

# Pacific timezone teams — used to detect west coast road trips
NBA_PACIFIC_TEAMS = {"GSW", "LAL", "LAC", "SAC", "POR", "PHX"}


# Earth radius in miles (mean)
_EARTH_RADIUS_MI = 3958.8


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance in miles using the Haversine formula."""
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return _EARTH_RADIUS_MI * c


def calc_travel_distance(away_abbr: str, home_abbr: str, sport: str = "nhl") -> float:
    """Compute great-circle distance in miles between two venues.

    Returns 0.0 if either abbreviation is unknown.
    """
    venues = NBA_VENUES if sport == "nba" else NHL_VENUES
    away_venue = venues.get(away_abbr)
    home_venue = venues.get(home_abbr)
    if not away_venue or not home_venue:
        return 0.0
    return round(_haversine(away_venue[0], away_venue[1], home_venue[0], home_venue[1]), 1)


def calc_timezone_delta(away_abbr: str, home_abbr: str, sport: str = "nhl") -> int:
    """Return timezone difference in hours (positive = away team traveling east).

    A positive value means the away team's body clock is behind the local time
    (e.g., a Pacific team playing in the Eastern timezone gets +3).
    """
    venues = NBA_VENUES if sport == "nba" else NHL_VENUES
    away_venue = venues.get(away_abbr)
    home_venue = venues.get(home_abbr)
    if not away_venue or not home_venue:
        return 0
    # home_tz - away_tz: positive when home is further east
    return home_venue[2] - away_venue[2]


def get_travel_context(away_abbr: str, home_abbr: str, sport: str = "nhl") -> Dict[str, Any]:
    """Build a travel context dict for the away team visiting the home venue.

    Returns:
        dict with distance_miles, timezone_delta, is_cross_country,
        is_timezone_mismatch, and fatigue_score (0-1 normalized, 1 = worst).
    """
    distance = calc_travel_distance(away_abbr, home_abbr, sport)
    tz_delta = calc_timezone_delta(away_abbr, home_abbr, sport)

    is_cross_country = distance > 1500.0
    is_timezone_mismatch = abs(tz_delta) >= 2

    # No fatigue penalty for short hops
    min_dist = _mc.travel_fatigue_min_distance
    effective_distance = max(0.0, distance - min_dist)

    # fatigue_score: blend of normalized distance and timezone shift
    fatigue_score = min(
        1.0,
        (effective_distance / 3000.0) * 0.6 + (abs(tz_delta) / 3.0) * 0.4,
    )

    result: Dict[str, Any] = {
        "distance_miles": distance,
        "timezone_delta": tz_delta,
        "is_cross_country": is_cross_country,
        "is_timezone_mismatch": is_timezone_mismatch,
        "fatigue_score": round(fatigue_score, 4),
    }

    # NBA-specific: altitude context
    if sport == "nba":
        altitude = NBA_ALTITUDE_TEAMS.get(home_abbr, 0)
        result["home_altitude_ft"] = altitude
        result["is_altitude_game"] = altitude >= 4000

    return result


def get_nba_home_court_advantage(home_abbr: str) -> float:
    """Return team-specific home court advantage in points.

    Falls back to the default from NBAModelConfig if the team is not
    in the lookup table.
    """
    return NBA_HOME_COURT_ADVANTAGES.get(
        home_abbr, settings.nba_model.home_court_advantage
    )


def get_nba_travel_context(away_abbr: str, home_abbr: str) -> Dict[str, Any]:
    """Build NBA-specific travel context including altitude and west coast info."""
    ctx = get_travel_context(away_abbr, home_abbr, sport="nba")

    # Add west coast trip tracking
    ctx["is_away_pacific"] = away_abbr in NBA_PACIFIC_TEAMS
    ctx["is_home_pacific"] = home_abbr in NBA_PACIFIC_TEAMS

    return ctx
