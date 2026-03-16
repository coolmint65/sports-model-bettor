"""
NHL venue locations and travel distance/timezone calculations.

Provides great-circle distance (Haversine) and timezone delta
computations for all 32 NHL teams to support graduated travel
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


def calc_travel_distance(away_abbr: str, home_abbr: str) -> float:
    """Compute great-circle distance in miles between two NHL venues.

    Returns 0.0 if either abbreviation is unknown.
    """
    away_venue = NHL_VENUES.get(away_abbr)
    home_venue = NHL_VENUES.get(home_abbr)
    if not away_venue or not home_venue:
        return 0.0
    return round(_haversine(away_venue[0], away_venue[1], home_venue[0], home_venue[1]), 1)


def calc_timezone_delta(away_abbr: str, home_abbr: str) -> int:
    """Return timezone difference in hours (positive = away team traveling east).

    A positive value means the away team's body clock is behind the local time
    (e.g., a Pacific team playing in the Eastern timezone gets +3).
    """
    away_venue = NHL_VENUES.get(away_abbr)
    home_venue = NHL_VENUES.get(home_abbr)
    if not away_venue or not home_venue:
        return 0
    # home_tz - away_tz: positive when home is further east
    return home_venue[2] - away_venue[2]


def get_travel_context(away_abbr: str, home_abbr: str) -> Dict[str, Any]:
    """Build a travel context dict for the away team visiting the home venue.

    Returns:
        dict with distance_miles, timezone_delta, is_cross_country,
        is_timezone_mismatch, and fatigue_score (0-1 normalized, 1 = worst).
    """
    distance = calc_travel_distance(away_abbr, home_abbr)
    tz_delta = calc_timezone_delta(away_abbr, home_abbr)

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

    return {
        "distance_miles": distance,
        "timezone_delta": tz_delta,
        "is_cross_country": is_cross_country,
        "is_timezone_mismatch": is_timezone_mismatch,
        "fatigue_score": round(fatigue_score, 4),
    }
