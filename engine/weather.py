"""
Weather data for MLB totals adjustments.

Uses Open-Meteo API (free, no key needed) to fetch current weather
conditions at game venues and compute run adjustments.
"""

import logging
from datetime import datetime

import requests

logger = logging.getLogger(__name__)


# ── Stadium coordinates ─────────────────────────────────────

STADIUM_COORDS = {
    "Yankee Stadium": (40.8296, -73.9262),
    "Citi Field": (40.7571, -73.8458),
    "Fenway Park": (42.3467, -71.0972),
    "Wrigley Field": (41.9484, -87.6553),
    "Dodger Stadium": (34.0739, -118.2400),
    "Oracle Park": (37.7786, -122.3893),
    "Petco Park": (32.7076, -117.1570),
    "Citizens Bank Park": (39.9061, -75.1665),
    "Nationals Park": (38.8730, -77.0074),
    "Truist Park": (33.8908, -84.4678),
    "Busch Stadium": (38.6226, -90.1928),
    "Great American Ball Park": (39.0974, -84.5082),
    "PNC Park": (40.4468, -80.0057),
    "Progressive Field": (41.4962, -81.6852),
    "Comerica Park": (42.3390, -83.0485),
    "Target Field": (44.9818, -93.2775),
    "Kauffman Stadium": (39.0517, -94.4803),
    "Guaranteed Rate Field": (41.8299, -87.6338),
    "Camden Yards": (39.2838, -76.6216),
    "Tropicana Field": (27.7682, -82.6534),
    "Minute Maid Park": (29.7572, -95.3555),
    "Globe Life Field": (32.7512, -97.0832),
    "Oakland Coliseum": (37.7516, -122.2005),
    "Angel Stadium": (33.8003, -117.8827),
    "T-Mobile Park": (47.5914, -122.3325),
    "Coors Field": (39.7559, -104.9942),
    "Chase Field": (33.4455, -112.0667),
    "loanDepot park": (25.7781, -80.2196),
    "Rogers Centre": (43.6414, -79.3894),
    "American Family Field": (43.0280, -87.9712),
}

DOMED_STADIUMS = {
    "Tropicana Field",
    "Minute Maid Park",
    "Globe Life Field",
    "loanDepot park",
    "Rogers Centre",
    "T-Mobile Park",
    "American Family Field",
    "Chase Field",
}


# ── Weather cache (avoid rate limiting) ────────────────────
import time as _time
_weather_cache: dict = {}  # key: "lat,lon" -> {"data": ..., "time": float}
_WEATHER_CACHE_TTL = 3600  # 1 hour — weather doesn't change that fast


# ── API fetch ───────────────────────────────────────────────

def get_game_weather(lat: float, lon: float, game_time: datetime | None = None) -> dict | None:
    """
    Fetch current weather conditions from Open-Meteo.

    Returns dict with temperature (°F), wind_speed (mph),
    wind_direction (degrees), humidity (%), precipitation (mm).
    Returns None on failure.
    """
    # Check cache first
    cache_key = f"{lat:.2f},{lon:.2f}"
    if cache_key in _weather_cache:
        entry = _weather_cache[cache_key]
        if _time.time() - entry["time"] < _WEATHER_CACHE_TTL:
            return entry["data"]

    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&current=temperature_2m,wind_speed_10m,wind_direction_10m,"
        f"relative_humidity_2m,precipitation"
        f"&temperature_unit=fahrenheit&wind_speed_unit=mph&timezone=auto"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("Failed to fetch weather at (%.4f, %.4f): %s", lat, lon, e)
        return None

    current = data.get("current", {})
    if not current:
        return None

    result = {
        "temperature": current.get("temperature_2m"),
        "wind_speed": current.get("wind_speed_10m"),
        "wind_direction": current.get("wind_direction_10m"),
        "humidity": current.get("relative_humidity_2m"),
        "precipitation": current.get("precipitation"),
    }
    _weather_cache[cache_key] = {"data": result, "time": _time.time()}
    return result


def compute_weather_adjustment(weather: dict | None, venue: str | None = None) -> float:
    """
    Compute a run multiplier based on weather conditions.

    - Hot (>85°F): +3% runs (balls carry further)
    - Cold (<50°F): -3% runs
    - High wind (>15 mph): +2-5% depending on direction
    - Rain/precipitation: -2% runs

    Returns multiplier (1.0 = neutral). Capped at ±8%.
    Always returns 1.0 for domed stadiums.
    """
    if venue and venue in DOMED_STADIUMS:
        return 1.0

    if not weather:
        return 1.0

    factor = 1.0

    # Temperature adjustment
    temp = weather.get("temperature")
    if temp is not None:
        if temp > 95:
            factor *= 1.04
        elif temp > 85:
            factor *= 1.03
        elif temp > 75:
            factor *= 1.01
        elif temp > 60:
            factor *= 1.0   # Neutral range
        elif temp > 50:
            factor *= 0.98
        else:
            factor *= 0.97  # Cold suppresses offense

    # Wind adjustment
    wind_speed = weather.get("wind_speed")
    wind_dir = weather.get("wind_direction")
    if wind_speed is not None and wind_speed > 15:
        # Wind direction: 0=N, 90=E, 180=S, 270=W
        # "Blowing out" depends on park orientation, but a rough heuristic:
        # winds from behind home plate (toward outfield) boost runs.
        # We approximate: wind from S-SW (150-240 degrees) is generally
        # blowing out in most parks. Otherwise, cross-wind or blowing in.
        if wind_dir is not None:
            if 150 <= wind_dir <= 240:
                # Blowing out — more runs
                boost = 0.02 + (wind_speed - 15) * 0.002
                factor *= 1.0 + min(boost, 0.05)
            elif (wind_dir <= 30 or wind_dir >= 330):
                # Blowing in — fewer runs
                penalty = 0.02 + (wind_speed - 15) * 0.002
                factor *= 1.0 - min(penalty, 0.05)
            else:
                # Cross-wind — slight boost (disrupts pitchers slightly)
                factor *= 1.01
        else:
            # Unknown direction, slight boost for high wind
            factor *= 1.02

    # Precipitation adjustment
    precip = weather.get("precipitation")
    if precip is not None and precip > 0:
        factor *= 0.98  # Rain suppresses offense

    return round(max(0.92, min(1.08, factor)), 4)


def get_weather_for_venue(venue: str) -> tuple[dict | None, bool]:
    """
    Convenience: fetch weather for a known MLB venue.

    Returns (weather_dict, is_domed).
    weather_dict is None if venue is unknown or fetch fails.
    """
    is_domed = venue in DOMED_STADIUMS
    coords = STADIUM_COORDS.get(venue)
    if not coords:
        return None, is_domed

    weather = get_game_weather(coords[0], coords[1])
    return weather, is_domed
