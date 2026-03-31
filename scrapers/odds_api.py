"""
The Odds API integration for MLB odds.

Free tier: 500 requests/month (plenty for ~15 games/day).
Returns ML, O/U with juice, and RL ±1.5 with juice from
DraftKings, FanDuel, BetMGM, and other books.

Sign up at: https://the-odds-api.com/
Set your API key in data/odds_api_key.txt or as environment variable ODDS_API_KEY.
"""

import json
import logging
import os
import urllib.request
from pathlib import Path

import time

logger = logging.getLogger(__name__)

API_BASE = "https://api.the-odds-api.com/v4"
KEY_FILE = Path(__file__).resolve().parent.parent / "data" / "odds_api_key.txt"

MLB_SPORT = "baseball_mlb"
PREFERRED_BOOK = "draftkings"

# Cache odds for 10 minutes to avoid burning API credits
_odds_cache: dict | None = None
_odds_cache_time: float = 0
ODDS_CACHE_TTL = 600  # 10 minutes


def _get_api_key() -> str | None:
    """Load API key from file or environment."""
    # Try environment variable first
    key = os.environ.get("ODDS_API_KEY")
    if key:
        return key.strip()

    # Try key file
    if KEY_FILE.exists():
        return KEY_FILE.read_text().strip()

    return None


def fetch_odds() -> dict:
    """
    Fetch MLB odds from The Odds API.

    Returns dict keyed by normalized matchup:
    {
        "MIN@KC": {
            "home_ml": -168, "away_ml": 139,
            "over_under": 10.5, "over_odds": -103, "under_odds": -117,
            "home_spread_odds": -130, "away_spread_odds": 110,
            "spread": -1.5,
            "provider": "DraftKings"
        }
    }
    """
    global _odds_cache, _odds_cache_time

    # Return cached odds if fresh
    if _odds_cache and (time.time() - _odds_cache_time) < ODDS_CACHE_TTL:
        logger.debug("Odds API: returning cached odds (%d games)", len(_odds_cache))
        return _odds_cache

    api_key = _get_api_key()
    if not api_key:
        logger.info("No Odds API key found. Set ODDS_API_KEY env var or create data/odds_api_key.txt")
        return {}

    # Fetch all three markets in one call
    url = (f"{API_BASE}/sports/{MLB_SPORT}/odds/"
           f"?apiKey={api_key}"
           f"&regions=us"
           f"&markets=h2h,spreads,totals"
           f"&oddsFormat=american"
           f"&bookmakers={PREFERRED_BOOK}")

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "MLBPredictionEngine/1.0",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            # Log remaining requests
            remaining = resp.headers.get("x-requests-remaining", "?")
            logger.info("Odds API: %s requests remaining this month", remaining)
    except Exception as e:
        logger.warning("Odds API failed: %s", e)
        return {}

    if not data or not isinstance(data, list):
        return {}

    odds_map = {}

    for game in data:
        home = game.get("home_team", "")
        away = game.get("away_team", "")

        h_abbr = _team_abbr(home)
        a_abbr = _team_abbr(away)
        key = f"{a_abbr}@{h_abbr}"

        result = {"provider": "DraftKings"}

        bookmakers = game.get("bookmakers", [])
        if not bookmakers:
            continue

        book = bookmakers[0]  # First (and only since we filtered) bookmaker

        for market in book.get("markets", []):
            mkey = market.get("key", "")
            outcomes = market.get("outcomes", [])

            if mkey == "h2h":  # Moneyline
                for o in outcomes:
                    name = o.get("name", "")
                    price = o.get("price", 0)
                    if name == home:
                        result["home_ml"] = price
                    elif name == away:
                        result["away_ml"] = price

            elif mkey == "spreads":  # Run Line
                for o in outcomes:
                    name = o.get("name", "")
                    price = o.get("price", 0)
                    point = o.get("point", 0)
                    if name == home:
                        result["home_spread_odds"] = price
                        result["spread"] = point
                    elif name == away:
                        result["away_spread_odds"] = price

            elif mkey == "totals":  # Over/Under
                for o in outcomes:
                    name = o.get("name", "").lower()
                    price = o.get("price", 0)
                    point = o.get("point", 0)
                    if "over" in name:
                        result["over_odds"] = price
                        result["over_under"] = point
                    elif "under" in name:
                        result["under_odds"] = price

        if result.get("home_ml"):
            odds_map[key] = result

    logger.info("Odds API: fetched odds for %d games", len(odds_map))

    # Cache the results
    _odds_cache = odds_map
    _odds_cache_time = time.time()

    return odds_map


# Team name to abbreviation mapping
_TEAM_MAP = {
    "Arizona Diamondbacks": "AZ", "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL", "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL", "Detroit Tigers": "DET",
    "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA", "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN", "New York Mets": "NYM",
    "New York Yankees": "NYY", "Oakland Athletics": "ATH",
    "Philadelphia Phillies": "PHI", "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD", "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB", "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR", "Washington Nationals": "WSH",
}


def _team_abbr(name: str) -> str:
    return _TEAM_MAP.get(name, name)
