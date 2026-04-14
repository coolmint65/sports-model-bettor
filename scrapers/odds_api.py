"""
The Odds API integration for MLB odds.

Paid tier ($30/mo = 20K credits) supports per-event markets.
Returns full-game ML/O/U/RL plus NRFI and F5 markets from DraftKings
(first inning total, F5 ML, F5 O/U, F5 RL).

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

# Per-event inning-specific markets (paid tier only; 1 credit per market per event)
PER_EVENT_MARKETS = [
    "totals_1st_1_innings",   # NRFI / YRFI
    "h2h_1st_5_innings",      # F5 ML
    "totals_1st_5_innings",   # F5 O/U
    "spreads_1st_5_innings",  # F5 RL
]

# Bookmaker preference for per-event markets. DK posts these lines very late
# (only hours before first pitch), while FanDuel / BetMGM / Bovada post them
# early. Fall through the list so we always get a price when one is available.
PER_EVENT_BOOK_PREFERENCE = ["draftkings", "fanduel", "betmgm", "bovada"]

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


def fetch_odds(include_per_event: bool = True) -> dict:
    """
    Fetch MLB odds from The Odds API.

    Args:
        include_per_event: If True (default), also fetch NRFI / F5 markets
            via per-event endpoint. Paid tier required.

    Returns dict keyed by normalized matchup:
    {
        "MIN@KC": {
            "home_ml": -168, "away_ml": 139,
            "over_under": 10.5, "over_odds": -103, "under_odds": -117,
            "home_spread_odds": -130, "away_spread_odds": 110,
            "spread": -1.5,
            # Per-event markets (when include_per_event=True):
            "nrfi_line": 0.5, "nrfi_over_odds": 115, "nrfi_under_odds": -140,
            "f5_home_ml": -135, "f5_away_ml": 115,
            "f5_total": 5.0, "f5_over_odds": -110, "f5_under_odds": -110,
            "f5_spread": -0.5, "f5_home_spread_odds": -115,
            "f5_away_spread_odds": -105,
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

    # Fetch all three full-game markets in one call
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
    # matchup_key -> odds-api event_id (used by per-event fetch)
    event_ids: dict[str, str] = {}

    for game in data:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        event_id = game.get("id", "")

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
                        result["home_spread_point"] = point
                    elif name == away:
                        result["away_spread_odds"] = price
                        result["away_spread_point"] = point
                # Keep generic spread for backward compat
                result["spread"] = 1.5

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
            if event_id:
                event_ids[key] = event_id

    # Per-event enrichment (NRFI + F5 markets)
    if include_per_event and event_ids:
        per_event = _fetch_per_event_markets(event_ids, api_key)
        for key, markets in per_event.items():
            if key in odds_map:
                odds_map[key].update(markets)

    logger.info("Odds API: fetched odds for %d games", len(odds_map))

    # Cache the merged results
    _odds_cache = odds_map
    _odds_cache_time = time.time()

    return odds_map


def _fetch_per_event_markets(event_ids: dict[str, str], api_key: str) -> dict:
    """Fetch NRFI + F5 markets for each event. Paid tier required.

    Args:
        event_ids: matchup_key -> odds-api event_id
        api_key: Odds API key

    Returns:
        matchup_key -> dict of per-event market fields (nrfi_*, f5_*)
    """
    markets_param = ",".join(PER_EVENT_MARKETS)
    books_param = ",".join(PER_EVENT_BOOK_PREFERENCE)
    out: dict = {}

    for key, event_id in event_ids.items():
        # Pull all preferred books in one call and let the parser pick the
        # first that has data. DK posts F5/NRFI lines only in the hours
        # before first pitch; FD/MGM/Bovada post them much earlier.
        url = (f"{API_BASE}/sports/{MLB_SPORT}/events/{event_id}/odds"
               f"?apiKey={api_key}"
               f"&regions=us"
               f"&markets={markets_param}"
               f"&oddsFormat=american"
               f"&bookmakers={books_param}")
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "MLBPredictionEngine/1.0",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                event = json.loads(resp.read().decode())
        except Exception as e:
            logger.warning("Odds API per-event failed for %s (%s): %s", key, event_id, e)
            continue

        parsed = _parse_per_event(event)
        if parsed:
            out[key] = parsed

    if out:
        logger.info("Odds API: fetched per-event markets for %d games", len(out))
    return out


def _parse_per_event(event: dict) -> dict:
    """Parse per-event response into flat field dict for NRFI + F5 markets.

    Iterates bookmakers in the configured preference order and uses the
    first one that returns any of the requested markets. When DK hasn't
    posted F5/NRFI lines yet, falls through to FanDuel / BetMGM / Bovada.
    """
    home = event.get("home_team", "")
    away = event.get("away_team", "")
    bookmakers = event.get("bookmakers", [])
    if not bookmakers:
        return {}

    by_key = {b.get("key"): b for b in bookmakers}
    book = None
    for pref in PER_EVENT_BOOK_PREFERENCE:
        if pref in by_key and by_key[pref].get("markets"):
            book = by_key[pref]
            break
    if book is None:
        # No preferred book -- fall back to the first bookmaker with markets
        for b in bookmakers:
            if b.get("markets"):
                book = b
                break
    if book is None:
        return {}

    result: dict = {"per_event_provider": book.get("title") or book.get("key")}

    for market in book.get("markets", []):
        mkey = market.get("key", "")
        outcomes = market.get("outcomes", [])

        if mkey == "totals_1st_1_innings":  # NRFI/YRFI
            for o in outcomes:
                name = o.get("name", "").lower()
                price = o.get("price")
                point = o.get("point")
                if "over" in name:
                    result["nrfi_over_odds"] = price
                    result["nrfi_line"] = point
                elif "under" in name:
                    result["nrfi_under_odds"] = price

        elif mkey == "h2h_1st_5_innings":  # F5 ML
            for o in outcomes:
                name = o.get("name", "")
                price = o.get("price")
                if name == home:
                    result["f5_home_ml"] = price
                elif name == away:
                    result["f5_away_ml"] = price

        elif mkey == "totals_1st_5_innings":  # F5 O/U
            for o in outcomes:
                name = o.get("name", "").lower()
                price = o.get("price")
                point = o.get("point")
                if "over" in name:
                    result["f5_over_odds"] = price
                    result["f5_total"] = point
                elif "under" in name:
                    result["f5_under_odds"] = price

        elif mkey == "spreads_1st_5_innings":  # F5 RL
            for o in outcomes:
                name = o.get("name", "")
                price = o.get("price")
                point = o.get("point")
                if name == home:
                    result["f5_home_spread_odds"] = price
                    result["f5_home_spread_point"] = point
                elif name == away:
                    result["f5_away_spread_odds"] = price
                    result["f5_away_spread_point"] = point
            # Convenience: canonical F5 spread is the home-side magnitude
            if "f5_home_spread_point" in result:
                result["f5_spread"] = result["f5_home_spread_point"]

    return result


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
