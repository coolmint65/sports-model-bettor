"""
The Odds API integration for NBA Q1 odds.

Fetches Q1-specific markets (moneyline, spread, total) in addition to full
game markets. Q1 markets use the The Odds API "alternate" market keys:
    h2h_q1       -> Q1 moneyline
    spreads_q1   -> Q1 spread
    totals_q1    -> Q1 total

If Q1 markets are unavailable (some games don't have them posted yet),
the function still returns full-game odds so the tracker can fall back
to -110 defaults for Q1 picks.

Returns dict keyed by "AWAY@HOME" with shape:
    {
        "home_ml": -150, "away_ml": +130,
        "over_under": 225.5, "over_odds": -110, "under_odds": -110,
        "home_spread_point": -3.5, "home_spread_odds": -110,
        "away_spread_point": +3.5, "away_spread_odds": -110,
        "q1_home_ml": -140, "q1_away_ml": +120,
        "q1_spread": -1.5, "q1_spread_home_odds": -110, "q1_spread_away_odds": -110,
        "q1_total": 55.5, "q1_over_odds": -110, "q1_under_odds": -110,
        "provider": "DraftKings"
    }
"""

import json
import logging
import os
import time
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

API_BASE = "https://api.the-odds-api.com/v4"
KEY_FILE = Path(__file__).resolve().parent.parent / "data" / "odds_api_key.txt"

NBA_SPORT = "basketball_nba"
PREFERRED_BOOK = "draftkings"

# Cache for 10 minutes to preserve API credits
_odds_cache: dict | None = None
_odds_cache_time: float = 0
ODDS_CACHE_TTL = 600


def _get_api_key() -> str | None:
    key = os.environ.get("ODDS_API_KEY")
    if key:
        return key.strip()
    if KEY_FILE.exists():
        return KEY_FILE.read_text().strip()
    return None


def fetch_nba_odds() -> dict:
    """Fetch NBA odds (full-game + Q1 markets) from The Odds API.

    Returns dict keyed by "AWAY@HOME" abbreviation.
    """
    global _odds_cache, _odds_cache_time

    if _odds_cache and (time.time() - _odds_cache_time) < ODDS_CACHE_TTL:
        return _odds_cache

    api_key = _get_api_key()
    if not api_key:
        logger.info("No Odds API key found. Set ODDS_API_KEY env var or create data/odds_api_key.txt")
        return {}

    # Q1 markets are alternate markets. Fetch both groups.
    # Note: The Odds API documents these as "h2h_q1", "spreads_q1", "totals_q1".
    markets = ",".join(["h2h", "spreads", "totals", "h2h_q1", "spreads_q1", "totals_q1"])
    url = (f"{API_BASE}/sports/{NBA_SPORT}/odds/"
           f"?apiKey={api_key}"
           f"&regions=us"
           f"&markets={markets}"
           f"&oddsFormat=american"
           f"&bookmakers={PREFERRED_BOOK}")

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "NBAQ1PredictionEngine/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            remaining = resp.headers.get("x-requests-remaining", "?")
            logger.info("Odds API (NBA): %s requests remaining this month", remaining)
    except Exception as e:
        logger.warning("Odds API (NBA) failed: %s", e)
        return {}

    if not data or not isinstance(data, list):
        return {}

    odds_map: dict[str, dict] = {}

    for game in data:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        h_abbr = _team_abbr(home)
        a_abbr = _team_abbr(away)
        key = f"{a_abbr}@{h_abbr}"

        result: dict = {"provider": "DraftKings"}

        bookmakers = game.get("bookmakers", [])
        if not bookmakers:
            continue
        book = bookmakers[0]

        for market in book.get("markets", []):
            mkey = market.get("key", "")
            outcomes = market.get("outcomes", [])

            if mkey == "h2h":
                for o in outcomes:
                    name = o.get("name", "")
                    price = o.get("price", 0)
                    if name == home:
                        result["home_ml"] = price
                    elif name == away:
                        result["away_ml"] = price

            elif mkey == "spreads":
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

            elif mkey == "totals":
                for o in outcomes:
                    name = o.get("name", "").lower()
                    price = o.get("price", 0)
                    point = o.get("point", 0)
                    if "over" in name:
                        result["over_odds"] = price
                        result["over_under"] = point
                    elif "under" in name:
                        result["under_odds"] = price

            # ── Q1-specific markets ──
            elif mkey == "h2h_q1":
                for o in outcomes:
                    name = o.get("name", "")
                    price = o.get("price", 0)
                    if name == home:
                        # Model's generate_q1_picks keys: home_ml (Q1 ML treats full-game
                        # odds as Q1 odds if nothing else is set). Expose both so callers
                        # can choose.
                        result["q1_home_ml"] = price
                    elif name == away:
                        result["q1_away_ml"] = price

            elif mkey == "spreads_q1":
                for o in outcomes:
                    name = o.get("name", "")
                    price = o.get("price", 0)
                    point = o.get("point", 0)
                    if name == home:
                        # generate_q1_picks expects q1_spread to be the HOME spread.
                        result["q1_spread"] = point
                        result["q1_spread_home_odds"] = price
                    elif name == away:
                        result["q1_spread_away_odds"] = price

            elif mkey == "totals_q1":
                for o in outcomes:
                    name = o.get("name", "").lower()
                    price = o.get("price", 0)
                    point = o.get("point", 0)
                    if "over" in name:
                        result["q1_total"] = point
                        result["q1_over_odds"] = price
                    elif "under" in name:
                        result["q1_under_odds"] = price

        if result.get("home_ml") or result.get("q1_home_ml"):
            odds_map[key] = result

    logger.info("Odds API (NBA): fetched odds for %d games (%d with Q1 markets)",
                len(odds_map),
                sum(1 for v in odds_map.values() if v.get("q1_spread") is not None))

    _odds_cache = odds_map
    _odds_cache_time = time.time()

    return odds_map


# NBA team name -> abbreviation
_TEAM_MAP = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "LA Clippers": "LAC", "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM", "Miami Heat": "MIA", "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN", "New Orleans Pelicans": "NO",
    "New York Knicks": "NY", "Oklahoma City Thunder": "OKC", "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX", "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC", "San Antonio Spurs": "SA", "Toronto Raptors": "TOR",
    "Utah Jazz": "UTAH", "Washington Wizards": "WSH",
}


def _team_abbr(name: str) -> str:
    return _TEAM_MAP.get(name, name)
