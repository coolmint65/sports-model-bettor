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


def _fetch_json(url: str) -> tuple[list | dict | None, str | None]:
    """Fetch JSON from The Odds API. Returns (data, x-requests-remaining)."""
    try:
        req = urllib.request.Request(url,
                                    headers={"User-Agent": "NBAQ1PredictionEngine/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            remaining = resp.headers.get("x-requests-remaining", "?")
            return data, remaining
    except Exception as e:
        logger.warning("Odds API request failed: %s (%s)", url.split("?")[0], e)
        return None, None


def fetch_nba_odds() -> dict:
    """Fetch NBA odds (full-game + Q1 markets) from The Odds API.

    Uses a two-step flow to access period markets:
      1. Bulk /sports/{sport}/odds — one call for h2h/spreads/totals
         across all games. Gets event IDs.
      2. Per-event /sports/{sport}/events/{id}/odds — one call per game
         for h2h_q1/spreads_q1/totals_q1. Period markets are only
         available through the per-event endpoint on every paid tier.

    Results are merged per matchup. On a 15-game slate this uses ~16
    credits per refresh; with the 10-min cache that's ~96 credits/hr.

    Returns dict keyed by "AWAY@HOME" abbreviation.
    """
    global _odds_cache, _odds_cache_time

    if _odds_cache and (time.time() - _odds_cache_time) < ODDS_CACHE_TTL:
        return _odds_cache

    api_key = _get_api_key()
    if not api_key:
        logger.info("No Odds API key found. Set ODDS_API_KEY env var or create data/odds_api_key.txt")
        return {}

    # Step 1: bulk full-game fetch
    bulk_url = (f"{API_BASE}/sports/{NBA_SPORT}/odds/"
                f"?apiKey={api_key}"
                f"&regions=us"
                f"&markets=h2h,spreads,totals"
                f"&oddsFormat=american"
                f"&bookmakers={PREFERRED_BOOK}")
    data, remaining = _fetch_json(bulk_url)
    if data is None:
        return {}
    logger.info("Odds API (NBA): %s requests remaining after bulk fetch", remaining)

    if not data or not isinstance(data, list):
        return {}

    odds_map: dict[str, dict] = {}
    # game_id -> (key, home_full_name, away_full_name) for step-2 Q1 fetches
    event_meta: dict[str, tuple[str, str, str]] = {}

    for game in data:
        event_id = str(game.get("id", ""))
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

        if result.get("home_ml"):
            odds_map[key] = result
            if event_id:
                event_meta[event_id] = (key, home, away)

    # Step 2: per-event Q1 markets. These are only exposed through the
    # per-event endpoint (the bulk /odds endpoint 422s on period market
    # keys regardless of plan tier).
    q1_markets = "h2h_q1,spreads_q1,totals_q1"
    logger.info("Per-event Q1 fetch: attempting %d events", len(event_meta))
    pe_attempted = 0
    pe_no_data = 0
    pe_no_bookmakers = 0
    pe_success = 0
    for event_id, (key, home_full, away_full) in event_meta.items():
        pe_attempted += 1
        ev_url = (f"{API_BASE}/sports/{NBA_SPORT}/events/{event_id}/odds/"
                  f"?apiKey={api_key}"
                  f"&regions=us"
                  f"&markets={q1_markets}"
                  f"&oddsFormat=american"
                  f"&bookmakers={PREFERRED_BOOK}")
        ev_data, _ = _fetch_json(ev_url)
        if not ev_data or not isinstance(ev_data, dict):
            pe_no_data += 1
            continue

        bookmakers = ev_data.get("bookmakers", []) or []
        if not bookmakers:
            pe_no_bookmakers += 1
            continue
        pe_success += 1
        book = bookmakers[0]
        result = odds_map[key]

        for market in book.get("markets", []) or []:
            mkey = market.get("key", "")
            outcomes = market.get("outcomes", []) or []

            if mkey == "h2h_q1":
                for o in outcomes:
                    name = o.get("name", "")
                    price = o.get("price")
                    if name == home_full:
                        result["q1_home_ml"] = price
                    elif name == away_full:
                        result["q1_away_ml"] = price
            elif mkey == "spreads_q1":
                for o in outcomes:
                    name = o.get("name", "")
                    price = o.get("price")
                    point = o.get("point")
                    if name == home_full:
                        result["q1_spread"] = point
                        result["q1_spread_home_odds"] = price
                    elif name == away_full:
                        result["q1_spread_away_odds"] = price
            elif mkey == "totals_q1":
                for o in outcomes:
                    name = (o.get("name", "") or "").lower()
                    price = o.get("price")
                    point = o.get("point")
                    if "over" in name:
                        result["q1_total"] = point
                        result["q1_over_odds"] = price
                    elif "under" in name:
                        result["q1_under_odds"] = price

    logger.info("Per-event Q1 fetch: %d attempted, %d success, %d empty-body, %d no-bookmaker",
                pe_attempted, pe_success, pe_no_data, pe_no_bookmakers)
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


# ── Unified fallback chain ───────────────────────────────

def _has_q1_data(odds_map: dict) -> bool:
    """True iff at least one game in the map has a Q1 market populated."""
    for v in (odds_map or {}).values():
        if (v.get("q1_spread") is not None
                or v.get("q1_total") is not None
                or v.get("q1_home_ml") is not None):
            return True
    return False


def _merge_odds(base: dict, extra: dict) -> dict:
    """Merge extra-source odds into base without overwriting populated keys."""
    out = dict(base)
    for key, payload in (extra or {}).items():
        existing = out.get(key, {}) or {}
        for k, v in (payload or {}).items():
            if v is None:
                continue
            if existing.get(k) in (None, 0, ""):
                existing[k] = v
        out[key] = existing
    return out


def fetch_all_nba_odds() -> dict:
    """Fetch NBA odds from The Odds API with ESPN + DK fallback chain.

    Order:
      1. The Odds API (fast, authoritative when plan includes Q1)
      2. DraftKings public sportsbook API (Q1 markets, may 403 by region)
      3. ESPN summary/core endpoints (Q1 markets via pickcenter)

    Each source is merged without overwriting data from a prior source.
    Returns a dict keyed by "AWAY@HOME" with the same schema
    fetch_nba_odds() returns.
    """
    odds = fetch_nba_odds() or {}

    if not _has_q1_data(odds):
        try:
            from .nba_dk_odds import fetch_nba_dk_odds
            dk = fetch_nba_dk_odds()
            if dk:
                odds = _merge_odds(odds, dk)
                logger.info("NBA odds fallback: merged %d DK games", len(dk))
        except Exception as e:
            logger.debug("DK NBA odds fallback failed: %s", e)

    if not _has_q1_data(odds):
        try:
            from .nba_espn_odds import fetch_nba_espn_odds
            espn = fetch_nba_espn_odds()
            if espn:
                odds = _merge_odds(odds, espn)
                logger.info("NBA odds fallback: merged %d ESPN games", len(espn))
        except Exception as e:
            logger.debug("ESPN NBA odds fallback failed: %s", e)

    return odds

