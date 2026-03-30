"""
ESPN odds scraper.

Fetches DraftKings odds from ESPN's matchup pages for MLB games.
ESPN displays odds powered by DraftKings with ML, O/U, and RL lines.

The public scoreboard API doesn't include odds consistently, but
each game's individual page has them in a separate API endpoint.
"""

import json
import logging
import time
import urllib.request

logger = logging.getLogger(__name__)

ESPN_ODDS_URL = "https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{event_id}/odds"


def fetch_game_odds(event_id: str) -> dict | None:
    """
    Fetch full odds for a specific game from ESPN's core API.

    Returns {
        home_ml, away_ml,
        over_under, over_odds, under_odds,
        home_spread, away_spread, spread_line
    }
    """
    url = ESPN_ODDS_URL.format(event_id=event_id)
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        logger.debug("Failed to fetch odds for event %s: %s", event_id, e)
        return None

    if not data:
        return None

    # ESPN returns odds as a list of providers
    items = data.get("items", [])
    if not items:
        return None

    # Use first provider (usually DraftKings)
    o = items[0]

    result = {}

    # Moneyline
    home_odds_data = o.get("homeTeamOdds", {}) or {}
    away_odds_data = o.get("awayTeamOdds", {}) or {}

    result["home_ml"] = home_odds_data.get("moneyLine")
    result["away_ml"] = away_odds_data.get("moneyLine")

    # Over/Under
    result["over_under"] = o.get("overUnder")
    result["over_odds"] = o.get("overOdds") or home_odds_data.get("overOdds")
    result["under_odds"] = o.get("underOdds") or away_odds_data.get("underOdds")

    # Spread (Run Line)
    result["spread"] = o.get("spread")
    result["spread_details"] = o.get("details", "")
    result["home_spread_odds"] = home_odds_data.get("spreadOdds")
    result["away_spread_odds"] = away_odds_data.get("spreadOdds")

    # Provider info
    provider = o.get("provider", {})
    result["provider"] = provider.get("name", "Unknown")

    # Log what we found
    logger.debug("Odds for %s: ML %s/%s, O/U %s (%s/%s), RL %s (%s/%s) via %s",
                event_id,
                result["home_ml"], result["away_ml"],
                result["over_under"], result["over_odds"], result["under_odds"],
                result["spread"], result["home_spread_odds"], result["away_spread_odds"],
                result["provider"])

    return result


def fetch_all_game_odds(games: list[dict]) -> dict:
    """
    Fetch odds for all games in a scoreboard list.
    Returns {game_id: odds_dict}.
    """
    odds_map = {}

    for game in games:
        event_id = game.get("id")
        if not event_id:
            continue

        # Skip if game already has good odds data
        existing = game.get("odds") or {}
        if existing.get("home_ml") and existing.get("over_under") and existing.get("home_spread_odds"):
            odds_map[event_id] = existing
            continue

        odds = fetch_game_odds(event_id)
        if odds:
            odds_map[event_id] = odds

        # Be polite — small delay between requests
        time.sleep(0.3)

    logger.info("Fetched odds for %d/%d games", len(odds_map), len(games))
    return odds_map
