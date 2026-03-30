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

# Per-game odds (has ML and O/U but NOT RL juice)
ESPN_ODDS_URL = "https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{event_id}/odds"

# Full odds page API (has all lines including RL juice)
ESPN_ODDS_PAGE_URL = "https://site.web.api.espn.com/apis/v2/scoreboard/header?sport=baseball&league=mlb"


def _fetch(url: str) -> dict | None:
    """Fetch JSON from a URL."""
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def _derive_rl_odds(ml: int, is_favorite: bool) -> int:
    """
    Derive approximate run line (±1.5) odds from moneyline.

    Based on real DraftKings data, the RL shift from ML is large in
    baseball (~250-320 points) because 1.5 runs is a huge spread.

    Calibrated from actual DK lines:
    -300 ML → -130 RL (-1.5)    |  +240 ML → +110 RL (+1.5)
    -210 ML → +110 RL (-1.5)    |  +175 ML → -130 RL (+1.5)
    -165 ML → +135 RL (-1.5)    |  +135 ML → -160 RL (+1.5)
    -135 ML → +150 RL (-1.5)    |  +115 ML → -185 RL (+1.5)
    """
    # Convert to implied probability, shift, convert back
    if ml < 0:
        impl = abs(ml) / (abs(ml) + 100)
    else:
        impl = 100 / (ml + 100)

    if is_favorite:
        # Favorite -1.5: probability drops ~25-30 percentage points
        # More lopsided favorites lose less
        if impl > 0.75:
            drop = 0.20  # -300+ ML: only drops 20pts
        elif impl > 0.65:
            drop = 0.25
        elif impl > 0.58:
            drop = 0.28
        else:
            drop = 0.30  # Small favorite: drops 30pts

        new_impl = max(0.25, impl - drop)
    else:
        # Underdog +1.5: probability increases ~25-30 percentage points
        if impl < 0.30:
            gain = 0.30  # Big underdog gains a lot
        elif impl < 0.38:
            gain = 0.28
        elif impl < 0.43:
            gain = 0.25
        else:
            gain = 0.22  # Slight underdog gains less

        new_impl = min(0.75, impl + gain)

    # Convert back to American odds
    if new_impl >= 0.5:
        rl = int(-100 * new_impl / (1 - new_impl))
    else:
        rl = int(100 * (1 - new_impl) / new_impl)

    return max(-250, min(250, rl))


def fetch_game_odds(event_id: str) -> dict | None:
    """
    Fetch full odds for a specific game from ESPN's core API.
    Tries multiple endpoints to get all lines including RL juice.
    """
    result = {}

    # Endpoint 1: Per-game odds (has ML, O/U, spread value)
    url = ESPN_ODDS_URL.format(event_id=event_id)
    data = _fetch(url)
    if data:
        items = data.get("items", [])
        if items:
            o = items[0]
            home_odds_data = o.get("homeTeamOdds", {}) or {}
            away_odds_data = o.get("awayTeamOdds", {}) or {}

            result["home_ml"] = home_odds_data.get("moneyLine")
            result["away_ml"] = away_odds_data.get("moneyLine")
            result["over_under"] = o.get("overUnder")
            result["over_odds"] = o.get("overOdds") or home_odds_data.get("overOdds")
            result["under_odds"] = o.get("underOdds") or away_odds_data.get("underOdds")
            result["spread"] = o.get("spread")
            result["spread_details"] = o.get("details", "")
            result["home_spread_odds"] = home_odds_data.get("spreadOdds")
            result["away_spread_odds"] = away_odds_data.get("spreadOdds")

            provider = o.get("provider", {})
            result["provider"] = provider.get("name", "Unknown")

    # Endpoint 2: Try the pickcenter/odds endpoint for RL juice
    pc_url = f"https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{event_id}/odds?limit=20"
    pc_data = _fetch(pc_url)
    if pc_data:
        for item in pc_data.get("items", []):
            ho = item.get("homeTeamOdds", {}) or {}
            ao = item.get("awayTeamOdds", {}) or {}
            # Some providers include spreadOdds
            if ho.get("spreadOdds") and not result.get("home_spread_odds"):
                result["home_spread_odds"] = ho["spreadOdds"]
                result["away_spread_odds"] = ao.get("spreadOdds")
                break

    # ESPN doesn't provide RL juice through the API.
    # Don't guess — leave as null and let the frontend/edge calculator
    # handle it with standard -110 assumption.

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
