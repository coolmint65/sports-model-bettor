"""
DraftKings odds scraper.

Fetches real-time MLB odds directly from DraftKings' public
sportsbook API. Returns ML, O/U with juice, and RL ±1.5 with
juice for all today's games.

No API key required. DraftKings exposes this endpoint publicly
(it powers their website). Use for personal, non-commercial purposes.

Endpoint: /api/v5/eventgroups/84240?format=json
Event Group 84240 = MLB
"""

import json
import logging
import urllib.request

logger = logging.getLogger(__name__)

# DraftKings public sportsbook API
DK_MLB_URL = "https://sportsbook.draftkings.com//sites/US-SB/api/v5/eventgroups/84240?format=json"

# Offer category IDs in DraftKings response
GAME_LINES_CAT = 0  # First category is usually "Game Lines"


def fetch_dk_odds() -> dict:
    """
    Fetch all MLB odds from DraftKings.

    Returns dict keyed by a normalized matchup string:
    {
        "MIN@KC": {
            "home_ml": -168, "away_ml": 139,
            "over_under": 10.5, "over_odds": -103, "under_odds": -117,
            "home_spread": -1.5, "home_spread_odds": -149,
            "away_spread": 1.5, "away_spread_odds": 123,
        },
        ...
    }
    """
    try:
        req = urllib.request.Request(DK_MLB_URL, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        logger.warning("Failed to fetch DraftKings odds: %s", e)
        return {}

    if not data:
        return {}

    odds_map = {}

    try:
        event_group = data.get("eventGroup", {})
        offer_cats = event_group.get("offerCategories", [])

        if not offer_cats:
            return {}

        # Find "Game Lines" category
        game_lines = None
        for cat in offer_cats:
            name = cat.get("name", "").lower()
            if "game" in name and "line" in name:
                game_lines = cat
                break

        if not game_lines:
            # Try first category
            game_lines = offer_cats[0]

        subcats = game_lines.get("offerSubcategoryDescriptors", [])

        for subcat in subcats:
            offers = subcat.get("offerSubcategory", {}).get("offers", [])

            for offer_group in offers:
                for offer in offer_group:
                    if not isinstance(offer, dict):
                        continue

                    label = offer.get("label", "").lower()
                    outcomes = offer.get("outcomes", [])

                    if len(outcomes) < 2:
                        continue

                    # Get event info
                    event_id = offer.get("eventId")

                    # Parse based on offer type
                    if "moneyline" in label or "money line" in label:
                        _parse_moneyline(odds_map, offer, outcomes)
                    elif "run line" in label or "spread" in label:
                        _parse_runline(odds_map, offer, outcomes)
                    elif "total" in label or "over" in label:
                        _parse_total(odds_map, offer, outcomes)

        # Also try parsing from events directly
        events = event_group.get("events", [])
        if events and not odds_map:
            _parse_from_events(odds_map, data)

    except Exception as e:
        logger.error("Error parsing DraftKings odds: %s", e)

    logger.info("DraftKings: fetched odds for %d games", len(odds_map))
    return odds_map


def _normalize_team(name: str) -> str:
    """Normalize team name to abbreviation for matching."""
    # DraftKings uses full names, we need abbreviations
    name = name.strip()
    abbr_map = {
        "Arizona Diamondbacks": "AZ", "Atlanta Braves": "ATL",
        "Baltimore Orioles": "BAL", "Boston Red Sox": "BOS",
        "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
        "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE",
        "Colorado Rockies": "COL", "Detroit Tigers": "DET",
        "Houston Astros": "HOU", "Kansas City Royals": "KC",
        "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD",
        "Miami Marlins": "MIA", "Milwaukee Brewers": "MIL",
        "Minnesota Twins": "MIN", "New York Mets": "NYM",
        "New York Yankees": "NYY", "Athletics": "ATH",
        "Oakland Athletics": "ATH", "Philadelphia Phillies": "PHI",
        "Pittsburgh Pirates": "PIT", "San Diego Padres": "SD",
        "San Francisco Giants": "SF", "Seattle Mariners": "SEA",
        "St. Louis Cardinals": "STL", "Tampa Bay Rays": "TB",
        "Texas Rangers": "TEX", "Toronto Blue Jays": "TOR",
        "Washington Nationals": "WSH",
    }
    return abbr_map.get(name, name)


def _get_matchup_key(outcomes: list) -> str | None:
    """Extract matchup key from outcomes (e.g. 'MIN@KC')."""
    teams = []
    for o in outcomes:
        name = o.get("participant", o.get("label", ""))
        abbr = _normalize_team(name)
        teams.append(abbr)
    if len(teams) >= 2:
        return f"{teams[0]}@{teams[1]}"
    return None


def _parse_moneyline(odds_map: dict, offer: dict, outcomes: list):
    key = _get_matchup_key(outcomes)
    if not key:
        return

    if key not in odds_map:
        odds_map[key] = {}

    for o in outcomes:
        name = _normalize_team(o.get("participant", o.get("label", "")))
        price = o.get("oddsAmerican", o.get("odds"))
        if price:
            try:
                price = int(price) if isinstance(price, (int, float)) else int(str(price).replace("+", ""))
            except (ValueError, TypeError):
                continue

            if name == key.split("@")[1]:  # Home team is second
                odds_map[key]["home_ml"] = price
            else:
                odds_map[key]["away_ml"] = price


def _parse_runline(odds_map: dict, offer: dict, outcomes: list):
    key = _get_matchup_key(outcomes)
    if not key:
        return

    if key not in odds_map:
        odds_map[key] = {}

    for o in outcomes:
        name = _normalize_team(o.get("participant", o.get("label", "")))
        price = o.get("oddsAmerican", o.get("odds"))
        line = o.get("line", o.get("handicap"))

        if price is None or line is None:
            continue

        try:
            price = int(price) if isinstance(price, (int, float)) else int(str(price).replace("+", ""))
            line = float(line)
        except (ValueError, TypeError):
            continue

        if name == key.split("@")[1]:  # Home
            odds_map[key]["home_spread"] = line
            odds_map[key]["home_spread_odds"] = price
        else:
            odds_map[key]["away_spread"] = line
            odds_map[key]["away_spread_odds"] = price


def _parse_total(odds_map: dict, offer: dict, outcomes: list):
    # For totals, outcomes are "Over" and "Under"
    # Need to get the matchup from the offer context
    # Try to find event-level info
    event_label = offer.get("eventLabel", "")

    # Find which matchup this belongs to
    key = None
    for k in odds_map:
        # Match by checking if event label contains team names
        if event_label:
            parts = event_label.replace(" vs ", "@").replace(" at ", "@")
            if "@" in parts:
                key = k  # Rough match
                break

    if not key and odds_map:
        # Fall back to most recent matchup key
        key = list(odds_map.keys())[-1]

    if not key:
        return

    for o in outcomes:
        label = o.get("label", "").lower()
        price = o.get("oddsAmerican", o.get("odds"))
        line = o.get("line", o.get("handicap"))

        if price is None:
            continue

        try:
            price = int(price) if isinstance(price, (int, float)) else int(str(price).replace("+", ""))
        except (ValueError, TypeError):
            continue

        if line:
            odds_map[key]["over_under"] = float(line)

        if "over" in label:
            odds_map[key]["over_odds"] = price
        elif "under" in label:
            odds_map[key]["under_odds"] = price


def _parse_from_events(odds_map: dict, data: dict):
    """Alternative parser using events structure."""
    try:
        events = data.get("eventGroup", {}).get("events", [])
        for event in events:
            # Events may have displayGroups with offers
            display_groups = event.get("displayGroups", [])
            for dg in display_groups:
                markets = dg.get("markets", [])
                for market in markets:
                    # Parse market based on description
                    pass
    except Exception:
        pass
