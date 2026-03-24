"""
ESPN public API scraper.

Fetches team info, stats, standings, and recent results from ESPN's
public JSON endpoints. No API key required.

Usage:
    from scrapers.espn import scrape_league
    teams = scrape_league("football", "nfl", "NFL")
"""

import json
import re
import time
import logging
import unicodedata
import urllib.request
import urllib.error
from pathlib import Path

from .config import (
    espn_teams_url,
    espn_team_stats_url,
    espn_standings_url,
    espn_team_schedule_url,
    espn_team_record_url,
    LEAGUE_SETTINGS,
)

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "teams"

# Polite delay between requests (seconds)
REQUEST_DELAY = 1.0
REQUEST_TIMEOUT = 15


def _fetch_json(url: str, retries: int = 2) -> dict | None:
    """Fetch JSON from a URL with retry. Returns None on failure."""
    for attempt in range(retries + 1):
        try:
            logger.debug(f"Fetching: {url}")
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                data = json.loads(resp.read().decode())
                if data:
                    return data
                logger.warning(f"Empty response from {url}")
                return None
        except urllib.error.HTTPError as e:
            logger.warning(f"HTTP {e.code} from {url}: {e.reason}")
            if e.code == 429 and attempt < retries:
                time.sleep(3 * (attempt + 1))
                continue
            return None
        except (urllib.error.URLError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            if attempt < retries:
                time.sleep(2 * (attempt + 1))
                continue
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {type(e).__name__}: {e}")
            return None
    return None


def _safe_float(val, default=0.0) -> float:
    """Convert a value to float safely."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _team_key_from_name(name: str) -> str:
    """Convert team name to a filesystem-safe key."""
    key = name.lower().strip()
    # Normalize unicode: decompose accented chars, strip combining marks
    key = unicodedata.normalize("NFKD", key)
    key = "".join(c for c in key if not unicodedata.combining(c))
    # Replace & with 'and' before stripping
    key = key.replace("&", "and")
    # Replace any non-alphanumeric char with underscore
    key = re.sub(r"[^a-z0-9]+", "_", key)
    return key.strip("_")


def scrape_league(espn_sport: str, espn_league: str, our_league: str) -> list[str]:
    """
    Scrape all teams for a league from ESPN.
    Returns list of team keys that were updated.
    """
    league_dir = DATA_DIR / our_league
    league_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Scraping {our_league} from ESPN ({espn_sport}/{espn_league})...")

    # Step 1: Get team list (with per-league limit and division filtering)
    settings = LEAGUE_SETTINGS.get(our_league, {})
    teams_url = espn_teams_url(
        espn_sport, espn_league,
        limit=settings.get("limit", 50),
        groups=settings.get("groups"),
    )
    teams_data = _fetch_json(teams_url)
    if not teams_data:
        logger.error(f"Failed to fetch teams for {our_league}")
        return []

    teams = _extract_teams(teams_data)
    if not teams:
        logger.error(f"No teams found for {our_league}")
        return []

    logger.info(f"Found {len(teams)} teams for {our_league}")

    # Step 2: Get standings for records + stats
    standings = _fetch_standings(espn_sport, espn_league)

    # Step 3: For each team, fetch detailed stats and schedule
    updated = []
    for i, team in enumerate(teams):
        team_id = team["id"]
        team_key = team["key"]

        logger.info(f"  [{i+1}/{len(teams)}] {team['name']} ({team_key})")

        # Load existing data to merge with
        existing = _load_existing(our_league, team_key)

        # Build team data
        team_data = {
            "name": team["name"],
            "abbreviation": team["abbreviation"],
            "city": team.get("city", ""),
            "short_name": team.get("short_name", team["name"]),
            "espn_id": team_id,
            "record": standings.get(team_id, {}).get("record", existing.get("record", "")),
            "stats": {},
            "home_away_splits": {},
            "recent_form": [],
            "strength_of_schedule": {},
        }

        # Fetch team stats
        time.sleep(REQUEST_DELAY)
        stats = _fetch_team_stats(espn_sport, espn_league, team_id, our_league)
        if stats:
            team_data["stats"] = stats

        # Fetch recent results for form
        time.sleep(REQUEST_DELAY)
        recent = _fetch_recent_results(espn_sport, espn_league, team_id)
        if recent:
            team_data["recent_form"] = recent
            team_data["home_away_splits"] = _compute_splits(recent)
            team_data["strength_of_schedule"] = _compute_sos(recent)

        # Merge with existing (keep fields we didn't scrape)
        merged = _merge_data(existing, team_data)

        # Save
        path = league_dir / f"{team_key}.json"
        with open(path, "w") as f:
            json.dump(merged, f, indent=2)

        updated.append(team_key)

    logger.info(f"Updated {len(updated)} teams for {our_league}")
    return updated


def _extract_teams(data: dict) -> list[dict]:
    """Extract team list from ESPN teams endpoint response."""
    teams = []
    logger.debug(f"Teams response top-level keys: {list(data.keys())}")

    # ESPN wraps teams in sports[0].leagues[0].teams[]
    try:
        raw_teams = []

        # Format 1: sports[].leagues[].teams[]
        sports = data.get("sports", [])
        for sport in sports:
            for league in sport.get("leagues", []):
                raw_teams.extend(league.get("teams", []))

        # Format 2: flat teams[] at top level
        if not raw_teams and "teams" in data:
            raw_teams = data["teams"]

        # Format 3: under league key
        if not raw_teams and "league" in data:
            raw_teams = data["league"].get("teams", [])

        logger.debug(f"Found {len(raw_teams)} raw team entries")

        for team_entry in raw_teams:
            t = team_entry.get("team", team_entry)
            teams.append({
                "id": t["id"],
                "name": t.get("displayName", t.get("name", "")),
                "abbreviation": t.get("abbreviation", ""),
                        "city": t.get("location", ""),
                        "short_name": t.get("shortDisplayName", t.get("name", "")),
                        "key": _team_key_from_name(
                            t.get("shortDisplayName", t.get("name", ""))
                        ),
                    })
    except (KeyError, IndexError, TypeError) as e:
        logger.warning(f"Error extracting teams: {e}")

    return teams


def _fetch_standings(espn_sport: str, espn_league: str) -> dict:
    """Fetch standings, return {team_id: {record, wins, losses, ...}}."""
    data = _fetch_json(espn_standings_url(espn_sport, espn_league))
    if not data:
        return {}

    standings = {}
    try:
        # Collect all groups that have standings - some sports (MLB) nest
        # children inside children (league > division)
        groups = []
        for child in data.get("children", []):
            if "standings" in child:
                groups.append(child)
            # Check for nested children (e.g. MLB divisions)
            for subchild in child.get("children", []):
                if "standings" in subchild:
                    groups.append(subchild)

        for group in groups:
            for entry in group.get("standings", {}).get("entries", []):
                team = entry.get("team", {})
                team_id = team.get("id", "")
                stats_map = {}
                for s in entry.get("stats", []):
                    stats_map[s.get("name", "")] = s.get("value", s.get("displayValue", ""))

                record_str = stats_map.get("overall", "")
                if not record_str:
                    w = stats_map.get("wins", 0)
                    l = stats_map.get("losses", 0)
                    record_str = f"{int(w)}-{int(l)}"

                standings[team_id] = {
                    "record": record_str,
                    "wins": _safe_float(stats_map.get("wins", 0)),
                    "losses": _safe_float(stats_map.get("losses", 0)),
                    "win_pct": _safe_float(stats_map.get("winPercent", 0)),
                    "home_record": stats_map.get("Home", stats_map.get("home", "")),
                    "away_record": stats_map.get("Road", stats_map.get("away", "")),
                    "streak": stats_map.get("streak", ""),
                    "point_diff": _safe_float(stats_map.get("pointDifferential",
                                              stats_map.get("differential", 0))),
                }
                if team_id:
                    standings[team_id] = standings[team_id]
    except (KeyError, TypeError) as e:
        logger.warning(f"Error parsing standings: {e}")

    return standings


def _fetch_team_stats(espn_sport: str, espn_league: str, team_id: str,
                      our_league: str) -> dict:
    """Fetch detailed team statistics."""
    url = espn_team_stats_url(espn_sport, espn_league, team_id)
    data = _fetch_json(url)
    if not data:
        return {}

    stats = {}
    try:
        # ESPN returns stats in categories
        results = data.get("results", data)
        splits = results.get("splits", {}) if isinstance(results, dict) else {}
        categories = splits.get("categories", [])

        if not categories:
            # Try alternate structure
            for stat_block in data.get("statistics", data.get("stats", [])):
                if isinstance(stat_block, dict):
                    categories.extend(stat_block.get("categories", []))

        for cat in categories:
            cat_name = cat.get("name", cat.get("displayName", "")).lower()
            for stat in cat.get("stats", []):
                name = stat.get("name", stat.get("abbreviation", "")).lower()
                value = stat.get("value", stat.get("displayValue", 0))
                if name and value is not None:
                    stats[name] = _safe_float(value, value)

    except (KeyError, TypeError) as e:
        logger.warning(f"Error parsing stats for team {team_id}: {e}")

    # Also try the team endpoint for record-level stats
    team_url = espn_team_record_url(espn_sport, espn_league, team_id)
    team_data = _fetch_json(team_url)
    if team_data:
        try:
            team_info = team_data.get("team", {})
            record = team_info.get("record", {})
            # ESPN returns record as list for some sports (MLB), dict with items for others
            record_items = record if isinstance(record, list) else record.get("items", [])
            for rec in record_items:
                rec_type = rec.get("type", "")
                if rec_type == "total":
                    for s in rec.get("stats", []):
                        name = s.get("name", "").lower()
                        val = s.get("value", 0)
                        if name == "pointsfor":
                            stats["ppg"] = round(_safe_float(val), 1)
                        elif name == "pointsagainst":
                            stats["opp_ppg"] = round(_safe_float(val), 1)
                        elif name == "avgpointsfor":
                            stats["ppg"] = round(_safe_float(val), 1)
                        elif name == "avgpointsagainst":
                            stats["opp_ppg"] = round(_safe_float(val), 1)
        except (KeyError, TypeError):
            pass

    return stats


def _fetch_recent_results(espn_sport: str, espn_league: str, team_id: str,
                          limit: int = 10) -> list[dict]:
    """Fetch recent game results for form/splits calculation."""
    url = espn_team_schedule_url(espn_sport, espn_league, team_id)
    data = _fetch_json(url)
    if not data:
        return []

    results = []
    try:
        events = data.get("events", [])
        for event in reversed(events):  # Most recent first
            status = event.get("competitions", [{}])[0].get("status", {})
            if status.get("type", {}).get("completed", False):
                comp = event["competitions"][0]
                competitors = comp.get("competitors", [])

                game = {"date": event.get("date", "")}
                for c in competitors:
                    team = c.get("team", {})
                    is_home = c.get("homeAway", "") == "home"
                    is_us = str(team.get("id", "")) == str(team_id)
                    # ESPN returns score as string "114" or object {"value": 114}
                    raw_score = c.get("score", 0)
                    if isinstance(raw_score, dict):
                        score = _safe_float(raw_score.get("value", 0))
                    else:
                        score = _safe_float(raw_score)

                    if is_us:
                        game["our_score"] = score
                        game["home"] = is_home
                        game["winner"] = c.get("winner", False)
                    else:
                        game["opp_score"] = score
                        game["opponent"] = team.get("displayName",
                                                     team.get("name", "Unknown"))
                        game["opp_id"] = team.get("id", "")

                if "our_score" in game and "opp_score" in game:
                    results.append(game)

                if len(results) >= limit:
                    break
    except (KeyError, IndexError, TypeError) as e:
        logger.warning(f"Error parsing schedule for team {team_id}: {e}")

    return results


def _compute_splits(recent: list[dict]) -> dict:
    """Compute home/away splits from recent results."""
    home_games = [g for g in recent if g.get("home")]
    away_games = [g for g in recent if not g.get("home")]

    def _avg(games, key):
        vals = [g.get(key, 0) for g in games]
        return round(sum(vals) / len(vals), 1) if vals else 0.0

    return {
        "home_games": len(home_games),
        "away_games": len(away_games),
        "home_ppg": _avg(home_games, "our_score"),
        "home_opp_ppg": _avg(home_games, "opp_score"),
        "home_wins": sum(1 for g in home_games if g.get("winner")),
        "away_ppg": _avg(away_games, "our_score"),
        "away_opp_ppg": _avg(away_games, "opp_score"),
        "away_wins": sum(1 for g in away_games if g.get("winner")),
    }


def _compute_sos(recent: list[dict]) -> dict:
    """Basic strength-of-schedule from recent opponents' scores."""
    if not recent:
        return {}

    opp_scores = [g.get("opp_score", 0) for g in recent]
    our_margins = [g.get("our_score", 0) - g.get("opp_score", 0) for g in recent]
    wins = sum(1 for g in recent if g.get("winner"))

    return {
        "recent_games": len(recent),
        "recent_wins": wins,
        "recent_losses": len(recent) - wins,
        "avg_margin": round(sum(our_margins) / len(our_margins), 1) if our_margins else 0,
        "avg_opp_score": round(sum(opp_scores) / len(opp_scores), 1) if opp_scores else 0,
    }


def _load_existing(league: str, team_key: str) -> dict:
    """Load existing team data to merge with."""
    path = DATA_DIR / league / f"{team_key}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def _merge_data(existing: dict, new: dict) -> dict:
    """Merge new scraped data with existing, preferring new non-empty values."""
    merged = dict(existing)
    for key, value in new.items():
        if isinstance(value, dict):
            merged[key] = {**merged.get(key, {}), **value}
        elif isinstance(value, list):
            if value:  # Only overwrite if new list is non-empty
                merged[key] = value
        elif value or value == 0:
            merged[key] = value
    return merged
