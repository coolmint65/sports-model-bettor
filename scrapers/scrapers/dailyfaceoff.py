"""
DailyFaceoff starting goalie scraper.

Fetches today's confirmed/expected starting goalies from DailyFaceoff.com.
Parses the __NEXT_DATA__ JSON embedded in the page (Next.js SSR).

Usage:
    from scrapers.dailyfaceoff import get_starting_goalies
    goalies = get_starting_goalies()
    # Returns: {"BUF": {"name": "Ukko-Pekka Luukkonen", "status": "unconfirmed",
    #                    "save_pct": 0.908, "gaa": 2.56, "wins": 19, ...}, ...}
"""

import json
import logging
import re
import time
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)

DF_URL = "https://www.dailyfaceoff.com/starting-goalies/"
CACHE_TTL = 600  # 10 min cache
_cache: dict | None = None
_cache_time: float = 0

# DailyFaceoff team name -> NHL abbreviation
_TEAM_ABBR = {
    "Anaheim Ducks": "ANA",
    "Boston Bruins": "BOS",
    "Buffalo Sabres": "BUF",
    "Calgary Flames": "CGY",
    "Carolina Hurricanes": "CAR",
    "Chicago Blackhawks": "CHI",
    "Colorado Avalanche": "COL",
    "Columbus Blue Jackets": "CBJ",
    "Dallas Stars": "DAL",
    "Detroit Red Wings": "DET",
    "Edmonton Oilers": "EDM",
    "Florida Panthers": "FLA",
    "Los Angeles Kings": "LAK",
    "Minnesota Wild": "MIN",
    "Montreal Canadiens": "MTL",
    "Montréal Canadiens": "MTL",
    "Nashville Predators": "NSH",
    "New Jersey Devils": "NJD",
    "New York Islanders": "NYI",
    "New York Rangers": "NYR",
    "Ottawa Senators": "OTT",
    "Philadelphia Flyers": "PHI",
    "Pittsburgh Penguins": "PIT",
    "San Jose Sharks": "SJS",
    "Seattle Kraken": "SEA",
    "St. Louis Blues": "STL",
    "Tampa Bay Lightning": "TBL",
    "Toronto Maple Leafs": "TOR",
    "Utah Hockey Club": "UTA",
    "Utah Mammoth": "UTA",
    "Vancouver Canucks": "VAN",
    "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH",
    "Winnipeg Jets": "WPG",
}


def get_starting_goalies(date: str | None = None) -> dict:
    """
    Fetch today's starting goalies from DailyFaceoff.

    Returns dict keyed by team abbreviation:
    {
        "BUF": {
            "name": "Ukko-Pekka Luukkonen",
            "status": "unconfirmed",
            "save_pct": 0.908,
            "gaa": 2.56,
            "wins": 19,
            "losses": 9,
            "otl": 3,
            "headshot": "https://...",
            "rating": 25,
        },
        ...
    }
    """
    global _cache, _cache_time

    if _cache and (time.time() - _cache_time) < CACHE_TTL:
        return _cache

    url = DF_URL
    if date:
        url = f"{DF_URL}{date}/"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("DailyFaceoff fetch failed: %s", e)
        return {}

    # Extract __NEXT_DATA__ JSON
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not match:
        logger.warning("DailyFaceoff: no __NEXT_DATA__ found in HTML")
        return {}

    try:
        next_data = json.loads(match.group(1))
    except json.JSONDecodeError as e:
        logger.warning("DailyFaceoff: failed to parse __NEXT_DATA__: %s", e)
        return {}

    # Navigate to the goalie data
    games = next_data.get("props", {}).get("pageProps", {}).get("data", [])
    if not games:
        logger.warning("DailyFaceoff: no games in pageProps.data")
        return {}

    goalies = {}

    for game in games:
        # Home goalie
        home_team = game.get("homeTeamName", "")
        home_abbr = _TEAM_ABBR.get(home_team, "")
        home_name = game.get("homeGoalieName", "")

        if home_abbr and home_name:
            # Determine status from newsStrengthName
            strength = game.get("homeNewsStrengthName")
            if strength:
                status = strength.lower()
                if "confirm" in status:
                    status = "confirmed"
                elif "likely" in status or "expect" in status or "probable" in status:
                    status = "expected"
                else:
                    status = status  # Use as-is
            else:
                status = "unconfirmed"

            goalies[home_abbr] = {
                "name": home_name,
                "status": status,
                "save_pct": _safe_float(game.get("homeGoalieSavePercentage")),
                "gaa": _safe_float(game.get("homeGoalieGoalsAgainstAvg")),
                "wins": game.get("homeGoalieWins", 0),
                "losses": game.get("homeGoalieLosses", 0),
                "otl": game.get("homeGoalieOvertimeLosses", 0),
                "shutouts": game.get("homeGoalieShutouts", 0),
                "headshot": game.get("homeGoalieHeadshotUrl", ""),
                "rating": game.get("homeGoalieOverallScore"),
            }

        # Away goalie
        away_team = game.get("awayTeamName", "")
        away_abbr = _TEAM_ABBR.get(away_team, "")
        away_name = game.get("awayGoalieName", "")

        if away_abbr and away_name:
            strength = game.get("awayNewsStrengthName")
            if strength:
                status = strength.lower()
                if "confirm" in status:
                    status = "confirmed"
                elif "likely" in status or "expect" in status or "probable" in status:
                    status = "expected"
                else:
                    status = status
            else:
                status = "unconfirmed"

            goalies[away_abbr] = {
                "name": away_name,
                "status": status,
                "save_pct": _safe_float(game.get("awayGoalieSavePercentage")),
                "gaa": _safe_float(game.get("awayGoalieGoalsAgainstAvg")),
                "wins": game.get("awayGoalieWins", 0),
                "losses": game.get("awayGoalieLosses", 0),
                "otl": game.get("awayGoalieOvertimeLosses", 0),
                "shutouts": game.get("awayGoalieShutouts", 0),
                "headshot": game.get("awayGoalieHeadshotUrl", ""),
                "rating": game.get("awayGoalieOverallScore"),
            }

    logger.info("DailyFaceoff: loaded %d starting goalies from %d games", len(goalies), len(games))

    if goalies:
        _cache = goalies
        _cache_time = time.time()

    return goalies


def get_all_matchups(date: str | None = None) -> list[dict]:
    """
    Return full matchup data from DailyFaceoff including odds.

    Returns list of game dicts with home/away goalie info, DK odds, etc.
    """
    url = DF_URL
    if date:
        url = f"{DF_URL}{date}/"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
    }

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("DailyFaceoff fetch failed: %s", e)
        return []

    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not match:
        return []

    try:
        next_data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return []

    return next_data.get("props", {}).get("pageProps", {}).get("data", [])


def match_goalie_to_player(goalie_name: str, team_abbr: str) -> int | None:
    """Match a DailyFaceoff goalie name to an NHL player ID in our DB."""
    try:
        from engine.nhl_db import get_conn
        conn = get_conn()

        # Exact match
        row = conn.execute("""
            SELECT p.id FROM nhl_players p
            JOIN nhl_teams t ON p.team_id = t.id
            WHERE p.name = ? AND t.abbreviation = ? AND p.position = 'G'
        """, (goalie_name, team_abbr)).fetchone()
        if row:
            return row["id"]

        # Last name match
        last_name = goalie_name.split()[-1] if goalie_name else ""
        if last_name:
            row = conn.execute("""
                SELECT p.id FROM nhl_players p
                JOIN nhl_teams t ON p.team_id = t.id
                WHERE p.name LIKE ? AND t.abbreviation = ? AND p.position = 'G'
            """, (f"%{last_name}%", team_abbr)).fetchone()
            if row:
                return row["id"]

    except Exception as e:
        logger.debug("Could not match goalie %s to DB: %s", goalie_name, e)

    return None


def _safe_float(val, default=0.0) -> float:
    """Convert a value to float safely."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    print("Fetching today's starting goalies from DailyFaceoff...", flush=True)
    goalies = get_starting_goalies()

    if goalies:
        print(f"\nFound {len(goalies)} starting goalies:\n")
        for abbr, info in sorted(goalies.items()):
            status_icon = {"confirmed": "V", "expected": "~", "unconfirmed": "?"}.get(info["status"], "?")
            sv = info.get("save_pct", 0)
            gaa = info.get("gaa", 0)
            record = f"{info.get('wins',0)}-{info.get('losses',0)}-{info.get('otl',0)}"
            rating = info.get("rating", "")
            print(f"  [{status_icon}] {abbr:4s} {info['name']:25s} SV%: {sv:.3f}  GAA: {gaa:.2f}  {record:10s} Rating: {rating}")
    else:
        print("\nNo goalies found. DailyFaceoff may be blocking or the page structure changed.")
