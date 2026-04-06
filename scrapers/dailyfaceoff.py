"""
DailyFaceoff starting goalie scraper.

Fetches today's confirmed/expected starting goalies from DailyFaceoff.com.
Used to determine goalie matchups for NHL predictions.

Usage:
    from scrapers.dailyfaceoff import get_starting_goalies
    goalies = get_starting_goalies()
    # Returns: {"BOS": {"name": "Jeremy Swayman", "status": "confirmed"}, ...}
"""

import json
import logging
import re
import time
import urllib.request
import urllib.error
from html.parser import HTMLParser
from datetime import datetime

logger = logging.getLogger(__name__)

DF_URL = "https://www.dailyfaceoff.com/starting-goalies/"
CACHE_TTL = 600  # 10 min cache
_cache: dict | None = None
_cache_time: float = 0

# Common NHL team name variations → abbreviation
_TEAM_ABBR = {
    "ducks": "ANA", "anaheim": "ANA",
    "bruins": "BOS", "boston": "BOS",
    "sabres": "BUF", "buffalo": "BUF",
    "flames": "CGY", "calgary": "CGY",
    "hurricanes": "CAR", "carolina": "CAR",
    "blackhawks": "CHI", "chicago": "CHI",
    "avalanche": "COL", "colorado": "COL",
    "blue jackets": "CBJ", "columbus": "CBJ",
    "stars": "DAL", "dallas": "DAL",
    "red wings": "DET", "detroit": "DET",
    "oilers": "EDM", "edmonton": "EDM",
    "panthers": "FLA", "florida": "FLA",
    "kings": "LAK", "los angeles": "LAK", "la kings": "LAK",
    "wild": "MIN", "minnesota": "MIN",
    "canadiens": "MTL", "montreal": "MTL", "montréal": "MTL",
    "predators": "NSH", "nashville": "NSH",
    "devils": "NJD", "new jersey": "NJD",
    "islanders": "NYI", "ny islanders": "NYI",
    "rangers": "NYR", "ny rangers": "NYR",
    "senators": "OTT", "ottawa": "OTT",
    "flyers": "PHI", "philadelphia": "PHI",
    "penguins": "PIT", "pittsburgh": "PIT",
    "sharks": "SJS", "san jose": "SJS",
    "kraken": "SEA", "seattle": "SEA",
    "blues": "STL", "st. louis": "STL", "st louis": "STL",
    "lightning": "TBL", "tampa bay": "TBL", "tampa": "TBL",
    "maple leafs": "TOR", "toronto": "TOR",
    "utah hockey club": "UTA", "utah": "UTA", "mammoth": "UTA",
    "canucks": "VAN", "vancouver": "VAN",
    "golden knights": "VGK", "vegas": "VGK",
    "capitals": "WSH", "washington": "WSH",
    "jets": "WPG", "winnipeg": "WPG",
}


def _team_to_abbr(name: str) -> str:
    """Convert a team name/city to NHL abbreviation."""
    name_lower = name.lower().strip()
    # Direct match
    if name_lower in _TEAM_ABBR:
        return _TEAM_ABBR[name_lower]
    # Check if any key is contained in the name
    for key, abbr in _TEAM_ABBR.items():
        if key in name_lower:
            return abbr
    # Already an abbreviation?
    if len(name) <= 4 and name.upper() == name:
        return name.upper()
    return name.upper()[:3]


class _GoalieParser(HTMLParser):
    """Parse DailyFaceoff starting goalies HTML."""

    def __init__(self):
        super().__init__()
        self.goalies = {}  # abbr -> {name, status}
        self._in_matchup = False
        self._in_goalie = False
        self._in_team = False
        self._in_status = False
        self._current_team = ""
        self._current_name = ""
        self._current_status = "unconfirmed"
        self._depth = 0
        self._text_parts = []
        self._capture_text = False

    def handle_starttag(self, tag, attrs):
        attr_dict = dict(attrs)
        cls = attr_dict.get("class", "")

        # Look for goalie-related containers
        if "goalie" in cls.lower() or "starter" in cls.lower():
            self._in_goalie = True
            self._text_parts = []

        # Look for team name containers
        if "team" in cls.lower() and ("name" in cls.lower() or "city" in cls.lower()):
            self._in_team = True
            self._text_parts = []
            self._capture_text = True

        # Look for status indicators
        if "confirm" in cls.lower() or "status" in cls.lower() or "expected" in cls.lower():
            self._in_status = True
            self._text_parts = []
            self._capture_text = True

        # Links often contain goalie names
        if tag == "a" and self._in_goalie:
            href = attr_dict.get("href", "")
            if "player" in href or "goalie" in href:
                self._capture_text = True
                self._text_parts = []

        # Capture text in relevant sections
        if self._in_goalie or self._in_team or self._in_status:
            self._capture_text = True

    def handle_endtag(self, tag):
        if self._capture_text and self._text_parts:
            text = " ".join(self._text_parts).strip()

            if self._in_status and text:
                status_lower = text.lower()
                if "confirm" in status_lower:
                    self._current_status = "confirmed"
                elif "expect" in status_lower or "likely" in status_lower or "probable" in status_lower:
                    self._current_status = "expected"
                else:
                    self._current_status = "unconfirmed"
                self._in_status = False

            if self._in_team and text and len(text) > 1:
                abbr = _team_to_abbr(text)
                if abbr and len(abbr) >= 2:
                    self._current_team = abbr
                self._in_team = False

        self._text_parts = []
        self._capture_text = False

    def handle_data(self, data):
        if self._capture_text:
            stripped = data.strip()
            if stripped:
                self._text_parts.append(stripped)


def _parse_with_regex(html: str) -> dict:
    """
    Fallback regex-based parser for DailyFaceoff HTML.
    Looks for common patterns in goalie starter pages.
    """
    goalies = {}

    # Pattern 1: Look for goalie names near team abbreviations
    # DailyFaceoff often uses data attributes or structured divs
    # Try to find patterns like: team abbr followed by goalie name

    # Find all three-letter team abbreviations in context
    team_pattern = re.compile(
        r'(?:data-team|team-abbr|abbreviation)["\s:=]+([A-Z]{2,3})',
        re.IGNORECASE
    )

    # Find goalie names (typically "First Last" near team context)
    name_pattern = re.compile(
        r'(?:goalie|starter|netminder)[^>]*>([^<]+)<',
        re.IGNORECASE
    )

    # Status pattern
    status_pattern = re.compile(
        r'(confirmed|expected|likely|probable|unconfirmed|tentative)',
        re.IGNORECASE
    )

    # Try a more structured approach — look for JSON-LD or structured data
    json_match = re.search(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                           html, re.DOTALL)
    if json_match:
        try:
            ld_data = json.loads(json_match.group(1))
            logger.info("Found JSON-LD data on DailyFaceoff")
        except json.JSONDecodeError:
            pass

    # Look for goalie sections with associated team/status
    # Common pattern: <div class="...team...">TEAM</div>...<a>Goalie Name</a>...<span>Confirmed</span>
    sections = re.split(r'(?=<(?:div|section|article)[^>]*(?:matchup|game|card))', html, flags=re.IGNORECASE)

    for section in sections:
        teams = re.findall(r'(?:alt|title|data-team)="([^"]*(?:' +
                          '|'.join(_TEAM_ABBR.keys()) + r')[^"]*)"',
                          section, re.IGNORECASE)
        names = re.findall(r'<a[^>]*href="[^"]*(?:player|goalie)[^"]*"[^>]*>([^<]+)</a>',
                          section, re.IGNORECASE)
        statuses = status_pattern.findall(section)

        if teams and names:
            for i, (team, name) in enumerate(zip(teams, names)):
                abbr = _team_to_abbr(team)
                status = statuses[i].lower() if i < len(statuses) else "unconfirmed"
                if "confirm" in status:
                    status = "confirmed"
                elif "expect" in status or "likely" in status or "probable" in status:
                    status = "expected"
                else:
                    status = "unconfirmed"

                goalies[abbr] = {"name": name.strip(), "status": status}

    return goalies


def get_starting_goalies(date: str | None = None) -> dict:
    """
    Fetch today's starting goalies from DailyFaceoff.

    Returns dict keyed by team abbreviation:
    {
        "BOS": {"name": "Jeremy Swayman", "status": "confirmed"},
        "TOR": {"name": "Joseph Woll", "status": "expected"},
        ...
    }

    Status values: "confirmed", "expected", "unconfirmed"
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
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        logger.warning("DailyFaceoff returned HTTP %d", e.code)
        return {}
    except Exception as e:
        logger.warning("DailyFaceoff fetch failed: %s", e)
        return {}

    # Try HTML parser first
    parser = _GoalieParser()
    try:
        parser.feed(html)
    except Exception as e:
        logger.debug("HTML parser error: %s", e)

    goalies = parser.goalies

    # If HTML parser didn't get results, try regex fallback
    if not goalies:
        goalies = _parse_with_regex(html)

    if goalies:
        logger.info("DailyFaceoff: got %d starting goalies", len(goalies))
        _cache = goalies
        _cache_time = time.time()
    else:
        logger.warning("DailyFaceoff: could not parse any goalies from HTML")

    return goalies


def match_goalie_to_player(goalie_name: str, team_abbr: str) -> int | None:
    """
    Try to match a DailyFaceoff goalie name to an NHL player ID in our DB.

    Returns player_id or None.
    """
    try:
        from engine.nhl_db import get_conn
        conn = get_conn()

        # Try exact match first
        row = conn.execute("""
            SELECT p.id FROM nhl_players p
            JOIN nhl_teams t ON p.team_id = t.id
            WHERE p.name = ? AND t.abbreviation = ? AND p.position = 'G'
        """, (goalie_name, team_abbr)).fetchone()
        if row:
            return row["id"]

        # Try last name match
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


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    print("Fetching today's starting goalies from DailyFaceoff...", flush=True)
    goalies = get_starting_goalies()

    if goalies:
        print(f"\nFound {len(goalies)} starting goalies:")
        for abbr, info in sorted(goalies.items()):
            status_icon = {"confirmed": "✓", "expected": "~", "unconfirmed": "?"}.get(info["status"], "?")
            print(f"  [{status_icon}] {abbr:4s} {info['name']:25s} ({info['status']})")
    else:
        print("\nNo goalies found. DailyFaceoff may be blocking or the page structure changed.")
        print("Try running from your local machine: python -m scrapers.dailyfaceoff")
