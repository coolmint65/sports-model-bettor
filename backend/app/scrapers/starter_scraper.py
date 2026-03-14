"""
NHL confirmed starting goalie scraper.

Two data sources, tried in order:
1. DailyFaceoff.com /starting-goalies — most reliable, updated hours
   before puck drop with Confirmed/Expected/Unconfirmed status.
2. NHL API gamecenter landing page — official but often only populated
   very close to puck drop.

Both sources are scraped per-day and merged.  DailyFaceoff is the
primary source; the NHL API fills in any gaps.
"""

import logging
import re
from datetime import date
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.game import Game
from app.models.team import Team

logger = logging.getLogger(__name__)

NHL_API_BASE = settings.nhl_api_base

# Map common DailyFaceoff team names to NHL abbreviations
_TEAM_ALIAS = {
    "montréal": "MTL", "montreal": "MTL",
    "st. louis": "STL", "st louis": "STL",
    "tampa bay": "TBL",
    "los angeles": "LAK", "la kings": "LAK",
    "new york rangers": "NYR", "ny rangers": "NYR",
    "new york islanders": "NYI", "ny islanders": "NYI",
    "new jersey": "NJD",
    "san jose": "SJS",
    "columbus": "CBJ", "blue jackets": "CBJ",
    "vegas": "VGK", "golden knights": "VGK",
    "utah": "UTA",
}


# ------------------------------------------------------------------ #
#  DailyFaceoff scraper (primary)                                     #
# ------------------------------------------------------------------ #

class _GoaliePageParser(HTMLParser):
    """Minimal HTML parser to extract goalie starters from DailyFaceoff.

    DailyFaceoff renders goalie matchup cards with team names, goalie
    names, and a status label (Confirmed / Expected / Unconfirmed).
    We look for elements containing these patterns and collect them.
    """

    def __init__(self):
        super().__init__()
        self._games: List[Dict[str, Any]] = []
        self._current_game: Dict[str, Any] = {}
        self._capture_text = False
        self._pending_tag = ""
        self._text_buf = ""
        self._in_matchup = False
        self._depth = 0

    def handle_starttag(self, tag, attrs):
        attr_dict = dict(attrs)
        cls = attr_dict.get("class", "")

        # Detect game/matchup container
        if "matchup" in cls.lower() or "starting-goalies" in cls.lower() or "goalie-card" in cls.lower():
            self._in_matchup = True
            self._depth += 1

        # Detect goalie name links or spans
        if tag == "a" and "player" in attr_dict.get("href", ""):
            self._capture_text = True
            self._pending_tag = "goalie_name"
            self._text_buf = ""

        # Detect status spans
        if "status" in cls.lower() or "confirmed" in cls.lower() or "expected" in cls.lower() or "unconfirmed" in cls.lower():
            self._capture_text = True
            self._pending_tag = "status"
            self._text_buf = ""

        # Detect team name
        if "team" in cls.lower() and ("name" in cls.lower() or "abbrev" in cls.lower()):
            self._capture_text = True
            self._pending_tag = "team"
            self._text_buf = ""

    def handle_data(self, data):
        if self._capture_text:
            self._text_buf += data

    def handle_endtag(self, tag):
        if self._capture_text and self._text_buf.strip():
            text = self._text_buf.strip()
            if self._pending_tag == "goalie_name":
                if "away_goalie" not in self._current_game:
                    self._current_game["away_goalie"] = text
                elif "home_goalie" not in self._current_game:
                    self._current_game["home_goalie"] = text
            elif self._pending_tag == "status":
                status = text.lower()
                if "away_status" not in self._current_game:
                    self._current_game["away_status"] = status
                elif "home_status" not in self._current_game:
                    self._current_game["home_status"] = status
            elif self._pending_tag == "team":
                if "away_team" not in self._current_game:
                    self._current_game["away_team"] = text
                elif "home_team" not in self._current_game:
                    self._current_game["home_team"] = text
            self._capture_text = False
            self._pending_tag = ""
            self._text_buf = ""

        if self._in_matchup and tag == "div":
            # If we have a complete game, save it
            if "away_goalie" in self._current_game and "home_goalie" in self._current_game:
                self._games.append(self._current_game)
                self._current_game = {}
                self._in_matchup = False

    @property
    def games(self):
        # Flush any remaining game
        if "away_goalie" in self._current_game and "home_goalie" in self._current_game:
            self._games.append(self._current_game)
            self._current_game = {}
        return self._games


def _parse_dailyfaceoff_html(html: str) -> List[Dict[str, str]]:
    """Parse DailyFaceoff starting goalies page using regex fallback.

    The HTML parser may miss elements on JS-heavy pages, so we also
    use regex to extract goalie matchup data from the raw HTML.
    """
    results = []

    # Try the HTML parser first
    parser = _GoaliePageParser()
    try:
        parser.feed(html)
        if parser.games:
            return parser.games
    except Exception:
        pass

    # Regex fallback: look for goalie names near team names
    # DailyFaceoff typically has patterns like:
    #   <a href="/players/...">Goalie Name</a>
    #   with nearby team info and status text

    # Find all player links (goalies)
    player_links = re.findall(
        r'<a[^>]*href="[^"]*player[^"]*"[^>]*>([^<]+)</a>',
        html, re.IGNORECASE,
    )

    # Find status indicators
    statuses = re.findall(
        r'(?:confirmed|expected|unconfirmed|likely|projected)',
        html, re.IGNORECASE,
    )

    # Find team abbreviations (3-letter codes in specific contexts)
    team_abbrs = re.findall(
        r'(?:class="[^"]*team[^"]*"[^>]*>)\s*([A-Z]{2,3})\s*<',
        html, re.IGNORECASE,
    )

    # Pair them up: every 2 goalies = 1 game (away, then home)
    for i in range(0, len(player_links) - 1, 2):
        game = {
            "away_goalie": player_links[i].strip(),
            "home_goalie": player_links[i + 1].strip(),
        }
        # Pair with statuses if available
        si = i  # status index aligns with goalie index
        if si < len(statuses):
            game["away_status"] = statuses[si].lower()
        if si + 1 < len(statuses):
            game["home_status"] = statuses[si + 1].lower()
        # Pair with teams if available
        if i < len(team_abbrs):
            game["away_team"] = team_abbrs[i].strip()
        if i + 1 < len(team_abbrs):
            game["home_team"] = team_abbrs[i + 1].strip()
        results.append(game)

    return results


async def _fetch_dailyfaceoff_starters(
    client: httpx.AsyncClient,
    target_date: date,
) -> List[Dict[str, str]]:
    """Scrape DailyFaceoff.com for today's starting goalies."""
    url = f"https://www.dailyfaceoff.com/starting-goalies/{target_date.isoformat()}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            logger.debug("DailyFaceoff returned %d", resp.status_code)
            return []
        games = _parse_dailyfaceoff_html(resp.text)
        logger.info("DailyFaceoff: parsed %d goalie matchups", len(games))
        return games
    except Exception as exc:
        logger.debug("DailyFaceoff scrape failed: %s", exc)
        return []


# ------------------------------------------------------------------ #
#  RotoWire scraper (secondary — lightweight HTML, no JS required)     #
# ------------------------------------------------------------------ #

# RotoWire uses short team names; map to NHL abbreviations
_ROTOWIRE_TEAM_MAP = {
    "ANA": "ANA", "ARI": "ARI", "BOS": "BOS", "BUF": "BUF",
    "CGY": "CGY", "CAR": "CAR", "CHI": "CHI", "COL": "COL",
    "CBJ": "CBJ", "DAL": "DAL", "DET": "DET", "EDM": "EDM",
    "FLA": "FLA", "LAK": "LAK", "LA": "LAK", "MIN": "MIN",
    "MTL": "MTL", "MON": "MTL", "NSH": "NSH", "NJD": "NJD",
    "NJ": "NJD", "NYI": "NYI", "NYR": "NYR", "OTT": "OTT",
    "PHI": "PHI", "PIT": "PIT", "SJS": "SJS", "SJ": "SJS",
    "SEA": "SEA", "STL": "STL", "TBL": "TBL", "TB": "TBL",
    "TOR": "TOR", "UTA": "UTA", "VAN": "VAN", "VGK": "VGK",
    "VEG": "VGK", "WSH": "WSH", "WAS": "WSH", "WPG": "WPG",
    "WIN": "WPG",
}


def _parse_rotowire_html(html: str) -> List[Dict[str, str]]:
    """Parse RotoWire goalie matchups page.

    RotoWire's goalie page has a simple HTML structure with matchup rows
    containing team abbreviations, goalie names, and status indicators.
    """
    results = []

    # RotoWire has matchup containers with teams and goalie names
    # Pattern: look for team abbreviations and goalie names near each other
    # Try to find matchup blocks first
    matchup_pattern = re.compile(
        r'class="[^"]*(?:lineup__matchup|goalie-matchup|matchup)[^"]*"',
        re.IGNORECASE,
    )

    # Extract goalie names from player links
    goalie_pattern = re.compile(
        r'<a[^>]*href="[^"]*hockey/player[^"]*"[^>]*>([^<]+)</a>',
        re.IGNORECASE,
    )
    goalies = goalie_pattern.findall(html)

    # Extract team abbreviations from lineup context
    team_pattern = re.compile(
        r'class="[^"]*lineup__abbr[^"]*"[^>]*>([A-Z]{2,3})<',
        re.IGNORECASE,
    )
    teams = team_pattern.findall(html)

    # Also try a broader team pattern
    if not teams:
        team_pattern2 = re.compile(
            r'<(?:span|div|a)[^>]*>\s*([A-Z]{2,3})\s*</(?:span|div|a)>',
        )
        # Filter to only known NHL abbreviations
        raw_teams = team_pattern2.findall(html)
        teams = [t for t in raw_teams if t.upper() in _ROTOWIRE_TEAM_MAP]

    # Status indicators
    status_pattern = re.compile(
        r'(?:Confirmed|Expected|Likely|Projected|Unconfirmed)',
        re.IGNORECASE,
    )
    statuses = status_pattern.findall(html)

    # Pair teams and goalies: every 2 teams + 2 goalies = 1 matchup
    for i in range(0, min(len(teams), len(goalies)) - 1, 2):
        game = {
            "away_team": _ROTOWIRE_TEAM_MAP.get(teams[i].upper(), teams[i].upper()),
            "home_team": _ROTOWIRE_TEAM_MAP.get(teams[i + 1].upper(), teams[i + 1].upper()),
            "away_goalie": goalies[i].strip(),
            "home_goalie": goalies[i + 1].strip(),
        }
        si = i
        if si < len(statuses):
            game["away_status"] = statuses[si].lower()
        if si + 1 < len(statuses):
            game["home_status"] = statuses[si + 1].lower()
        results.append(game)

    return results


async def _fetch_rotowire_starters(
    client: httpx.AsyncClient,
    target_date: date,
) -> List[Dict[str, str]]:
    """Scrape RotoWire.com for today's starting goalies.

    RotoWire serves lightweight HTML (no JS rendering needed) and is
    a reliable backup when DailyFaceoff blocks or requires JS.
    """
    url = "https://www.rotowire.com/hockey/goalie-matchups.php"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            logger.debug("RotoWire returned %d", resp.status_code)
            return []
        games = _parse_rotowire_html(resp.text)
        logger.info("RotoWire: parsed %d goalie matchups", len(games))
        return games
    except Exception as exc:
        logger.debug("RotoWire scrape failed: %s", exc)
        return []


def _match_team_abbrev(raw_name: str, db_teams: Dict[str, int]) -> Optional[str]:
    """Try to match a scraped team name/abbrev to our DB abbreviation."""
    raw = raw_name.strip().upper()
    if raw in db_teams:
        return raw

    # Try alias map
    raw_lower = raw_name.strip().lower()
    alias = _TEAM_ALIAS.get(raw_lower, "").upper()
    if alias and alias in db_teams:
        return alias

    # Fuzzy: check if raw is a substring of any team name
    for abbrev in db_teams:
        if raw_lower in abbrev.lower():
            return abbrev

    return None


# ------------------------------------------------------------------ #
#  NHL API scraper (secondary / fallback)                             #
# ------------------------------------------------------------------ #

async def _fetch_nhl_api_starters(
    client: httpx.AsyncClient,
    db: AsyncSession,
    game: Game,
) -> List[Dict[str, Any]]:
    """Extract starting goalie info from the NHL API landing page.

    Tries multiple paths through the response since the API structure
    varies by game state and season.
    """
    game_ext_id = game.external_id
    url = f"{NHL_API_BASE}/gamecenter/{game_ext_id}/landing"

    resp = await client.get(url)
    if resp.status_code != 200:
        logger.debug("NHL landing %s returned %d", game_ext_id, resp.status_code)
        return []

    data = resp.json()
    results: List[Dict[str, Any]] = []

    for side, is_home in [("homeTeam", True), ("awayTeam", False)]:
        team_id = game.home_team_id if is_home else game.away_team_id
        goalie_info = _extract_nhl_goalie(data, side)

        if not goalie_info:
            continue

        # Resolve team abbreviation
        team_block = data.get(side, {})
        team_abbrev = team_block.get("abbrev", "")
        if not team_abbrev:
            team_obj = await db.get(Team, team_id)
            team_abbrev = team_obj.abbreviation if team_obj else ""

        results.append({
            "game_id": game.id,
            "team_id": team_id,
            "team_abbrev": team_abbrev,
            "goalie_name": goalie_info["name"],
            "goalie_external_id": str(goalie_info.get("player_id", "")),
            "confirmed": goalie_info["confirmed"],
        })

    return results


def _extract_nhl_goalie(data: dict, side: str) -> Optional[Dict[str, Any]]:
    """Try multiple paths to extract a goalie from the NHL API response.

    The NHL API has used different structures across seasons:
    - matchup.goalieComparison.{side}.{name, playerId}
    - matchup.goalieComparison.{side}.starter.{name, playerId}
    - {side}.startingGoalie.{name, id}
    - summary.goalieComparison (some endpoints)
    """
    paths_to_try = [
        # Path 1: matchup.goalieComparison.homeTeam
        lambda: data.get("matchup", {}).get("goalieComparison", {}).get(side, {}),
        # Path 2: matchup.goalieComparison.homeTeam.starter
        lambda: data.get("matchup", {}).get("goalieComparison", {}).get(side, {}).get("starter", {}),
        # Path 3: top-level startingGoalie nested under team
        lambda: data.get(side, {}).get("startingGoalie", {}),
        # Path 4: summary section
        lambda: data.get("summary", {}).get("goalieComparison", {}).get(side, {}),
    ]

    for path_fn in paths_to_try:
        try:
            block = path_fn()
            if not block or not isinstance(block, dict):
                continue
            info = _parse_goalie_block(block)
            if info:
                return info
        except Exception:
            continue

    # Log what we found for debugging
    matchup = data.get("matchup", {})
    gc = matchup.get("goalieComparison", {})
    if gc:
        side_data = gc.get(side, {})
        logger.debug(
            "NHL API goalie extraction failed for %s. "
            "goalieComparison.%s keys: %s, sample: %.200s",
            side, side,
            list(side_data.keys()) if isinstance(side_data, dict) else type(side_data).__name__,
            str(side_data)[:200],
        )
    else:
        logger.debug(
            "NHL API: no goalieComparison in matchup. matchup keys: %s",
            list(matchup.keys()) if matchup else "empty",
        )

    return None


def _parse_goalie_block(block: dict) -> Optional[Dict[str, Any]]:
    """Parse a goalie info block from various NHL API structures."""
    name = ""
    player_id = None

    # Try name as dict (name.default) or string
    name_obj = block.get("name") or block.get("firstName", {})
    if isinstance(name_obj, dict):
        name = name_obj.get("default", "")
        if not name:
            # Try firstName + lastName pattern
            first = name_obj.get("default", "")
            last_obj = block.get("lastName", {})
            last = last_obj.get("default", "") if isinstance(last_obj, dict) else str(last_obj) if last_obj else ""
            name = f"{first} {last}".strip()
    elif isinstance(name_obj, str):
        name = name_obj

    # If name field didn't work, try firstName/lastName at top level
    if not name:
        first = block.get("firstName", "")
        last = block.get("lastName", "")
        if isinstance(first, dict):
            first = first.get("default", "")
        if isinstance(last, dict):
            last = last.get("default", "")
        if first or last:
            name = f"{first} {last}".strip()

    # Player ID
    player_id = block.get("playerId") or block.get("id") or block.get("player_id")

    if not name:
        return None

    # Confirmed status
    confirmed = bool(block.get("confirmed", False))

    return {
        "name": name,
        "player_id": player_id,
        "confirmed": confirmed,
    }


# ------------------------------------------------------------------ #
#  Main entry points                                                  #
# ------------------------------------------------------------------ #

async def sync_confirmed_starters(db: AsyncSession) -> List[Dict[str, Any]]:
    """Fetch confirmed starting goalies for today's games.

    Tries DailyFaceoff first (more reliable, available earlier),
    then falls back to the NHL API for any games not covered.

    Returns:
        List of dicts with keys: game_id, team_id, team_abbrev,
        goalie_name, goalie_external_id, confirmed (bool).
    """
    today = date.today()

    # Get today's games that haven't started yet
    stmt = select(Game).where(
        Game.date == today,
        Game.status.in_(("scheduled", "preview", "pre-game", "FUT", "PRE")),
    )
    result = await db.execute(stmt)
    games = result.scalars().all()

    if not games:
        logger.debug("Starter scraper: no upcoming games today")
        return []

    # Load team abbreviations for matching
    teams_result = await db.execute(select(Team).where(Team.active == True))
    all_teams = teams_result.scalars().all()
    team_by_abbrev: Dict[str, int] = {t.abbreviation.upper(): t.id for t in all_teams}
    team_by_id: Dict[int, str] = {t.id: t.abbreviation for t in all_teams}

    starters: List[Dict[str, Any]] = []
    covered_game_ids: set = set()

    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        # Source 1: DailyFaceoff
        dfo_games = await _fetch_dailyfaceoff_starters(client, today)
        if dfo_games:
            for dfo_game in dfo_games:
                # Try to match DFO game to our DB games
                away_abbrev = _match_team_abbrev(
                    dfo_game.get("away_team", ""), team_by_abbrev,
                )
                home_abbrev = _match_team_abbrev(
                    dfo_game.get("home_team", ""), team_by_abbrev,
                )

                if not away_abbrev or not home_abbrev:
                    continue

                away_team_id = team_by_abbrev.get(away_abbrev)
                home_team_id = team_by_abbrev.get(home_abbrev)

                # Find matching game in our DB
                for game in games:
                    if game.home_team_id == home_team_id and game.away_team_id == away_team_id:
                        for side, goalie_key, status_key, tid, abbrev in [
                            ("away", "away_goalie", "away_status", away_team_id, away_abbrev),
                            ("home", "home_goalie", "home_status", home_team_id, home_abbrev),
                        ]:
                            goalie_name = dfo_game.get(goalie_key, "")
                            status = dfo_game.get(status_key, "")
                            if goalie_name:
                                starters.append({
                                    "game_id": game.id,
                                    "team_id": tid,
                                    "team_abbrev": abbrev,
                                    "goalie_name": goalie_name,
                                    "goalie_external_id": "",
                                    "confirmed": "confirm" in status,
                                })
                        covered_game_ids.add(game.id)
                        break

        # Source 2: RotoWire — lightweight HTML, no JS needed.
        # Fills in any games DailyFaceoff missed (DFO often returns 0
        # due to JS rendering requirements).
        uncovered_rw = [g for g in games if g.id not in covered_game_ids]
        if uncovered_rw:
            rw_games = await _fetch_rotowire_starters(client, today)
            for rw_game in rw_games:
                away_abbrev = rw_game.get("away_team", "").upper()
                home_abbrev = rw_game.get("home_team", "").upper()

                # Normalize through alias map
                if away_abbrev not in team_by_abbrev:
                    away_abbrev = _match_team_abbrev(away_abbrev, team_by_abbrev) or ""
                if home_abbrev not in team_by_abbrev:
                    home_abbrev = _match_team_abbrev(home_abbrev, team_by_abbrev) or ""

                if not away_abbrev or not home_abbrev:
                    continue

                away_team_id = team_by_abbrev.get(away_abbrev)
                home_team_id = team_by_abbrev.get(home_abbrev)

                for game in uncovered_rw:
                    if game.home_team_id == home_team_id and game.away_team_id == away_team_id:
                        for side, goalie_key, status_key, tid, abbrev in [
                            ("away", "away_goalie", "away_status", away_team_id, away_abbrev),
                            ("home", "home_goalie", "home_status", home_team_id, home_abbrev),
                        ]:
                            goalie_name = rw_game.get(goalie_key, "")
                            status = rw_game.get(status_key, "")
                            if goalie_name:
                                starters.append({
                                    "game_id": game.id,
                                    "team_id": tid,
                                    "team_abbrev": abbrev,
                                    "goalie_name": goalie_name,
                                    "goalie_external_id": "",
                                    "confirmed": "confirm" in status,
                                })
                        covered_game_ids.add(game.id)
                        break

        # Source 3: NHL API — fill in any games still uncovered
        uncovered = [g for g in games if g.id not in covered_game_ids]
        if uncovered:
            for game in uncovered:
                try:
                    game_starters = await _fetch_nhl_api_starters(client, db, game)
                    # Only keep starters that have an actual goalie name
                    game_starters = [s for s in game_starters if s.get("goalie_name")]
                    starters.extend(game_starters)
                    if game_starters:
                        covered_game_ids.add(game.id)
                except Exception as exc:
                    logger.debug(
                        "NHL API starters failed for game %s: %s",
                        game.external_id, exc,
                    )

    confirmed_count = sum(1 for s in starters if s["confirmed"])
    logger.info(
        "Starter scraper: %d starters found (%d confirmed) for %d games "
        "(%d from DFO, %d from NHL API)",
        len(starters), confirmed_count, len(games),
        sum(1 for s in starters if not s.get("goalie_external_id")),
        sum(1 for s in starters if s.get("goalie_external_id")),
    )
    return starters


async def get_confirmed_starter_for_team(
    db: AsyncSession,
    game_id: int,
    team_id: int,
) -> Optional[Dict[str, Any]]:
    """Look up the confirmed starter for a specific team in a game.

    Returns:
        Dict with goalie_name, goalie_external_id, confirmed, or None.
    """
    starters = await sync_confirmed_starters(db)
    for s in starters:
        if s["game_id"] == game_id and s["team_id"] == team_id:
            return s
    return None
