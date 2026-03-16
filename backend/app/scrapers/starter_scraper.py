"""
NHL confirmed starting goalie scraper.

Data sources, tried in order:
1. DailyFaceoff.com — embeddable widget + main page scraping
2. RotoWire.com — embeddable/static endpoint
3. NHL API gamecenter landing page — official but often only populated
   very close to puck drop.

All sources are scraped per-day and merged.  DailyFaceoff is the
primary source; the NHL API fills in any gaps.
"""

import logging
import re
from datetime import date
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import func, select
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

# DailyFaceoff is a Next.js SSR site. The main page at
# www.dailyfaceoff.com returns server-rendered HTML with goalie data
# embedded directly. The publish.dailyfaceoff.com widget is defunct.

_DFO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:148.0) "
        "Gecko/20100101 Firefox/148.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}

# Map DFO team slugs (from /teams/SLUG/) to NHL abbreviations
_DFO_SLUG_MAP = {
    "anaheim-ducks": "ANA", "boston-bruins": "BOS", "buffalo-sabres": "BUF",
    "calgary-flames": "CGY", "carolina-hurricanes": "CAR",
    "chicago-blackhawks": "CHI", "colorado-avalanche": "COL",
    "columbus-blue-jackets": "CBJ", "dallas-stars": "DAL",
    "detroit-red-wings": "DET", "edmonton-oilers": "EDM",
    "florida-panthers": "FLA", "los-angeles-kings": "LAK",
    "minnesota-wild": "MIN", "montreal-canadiens": "MTL",
    "nashville-predators": "NSH", "new-jersey-devils": "NJD",
    "new-york-islanders": "NYI", "new-york-rangers": "NYR",
    "ottawa-senators": "OTT", "philadelphia-flyers": "PHI",
    "pittsburgh-penguins": "PIT", "san-jose-sharks": "SJS",
    "seattle-kraken": "SEA", "st-louis-blues": "STL",
    "tampa-bay-lightning": "TBL", "toronto-maple-leafs": "TOR",
    "utah-mammoth": "UTA", "utah-hockey-club": "UTA",
    "vancouver-canucks": "VAN", "vegas-golden-knights": "VGK",
    "washington-capitals": "WSH", "winnipeg-jets": "WPG",
}


def _parse_dfo_html(html: str) -> List[Dict[str, str]]:
    """Parse DailyFaceoff Next.js SSR HTML for goalie matchups.

    Each game is an <article> block containing:
    - Team matchup in a header span (text-3xl text-white)
    - Two goalie names in spans (text-lg xl:text-2xl)
    - Status indicators (Confirmed/Unconfirmed) in spans
    - Team slugs in /teams/SLUG/ links
    """
    results: List[Dict[str, str]] = []

    # Split HTML into <article> blocks — each is one game matchup
    articles = re.split(r'<article[^>]*>', html)
    if len(articles) < 2:
        return results

    for article_html in articles[1:]:  # skip content before first <article>
        # Cut at </article> to avoid bleeding into next game
        end = article_html.find("</article>")
        if end > 0:
            article_html = article_html[:end]

        # Extract goalie names from spans with text-lg xl:text-2xl
        goalie_names = re.findall(
            r'<span[^>]*class="[^"]*text-lg[^"]*text-2xl[^"]*"[^>]*>'
            r'\s*([^<]{2,40})\s*</span>',
            article_html,
        )
        if len(goalie_names) < 2:
            continue

        # Extract confirmation status (Confirmed/Unconfirmed/Expected)
        # These appear as >Confirmed</span> or >Unconfirmed</span>
        statuses = re.findall(
            r'>(?:Confirmed|Unconfirmed|Expected|Likely|Projected)</span>',
            article_html,
        )
        # Clean to just the status word
        statuses = [
            re.search(r'(Confirmed|Unconfirmed|Expected|Likely|Projected)', s).group(1).lower()
            for s in statuses
        ]

        # Extract team slugs from /teams/SLUG/ links
        team_slugs = re.findall(
            r'/teams/([a-z0-9-]+)/(?:line-combinations|news|schedule)',
            article_html,
        )
        # Deduplicate while preserving order
        seen: set = set()
        unique_slugs = []
        for s in team_slugs:
            if s not in seen:
                seen.add(s)
                unique_slugs.append(s)
        team_slugs = unique_slugs

        game: Dict[str, str] = {
            "away_goalie": goalie_names[0].strip(),
            "home_goalie": goalie_names[1].strip(),
        }

        if len(statuses) >= 1:
            game["away_status"] = statuses[0]
        if len(statuses) >= 2:
            game["home_status"] = statuses[1]

        if len(team_slugs) >= 1:
            abbrev = _DFO_SLUG_MAP.get(team_slugs[0], "")
            if abbrev:
                game["away_team"] = abbrev
        if len(team_slugs) >= 2:
            abbrev = _DFO_SLUG_MAP.get(team_slugs[1], "")
            if abbrev:
                game["home_team"] = abbrev

        results.append(game)

    return results


async def _fetch_dailyfaceoff_starters(
    client: httpx.AsyncClient,
    target_date: date,
) -> List[Dict[str, str]]:
    """Fetch starting goalies from DailyFaceoff.

    The main site is a Next.js SSR app that returns server-rendered
    HTML with goalie data embedded. Date-specific URLs are tried first.
    """
    urls = [
        # Date-specific page (SSR, no JS needed)
        f"https://www.dailyfaceoff.com/starting-goalies/{target_date.isoformat()}",
        # Default page (today's games)
        "https://www.dailyfaceoff.com/starting-goalies",
    ]

    for url in urls:
        try:
            resp = await client.get(url, headers=_DFO_HEADERS)
            if resp.status_code != 200:
                logger.debug("DailyFaceoff %s returned %d", url, resp.status_code)
                continue

            html = resp.text
            games = _parse_dfo_html(html)
            if games:
                logger.info(
                    "DailyFaceoff: parsed %d goalie matchups from %s",
                    len(games), url,
                )
                return games

            logger.info(
                "DailyFaceoff: 0 matchups from %s (%d bytes)",
                url, len(html),
            )
        except Exception as exc:
            logger.debug("DailyFaceoff %s failed: %s", url, exc)
            continue

    logger.warning("DailyFaceoff: all URLs failed to produce matchups")
    return []


# ------------------------------------------------------------------ #
#  RotoWire — JS SPA, cannot be scraped without a browser.             #
#  Kept as a stub; may be re-enabled if a static endpoint is found.    #
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


async def _fetch_rotowire_starters(
    client: httpx.AsyncClient,
    target_date: date,
) -> List[Dict[str, str]]:
    """RotoWire is a JS SPA — skip it to avoid wasted requests.

    The page at rotowire.com/hockey/starting-goalies.php returns a
    306 KB JavaScript shell with no goalie data in the HTML. Parsing
    it requires a headless browser which we don't have.
    """
    logger.debug("RotoWire: skipped (requires JS rendering)")
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
            "status": "Confirmed" if goalie_info["confirmed"] else "Projected",
        })

    return results


def _extract_nhl_goalie(data: dict, side: str) -> Optional[Dict[str, Any]]:
    """Try multiple paths to extract a goalie from the NHL API response.

    The NHL API has used different structures across seasons:
    - matchup.goalieComparison.{side}.leaders[0] (2025-26 format)
    - matchup.goalieComparison.{side}.{name, playerId}
    - matchup.goalieComparison.{side}.starter.{name, playerId}
    - {side}.startingGoalie.{name, id}
    - summary.goalieComparison (some endpoints)
    """
    def _gc_leaders_first():
        """2025-26 format: goalieComparison.{side}.leaders is a list of goalies."""
        leaders = (
            data.get("matchup", {})
            .get("goalieComparison", {})
            .get(side, {})
            .get("leaders", [])
        )
        if leaders and isinstance(leaders, list):
            return leaders[0]
        return {}

    paths_to_try = [
        # Path 1 (2025-26): matchup.goalieComparison.homeTeam.leaders[0]
        _gc_leaders_first,
        # Path 2: matchup.goalieComparison.homeTeam directly
        lambda: data.get("matchup", {}).get("goalieComparison", {}).get(side, {}),
        # Path 3: matchup.goalieComparison.homeTeam.starter
        lambda: data.get("matchup", {}).get("goalieComparison", {}).get(side, {}).get("starter", {}),
        # Path 4: top-level startingGoalie nested under team
        lambda: data.get(side, {}).get("startingGoalie", {}),
        # Path 5: summary section
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

    # Prefer firstName + lastName (full names) over name (often abbreviated)
    first_obj = block.get("firstName", "")
    last_obj = block.get("lastName", "")
    first = first_obj.get("default", "") if isinstance(first_obj, dict) else str(first_obj) if first_obj else ""
    last = last_obj.get("default", "") if isinstance(last_obj, dict) else str(last_obj) if last_obj else ""
    if first or last:
        name = f"{first} {last}".strip()

    # Fall back to name field (may be abbreviated like "C. Hellebuyck")
    if not name:
        name_obj = block.get("name", "")
        if isinstance(name_obj, dict):
            name = name_obj.get("default", "")
        elif isinstance(name_obj, str):
            name = name_obj

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

    # Get today's games (include pregame/live so starters can be resolved
    # even after the schedule status changes from "scheduled" to "pregame")
    stmt = select(Game).where(
        Game.date == today,
        func.lower(Game.status).in_((
            "scheduled", "preview", "pre-game", "pregame",
            "fut", "pre", "in_progress", "live",
        )),
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
            logger.info(
                "DFO raw data: %s",
                [{k: v for k, v in g.items()} for g in dfo_games],
            )
            for dfo_game in dfo_games:
                raw_away = dfo_game.get("away_team", "")
                raw_home = dfo_game.get("home_team", "")
                # Try to match DFO game to our DB games
                away_abbrev = _match_team_abbrev(raw_away, team_by_abbrev)
                home_abbrev = _match_team_abbrev(raw_home, team_by_abbrev)

                if not away_abbrev or not home_abbrev:
                    logger.warning(
                        "DFO team match failed: away=%r→%s, home=%r→%s, "
                        "goalies=%s vs %s",
                        raw_away, away_abbrev, raw_home, home_abbrev,
                        dfo_game.get("away_goalie", "?"),
                        dfo_game.get("home_goalie", "?"),
                    )
                    continue

                away_team_id = team_by_abbrev.get(away_abbrev)
                home_team_id = team_by_abbrev.get(home_abbrev)

                # Find matching game in our DB
                matched = False
                for game in games:
                    if game.home_team_id == home_team_id and game.away_team_id == away_team_id:
                        matched = True
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
                                    "confirmed": status.lower() == "confirmed",
                                    "status": status.strip().title() if status else "Projected",
                                })
                        covered_game_ids.add(game.id)
                        break
                if not matched:
                    logger.warning(
                        "DFO game not found in DB: %s@%s (ids %s@%s), "
                        "DB games: %s",
                        away_abbrev, home_abbrev,
                        away_team_id, home_team_id,
                        [(g.away_team_id, g.home_team_id) for g in games],
                    )

        # Source 2: RotoWire — currently disabled (JS SPA).
        # Kept as a stub in case a static endpoint is found later.
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
                                    "confirmed": status.lower() == "confirmed",
                                    "status": status.strip().title() if status else "Projected",
                                })
                        covered_game_ids.add(game.id)
                        break

        # Source 3: NHL API — fill in any games still uncovered
        uncovered = [g for g in games if g.id not in covered_game_ids]
        if uncovered:
            logger.info(
                "NHL API: checking %d uncovered games: %s",
                len(uncovered),
                [g.external_id for g in uncovered],
            )
            for game in uncovered:
                try:
                    game_starters = await _fetch_nhl_api_starters(client, db, game)
                    # Only keep starters that have an actual goalie name
                    game_starters = [s for s in game_starters if s.get("goalie_name")]
                    starters.extend(game_starters)
                    if game_starters:
                        covered_game_ids.add(game.id)
                        logger.info(
                            "NHL API found starters for %s: %s",
                            game.external_id,
                            [(s["team_abbrev"], s["goalie_name"], s["status"]) for s in game_starters],
                        )
                except Exception as exc:
                    logger.warning(
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
