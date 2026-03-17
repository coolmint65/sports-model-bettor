"""
ESPN NHL data scraper.

Fetches team statistics, standings, and power rankings from ESPN's public API
to supplement data from the NHL API. ESPN provides PP%, PK%, shots per game,
faceoff win %, and other key stats that the NHL standings endpoint lacks.

ESPN Public API base: https://site.api.espn.com/apis/site/v2/sports/hockey/nhl
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.team import Team, TeamStats
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# ESPN API team abbreviation mapping to NHL standard abbreviations
# ESPN uses slightly different codes for some teams
ESPN_ABBREV_MAP = {
    "WSH": "WSH",
    "TB": "TBL",
    "SJ": "SJS",
    "NJ": "NJD",
    "LA": "LAK",
    "NY": "NYR",  # Sometimes ESPN uses NY for Rangers
    "MON": "MTL",
    "CLB": "CBJ",
    "WPG": "WPG",
    "UTA": "UTA",
}


def _normalize_abbrev(espn_abbrev: str) -> str:
    """Convert ESPN abbreviation to NHL standard."""
    return ESPN_ABBREV_MAP.get(espn_abbrev, espn_abbrev)


class ESPNScraper(BaseScraper):
    """
    Scraper for ESPN's public NHL API.

    Provides team-level statistics that complement the NHL standings data:
    - Power play percentage
    - Penalty kill percentage
    - Shots for/against per game
    - Faceoff win percentage
    - Save percentage
    - Goals for/against per game
    """

    # Cache ESPN responses for 10 minutes — stats update slowly.
    DEFAULT_CACHE_TTL = 600.0

    def __init__(self, **kwargs):
        super().__init__(
            base_url="https://site.api.espn.com/apis/site/v2/sports/hockey/nhl",
            rate_limit=0.5,
            **kwargs,
        )

    async def sync_all(self, db_session: AsyncSession) -> None:
        """Run full ESPN data sync."""
        await self.sync_team_stats(db_session)

    async def fetch_teams(self) -> List[Dict[str, Any]]:
        """Fetch list of all NHL teams from ESPN."""
        data = await self.fetch_json("/teams", params={"limit": 50})
        teams = []
        for group in data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", []):
            team = group.get("team", {})
            if team:
                teams.append({
                    "espn_id": team.get("id"),
                    "abbreviation": team.get("abbreviation", ""),
                    "name": team.get("displayName", ""),
                    "short_name": team.get("shortDisplayName", ""),
                    "location": team.get("location", ""),
                    "logo": team.get("logos", [{}])[0].get("href") if team.get("logos") else None,
                })
        logger.info("Fetched %d teams from ESPN", len(teams))
        return teams

    async def fetch_team_statistics(self, espn_team_id: str) -> Dict[str, Any]:
        """
        Fetch detailed statistics for a specific team.

        Returns dict with stat categories and values.
        """
        data = await self.fetch_json(f"/teams/{espn_team_id}/statistics")
        return data

    async def fetch_all_team_stats(self) -> Dict[str, Dict[str, float]]:
        """
        Fetch stats for all teams and return a dict keyed by NHL abbreviation.

        Returns:
            Dict[abbrev, {pp_pct, pk_pct, shots_per_game, shots_against_per_game,
                          faceoff_pct, save_pct, goals_per_game, goals_against_per_game}]
        """
        teams = await self.fetch_teams()
        all_stats = {}

        for team_info in teams:
            espn_id = team_info.get("espn_id")
            espn_abbrev = team_info.get("abbreviation", "")
            nhl_abbrev = _normalize_abbrev(espn_abbrev)

            if not espn_id:
                continue

            try:
                data = await self.fetch_team_statistics(espn_id)
                stats = self._parse_team_stats(data)
                if stats:
                    all_stats[nhl_abbrev] = stats
                    logger.debug("ESPN stats for %s: %s", nhl_abbrev, stats)
            except Exception as exc:
                logger.warning("Failed to fetch ESPN stats for %s: %s", nhl_abbrev, exc)
                continue

        logger.info("Fetched ESPN stats for %d teams", len(all_stats))
        return all_stats

    def _parse_team_stats(self, data: Dict[str, Any]) -> Optional[Dict[str, float]]:
        """
        Parse ESPN team statistics response into a flat dict of key stats.

        ESPN returns stats in categories with nested stat items.
        """
        stats = {}

        # Navigate the ESPN response structure
        # ESPN returns: {results: {stats: {categories: [...]}}}
        results = data.get("results", {})
        if not results:
            # Alternative structure: {statistics: {splits: {categories: [...]}}}
            results = data

        # Try to find stats in various ESPN response formats
        categories = []

        # Format 1: results.stats.categories
        if "stats" in results:
            categories = results["stats"].get("categories", [])

        # Format 2: statistics.splits.categories
        if not categories and "statistics" in results:
            for split in results.get("statistics", {}).get("splits", []):
                categories.extend(split.get("categories", []))

        # Format 3: Direct categories list
        if not categories:
            categories = results.get("categories", [])

        # Build a flat lookup of stat name → value
        stat_lookup = {}
        for category in categories:
            for stat_item in category.get("stats", []):
                name = stat_item.get("name", "").lower()
                value = stat_item.get("value")
                if value is not None:
                    try:
                        stat_lookup[name] = float(value)
                    except (ValueError, TypeError):
                        pass

        if not stat_lookup:
            return None

        # Map ESPN stat names to our internal names
        # ESPN uses various stat name formats
        stat_name_map = {
            # Power play
            "powerplaygoals": "pp_goals",
            "powerplayopportunities": "pp_opportunities",
            "powerplaypct": "pp_pct",
            "powerplaypercentage": "pp_pct",
            "ppPct": "pp_pct",
            # Penalty kill
            "penaltykillpct": "pk_pct",
            "penaltykillpercentage": "pk_pct",
            "pkPct": "pk_pct",
            # Shots
            "shotsperGame": "shots_per_game",
            "shotspergame": "shots_per_game",
            "shotsagainstpergame": "shots_against_per_game",
            # Faceoff
            "faceoffwinpct": "faceoff_pct",
            "faceoffwinpercentage": "faceoff_pct",
            "faceoffpct": "faceoff_pct",
            # Save
            "savepct": "save_pct",
            "savepercentage": "save_pct",
            # Goals
            "goalspergame": "goals_per_game",
            "goalsagainstpergame": "goals_against_per_game",
            "goalsfor": "goals_for",
            "goalsagainst": "goals_against",
            "gamesplayed": "games_played",
        }

        result = {}
        for espn_name, our_name in stat_name_map.items():
            val = stat_lookup.get(espn_name.lower())
            if val is not None:
                result[our_name] = val

        # Compute derived stats if components are available
        gp = result.get("games_played", 0)
        if gp > 0:
            if "goals_per_game" not in result and "goals_for" in result:
                result["goals_per_game"] = round(result["goals_for"] / gp, 2)
            if "goals_against_per_game" not in result and "goals_against" in result:
                result["goals_against_per_game"] = round(result["goals_against"] / gp, 2)

        # Compute PP% from goals/opportunities if percentage not directly available
        if "pp_pct" not in result and "pp_goals" in result and "pp_opportunities" in result:
            opp = result["pp_opportunities"]
            if opp > 0:
                result["pp_pct"] = round(result["pp_goals"] / opp * 100, 1)

        return result if result else None

    async def sync_team_stats(self, db: AsyncSession) -> int:
        """
        Fetch ESPN team stats and update TeamStats records in the database.

        Only updates fields that are currently NULL in the database,
        so ESPN acts as a supplement, not an override.

        Returns:
            Number of teams updated.
        """
        all_stats = await self.fetch_all_team_stats()
        if not all_stats:
            logger.warning("No ESPN stats fetched, skipping sync")
            return 0

        updated = 0
        for abbrev, espn_stats in all_stats.items():
            # Find the team
            result = await db.execute(
                select(Team).where(Team.abbreviation == abbrev)
            )
            team = result.scalar_one_or_none()
            if not team:
                logger.debug("No team found for ESPN abbrev %s", abbrev)
                continue

            # Find latest TeamStats
            result = await db.execute(
                select(TeamStats)
                .where(TeamStats.team_id == team.id)
                .order_by(TeamStats.season.desc())
                .limit(1)
            )
            stats = result.scalar_one_or_none()
            if not stats:
                continue

            # Update NULL fields with ESPN data
            changed = False

            if stats.power_play_pct is None and "pp_pct" in espn_stats:
                stats.power_play_pct = espn_stats["pp_pct"]
                changed = True

            if stats.penalty_kill_pct is None and "pk_pct" in espn_stats:
                stats.penalty_kill_pct = espn_stats["pk_pct"]
                changed = True

            if stats.shots_for_per_game is None and "shots_per_game" in espn_stats:
                stats.shots_for_per_game = espn_stats["shots_per_game"]
                changed = True

            if stats.shots_against_per_game is None and "shots_against_per_game" in espn_stats:
                stats.shots_against_per_game = espn_stats["shots_against_per_game"]
                changed = True

            if stats.faceoff_win_pct is None and "faceoff_pct" in espn_stats:
                stats.faceoff_win_pct = espn_stats["faceoff_pct"]
                changed = True

            if changed:
                stats.date_updated = datetime.now(timezone.utc)
                updated += 1

        await db.flush()
        logger.info("Updated %d teams with ESPN stats", updated)
        return updated
