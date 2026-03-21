"""
ESPN sports data scrapers.

Fetches team statistics from ESPN's public API to supplement data from
league-specific APIs. ESPN provides stats that other APIs may require
paid tiers for.

ESPN Public API bases:
  NHL: https://site.api.espn.com/apis/site/v2/sports/hockey/nhl
  NBA: https://site.api.espn.com/apis/site/v2/sports/basketball/nba
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
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
                select(Team).where(
                    Team.abbreviation == abbrev,
                    Team.sport == "nhl",
                ).order_by(Team.id)
            )
            team = result.scalars().first()
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


# -----------------------------------------------------------------------
# ESPN NBA scraper
# -----------------------------------------------------------------------

# ESPN uses slightly different abbreviations for some NBA teams
ESPN_NBA_ABBREV_MAP = {
    "GS": "GSW",
    "SA": "SAS",
    "NO": "NOP",
    "NY": "NYK",
    "WSH": "WAS",
    "UTAH": "UTA",
    "PHX": "PHX",
}


def _normalize_nba_abbrev(espn_abbrev: str) -> str:
    """Convert ESPN NBA abbreviation to our canonical abbreviation."""
    return ESPN_NBA_ABBREV_MAP.get(espn_abbrev, espn_abbrev)


class ESPNNBAScraper(BaseScraper):
    """
    Scraper for ESPN's public NBA API.

    Provides team-level statistics as a free alternative to paid
    endpoints on other APIs: FG%, 3PT%, FT%, rebounds, assists,
    turnovers, steals, blocks, pace, and offensive/defensive rating.
    """

    DEFAULT_CACHE_TTL = 600.0  # 10 minutes

    def __init__(self, **kwargs):
        super().__init__(
            base_url="https://site.api.espn.com/apis/site/v2/sports/basketball/nba",
            rate_limit=0.5,
            **kwargs,
        )

    async def sync_all(self, db_session: AsyncSession) -> None:
        """Run full ESPN NBA data sync."""
        await self.sync_team_stats(db_session)

    async def fetch_teams(self) -> List[Dict[str, Any]]:
        """Fetch list of all NBA teams from ESPN."""
        data = await self.fetch_json("/teams", params={"limit": 50})
        teams = []
        for group in data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", []):
            team = group.get("team", {})
            if team:
                teams.append({
                    "espn_id": team.get("id"),
                    "abbreviation": team.get("abbreviation", ""),
                    "name": team.get("displayName", ""),
                })
        logger.info("Fetched %d NBA teams from ESPN", len(teams))
        return teams

    async def fetch_team_statistics(self, espn_team_id: str) -> Dict[str, Any]:
        """Fetch detailed statistics for a specific NBA team."""
        return await self.fetch_json(f"/teams/{espn_team_id}/statistics")

    def _parse_team_stats(self, data: Dict[str, Any]) -> Optional[Dict[str, float]]:
        """Parse ESPN NBA team statistics response into a flat dict.

        ESPN returns stats in several different formats depending on the
        endpoint version and sport.  This parser handles all known layouts:

        1. ``{results: {stats: {categories: [...]}}}``
        2. ``{statistics: {splits: [{categories: [...]}]}}``
        3. ``{statistics: [{splits: [{categories: [...]}]}]}``  (list)
        4. ``{results: {statistics: [{categories: [...]}]}}``
        5. Top-level ``{categories: [...]}``
        6. Flat ``{splitCategories: [{stats: [...]}]}``
        """
        stat_lookup: Dict[str, float] = {}

        results = data.get("results", data)
        categories: list = []

        # Format 1: results.stats.categories
        if "stats" in results:
            stats_obj = results["stats"]
            if isinstance(stats_obj, dict):
                categories = stats_obj.get("categories", [])

        # Format 2/3: statistics (dict or list)
        if not categories and "statistics" in results:
            statistics = results["statistics"]
            if isinstance(statistics, list):
                # Format 3: statistics is a list of split groups
                for stat_group in statistics:
                    if isinstance(stat_group, dict):
                        # Each group may have direct categories
                        categories.extend(stat_group.get("categories", []))
                        # Or nested splits
                        for split in stat_group.get("splits", []):
                            if isinstance(split, dict):
                                categories.extend(split.get("categories", []))
            elif isinstance(statistics, dict):
                # Format 2: statistics.splits.categories
                splits = statistics.get("splits", [])
                if isinstance(splits, list):
                    for split in splits:
                        if isinstance(split, dict):
                            categories.extend(split.get("categories", []))
                # Also try direct categories on the statistics object
                categories.extend(statistics.get("categories", []))

        # Format 6: splitCategories (newer ESPN format)
        if not categories:
            split_cats = results.get("splitCategories", [])
            if isinstance(split_cats, list):
                categories.extend(split_cats)

        # Format 5: top-level categories
        if not categories:
            categories = results.get("categories", [])

        for category in categories:
            for stat_item in category.get("stats", []):
                value = stat_item.get("value")
                if value is None:
                    continue
                try:
                    fval = float(value)
                except (ValueError, TypeError):
                    continue
                # Index by every available identifier so we match regardless
                # of which naming convention this ESPN API version uses.
                for key_field in ("name", "displayName", "abbreviation",
                                  "shortDisplayName"):
                    raw = stat_item.get(key_field, "")
                    if raw:
                        stat_lookup[raw.lower()] = fval
                # ESPN often provides a per-game value separately
                per_game = stat_item.get("perGame")
                if per_game is not None:
                    try:
                        pg_val = float(per_game)
                        name = stat_item.get("name", "")
                        if name:
                            stat_lookup[name.lower() + "pergame"] = pg_val
                    except (ValueError, TypeError):
                        pass

        if not stat_lookup:
            # Log what keys we DID find in the response for diagnostics
            logger.warning(
                "ESPN NBA stat_lookup empty. Response keys: %s, categories found: %d",
                list(data.keys())[:10] if isinstance(data, dict) else type(data).__name__,
                len(categories),
            )
            return None

        # Log raw stat names at INFO level to diagnose mapping gaps
        logger.info("ESPN NBA raw stat names: %s", sorted(stat_lookup.keys())[:40])

        # Map ESPN stat names (lowercased) to our internal field names.
        # ESPN uses various naming conventions across API versions:
        #   - Long camelCase: "fieldGoalPct", "reboundsPerGame"
        #   - Abbreviated: "FG%", "REB", "AST", "STL"
        #   - Display names: "Field Goal Pct", "Rebounds Per Game"
        # We index by name, displayName, abbreviation, and shortDisplayName
        # so all conventions are covered.
        stat_name_map = {
            # Shooting percentages -- long form
            "fieldgoalpct": "fg_pct",
            "fieldgoalpercentage": "fg_pct",
            "fgpct": "fg_pct",
            "field goal pct": "fg_pct",
            "field goal percentage": "fg_pct",
            "field goals percentage": "fg_pct",
            "fg%": "fg_pct",
            "threepointfieldgoalpct": "three_pt_pct",
            "threepointfieldgoalpercentage": "three_pt_pct",
            "3ptpct": "three_pt_pct",
            "threepointpct": "three_pt_pct",
            "3pt%": "three_pt_pct",
            "3p%": "three_pt_pct",
            "threepointers%": "three_pt_pct",
            "three point pct": "three_pt_pct",
            "three point field goal pct": "three_pt_pct",
            "three point field goal percentage": "three_pt_pct",
            "3-point field goal pct": "three_pt_pct",
            "three point %": "three_pt_pct",
            "freethrowpct": "ft_pct",
            "freethrowpercentage": "ft_pct",
            "ftpct": "ft_pct",
            "ft%": "ft_pct",
            "free throw pct": "ft_pct",
            "free throw percentage": "ft_pct",
            # Per-game averages -- long form
            "reboundspergame": "rebounds_per_game",
            "avgrebounds": "rebounds_per_game",
            "totalreboundspergame": "rebounds_per_game",
            "rebpergame": "rebounds_per_game",
            "rebounds per game": "rebounds_per_game",
            "total rebounds per game": "rebounds_per_game",
            "assistspergame": "assists_per_game",
            "avgassists": "assists_per_game",
            "astpergame": "assists_per_game",
            "assists per game": "assists_per_game",
            "turnoverspergame": "turnovers_per_game",
            "avgturnovers": "turnovers_per_game",
            "tovpergame": "turnovers_per_game",
            "turnovers per game": "turnovers_per_game",
            "stealspergame": "steals_per_game",
            "avgsteals": "steals_per_game",
            "stlpergame": "steals_per_game",
            "steals per game": "steals_per_game",
            "blockspergame": "blocks_per_game",
            "avgblocks": "blocks_per_game",
            "blkpergame": "blocks_per_game",
            "blocks per game": "blocks_per_game",
            "pointspergame": "points_per_game",
            "avgpoints": "points_per_game",
            "ppg": "points_per_game",
            "points per game": "points_per_game",
            "threepointfieldgoalsmadepergame": "three_pt_made_per_game",
            "avg3pointfieldgoalsmade": "three_pt_made_per_game",
            "threepointsmadepergame": "three_pt_made_per_game",
            "3ptmadepergame": "three_pt_made_per_game",
            "three point field goals made per game": "three_pt_made_per_game",
            # ESPN abbreviated stat names (common in splitCategories format)
            "reb": "rebounds_per_game",
            "ast": "assists_per_game",
            "stl": "steals_per_game",
            "blk": "blocks_per_game",
            "tov": "turnovers_per_game",
            "to": "turnovers_per_game",
            "3pm": "three_pt_made_per_game",
            "pts": "points_per_game",
            "gp": "games_played",
            # Advanced
            "offensiverating": "offensive_rating",
            "offrtg": "offensive_rating",
            "ortg": "offensive_rating",
            "offensive rating": "offensive_rating",
            "defensiverating": "defensive_rating",
            "defrtg": "defensive_rating",
            "drtg": "defensive_rating",
            "defensive rating": "defensive_rating",
            "pace": "pace",
            "possessionspergame": "pace",
            "possessions per game": "pace",
            # Scoring
            "gamesplayed": "games_played",
            "games played": "games_played",
            "totalpoints": "total_points",
            "total points": "total_points",
            "pointsagainstpergame": "points_against_per_game",
            "avgpointsagainst": "points_against_per_game",
            "oppavgpoints": "points_against_per_game",
            "opponentpointspergame": "points_against_per_game",
            "oppppg": "points_against_per_game",
            "opponent points per game": "points_against_per_game",
            # Defensive stats
            "opponentfieldgoalpct": "opp_fg_pct",
            "oppfg%": "opp_fg_pct",
            "opponentthreepointpct": "opp_three_pt_pct",
            "opp3pt%": "opp_three_pt_pct",
        }

        result = {}
        for espn_name, our_name in stat_name_map.items():
            val = stat_lookup.get(espn_name)
            if val is not None and our_name not in result:
                result[our_name] = val

        # Log which stats we mapped vs total available for diagnostics
        if result:
            logger.debug(
                "ESPN NBA mapped %d stats from %d raw entries",
                len(result), len(stat_lookup),
            )
        else:
            logger.warning(
                "ESPN NBA stat mapping produced no results. "
                "Raw stat names: %s",
                sorted(stat_lookup.keys())[:30],
            )

        # ESPN abbreviated names (reb, ast, etc.) may return season totals
        # instead of per-game averages.  Detect using stat-specific upper
        # bounds — no NBA team per-game average exceeds these values.
        gp = result.get("games_played", 0)
        per_game_max = {
            "points_per_game": 200,
            "rebounds_per_game": 80,
            "assists_per_game": 50,
            "turnovers_per_game": 30,
            "steals_per_game": 25,
            "blocks_per_game": 20,
            "three_pt_made_per_game": 30,
        }
        for key, threshold in per_game_max.items():
            val = result.get(key)
            if val is not None and val > threshold and gp > 0:
                # Almost certainly a season total — convert to per-game
                result[key] = round(val / gp, 1)

        # Convert percentages if they're in decimal form (0.48 → 48.0)
        for pct_key in ("fg_pct", "three_pt_pct", "ft_pct"):
            if pct_key in result and result[pct_key] < 1:
                result[pct_key] = round(result[pct_key] * 100, 1)

        return result if result else None

    async def fetch_all_team_stats(self) -> Dict[str, Dict[str, float]]:
        """Fetch stats for all NBA teams, keyed by canonical abbreviation."""
        teams = await self.fetch_teams()
        all_stats = {}
        parse_failures = 0

        for team_info in teams:
            espn_id = team_info.get("espn_id")
            espn_abbrev = team_info.get("abbreviation", "")
            nba_abbrev = _normalize_nba_abbrev(espn_abbrev)

            if not espn_id:
                continue

            try:
                data = await self.fetch_team_statistics(espn_id)
                stats = self._parse_team_stats(data)
                if stats:
                    all_stats[nba_abbrev] = stats
                    logger.debug("ESPN NBA stats for %s: %s", nba_abbrev, stats)
                else:
                    parse_failures += 1
                    # Log the response structure to help diagnose parsing issues
                    top_keys = list(data.keys())[:10] if isinstance(data, dict) else type(data).__name__
                    logger.warning(
                        "ESPN NBA stats parsed to empty for %s. Response top-level keys: %s",
                        nba_abbrev, top_keys,
                    )
            except Exception as exc:
                logger.warning("Failed to fetch ESPN NBA stats for %s: %s", nba_abbrev, exc)
                continue

        if parse_failures > 10:
            logger.error(
                "ESPN NBA stat parsing failed for %d/%d teams — response format may have changed",
                parse_failures, len(teams),
            )
        logger.info(
            "Fetched ESPN NBA stats for %d/%d teams (%d parse failures)",
            len(all_stats), len(teams), parse_failures,
        )
        return all_stats

    async def sync_team_stats(self, db: AsyncSession) -> int:
        """Fetch ESPN NBA team stats and update TeamStats records.

        Updates fields that are currently NULL in the database so ESPN
        fills in what the primary API couldn't provide.

        Returns:
            Number of teams updated.
        """
        all_stats = await self.fetch_all_team_stats()
        if not all_stats:
            logger.warning("No ESPN NBA stats fetched, skipping sync")
            return 0

        sport_cfg = settings.get_sport_config("nba")
        season = sport_cfg.default_season

        updated = 0
        for abbrev, espn_stats in all_stats.items():
            # Find the NBA team
            result = await db.execute(
                select(Team).where(
                    Team.abbreviation == abbrev,
                    Team.sport == "nba",
                )
            )
            team = result.scalars().first()
            if not team:
                logger.debug("No NBA team found for ESPN abbrev %s", abbrev)
                continue

            # Find or create TeamStats for current season
            result = await db.execute(
                select(TeamStats).where(
                    TeamStats.team_id == team.id,
                    TeamStats.season == season,
                )
            )
            stats = result.scalar_one_or_none()
            if not stats:
                stats = TeamStats(
                    team_id=team.id,
                    season=season,
                    games_played=0,
                    wins=0,
                    losses=0,
                    ot_losses=0,
                    points=0,
                    goals_for=0,
                    goals_against=0,
                )
                db.add(stats)

            changed = False

            # NBA shooting stats — always update from ESPN since it's
            # authoritative and free (unlike paid BallDontLie endpoints).
            # Only skip if ESPN didn't return the stat at all.
            for field, key in [
                ("fg_pct", "fg_pct"),
                ("three_pt_pct", "three_pt_pct"),
                ("ft_pct", "ft_pct"),
                ("rebounds_per_game", "rebounds_per_game"),
                ("assists_per_game", "assists_per_game"),
                ("turnovers_per_game", "turnovers_per_game"),
                ("steals_per_game", "steals_per_game"),
                ("blocks_per_game", "blocks_per_game"),
                ("three_pt_made_per_game", "three_pt_made_per_game"),
                ("pace", "pace"),
                ("offensive_rating", "offensive_rating"),
                ("defensive_rating", "defensive_rating"),
            ]:
                if key in espn_stats:
                    setattr(stats, field, espn_stats[key])
                    changed = True

            # Also fill in scoring per-game from ESPN
            if "points_per_game" in espn_stats:
                stats.goals_for_per_game = espn_stats["points_per_game"]
                changed = True
            if "points_against_per_game" in espn_stats:
                stats.goals_against_per_game = espn_stats["points_against_per_game"]
                changed = True

            if changed:
                stats.date_updated = datetime.now(timezone.utc)
                updated += 1

        await db.flush()
        logger.info("Updated %d NBA teams with ESPN stats", updated)
        return updated
