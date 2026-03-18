"""
NBA data scraper using the balldontlie API (https://api.balldontlie.io).

Fetches schedules, teams, players, and box scores, then synchronises the
data into the local SQLAlchemy database.  Requires a free API key set via
the BALLDONTLIE_API_KEY environment variable.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.game import Game, GamePlayerStats, HeadToHead
from app.models.player import Player
from app.models.team import Team, TeamStats
from app.scrapers.base import APIResponseError, BaseScraper

logger = logging.getLogger(__name__)

# balldontlie abbreviation -> our canonical abbreviation
_BDL_ABBREV_MAP: Dict[str, str] = {
    "ATL": "ATL", "BOS": "BOS", "BKN": "BKN", "CHA": "CHA",
    "CHI": "CHI", "CLE": "CLE", "DAL": "DAL", "DEN": "DEN",
    "DET": "DET", "GSW": "GSW", "HOU": "HOU", "IND": "IND",
    "LAC": "LAC", "LAL": "LAL", "MEM": "MEM", "MIA": "MIA",
    "MIL": "MIL", "MIN": "MIN", "NOP": "NOP", "NYK": "NYK",
    "OKC": "OKC", "ORL": "ORL", "PHI": "PHI", "PHX": "PHX",
    "POR": "POR", "SAC": "SAC", "SAS": "SAS", "TOR": "TOR",
    "UTA": "UTA", "WAS": "WAS",
}

# Canonical abbreviation -> ESPN CDN abbreviation (lowercase).
# ESPN uses shorter codes for a handful of teams.
_ESPN_ABBREV_MAP: Dict[str, str] = {
    "GSW": "gs", "NOP": "no", "NYK": "ny", "SAS": "sa", "WAS": "wsh",
}


def _espn_logo_url(abbreviation: str) -> str:
    """Return an ESPN CDN logo URL for the given NBA team abbreviation."""
    espn_abbr = _ESPN_ABBREV_MAP.get(abbreviation, abbreviation.lower())
    return f"https://a.espncdn.com/i/teamlogos/nba/500/{espn_abbr}.png"


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return round(float(val), 2)
    except (ValueError, TypeError):
        return None


class NBAScraper(BaseScraper):
    """
    Scraper for the balldontlie NBA API.

    Uses the public JSON API at https://api.balldontlie.io/nba/v1 to retrieve
    teams, players, schedules, and box scores.
    """

    # Cache NBA API responses for 2 minutes.
    DEFAULT_CACHE_TTL = 120.0

    def __init__(
        self,
        base_url: str = "https://api.balldontlie.io/nba/v1",
        rate_limit: float = 2.0,
        **kwargs,
    ):
        api_key = settings.balldontlie_api_key
        headers = {}
        if api_key:
            headers["Authorization"] = api_key
        super().__init__(
            base_url=base_url,
            rate_limit=rate_limit,
            headers=headers,
            **kwargs,
        )
        sport_cfg = settings.get_sport_config("nba")
        self.default_season: str = sport_cfg.default_season

        if not api_key:
            logger.warning(
                "No BALLDONTLIE_API_KEY configured. NBA data fetching will be limited. "
                "Set the BALLDONTLIE_API_KEY environment variable to enable."
            )

    # ------------------------------------------------------------------
    # Teams
    # ------------------------------------------------------------------

    async def sync_teams(self, session: AsyncSession) -> int:
        """Sync all NBA teams into the database."""
        try:
            data = await self.fetch_json("/teams")
        except Exception as exc:
            logger.error("Failed to fetch NBA teams: %s", exc)
            return 0

        teams_data = data.get("data", data) if isinstance(data, dict) else data
        if not isinstance(teams_data, list):
            logger.warning("Unexpected NBA teams response format")
            return 0

        synced = 0
        for t in teams_data:
            team_id = str(t.get("id", ""))
            if not team_id:
                continue

            abbreviation = t.get("abbreviation", "")
            # Normalize abbreviation
            abbreviation = _BDL_ABBREV_MAP.get(abbreviation, abbreviation)

            full_name = t.get("full_name", "")
            city = t.get("city", "")
            conference = t.get("conference", "")
            division = t.get("division", "")

            result = await session.execute(
                select(Team).where(Team.external_id == f"nba_{team_id}")
            )
            existing = result.scalar_one_or_none()

            logo_url = _espn_logo_url(abbreviation)

            if existing:
                existing.name = full_name
                existing.abbreviation = abbreviation
                existing.city = city
                existing.conference = conference
                existing.division = division
                existing.sport = "nba"
                existing.active = True
                existing.logo_url = logo_url
            else:
                team = Team(
                    external_id=f"nba_{team_id}",
                    name=full_name,
                    abbreviation=abbreviation,
                    city=city,
                    conference=conference,
                    division=division,
                    sport="nba",
                    active=True,
                    logo_url=logo_url,
                )
                session.add(team)

            synced += 1

        await session.flush()
        logger.info("NBA teams synced: %d", synced)
        return synced

    # ------------------------------------------------------------------
    # Players (paginated)
    # ------------------------------------------------------------------

    async def sync_players(self, session: AsyncSession) -> int:
        """Sync NBA players into the database (paginated).

        Players rarely change, so this uses a 24-hour cache TTL to avoid
        hitting the API on every sync cycle.
        """
        synced = 0
        cursor = None

        # 24-hour cache — rosters barely change intra-day
        player_cache_ttl = 86_400.0

        for _ in range(100):  # safety limit
            params: Dict[str, Any] = {"per_page": 100}
            if cursor:
                params["cursor"] = cursor

            try:
                data = await self.fetch_json(
                    "/players", params=params, cache_ttl=player_cache_ttl
                )
            except Exception as exc:
                logger.error("Failed to fetch NBA players: %s", exc)
                break

            players = data.get("data", [])
            if not players:
                break

            for p in players:
                player_id = str(p.get("id", ""))
                if not player_id:
                    continue

                first_name = p.get("first_name", "")
                last_name = p.get("last_name", "")
                full_name = f"{first_name} {last_name}".strip()
                position = p.get("position", "")
                jersey = _safe_int(p.get("jersey_number"))

                # Resolve team
                team_data = p.get("team", {})
                team_bdl_id = str(team_data.get("id", "")) if team_data else ""
                team_db = None
                if team_bdl_id:
                    team_result = await session.execute(
                        select(Team).where(Team.external_id == f"nba_{team_bdl_id}")
                    )
                    team_db = team_result.scalar_one_or_none()

                result = await session.execute(
                    select(Player).where(Player.external_id == f"nba_{player_id}")
                )
                existing = result.scalar_one_or_none()

                if existing:
                    existing.name = full_name
                    existing.position = position
                    existing.jersey_number = jersey
                    existing.sport = "nba"
                    existing.active = True
                    if team_db:
                        existing.team_id = team_db.id
                else:
                    player = Player(
                        external_id=f"nba_{player_id}",
                        name=full_name,
                        team_id=team_db.id if team_db else None,
                        position=position,
                        jersey_number=jersey,
                        sport="nba",
                        active=True,
                    )
                    session.add(player)

                synced += 1

            # Check for next page
            meta = data.get("meta", {})
            cursor = meta.get("next_cursor")
            if not cursor:
                break

        await session.flush()
        logger.info("NBA players synced: %d", synced)
        return synced

    # ------------------------------------------------------------------
    # Schedule
    # ------------------------------------------------------------------

    async def sync_schedule(
        self, session: AsyncSession, target_date: Optional[str] = None
    ) -> int:
        """Sync NBA games for a given date (or today)."""
        if target_date is None:
            target_date = date.today().isoformat()

        params = {
            "dates[]": target_date,
            "per_page": 100,
        }

        try:
            data = await self.fetch_json("/games", params=params)
        except Exception as exc:
            logger.error("Failed to fetch NBA schedule: %s", exc)
            return 0

        games_data = data.get("data", [])
        synced = 0

        for g in games_data:
            game_id = str(g.get("id", ""))
            if not game_id:
                continue

            # Resolve teams
            home_team_data = g.get("home_team", {})
            away_team_data = g.get("visitor_team", {})

            home_bdl_id = str(home_team_data.get("id", ""))
            away_bdl_id = str(away_team_data.get("id", ""))

            home_result = await session.execute(
                select(Team).where(Team.external_id == f"nba_{home_bdl_id}")
            )
            home_team = home_result.scalar_one_or_none()

            away_result = await session.execute(
                select(Team).where(Team.external_id == f"nba_{away_bdl_id}")
            )
            away_team = away_result.scalar_one_or_none()

            if not home_team or not away_team:
                logger.debug(
                    "NBA schedule: teams not found for game %s (home=%s, away=%s)",
                    game_id, home_bdl_id, away_bdl_id,
                )
                continue

            # Parse date and status
            game_date_str = g.get("date", "")[:10]  # "2025-03-17T00:00:00.000Z"
            try:
                game_date = date.fromisoformat(game_date_str)
            except (ValueError, TypeError):
                game_date = date.today()

            # Map status.
            # BallDontLie API status values:
            #   - "Final" → game is over
            #   - "1st Qtr", "2nd Qtr", "3rd Qtr", "4th Qtr",
            #     "Halftime", "OT", "In Progress" → game is live
            #   - ISO datetime string or empty → game hasn't started
            api_status = g.get("status", "")
            status_lower = api_status.lower().strip()
            if status_lower in ("final",):
                status = "final"
            elif status_lower in (
                "in progress", "in_progress",
                "1st qtr", "2nd qtr", "3rd qtr", "4th qtr",
                "halftime", "ot", "overtime",
                "1st quarter", "2nd quarter", "3rd quarter", "4th quarter",
            ):
                status = "in_progress"
            else:
                status = "scheduled"

            home_score = _safe_int(g.get("home_team_score"))
            away_score = _safe_int(g.get("visitor_team_score"))

            # Parse period/quarter from status for live games
            period = None
            period_type = None
            in_intermission = False
            clock = None
            if status == "in_progress":
                if "1st" in status_lower:
                    period = 1
                elif "2nd" in status_lower:
                    period = 2
                elif "3rd" in status_lower:
                    period = 3
                elif "4th" in status_lower:
                    period = 4
                elif "ot" in status_lower or "overtime" in status_lower:
                    period = 5
                    period_type = "OT"
                if "halftime" in status_lower:
                    period = 2
                    in_intermission = True

                # Also try the "period" field from the API if available
                api_period = g.get("period")
                if api_period and isinstance(api_period, int) and api_period > 0:
                    period = api_period
                    if api_period > 4:
                        period_type = "OT"

                # Try multiple field names for the game clock.
                # BallDontLie uses "time" but may also have "clock" or
                # "game_clock" depending on the API version.
                for clock_field in ("time", "clock", "game_clock"):
                    api_time = g.get(clock_field)
                    if (
                        api_time
                        and isinstance(api_time, str)
                        and ":" in api_time
                        and api_time.lower() not in ("final", "half", "halftime")
                    ):
                        clock = api_time  # e.g., "5:30"
                        break
                else:
                    clock = None

            # Determine season
            season = g.get("season", self.default_season)

            # Start time
            start_time = None
            datetime_str = g.get("datetime") or g.get("date", "")
            if datetime_str:
                try:
                    start_time = datetime.fromisoformat(
                        datetime_str.replace("Z", "+00:00")
                    )
                except (ValueError, TypeError):
                    pass

            # Upsert game
            external_id = f"nba_{game_id}"
            result = await session.execute(
                select(Game).where(Game.external_id == external_id)
            )
            existing = result.scalar_one_or_none()

            if existing:
                existing.status = status
                existing.home_score = home_score
                existing.away_score = away_score
                if start_time:
                    existing.start_time = start_time
                # Update live game info
                if status == "in_progress":
                    if period is not None:
                        existing.period = period
                    if period_type is not None:
                        existing.period_type = period_type
                    existing.in_intermission = in_intermission
                    if clock is not None:
                        existing.clock = clock
                # Determine winner
                if status == "final" and home_score is not None and away_score is not None:
                    if home_score > away_score:
                        existing.winning_team_id = home_team.id
                    elif away_score > home_score:
                        existing.winning_team_id = away_team.id
            else:
                game = Game(
                    external_id=external_id,
                    sport="nba",
                    season=str(season),
                    game_type="regular",
                    date=game_date,
                    start_time=start_time,
                    home_team_id=home_team.id,
                    away_team_id=away_team.id,
                    status=status,
                    home_score=home_score,
                    away_score=away_score,
                )
                # Determine winner
                if status == "final" and home_score is not None and away_score is not None:
                    if home_score > away_score:
                        game.winning_team_id = home_team.id
                    elif away_score > home_score:
                        game.winning_team_id = away_team.id
                session.add(game)

            synced += 1

        await session.flush()
        logger.info("NBA schedule synced for %s: %d games", target_date, synced)
        return synced

    # ------------------------------------------------------------------
    # Season schedule (paginated – fetches the full season)
    # ------------------------------------------------------------------

    async def sync_season_schedule(
        self, session: AsyncSession, season: Optional[int] = None
    ) -> int:
        """Sync all NBA games for a full season using cursor pagination.

        The balldontlie API supports a ``seasons[]`` query parameter and
        returns up to 100 results per page with cursor-based pagination.

        Uses a 6-hour cache TTL since historical game results don't
        change.  Today's live games are refreshed separately via
        ``sync_schedule``.
        """
        if season is None:
            season = int(self.default_season)

        # 6-hour cache — historical games don't change; live/today
        # are refreshed via sync_schedule with the default shorter TTL.
        season_cache_ttl = 21_600.0

        synced_total = 0
        cursor = None

        for _ in range(200):  # safety limit (~200 pages × 100 = 20 000 games max)
            params: Dict[str, Any] = {
                "seasons[]": season,
                "per_page": 100,
            }
            if cursor:
                params["cursor"] = cursor

            try:
                data = await self.fetch_json(
                    "/games", params=params, cache_ttl=season_cache_ttl
                )
            except Exception as exc:
                logger.error("Failed to fetch NBA season schedule (season=%s): %s", season, exc)
                break

            games_data = data.get("data", [])
            if not games_data:
                break

            for g in games_data:
                game_id = str(g.get("id", ""))
                if not game_id:
                    continue

                home_team_data = g.get("home_team", {})
                away_team_data = g.get("visitor_team", {})

                home_bdl_id = str(home_team_data.get("id", ""))
                away_bdl_id = str(away_team_data.get("id", ""))

                home_result = await session.execute(
                    select(Team).where(Team.external_id == f"nba_{home_bdl_id}")
                )
                home_team = home_result.scalar_one_or_none()

                away_result = await session.execute(
                    select(Team).where(Team.external_id == f"nba_{away_bdl_id}")
                )
                away_team = away_result.scalar_one_or_none()

                if not home_team or not away_team:
                    continue

                game_date_str = g.get("date", "")[:10]
                try:
                    game_date = date.fromisoformat(game_date_str)
                except (ValueError, TypeError):
                    game_date = date.today()

                api_status = g.get("status", "")
                status_lower = api_status.lower().strip()
                if status_lower in ("final",):
                    status = "final"
                elif status_lower in (
                    "in progress", "in_progress",
                    "1st qtr", "2nd qtr", "3rd qtr", "4th qtr",
                    "halftime", "ot", "overtime",
                    "1st quarter", "2nd quarter", "3rd quarter", "4th quarter",
                ):
                    status = "in_progress"
                else:
                    status = "scheduled"

                home_score = _safe_int(g.get("home_team_score"))
                away_score = _safe_int(g.get("visitor_team_score"))

                game_season = g.get("season", self.default_season)

                start_time = None
                datetime_str = g.get("datetime") or g.get("date", "")
                if datetime_str:
                    try:
                        start_time = datetime.fromisoformat(
                            datetime_str.replace("Z", "+00:00")
                        )
                    except (ValueError, TypeError):
                        pass

                external_id = f"nba_{game_id}"
                result = await session.execute(
                    select(Game).where(Game.external_id == external_id)
                )
                existing = result.scalar_one_or_none()

                if existing:
                    existing.status = status
                    existing.home_score = home_score
                    existing.away_score = away_score
                    if start_time:
                        existing.start_time = start_time
                    if status == "final" and home_score is not None and away_score is not None:
                        if home_score > away_score:
                            existing.winning_team_id = home_team.id
                        elif away_score > home_score:
                            existing.winning_team_id = away_team.id
                else:
                    game_obj = Game(
                        external_id=external_id,
                        sport="nba",
                        season=str(game_season),
                        game_type="regular",
                        date=game_date,
                        start_time=start_time,
                        home_team_id=home_team.id,
                        away_team_id=away_team.id,
                        status=status,
                        home_score=home_score,
                        away_score=away_score,
                    )
                    if status == "final" and home_score is not None and away_score is not None:
                        if home_score > away_score:
                            game_obj.winning_team_id = home_team.id
                        elif away_score > home_score:
                            game_obj.winning_team_id = away_team.id
                    session.add(game_obj)

                synced_total += 1

            await session.flush()

            # Next page
            meta = data.get("meta", {})
            cursor = meta.get("next_cursor")
            if not cursor:
                break

        logger.info(
            "NBA season schedule synced (season=%s): %d games", season, synced_total
        )
        return synced_total

    # ------------------------------------------------------------------
    # Box scores (game stats)
    # ------------------------------------------------------------------

    async def sync_game_stats(
        self, session: AsyncSession, game_external_id: str
    ) -> int:
        """Fetch and sync box score stats for a single game."""
        # Extract numeric ID from "nba_12345"
        numeric_id = game_external_id.replace("nba_", "")

        params = {"game_ids[]": numeric_id, "per_page": 100}
        try:
            # Box scores for finished games never change — cache 7 days
            data = await self.fetch_json(
                "/stats", params=params, cache_ttl=604_800.0
            )
        except Exception as exc:
            logger.error("Failed to fetch NBA stats for game %s: %s", numeric_id, exc)
            return 0

        stats_data = data.get("data", [])
        if not stats_data:
            return 0

        # Find the game in our DB
        result = await session.execute(
            select(Game).where(Game.external_id == game_external_id)
        )
        game = result.scalar_one_or_none()
        if not game:
            return 0

        synced = 0
        for stat in stats_data:
            player_data = stat.get("player", {})
            player_id = str(player_data.get("id", ""))
            if not player_id:
                continue

            # Find player in DB
            player_result = await session.execute(
                select(Player).where(Player.external_id == f"nba_{player_id}")
            )
            player = player_result.scalar_one_or_none()
            if not player:
                continue

            minutes_str = stat.get("min", "")
            minutes = None
            if minutes_str:
                try:
                    # Format can be "32:15" or "32"
                    if ":" in str(minutes_str):
                        parts = str(minutes_str).split(":")
                        minutes = float(parts[0]) + float(parts[1]) / 60.0
                    else:
                        minutes = float(minutes_str)
                except (ValueError, TypeError):
                    pass

            # Check for existing stat
            existing_result = await session.execute(
                select(GamePlayerStats).where(
                    GamePlayerStats.game_id == game.id,
                    GamePlayerStats.player_id == player.id,
                )
            )
            existing = existing_result.scalar_one_or_none()

            pts = _safe_int(stat.get("pts")) or 0
            ast = _safe_int(stat.get("ast")) or 0
            reb = _safe_int(stat.get("reb")) or 0
            stl = _safe_int(stat.get("stl")) or 0
            blk = _safe_int(stat.get("blk")) or 0
            tov = _safe_int(stat.get("turnover")) or 0
            fga = _safe_int(stat.get("fga")) or 0
            fg3m = _safe_int(stat.get("fg3m")) or 0
            ftm = _safe_int(stat.get("ftm")) or 0
            fta = _safe_int(stat.get("fta")) or 0
            plus_minus = _safe_int(stat.get("plus_minus")) or 0

            if existing:
                existing.goals = pts  # 'goals' field repurposed as points for NBA
                existing.assists = ast
                existing.points = pts
                existing.plus_minus = plus_minus
                existing.shots = fga
                existing.toi = minutes
                existing.rebounds = reb
                existing.steals = stl
                existing.blocks = blk
                existing.turnovers = tov
                existing.three_pointers_made = fg3m
                existing.free_throws_made = ftm
                existing.free_throws_attempted = fta
            else:
                gps = GamePlayerStats(
                    game_id=game.id,
                    player_id=player.id,
                    goals=pts,
                    assists=ast,
                    points=pts,
                    plus_minus=plus_minus,
                    shots=fga,
                    toi=minutes,
                    rebounds=reb,
                    steals=stl,
                    blocks=blk,
                    turnovers=tov,
                    three_pointers_made=fg3m,
                    free_throws_made=ftm,
                    free_throws_attempted=fta,
                )
                session.add(gps)

            synced += 1

        await session.flush()
        logger.info("NBA game stats synced for %s: %d player lines", game_external_id, synced)
        return synced

    # ------------------------------------------------------------------
    # Team stats (standings + averages)
    # ------------------------------------------------------------------

    async def sync_team_stats_from_api(self, session: AsyncSession) -> int:
        """Fetch team season averages from the balldontlie API.

        Uses the ``/team_season_averages/general?type=base`` endpoint to
        get team-level stats (FG%, 3PT%, rebounds, etc.) in a single
        paginated call — no per-game box score fetching required.
        """
        sport_cfg = settings.get_sport_config("nba")
        season = sport_cfg.default_season

        # Fetch team season averages from the API (all 30 teams fit in one page)
        params: Dict[str, Any] = {
            "season": int(season),
            "season_type": "regular",
            "type": "base",
            "per_page": 100,
        }

        # 1-hour cache — team averages update slowly
        try:
            data = await self.fetch_json(
                "/team_season_averages/general", params=params,
                cache_ttl=3_600.0,
            )
        except Exception as exc:
            logger.error("Failed to fetch NBA team season averages: %s", exc)
            return 0

        entries = data.get("data", [])
        if not entries:
            logger.warning("NBA team_season_averages returned no data")
            return 0

        # Build a map of abbreviation -> DB team for quick lookup
        team_result = await session.execute(
            select(Team).where(Team.sport == "nba", Team.active == True)
        )
        teams_by_abbr = {t.abbreviation: t for t in team_result.scalars().all()}
        teams_by_name = {t.name: t for t in teams_by_abbr.values()}

        synced = 0
        for entry in entries:
            api_team = entry.get("team", {})
            stats = entry.get("stats", {})
            if not api_team or not stats:
                continue

            # Resolve team
            abbr = api_team.get("abbreviation", "")
            abbr = _BDL_ABBREV_MAP.get(abbr, abbr)
            team = teams_by_abbr.get(abbr)
            if not team:
                team = teams_by_name.get(api_team.get("full_name", ""))
            if not team:
                logger.debug("NBA team_season_averages: team not found for %s", abbr)
                continue

            # Extract stats — field names come from the API response
            gp = _safe_int(stats.get("gp")) or _safe_int(stats.get("games_played")) or 0
            wins = _safe_int(stats.get("w")) or _safe_int(stats.get("wins")) or 0
            losses = _safe_int(stats.get("l")) or _safe_int(stats.get("losses")) or 0
            pts = _safe_float(stats.get("pts")) or 0
            fg_pct = _safe_float(stats.get("fg_pct"))
            fg3_pct = _safe_float(stats.get("fg3_pct"))
            ft_pct = _safe_float(stats.get("ft_pct"))
            reb = _safe_float(stats.get("reb"))
            ast = _safe_float(stats.get("ast"))
            tov = _safe_float(stats.get("tov")) or _safe_float(stats.get("turnover"))
            stl = _safe_float(stats.get("stl"))
            blk = _safe_float(stats.get("blk"))
            fg3m = _safe_float(stats.get("fg3m"))
            opp_pts = _safe_float(stats.get("opp_pts"))
            pace_val = _safe_float(stats.get("pace"))
            off_rating = _safe_float(stats.get("off_rating"))
            def_rating = _safe_float(stats.get("def_rating"))
            fga = _safe_float(stats.get("fga"))
            fta = _safe_float(stats.get("fta"))
            min_val = _safe_float(stats.get("min"))

            # Convert percentages: API may return as 0.48 or 48.0
            if fg_pct is not None and fg_pct < 1:
                fg_pct = round(fg_pct * 100, 1)
            if fg3_pct is not None and fg3_pct < 1:
                fg3_pct = round(fg3_pct * 100, 1)
            if ft_pct is not None and ft_pct < 1:
                ft_pct = round(ft_pct * 100, 1)

            # Estimate pace if not directly provided
            if pace_val is None and fga and reb and tov and fta:
                oreb_est = (reb or 0) * 0.25
                possessions = fga - oreb_est + (tov or 0) + 0.44 * fta
                pace_val = round(possessions, 1)

            # Estimate offensive rating if not provided
            if off_rating is None and pace_val and pace_val > 0 and pts:
                off_rating = round(pts / pace_val * 100, 1)

            # Estimate defensive rating if not provided
            if def_rating is None and pace_val and pace_val > 0 and opp_pts:
                def_rating = round(opp_pts / pace_val * 100, 1)

            # Also compute W-L from completed games in DB for records
            games_result = await session.execute(
                select(Game).where(
                    Game.sport == "nba",
                    Game.season == season,
                    func.lower(Game.status).in_(("final", "completed")),
                    (Game.home_team_id == team.id) | (Game.away_team_id == team.id),
                ).order_by(Game.date.desc())
            )
            db_games = games_result.scalars().all()

            # Compute records from DB games
            home_w = home_l = away_w = away_l = 0
            total_pf = total_pa = 0
            recent_results: List[str] = []

            for g in db_games:
                is_home = g.home_team_id == team.id
                pf = (g.home_score or 0) if is_home else (g.away_score or 0)
                pa = (g.away_score or 0) if is_home else (g.home_score or 0)
                total_pf += pf
                total_pa += pa
                won = pf > pa
                if won:
                    if is_home:
                        home_w += 1
                    else:
                        away_w += 1
                else:
                    if is_home:
                        home_l += 1
                    else:
                        away_l += 1
                recent_results.append("W" if won else "L")

            # Use DB-computed W/L if API didn't provide
            if wins == 0 and losses == 0 and db_games:
                wins = recent_results.count("W")
                losses = recent_results.count("L")
                gp = len(db_games)

            def _fmt_record(results: List[str]) -> str:
                w = results.count("W")
                l_ = results.count("L")
                return f"{w}-{l_}"

            home_record = f"{home_w}-{home_l}"
            away_record = f"{away_w}-{away_l}"
            record_last_5 = _fmt_record(recent_results[:5]) if len(recent_results) >= 5 else None
            record_last_10 = _fmt_record(recent_results[:10]) if len(recent_results) >= 10 else None
            record_last_20 = _fmt_record(recent_results[:20]) if len(recent_results) >= 20 else None

            goals_for_pg = round(total_pf / gp, 2) if gp > 0 else (pts or 0)
            goals_against_pg = round(total_pa / gp, 2) if gp > 0 else (opp_pts or 0)

            # Upsert TeamStats
            stats_result = await session.execute(
                select(TeamStats).where(
                    TeamStats.team_id == team.id,
                    TeamStats.season == season,
                )
            )
            existing = stats_result.scalar_one_or_none()

            stat_fields = dict(
                games_played=gp,
                wins=wins,
                losses=losses,
                goals_for=total_pf or int(pts * gp) if pts else 0,
                goals_against=total_pa or int((opp_pts or 0) * gp) if opp_pts else 0,
                goals_for_per_game=goals_for_pg,
                goals_against_per_game=goals_against_pg,
                points=wins,
                home_record=home_record,
                away_record=away_record,
                record_last_5=record_last_5,
                record_last_10=record_last_10,
                record_last_20=record_last_20,
                fg_pct=fg_pct,
                three_pt_pct=fg3_pct,
                ft_pct=ft_pct,
                rebounds_per_game=reb,
                assists_per_game=ast,
                turnovers_per_game=tov,
                steals_per_game=stl,
                blocks_per_game=blk,
                three_pt_made_per_game=fg3m,
                pace=pace_val,
                offensive_rating=off_rating,
                defensive_rating=def_rating,
                date_updated=datetime.now(timezone.utc),
            )

            if existing:
                for k, v in stat_fields.items():
                    setattr(existing, k, v)
            else:
                ts = TeamStats(
                    team_id=team.id,
                    season=season,
                    ot_losses=0,
                    **stat_fields,
                )
                session.add(ts)

            synced += 1

        await session.flush()
        logger.info("NBA team stats synced from API: %d teams", synced)
        return synced

    async def sync_team_stats(self, session: AsyncSession) -> int:
        """Compute team stats from completed games this season.

        Aggregates box-score-level player stats per game to derive
        NBA-specific team averages: FG%, 3PT%, FT%, rebounds, assists,
        turnovers, steals, blocks, pace, and offensive/defensive rating.

        Prefer ``sync_team_stats_from_api`` when possible — it fetches
        the same data in a single API call instead of requiring box
        scores for every game.
        """
        sport_cfg = settings.get_sport_config("nba")
        season = sport_cfg.default_season

        # Get all NBA teams
        team_result = await session.execute(
            select(Team).where(Team.sport == "nba", Team.active == True)
        )
        teams = team_result.scalars().all()

        synced = 0
        for team in teams:
            # Get completed games for this team this season
            games_result = await session.execute(
                select(Game).where(
                    Game.sport == "nba",
                    Game.season == season,
                    func.lower(Game.status).in_(("final", "completed")),
                    (Game.home_team_id == team.id) | (Game.away_team_id == team.id),
                ).order_by(Game.date.desc())
            )
            games = games_result.scalars().all()

            if not games:
                continue

            # Games are ordered by date DESC (most recent first).
            # Accumulate overall, home/away, and recent-form records.
            wins = losses = 0
            home_w = home_l = away_w = away_l = 0
            total_pf = total_pa = 0

            # Recent form: track W/L for the N most-recent games
            recent_results: List[str] = []  # "W" or "L", newest first

            for g in games:
                is_home = g.home_team_id == team.id
                pf = (g.home_score or 0) if is_home else (g.away_score or 0)
                pa = (g.away_score or 0) if is_home else (g.home_score or 0)
                total_pf += pf
                total_pa += pa
                won = pf > pa
                if won:
                    wins += 1
                    if is_home:
                        home_w += 1
                    else:
                        away_w += 1
                else:
                    losses += 1
                    if is_home:
                        home_l += 1
                    else:
                        away_l += 1
                recent_results.append("W" if won else "L")

            gp = len(games)

            def _fmt_record(results: List[str]) -> str:
                """Format a W-L record string from a list of 'W'/'L' entries."""
                w = results.count("W")
                l_ = results.count("L")
                return f"{w}-{l_}"

            home_record = f"{home_w}-{home_l}"
            away_record = f"{away_w}-{away_l}"
            record_last_5 = _fmt_record(recent_results[:5]) if len(recent_results) >= 5 else None
            record_last_10 = _fmt_record(recent_results[:10]) if len(recent_results) >= 10 else None
            record_last_20 = _fmt_record(recent_results[:20]) if len(recent_results) >= 20 else None

            # ----------------------------------------------------------
            # NBA advanced stats from box-score player stats
            # ----------------------------------------------------------
            game_ids = [g.id for g in games]
            nba_stats = await self._compute_nba_advanced_stats(
                session, team.id, game_ids, gp
            )

            # Compute defensive rating from game scores and pace
            # DRtg = opponent points allowed per 100 possessions
            if nba_stats.get("pace") and gp > 0:
                papg = total_pa / gp
                team_pace = nba_stats["pace"]
                if team_pace > 0:
                    nba_stats["defensive_rating"] = round(papg / team_pace * 100, 1)

            # Upsert TeamStats
            stats_result = await session.execute(
                select(TeamStats).where(
                    TeamStats.team_id == team.id,
                    TeamStats.season == season,
                )
            )
            existing = stats_result.scalar_one_or_none()

            stat_fields = dict(
                games_played=gp,
                wins=wins,
                losses=losses,
                goals_for=total_pf,
                goals_against=total_pa,
                goals_for_per_game=round(total_pf / gp, 2) if gp else 0,
                goals_against_per_game=round(total_pa / gp, 2) if gp else 0,
                points=wins,  # NBA "points" = wins for standings
                home_record=home_record,
                away_record=away_record,
                record_last_5=record_last_5,
                record_last_10=record_last_10,
                record_last_20=record_last_20,
                date_updated=datetime.now(timezone.utc),
                **nba_stats,
            )

            if existing:
                for k, v in stat_fields.items():
                    setattr(existing, k, v)
            else:
                ts = TeamStats(
                    team_id=team.id,
                    season=season,
                    ot_losses=0,
                    **stat_fields,
                )
                session.add(ts)

            synced += 1

        await session.flush()
        logger.info("NBA team stats synced: %d teams", synced)
        return synced

    async def _compute_nba_advanced_stats(
        self,
        session: AsyncSession,
        team_id: int,
        game_ids: List[int],
        games_played: int,
    ) -> Dict[str, Any]:
        """Aggregate box-score player stats into NBA team averages.

        Returns a dict of TeamStats fields for NBA-specific columns.
        """
        if not game_ids or games_played == 0:
            return {}

        from app.models.player import Player

        # Fetch all player stats for this team's games
        # Join through Player to filter by team_id
        stats_result = await session.execute(
            select(GamePlayerStats, Game)
            .join(Game, GamePlayerStats.game_id == Game.id)
            .join(Player, GamePlayerStats.player_id == Player.id)
            .where(
                GamePlayerStats.game_id.in_(game_ids),
                Player.team_id == team_id,
            )
        )
        rows = stats_result.all()

        if not rows:
            return {}

        # Aggregate per-game totals, then average
        game_totals: Dict[int, Dict[str, float]] = {}
        for gps, game in rows:
            gid = game.id
            if gid not in game_totals:
                game_totals[gid] = {
                    "pts": 0, "fga": 0, "fgm": 0, "fg3a": 0, "fg3m": 0,
                    "fta": 0, "ftm": 0, "reb": 0, "ast": 0, "tov": 0,
                    "stl": 0, "blk": 0, "minutes": 0,
                    "opp_pts": 0,
                }

            gt = game_totals[gid]
            gt["pts"] += gps.points or gps.goals or 0
            gt["fga"] += gps.shots or 0  # shots = FGA
            # Derive FGM from points, 3PM, FTM: FGM = (PTS - 3PM - FTM) / 2 + 3PM
            # But we don't have per-player FGM directly; approximate from FGA and scoring
            fg3m = gps.three_pointers_made or 0
            ftm = gps.free_throws_made or 0
            pts = gps.points or gps.goals or 0
            # FGM = (PTS - FTM) / 2   (each FG = 2 or 3 pts; with 3PM counted)
            # More accurately: PTS = 2*(FGM - FG3M) + 3*FG3M + FTM
            # => FGM = (PTS - FG3M - FTM) / 2 + FG3M  (rearranging is not exact per player)
            # We'll just count FGA and derive FG% from team totals
            gt["fg3m"] += fg3m
            gt["ftm"] += ftm
            gt["fta"] += gps.free_throws_attempted or 0
            gt["reb"] += gps.rebounds or 0
            gt["ast"] += gps.assists or 0
            gt["tov"] += gps.turnovers or 0
            gt["stl"] += gps.steals or 0
            gt["blk"] += gps.blocks or 0
            gt["minutes"] += gps.toi or 0  # toi = minutes

        # Also need opponent points per game for defensive rating
        # This is already captured in total_pa from game scores, so we use that approach

        n_games = len(game_totals)
        if n_games == 0:
            return {}

        # Sum across all games
        totals = {k: sum(gt[k] for gt in game_totals.values()) for k in game_totals[next(iter(game_totals))]}

        total_fga = totals["fga"]
        total_fg3m = totals["fg3m"]
        total_ftm = totals["ftm"]
        total_fta = totals["fta"]
        total_pts = totals["pts"]

        # Derive FGM from scoring: PTS = 2*FG2M + 3*FG3M + FTM
        # FG2M = (PTS - 3*FG3M - FTM) / 2
        fg2m = max(0, (total_pts - 3 * total_fg3m - total_ftm) / 2)
        total_fgm = fg2m + total_fg3m

        # Derive FG3A estimate: assume league-average 3PT% ~36% if we have 3PM
        # This is approximate since we don't track FG3A directly
        fg3a_est = total_fg3m / 0.36 if total_fg3m > 0 else 0

        fg_pct = round(total_fgm / total_fga * 100, 1) if total_fga > 0 else None
        three_pt_pct = round(total_fg3m / fg3a_est * 100, 1) if fg3a_est > 0 else None
        ft_pct = round(total_ftm / total_fta * 100, 1) if total_fta > 0 else None

        # Per-game averages
        reb_pg = round(totals["reb"] / n_games, 1)
        ast_pg = round(totals["ast"] / n_games, 1)
        tov_pg = round(totals["tov"] / n_games, 1)
        stl_pg = round(totals["stl"] / n_games, 1)
        blk_pg = round(totals["blk"] / n_games, 1)
        fg3m_pg = round(total_fg3m / n_games, 1)

        # Pace estimate: possessions = FGA - OREB + TOV + 0.44*FTA
        # We don't have OREB separately, use approximate: OREB ~ 25% of total REB
        oreb_est = totals["reb"] * 0.25
        possessions = total_fga - oreb_est + totals["tov"] + 0.44 * total_fta
        pace = round(possessions / n_games, 1) if n_games > 0 else None

        # Offensive rating: points per 100 possessions
        off_rating = round(total_pts / possessions * 100, 1) if possessions > 0 else None

        return {
            "fg_pct": fg_pct,
            "three_pt_pct": three_pt_pct,
            "ft_pct": ft_pct,
            "rebounds_per_game": reb_pg,
            "assists_per_game": ast_pg,
            "turnovers_per_game": tov_pg,
            "steals_per_game": stl_pg,
            "blocks_per_game": blk_pg,
            "three_pt_made_per_game": fg3m_pg,
            "pace": pace,
            "offensive_rating": off_rating,
        }

    # ------------------------------------------------------------------
    # Full sync orchestrator
    # ------------------------------------------------------------------

    async def sync_all(self, session: AsyncSession) -> None:
        """Run the full NBA sync pipeline.

        All expensive API calls use long cache TTLs so repeated syncs
        (every 60 min) serve from SQLite cache and make zero network
        requests until the cache expires:

        - Teams: 120s (default) — 1 call, 30 teams
        - Players: 24h cache — paginated, ~5 calls
        - Season schedule: 6h cache — paginated, ~15 calls
        - Team season averages: 1h cache — 1 call, 30 teams
        - Box scores: 120s (default) — only last 14 days, only missing
        """
        await self.sync_teams(session)

        # Players (24h cache — rosters barely change)
        await self.sync_players(session)

        # Full current season schedule (6h cache per page)
        current_season = int(self.default_season)
        await self.sync_season_schedule(session, season=current_season)

        # Today/tomorrow via date endpoint for live status (short cache)
        today = date.today()
        for offset in range(0, 3):
            target = today + timedelta(days=offset)
            await self.sync_schedule(session, target.isoformat())

        # Team stats from the API (1h cache, 1 call)
        await self.sync_team_stats_from_api(session)

        # Box scores for recent games only — player-level analytics.
        # Only fetches games missing stats to avoid redundant calls.
        result = await session.execute(
            select(Game).where(
                Game.sport == "nba",
                Game.season == str(current_season),
                func.lower(Game.status).in_(("final", "completed")),
                Game.date >= today - timedelta(days=14),
            )
        )
        final_games = result.scalars().all()

        synced_box = 0
        for game in final_games:
            stats_result = await session.execute(
                select(func.count(GamePlayerStats.id)).where(
                    GamePlayerStats.game_id == game.id
                )
            )
            stats_count = stats_result.scalar() or 0
            if stats_count == 0:
                await self.sync_game_stats(session, game.external_id)
                synced_box += 1

        if synced_box:
            logger.info("NBA box scores synced for %d games", synced_box)
        logger.info("NBA full sync completed")
