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
        rate_limit: float = 0.5,
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

            if existing:
                existing.name = full_name
                existing.abbreviation = abbreviation
                existing.city = city
                existing.conference = conference
                existing.division = division
                existing.sport = "nba"
                existing.active = True
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
        """Sync NBA players into the database (paginated)."""
        synced = 0
        cursor = None

        for _ in range(100):  # safety limit
            params: Dict[str, Any] = {"per_page": 100}
            if cursor:
                params["cursor"] = cursor

            try:
                data = await self.fetch_json("/players", params=params)
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

            # Map status
            api_status = g.get("status", "")
            if api_status in ("Final", "final"):
                status = "final"
            elif api_status in ("In Progress", "in_progress"):
                status = "in_progress"
            else:
                status = "scheduled"

            home_score = _safe_int(g.get("home_team_score"))
            away_score = _safe_int(g.get("visitor_team_score"))

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
            data = await self.fetch_json("/stats", params=params)
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

    async def sync_team_stats(self, session: AsyncSession) -> int:
        """Compute team stats from completed games this season."""
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

            wins = losses = 0
            total_pf = total_pa = 0

            for g in games:
                is_home = g.home_team_id == team.id
                pf = (g.home_score or 0) if is_home else (g.away_score or 0)
                pa = (g.away_score or 0) if is_home else (g.home_score or 0)
                total_pf += pf
                total_pa += pa
                if pf > pa:
                    wins += 1
                else:
                    losses += 1

            gp = len(games)

            # Upsert TeamStats
            stats_result = await session.execute(
                select(TeamStats).where(
                    TeamStats.team_id == team.id,
                    TeamStats.season == season,
                )
            )
            existing = stats_result.scalar_one_or_none()

            if existing:
                existing.games_played = gp
                existing.wins = wins
                existing.losses = losses
                existing.goals_for = total_pf
                existing.goals_against = total_pa
                existing.goals_for_per_game = round(total_pf / gp, 2) if gp else 0
                existing.goals_against_per_game = round(total_pa / gp, 2) if gp else 0
                existing.points = wins  # NBA "points" = wins for standings
                existing.date_updated = datetime.now(timezone.utc)
            else:
                ts = TeamStats(
                    team_id=team.id,
                    season=season,
                    games_played=gp,
                    wins=wins,
                    losses=losses,
                    ot_losses=0,
                    goals_for=total_pf,
                    goals_against=total_pa,
                    goals_for_per_game=round(total_pf / gp, 2) if gp else 0,
                    goals_against_per_game=round(total_pa / gp, 2) if gp else 0,
                    points=wins,
                    date_updated=datetime.now(timezone.utc),
                )
                session.add(ts)

            synced += 1

        await session.flush()
        logger.info("NBA team stats synced: %d teams", synced)
        return synced

    # ------------------------------------------------------------------
    # Full sync orchestrator
    # ------------------------------------------------------------------

    async def sync_all(self, session: AsyncSession) -> None:
        """Run the full NBA sync pipeline."""
        await self.sync_teams(session)

        # Sync schedule for today and surrounding days
        today = date.today()
        for offset in range(-1, 2):
            target = today + timedelta(days=offset)
            await self.sync_schedule(session, target.isoformat())

        # Sync box scores for completed games without stats
        result = await session.execute(
            select(Game).where(
                Game.sport == "nba",
                func.lower(Game.status).in_(("final", "completed")),
                Game.date >= today - timedelta(days=3),
            )
        )
        final_games = result.scalars().all()

        for game in final_games:
            # Check if stats already exist
            stats_result = await session.execute(
                select(func.count(GamePlayerStats.id)).where(
                    GamePlayerStats.game_id == game.id
                )
            )
            stats_count = stats_result.scalar() or 0
            if stats_count == 0:
                await self.sync_game_stats(session, game.external_id)

        await self.sync_team_stats(session)

        logger.info("NBA full sync completed")
