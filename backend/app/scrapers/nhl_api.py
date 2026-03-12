"""
NHL data scraper using the official NHL Stats API (api-web.nhle.com/v1).

Fetches schedules, standings, rosters, boxscores, and player stats,
then synchronises the data into the local SQLAlchemy database.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.game import Game, GameGoalieStats, GamePlayerStats, HeadToHead
from app.models.player import Player, GoalieStats, PlayerStats
from app.models.team import Team, TeamStats
from app.scrapers.base import APIResponseError, BaseScraper

logger = logging.getLogger(__name__)


class NHLScraper(BaseScraper):
    """
    Scraper for the NHL Stats API.

    Uses the public JSON API at https://api-web.nhle.com/v1 to retrieve
    schedules, standings, team/player stats, rosters, and boxscores.
    All data is normalised and persisted through SQLAlchemy async sessions.
    """

    def __init__(
        self,
        base_url: str = settings.nhl_api_base,
        rate_limit: float = 0.5,
        **kwargs,
    ):
        super().__init__(base_url=base_url, rate_limit=rate_limit, **kwargs)
        sport_cfg = settings.get_sport_config("nhl")
        self.default_season: str = sport_cfg.default_season

    # ------------------------------------------------------------------
    # A) Fetch schedule
    # ------------------------------------------------------------------

    async def fetch_schedule(self, date_str: str = None) -> List[Dict[str, Any]]:
        """
        Fetch the NHL schedule for a given date.

        Uses the ``/score`` endpoint instead of ``/schedule`` because
        ``/score`` includes live game data (period, clock, scores) that
        ``/schedule`` omits.

        Args:
            date_str: Date in YYYY-MM-DD format, or None for today.

        Returns:
            List of game dicts with keys: id, start_time, home_team,
            away_team, venue, status, season, game_type, period, clock.
        """
        # /score has everything /schedule has, plus periodDescriptor
        # and clock for live games.
        if date_str:
            path = f"/score/{date_str}"
        else:
            path = "/score/now"

        data = await self.fetch_json(path)
        games: List[Dict[str, Any]] = []

        # /score returns a top-level "games" array
        games_list = data.get("games", [])
        if games_list:
            day_date = data.get("currentDate", date_str or "")
            for game_raw in games_list:
                game = self._parse_schedule_game(game_raw, day_date)
                if game:
                    games.append(game)
        else:
            # Fallback: /schedule uses gameWeek > games
            game_week = data.get("gameWeek", [])
            for day_entry in game_week:
                day_date = day_entry.get("date", "")
                for game_raw in day_entry.get("games", []):
                    game = self._parse_schedule_game(game_raw, day_date)
                    if game:
                        games.append(game)

        logger.info("Fetched %d games from schedule", len(games))
        return games

    def _parse_schedule_game(
        self, game_raw: dict, day_date: str
    ) -> Optional[Dict[str, Any]]:
        """Parse a single game entry from the schedule response."""
        try:
            home = game_raw.get("homeTeam", {})
            away = game_raw.get("awayTeam", {})

            # Extract live game clock info if available
            period_desc = game_raw.get("periodDescriptor", {})
            clock_info = game_raw.get("clock", {})

            return {
                "id": game_raw.get("id"),
                "season": str(game_raw.get("season", self.default_season)),
                "game_type": str(game_raw.get("gameType", "2")),
                "game_date": day_date,
                "start_time": game_raw.get("startTimeUTC"),
                "venue": self.safe_get(game_raw, "venue", "default")
                or self.safe_get(game_raw, "venue", "name"),
                "status": game_raw.get("gameState", "FUT"),
                "period": period_desc.get("number"),
                "period_type": period_desc.get("periodType"),  # REG, OT, SO
                "clock": clock_info.get("timeRemaining"),
                "clock_running": clock_info.get("running", False),
                "in_intermission": clock_info.get("inIntermission", False),
                "home_team": {
                    "abbrev": self.safe_get(home, "abbrev"),
                    "id": home.get("id"),
                    "score": home.get("score"),
                    "name": self.safe_get(home, "placeName", "default")
                    or self.safe_get(home, "name", "default"),
                },
                "away_team": {
                    "abbrev": self.safe_get(away, "abbrev"),
                    "id": away.get("id"),
                    "score": away.get("score"),
                    "name": self.safe_get(away, "placeName", "default")
                    or self.safe_get(away, "name", "default"),
                },
            }
        except Exception as exc:
            logger.warning("Failed to parse schedule game: %s", exc)
            return None

    # ------------------------------------------------------------------
    # B) Fetch standings
    # ------------------------------------------------------------------

    async def fetch_standings(self) -> List[Dict[str, Any]]:
        """
        Fetch current NHL standings.

        Returns:
            List of team standing dicts with record, points, goals, etc.
        """
        data = await self.fetch_json("/standings/now")
        standings: List[Dict[str, Any]] = []

        for entry in data.get("standings", []):
            parsed = self._parse_standing(entry)
            if parsed:
                standings.append(parsed)

        logger.info("Fetched standings for %d teams", len(standings))
        return standings

    def _parse_standing(self, entry: dict) -> Optional[Dict[str, Any]]:
        """Parse a single standings entry."""
        try:
            return {
                "team_abbrev": self.safe_get(entry, "teamAbbrev", "default"),
                "team_name": self.safe_get(entry, "teamName", "default"),
                "team_common_name": self.safe_get(
                    entry, "teamCommonName", "default"
                ),
                "team_logo": self.safe_get(entry, "teamLogo"),
                "external_id": str(entry.get("teamId", "")),
                "season": str(entry.get("seasonId", self.default_season)),
                "conference": self.safe_get(entry, "conferenceName"),
                "division": self.safe_get(entry, "divisionName"),
                "games_played": entry.get("gamesPlayed", 0),
                "wins": entry.get("wins", 0),
                "losses": entry.get("losses", 0),
                "ot_losses": entry.get("otLosses", 0),
                "points": entry.get("points", 0),
                "points_pct": entry.get("pointPctg", 0.0),
                "goals_for": entry.get("goalFor", 0),
                "goals_against": entry.get("goalAgainst", 0),
                "goals_for_per_game": entry.get("goalsForPctg", None),
                "goals_against_per_game": entry.get("goalsAgainstPctg", None),
                "power_play_pct": entry.get("powerPlayPctg", None),
                "penalty_kill_pct": entry.get("penaltyKillPctg", None),
                "regulation_wins": entry.get("regulationWins", 0),
                "regulation_plus_ot_wins": entry.get(
                    "regulationPlusOtWins", 0
                ),
                "goal_differential": entry.get("goalDifferential", 0),
                "home_wins": entry.get("homeWins", 0),
                "home_losses": entry.get("homeLosses", 0),
                "home_ot_losses": entry.get("homeOtLosses", 0),
                "away_wins": entry.get("roadWins", 0),
                "away_losses": entry.get("roadLosses", 0),
                "away_ot_losses": entry.get("roadOtLosses", 0),
                "l10_wins": entry.get("l10Wins", 0),
                "l10_losses": entry.get("l10Losses", 0),
                "l10_ot_losses": entry.get("l10OtLosses", 0),
                "streak_code": self.safe_get(entry, "streakCode"),
                "streak_count": entry.get("streakCount", 0),
                "wins_in_regulation": entry.get("winsInRegulation", 0),
                "wins_in_ot": entry.get("winsInOt", 0),
                "wins_in_shootout": entry.get("winsInShootout", 0),
                "shots_for_per_game": entry.get("shotsForPerGame", None),
                "shots_against_per_game": entry.get("shotsAgainstPerGame", None),
                "faceoff_win_pct": entry.get("faceoffWinPct", None) or entry.get("faceoffWinPctg", None),
            }
        except Exception as exc:
            logger.warning("Failed to parse standing entry: %s", exc)
            return None

    # ------------------------------------------------------------------
    # C) Fetch team / club stats
    # ------------------------------------------------------------------

    async def fetch_team_stats(self, team_abbrev: str) -> Dict[str, Any]:
        """
        Fetch club-level player stats for a given team.

        Args:
            team_abbrev: Three-letter team abbreviation (e.g., "BOS").

        Returns:
            Dict with skaters and goalies arrays from the API.
        """
        path = f"/club-stats/{team_abbrev}/now"
        data = await self.fetch_json(path)
        return data

    # ------------------------------------------------------------------
    # D) Fetch roster
    # ------------------------------------------------------------------

    async def fetch_roster(self, team_abbrev: str) -> Dict[str, Any]:
        """
        Fetch the current roster for a team.

        Args:
            team_abbrev: Three-letter team abbreviation (e.g., "BOS").

        Returns:
            Dict with forwards, defensemen, goalies arrays.
        """
        path = f"/roster/{team_abbrev}/current"
        data = await self.fetch_json(path)
        return data

    # ------------------------------------------------------------------
    # E) Fetch game boxscore
    # ------------------------------------------------------------------

    async def fetch_game_boxscore(self, game_id: int) -> Dict[str, Any]:
        """
        Fetch the full boxscore for a completed game.

        Args:
            game_id: NHL API game ID.

        Returns:
            Full boxscore dict including playerByGameStats.
        """
        path = f"/gamecenter/{game_id}/boxscore"
        data = await self.fetch_json(path)
        return data

    # ------------------------------------------------------------------
    # F) Fetch game landing
    # ------------------------------------------------------------------

    async def fetch_game_landing(self, game_id: int) -> Dict[str, Any]:
        """
        Fetch the game landing page data (preview/recap).

        Args:
            game_id: NHL API game ID.

        Returns:
            Game landing dict with summary, stats, plays, etc.
        """
        path = f"/gamecenter/{game_id}/landing"
        data = await self.fetch_json(path)
        return data

    # ------------------------------------------------------------------
    # G) Fetch player stats
    # ------------------------------------------------------------------

    async def fetch_player_stats(self, player_id: int) -> Dict[str, Any]:
        """
        Fetch career and current-season stats for a player.

        Args:
            player_id: NHL API player ID.

        Returns:
            Player landing dict with bio, season stats, career stats.
        """
        path = f"/player/{player_id}/landing"
        data = await self.fetch_json(path)
        return data

    # ------------------------------------------------------------------
    # H) Fetch team schedule
    # ------------------------------------------------------------------

    async def fetch_team_schedule(
        self, team_abbrev: str, season: str = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch the full season schedule for a specific team.

        Args:
            team_abbrev: Three-letter team abbreviation.
            season: Season string (e.g., "20252026"). Defaults to "now".

        Returns:
            List of game dicts from the team schedule.
        """
        season = season or "now"
        path = f"/club-schedule-season/{team_abbrev}/{season}"
        data = await self.fetch_json(path)

        games = data.get("games", [])
        logger.info(
            "Fetched %d games for %s schedule (%s)",
            len(games),
            team_abbrev,
            season,
        )
        return games

    # ------------------------------------------------------------------
    # Helper: look up or create a Team by abbreviation
    # ------------------------------------------------------------------

    async def _get_or_create_team(
        self,
        db: AsyncSession,
        abbrev: str,
        name: str = "",
        external_id: str = "",
        **kwargs,
    ) -> Optional[Team]:
        """
        Find an existing Team by abbreviation, or create a new one.

        Returns the Team ORM instance, or None if abbrev is empty.
        """
        if not abbrev:
            return None

        result = await db.execute(
            select(Team).where(Team.abbreviation == abbrev)
        )
        team = result.scalar_one_or_none()

        if team is None:
            team = Team(
                external_id=external_id or abbrev,
                name=name or abbrev,
                abbreviation=abbrev,
                sport="nhl",
                active=True,
                **kwargs,
            )
            db.add(team)
            await db.flush()
            logger.info("Created new team: %s (%s)", team.name, abbrev)

        return team

    async def _get_or_create_player(
        self,
        db: AsyncSession,
        external_id: str,
        name: str,
        team_id: Optional[int] = None,
        position: Optional[str] = None,
        **kwargs,
    ) -> Player:
        """
        Find an existing Player by external_id, or create a new one.
        Updates mutable fields if the player already exists.
        """
        result = await db.execute(
            select(Player).where(Player.external_id == external_id)
        )
        player = result.scalar_one_or_none()

        if player is None:
            player = Player(
                external_id=external_id,
                name=name,
                team_id=team_id,
                position=position,
                sport="nhl",
                active=True,
                **kwargs,
            )
            db.add(player)
            await db.flush()
            logger.debug("Created player: %s (ext_id=%s)", name, external_id)
        else:
            # Update team assignment and position if changed
            if team_id is not None and player.team_id != team_id:
                player.team_id = team_id
            if position and player.position != position:
                player.position = position
            for key, val in kwargs.items():
                if hasattr(player, key) and val is not None:
                    setattr(player, key, val)

        return player

    # ------------------------------------------------------------------
    # I) Sync teams from standings
    # ------------------------------------------------------------------

    async def sync_teams(self, db: AsyncSession) -> None:
        """
        Fetch current standings and create/update Team and TeamStats records.

        For each team in the standings:
        - Create or update the Team record (name, conference, division, logo).
        - Create or update the TeamStats record for the current season.
        """
        logger.info("Syncing teams from standings...")
        standings = await self.fetch_standings()

        for entry in standings:
            abbrev = entry.get("team_abbrev")
            if not abbrev:
                continue

            # -- Team record --
            team = await self._get_or_create_team(
                db,
                abbrev=abbrev,
                name=entry.get("team_name", abbrev),
                external_id=entry.get("external_id", abbrev),
                city=entry.get("team_common_name"),
                division=entry.get("division"),
                conference=entry.get("conference"),
                logo_url=entry.get("team_logo"),
            )

            # Update mutable fields on existing teams
            if team is not None:
                team.division = entry.get("division") or team.division
                team.conference = entry.get("conference") or team.conference
                team.logo_url = entry.get("team_logo") or team.logo_url
                if entry.get("team_name"):
                    team.name = entry["team_name"]

            # -- TeamStats record --
            season = entry.get("season", self.default_season)
            result = await db.execute(
                select(TeamStats).where(
                    TeamStats.team_id == team.id,
                    TeamStats.season == season,
                )
            )
            stats = result.scalar_one_or_none()

            home_w = entry.get("home_wins", 0)
            home_l = entry.get("home_losses", 0)
            home_otl = entry.get("home_ot_losses", 0)
            away_w = entry.get("away_wins", 0)
            away_l = entry.get("away_losses", 0)
            away_otl = entry.get("away_ot_losses", 0)
            l10_w = entry.get("l10_wins", 0)
            l10_l = entry.get("l10_losses", 0)
            l10_otl = entry.get("l10_ot_losses", 0)

            gp = entry.get("games_played", 0)
            gf = entry.get("goals_for", 0)
            ga = entry.get("goals_against", 0)

            stats_data = dict(
                games_played=gp,
                wins=entry.get("wins", 0),
                losses=entry.get("losses", 0),
                ot_losses=entry.get("ot_losses", 0),
                points=entry.get("points", 0),
                goals_for=gf,
                goals_against=ga,
                goals_for_per_game=round(gf / gp, 2) if gp > 0 else None,
                goals_against_per_game=round(ga / gp, 2) if gp > 0 else None,
                power_play_pct=entry.get("power_play_pct"),
                penalty_kill_pct=entry.get("penalty_kill_pct"),
                shots_for_per_game=entry.get("shots_for_per_game"),
                shots_against_per_game=entry.get("shots_against_per_game"),
                faceoff_win_pct=entry.get("faceoff_win_pct"),
                home_record=f"{home_w}-{home_l}-{home_otl}",
                away_record=f"{away_w}-{away_l}-{away_otl}",
                record_last_10=f"{l10_w}-{l10_l}-{l10_otl}",
                date_updated=datetime.now(timezone.utc),
            )

            if stats is None:
                stats = TeamStats(
                    team_id=team.id,
                    season=season,
                    **stats_data,
                )
                db.add(stats)
            else:
                for key, val in stats_data.items():
                    setattr(stats, key, val)

        await db.flush()

        # Mark teams not in the current standings as inactive (e.g. ARI
        # after relocation to UTA).  This prevents sync_rosters from
        # hitting 404s for defunct teams.
        active_abbrevs = {
            e.get("team_abbrev") for e in standings if e.get("team_abbrev")
        }
        if active_abbrevs:
            all_teams_result = await db.execute(
                select(Team).where(Team.sport == "nhl")
            )
            for t in all_teams_result.scalars():
                if t.abbreviation not in active_abbrevs and t.active:
                    t.active = False
                    logger.info(
                        "Deactivated team not in standings: %s (%s)",
                        t.name, t.abbreviation,
                    )
                elif t.abbreviation in active_abbrevs and not t.active:
                    t.active = True
            await db.flush()

        logger.info("Teams sync complete: %d teams processed", len(standings))

    # ------------------------------------------------------------------
    # J) Sync schedule
    # ------------------------------------------------------------------

    async def sync_schedule(
        self, db: AsyncSession, date_str: str = None
    ) -> List[Game]:
        """
        Fetch the schedule for a date and create/update Game records.

        Args:
            db: Async SQLAlchemy session.
            date_str: Date in YYYY-MM-DD format, or None for today.

        Returns:
            List of Game ORM objects that were created or updated.
        """
        logger.info("Syncing schedule for date=%s", date_str or "today")
        schedule = await self.fetch_schedule(date_str)
        games: List[Game] = []

        for game_data in schedule:
            game_ext_id = str(game_data.get("id", ""))
            if not game_ext_id:
                continue

            # Look up teams
            home_info = game_data.get("home_team", {})
            away_info = game_data.get("away_team", {})

            home_team = await self._get_or_create_team(
                db,
                abbrev=home_info.get("abbrev", ""),
                name=home_info.get("name", ""),
                external_id=str(home_info.get("id", "")),
            )
            away_team = await self._get_or_create_team(
                db,
                abbrev=away_info.get("abbrev", ""),
                name=away_info.get("name", ""),
                external_id=str(away_info.get("id", "")),
            )

            if not home_team or not away_team:
                logger.warning(
                    "Skipping game %s: could not resolve teams", game_ext_id
                )
                continue

            # Parse date
            game_date_str = game_data.get("game_date", "")
            try:
                game_date_val = date.fromisoformat(game_date_str)
            except (ValueError, TypeError):
                game_date_val = date.today()

            # Parse start time
            start_time = None
            start_str = game_data.get("start_time")
            if start_str:
                try:
                    start_time = datetime.fromisoformat(
                        start_str.replace("Z", "+00:00")
                    )
                except (ValueError, TypeError):
                    start_time = None

            # Map API game state to our status
            api_status = game_data.get("status", "FUT")
            status = self._map_game_status(api_status)

            # Season / game_type
            season = game_data.get("season", self.default_season)
            game_type_raw = game_data.get("game_type", "2")
            game_type_map = {
                "1": "preseason",
                "2": "regular",
                "3": "playoffs",
                "4": "allstar",
            }
            game_type = game_type_map.get(str(game_type_raw), str(game_type_raw))

            # Check if game already exists
            result = await db.execute(
                select(Game).where(Game.external_id == game_ext_id)
            )
            game = result.scalar_one_or_none()

            if game is None:
                game = Game(
                    external_id=game_ext_id,
                    sport="nhl",
                    season=season,
                    game_type=game_type,
                    date=game_date_val,
                    start_time=start_time,
                    venue=game_data.get("venue"),
                    status=status,
                    home_team_id=home_team.id,
                    away_team_id=away_team.id,
                    home_score=home_info.get("score"),
                    away_score=away_info.get("score"),
                )
                db.add(game)
                await db.flush()
                logger.debug("Created game: %s", game_ext_id)
            else:
                # Snapshot pregame odds when game transitions to live
                old_status = game.status
                if (
                    old_status and old_status.lower() in ("scheduled", "pregame", "preview")
                    and status in ("in_progress", "live")
                    and game.pregame_home_moneyline is None
                    and game.home_moneyline is not None
                ):
                    game.pregame_home_moneyline = game.home_moneyline
                    game.pregame_away_moneyline = game.away_moneyline
                    game.pregame_over_under_line = game.over_under_line
                    game.pregame_home_spread_line = game.home_spread_line
                    game.pregame_away_spread_line = game.away_spread_line
                    game.pregame_home_spread_price = game.home_spread_price
                    game.pregame_away_spread_price = game.away_spread_price
                    game.pregame_over_price = game.over_price
                    game.pregame_under_price = game.under_price
                    logger.info(
                        "Pregame odds snapshot saved for game %s (status %s -> %s)",
                        game_ext_id, old_status, status,
                    )

                # Update status and scores
                game.status = status
                if home_info.get("score") is not None:
                    game.home_score = home_info["score"]
                if away_info.get("score") is not None:
                    game.away_score = away_info["score"]
                if start_time:
                    game.start_time = start_time
                if game_data.get("venue"):
                    game.venue = game_data["venue"]

            # Update live clock info (period, time remaining)
            if game_data.get("period") is not None:
                game.period = game_data["period"]
            if game_data.get("period_type"):
                game.period_type = game_data["period_type"]
            if game_data.get("clock") is not None:
                game.clock = game_data["clock"]
            if game_data.get("clock_running") is not None:
                game.clock_running = game_data["clock_running"]
            if game_data.get("in_intermission") is not None:
                game.in_intermission = game_data["in_intermission"]

            # Infer went_to_overtime from schedule data when boxscore
            # hasn't been fetched yet. The schedule API provides period
            # number and periodType (REG, OT, SO) which is enough to
            # determine OT status without waiting for the full boxscore.
            if (
                status == "final"
                and game.went_to_overtime is None
            ):
                period_num = game_data.get("period")
                period_type = (game_data.get("period_type") or "").upper()
                if period_type in ("OT", "SO") or (
                    period_num is not None and period_num > 3
                ):
                    game.went_to_overtime = True
                    logger.info(
                        "Inferred OT=True for game %s from schedule "
                        "(period=%s, type=%s)",
                        game_ext_id, period_num, period_type,
                    )
                elif period_num is not None and period_num <= 3:
                    game.went_to_overtime = False

            games.append(game)

        await db.flush()
        logger.info("Schedule sync complete: %d games", len(games))
        return games

    @staticmethod
    def _map_game_status(api_status: str) -> str:
        """Map NHL API gameState codes to our internal status strings.

        Case-insensitive: the NHL API typically returns uppercase codes
        (FUT, LIVE, …) but we normalise to uppercase before lookup so
        mixed-case or lowercase values don't slip through unmapped.
        """
        mapping = {
            "FUT": "scheduled",
            "PRE": "scheduled",
            "PREGAME": "scheduled",
            "LIVE": "in_progress",
            "CRIT": "in_progress",
            "OFF": "final",
            "FINAL": "final",
            "OVER": "final",
        }
        return mapping.get(api_status.upper(), api_status.lower())

    # ------------------------------------------------------------------
    # K) Sync game results (boxscore)
    # ------------------------------------------------------------------

    async def sync_game_results(
        self, db: AsyncSession, game_id: int
    ) -> None:
        """
        Fetch the boxscore for a completed game and update all related records.

        Updates:
        - Game scores, per-period scores, overtime flag, status
        - GamePlayerStats for every skater
        - GameGoalieStats for every goalie
        - winning_team_id and first_goal_team_id
        - HeadToHead record for the two teams

        Args:
            db: Async SQLAlchemy session.
            game_id: The NHL API game ID (external id).
        """
        logger.debug("Syncing game results for game_id=%d", game_id)

        # Fetch boxscore
        try:
            boxscore = await self.fetch_game_boxscore(game_id)
        except APIResponseError as exc:
            logger.error(
                "Failed to fetch boxscore for game %d: %s", game_id, exc
            )
            return

        # Find the Game record
        result = await db.execute(
            select(Game).where(Game.external_id == str(game_id))
        )
        game = result.scalar_one_or_none()
        if game is None:
            logger.warning(
                "Game %d not found in DB; skipping results sync", game_id
            )
            return

        # -- Snapshot closing odds before marking final (for CLV tracking) --
        # Only snapshot if we haven't already (closing fields still null)
        if game.closing_home_moneyline is None and game.home_moneyline is not None:
            game.closing_home_moneyline = game.home_moneyline
            game.closing_away_moneyline = game.away_moneyline
            game.closing_over_under_line = game.over_under_line
            game.closing_over_price = game.over_price
            game.closing_under_price = game.under_price
            game.closing_home_spread_line = game.home_spread_line
            game.closing_home_spread_price = game.home_spread_price
            game.closing_away_spread_price = game.away_spread_price

        # -- Final scores --
        home_score = self.safe_get(boxscore, "homeTeam", "score") or 0
        away_score = self.safe_get(boxscore, "awayTeam", "score") or 0
        game.home_score = home_score
        game.away_score = away_score
        game.status = "final"

        # -- Period scores --
        linescore = boxscore.get("linescore", {})
        by_period = linescore.get("byPeriod", [])

        ot_home_total = 0
        ot_away_total = 0
        went_to_ot = False

        # Only reset period columns when the API actually provides
        # period data.  Without this guard, games whose boxscore
        # lacks a byPeriod breakdown get their scores reset to NULL
        # on every sync, causing the backfill query (which checks
        # home_score_p1 IS NULL) to re-select them indefinitely.
        if by_period:
            game.home_score_p1 = None
            game.away_score_p1 = None
            game.home_score_p2 = None
            game.away_score_p2 = None
            game.home_score_p3 = None
            game.away_score_p3 = None
            game.home_score_ot = None
            game.away_score_ot = None

        for period in by_period:
            # Each byPeriod entry has periodDescriptor with number/periodType
            pd = period.get("periodDescriptor", {})
            period_num = pd.get("number")
            period_type = (pd.get("periodType") or "").upper()

            # Scores may be plain ints or objects with a "goals" sub-key
            raw_home = period.get("home", 0)
            raw_away = period.get("away", 0)
            home_p = raw_home.get("goals", 0) if isinstance(raw_home, dict) else (raw_home or 0)
            away_p = raw_away.get("goals", 0) if isinstance(raw_away, dict) else (raw_away or 0)

            if period_type in ("OT", "SO") or (period_num is not None and period_num > 3):
                ot_home_total += home_p
                ot_away_total += away_p
                went_to_ot = True
            elif period_num == 1:
                game.home_score_p1 = home_p
                game.away_score_p1 = away_p
            elif period_num == 2:
                game.home_score_p2 = home_p
                game.away_score_p2 = away_p
            elif period_num == 3:
                game.home_score_p3 = home_p
                game.away_score_p3 = away_p

        if went_to_ot:
            game.home_score_ot = ot_home_total
            game.away_score_ot = ot_away_total

        # Also check gameOutcome.lastPeriodType as a fallback for OT detection
        if not went_to_ot:
            last_period_type = (
                self.safe_get(boxscore, "gameOutcome", "lastPeriodType") or ""
            ).upper()
            if last_period_type in ("OT", "SO"):
                went_to_ot = True

        game.went_to_overtime = went_to_ot

        # -- Shots on goal --
        home_shots = self.safe_get(boxscore, "homeTeam", "sog")
        away_shots = self.safe_get(boxscore, "awayTeam", "sog")
        if home_shots is not None:
            game.home_shots = home_shots
        if away_shots is not None:
            game.away_shots = away_shots

        # -- Winning team --
        if home_score > away_score:
            game.winning_team_id = game.home_team_id
        elif away_score > home_score:
            game.winning_team_id = game.away_team_id

        # -- First goal team (from landing data) --
        try:
            landing = await self.fetch_game_landing(game_id)
            first_goal_team_id = self._determine_first_goal_team(
                landing, game
            )
            if first_goal_team_id is not None:
                game.first_goal_team_id = first_goal_team_id
        except Exception as exc:
            logger.debug(
                "Could not determine first goal team for game %d: %s",
                game_id,
                exc,
            )

        # -- Player stats from boxscore --
        player_by_game = boxscore.get("playerByGameStats", {})

        # Process home team players
        await self._process_team_boxscore(
            db, game, player_by_game.get("homeTeam", {})
        )

        # Process away team players
        await self._process_team_boxscore(
            db, game, player_by_game.get("awayTeam", {})
        )

        # -- Update head-to-head --
        await self._update_head_to_head(db, game)

        await db.flush()
        logger.debug(
            "Game results synced: %d (home %d - away %d, OT=%s)",
            game_id,
            home_score,
            away_score,
            game.went_to_overtime,
        )

    def _determine_first_goal_team(
        self, landing: dict, game: Game
    ) -> Optional[int]:
        """
        Determine which team scored first from the game landing data.

        The landing response has a ``summary.scoring`` array with
        period-by-period goals. The first goal in the first period
        with goals determines the first-scoring team.
        """
        scoring = self.safe_get(landing, "summary", "scoring") or []
        for period in scoring:
            goals = period.get("goals", [])
            if goals:
                first_goal = goals[0]
                # The goal entry has a teamAbbrev field (or teamAbbrev.default)
                scoring_abbrev = self.safe_get(
                    first_goal, "teamAbbrev", "default"
                )
                if scoring_abbrev is None:
                    scoring_abbrev = first_goal.get("teamAbbrev")
                if isinstance(scoring_abbrev, dict):
                    scoring_abbrev = scoring_abbrev.get("default")

                if not scoring_abbrev:
                    return None

                home_abbrev = self.safe_get(landing, "homeTeam", "abbrev")
                away_abbrev = self.safe_get(landing, "awayTeam", "abbrev")

                if scoring_abbrev == home_abbrev:
                    return game.home_team_id
                elif scoring_abbrev == away_abbrev:
                    return game.away_team_id

        return None

    async def _process_team_boxscore(
        self,
        db: AsyncSession,
        game: Game,
        team_stats: dict,
    ) -> None:
        """
        Process the boxscore stats for one team (home or away).

        Creates GamePlayerStats for skaters and GameGoalieStats for goalies.
        The team_stats dict has keys: forwards, defense, goalies.
        """
        # Process forwards and defensemen (skaters)
        skaters = team_stats.get("forwards", []) + team_stats.get("defense", [])
        for skater_data in skaters:
            await self._upsert_game_player_stats(db, game, skater_data)

        # Process goalies
        for goalie_data in team_stats.get("goalies", []):
            await self._upsert_game_goalie_stats(db, game, goalie_data)

    async def _upsert_game_player_stats(
        self,
        db: AsyncSession,
        game: Game,
        skater_data: dict,
    ) -> None:
        """Create or update a GamePlayerStats record for a single skater."""
        player_id_ext = str(skater_data.get("playerId", ""))
        if not player_id_ext:
            return

        # Resolve player name from the various API formats
        player_name = self.safe_get(skater_data, "name", "default")
        if not player_name:
            first = self.safe_get(skater_data, "firstName", "default") or ""
            last = self.safe_get(skater_data, "lastName", "default") or ""
            player_name = f"{first} {last}".strip()
        if not player_name:
            player_name = f"Player {player_id_ext}"

        position = skater_data.get("position", "")

        player = await self._get_or_create_player(
            db,
            external_id=player_id_ext,
            name=player_name,
            position=position,
        )

        # Check if record already exists (unique on game_id + player_id)
        result = await db.execute(
            select(GamePlayerStats).where(
                GamePlayerStats.game_id == game.id,
                GamePlayerStats.player_id == player.id,
            )
        )
        existing = result.scalar_one_or_none()

        # Parse stats from boxscore
        goals = skater_data.get("goals", 0) or 0
        assists = skater_data.get("assists", 0) or 0
        points = goals + assists
        plus_minus = skater_data.get("plusMinus", 0) or 0
        pim = skater_data.get("pim", 0) or 0
        shots = skater_data.get("shots", 0) or skater_data.get("sog", 0) or 0
        hits = skater_data.get("hits", 0) or 0
        blocked = (
            skater_data.get("blockedShots", 0)
            or skater_data.get("blockingShots", 0)
            or 0
        )

        # Power-play / shorthanded goals
        pp_goals = skater_data.get("powerPlayGoals", 0) or 0
        sh_goals = skater_data.get("shorthandedGoals", 0) or 0

        # Time on ice
        toi_str = skater_data.get("toi", "")
        toi_minutes = (
            self.parse_toi_minutes(toi_str)
            if isinstance(toi_str, str)
            else toi_str
        )

        if existing is None:
            gps = GamePlayerStats(
                game_id=game.id,
                player_id=player.id,
                goals=goals,
                assists=assists,
                points=points,
                plus_minus=plus_minus,
                pim=pim,
                shots=shots,
                hits=hits,
                blocked_shots=blocked,
                pp_goals=pp_goals,
                sh_goals=sh_goals,
                toi=toi_minutes,
            )
            db.add(gps)
        else:
            existing.goals = goals
            existing.assists = assists
            existing.points = points
            existing.plus_minus = plus_minus
            existing.pim = pim
            existing.shots = shots
            existing.hits = hits
            existing.blocked_shots = blocked
            existing.pp_goals = pp_goals
            existing.sh_goals = sh_goals
            existing.toi = toi_minutes

    async def _upsert_game_goalie_stats(
        self,
        db: AsyncSession,
        game: Game,
        goalie_data: dict,
    ) -> None:
        """Create or update a GameGoalieStats record for a single goalie."""
        player_id_ext = str(goalie_data.get("playerId", ""))
        if not player_id_ext:
            return

        player_name = self.safe_get(goalie_data, "name", "default")
        if not player_name:
            first = self.safe_get(goalie_data, "firstName", "default") or ""
            last = self.safe_get(goalie_data, "lastName", "default") or ""
            player_name = f"{first} {last}".strip()
        if not player_name:
            player_name = f"Goalie {player_id_ext}"

        player = await self._get_or_create_player(
            db,
            external_id=player_id_ext,
            name=player_name,
            position="G",
        )

        # Check if record already exists (unique on game_id + player_id)
        result = await db.execute(
            select(GameGoalieStats).where(
                GameGoalieStats.game_id == game.id,
                GameGoalieStats.player_id == player.id,
            )
        )
        existing = result.scalar_one_or_none()

        # Parse goalie stats
        saves_val = goalie_data.get("saves", 0) or 0
        goals_against_val = goalie_data.get("goalsAgainst", 0) or 0

        # shots_against may come from different field names
        shots_against_val = goalie_data.get("shotsAgainst", 0)
        if not shots_against_val:
            sa_str = goalie_data.get("saveShotsAgainst", "")
            if isinstance(sa_str, str) and sa_str.isdigit():
                shots_against_val = int(sa_str)
            elif isinstance(sa_str, (int, float)):
                shots_against_val = int(sa_str)
            else:
                shots_against_val = 0

        # Derive missing values
        if shots_against_val == 0 and saves_val > 0:
            shots_against_val = saves_val + goals_against_val
        if goals_against_val == 0 and shots_against_val > saves_val:
            goals_against_val = shots_against_val - saves_val

        # Save percentage
        save_pct = None
        save_pct_raw = goalie_data.get("savePctg") or goalie_data.get("savePct")
        if save_pct_raw is not None:
            try:
                save_pct = float(save_pct_raw)
            except (ValueError, TypeError):
                save_pct = None
        elif shots_against_val > 0:
            save_pct = round(saves_val / shots_against_val, 4)

        decision = goalie_data.get("decision")

        # Time on ice
        toi_str = goalie_data.get("toi", "")
        toi_minutes = (
            self.parse_toi_minutes(toi_str)
            if isinstance(toi_str, str)
            else toi_str
        )

        if existing is None:
            ggs = GameGoalieStats(
                game_id=game.id,
                player_id=player.id,
                saves=saves_val,
                shots_against=shots_against_val,
                goals_against=goals_against_val,
                save_pct=save_pct,
                decision=decision,
                toi=toi_minutes,
            )
            db.add(ggs)
        else:
            existing.saves = saves_val
            existing.shots_against = shots_against_val
            existing.goals_against = goals_against_val
            existing.save_pct = save_pct
            existing.decision = decision
            existing.toi = toi_minutes

    async def _update_head_to_head(
        self, db: AsyncSession, game: Game
    ) -> None:
        """
        Update the HeadToHead record for the two teams in a completed game.

        Ensures team1_id < team2_id to avoid duplicate records.
        """
        if (
            game.home_team_id is None
            or game.away_team_id is None
            or game.home_score is None
            or game.away_score is None
        ):
            return

        # Consistent ordering: team1 has the lower ID
        t1 = min(game.home_team_id, game.away_team_id)
        t2 = max(game.home_team_id, game.away_team_id)
        season = game.season or self.default_season

        result = await db.execute(
            select(HeadToHead).where(
                HeadToHead.team1_id == t1,
                HeadToHead.team2_id == t2,
                HeadToHead.season == season,
            )
        )
        h2h = result.scalar_one_or_none()

        # Determine winner from team1's perspective
        if game.home_team_id == t1:
            t1_score = game.home_score
            t2_score = game.away_score
        else:
            t1_score = game.away_score
            t2_score = game.home_score

        went_ot = bool(game.went_to_overtime)

        # Determine which internal team id won
        winner_id = game.winning_team_id

        if h2h is None:
            h2h = HeadToHead(
                team1_id=t1,
                team2_id=t2,
                season=season,
                games_played=1,
                team1_wins=1 if t1_score > t2_score else 0,
                team2_wins=1 if t2_score > t1_score else 0,
                ot_games=1 if went_ot else 0,
                team1_goals=t1_score,
                team2_goals=t2_score,
                last_meeting_date=game.date,
                last_meeting_winner_id=winner_id,
            )
            db.add(h2h)
        else:
            h2h.games_played += 1
            if t1_score > t2_score:
                h2h.team1_wins += 1
            elif t2_score > t1_score:
                h2h.team2_wins += 1
            if went_ot:
                h2h.ot_games += 1
            h2h.team1_goals += t1_score
            h2h.team2_goals += t2_score
            h2h.last_meeting_date = game.date
            h2h.last_meeting_winner_id = winner_id

    # ------------------------------------------------------------------
    # L) Sync rosters
    # ------------------------------------------------------------------

    async def sync_rosters(self, db: AsyncSession) -> None:
        """
        Fetch rosters for all teams in the database and create/update
        Player records.
        """
        logger.info("Syncing rosters for all teams...")
        result = await db.execute(
            select(Team).where(Team.sport == "nhl", Team.active == True)  # noqa: E712
        )
        teams = result.scalars().all()

        for team in teams:
            try:
                roster_data = await self.fetch_roster(team.abbreviation)
            except APIResponseError as exc:
                logger.warning(
                    "Failed to fetch roster for %s: %s",
                    team.abbreviation,
                    exc,
                )
                continue

            # Process each position group
            for group_key in ("forwards", "defensemen", "goalies"):
                players_raw = roster_data.get(group_key, [])
                for player_raw in players_raw:
                    player_ext_id = str(player_raw.get("id", ""))
                    if not player_ext_id:
                        continue

                    first_name = (
                        self.safe_get(player_raw, "firstName", "default") or ""
                    )
                    last_name = (
                        self.safe_get(player_raw, "lastName", "default") or ""
                    )
                    full_name = f"{first_name} {last_name}".strip()

                    position_code = self.safe_get(player_raw, "positionCode")

                    # Parse optional fields
                    jersey = player_raw.get("sweaterNumber")
                    shoots_catches = player_raw.get("shootsCatches")
                    height_inches = player_raw.get("heightInInches")
                    weight_pounds = player_raw.get("weightInPounds")

                    birth_date_val = None
                    birth_date_str = player_raw.get("birthDate")
                    if birth_date_str:
                        try:
                            birth_date_val = date.fromisoformat(birth_date_str)
                        except (ValueError, TypeError):
                            birth_date_val = None

                    await self._get_or_create_player(
                        db,
                        external_id=player_ext_id,
                        name=full_name or f"Player {player_ext_id}",
                        team_id=team.id,
                        position=position_code,
                        jersey_number=jersey,
                        shoots_catches=shoots_catches,
                        height=height_inches,
                        weight=weight_pounds,
                        birth_date=birth_date_val,
                    )

            logger.debug(
                "Processed roster for %s (%s)",
                team.name,
                team.abbreviation,
            )

        await db.flush()
        logger.info("Rosters sync complete for %d teams", len(teams))

    # ------------------------------------------------------------------
    # M) Sync all
    # ------------------------------------------------------------------

    async def sync_all(self, db: AsyncSession) -> None:
        """
        Run all sync operations in logical order:

        1. sync_teams    - populate/update Team and TeamStats from standings
        2. sync_rosters  - populate/update Player records from rosters
        3. sync_schedule - populate/update Game records for today
        4. sync results  - backfill boxscore data for completed games

        This is the primary entry point for a full data refresh.
        """
        logger.info("Starting full NHL data sync...")

        # 1. Teams & standings
        await self.sync_teams(db)

        # 2. Rosters
        await self.sync_rosters(db)

        # 3. Today's schedule
        today_str = date.today().isoformat()
        games = await self.sync_schedule(db, today_str)

        # 4. Sync results for any completed games
        completed_games = [g for g in games if g.status == "final"]
        for game in completed_games:
            try:
                await self.sync_game_results(db, int(game.external_id))
            except Exception as exc:
                logger.error(
                    "Failed to sync results for game %s: %s",
                    game.external_id,
                    exc,
                )

        await db.flush()
        logger.info("Full NHL data sync complete.")

    # ------------------------------------------------------------------
    # Convenience: sync results for recent completed games
    # ------------------------------------------------------------------

    async def sync_recent_results(
        self,
        db: AsyncSession,
        days_back: int = 3,
    ) -> None:
        """
        Sync results for recently completed games that may not yet
        have full boxscore data.

        Args:
            db: Async SQLAlchemy session.
            days_back: Number of past days to scan.
        """
        cutoff = date.today() - timedelta(days=days_back)
        result = await db.execute(
            select(Game).where(
                Game.sport == "nhl",
                Game.date >= cutoff,
                Game.status == "final",
                # went_to_overtime is always set (True/False) after a
                # successful sync, so it reliably gates re-processing.
                # Previously used OR(home_score_p1 IS NULL, ...) but
                # home_score_p1 can stay NULL if the API lacks period
                # data, causing infinite re-sync loops.
                Game.went_to_overtime.is_(None),
            )
        )
        games = result.scalars().all()

        if not games:
            logger.info("Period scores backfill: all games up to date")
            return

        logger.info(
            "Backfilling boxscore data for %d games (past %d days)",
            len(games),
            days_back,
        )

        synced = 0
        for game in games:
            try:
                await self.sync_game_results(db, int(game.external_id))
                synced += 1
            except Exception as exc:
                logger.error(
                    "Failed to sync results for game %s: %s",
                    game.external_id,
                    exc,
                )

        logger.info("Period scores backfill complete: %d/%d games synced", synced, len(games))

        await db.flush()

    # ------------------------------------------------------------------
    # Sync historical seasons for H2H data
    # ------------------------------------------------------------------

    async def sync_historical_season(
        self,
        db: AsyncSession,
        season: str,
    ) -> int:
        """
        Sync a full past season's schedule and results to build H2H history.

        Fetches the schedule for a previous season using team schedules,
        creates Game records with final scores. This populates the Game
        table with historical data so that H2H queries return richer results.

        Args:
            db: Async SQLAlchemy session.
            season: Season string (e.g., "20242025" for 2024-25 season).

        Returns:
            Number of games synced.
        """
        logger.info("Syncing historical season: %s", season)

        # Get all teams from DB
        result = await db.execute(
            select(Team).where(Team.sport == "nhl", Team.active == True)
        )
        teams = result.scalars().all()

        if not teams:
            logger.warning("No teams found; run sync_teams first.")
            return 0

        # Fetch schedules for half the league (16 teams) to get broad
        # coverage (~75% of all games) while keeping sync fast.
        # Each game appears on two teams' schedules, and we deduplicate
        # by game_id so no double-counting occurs.
        sample_teams = teams[:16]
        seen_game_ids = set()
        games_created = 0

        for team in sample_teams:
            try:
                season_games = await self.fetch_team_schedule(
                    team.abbreviation, season
                )
            except Exception as exc:
                logger.warning(
                    "Failed to fetch %s schedule for season %s: %s",
                    team.abbreviation, season, exc,
                )
                continue

            for game_raw in season_games:
                game_id = game_raw.get("id")
                if not game_id or game_id in seen_game_ids:
                    continue
                seen_game_ids.add(game_id)

                # Only regular season games
                game_type = game_raw.get("gameType", 2)
                if str(game_type) != "2":
                    continue

                # Only completed games
                state = game_raw.get("gameState", "FUT")
                if state not in ("OFF", "FINAL"):
                    continue

                game_ext_id = str(game_id)

                # Skip if already in DB
                existing = await db.execute(
                    select(Game).where(Game.external_id == game_ext_id)
                )
                if existing.scalar_one_or_none():
                    continue

                # Parse teams
                home_info = game_raw.get("homeTeam", {})
                away_info = game_raw.get("awayTeam", {})
                home_abbrev = self.safe_get(home_info, "abbrev")
                away_abbrev = self.safe_get(away_info, "abbrev")

                if not home_abbrev or not away_abbrev:
                    continue

                home_team = await self._get_or_create_team(
                    db,
                    abbrev=home_abbrev,
                    name=self.safe_get(home_info, "placeName", "default") or home_abbrev,
                    external_id=str(home_info.get("id", "")),
                )
                away_team = await self._get_or_create_team(
                    db,
                    abbrev=away_abbrev,
                    name=self.safe_get(away_info, "placeName", "default") or away_abbrev,
                    external_id=str(away_info.get("id", "")),
                )

                if not home_team or not away_team:
                    continue

                # Parse date
                game_date_str = game_raw.get("gameDate", "")
                try:
                    game_date_val = date.fromisoformat(game_date_str)
                except (ValueError, TypeError):
                    continue

                # Parse start time
                start_time = None
                start_str = game_raw.get("startTimeUTC")
                if start_str:
                    try:
                        start_time = datetime.fromisoformat(
                            start_str.replace("Z", "+00:00")
                        )
                    except (ValueError, TypeError):
                        pass

                home_score = home_info.get("score")
                away_score = away_info.get("score")

                # Determine winner
                winning_team_id = None
                if home_score is not None and away_score is not None:
                    if home_score > away_score:
                        winning_team_id = home_team.id
                    elif away_score > home_score:
                        winning_team_id = away_team.id

                game = Game(
                    external_id=game_ext_id,
                    sport="nhl",
                    season=season,
                    game_type="regular",
                    date=game_date_val,
                    start_time=start_time,
                    venue=self.safe_get(game_raw, "venue", "default")
                    or self.safe_get(game_raw, "venue", "name"),
                    status="final",
                    home_team_id=home_team.id,
                    away_team_id=away_team.id,
                    home_score=home_score,
                    away_score=away_score,
                    winning_team_id=winning_team_id,
                )
                db.add(game)
                games_created += 1

                # Build H2H record from this game's scores
                await self._update_head_to_head(db, game)

        await db.flush()

        logger.info(
            "Historical season %s sync complete: %d new games",
            season, games_created,
        )
        return games_created
