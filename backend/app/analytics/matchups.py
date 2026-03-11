"""
Matchup analysis engine.

Computes player-vs-team and team-vs-team matchup profiles from
historical game data. These profiles reveal tendencies that basic
form and H2H win rates miss — e.g., a player who consistently
produces against a specific opponent, or two teams that always
play high-scoring games against each other.
"""

import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, case, desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.game import Game, GamePlayerStats
from app.models.matchup import PlayerMatchupStats, TeamMatchupProfile
from app.models.player import Player

logger = logging.getLogger(__name__)

_mc = settings.matchup


class MatchupEngine:
    """Computes and stores matchup analytics from historical game data."""

    # ------------------------------------------------------------------ #
    #  Player vs Team matchup stats                                       #
    # ------------------------------------------------------------------ #

    async def compute_player_matchup(
        self,
        db: AsyncSession,
        player_id: int,
        opponent_team_id: int,
        season: Optional[str] = None,
    ) -> Optional[PlayerMatchupStats]:
        """Compute a player's performance against a specific opponent.

        Aggregates all games where this player faced the opponent team
        and calculates per-game rates + deviation from their overall average.

        Returns the updated PlayerMatchupStats record, or None if
        insufficient data.
        """
        # Get all games involving this opponent
        games_stmt = (
            select(Game.id)
            .where(
                and_(
                    Game.status == "final",
                    or_(
                        Game.home_team_id == opponent_team_id,
                        Game.away_team_id == opponent_team_id,
                    ),
                )
            )
        )
        if season:
            games_stmt = games_stmt.where(Game.season == season)
        games_result = await db.execute(games_stmt)
        opponent_game_ids = [row[0] for row in games_result.all()]

        if not opponent_game_ids:
            return None

        # Get player stats in those games
        stats_stmt = (
            select(
                func.count().label("games"),
                func.sum(GamePlayerStats.goals).label("goals"),
                func.sum(GamePlayerStats.assists).label("assists"),
                func.sum(GamePlayerStats.points).label("points"),
                func.sum(GamePlayerStats.shots).label("shots"),
                func.sum(GamePlayerStats.plus_minus).label("plus_minus"),
                func.sum(GamePlayerStats.hits).label("hits"),
                func.avg(GamePlayerStats.toi).label("avg_toi"),
            )
            .where(
                and_(
                    GamePlayerStats.player_id == player_id,
                    GamePlayerStats.game_id.in_(opponent_game_ids),
                )
            )
        )
        result = await db.execute(stats_stmt)
        row = result.one_or_none()

        if not row or (row.games or 0) < 1:
            return None

        games = row.games
        matchup_ppg = (row.points or 0) / games
        matchup_gpg = (row.goals or 0) / games

        # Get overall stats for deviation comparison
        overall_stmt = (
            select(
                func.count().label("games"),
                func.sum(GamePlayerStats.points).label("points"),
                func.sum(GamePlayerStats.goals).label("goals"),
            )
            .where(GamePlayerStats.player_id == player_id)
        )
        overall_result = await db.execute(overall_stmt)
        overall = overall_result.one_or_none()

        overall_ppg = 0.0
        overall_gpg = 0.0
        if overall and (overall.games or 0) > 0:
            overall_ppg = (overall.points or 0) / overall.games
            overall_gpg = (overall.goals or 0) / overall.games

        # Calculate deviations
        ppg_dev = ((matchup_ppg - overall_ppg) / overall_ppg) if overall_ppg > 0 else 0.0
        gpg_dev = ((matchup_gpg - overall_gpg) / overall_gpg) if overall_gpg > 0 else 0.0

        season_key = season or settings.get_sport_config().default_season

        # Upsert
        existing_stmt = select(PlayerMatchupStats).where(
            and_(
                PlayerMatchupStats.player_id == player_id,
                PlayerMatchupStats.opponent_team_id == opponent_team_id,
                PlayerMatchupStats.season == season_key,
            )
        )
        existing_result = await db.execute(existing_stmt)
        record = existing_result.scalars().first()

        if record:
            record.games_played = games
            record.goals = row.goals or 0
            record.assists = row.assists or 0
            record.points = row.points or 0
            record.shots = row.shots or 0
            record.plus_minus = row.plus_minus or 0
            record.hits = row.hits or 0
            record.avg_toi = round(row.avg_toi or 0, 1)
            record.ppg = round(matchup_ppg, 3)
            record.gpg = round(matchup_gpg, 3)
            record.overall_ppg = round(overall_ppg, 3)
            record.overall_gpg = round(overall_gpg, 3)
            record.ppg_deviation = round(ppg_dev, 4)
            record.gpg_deviation = round(gpg_dev, 4)
            record.last_computed = datetime.now(timezone.utc)
        else:
            record = PlayerMatchupStats(
                player_id=player_id,
                opponent_team_id=opponent_team_id,
                season=season_key,
                games_played=games,
                goals=row.goals or 0,
                assists=row.assists or 0,
                points=row.points or 0,
                shots=row.shots or 0,
                plus_minus=row.plus_minus or 0,
                hits=row.hits or 0,
                avg_toi=round(row.avg_toi or 0, 1),
                ppg=round(matchup_ppg, 3),
                gpg=round(matchup_gpg, 3),
                overall_ppg=round(overall_ppg, 3),
                overall_gpg=round(overall_gpg, 3),
                ppg_deviation=round(ppg_dev, 4),
                gpg_deviation=round(gpg_dev, 4),
                last_computed=datetime.now(timezone.utc),
            )
            db.add(record)

        return record

    async def get_team_player_matchup_impact(
        self,
        db: AsyncSession,
        team_id: int,
        opponent_team_id: int,
    ) -> Dict[str, Any]:
        """Calculate aggregate matchup boost for a team's key players
        against a specific opponent.

        Looks at the top players on the team and their historical
        performance against this opponent. Returns a matchup boost
        factor that the model uses to adjust xG.

        Returns:
            dict with matchup_boost (float), players_with_data (int),
            top_performers (list of dicts), and games_analyzed (int).
        """
        # Get key players on the team (top scorers from recent games)
        player_stmt = (
            select(Player.id)
            .where(
                and_(
                    Player.team_id == team_id,
                    Player.position != "G",
                    Player.active == True,
                )
            )
        )
        result = await db.execute(player_stmt)
        player_ids = [row[0] for row in result.all()]

        if not player_ids:
            return self._empty_matchup_impact()

        # Get matchup stats for these players
        matchup_stmt = (
            select(PlayerMatchupStats)
            .where(
                and_(
                    PlayerMatchupStats.player_id.in_(player_ids),
                    PlayerMatchupStats.opponent_team_id == opponent_team_id,
                    PlayerMatchupStats.games_played >= _mc.min_player_games_vs_team,
                )
            )
        )
        matchup_result = await db.execute(matchup_stmt)
        matchups = matchup_result.scalars().all()

        if not matchups:
            return self._empty_matchup_impact()

        # Calculate weighted matchup boost
        total_weight = 0.0
        weighted_deviation = 0.0
        top_performers = []

        for m in matchups:
            if m.ppg_deviation is None or m.overall_ppg is None:
                continue

            # Weight by player's overall production (better players matter more)
            weight = max(m.overall_ppg or 0, 0.1)
            total_weight += weight

            # Only apply if deviation exceeds threshold
            dev = m.ppg_deviation
            if abs(dev) >= _mc.deviation_threshold:
                weighted_deviation += dev * weight

            if abs(dev) >= _mc.deviation_threshold:
                top_performers.append({
                    "player_id": m.player_id,
                    "ppg_vs_opponent": m.ppg,
                    "overall_ppg": m.overall_ppg,
                    "deviation": round(dev, 3),
                    "games": m.games_played,
                })

        matchup_boost = 0.0
        if total_weight > 0:
            matchup_boost = weighted_deviation / total_weight

        # Cap the boost to prevent extreme adjustments
        matchup_boost = max(-0.30, min(0.30, matchup_boost))

        return {
            "matchup_boost": round(matchup_boost, 4),
            "players_with_data": len(matchups),
            "top_performers": sorted(
                top_performers, key=lambda x: abs(x["deviation"]), reverse=True
            )[:5],
            "games_analyzed": sum(m.games_played for m in matchups),
        }

    # ------------------------------------------------------------------ #
    #  Team vs Team matchup profile                                       #
    # ------------------------------------------------------------------ #

    async def compute_team_matchup_profile(
        self,
        db: AsyncSession,
        team1_id: int,
        team2_id: int,
        season: Optional[str] = None,
    ) -> Optional[TeamMatchupProfile]:
        """Compute a detailed matchup profile between two teams.

        Analyzes scoring patterns, pace, special teams, and period-level
        trends from their historical games.

        Returns the updated TeamMatchupProfile record.
        """
        # Normalize team order (lower ID = team1)
        if team1_id > team2_id:
            team1_id, team2_id = team2_id, team1_id

        # Fetch H2H games
        games_stmt = (
            select(Game)
            .where(
                and_(
                    Game.status == "final",
                    or_(
                        and_(Game.home_team_id == team1_id, Game.away_team_id == team2_id),
                        and_(Game.home_team_id == team2_id, Game.away_team_id == team1_id),
                    ),
                )
            )
            .order_by(desc(Game.date))
            .limit(30)
        )
        if season:
            games_stmt = games_stmt.where(Game.season == season)

        result = await db.execute(games_stmt)
        games = result.scalars().all()

        if not games:
            return None

        # Compute metrics
        total_goals_list = []
        margins = []
        ot_count = 0
        t1_goals_list = []
        t2_goals_list = []
        t1_p1 = []
        t2_p1 = []
        t1_p3 = []
        t2_p3 = []
        total_shots = []
        total_pim = []

        for game in games:
            if game.home_score is None or game.away_score is None:
                continue

            is_t1_home = game.home_team_id == team1_id
            t1_score = game.home_score if is_t1_home else game.away_score
            t2_score = game.away_score if is_t1_home else game.home_score

            total_goals_list.append(t1_score + t2_score)
            margins.append(t1_score - t2_score)
            t1_goals_list.append(t1_score)
            t2_goals_list.append(t2_score)

            if game.went_to_overtime:
                ot_count += 1

            # Period scores
            if game.home_score_p1 is not None:
                t1_p1_val = game.home_score_p1 if is_t1_home else game.away_score_p1
                t2_p1_val = game.away_score_p1 if is_t1_home else game.home_score_p1
                t1_p1.append(t1_p1_val or 0)
                t2_p1.append(t2_p1_val or 0)

            if game.home_score_p3 is not None:
                t1_p3_val = game.home_score_p3 if is_t1_home else game.away_score_p3
                t2_p3_val = game.away_score_p3 if is_t1_home else game.home_score_p3
                t1_p3.append(t1_p3_val or 0)
                t2_p3.append(t2_p3_val or 0)

            # Shots
            if game.home_shots and game.away_shots:
                total_shots.append(game.home_shots + game.away_shots)

        n = len(total_goals_list)
        if n == 0:
            return None

        avg_total = sum(total_goals_list) / n
        avg_margin = sum(margins) / n

        # Scoring variance
        variance = sum((g - avg_total) ** 2 for g in total_goals_list) / n
        scoring_var = math.sqrt(variance)

        season_key = season or settings.get_sport_config().default_season

        # Upsert
        existing_stmt = select(TeamMatchupProfile).where(
            and_(
                TeamMatchupProfile.team1_id == team1_id,
                TeamMatchupProfile.team2_id == team2_id,
                TeamMatchupProfile.season == season_key,
            )
        )
        existing_result = await db.execute(existing_stmt)
        profile = existing_result.scalars().first()

        data = {
            "games_played": n,
            "avg_total_goals": round(avg_total, 2),
            "scoring_variance": round(scoring_var, 2),
            "avg_margin": round(avg_margin, 2),
            "ot_rate": round(ot_count / n, 3),
            "team1_goals_pg": round(sum(t1_goals_list) / n, 2),
            "team2_goals_pg": round(sum(t2_goals_list) / n, 2),
            "team1_p1_goals_avg": round(sum(t1_p1) / len(t1_p1), 2) if t1_p1 else None,
            "team2_p1_goals_avg": round(sum(t2_p1) / len(t2_p1), 2) if t2_p1 else None,
            "team1_p3_goals_avg": round(sum(t1_p3) / len(t1_p3), 2) if t1_p3 else None,
            "team2_p3_goals_avg": round(sum(t2_p3) / len(t2_p3), 2) if t2_p3 else None,
            "pace_indicator": round(sum(total_shots) / len(total_shots), 1) if total_shots else None,
            "last_computed": datetime.now(timezone.utc),
        }

        if profile:
            for key, val in data.items():
                setattr(profile, key, val)
        else:
            profile = TeamMatchupProfile(
                team1_id=team1_id,
                team2_id=team2_id,
                season=season_key,
                **data,
            )
            db.add(profile)

        return profile

    async def get_team_matchup_features(
        self,
        db: AsyncSession,
        team1_id: int,
        team2_id: int,
    ) -> Dict[str, Any]:
        """Get matchup features for the prediction model.

        Returns a dict suitable for inclusion in the features dict
        under the 'team_matchup' key.
        """
        # Normalize order
        t1 = min(team1_id, team2_id)
        t2 = max(team1_id, team2_id)

        stmt = (
            select(TeamMatchupProfile)
            .where(
                and_(
                    TeamMatchupProfile.team1_id == t1,
                    TeamMatchupProfile.team2_id == t2,
                )
            )
            .order_by(desc(TeamMatchupProfile.season))
            .limit(1)
        )
        result = await db.execute(stmt)
        profile = result.scalars().first()

        if not profile or (profile.games_played or 0) < _mc.min_team_h2h_games:
            return self._empty_team_matchup()

        return {
            "games_found": profile.games_played,
            "avg_total_goals": profile.avg_total_goals,
            "scoring_variance": profile.scoring_variance,
            "avg_margin": profile.avg_margin,
            "ot_rate": profile.ot_rate,
            "team1_goals_pg": profile.team1_goals_pg,
            "team2_goals_pg": profile.team2_goals_pg,
            "pace_indicator": profile.pace_indicator,
            "team1_p1_goals_avg": profile.team1_p1_goals_avg,
            "team2_p1_goals_avg": profile.team2_p1_goals_avg,
        }

    # ------------------------------------------------------------------ #
    #  Batch computation                                                   #
    # ------------------------------------------------------------------ #

    async def refresh_matchup_data(
        self,
        db: AsyncSession,
        team1_id: int,
        team2_id: int,
    ) -> None:
        """Refresh all matchup data for a game between two teams.

        Computes:
        1. Team matchup profile
        2. Player matchup stats for key players on both teams
        """
        # Team matchup profile
        await self.compute_team_matchup_profile(db, team1_id, team2_id)

        # Player matchups for key players
        for team_id, opp_id in [(team1_id, team2_id), (team2_id, team1_id)]:
            player_stmt = (
                select(Player.id)
                .where(
                    and_(
                        Player.team_id == team_id,
                        Player.position != "G",
                        Player.active == True,
                    )
                )
            )
            result = await db.execute(player_stmt)
            player_ids = [row[0] for row in result.all()]

            for pid in player_ids:
                try:
                    await self.compute_player_matchup(db, pid, opp_id)
                except Exception as e:
                    logger.debug("Could not compute matchup for player %d: %s", pid, e)

    # ------------------------------------------------------------------ #
    #  Empty defaults                                                      #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _empty_matchup_impact() -> Dict[str, Any]:
        return {
            "matchup_boost": 0.0,
            "players_with_data": 0,
            "top_performers": [],
            "games_analyzed": 0,
        }

    @staticmethod
    def _empty_team_matchup() -> Dict[str, Any]:
        return {
            "games_found": 0,
            "avg_total_goals": None,
            "scoring_variance": None,
            "avg_margin": None,
            "ot_rate": None,
            "team1_goals_pg": None,
            "team2_goals_pg": None,
            "pace_indicator": None,
            "team1_p1_goals_avg": None,
            "team2_p1_goals_avg": None,
        }
