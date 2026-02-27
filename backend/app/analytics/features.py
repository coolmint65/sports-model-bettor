"""
Feature engineering for sports betting prediction models.

Extracts and computes features from historical game data, team statistics,
goalie performance, period-level scoring, and head-to-head matchup history.
All features are designed for NHL hockey but structured to be sport-adaptable.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, case, desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.game import Game, GameGoalieStats, HeadToHead
from app.models.player import GoalieStats, Player
from app.models.team import Team, TeamStats

logger = logging.getLogger(__name__)


class FeatureEngine:
    """
    Extracts and engineers features from historical game data for prediction models.

    All methods are async and accept a SQLAlchemy AsyncSession. Features are
    returned as flat dictionaries suitable for direct consumption by the
    BettingModel class.
    """

    # ------------------------------------------------------------------ #
    #  Team recent form                                                   #
    # ------------------------------------------------------------------ #

    async def get_team_form(
        self,
        db: AsyncSession,
        team_id: int,
        last_n: int = 5,
    ) -> Dict[str, Any]:
        """
        Calculate recent form metrics for a team over their last N completed games.

        Returns:
            dict with keys: win_rate, avg_goals_for, avg_goals_against,
            avg_shots, avg_total_goals, games_found.
        """
        games = await self._get_recent_games(db, team_id, last_n)

        if not games:
            return self._empty_form()

        wins = 0
        goals_for_total = 0
        goals_against_total = 0
        total_goals_sum = 0
        games_counted = len(games)

        for game in games:
            is_home = game.home_team_id == team_id
            gf = game.home_score if is_home else game.away_score
            ga = game.away_score if is_home else game.home_score

            if gf is None or ga is None:
                games_counted -= 1
                continue

            goals_for_total += gf
            goals_against_total += ga
            total_goals_sum += gf + ga

            # Determine winner
            if gf > ga:
                wins += 1

        if games_counted == 0:
            return self._empty_form()

        return {
            "win_rate": round(wins / games_counted, 4),
            "avg_goals_for": round(goals_for_total / games_counted, 3),
            "avg_goals_against": round(goals_against_total / games_counted, 3),
            "avg_total_goals": round(total_goals_sum / games_counted, 3),
            "games_found": games_counted,
        }

    # ------------------------------------------------------------------ #
    #  Home / away splits                                                 #
    # ------------------------------------------------------------------ #

    async def get_team_home_away_splits(
        self,
        db: AsyncSession,
        team_id: int,
        is_home: bool,
        last_n: int = 20,
    ) -> Dict[str, Any]:
        """
        Calculate home-only or away-only performance for a team.

        Args:
            is_home: True for home splits, False for away splits.

        Returns:
            dict with keys: win_rate, avg_goals_for, avg_goals_against,
            avg_total_goals, games_found.
        """
        if is_home:
            filter_clause = Game.home_team_id == team_id
        else:
            filter_clause = Game.away_team_id == team_id

        stmt = (
            select(Game)
            .where(
                and_(
                    filter_clause,
                    Game.status == "final",
                )
            )
            .order_by(desc(Game.date))
            .limit(last_n)
        )
        result = await db.execute(stmt)
        games = result.scalars().all()

        if not games:
            return self._empty_form()

        wins = 0
        goals_for_total = 0
        goals_against_total = 0
        total_goals_sum = 0
        games_counted = len(games)

        for game in games:
            gf = game.home_score if is_home else game.away_score
            ga = game.away_score if is_home else game.home_score

            if gf is None or ga is None:
                games_counted -= 1
                continue

            goals_for_total += gf
            goals_against_total += ga
            total_goals_sum += gf + ga

            if gf > ga:
                wins += 1

        if games_counted == 0:
            return self._empty_form()

        return {
            "win_rate": round(wins / games_counted, 4),
            "avg_goals_for": round(goals_for_total / games_counted, 3),
            "avg_goals_against": round(goals_against_total / games_counted, 3),
            "avg_total_goals": round(total_goals_sum / games_counted, 3),
            "games_found": games_counted,
        }

    # ------------------------------------------------------------------ #
    #  Head-to-head stats                                                 #
    # ------------------------------------------------------------------ #

    async def get_h2h_stats(
        self,
        db: AsyncSession,
        team1_id: int,
        team2_id: int,
        last_n: int = 10,
    ) -> Dict[str, Any]:
        """
        Calculate head-to-head record between two teams from recent games.

        Uses actual game results rather than the HeadToHead aggregate table
        so that recency weighting applies correctly.

        Returns:
            dict with keys: team1_win_rate, team2_win_rate, avg_total_goals,
            team1_avg_goals, team2_avg_goals, games_found.
        """
        stmt = (
            select(Game)
            .where(
                and_(
                    Game.status == "final",
                    or_(
                        and_(
                            Game.home_team_id == team1_id,
                            Game.away_team_id == team2_id,
                        ),
                        and_(
                            Game.home_team_id == team2_id,
                            Game.away_team_id == team1_id,
                        ),
                    ),
                )
            )
            .order_by(desc(Game.date))
            .limit(last_n)
        )
        result = await db.execute(stmt)
        games = result.scalars().all()

        if not games:
            return {
                "team1_win_rate": 0.5,
                "team2_win_rate": 0.5,
                "avg_total_goals": 5.5,
                "team1_avg_goals": 2.75,
                "team2_avg_goals": 2.75,
                "games_found": 0,
            }

        team1_wins = 0
        team1_goals_total = 0
        team2_goals_total = 0
        games_counted = 0

        for game in games:
            if game.home_score is None or game.away_score is None:
                continue
            games_counted += 1

            if game.home_team_id == team1_id:
                t1_goals = game.home_score
                t2_goals = game.away_score
            else:
                t1_goals = game.away_score
                t2_goals = game.home_score

            team1_goals_total += t1_goals
            team2_goals_total += t2_goals

            if t1_goals > t2_goals:
                team1_wins += 1

        if games_counted == 0:
            return {
                "team1_win_rate": 0.5,
                "team2_win_rate": 0.5,
                "avg_total_goals": 5.5,
                "team1_avg_goals": 2.75,
                "team2_avg_goals": 2.75,
                "games_found": 0,
            }

        team1_wr = round(team1_wins / games_counted, 4)
        return {
            "team1_win_rate": team1_wr,
            "team2_win_rate": round(1.0 - team1_wr, 4),
            "avg_total_goals": round(
                (team1_goals_total + team2_goals_total) / games_counted, 3
            ),
            "team1_avg_goals": round(team1_goals_total / games_counted, 3),
            "team2_avg_goals": round(team2_goals_total / games_counted, 3),
            "games_found": games_counted,
        }

    # ------------------------------------------------------------------ #
    #  Goalie features                                                    #
    # ------------------------------------------------------------------ #

    async def get_goalie_features(
        self,
        db: AsyncSession,
        team_id: int,
    ) -> Dict[str, Any]:
        """
        Get starting goalie statistics for a team.

        Identifies the likely starter based on most recent starts, then
        pulls season stats and recent game-level stats (last 5 and 10 games).

        Returns:
            dict with keys: goalie_name, season_save_pct, season_gaa,
            last5_save_pct, last5_gaa, last10_save_pct, last10_gaa,
            games_started_season.
        """
        # Find the most likely starter: goalie with the most recent start
        recent_start_stmt = (
            select(GameGoalieStats)
            .join(Game, GameGoalieStats.game_id == Game.id)
            .where(
                and_(
                    GameGoalieStats.team_id == team_id,
                    GameGoalieStats.starter == True,  # noqa: E712
                    Game.status == "final",
                )
            )
            .order_by(desc(Game.date))
            .limit(1)
        )
        result = await db.execute(recent_start_stmt)
        recent_start = result.scalars().first()

        if not recent_start:
            return self._empty_goalie_features()

        goalie_id = recent_start.player_id

        # Get goalie name
        player_stmt = select(Player).where(Player.id == goalie_id)
        player_result = await db.execute(player_stmt)
        goalie = player_result.scalars().first()
        goalie_name = goalie.name if goalie else "Unknown"

        # Get season stats
        season_stmt = (
            select(GoalieStats)
            .where(GoalieStats.player_id == goalie_id)
            .order_by(desc(GoalieStats.season))
            .limit(1)
        )
        season_result = await db.execute(season_stmt)
        season_stats = season_result.scalars().first()

        season_save_pct = season_stats.save_pct if season_stats and season_stats.save_pct else 0.900
        season_gaa = season_stats.gaa if season_stats and season_stats.gaa else 3.00
        games_started = season_stats.games_started if season_stats else 0

        # Get recent game-level goalie stats (last 10 starts)
        recent_games_stmt = (
            select(GameGoalieStats)
            .join(Game, GameGoalieStats.game_id == Game.id)
            .where(
                and_(
                    GameGoalieStats.player_id == goalie_id,
                    GameGoalieStats.starter == True,  # noqa: E712
                    Game.status == "final",
                )
            )
            .order_by(desc(Game.date))
            .limit(10)
        )
        recent_result = await db.execute(recent_games_stmt)
        recent_games = recent_result.scalars().all()

        last5_save_pct, last5_gaa = self._calc_goalie_recent(recent_games[:5])
        last10_save_pct, last10_gaa = self._calc_goalie_recent(recent_games[:10])

        return {
            "goalie_name": goalie_name,
            "goalie_id": goalie_id,
            "season_save_pct": round(season_save_pct, 4),
            "season_gaa": round(season_gaa, 3),
            "last5_save_pct": round(last5_save_pct, 4),
            "last5_gaa": round(last5_gaa, 3),
            "last10_save_pct": round(last10_save_pct, 4),
            "last10_gaa": round(last10_gaa, 3),
            "games_started_season": games_started,
        }

    # ------------------------------------------------------------------ #
    #  Period-level statistics                                            #
    # ------------------------------------------------------------------ #

    async def get_period_stats(
        self,
        db: AsyncSession,
        team_id: int,
        last_n: int = 20,
    ) -> Dict[str, Any]:
        """
        Compute period-by-period scoring averages for a team.

        Parses the JSON period_scores column from recent completed games.
        Expected format: {"home": [1, 2, 0], "away": [0, 1, 1]}

        Returns:
            dict with keys: avg_p1_for, avg_p2_for, avg_p3_for,
            avg_p1_against, avg_p2_against, avg_p3_against,
            first_period_scoring_rate, games_found.
        """
        games = await self._get_recent_games(db, team_id, last_n)

        p_for = [0.0, 0.0, 0.0]
        p_against = [0.0, 0.0, 0.0]
        first_period_scored = 0
        games_with_periods = 0

        for game in games:
            period_data = self._parse_period_scores(game.period_scores)
            if period_data is None:
                continue

            is_home = game.home_team_id == team_id
            team_periods = period_data.get("home" if is_home else "away", [])
            opp_periods = period_data.get("away" if is_home else "home", [])

            # Ensure we have at least 3 periods of data
            if len(team_periods) < 3 or len(opp_periods) < 3:
                continue

            games_with_periods += 1

            for i in range(3):
                p_for[i] += team_periods[i]
                p_against[i] += opp_periods[i]

            if team_periods[0] > 0:
                first_period_scored += 1

        if games_with_periods == 0:
            return {
                "avg_p1_for": 0.0,
                "avg_p2_for": 0.0,
                "avg_p3_for": 0.0,
                "avg_p1_against": 0.0,
                "avg_p2_against": 0.0,
                "avg_p3_against": 0.0,
                "first_period_scoring_rate": 0.0,
                "games_found": 0,
            }

        n = games_with_periods
        return {
            "avg_p1_for": round(p_for[0] / n, 3),
            "avg_p2_for": round(p_for[1] / n, 3),
            "avg_p3_for": round(p_for[2] / n, 3),
            "avg_p1_against": round(p_against[0] / n, 3),
            "avg_p2_against": round(p_against[1] / n, 3),
            "avg_p3_against": round(p_against[2] / n, 3),
            "first_period_scoring_rate": round(first_period_scored / n, 4),
            "games_found": games_with_periods,
        }

    # ------------------------------------------------------------------ #
    #  Overtime tendency                                                  #
    # ------------------------------------------------------------------ #

    async def get_overtime_tendency(
        self,
        db: AsyncSession,
        team_id: int,
        last_n: int = 30,
    ) -> Dict[str, Any]:
        """
        Calculate how often a team's games go to overtime and their OT win rate.

        Returns:
            dict with keys: ot_pct, ot_win_rate, games_found.
        """
        games = await self._get_recent_games(db, team_id, last_n)

        if not games:
            return {"ot_pct": 0.0, "ot_win_rate": 0.5, "games_found": 0}

        total = len(games)
        ot_games = 0
        ot_wins = 0

        for game in games:
            if game.overtime or game.shootout:
                ot_games += 1
                is_home = game.home_team_id == team_id
                gf = game.home_score if is_home else game.away_score
                ga = game.away_score if is_home else game.home_score
                if gf is not None and ga is not None and gf > ga:
                    ot_wins += 1

        ot_pct = round(ot_games / total, 4) if total > 0 else 0.0
        ot_win_rate = round(ot_wins / ot_games, 4) if ot_games > 0 else 0.5

        return {
            "ot_pct": ot_pct,
            "ot_win_rate": ot_win_rate,
            "games_found": total,
        }

    # ------------------------------------------------------------------ #
    #  Scoring patterns                                                   #
    # ------------------------------------------------------------------ #

    async def get_scoring_patterns(
        self,
        db: AsyncSession,
        team_id: int,
        last_n: int = 20,
    ) -> Dict[str, Any]:
        """
        Analyze scoring patterns for a team across recent games.

        Returns:
            dict with keys: first_goal_pct, btts_pct (both teams to score),
            over_5_5_pct, under_5_5_pct, odd_total_pct, even_total_pct,
            games_found.
        """
        games = await self._get_recent_games(db, team_id, last_n)

        if not games:
            return {
                "first_goal_pct": 0.5,
                "btts_pct": 0.85,
                "over_5_5_pct": 0.5,
                "under_5_5_pct": 0.5,
                "odd_total_pct": 0.5,
                "even_total_pct": 0.5,
                "games_found": 0,
            }

        first_goal_count = 0
        btts_count = 0
        over_55_count = 0
        odd_total_count = 0
        games_counted = 0

        for game in games:
            if game.home_score is None or game.away_score is None:
                continue
            games_counted += 1

            is_home = game.home_team_id == team_id
            gf = game.home_score if is_home else game.away_score
            ga = game.away_score if is_home else game.home_score
            total = gf + ga

            # Both teams scored
            if gf > 0 and ga > 0:
                btts_count += 1

            # Over 5.5
            if total > 5:
                over_55_count += 1

            # Odd total
            if total % 2 == 1:
                odd_total_count += 1

            # First goal (check period_scores for first goal info)
            period_data = self._parse_period_scores(game.period_scores)
            if period_data:
                team_key = "home" if is_home else "away"
                opp_key = "away" if is_home else "home"
                team_p = period_data.get(team_key, [])
                opp_p = period_data.get(opp_key, [])
                if team_p and opp_p:
                    # Check who scored first by period
                    for pi in range(min(len(team_p), len(opp_p))):
                        if team_p[pi] > 0 and opp_p[pi] == 0:
                            first_goal_count += 1
                            break
                        elif opp_p[pi] > 0 and team_p[pi] == 0:
                            break
                        elif team_p[pi] > 0 and opp_p[pi] > 0:
                            # Both scored in same period - 50/50 approximation
                            first_goal_count += 0.5
                            break

        if games_counted == 0:
            return {
                "first_goal_pct": 0.5,
                "btts_pct": 0.85,
                "over_5_5_pct": 0.5,
                "under_5_5_pct": 0.5,
                "odd_total_pct": 0.5,
                "even_total_pct": 0.5,
                "games_found": 0,
            }

        n = games_counted
        over_55_pct = round(over_55_count / n, 4)
        odd_pct = round(odd_total_count / n, 4)
        return {
            "first_goal_pct": round(first_goal_count / n, 4),
            "btts_pct": round(btts_count / n, 4),
            "over_5_5_pct": over_55_pct,
            "under_5_5_pct": round(1.0 - over_55_pct, 4),
            "odd_total_pct": odd_pct,
            "even_total_pct": round(1.0 - odd_pct, 4),
            "games_found": games_counted,
        }

    # ------------------------------------------------------------------ #
    #  Season-level team stats (from TeamStats table)                     #
    # ------------------------------------------------------------------ #

    async def get_season_stats(
        self,
        db: AsyncSession,
        team_id: int,
    ) -> Dict[str, Any]:
        """
        Retrieve the latest season-level aggregate stats for a team.

        Returns:
            dict with keys: goals_for_pg, goals_against_pg, pp_pct, pk_pct,
            shots_for_pg, shots_against_pg, faceoff_pct, win_pct.
        """
        stmt = (
            select(TeamStats)
            .where(TeamStats.team_id == team_id)
            .order_by(desc(TeamStats.season))
            .limit(1)
        )
        result = await db.execute(stmt)
        stats = result.scalars().first()

        if not stats or stats.games_played == 0:
            return {
                "goals_for_pg": 3.0,
                "goals_against_pg": 3.0,
                "pp_pct": 20.0,
                "pk_pct": 80.0,
                "shots_for_pg": 30.0,
                "shots_against_pg": 30.0,
                "faceoff_pct": 50.0,
                "win_pct": 0.5,
            }

        gp = stats.games_played
        win_pct = round(stats.wins / gp, 4) if gp > 0 else 0.5

        return {
            "goals_for_pg": stats.goals_for_per_game or round(stats.goals_for / gp, 3),
            "goals_against_pg": stats.goals_against_per_game or round(stats.goals_against / gp, 3),
            "pp_pct": stats.power_play_pct or 20.0,
            "pk_pct": stats.penalty_kill_pct or 80.0,
            "shots_for_pg": stats.shots_for_per_game or 30.0,
            "shots_against_pg": stats.shots_against_per_game or 30.0,
            "faceoff_pct": stats.faceoff_win_pct or 50.0,
            "win_pct": win_pct,
        }

    # ------------------------------------------------------------------ #
    #  Build comprehensive feature set for a game                         #
    # ------------------------------------------------------------------ #

    async def build_game_features(
        self,
        db: AsyncSession,
        game_id: int,
    ) -> Dict[str, Any]:
        """
        Build a comprehensive feature dictionary for a specific game.

        Combines all feature extraction methods for both the home and away
        teams. The returned dictionary is structured with prefixed keys
        (home_*, away_*, h2h_*) so it can be directly consumed by the
        BettingModel.

        Returns:
            dict with nested feature groups for home team, away team,
            head-to-head, and game metadata.
        """
        # Fetch the game record
        stmt = select(Game).where(Game.id == game_id)
        result = await db.execute(stmt)
        game = result.scalars().first()

        if not game:
            raise ValueError(f"Game with id={game_id} not found")

        home_id = game.home_team_id
        away_id = game.away_team_id

        # Fetch team names
        home_team = await self._get_team(db, home_id)
        away_team = await self._get_team(db, away_id)

        # Build all features concurrently-style (sequential in async)
        # Home team features
        home_form_5 = await self.get_team_form(db, home_id, last_n=5)
        home_form_10 = await self.get_team_form(db, home_id, last_n=10)
        home_season = await self.get_season_stats(db, home_id)
        home_splits = await self.get_team_home_away_splits(db, home_id, is_home=True)
        home_goalie = await self.get_goalie_features(db, home_id)
        home_periods = await self.get_period_stats(db, home_id)
        home_ot = await self.get_overtime_tendency(db, home_id)
        home_patterns = await self.get_scoring_patterns(db, home_id)

        # Away team features
        away_form_5 = await self.get_team_form(db, away_id, last_n=5)
        away_form_10 = await self.get_team_form(db, away_id, last_n=10)
        away_season = await self.get_season_stats(db, away_id)
        away_splits = await self.get_team_home_away_splits(db, away_id, is_home=False)
        away_goalie = await self.get_goalie_features(db, away_id)
        away_periods = await self.get_period_stats(db, away_id)
        away_ot = await self.get_overtime_tendency(db, away_id)
        away_patterns = await self.get_scoring_patterns(db, away_id)

        # Head-to-head
        h2h = await self.get_h2h_stats(db, home_id, away_id)

        features = {
            # Game metadata
            "game_id": game.id,
            "game_date": str(game.date),
            "home_team_id": home_id,
            "away_team_id": away_id,
            "home_team_name": home_team.name if home_team else "Unknown",
            "away_team_name": away_team.name if away_team else "Unknown",
            "home_team_abbr": home_team.abbreviation if home_team else "UNK",
            "away_team_abbr": away_team.abbreviation if away_team else "UNK",
            # Home team features
            "home_form_5": home_form_5,
            "home_form_10": home_form_10,
            "home_season": home_season,
            "home_splits": home_splits,
            "home_goalie": home_goalie,
            "home_periods": home_periods,
            "home_ot": home_ot,
            "home_patterns": home_patterns,
            # Away team features
            "away_form_5": away_form_5,
            "away_form_10": away_form_10,
            "away_season": away_season,
            "away_splits": away_splits,
            "away_goalie": away_goalie,
            "away_periods": away_periods,
            "away_ot": away_ot,
            "away_patterns": away_patterns,
            # Head-to-head
            "h2h": h2h,
        }

        return features

    # ------------------------------------------------------------------ #
    #  Private helpers                                                    #
    # ------------------------------------------------------------------ #

    async def _get_recent_games(
        self,
        db: AsyncSession,
        team_id: int,
        last_n: int,
    ) -> List[Game]:
        """Fetch the last N completed games involving a team."""
        stmt = (
            select(Game)
            .where(
                and_(
                    or_(
                        Game.home_team_id == team_id,
                        Game.away_team_id == team_id,
                    ),
                    Game.status == "final",
                )
            )
            .order_by(desc(Game.date))
            .limit(last_n)
        )
        result = await db.execute(stmt)
        return list(result.scalars().all())

    async def _get_team(
        self,
        db: AsyncSession,
        team_id: int,
    ) -> Optional[Team]:
        """Fetch a team by ID."""
        stmt = select(Team).where(Team.id == team_id)
        result = await db.execute(stmt)
        return result.scalars().first()

    @staticmethod
    def _parse_period_scores(
        period_scores_raw: Optional[str],
    ) -> Optional[Dict[str, List[int]]]:
        """
        Parse the JSON period_scores string from a Game record.

        Expected format: '{"home": [1, 2, 0], "away": [0, 1, 1]}'
        Returns None if parsing fails or data is missing.
        """
        if not period_scores_raw:
            return None
        try:
            data = json.loads(period_scores_raw)
            if isinstance(data, dict) and "home" in data and "away" in data:
                return data
        except (json.JSONDecodeError, TypeError):
            pass
        return None

    @staticmethod
    def _calc_goalie_recent(
        game_stats: List[GameGoalieStats],
    ) -> tuple:
        """
        Calculate save percentage and GAA from a list of game goalie stats.

        Returns:
            (save_pct, gaa) tuple with defaults if no data.
        """
        if not game_stats:
            return 0.900, 3.00

        total_saves = 0
        total_shots_against = 0
        total_goals_against = 0
        total_games = 0

        for gs in game_stats:
            total_saves += gs.saves
            total_shots_against += gs.shots_against
            total_goals_against += gs.goals_against
            total_games += 1

        if total_shots_against > 0:
            save_pct = total_saves / total_shots_against
        else:
            save_pct = 0.900

        # GAA approximation: goals_against per game
        gaa = total_goals_against / total_games if total_games > 0 else 3.00

        return save_pct, gaa

    @staticmethod
    def _empty_form() -> Dict[str, Any]:
        """Return a default empty form dictionary."""
        return {
            "win_rate": 0.5,
            "avg_goals_for": 3.0,
            "avg_goals_against": 3.0,
            "avg_total_goals": 6.0,
            "games_found": 0,
        }

    @staticmethod
    def _empty_goalie_features() -> Dict[str, Any]:
        """Return default goalie features when no data is available."""
        return {
            "goalie_name": "Unknown",
            "goalie_id": None,
            "season_save_pct": 0.900,
            "season_gaa": 3.00,
            "last5_save_pct": 0.900,
            "last5_gaa": 3.00,
            "last10_save_pct": 0.900,
            "last10_gaa": 3.00,
            "games_started_season": 0,
        }
