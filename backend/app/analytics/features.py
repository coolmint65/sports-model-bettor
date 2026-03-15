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


from app.config import settings
from app.models.game import Game, GameGoalieStats, GamePlayerStats, HeadToHead
from app.models.injury import InjuryReport
from app.models.odds_history import OddsSnapshot
from app.models.player import GoalieStats, Player, PlayerStats
from app.models.team import Team, TeamStats

logger = logging.getLogger(__name__)

_mc = settings.model
_ic = settings.injury


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
        shots_for_total = 0
        shots_against_total = 0
        games_counted = len(games)

        # Momentum-weighted scoring: exponential decay gives more recent
        # games higher influence. Games are ordered most-recent-first.
        decay = _mc.momentum_decay
        weighted_gf = 0.0
        weight_sum = 0.0

        idx = 0
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

            # Accumulate shots for PDO calculation
            sf = (game.home_shots if is_home else game.away_shots) or 0
            sa = (game.away_shots if is_home else game.home_shots) or 0
            shots_for_total += sf
            shots_against_total += sa

            # Momentum weighting: w = decay^idx (most recent = 1.0)
            w = decay ** idx
            weighted_gf += gf * w
            weight_sum += w
            idx += 1

            if gf > ga:
                wins += 1

        if games_counted == 0:
            return self._empty_form()

        # PDO = shooting% + save% (league average = 1.000)
        shooting_pct = goals_for_total / shots_for_total if shots_for_total > 0 else 0.09
        save_pct = 1.0 - (goals_against_total / shots_against_total) if shots_against_total > 0 else 0.91
        pdo = shooting_pct + save_pct

        # Momentum-weighted avg: captures scoring direction/trend
        momentum_avg_gf = weighted_gf / weight_sum if weight_sum > 0 else goals_for_total / games_counted

        return {
            "win_rate": round(wins / games_counted, 4),
            "avg_goals_for": round(goals_for_total / games_counted, 3),
            "avg_goals_against": round(goals_against_total / games_counted, 3),
            "avg_total_goals": round(total_goals_sum / games_counted, 3),
            "games_found": games_counted,
            "pdo": round(pdo, 4),
            "shooting_pct": round(shooting_pct, 4),
            "save_pct": round(save_pct, 4),
            "momentum_avg_gf": round(momentum_avg_gf, 3),
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
        last_n: int = 20,
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
        # Find the most likely starter: goalie with the most recent game
        # GameGoalieStats doesn't have team_id/starter columns, so we
        # join through Game to filter by team and use decision to find starters.
        recent_start_stmt = (
            select(GameGoalieStats)
            .join(Game, GameGoalieStats.game_id == Game.id)
            .join(Player, GameGoalieStats.player_id == Player.id)
            .where(
                and_(
                    Player.team_id == team_id,
                    Game.status == "final",
                    GameGoalieStats.decision.isnot(None),
                )
            )
            .order_by(desc(Game.date))
            .limit(1)
        )
        result = await db.execute(recent_start_stmt)
        recent_start = result.scalars().first()

        if not recent_start:
            logger.info("Goalie: no recent starter found for team_id=%d", team_id)
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

        # Get recent game-level goalie stats (last 10 games with a decision)
        recent_games_stmt = (
            select(GameGoalieStats)
            .join(Game, GameGoalieStats.game_id == Game.id)
            .where(
                and_(
                    GameGoalieStats.player_id == goalie_id,
                    GameGoalieStats.decision.isnot(None),
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

        # Count consecutive starts for workload/fatigue detection.
        # recent_games is ordered by date desc, so count how many
        # consecutive games this goalie started (all have decisions).
        consecutive_starts = len(recent_games)  # all fetched have decisions = starts

        # Also check if team had a game where a *different* goalie started
        # (i.e., the backup played). Query the team's recent games to see
        # if this goalie started all of them.
        team_recent = await self._get_recent_games(db, team_id, 10)
        consecutive_starts = 0
        for tg in team_recent:
            # Check if this goalie got the decision in this game
            gs_stmt = (
                select(GameGoalieStats)
                .where(
                    and_(
                        GameGoalieStats.game_id == tg.id,
                        GameGoalieStats.player_id == goalie_id,
                        GameGoalieStats.decision.isnot(None),
                    )
                )
                .limit(1)
            )
            gs_result = await db.execute(gs_stmt)
            if gs_result.scalars().first():
                consecutive_starts += 1
            else:
                break  # different goalie started — streak over

        logger.info(
            "Goalie: team_id=%d → %s (id=%d) | SV%% %.3f GAA %.2f | "
            "L5 SV%% %.3f GAA %.2f | L10 SV%% %.3f GAA %.2f | %d GS | %d consec",
            team_id, goalie_name, goalie_id,
            season_save_pct, season_gaa,
            last5_save_pct, last5_gaa,
            last10_save_pct, last10_gaa,
            games_started, consecutive_starts,
        )

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
            "consecutive_starts": consecutive_starts,
        }

    # ------------------------------------------------------------------ #
    #  Goalie vs. specific opponent                                       #
    # ------------------------------------------------------------------ #

    async def get_goalie_vs_team(
        self,
        db: AsyncSession,
        goalie_id: Optional[int],
        goalie_team_id: int,
        opponent_team_id: int,
    ) -> Dict[str, Any]:
        """
        Calculate a goalie's historical performance against a specific opponent.

        Joins GameGoalieStats → Game to find games where this goalie played
        (had a decision) and the opponent was the other team. Returns save%,
        GAA, record, and the number of games found.

        Returns:
            dict with keys: vs_save_pct, vs_gaa, vs_record (W-L-OTL),
            vs_games, vs_goals_against_avg, significant (bool).
        """
        empty = self._empty_goalie_vs_team()
        if goalie_id is None:
            return empty

        # Find games where this goalie got a decision against the opponent
        stmt = (
            select(GameGoalieStats, Game)
            .join(Game, GameGoalieStats.game_id == Game.id)
            .where(
                and_(
                    GameGoalieStats.player_id == goalie_id,
                    GameGoalieStats.decision.isnot(None),
                    Game.status == "final",
                    or_(
                        and_(
                            Game.home_team_id == goalie_team_id,
                            Game.away_team_id == opponent_team_id,
                        ),
                        and_(
                            Game.home_team_id == opponent_team_id,
                            Game.away_team_id == goalie_team_id,
                        ),
                    ),
                )
            )
            .order_by(desc(Game.date))
            .limit(10)
        )
        result = await db.execute(stmt)
        rows = result.all()

        if not rows:
            return empty

        total_saves = 0
        total_shots = 0
        total_goals_against = 0
        wins = 0
        losses = 0
        ot_losses = 0

        for ggs, game in rows:
            total_saves += ggs.saves or 0
            total_shots += ggs.shots_against or 0
            total_goals_against += ggs.goals_against or 0
            if ggs.decision == "W":
                wins += 1
            elif ggs.decision == "L":
                losses += 1
            elif ggs.decision == "OTL":
                ot_losses += 1

        games_count = len(rows)
        vs_save_pct = total_saves / total_shots if total_shots > 0 else 0.900
        vs_gaa = total_goals_against / games_count if games_count > 0 else 3.00

        result_dict = {
            "vs_save_pct": round(vs_save_pct, 4),
            "vs_gaa": round(vs_gaa, 3),
            "vs_record": f"{wins}-{losses}-{ot_losses}",
            "vs_wins": wins,
            "vs_losses": losses,
            "vs_ot_losses": ot_losses,
            "vs_games": games_count,
            "significant": games_count >= _mc.goalie_vs_team_min_games,
        }

        logger.info(
            "Goalie vs team: goalie_id=%d vs team_id=%d | %d GP | "
            "SV%% %.3f GAA %.2f | %s",
            goalie_id, opponent_team_id, games_count,
            vs_save_pct, vs_gaa, result_dict["vs_record"],
        )

        return result_dict

    @staticmethod
    def _empty_goalie_vs_team() -> Dict[str, Any]:
        """Return default goalie vs team features when no data is available."""
        return {
            "vs_save_pct": 0.900,
            "vs_gaa": 3.00,
            "vs_record": "0-0-0",
            "vs_wins": 0,
            "vs_losses": 0,
            "vs_ot_losses": 0,
            "vs_games": 0,
            "significant": False,
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
            # Use per-period score columns instead of JSON period_scores
            if game.home_score_p1 is None:
                continue

            is_home = game.home_team_id == team_id
            team_periods = [
                (game.home_score_p1 or 0) if is_home else (game.away_score_p1 or 0),
                (game.home_score_p2 or 0) if is_home else (game.away_score_p2 or 0),
                (game.home_score_p3 or 0) if is_home else (game.away_score_p3 or 0),
            ]
            opp_periods = [
                (game.away_score_p1 or 0) if is_home else (game.home_score_p1 or 0),
                (game.away_score_p2 or 0) if is_home else (game.home_score_p2 or 0),
                (game.away_score_p3 or 0) if is_home else (game.home_score_p3 or 0),
            ]

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
            if game.went_to_overtime:
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
    #  Skater talent / offensive depth                                   #
    # ------------------------------------------------------------------ #

    async def get_skater_impact(
        self,
        db: AsyncSession,
        team_id: int,
        n_games: int = 10,
    ) -> Dict[str, Any]:
        """
        Measure offensive talent depth for a team from recent game boxscores.

        Queries per-game player stats (GamePlayerStats) to calculate:
        - Top-6 forward production (points/game)
        - Top-4 defenseman production (points/game)
        - Star player contribution (top scorer points/game)
        - Team total points/game from skaters

        Returns:
            dict with keys: top6_fwd_ppg, top4_def_ppg, star_ppg,
            team_skater_ppg, games_found.
        """
        # Get recent completed games for this team
        recent_games = await self._get_recent_games(db, team_id, n_games)
        if not recent_games:
            return self._empty_skater_impact()

        game_ids = [g.id for g in recent_games]
        n_actual = len(game_ids)

        # Get all skater stats from these games for players on this team
        stmt = (
            select(
                GamePlayerStats.player_id,
                Player.position,
                func.sum(GamePlayerStats.goals).label("total_goals"),
                func.sum(GamePlayerStats.assists).label("total_assists"),
                func.sum(GamePlayerStats.points).label("total_points"),
                func.sum(GamePlayerStats.shots).label("total_shots"),
                func.count().label("games"),
            )
            .join(Player, GamePlayerStats.player_id == Player.id)
            .where(
                and_(
                    GamePlayerStats.game_id.in_(game_ids),
                    Player.team_id == team_id,
                    Player.position != "G",
                )
            )
            .group_by(GamePlayerStats.player_id, Player.position)
        )
        result = await db.execute(stmt)
        rows = result.all()

        if not rows:
            return self._empty_skater_impact()

        forwards = []
        defensemen = []

        for row in rows:
            ppg = row.total_points / row.games if row.games > 0 else 0
            gpg = row.total_goals / row.games if row.games > 0 else 0
            entry = {
                "player_id": row.player_id,
                "ppg": ppg,
                "gpg": gpg,
                "games": row.games,
                "total_points": row.total_points,
                "total_goals": row.total_goals,
            }
            if row.position in ("D",):
                defensemen.append(entry)
            else:
                forwards.append(entry)

        # Sort by points per game
        forwards.sort(key=lambda x: x["ppg"], reverse=True)
        defensemen.sort(key=lambda x: x["ppg"], reverse=True)

        top6_fwd = forwards[:6]
        top4_def = defensemen[:4]
        all_skaters = forwards + defensemen

        top6_fwd_ppg = (
            sum(p["ppg"] for p in top6_fwd) / len(top6_fwd)
            if top6_fwd else 0.0
        )
        top4_def_ppg = (
            sum(p["ppg"] for p in top4_def) / len(top4_def)
            if top4_def else 0.0
        )
        star_ppg = all_skaters[0]["ppg"] if all_skaters else 0.0
        team_skater_ppg = (
            sum(p["ppg"] for p in all_skaters) / len(all_skaters)
            if all_skaters else 0.0
        )

        return {
            "top6_fwd_ppg": round(top6_fwd_ppg, 3),
            "top4_def_ppg": round(top4_def_ppg, 3),
            "star_ppg": round(star_ppg, 3),
            "team_skater_ppg": round(team_skater_ppg, 3),
            "games_found": n_actual,
        }

    # ------------------------------------------------------------------ #
    #  Lineup availability / missing player impact                       #
    # ------------------------------------------------------------------ #

    async def get_lineup_status(
        self,
        db: AsyncSession,
        team_id: int,
        window: int = 20,
        recent: int = 3,
    ) -> Dict[str, Any]:
        """
        Detect missing regular players by comparing recent games to
        the broader window.

        A "regular" is any skater who appeared in >= 70% of the last
        *window* games. A player is considered "missing" if they did
        not appear in any of the last *recent* games.

        Returns:
            dict with keys: regulars_count, missing_count,
            missing_points_per_game, missing_goals_per_game,
            lineup_strength (1.0 = full, lower = depleted).
        """
        all_games = await self._get_recent_games(db, team_id, window)
        if len(all_games) < 5:
            return self._empty_lineup_status()

        all_game_ids = [g.id for g in all_games]
        recent_game_ids = [g.id for g in all_games[:recent]]
        n_total = len(all_game_ids)

        # Count appearances per player across the full window
        stmt = (
            select(
                GamePlayerStats.player_id,
                func.count().label("appearances"),
                func.sum(GamePlayerStats.points).label("total_points"),
                func.sum(GamePlayerStats.goals).label("total_goals"),
            )
            .join(Player, GamePlayerStats.player_id == Player.id)
            .where(
                and_(
                    GamePlayerStats.game_id.in_(all_game_ids),
                    Player.team_id == team_id,
                    Player.position != "G",
                )
            )
            .group_by(GamePlayerStats.player_id)
        )
        result = await db.execute(stmt)
        all_rows = {row.player_id: row for row in result.all()}

        # Identify regulars (>= 70% appearance rate)
        threshold = max(1, int(n_total * 0.70))
        regulars = {
            pid: row for pid, row in all_rows.items()
            if row.appearances >= threshold
        }

        if not regulars:
            return self._empty_lineup_status()

        # Find who played in the recent games
        recent_stmt = (
            select(GamePlayerStats.player_id)
            .where(
                and_(
                    GamePlayerStats.game_id.in_(recent_game_ids),
                    GamePlayerStats.player_id.in_(list(regulars.keys())),
                )
            )
            .distinct()
        )
        recent_result = await db.execute(recent_stmt)
        recent_players = {row[0] for row in recent_result.all()}

        # Missing regulars = regulars not in recent games AND confirmed
        # injured.  Without the injury cross-check, healthy scratches,
        # trades, AHL assignments, and rest days inflate the count.
        absent_pids = set(regulars.keys()) - recent_players

        # Query active injury reports for this team
        inj_stmt = select(InjuryReport.player_id).where(
            and_(
                InjuryReport.team_id == team_id,
                InjuryReport.active == True,
            )
        )
        inj_result = await db.execute(inj_stmt)
        injured_pids = {row[0] for row in inj_result.all()}

        # Only count absent players who have an active injury report
        missing_pids = absent_pids & injured_pids

        missing_ppg = 0.0
        missing_gpg = 0.0
        for pid in missing_pids:
            row = regulars[pid]
            games = row.appearances
            if games > 0:
                missing_ppg += row.total_points / games
                missing_gpg += row.total_goals / games

        total_regular_ppg = sum(
            r.total_points / r.appearances
            for r in regulars.values()
            if r.appearances > 0
        )

        lineup_strength = 1.0
        if total_regular_ppg > 0 and missing_ppg > 0:
            lineup_strength = max(0.70, 1.0 - (missing_ppg / total_regular_ppg) * 0.5)

        return {
            "regulars_count": len(regulars),
            "missing_count": len(missing_pids),
            "missing_points_per_game": round(missing_ppg, 3),
            "missing_goals_per_game": round(missing_gpg, 3),
            "lineup_strength": round(lineup_strength, 4),
            "total_regular_ppg": round(total_regular_ppg, 3),
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
    #  Injury impact features                                              #
    # ------------------------------------------------------------------ #

    async def get_injury_impact(
        self,
        db: AsyncSession,
        team_id: int,
    ) -> Dict[str, Any]:
        """
        Calculate the impact of known injuries on a team's expected output.

        Uses active InjuryReport records to determine which players are
        out or limited, then estimates xG reduction based on their
        production metrics and position importance.

        Returns:
            dict with xg_reduction (float 0-1), injured_players (list),
            total_missing_ppg, total_missing_gpg, goalie_injured (bool).
        """
        stmt = (
            select(InjuryReport)
            .join(Player, InjuryReport.player_id == Player.id)
            .where(
                and_(
                    InjuryReport.team_id == team_id,
                    InjuryReport.active == True,
                )
            )
        )
        result = await db.execute(stmt)
        injuries = result.scalars().all()

        if not injuries:
            return self._empty_injury_impact()

        total_ppg_lost = 0.0
        total_gpg_lost = 0.0
        goalie_injured = False
        injured_players = []

        for inj in injuries:
            status_weight = _ic.status_weights.get(inj.status, 0.5)

            # Get player position for multiplier
            player_stmt = select(Player.position).where(Player.id == inj.player_id)
            pos_result = await db.execute(player_stmt)
            pos_row = pos_result.one_or_none()
            position = pos_row[0] if pos_row else "C"

            pos_mult = _ic.position_multipliers.get(position, 1.0)

            ppg_impact = (inj.player_ppg or 0) * status_weight * pos_mult
            gpg_impact = (inj.player_gpg or 0) * status_weight * pos_mult

            total_ppg_lost += ppg_impact
            total_gpg_lost += gpg_impact

            if position == "G":
                goalie_injured = True

            injured_players.append({
                "player_id": inj.player_id,
                "status": inj.status,
                "position": position,
                "ppg_impact": round(ppg_impact, 3),
                "gpg_impact": round(gpg_impact, 3),
                "injury_type": inj.injury_type,
            })

        # Calculate xG reduction as a fraction of team's expected output
        # Use team's season goals-per-game as denominator
        season_stats = await self.get_season_stats(db, team_id)
        team_gpg = season_stats.get("goals_for_pg", 3.0)

        xg_reduction = 0.0
        if team_gpg > 0:
            xg_reduction = min(
                total_gpg_lost / team_gpg,
                _ic.max_injury_reduction,
            )

        return {
            "xg_reduction": round(xg_reduction, 4),
            "total_missing_ppg": round(total_ppg_lost, 3),
            "total_missing_gpg": round(total_gpg_lost, 3),
            "injured_count": len(injuries),
            "goalie_injured": goalie_injured,
            "injured_players": injured_players,
        }

    # ------------------------------------------------------------------ #
    #  Schedule context (B2B, rest, road trips)                           #
    # ------------------------------------------------------------------ #

    async def get_schedule_context(
        self,
        db: AsyncSession,
        team_id: int,
        game_date: Any,
    ) -> Dict[str, Any]:
        """
        Compute schedule fatigue factors for a team.

        Detects back-to-back games, rest days, games in last 7 days,
        and consecutive road games.

        Args:
            game_date: The date of the game being predicted.

        Returns:
            dict with is_back_to_back, days_rest, games_last_7,
            consecutive_road_games.
        """
        from datetime import timedelta

        if isinstance(game_date, str):
            from datetime import date as date_type
            game_date = date_type.fromisoformat(game_date)

        # Get recent games for this team ordered by date
        lookback = _mc.schedule_lookback
        start_date = game_date - timedelta(days=lookback)

        stmt = (
            select(Game)
            .where(
                and_(
                    or_(
                        Game.home_team_id == team_id,
                        Game.away_team_id == team_id,
                    ),
                    Game.date >= start_date,
                    Game.date < game_date,
                    Game.status == "final",
                )
            )
            .order_by(desc(Game.date))
        )
        result = await db.execute(stmt)
        recent_games = result.scalars().all()

        if not recent_games:
            return {
                "is_back_to_back": False,
                "days_rest": 3,
                "games_last_7": 0,
                "consecutive_road_games": 0,
            }

        # Days rest
        last_game = recent_games[0]
        days_rest = (game_date - last_game.date).days

        # Back-to-back (played yesterday)
        is_b2b = days_rest <= 1

        # Games in last 7 days
        week_ago = game_date - timedelta(days=7)
        games_last_7 = sum(1 for g in recent_games if g.date >= week_ago)

        # Consecutive road games
        consecutive_road = 0
        for game in recent_games:
            if game.away_team_id == team_id:
                consecutive_road += 1
            else:
                break

        # ---- Lookahead/letdown detection ----
        is_lookahead = False
        is_letdown = False

        # Fetch team object once for both checks
        team_obj = await self._get_team(db, team_id)

        # Check next game: if it's a divisional matchup, this game may
        # see reduced effort (lookahead spot).
        from datetime import timedelta as _td
        next_stmt = (
            select(Game)
            .where(
                and_(
                    or_(
                        Game.home_team_id == team_id,
                        Game.away_team_id == team_id,
                    ),
                    Game.date > game_date,
                    Game.date <= game_date + _td(days=3),
                )
            )
            .order_by(Game.date)
            .limit(1)
        )
        next_result = await db.execute(next_stmt)
        next_game = next_result.scalars().first()

        if next_game and team_obj:
            next_opp_id = self._get_opponent_id(next_game, team_id)
            next_opp_obj = await self._get_team(db, next_opp_id)
            if next_opp_obj and self._is_same_division(team_obj, next_opp_obj):
                is_lookahead = True

        # Letdown: previous game was an OT divisional battle
        if recent_games and team_obj:
            prev = recent_games[0]
            prev_opp_id = self._get_opponent_id(prev, team_id)
            prev_opp_obj = await self._get_team(db, prev_opp_id)
            if prev_opp_obj and self._is_same_division(team_obj, prev_opp_obj):
                if prev.went_to_overtime:
                    is_letdown = True

        # Travel disadvantage: extended road trips
        is_travel_disadvantage = consecutive_road >= 3

        return {
            "is_back_to_back": is_b2b,
            "days_rest": days_rest,
            "games_last_7": games_last_7,
            "consecutive_road_games": consecutive_road,
            "is_lookahead": is_lookahead,
            "is_letdown": is_letdown,
            "is_travel_disadvantage": is_travel_disadvantage,
        }

    # ------------------------------------------------------------------ #
    #  Special teams matchup                                               #
    # ------------------------------------------------------------------ #

    async def get_special_teams_matchup(
        self,
        db: AsyncSession,
        team_id: int,
    ) -> Dict[str, Any]:
        """
        Get a team's special teams metrics for matchup comparison.

        Returns PP% and PK% from season stats plus recent form.

        Returns:
            dict with pp_pct, pk_pct, pp_goals_per_game,
            pk_goals_against_per_game, penalty_minutes_per_game.
        """
        season = await self.get_season_stats(db, team_id)

        return {
            "pp_pct": season.get("pp_pct", 20.0),
            "pk_pct": season.get("pk_pct", 80.0),
        }

    # ------------------------------------------------------------------ #
    #  Advanced NHL metrics (Corsi-proxy, shot quality, PDO)              #
    # ------------------------------------------------------------------ #

    async def get_advanced_metrics(
        self,
        db: AsyncSession,
        team_id: int,
        last_n: int = 15,
    ) -> Dict[str, Any]:
        """
        Compute advanced possession and shot-quality metrics from existing data.

        Uses shots on goal (from Game) and blocked shots (from GamePlayerStats)
        to approximate Corsi (shot attempts) without needing missed-shot data.

        Corsi-proxy: CF = team_sog + opponent_blocked_shots
                     CA = opponent_sog + team_blocked_shots

        Returns:
            dict with corsi_for_pct, corsi_for_per60, shot_share,
            shooting_pct, opp_save_pct, pdo, high_danger_proxy, games_found.
        """
        games = await self._get_recent_games(db, team_id, last_n)

        if not games:
            return self._empty_advanced_metrics()

        total_cf = 0.0
        total_ca = 0.0
        total_shots_for = 0.0
        total_shots_against = 0.0
        total_goals_for = 0.0
        total_goals_against = 0.0
        total_blocked_for = 0.0     # blocks by this team (defensive)
        total_blocked_against = 0.0  # blocks by opponent (defensive)
        games_counted = 0

        for game in games:
            is_home = game.home_team_id == team_id
            gf = (game.home_score if is_home else game.away_score) or 0
            ga = (game.away_score if is_home else game.home_score) or 0
            sf = (game.home_shots if is_home else game.away_shots) or 0
            sa = (game.away_shots if is_home else game.home_shots) or 0

            if sf == 0 and sa == 0:
                continue

            # Get blocked shots per team from GamePlayerStats
            opp_id = game.away_team_id if is_home else game.home_team_id

            # Team's blocked shots (defensive blocks by this team's players)
            team_blocks = await self._get_team_game_blocks(db, game.id, team_id)
            # Opponent's blocked shots (defensive blocks by opponent's players)
            opp_blocks = await self._get_team_game_blocks(db, game.id, opp_id)

            # Corsi-proxy: shot attempts = SOG + opponent's blocks
            # (opponent blocked our shot attempts, so those are our unrecorded attempts)
            cf = sf + opp_blocks   # our shot attempts
            ca = sa + team_blocks  # their shot attempts

            total_cf += cf
            total_ca += ca
            total_shots_for += sf
            total_shots_against += sa
            total_goals_for += gf
            total_goals_against += ga
            total_blocked_for += team_blocks
            total_blocked_against += opp_blocks
            games_counted += 1

        if games_counted == 0:
            return self._empty_advanced_metrics()

        # Corsi metrics
        corsi_total = total_cf + total_ca
        cf_pct = (total_cf / corsi_total * 100.0) if corsi_total > 0 else 50.0
        cf_per60 = (total_cf / games_counted) * 3.0  # ~3 periods * 20 min, rough per-60

        # Shot share (SOG-based, simpler than Corsi)
        shot_total = total_shots_for + total_shots_against
        shot_share = (total_shots_for / shot_total * 100.0) if shot_total > 0 else 50.0

        # Shooting percentage
        shooting_pct = (total_goals_for / total_shots_for * 100.0) if total_shots_for > 0 else 8.0

        # Opponent save percentage (inverse of our shooting effectiveness)
        opp_save_pct = 1.0 - (total_goals_for / total_shots_for) if total_shots_for > 0 else 0.905

        # Team save percentage
        team_save_pct = 1.0 - (total_goals_against / total_shots_against) if total_shots_against > 0 else 0.905

        # PDO = shooting% + save% (league average ≈ 1.000)
        pdo = (total_goals_for / total_shots_for if total_shots_for > 0 else 0.08) + team_save_pct

        # High-danger proxy: goals per shot attempt (Corsi-based)
        # Higher = better quality chances
        high_danger_proxy = (total_goals_for / total_cf * 100.0) if total_cf > 0 else 5.0

        # Fenwick-proxy (unblocked shot attempts) = SOG only (we don't have missed shots)
        # So Fenwick ≈ shot_share in our case. We'll compute a differential metric instead.
        # Expected goals share proxy: weight goals by shot quality
        xgf_share = cf_pct  # Corsi% is our best xGF% proxy with available data

        return {
            "corsi_for_pct": round(cf_pct, 2),
            "corsi_for_per60": round(cf_per60, 2),
            "corsi_against_per60": round((total_ca / games_counted) * 3.0, 2) if games_counted > 0 else 0.0,
            "shot_share": round(shot_share, 2),
            "shooting_pct": round(shooting_pct, 2),
            "team_save_pct": round(team_save_pct, 4),
            "pdo": round(pdo, 4),
            "high_danger_proxy": round(high_danger_proxy, 2),
            "xgf_share": round(xgf_share, 2),
            "avg_blocks_for": round(total_blocked_for / games_counted, 2),
            "avg_blocks_against": round(total_blocked_against / games_counted, 2),
            "games_found": games_counted,
        }

    async def _get_team_game_blocks(
        self,
        db: AsyncSession,
        game_id: int,
        team_id: int,
    ) -> int:
        """Sum blocked_shots for all players on a team in a specific game."""
        from app.models.player import Player

        stmt = (
            select(func.coalesce(func.sum(GamePlayerStats.blocked_shots), 0))
            .join(Player, GamePlayerStats.player_id == Player.id)
            .where(
                and_(
                    GamePlayerStats.game_id == game_id,
                    Player.team_id == team_id,
                )
            )
        )
        result = await db.execute(stmt)
        return int(result.scalar() or 0)

    @staticmethod
    def _empty_advanced_metrics() -> Dict[str, Any]:
        """Return default advanced metrics when no data is available."""
        return {
            "corsi_for_pct": 50.0,
            "corsi_for_per60": 0.0,
            "corsi_against_per60": 0.0,
            "shot_share": 50.0,
            "shooting_pct": 8.0,
            "team_save_pct": 0.905,
            "pdo": 1.0,
            "high_danger_proxy": 5.0,
            "xgf_share": 50.0,
            "avg_blocks_for": 0.0,
            "avg_blocks_against": 0.0,
            "games_found": 0,
        }

    # ------------------------------------------------------------------ #
    #  5v5 Even-strength possession (from MoneyPuck)                     #
    # ------------------------------------------------------------------ #

    async def get_ev_possession_metrics(
        self,
        db: AsyncSession,
        team_id: int,
    ) -> Dict[str, Any]:
        """Fetch 5v5 even-strength possession metrics from TeamEVStats.

        Returns MoneyPuck-sourced Corsi, Fenwick, and xGF percentages
        at 5-on-5, which are more predictive than all-situations metrics.
        """
        from app.models.team import TeamEVStats

        stmt = (
            select(TeamEVStats)
            .where(TeamEVStats.team_id == team_id)
            .order_by(TeamEVStats.scrape_date.desc())
            .limit(1)
        )
        result = await db.execute(stmt)
        row = result.scalars().first()

        if not row:
            return self._empty_ev_possession()

        return {
            "ev_cf_pct": row.ev_cf_pct or 50.0,
            "ev_ff_pct": row.ev_ff_pct or 50.0,
            "ev_xgf_pct": row.ev_xgf_pct or 50.0,
            "ev_shots_for_pct": row.ev_shots_for_pct or 50.0,
            "games_found": row.games_played or 0,
        }

    @staticmethod
    def _empty_ev_possession() -> Dict[str, Any]:
        return {
            "ev_cf_pct": 50.0,
            "ev_ff_pct": 50.0,
            "ev_xgf_pct": 50.0,
            "ev_shots_for_pct": 50.0,
            "games_found": 0,
        }

    # ------------------------------------------------------------------ #
    #  Close-game possession (CF% in tight games)                        #
    # ------------------------------------------------------------------ #

    async def get_close_game_possession(
        self,
        db: AsyncSession,
        team_id: int,
        last_n: int = 20,
    ) -> Dict[str, Any]:
        """Compute Corsi-proxy filtered to close games only.

        Close games = final score margin <= config threshold OR went to OT.
        CF% in close games is more predictive than overall CF% because
        blowout Corsi is heavily influenced by score effects.
        """
        games = await self._get_recent_games(db, team_id, last_n)
        if not games:
            return self._empty_close_game_possession()

        margin_threshold = _mc.close_game_margin
        total_cf = 0.0
        total_ca = 0.0
        close_wins = 0
        close_games = 0

        for game in games:
            is_home = game.home_team_id == team_id
            hs = game.home_score or 0
            aws = game.away_score or 0
            margin = abs(hs - aws)
            went_ot = getattr(game, "went_to_overtime", False) or False

            # Only count close games
            if margin > margin_threshold and not went_ot:
                continue

            sf = (game.home_shots if is_home else game.away_shots) or 0
            sa = (game.away_shots if is_home else game.home_shots) or 0
            if sf == 0 and sa == 0:
                continue

            opp_id = game.away_team_id if is_home else game.home_team_id
            team_blocks = await self._get_team_game_blocks(db, game.id, team_id)
            opp_blocks = await self._get_team_game_blocks(db, game.id, opp_id)

            total_cf += sf + opp_blocks
            total_ca += sa + team_blocks
            close_games += 1

            # Did we win?
            gf = hs if is_home else aws
            ga = aws if is_home else hs
            if gf > ga:
                close_wins += 1

        if close_games == 0:
            return self._empty_close_game_possession()

        corsi_total = total_cf + total_ca
        cf_pct = (total_cf / corsi_total * 100.0) if corsi_total > 0 else 50.0

        return {
            "close_cf_pct": round(cf_pct, 2),
            "close_cf_differential": round(cf_pct - 50.0, 2),
            "close_game_win_rate": round(close_wins / close_games, 3) if close_games > 0 else 0.5,
            "close_games_found": close_games,
            "total_games_checked": len(games),
        }

    @staticmethod
    def _empty_close_game_possession() -> Dict[str, Any]:
        return {
            "close_cf_pct": 50.0,
            "close_cf_differential": 0.0,
            "close_game_win_rate": 0.5,
            "close_games_found": 0,
            "total_games_checked": 0,
        }

    # ------------------------------------------------------------------ #
    #  Goalie tier classification                                        #
    # ------------------------------------------------------------------ #

    def classify_goalie_tier(self, goalie: Dict[str, Any]) -> Dict[str, Any]:
        """Add tier classification to goalie features.

        Tiers:
        - elite (3): SV% >= .920 AND games_started >= threshold
        - starter (2): SV% >= .905 AND games_started >= threshold
        - backup (1): everything else

        Returns the goalie dict augmented with 'tier' and 'tier_rank'.
        """
        sv_pct = goalie.get("season_save_pct", 0.900)
        gs = goalie.get("games_started_season", 0)
        min_gs = _mc.goalie_tier_starter_min_gs

        if sv_pct >= _mc.goalie_tier_elite_sv and gs >= min_gs:
            tier = "elite"
            tier_rank = 3
        elif sv_pct >= _mc.goalie_tier_starter_sv and gs >= min_gs:
            tier = "starter"
            tier_rank = 2
        else:
            tier = "backup"
            tier_rank = 1

        return {
            **goalie,
            "tier": tier,
            "tier_rank": tier_rank,
        }

    # ------------------------------------------------------------------ #
    #  Line movement (opening vs current odds)                           #
    # ------------------------------------------------------------------ #

    async def get_line_movement(
        self,
        db: AsyncSession,
        game_id: int,
        game: Game,
    ) -> Dict[str, Any]:
        """
        Compute line movement features by comparing opening and current odds snapshots.

        Sharp money moving a line is one of the strongest signals in sports
        betting. A significant move toward one side suggests informed bettors
        have taken a position.

        Returns dict with movement deltas and a directional signal.
        """
        defaults = {
            "home_ml_open": None,
            "away_ml_open": None,
            "home_ml_current": None,
            "away_ml_current": None,
            "home_ml_move": 0.0,
            "away_ml_move": 0.0,
            "total_open": None,
            "total_current": None,
            "total_move": 0.0,
            "spread_open": None,
            "spread_current": None,
            "spread_move": 0.0,
            "sharp_signal": "neutral",
            "snapshots_count": 0,
        }

        try:
            # Get earliest (opening) snapshot
            opening_stmt = (
                select(OddsSnapshot)
                .where(OddsSnapshot.game_id == game_id)
                .order_by(OddsSnapshot.captured_at.asc())
                .limit(1)
            )
            opening_result = await db.execute(opening_stmt)
            opening = opening_result.scalar_one_or_none()

            # Get latest (current) snapshot
            current_stmt = (
                select(OddsSnapshot)
                .where(OddsSnapshot.game_id == game_id)
                .order_by(OddsSnapshot.captured_at.desc())
                .limit(1)
            )
            current_result = await db.execute(current_stmt)
            current = current_result.scalar_one_or_none()

            # Count total snapshots for this game
            count_stmt = (
                select(func.count(OddsSnapshot.id))
                .where(OddsSnapshot.game_id == game_id)
            )
            count_result = await db.execute(count_stmt)
            snap_count = count_result.scalar() or 0

            if not opening or not current or snap_count < 2:
                # Use game-level odds as fallback if only one snapshot
                if opening:
                    defaults["home_ml_open"] = opening.home_moneyline
                    defaults["away_ml_open"] = opening.away_moneyline
                    defaults["total_open"] = opening.over_under_line
                    defaults["spread_open"] = opening.home_spread_line
                defaults["snapshots_count"] = snap_count
                return defaults

            result = {
                "home_ml_open": opening.home_moneyline,
                "away_ml_open": opening.away_moneyline,
                "home_ml_current": current.home_moneyline,
                "away_ml_current": current.away_moneyline,
                "total_open": opening.over_under_line,
                "total_current": current.over_under_line,
                "spread_open": opening.home_spread_line,
                "spread_current": current.home_spread_line,
                "snapshots_count": snap_count,
            }

            # Compute movement deltas
            home_ml_move = 0.0
            if opening.home_moneyline and current.home_moneyline:
                home_ml_move = current.home_moneyline - opening.home_moneyline
            result["home_ml_move"] = home_ml_move

            away_ml_move = 0.0
            if opening.away_moneyline and current.away_moneyline:
                away_ml_move = current.away_moneyline - opening.away_moneyline
            result["away_ml_move"] = away_ml_move

            total_move = 0.0
            if opening.over_under_line and current.over_under_line:
                total_move = current.over_under_line - opening.over_under_line
            result["total_move"] = total_move

            spread_move = 0.0
            if opening.home_spread_line and current.home_spread_line:
                spread_move = current.home_spread_line - opening.home_spread_line
            result["spread_move"] = spread_move

            # Determine sharp signal based on moneyline movement
            # In American odds: line moving more negative = more favored
            # Significant threshold: 15+ points of ML movement
            signal = "neutral"
            if abs(home_ml_move) >= 15:
                if home_ml_move < 0:
                    signal = "sharp_home"  # Home becoming more favored
                else:
                    signal = "sharp_away"  # Home becoming less favored
            result["sharp_signal"] = signal

            return result

        except Exception as exc:
            logger.warning("Failed to compute line movement for game %s: %s", game_id, exc)
            return defaults

    # ------------------------------------------------------------------ #
    #  Starter confirmation confidence                                   #
    # ------------------------------------------------------------------ #

    def assess_starter_confidence(
        self,
        goalie: Dict[str, Any],
        schedule: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Assess how confident we are that the projected starter will play.

        Factors that reduce confidence:
        - Team on a back-to-back
        - High consecutive starts (fatigue)
        - Goalie not the regular starter (low games_started)

        Returns dict with starter_confidence (0-1), confidence_level, reasons.
        """
        confidence = _mc.starter_confidence_high
        reasons = []

        consecutive = goalie.get("consecutive_starts", 0)
        is_b2b = schedule.get("is_back_to_back", False)
        gs = goalie.get("games_started_season", 0)
        fatigue_threshold = _mc.starter_fatigue_threshold

        # If starter is confirmed via DFO or NHL API, confidence is high
        starter_status = goalie.get("starter_status", "")
        if goalie.get("starter_confirmed", False) or starter_status == "confirmed":
            source = goalie.get("starter_source", "external")
            return {
                "projected_starter": goalie.get("goalie_name", "Unknown"),
                "starter_confidence": 0.98,
                "confidence_level": "high",
                "confidence_reasons": [f"Confirmed starter ({source})"],
            }

        # DFO "expected" / "likely" is strong signal, boost confidence
        if starter_status in ("expected", "likely"):
            return {
                "projected_starter": goalie.get("goalie_name", "Unknown"),
                "starter_confidence": 0.90,
                "confidence_level": "high",
                "confidence_reasons": [f"Expected starter ({starter_status} via DFO)"],
            }

        # Back-to-back reduces confidence
        if is_b2b:
            confidence = min(confidence, _mc.starter_confidence_medium)
            reasons.append("Team on back-to-back")

        # High consecutive starts (fatigue)
        if consecutive >= fatigue_threshold:
            extra = consecutive - fatigue_threshold
            confidence -= 0.10 * (1 + extra)
            reasons.append(f"{consecutive} consecutive starts")

        # Low games started = not the clear #1
        if gs < _mc.goalie_tier_starter_min_gs // 2:
            confidence -= 0.10
            reasons.append("Limited starts this season")

        # Clamp
        confidence = max(_mc.starter_confidence_low, min(1.0, confidence))

        if confidence >= _mc.starter_confidence_high:
            level = "high"
        elif confidence >= _mc.starter_confidence_medium:
            level = "medium"
        else:
            level = "low"

        if not reasons:
            reasons.append("Regular starter, well rested")

        return {
            "projected_starter": goalie.get("goalie_name", "Unknown"),
            "starter_confidence": round(confidence, 2),
            "confidence_level": level,
            "confidence_reasons": reasons,
        }

    # ------------------------------------------------------------------ #
    #  Penalty discipline                                                 #
    # ------------------------------------------------------------------ #

    async def get_penalty_discipline(
        self,
        db: AsyncSession,
        team_id: int,
        last_n: int = 10,
    ) -> Dict[str, Any]:
        """
        Calculate penalty discipline metrics from recent games.

        Teams that take more penalties give their opponents more power-play
        chances, effectively boosting the opponent's xG. Uses per-game
        penalty minutes (PIM) from GamePlayerStats.

        Returns:
            dict with keys: avg_pim_per_game, avg_penalties_drawn,
            discipline_rating (0-1, higher = more disciplined), games_found.
        """
        games = await self._get_recent_games(db, team_id, last_n)
        if not games:
            return {"avg_pim_per_game": 6.0, "discipline_rating": 0.5, "games_found": 0}

        game_ids = [g.id for g in games]

        # Total PIM taken by this team across recent games
        stmt = (
            select(
                func.sum(GamePlayerStats.pim).label("total_pim"),
                func.count(func.distinct(GamePlayerStats.game_id)).label("game_count"),
            )
            .join(Player, GamePlayerStats.player_id == Player.id)
            .where(
                and_(
                    GamePlayerStats.game_id.in_(game_ids),
                    Player.team_id == team_id,
                )
            )
        )
        result = await db.execute(stmt)
        row = result.one_or_none()

        if not row or not row.game_count:
            return {"avg_pim_per_game": 6.0, "discipline_rating": 0.5, "games_found": 0}

        total_pim = row.total_pim or 0
        n_games = row.game_count
        avg_pim = total_pim / n_games

        # Discipline rating: league avg ~8 PIM/game. Scale so lower PIM = higher rating.
        # 4 PIM → 1.0 (very disciplined), 12 PIM → 0.0 (undisciplined)
        discipline_rating = max(0.0, min(1.0, (12.0 - avg_pim) / 8.0))

        return {
            "avg_pim_per_game": round(avg_pim, 2),
            "discipline_rating": round(discipline_rating, 4),
            "games_found": n_games,
        }

    # ------------------------------------------------------------------ #
    #  Close-game record (1-goal games — clutch factor)                   #
    # ------------------------------------------------------------------ #

    async def get_close_game_record(
        self,
        db: AsyncSession,
        team_id: int,
        last_n: int = 20,
    ) -> Dict[str, Any]:
        """
        Calculate win rate in close games (decided by 1 goal in regulation).

        Teams that consistently win or lose tight games exhibit a clutch
        factor that raw xG doesn't capture.

        Returns:
            dict with keys: close_game_win_rate, close_games_found,
            scoring_first_rate.
        """
        games = await self._get_recent_games(db, team_id, last_n)
        if not games:
            return {
                "close_game_win_rate": 0.5,
                "close_games_found": 0,
                "scoring_first_rate": 0.5,
            }

        close_wins = 0
        close_games = 0
        scored_first = 0
        games_with_p1 = 0

        for game in games:
            is_home = game.home_team_id == team_id
            gf = game.home_score if is_home else game.away_score
            ga = game.away_score if is_home else game.home_score

            if gf is None or ga is None:
                continue

            margin = abs(gf - ga)
            # 1-goal games (regulation or OT) are "close"
            if margin <= 1:
                close_games += 1
                if gf > ga:
                    close_wins += 1

            # Scoring first: did this team score in P1 while opponent didn't?
            if game.home_score_p1 is not None:
                p1_for = (game.home_score_p1 if is_home else game.away_score_p1) or 0
                p1_against = (game.away_score_p1 if is_home else game.home_score_p1) or 0
                games_with_p1 += 1
                if p1_for > p1_against:
                    scored_first += 1

        close_wr = close_wins / close_games if close_games > 0 else 0.5
        scoring_first_rate = scored_first / games_with_p1 if games_with_p1 > 0 else 0.5

        return {
            "close_game_win_rate": round(close_wr, 4),
            "close_games_found": close_games,
            "scoring_first_rate": round(scoring_first_rate, 4),
        }

    # ------------------------------------------------------------------ #
    #  Confirmed starting goalie (NHL API)                                #
    # ------------------------------------------------------------------ #

    async def get_confirmed_starter(
        self,
        db: AsyncSession,
        game_id: int,
        team_id: int,
        goalie_features: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Check if the starting goalie has been officially confirmed.

        Queries the NHL API game landing page for confirmed starter info.
        If a confirmed starter differs from the projected starter (from
        recent game history), updates goalie features with the actual
        starter's stats.

        Returns the goalie_features dict, potentially updated with the
        confirmed starter's data, plus confirmation metadata.
        """
        try:
            from app.scrapers.starter_scraper import get_confirmed_starter_for_team
            confirmed = await get_confirmed_starter_for_team(db, game_id, team_id)
        except Exception as exc:
            logger.debug("Starter confirmation unavailable: %s", exc)
            confirmed = None

        if not confirmed:
            return goalie_features

        confirmed_name = confirmed.get("goalie_name", "")
        projected_name = goalie_features.get("goalie_name", "")
        is_confirmed = confirmed.get("confirmed", False)
        status = confirmed.get("status", "").lower()
        source = confirmed.get("starter_source", "dfo")

        # DFO statuses like "confirmed", "expected", "likely" are all more
        # reliable than our "whoever started the last game" heuristic.
        # Only skip if the status is explicitly "unconfirmed" or empty.
        is_actionable = is_confirmed or status in (
            "confirmed", "expected", "likely", "projected",
        )

        # If the starter differs from our projected one, swap goalie stats
        if confirmed_name and confirmed_name != projected_name and is_actionable:
            # Try finding the goalie by external_id first, then by name
            player = None
            ext_id = confirmed.get("goalie_external_id", "")
            if ext_id:
                stmt = select(Player).where(Player.external_id == str(ext_id))
                result = await db.execute(stmt)
                player = result.scalars().first()

            if not player:
                # Fall back to name-based lookup on this team's goalies
                stmt = select(Player).where(
                    Player.team_id == team_id,
                    Player.position == "G",
                    func.lower(Player.name) == confirmed_name.lower(),
                )
                result = await db.execute(stmt)
                player = result.scalars().first()

            if not player:
                # Try partial name match (DFO may use slightly different formatting)
                stmt = select(Player).where(
                    Player.team_id == team_id,
                    Player.position == "G",
                )
                result = await db.execute(stmt)
                team_goalies = result.scalars().all()
                confirmed_lower = confirmed_name.lower()
                for g in team_goalies:
                    # Match on last name (handles "J. Dobes" vs "Jakub Dobes")
                    g_last = g.name.split()[-1].lower() if g.name else ""
                    c_last = confirmed_lower.split()[-1] if confirmed_lower else ""
                    if g_last and c_last and g_last == c_last:
                        player = g
                        break

            if player:
                logger.info(
                    "Starter switch: %s → %s (team_id=%d, status=%s, source=%s)",
                    projected_name, confirmed_name, team_id, status, source,
                )
                new_features = await self._get_goalie_features_for_player(
                    db, player.id, player.name
                )
                new_features["starter_confirmed"] = is_confirmed
                new_features["starter_source"] = source
                new_features["starter_status"] = status or ("confirmed" if is_confirmed else "projected")
                return new_features
            else:
                logger.warning(
                    "Starter %s (team_id=%d) from %s not found in DB, "
                    "keeping projected starter %s",
                    confirmed_name, team_id, source, projected_name,
                )

        # Same goalie or no actionable switch — mark confirmation status
        result = {**goalie_features}
        if is_confirmed:
            result["starter_confirmed"] = True
            result["starter_source"] = source
        else:
            result["starter_confirmed"] = False
            result["starter_source"] = source if confirmed_name else "projected"
        result["starter_status"] = status or ("confirmed" if is_confirmed else "projected")
        return result

    async def _get_goalie_features_for_player(
        self,
        db: AsyncSession,
        goalie_id: int,
        goalie_name: str,
    ) -> Dict[str, Any]:
        """Get goalie features for a specific player ID (used when starter is confirmed)."""
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

        recent_games_stmt = (
            select(GameGoalieStats)
            .join(Game, GameGoalieStats.game_id == Game.id)
            .where(
                and_(
                    GameGoalieStats.player_id == goalie_id,
                    GameGoalieStats.decision.isnot(None),
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
            "consecutive_starts": len(recent_games),
        }

    # ------------------------------------------------------------------ #
    #  Goalie venue splits (home vs away performance)                     #
    # ------------------------------------------------------------------ #

    async def get_goalie_venue_splits(
        self,
        db: AsyncSession,
        goalie_id: Optional[int],
        team_id: int,
        is_home: bool,
    ) -> Dict[str, Any]:
        """Calculate a goalie's performance split by venue (home vs away).

        Some goalies perform significantly differently at home vs on
        the road. This captures that split.

        Returns:
            dict with venue_save_pct, venue_gaa, venue_record, venue_games,
            significant (bool).
        """
        empty = self._empty_venue_splits()
        if goalie_id is None:
            return empty

        # Find games where this goalie played at home or away
        if is_home:
            venue_filter = Game.home_team_id == team_id
        else:
            venue_filter = Game.away_team_id == team_id

        stmt = (
            select(GameGoalieStats)
            .join(Game, GameGoalieStats.game_id == Game.id)
            .where(
                and_(
                    GameGoalieStats.player_id == goalie_id,
                    GameGoalieStats.decision.isnot(None),
                    Game.status == "final",
                    venue_filter,
                )
            )
            .order_by(desc(Game.date))
            .limit(15)
        )
        result = await db.execute(stmt)
        games = result.scalars().all()

        if not games:
            return empty

        total_saves = 0
        total_shots = 0
        total_ga = 0
        wins = 0

        for ggs in games:
            total_saves += ggs.saves or 0
            total_shots += ggs.shots_against or 0
            total_ga += ggs.goals_against or 0
            if ggs.decision == "W":
                wins += 1

        count = len(games)
        sv_pct = total_saves / total_shots if total_shots > 0 else 0.900
        gaa = total_ga / count if count > 0 else 3.00

        return {
            "venue_save_pct": round(sv_pct, 4),
            "venue_gaa": round(gaa, 3),
            "venue_record": f"{wins}-{count - wins}",
            "venue_games": count,
            "significant": count >= _mc.goalie_venue_min_games,
        }

    @staticmethod
    def _empty_venue_splits() -> Dict[str, Any]:
        return {
            "venue_save_pct": 0.900,
            "venue_gaa": 3.00,
            "venue_record": "0-0",
            "venue_games": 0,
            "significant": False,
        }

    # ------------------------------------------------------------------ #
    #  Goalie workload / fatigue (shots-based)                            #
    # ------------------------------------------------------------------ #

    async def get_goalie_workload(
        self,
        db: AsyncSession,
        goalie_id: Optional[int],
    ) -> Dict[str, Any]:
        """Calculate recent workload for a goalie based on shots faced.

        Goes beyond simple consecutive starts — measures actual shot
        volume faced in recent games to detect true fatigue.

        Returns:
            dict with recent_shots_faced (last 3 games), avg_shots_per_start,
            heavy_workload (bool), workload_factor (0-1 multiplier).
        """
        empty = {
            "recent_shots_3g": 0,
            "avg_shots_per_start": 30.0,
            "heavy_workload": False,
            "workload_factor": 1.0,
        }
        if goalie_id is None:
            return empty

        stmt = (
            select(GameGoalieStats)
            .join(Game, GameGoalieStats.game_id == Game.id)
            .where(
                and_(
                    GameGoalieStats.player_id == goalie_id,
                    GameGoalieStats.decision.isnot(None),
                    Game.status == "final",
                )
            )
            .order_by(desc(Game.date))
            .limit(5)
        )
        result = await db.execute(stmt)
        games = result.scalars().all()

        if not games:
            return empty

        # Last 3 games for recent workload
        recent_3 = games[:3]
        shots_3g = sum(g.shots_against or 0 for g in recent_3)
        avg_shots = shots_3g / len(recent_3) if recent_3 else 30.0

        # All 5 for overall average
        total_shots = sum(g.shots_against or 0 for g in games)
        avg_shots_5g = total_shots / len(games)

        # Heavy workload: averaging 35+ shots per game over last 3
        heavy = avg_shots >= _mc.goalie_heavy_workload_threshold

        # Workload factor: 1.0 = normal, >1.0 = heavy (opponent xG boost)
        # Each shot above league avg (30) in last 3 games adds a small penalty
        excess_shots = max(0, avg_shots - 30.0)
        factor = 1.0 + excess_shots * _mc.goalie_workload_per_shot

        return {
            "recent_shots_3g": shots_3g,
            "avg_shots_per_start": round(avg_shots_5g, 1),
            "heavy_workload": heavy,
            "workload_factor": round(min(factor, 1.15), 4),
        }

    # ------------------------------------------------------------------ #
    #  Pace / tempo metrics                                               #
    # ------------------------------------------------------------------ #

    async def get_pace_metrics(
        self,
        db: AsyncSession,
        team_id: int,
        last_n: int = 15,
    ) -> Dict[str, Any]:
        """Calculate pace/tempo metrics for a team.

        Pace = total shots + opponent shots per game. High-pace teams
        generate (and allow) more shot attempts, leading to more
        scoring opportunities. When two high-pace teams meet, the
        total goals tend to exceed what individual team averages suggest.

        Returns:
            dict with pace (shots/game), shot_generation, shots_allowed,
            pace_category (fast/average/slow), games_found.
        """
        games = await self._get_recent_games(db, team_id, last_n)

        if not games:
            return self._empty_pace_metrics()

        total_shots_for = 0
        total_shots_against = 0
        total_goals_for = 0
        total_goals_against = 0
        games_counted = 0

        for game in games:
            is_home = game.home_team_id == team_id
            sf = (game.home_shots if is_home else game.away_shots) or 0
            sa = (game.away_shots if is_home else game.home_shots) or 0
            gf = (game.home_score if is_home else game.away_score) or 0
            ga = (game.away_score if is_home else game.home_score) or 0

            if sf == 0 and sa == 0:
                continue

            total_shots_for += sf
            total_shots_against += sa
            total_goals_for += gf
            total_goals_against += ga
            games_counted += 1

        if games_counted == 0:
            return self._empty_pace_metrics()

        avg_sf = total_shots_for / games_counted
        avg_sa = total_shots_against / games_counted
        pace = avg_sf + avg_sa  # total shots per game
        avg_total_goals = (total_goals_for + total_goals_against) / games_counted

        # Classify pace
        if pace >= _mc.pace_fast_threshold:
            category = "fast"
        elif pace <= _mc.pace_slow_threshold:
            category = "slow"
        else:
            category = "average"

        return {
            "pace": round(pace, 1),
            "shot_generation": round(avg_sf, 1),
            "shots_allowed": round(avg_sa, 1),
            "avg_total_goals": round(avg_total_goals, 2),
            "pace_category": category,
            "games_found": games_counted,
        }

    @staticmethod
    def _empty_pace_metrics() -> Dict[str, Any]:
        return {
            "pace": 60.0,
            "shot_generation": 30.0,
            "shots_allowed": 30.0,
            "avg_total_goals": 6.0,
            "pace_category": "average",
            "games_found": 0,
        }

    # ------------------------------------------------------------------ #
    #  Score-close possession (within 1 goal, regulation only)            #
    # ------------------------------------------------------------------ #

    async def get_score_close_stats(
        self,
        db: AsyncSession,
        team_id: int,
        last_n: int = 20,
    ) -> Dict[str, Any]:
        """Calculate offensive/defensive metrics from score-close games.

        "Score-close" means games decided by 1 goal or that went to OT.
        These games better reflect true team quality because blowouts
        skew stats (teams protecting leads play differently).

        Returns:
            dict with close_gf_pg, close_ga_pg, close_shot_share,
            close_games_found.
        """
        games = await self._get_recent_games(db, team_id, last_n)

        if not games:
            return self._empty_score_close_stats()

        total_gf = 0
        total_ga = 0
        total_sf = 0
        total_sa = 0
        close_games = 0

        for game in games:
            if game.home_score is None or game.away_score is None:
                continue

            is_home = game.home_team_id == team_id
            gf = game.home_score if is_home else game.away_score
            ga = game.away_score if is_home else game.home_score
            sf = (game.home_shots if is_home else game.away_shots) or 0
            sa = (game.away_shots if is_home else game.home_shots) or 0

            # Score-close: decided by 1 goal or went to OT
            margin = abs(gf - ga)
            went_ot = game.went_to_overtime or False
            if margin <= 1 or went_ot:
                total_gf += gf
                total_ga += ga
                total_sf += sf
                total_sa += sa
                close_games += 1

        if close_games == 0:
            return self._empty_score_close_stats()

        shot_total = total_sf + total_sa
        shot_share = (total_sf / shot_total * 100.0) if shot_total > 0 else 50.0

        return {
            "close_gf_pg": round(total_gf / close_games, 3),
            "close_ga_pg": round(total_ga / close_games, 3),
            "close_shot_share": round(shot_share, 2),
            "close_games_found": close_games,
        }

    @staticmethod
    def _empty_score_close_stats() -> Dict[str, Any]:
        return {
            "close_gf_pg": 3.0,
            "close_ga_pg": 3.0,
            "close_shot_share": 50.0,
            "close_games_found": 0,
        }

    # ------------------------------------------------------------------ #
    #  Feature #6: PP opportunity rate vs opponent                        #
    # ------------------------------------------------------------------ #

    async def get_pp_opportunity_rate(
        self,
        db: AsyncSession,
        team_id: int,
        opponent_id: int,
        last_n: int = 15,
    ) -> Dict[str, Any]:
        """Estimate power-play opportunities each team creates/allows.

        An undisciplined team facing an elite PP is far worse than the
        current model captures (which only uses PP% and PK%).  This
        estimates *how many* PP chances opponents get from penalty
        minutes, then multiplies by the opponent's PP conversion rate.

        Returns:
            dict with pp_opportunities_for, pp_opportunities_against,
            opponent_pp_pct, expected_pp_goals_against,
            opponent_pk_pct, expected_pp_goals_for, games_found.
        """
        # Get this team's penalty discipline (PIM) and opponent's PP%
        team_disc = await self.get_penalty_discipline(db, team_id, last_n)
        opp_disc = await self.get_penalty_discipline(db, opponent_id, last_n)
        team_special = await self.get_special_teams_matchup(db, team_id)
        opp_special = await self.get_special_teams_matchup(db, opponent_id)

        # Estimate PP opportunities from PIM: ~1 PP per 4 PIM (2min minors)
        team_pim = team_disc.get("avg_pim_per_game", 6.0)
        opp_pim = opp_disc.get("avg_pim_per_game", 6.0)
        pp_opp_for = opp_pim / 4.0   # opponent's penalties = our PP chances
        pp_opp_against = team_pim / 4.0  # our penalties = their PP chances

        opp_pp_pct = opp_special.get("pp_pct", 20.0) / 100.0
        team_pp_pct = team_special.get("pp_pct", 20.0) / 100.0
        opp_pk_pct = opp_special.get("pk_pct", 80.0) / 100.0

        # Expected PP goals: opportunities × conversion rate
        expected_pp_goals_against = pp_opp_against * opp_pp_pct
        expected_pp_goals_for = pp_opp_for * team_pp_pct * (1.0 - opp_pk_pct + opp_pp_pct)
        # Simplified: just opportunities × their PP%
        expected_pp_goals_for = pp_opp_for * team_pp_pct
        expected_pp_goals_against = pp_opp_against * opp_pp_pct

        # Net PP impact: positive = we gain more from PP than we give up
        net_pp_impact = expected_pp_goals_for - expected_pp_goals_against

        return {
            "pp_opportunities_for": round(pp_opp_for, 2),
            "pp_opportunities_against": round(pp_opp_against, 2),
            "expected_pp_goals_for": round(expected_pp_goals_for, 3),
            "expected_pp_goals_against": round(expected_pp_goals_against, 3),
            "net_pp_impact": round(net_pp_impact, 3),
            "opponent_pp_pct": round(opp_pp_pct * 100, 1),
            "opponent_pk_pct": round(opp_pk_pct * 100, 1),
            "team_pp_pct": round(team_pp_pct * 100, 1),
            "games_found": min(
                team_disc.get("games_found", 0),
                opp_disc.get("games_found", 0),
            ),
        }

    # ------------------------------------------------------------------ #
    #  Feature #7: Shooting quality against (HDSV% proxy)                 #
    # ------------------------------------------------------------------ #

    async def get_shooting_quality_against(
        self,
        db: AsyncSession,
        team_id: int,
        last_n: int = 15,
    ) -> Dict[str, Any]:
        """Estimate shot quality a team faces using goalie performance.

        Two goalies can have the same SV% but face very different shot
        quality.  A goalie with a high save% against a high-shooting%
        team is genuinely better than one padding stats on perimeter
        shots.  We approximate HDSV% by comparing the opposing team's
        shooting% against this team vs their overall shooting%.

        Returns:
            dict with avg_opp_shooting_pct, league_avg_shooting_pct,
            shot_quality_index (>1 = faces harder shots), games_found.
        """
        games = await self._get_recent_games(db, team_id, last_n)
        if not games:
            return {
                "avg_opp_shooting_pct": 8.0,
                "shot_quality_index": 1.0,
                "goals_saved_above_expected": 0.0,
                "games_found": 0,
            }

        # For each game, compute how the opposing team shot against us
        total_opp_shots = 0
        total_opp_goals = 0
        # Also compute expected goals against based on league avg SV%
        games_counted = 0
        league_avg_sv = _mc.league_avg_save_pct  # ~0.905

        for game in games:
            if game.home_score is None or game.away_score is None:
                continue
            is_home = game.home_team_id == team_id
            opp_goals = game.away_score if is_home else game.home_score
            opp_shots = (game.away_shots if is_home else game.home_shots) or 0

            if opp_shots == 0:
                continue

            total_opp_shots += opp_shots
            total_opp_goals += opp_goals
            games_counted += 1

        if games_counted == 0 or total_opp_shots == 0:
            return {
                "avg_opp_shooting_pct": 8.0,
                "shot_quality_index": 1.0,
                "goals_saved_above_expected": 0.0,
                "games_found": 0,
            }

        avg_opp_sh_pct = total_opp_goals / total_opp_shots * 100.0
        league_avg_sh_pct = (1.0 - league_avg_sv) * 100.0  # ~9.5%

        # Shot quality index: >1 means facing harder shots than average
        shot_quality_index = avg_opp_sh_pct / league_avg_sh_pct if league_avg_sh_pct > 0 else 1.0

        # Goals saved above expected: negative = letting in more than expected
        expected_goals = total_opp_shots * (1.0 - league_avg_sv)
        gsae = expected_goals - total_opp_goals  # positive = saved more than expected

        return {
            "avg_opp_shooting_pct": round(avg_opp_sh_pct, 2),
            "shot_quality_index": round(shot_quality_index, 3),
            "goals_saved_above_expected": round(gsae, 2),
            "games_found": games_counted,
        }

    # ------------------------------------------------------------------ #
    #  Feature #9: Line combination tracking (forward line stability)     #
    # ------------------------------------------------------------------ #

    async def get_line_combination_stability(
        self,
        db: AsyncSession,
        team_id: int,
        last_n: int = 10,
    ) -> Dict[str, Any]:
        """Track forward line stability based on co-appearance patterns.

        When top-line combos get broken up (injury, trade, coaching
        decision), the impact is bigger than individual player PPG
        suggests.  Chemistry matters.  We measure this by tracking
        how consistently the top-6 forwards appear together.

        Returns:
            dict with top6_stability (0-1), top6_players, games_found.
        """
        games = await self._get_recent_games(db, team_id, last_n)
        if not games:
            return {"top6_stability": 0.5, "top6_players": 0, "games_found": 0}

        game_ids = [g.id for g in games]
        n_games = len(game_ids)

        # Get all forward appearances (C, LW, RW) for this team in recent games
        stmt = (
            select(
                GamePlayerStats.player_id,
                func.count(GamePlayerStats.game_id).label("appearances"),
                func.sum(GamePlayerStats.goals + GamePlayerStats.assists).label("total_points"),
            )
            .join(Player, GamePlayerStats.player_id == Player.id)
            .where(
                and_(
                    GamePlayerStats.game_id.in_(game_ids),
                    Player.team_id == team_id,
                    Player.position.in_(["C", "LW", "RW", "F"]),
                )
            )
            .group_by(GamePlayerStats.player_id)
            .order_by(desc("total_points"))
        )
        result = await db.execute(stmt)
        rows = result.all()

        if not rows:
            return {"top6_stability": 0.5, "top6_players": 0, "games_found": 0}

        # Take top 6 by production
        top6 = rows[:6]
        top6_count = len(top6)

        # Stability = avg appearance rate of top-6 forwards
        # If all 6 play every game → 1.0. If they miss games → lower.
        if n_games > 0 and top6_count > 0:
            stability = sum(r.appearances / n_games for r in top6) / top6_count
        else:
            stability = 0.5

        return {
            "top6_stability": round(min(1.0, stability), 3),
            "top6_players": top6_count,
            "games_found": n_games,
        }

    # ------------------------------------------------------------------ #
    #  Feature #11: Recency-weighted H2H                                  #
    # ------------------------------------------------------------------ #

    async def get_recency_weighted_h2h(
        self,
        db: AsyncSession,
        team1_id: int,
        team2_id: int,
        last_n: int = 20,
        decay: float = 0.85,
    ) -> Dict[str, Any]:
        """Recency-weighted head-to-head stats with exponential decay.

        Current H2H treats a game from October the same as last week.
        Exponential decay (like we do for form) better captures evolving
        matchup dynamics — roster changes, coaching adjustments, etc.

        Returns:
            dict with team1_win_rate_weighted, team2_win_rate_weighted,
            avg_total_goals_weighted, recency_shift, games_found.
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

        empty = {
            "team1_win_rate_weighted": 0.5,
            "team2_win_rate_weighted": 0.5,
            "avg_total_goals_weighted": 5.5,
            "recency_shift": 0.0,
            "games_found": 0,
        }

        if not games:
            return empty

        t1_wins_weighted = 0.0
        total_goals_weighted = 0.0
        weight_sum = 0.0
        # Also compute unweighted for comparison
        t1_wins_raw = 0
        games_counted = 0

        for idx, game in enumerate(games):
            if game.home_score is None or game.away_score is None:
                continue
            games_counted += 1

            w = decay ** idx  # most recent = 1.0, next = 0.85, etc.
            weight_sum += w

            if game.home_team_id == team1_id:
                t1_goals = game.home_score
                t2_goals = game.away_score
            else:
                t1_goals = game.away_score
                t2_goals = game.home_score

            total_goals_weighted += (t1_goals + t2_goals) * w

            if t1_goals > t2_goals:
                t1_wins_weighted += w
                t1_wins_raw += 1

        if games_counted == 0 or weight_sum == 0:
            return empty

        weighted_wr = t1_wins_weighted / weight_sum
        raw_wr = t1_wins_raw / games_counted
        recency_shift = weighted_wr - raw_wr  # positive = team1 trending up

        return {
            "team1_win_rate_weighted": round(weighted_wr, 4),
            "team2_win_rate_weighted": round(1.0 - weighted_wr, 4),
            "avg_total_goals_weighted": round(total_goals_weighted / weight_sum, 3),
            "recency_shift": round(recency_shift, 4),
            "games_found": games_counted,
        }

    # ------------------------------------------------------------------ #
    #  Feature #13: Consensus line aggregation                            #
    # ------------------------------------------------------------------ #

    async def get_consensus_line(
        self,
        db: AsyncSession,
        game_id: int,
    ) -> Dict[str, Any]:
        """Build consensus odds by averaging across all sportsbook sources.

        Comparing our model against the consensus (average across books)
        rather than a single book's line gives a truer edge measurement.
        A single book may be an outlier; the market average is the real
        "true line" to beat.

        Returns:
            dict with consensus_home_ml, consensus_away_ml,
            consensus_total, consensus_home_implied, consensus_away_implied,
            sources_count.
        """
        # Query recent odds snapshots from different sources for this game
        stmt = (
            select(OddsSnapshot)
            .where(OddsSnapshot.game_id == game_id)
            .order_by(desc(OddsSnapshot.captured_at))
        )
        result = await db.execute(stmt)
        snapshots = result.scalars().all()

        empty = {
            "consensus_home_ml": None,
            "consensus_away_ml": None,
            "consensus_total": None,
            "consensus_home_implied": None,
            "consensus_away_implied": None,
            "sources_count": 0,
        }

        if not snapshots:
            return empty

        # Group by source, take the most recent snapshot from each
        by_source: Dict[str, Any] = {}
        for snap in snapshots:
            src = snap.source or "unknown"
            if src not in by_source:
                by_source[src] = snap

        if len(by_source) < 2:
            return empty

        # Average moneylines and totals across sources
        home_mls = [s.home_moneyline for s in by_source.values() if s.home_moneyline]
        away_mls = [s.away_moneyline for s in by_source.values() if s.away_moneyline]
        totals = [s.over_under_line for s in by_source.values() if s.over_under_line]

        consensus_home_ml = sum(home_mls) / len(home_mls) if home_mls else None
        consensus_away_ml = sum(away_mls) / len(away_mls) if away_mls else None
        consensus_total = sum(totals) / len(totals) if totals else None

        # Convert consensus ML to implied probability
        from app.analytics.models import american_odds_to_implied_prob
        consensus_home_implied = (
            american_odds_to_implied_prob(consensus_home_ml) if consensus_home_ml else None
        )
        consensus_away_implied = (
            american_odds_to_implied_prob(consensus_away_ml) if consensus_away_ml else None
        )

        return {
            "consensus_home_ml": round(consensus_home_ml) if consensus_home_ml else None,
            "consensus_away_ml": round(consensus_away_ml) if consensus_away_ml else None,
            "consensus_total": round(consensus_total, 1) if consensus_total else None,
            "consensus_home_implied": round(consensus_home_implied, 4) if consensus_home_implied else None,
            "consensus_away_implied": round(consensus_away_implied, 4) if consensus_away_implied else None,
            "sources_count": len(by_source),
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

        # Away team features
        away_form_5 = await self.get_team_form(db, away_id, last_n=5)
        away_form_10 = await self.get_team_form(db, away_id, last_n=10)
        away_season = await self.get_season_stats(db, away_id)
        away_splits = await self.get_team_home_away_splits(db, away_id, is_home=False)
        away_goalie = await self.get_goalie_features(db, away_id)
        away_periods = await self.get_period_stats(db, away_id)
        away_ot = await self.get_overtime_tendency(db, away_id)

        # Player talent and lineup status
        home_skaters = await self.get_skater_impact(db, home_id)
        away_skaters = await self.get_skater_impact(db, away_id)
        home_lineup = await self.get_lineup_status(db, home_id)
        away_lineup = await self.get_lineup_status(db, away_id)

        # Head-to-head
        h2h = await self.get_h2h_stats(db, home_id, away_id)

        # Injury impact
        home_injuries = await self.get_injury_impact(db, home_id)
        away_injuries = await self.get_injury_impact(db, away_id)

        # Schedule context
        home_schedule = await self.get_schedule_context(db, home_id, game.date)
        away_schedule = await self.get_schedule_context(db, away_id, game.date)

        # Special teams
        home_special = await self.get_special_teams_matchup(db, home_id)
        away_special = await self.get_special_teams_matchup(db, away_id)

        # Advanced metrics (Corsi-proxy, shot quality, PDO)
        home_advanced = await self.get_advanced_metrics(db, home_id)
        away_advanced = await self.get_advanced_metrics(db, away_id)

        # 5v5 even-strength possession (MoneyPuck)
        home_ev_possession = await self.get_ev_possession_metrics(db, home_id)
        away_ev_possession = await self.get_ev_possession_metrics(db, away_id)

        # Close-game possession
        home_close_possession = await self.get_close_game_possession(db, home_id)
        away_close_possession = await self.get_close_game_possession(db, away_id)

        # Goalie tier classification (augments existing goalie features)
        home_goalie = self.classify_goalie_tier(home_goalie)
        away_goalie = self.classify_goalie_tier(away_goalie)

        # Feature #1: Confirmed starter integration (NHL API)
        home_goalie = await self.get_confirmed_starter(
            db, game.id, home_id, home_goalie
        )
        away_goalie = await self.get_confirmed_starter(
            db, game.id, away_id, away_goalie
        )

        # Feature #3: Goalie venue splits (home vs away)
        home_goalie_venue = await self.get_goalie_venue_splits(
            db, home_goalie.get("goalie_id"), home_id, is_home=True
        )
        away_goalie_venue = await self.get_goalie_venue_splits(
            db, away_goalie.get("goalie_id"), away_id, is_home=False
        )

        # Feature #5: Goalie workload / shot-based fatigue
        home_goalie_workload = await self.get_goalie_workload(
            db, home_goalie.get("goalie_id")
        )
        away_goalie_workload = await self.get_goalie_workload(
            db, away_goalie.get("goalie_id")
        )

        # Goalie vs. specific opponent history
        home_goalie_vs_team = await self.get_goalie_vs_team(
            db, home_goalie.get("goalie_id"), home_id, away_id
        )
        away_goalie_vs_team = await self.get_goalie_vs_team(
            db, away_goalie.get("goalie_id"), away_id, home_id
        )

        # Starter confirmation confidence
        home_starter_status = self.assess_starter_confidence(home_goalie, home_schedule)
        away_starter_status = self.assess_starter_confidence(away_goalie, away_schedule)

        # Player and team matchups (uses MatchupEngine)
        from app.analytics.matchups import MatchupEngine
        matchup_engine = MatchupEngine()
        home_player_matchup = await matchup_engine.get_team_player_matchup_impact(
            db, home_id, away_id
        )
        away_player_matchup = await matchup_engine.get_team_player_matchup_impact(
            db, away_id, home_id
        )
        team_matchup = await matchup_engine.get_team_matchup_features(
            db, home_id, away_id
        )

        # Feature #4: Pace / tempo metrics
        home_pace = await self.get_pace_metrics(db, home_id)
        away_pace = await self.get_pace_metrics(db, away_id)

        # Feature #2: Score-close stats
        home_score_close = await self.get_score_close_stats(db, home_id)
        away_score_close = await self.get_score_close_stats(db, away_id)

        # Penalty discipline
        home_discipline = await self.get_penalty_discipline(db, home_id)
        away_discipline = await self.get_penalty_discipline(db, away_id)

        # Close-game record (clutch factor + scoring first)
        home_close_record = await self.get_close_game_record(db, home_id)
        away_close_record = await self.get_close_game_record(db, away_id)

        # Feature #6: PP opportunity rate vs opponent
        home_pp_opp = await self.get_pp_opportunity_rate(db, home_id, away_id)
        away_pp_opp = await self.get_pp_opportunity_rate(db, away_id, home_id)

        # Feature #7: Shooting quality against (HDSV% proxy)
        home_shot_quality = await self.get_shooting_quality_against(db, home_id)
        away_shot_quality = await self.get_shooting_quality_against(db, away_id)

        # Feature #9: Line combination stability
        home_line_stability = await self.get_line_combination_stability(db, home_id)
        away_line_stability = await self.get_line_combination_stability(db, away_id)

        # Feature #11: Recency-weighted H2H
        h2h_weighted = await self.get_recency_weighted_h2h(db, home_id, away_id)

        # Feature #13: Consensus line aggregation
        consensus_line = await self.get_consensus_line(db, game.id)

        # Line movement features (opening vs current odds)
        line_movement = await self.get_line_movement(db, game.id, game)

        # Divisional matchup detection (affects total scoring tendencies)
        is_divisional = (
            self._is_same_division(home_team, away_team)
            if home_team and away_team
            else False
        )

        # Conference mismatch for travel (west vs east)
        is_cross_conference = (
            self._is_cross_conference(home_team, away_team)
            if home_team and away_team
            else False
        )

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
            # Betting odds (from The Odds API, American format)
            "odds": {
                "home_moneyline": getattr(game, "home_moneyline", None),
                "away_moneyline": getattr(game, "away_moneyline", None),
                "over_under_line": getattr(game, "over_under_line", None),
                "home_spread_line": getattr(game, "home_spread_line", None),
                "away_spread_line": getattr(game, "away_spread_line", None),
                "home_spread_price": getattr(game, "home_spread_price", None),
                "away_spread_price": getattr(game, "away_spread_price", None),
                "over_price": getattr(game, "over_price", None),
                "under_price": getattr(game, "under_price", None),
                "all_total_lines": getattr(game, "all_total_lines", None) or [],
                "all_spread_lines": getattr(game, "all_spread_lines", None) or [],
                # 1st period odds (from Game model columns)
                "p1_over_price": getattr(game, "period1_over_price", None),
                "p1_under_price": getattr(game, "period1_under_price", None),
                "p1_total_line": getattr(game, "period1_total_line", None),
                "p1_home_price": getattr(game, "period1_home_ml", None),
                "p1_away_price": getattr(game, "period1_away_ml", None),
                "p1_draw_price": getattr(game, "period1_draw_price", None),
                "p1_spread_line": getattr(game, "period1_spread_line", None),
                "p1_home_spread_price": getattr(game, "period1_home_spread_price", None),
                "p1_away_spread_price": getattr(game, "period1_away_spread_price", None),
                # Other prop odds (keys match what each prop type expects)
                "btts_yes_price": getattr(game, "btts_yes_price", None),
                "btts_no_price": getattr(game, "btts_no_price", None),
                "first_goal_home_price": getattr(game, "first_goal_home_price", None),
                "first_goal_away_price": getattr(game, "first_goal_away_price", None),
                "ot_yes_price": getattr(game, "overtime_yes_price", None),
                "ot_no_price": getattr(game, "overtime_no_price", None),
                "reg_home_price": getattr(game, "reg_home_price", None),
                "reg_away_price": getattr(game, "reg_away_price", None),
                "reg_draw_price": getattr(game, "reg_draw_price", None),
            },
            # Home team features
            "home_form_5": home_form_5,
            "home_form_10": home_form_10,
            "home_season": home_season,
            "home_splits": home_splits,
            "home_goalie": home_goalie,
            "home_periods": home_periods,
            "home_ot": home_ot,
            # Away team features
            "away_form_5": away_form_5,
            "away_form_10": away_form_10,
            "away_season": away_season,
            "away_splits": away_splits,
            "away_goalie": away_goalie,
            "away_periods": away_periods,
            "away_ot": away_ot,
            # Player talent and lineup
            "home_skaters": home_skaters,
            "away_skaters": away_skaters,
            "home_lineup": home_lineup,
            "away_lineup": away_lineup,
            # Head-to-head
            "h2h": h2h,
            # Injury impact
            "home_injuries": home_injuries,
            "away_injuries": away_injuries,
            # Schedule context
            "home_schedule": home_schedule,
            "away_schedule": away_schedule,
            # Special teams
            "home_special_teams": home_special,
            "away_special_teams": away_special,
            # Advanced metrics
            "home_advanced": home_advanced,
            "away_advanced": away_advanced,
            # 5v5 even-strength possession (MoneyPuck)
            "home_ev_possession": home_ev_possession,
            "away_ev_possession": away_ev_possession,
            # Close-game possession
            "home_close_possession": home_close_possession,
            "away_close_possession": away_close_possession,
            # Starter confidence
            "home_starter_status": home_starter_status,
            "away_starter_status": away_starter_status,
            # Goalie vs. specific opponent history
            "home_goalie_vs_team": home_goalie_vs_team,
            "away_goalie_vs_team": away_goalie_vs_team,
            # Goalie venue splits (home/away performance)
            "home_goalie_venue": home_goalie_venue,
            "away_goalie_venue": away_goalie_venue,
            # Goalie workload (shot-based fatigue)
            "home_goalie_workload": home_goalie_workload,
            "away_goalie_workload": away_goalie_workload,
            # Pace / tempo
            "home_pace": home_pace,
            "away_pace": away_pace,
            # Score-close stats
            "home_score_close": home_score_close,
            "away_score_close": away_score_close,
            # Player matchups (how key players perform vs this opponent)
            "home_player_matchup": home_player_matchup,
            "away_player_matchup": away_player_matchup,
            # Team matchup profile (scoring tendencies between these teams)
            "team_matchup": team_matchup,
            # Penalty discipline
            "home_discipline": home_discipline,
            "away_discipline": away_discipline,
            # Close-game record (clutch + scoring first)
            "home_close_record": home_close_record,
            "away_close_record": away_close_record,
            # Schedule spot / situational awareness
            "is_divisional": is_divisional,
            "is_cross_conference": is_cross_conference,
            # Line movement (opening vs current odds)
            "line_movement": line_movement,
            # Feature #6: PP opportunity rate vs opponent
            "home_pp_opportunity": home_pp_opp,
            "away_pp_opportunity": away_pp_opp,
            # Feature #7: Shooting quality against (HDSV% proxy)
            "home_shot_quality": home_shot_quality,
            "away_shot_quality": away_shot_quality,
            # Feature #9: Line combination stability
            "home_line_stability": home_line_stability,
            "away_line_stability": away_line_stability,
            # Feature #11: Recency-weighted H2H
            "h2h_weighted": h2h_weighted,
            # Feature #13: Consensus line aggregation
            "consensus_line": consensus_line,
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
    def _get_opponent_id(game: Game, team_id: int) -> int:
        """Return the opponent team ID for a given team in a game."""
        return game.away_team_id if game.home_team_id == team_id else game.home_team_id

    @staticmethod
    def _is_same_division(team_a: Team, team_b: Team) -> bool:
        """Check if two teams are in the same division."""
        return bool(
            team_a.division
            and team_b.division
            and team_a.division == team_b.division
        )

    @staticmethod
    def _is_cross_conference(team_a: Team, team_b: Team) -> bool:
        """Check if two teams are in different conferences."""
        return bool(
            team_a.conference
            and team_b.conference
            and team_a.conference != team_b.conference
        )

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

    @staticmethod
    def _empty_skater_impact() -> Dict[str, Any]:
        """Return default skater impact features when no data is available."""
        return {
            "top6_fwd_ppg": 0.0,
            "top4_def_ppg": 0.0,
            "star_ppg": 0.0,
            "team_skater_ppg": 0.0,
            "games_found": 0,
        }

    @staticmethod
    def _empty_lineup_status() -> Dict[str, Any]:
        """Return default lineup status when no data is available."""
        return {
            "regulars_count": 0,
            "missing_count": 0,
            "missing_points_per_game": 0.0,
            "missing_goals_per_game": 0.0,
            "lineup_strength": 1.0,
            "total_regular_ppg": 0.0,
        }

    @staticmethod
    def _empty_injury_impact() -> Dict[str, Any]:
        """Return default injury impact when no injuries are known."""
        return {
            "xg_reduction": 0.0,
            "total_missing_ppg": 0.0,
            "total_missing_gpg": 0.0,
            "injured_count": 0,
            "goalie_injured": False,
            "injured_players": [],
        }
