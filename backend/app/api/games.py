"""
Games API routes.

Provides endpoints for retrieving detailed game information, including
team form, head-to-head records, goalie stats, predictions, and computed
analytical features for a specific game.
"""

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.constants import GAME_FINAL_STATUSES, composite_pick_score, is_heavy_juice
from app.database import get_session
from app.models.game import Game, GameGoalieStats, HeadToHead
from app.models.injury import InjuryReport
from app.models.odds_history import OddsSnapshot
from app.models.player import GoalieStats, Player
from app.models.prediction import Prediction
from app.models.team import Team, TeamStats
from app.utils import serialize_utc_datetime

router = APIRouter(prefix="/api/games", tags=["games"])


# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------

class TeamForm(BaseModel):
    """Recent form / record information for a team."""

    team_id: int
    team_name: str
    abbreviation: str
    logo_url: Optional[str] = None
    wins: int = 0
    losses: int = 0
    ot_losses: int = 0
    points: int = 0
    games_played: int = 0
    points_pct: Optional[float] = None
    goal_diff: Optional[int] = None
    record_last_5: Optional[str] = None
    record_last_10: Optional[str] = None
    record_last_20: Optional[str] = None
    home_record: Optional[str] = None
    away_record: Optional[str] = None
    goals_for_per_game: Optional[float] = None
    goals_against_per_game: Optional[float] = None
    power_play_pct: Optional[float] = None
    penalty_kill_pct: Optional[float] = None
    shots_for_per_game: Optional[float] = None
    shots_against_per_game: Optional[float] = None
    faceoff_win_pct: Optional[float] = None
    division_rank: Optional[int] = None
    division_name: Optional[str] = None
    division_size: Optional[int] = None

    model_config = {"from_attributes": True}


class HeadToHeadRecord(BaseModel):
    """Head-to-head summary between two teams."""

    team1_id: int
    team2_id: int
    season: str
    games_played: int = 0
    team1_wins: int = 0
    team2_wins: int = 0
    draws: int = 0
    team1_goals: int = 0
    team2_goals: int = 0
    last_meeting: Optional[date] = None

    model_config = {"from_attributes": True}


class GoalieInfo(BaseModel):
    """Goalie information and season stats."""

    player_id: int
    name: str
    team_id: Optional[int] = None

    games_played: int = 0
    games_started: int = 0
    wins: int = 0
    losses: int = 0
    ot_losses: int = 0
    save_pct: Optional[float] = None
    gaa: Optional[float] = None
    shutouts: int = 0

    model_config = {"from_attributes": True}


class PeriodScoring(BaseModel):
    """Period-by-period scoring averages for a team."""

    period_1_avg: Optional[float] = None
    period_2_avg: Optional[float] = None
    period_3_avg: Optional[float] = None


class GamePredictionBrief(BaseModel):
    """Compact prediction info embedded in game details."""

    id: int
    bet_type: Optional[str] = None
    prediction_value: Optional[str] = None
    confidence: Optional[float] = None
    edge: Optional[float] = None
    recommended: bool = False
    best_bet: bool = False
    is_fallback: bool = False
    reasoning: Optional[str] = None
    created_at: Optional[str] = None

    model_config = {"from_attributes": True}


class RecentGameResult(BaseModel):
    """A single recent game result for a team."""

    game_date: date
    opponent_abbrev: str
    opponent_name: str
    home_away: str  # "home" or "away"
    goals_for: int = 0
    goals_against: int = 0
    result: str  # "W", "L", "OTL"
    score_display: str = ""  # e.g. "4-2"
    overtime: bool = False


class OddsInfo(BaseModel):
    """Current betting odds for a game."""

    home_moneyline: Optional[float] = None
    away_moneyline: Optional[float] = None
    over_under_line: Optional[float] = None
    home_spread_line: Optional[float] = None
    away_spread_line: Optional[float] = None
    home_spread_price: Optional[float] = None
    away_spread_price: Optional[float] = None
    over_price: Optional[float] = None
    under_price: Optional[float] = None
    odds_updated_at: Optional[str] = None


class GamePropsInfo(BaseModel):
    """Game-level prop odds (BTTS, regulation winner, period markets)."""

    # Both Teams to Score
    btts_yes_price: Optional[float] = None
    btts_no_price: Optional[float] = None
    # Regulation winner (3-way)
    reg_home_price: Optional[float] = None
    reg_away_price: Optional[float] = None
    reg_draw_price: Optional[float] = None
    # 1st period
    period1_home_ml: Optional[float] = None
    period1_away_ml: Optional[float] = None
    period1_draw_price: Optional[float] = None
    period1_spread_line: Optional[float] = None
    period1_home_spread_price: Optional[float] = None
    period1_away_spread_price: Optional[float] = None
    period1_total_line: Optional[float] = None
    period1_over_price: Optional[float] = None
    period1_under_price: Optional[float] = None


class GameDetailResponse(BaseModel):
    """Full game details response with analytics context."""

    id: int
    external_id: str
    sport: str = "nhl"
    game_date: date
    start_time: Optional[str] = None
    venue: Optional[str] = None
    status: str
    game_type: str
    season: str

    home_score: Optional[int] = None
    away_score: Optional[int] = None
    total_goals: Optional[int] = None
    overtime: bool = False
    shootout: bool = False
    period_scores: Optional[Dict[str, Any]] = None

    # Live game info
    period: Optional[int] = None
    period_type: Optional[str] = None  # REG, OT, SO
    clock: Optional[str] = None  # e.g. "12:34"
    clock_running: Optional[bool] = None
    in_intermission: Optional[bool] = None
    home_shots: Optional[int] = None
    away_shots: Optional[int] = None

    odds: Optional[OddsInfo] = None
    pregame_odds: Optional[OddsInfo] = None
    game_props: Optional[GamePropsInfo] = None

    home_team_form: TeamForm
    away_team_form: TeamForm

    home_recent_games: List[RecentGameResult] = []
    away_recent_games: List[RecentGameResult] = []

    head_to_head: Optional[HeadToHeadRecord] = None
    h2h_games: List[RecentGameResult] = []

    home_period_scoring: PeriodScoring
    away_period_scoring: PeriodScoring

    home_goalies: List[GoalieInfo] = []
    away_goalies: List[GoalieInfo] = []

    predictions: List[GamePredictionBrief] = []
    top_pick: Optional[Dict[str, Any]] = None

    league_averages: Optional[Dict[str, Any]] = None


class PredictionResponse(BaseModel):
    """Prediction list response for a game."""

    game_id: int
    predictions: List[GamePredictionBrief]


class FeatureResponse(BaseModel):
    """Computed analytical features for a game."""

    game_id: int
    features: Dict[str, Any]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_game_or_404(game_id: int, session: AsyncSession) -> Game:
    """Load a Game by its primary key or raise 404."""
    result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.id == game_id)
    )
    game = result.scalar_one_or_none()
    if game is None:
        raise HTTPException(status_code=404, detail=f"Game with id {game_id} not found.")
    return game


async def _get_team_form(team: Team, session: AsyncSession) -> TeamForm:
    """Build a TeamForm from the latest TeamStats row for the given team."""
    result = await session.execute(
        select(TeamStats)
        .where(TeamStats.team_id == team.id)
        .order_by(TeamStats.season.desc())
        .limit(1)
    )
    stats: Optional[TeamStats] = result.scalar_one_or_none()

    form = TeamForm(
        team_id=team.id,
        team_name=team.name,
        abbreviation=team.abbreviation,
        logo_url=team.logo_url,
    )
    if stats:
        form.wins = stats.wins
        form.losses = stats.losses
        form.ot_losses = stats.ot_losses
        form.points = stats.points
        form.games_played = stats.games_played
        gd = stats.goals_for - stats.goals_against if stats.goals_for and stats.goals_against else None
        form.goal_diff = gd
        total_possible = stats.games_played * 2 if stats.games_played else 0
        form.points_pct = round(stats.points / total_possible, 3) if total_possible > 0 else None
        form.record_last_5 = stats.record_last_5
        form.record_last_10 = stats.record_last_10
        form.record_last_20 = stats.record_last_20
        form.home_record = stats.home_record
        form.away_record = stats.away_record
        form.goals_for_per_game = stats.goals_for_per_game
        form.goals_against_per_game = stats.goals_against_per_game
        form.power_play_pct = stats.power_play_pct
        form.penalty_kill_pct = stats.penalty_kill_pct
        form.shots_for_per_game = stats.shots_for_per_game
        form.shots_against_per_game = stats.shots_against_per_game
        form.faceoff_win_pct = stats.faceoff_win_pct
        form.division_rank = stats.division_rank
        form.division_name = team.division
        if team.division:
            div_count = await session.execute(
                select(func.count(Team.id)).where(
                    Team.division == team.division,
                    Team.active.is_(True),
                )
            )
            form.division_size = div_count.scalar() or 0

    # Fallback: compute shots per game from Game records if still missing
    if form.shots_for_per_game is None or form.shots_against_per_game is None:
        shot_result = await session.execute(
            select(Game)
            .where(
                or_(Game.home_team_id == team.id, Game.away_team_id == team.id),
                func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                Game.home_shots.isnot(None),
                Game.away_shots.isnot(None),
            )
            .order_by(Game.date.desc())
            .limit(30)
        )
        recent_games = shot_result.scalars().all()
        if recent_games:
            shots_for_total = 0
            shots_against_total = 0
            for g in recent_games:
                if g.home_team_id == team.id:
                    shots_for_total += g.home_shots
                    shots_against_total += g.away_shots
                else:
                    shots_for_total += g.away_shots
                    shots_against_total += g.home_shots
            n = len(recent_games)
            if form.shots_for_per_game is None:
                form.shots_for_per_game = round(shots_for_total / n, 1)
            if form.shots_against_per_game is None:
                form.shots_against_per_game = round(shots_against_total / n, 1)

    return form


async def _get_recent_games(
    team_id: int, session: AsyncSession, limit: int = 10
) -> List[RecentGameResult]:
    """Return the last N completed games for a team, most recent first."""
    result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(
            or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
            func.lower(Game.status).in_(GAME_FINAL_STATUSES),
            Game.home_score.isnot(None),
        )
        .order_by(Game.date.desc())
        .limit(limit)
    )
    games = result.scalars().all()

    results: List[RecentGameResult] = []
    for game in games:
        is_home = game.home_team_id == team_id
        gf = game.home_score if is_home else game.away_score
        ga = game.away_score if is_home else game.home_score
        opponent = game.away_team if is_home else game.home_team
        won = gf > ga
        ot = bool(game.went_to_overtime)
        if won:
            res = "W"
        elif ot:
            res = "OTL"
        else:
            res = "L"

        results.append(RecentGameResult(
            game_date=game.date,
            opponent_abbrev=opponent.abbreviation if opponent else "???",
            opponent_name=opponent.name if opponent else "Unknown",
            home_away="home" if is_home else "away",
            goals_for=gf or 0,
            goals_against=ga or 0,
            result=res,
            score_display=f"{gf}-{ga}",
            overtime=ot,
        ))
    return results


async def _get_league_context(
    session: AsyncSession, season: str, home_team_id: int, away_team_id: int,
) -> Dict[str, Any]:
    """Compute league averages and per-team ranks for key stats."""
    result = await session.execute(
        select(TeamStats).where(
            TeamStats.season == season,
            TeamStats.games_played > 0,
        )
    )
    all_stats = result.scalars().all()
    if not all_stats:
        return {}

    stat_keys = [
        "goals_for_per_game", "goals_against_per_game",
        "power_play_pct", "penalty_kill_pct",
        "shots_for_per_game", "shots_against_per_game",
        "faceoff_win_pct",
    ]
    # Stats where lower is better (rank 1 = lowest)
    lower_is_better = {"goals_against_per_game", "shots_against_per_game"}

    def _avg(attr):
        vals = [getattr(s, attr) for s in all_stats if getattr(s, attr) is not None]
        return round(sum(vals) / len(vals), 2) if vals else None

    averages = {k: _avg(k) for k in stat_keys}

    # Compute ranks for both teams
    def _rank(attr, team_id):
        vals = [(getattr(s, attr), s.team_id) for s in all_stats if getattr(s, attr) is not None]
        if not vals:
            return None
        reverse = attr not in lower_is_better  # higher is better = descending sort
        vals.sort(key=lambda x: x[0], reverse=reverse)
        for i, (_, tid) in enumerate(vals, 1):
            if tid == team_id:
                return i
        return None

    home_ranks = {k: _rank(k, home_team_id) for k in stat_keys}
    away_ranks = {k: _rank(k, away_team_id) for k in stat_keys}

    return {
        **averages,
        "home_ranks": home_ranks,
        "away_ranks": away_ranks,
        "total_teams": len(all_stats),
    }


async def _get_head_to_head(
    team1_id: int, team2_id: int, session: AsyncSession
) -> Optional[HeadToHeadRecord]:
    """Compute H2H from ALL completed regular-season Game records.

    Includes data across all seasons in the database, not just the
    current season, so the user can see the full historical matchup.
    """
    lo, hi = sorted([team1_id, team2_id])

    result = await session.execute(
        select(Game)
        .where(
            or_(
                and_(Game.home_team_id == lo, Game.away_team_id == hi),
                and_(Game.home_team_id == hi, Game.away_team_id == lo),
            ),
            func.lower(Game.status).in_(GAME_FINAL_STATUSES),
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
            # NHL API stores game_type as "2" for regular season
            Game.game_type.in_(("2", "regular")),
        )
        .order_by(Game.date.desc())
    )
    games = result.scalars().all()

    if not games:
        # Fall back to the HeadToHead table if no Game records found
        h2h_result = await session.execute(
            select(HeadToHead)
            .where(HeadToHead.team1_id == lo, HeadToHead.team2_id == hi)
            .order_by(HeadToHead.season.desc())
        )
        records = h2h_result.scalars().all()
        if not records:
            return None

        total_gp = sum(r.games_played for r in records)
        total_t1w = sum(r.team1_wins for r in records)
        total_t2w = sum(r.team2_wins for r in records)
        total_t1g = sum(r.team1_goals for r in records)
        total_t2g = sum(r.team2_goals for r in records)
        last_meeting = records[0].last_meeting_date

        return HeadToHeadRecord(
            team1_id=lo, team2_id=hi, season="All Time",
            games_played=total_gp, team1_wins=total_t1w,
            team2_wins=total_t2w, draws=0,
            team1_goals=total_t1g, team2_goals=total_t2g,
            last_meeting=last_meeting,
        )

    # Compute directly from Game records
    total_gp = len(games)
    t1_wins = 0
    t2_wins = 0
    t1_goals = 0
    t2_goals = 0

    for game in games:
        if game.home_team_id == lo:
            g1, g2 = game.home_score, game.away_score
        else:
            g1, g2 = game.away_score, game.home_score
        t1_goals += g1
        t2_goals += g2
        if g1 > g2:
            t1_wins += 1
        elif g2 > g1:
            t2_wins += 1

    return HeadToHeadRecord(
        team1_id=lo,
        team2_id=hi,
        season="All Time",
        games_played=total_gp,
        team1_wins=t1_wins,
        team2_wins=t2_wins,
        draws=0,
        team1_goals=t1_goals,
        team2_goals=t2_goals,
        last_meeting=games[0].date,
    )


async def _get_h2h_games(
    team1_id: int, team2_id: int, session: AsyncSession, limit: int = 10
) -> List[RecentGameResult]:
    """Return individual H2H game records between two teams."""
    lo, hi = sorted([team1_id, team2_id])
    result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(
            or_(
                and_(Game.home_team_id == lo, Game.away_team_id == hi),
                and_(Game.home_team_id == hi, Game.away_team_id == lo),
            ),
            func.lower(Game.status).in_(GAME_FINAL_STATUSES),
            Game.game_type.in_(("2", "regular")),
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
        )
        .order_by(Game.date.desc())
        .limit(limit)
    )
    games = result.scalars().all()

    records = []
    for game in games:
        # Always from team1 (home team) perspective
        is_t1_home = game.home_team_id == team1_id
        gf = game.home_score if is_t1_home else game.away_score
        ga = game.away_score if is_t1_home else game.home_score
        opponent = game.away_team if is_t1_home else game.home_team
        won = gf > ga
        ot = bool(game.went_to_overtime)
        res = "W" if won else ("OTL" if ot else "L")

        records.append(RecentGameResult(
            game_date=game.date,
            opponent_abbrev=opponent.abbreviation if opponent else "???",
            opponent_name=opponent.name if opponent else "Unknown",
            home_away="home" if is_t1_home else "away",
            goals_for=gf or 0,
            goals_against=ga or 0,
            result=res,
            overtime=ot,
            score_display=f"{gf}-{ga}" if gf is not None else None,
        ))

    return records


async def _get_team_goalies(
    team_id: int, session: AsyncSession
) -> List[GoalieInfo]:
    """Get goalies for a team with season stats aggregated from game logs."""
    result = await session.execute(
        select(Player).where(
            Player.team_id == team_id,
            Player.position == "G",
            Player.active.is_(True),
        )
    )
    goalies = result.scalars().all()

    goalie_infos: List[GoalieInfo] = []
    for goalie in goalies:
        # Aggregate season stats from per-game records (GameGoalieStats)
        # since the GoalieStats table isn't populated.
        agg_result = await session.execute(
            select(
                func.count(GameGoalieStats.id).label("gp"),
                func.sum(case(
                    (GameGoalieStats.decision == "W", 1), else_=0
                )).label("wins"),
                func.sum(case(
                    (GameGoalieStats.decision == "L", 1), else_=0
                )).label("losses"),
                func.sum(case(
                    (GameGoalieStats.decision == "OTL", 1), else_=0
                )).label("ot_losses"),
                func.sum(GameGoalieStats.saves).label("total_saves"),
                func.sum(GameGoalieStats.shots_against).label("total_shots"),
                func.sum(GameGoalieStats.goals_against).label("total_ga"),
                func.sum(GameGoalieStats.toi).label("total_toi"),
                func.sum(case(
                    (and_(GameGoalieStats.decision == "W",
                          GameGoalieStats.goals_against == 0), 1),
                    else_=0,
                )).label("shutouts"),
            )
            .join(Game, GameGoalieStats.game_id == Game.id)
            .where(
                GameGoalieStats.player_id == goalie.id,
                func.lower(Game.status).in_(GAME_FINAL_STATUSES),
            )
        )
        row = agg_result.one()

        gp = row.gp or 0
        total_saves = row.total_saves or 0
        total_shots = row.total_shots or 0
        total_ga = row.total_ga or 0
        total_toi = row.total_toi or 0

        info = GoalieInfo(
            player_id=goalie.id,
            name=goalie.name,
            team_id=goalie.team_id,
            games_played=gp,
            games_started=gp,  # GameGoalieStats only has starters
            wins=row.wins or 0,
            losses=row.losses or 0,
            ot_losses=row.ot_losses or 0,
            save_pct=round(total_saves / total_shots, 3) if total_shots > 0 else None,
            gaa=round((total_ga / total_toi) * 60, 2) if total_toi > 0 else None,
            shutouts=row.shutouts or 0,
        )
        goalie_infos.append(info)

    return goalie_infos


async def _compute_period_scoring(
    team_id: int, session: AsyncSession
) -> PeriodScoring:
    """
    Compute average period-by-period scoring for a team from completed games.

    Tries three approaches in order:
      1. Games with detailed per-period scores (from boxscore sync).
      2. Games with total scores (from historical sync) — estimates period
         breakdown using the typical NHL 32/33/35% distribution.
      3. TeamStats ``goals_for_per_game`` as a final fallback.
    """
    # ---- Approach 1: per-period scores from boxscore data ----
    result = await session.execute(
        select(Game).where(
            or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
            func.lower(Game.status).in_(GAME_FINAL_STATUSES),
            Game.home_score_p1.isnot(None),
        )
        .order_by(Game.date.desc())
        .limit(20)
    )
    games = result.scalars().all()

    if games:
        period_totals = [0.0, 0.0, 0.0]
        counted = 0
        for game in games:
            is_home = game.home_team_id == team_id
            p1 = (game.home_score_p1 if is_home else game.away_score_p1) or 0
            p2 = (game.home_score_p2 if is_home else game.away_score_p2) or 0
            p3 = (game.home_score_p3 if is_home else game.away_score_p3) or 0
            period_totals[0] += p1
            period_totals[1] += p2
            period_totals[2] += p3
            counted += 1

        if counted > 0:
            return PeriodScoring(
                period_1_avg=round(period_totals[0] / counted, 2),
                period_2_avg=round(period_totals[1] / counted, 2),
                period_3_avg=round(period_totals[2] / counted, 2),
            )

    # ---- Approach 2: estimate from total game scores ----
    result = await session.execute(
        select(Game).where(
            or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
            func.lower(Game.status).in_(GAME_FINAL_STATUSES),
            Game.home_score.isnot(None),
        )
        .order_by(Game.date.desc())
        .limit(20)
    )
    games_total = result.scalars().all()

    if games_total:
        total_goals = 0.0
        counted = 0
        for game in games_total:
            is_home = game.home_team_id == team_id
            goals = (game.home_score if is_home else game.away_score) or 0
            total_goals += goals
            counted += 1

        if counted > 0:
            avg_per_game = total_goals / counted
            # NHL scoring is roughly evenly distributed across periods
            # with a slight uptick in the 3rd period
            return PeriodScoring(
                period_1_avg=round(avg_per_game * 0.32, 2),
                period_2_avg=round(avg_per_game * 0.33, 2),
                period_3_avg=round(avg_per_game * 0.35, 2),
            )

    # ---- Approach 3: fall back to TeamStats goals_for_per_game ----
    ts_result = await session.execute(
        select(TeamStats)
        .where(TeamStats.team_id == team_id)
        .order_by(TeamStats.season.desc())
        .limit(1)
    )
    ts = ts_result.scalar_one_or_none()
    if ts and ts.goals_for_per_game:
        gf = ts.goals_for_per_game
        return PeriodScoring(
            period_1_avg=round(gf * 0.32, 2),
            period_2_avg=round(gf * 0.33, 2),
            period_3_avg=round(gf * 0.35, 2),
        )

    return PeriodScoring()


from app.services.odds import fresh_implied_prob as _fresh_implied_for_pred  # noqa: E402


async def _get_game_predictions(
    game_id: int, session: AsyncSession, game: Optional[Game] = None
) -> List[GamePredictionBrief]:
    """Return predictions for a game, sorted: recommended first, then fallback.

    A prediction is "fallback" when it has real edge (>= min_edge) and
    confidence (>= min_confidence) but sits on a heavy-juice line
    (implied_prob >= best_bet_max_implied).  The juice check uses the
    Game's current odds (not the potentially stale Prediction record)
    to avoid false Heavy Juice labels after line movement.
    """
    result = await session.execute(
        select(Prediction).where(Prediction.game_id == game_id)
    )
    preds = result.scalars().all()

    max_implied = settings.best_bet_max_implied
    min_edge = settings.min_edge
    min_conf = settings.min_confidence

    briefs: List[GamePredictionBrief] = []
    # Map brief id → implied_prob for composite scoring.
    # Use fresh Game odds when available, fall back to stored value.
    implied_map: dict[int, float | None] = {}
    for p in preds:
        fresh = _fresh_implied_for_pred(p, game)
        cur_impl = fresh if fresh is not None else p.odds_implied_prob
        implied_map[p.id] = cur_impl

        # Fallback = has real edge but heavy juice (above implied ceiling).
        # Uses fresh odds when available, otherwise stored implied prob.
        is_fb = (
            p.edge is not None
            and p.edge >= min_edge
            and (p.confidence or 0) >= min_conf
            and is_heavy_juice(cur_impl, max_implied)
        )
        # A pick that meets edge/confidence thresholds AND has acceptable
        # juice should be treated as recommended regardless of stale flag.
        # Always enforce the juice ceiling using fresh odds.
        _meets_thresholds = (
            p.recommended
            or (
                (p.confidence or 0) >= min_conf
                and (p.edge or 0) >= min_edge
            )
        )
        effectively_recommended = (
            _meets_thresholds
            and not is_heavy_juice(cur_impl, max_implied)
        )
        briefs.append(
            GamePredictionBrief(
                id=p.id,
                bet_type=p.bet_type,
                prediction_value=p.prediction_value,
                confidence=p.confidence,
                edge=p.edge,
                recommended=effectively_recommended,
                best_bet=p.best_bet,
                is_fallback=is_fb,
                reasoning=p.reasoning,
                created_at=str(p.created_at) if p.created_at else None,
            )
        )

    # Sort: recommended first (by composite score), then fallback, then rest
    def sort_key(b: GamePredictionBrief):
        tier = 0 if b.recommended else (1 if b.is_fallback else 2)
        score = composite_pick_score(b.confidence, b.edge, implied_map.get(b.id))
        return (tier, -score)

    briefs.sort(key=sort_key)
    return briefs


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

@router.get(
    "/{game_id}",
    response_model=GameDetailResponse,
    summary="Get full game details",
)
async def get_game_details(
    game_id: int,
    session: AsyncSession = Depends(get_session),
):
    """
    Return comprehensive details for a single game.

    Includes both teams' recent form (last-5, last-10 records), head-to-head
    record, period-by-period scoring averages, goalie season stats, and any
    model predictions that have been generated.
    """
    game = await _get_game_or_404(game_id, session)

    # Sync scores/clock from NHL API for live games
    # Odds syncing is handled by the background scheduler (app.live)
    if game.status and game.status.lower() in ("in_progress", "live"):
        try:
            from app.scrapers.nhl_api import NHLScraper

            scraper = NHLScraper()
            await scraper.sync_schedule(session, str(game.date))
            await session.flush()
            game = await _get_game_or_404(game_id, session)
        except Exception as exc:
            logger.warning("Game %s live schedule sync failed: %s", game_id, exc)

    # Gather all analytics data
    home_form = await _get_team_form(game.home_team, session)
    away_form = await _get_team_form(game.away_team, session)
    league_avgs = await _get_league_context(session, game.season, game.home_team_id, game.away_team_id)
    h2h = await _get_head_to_head(game.home_team_id, game.away_team_id, session)
    h2h_game_records = await _get_h2h_games(game.home_team_id, game.away_team_id, session)
    home_period = await _compute_period_scoring(game.home_team_id, session)
    away_period = await _compute_period_scoring(game.away_team_id, session)
    home_goalies = await _get_team_goalies(game.home_team_id, session)
    away_goalies = await _get_team_goalies(game.away_team_id, session)
    predictions = await _get_game_predictions(game.id, session, game=game)

    # Compute the same top_pick the dashboard uses so the game detail
    # page always agrees with the schedule card on which bet to show.
    from app.api.schedule import _compute_top_picks
    top_picks_map = await _compute_top_picks([game], session)
    top_pick_obj = top_picks_map.get(game.id)
    top_pick_dict = top_pick_obj.model_dump() if top_pick_obj else None

    home_recent = await _get_recent_games(game.home_team_id, session, limit=50)
    away_recent = await _get_recent_games(game.away_team_id, session, limit=50)

    # Build period scores from individual columns
    parsed_period_scores = None
    if game.home_score_p1 is not None:
        parsed_period_scores = {
            "home": [game.home_score_p1 or 0, game.home_score_p2 or 0, game.home_score_p3 or 0],
            "away": [game.away_score_p1 or 0, game.away_score_p2 or 0, game.away_score_p3 or 0],
        }
        if game.home_score_ot is not None:
            parsed_period_scores["home"].append(game.home_score_ot)
            parsed_period_scores["away"].append(game.away_score_ot or 0)

    total_goals = None
    if game.home_score is not None and game.away_score is not None:
        total_goals = game.home_score + game.away_score

    # Build odds info from Game model fields (populated by OddsScraper)
    odds_info = None
    if any([game.home_moneyline, game.away_moneyline, game.over_under_line, game.home_spread_line]):
        odds_info = OddsInfo(
            home_moneyline=game.home_moneyline,
            away_moneyline=game.away_moneyline,
            over_under_line=game.over_under_line,
            home_spread_line=game.home_spread_line,
            away_spread_line=game.away_spread_line,
            home_spread_price=game.home_spread_price,
            away_spread_price=game.away_spread_price,
            over_price=game.over_price,
            under_price=game.under_price,
            odds_updated_at=serialize_utc_datetime(game.odds_updated_at),
        )

    # Build pregame odds snapshot (only populated once game goes live)
    pregame_odds_info = None
    if any([game.pregame_home_moneyline, game.pregame_away_moneyline,
            game.pregame_over_under_line, game.pregame_home_spread_line]):
        pregame_odds_info = OddsInfo(
            home_moneyline=game.pregame_home_moneyline,
            away_moneyline=game.pregame_away_moneyline,
            over_under_line=game.pregame_over_under_line,
            home_spread_line=game.pregame_home_spread_line,
            away_spread_line=game.pregame_away_spread_line,
            home_spread_price=game.pregame_home_spread_price,
            away_spread_price=game.pregame_away_spread_price,
            over_price=game.pregame_over_price,
            under_price=game.pregame_under_price,
        )

    # Build game props info
    game_props_info = None
    has_any_game_prop = any([
        game.btts_yes_price, game.btts_no_price,
        game.reg_home_price, game.reg_away_price, game.reg_draw_price,
        game.period1_home_ml, game.period1_away_ml,
        game.period1_total_line,
    ])
    if has_any_game_prop:
        game_props_info = GamePropsInfo(
            btts_yes_price=game.btts_yes_price,
            btts_no_price=game.btts_no_price,
            reg_home_price=game.reg_home_price,
            reg_away_price=game.reg_away_price,
            reg_draw_price=game.reg_draw_price,
            period1_home_ml=game.period1_home_ml,
            period1_away_ml=game.period1_away_ml,
            period1_draw_price=game.period1_draw_price,
            period1_spread_line=game.period1_spread_line,
            period1_home_spread_price=game.period1_home_spread_price,
            period1_away_spread_price=game.period1_away_spread_price,
            period1_total_line=game.period1_total_line,
            period1_over_price=game.period1_over_price,
            period1_under_price=game.period1_under_price,
        )

    return GameDetailResponse(
        id=game.id,
        external_id=game.external_id,
        sport=game.sport,
        game_date=game.date,
        start_time=game.start_time.isoformat() if game.start_time else None,
        venue=game.venue,
        status=game.status,
        game_type=game.game_type,
        season=game.season,
        home_score=game.home_score,
        away_score=game.away_score,
        total_goals=total_goals,
        overtime=game.went_to_overtime or False,
        shootout=False,
        period_scores=parsed_period_scores,
        period=game.period,
        period_type=game.period_type,
        clock=game.clock,
        clock_running=game.clock_running,
        in_intermission=game.in_intermission,
        home_shots=game.home_shots,
        away_shots=game.away_shots,
        odds=odds_info,
        pregame_odds=pregame_odds_info,
        game_props=game_props_info,
        home_team_form=home_form,
        away_team_form=away_form,
        home_recent_games=home_recent,
        away_recent_games=away_recent,
        head_to_head=h2h,
        h2h_games=h2h_game_records,
        home_period_scoring=home_period,
        away_period_scoring=away_period,
        home_goalies=home_goalies,
        away_goalies=away_goalies,
        predictions=predictions,
        top_pick=top_pick_dict,
        league_averages=league_avgs or None,
    )


@router.get(
    "/{game_id}/predictions",
    response_model=PredictionResponse,
    summary="Get predictions for a game",
)
async def get_game_predictions(
    game_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Return all model predictions generated for the specified game."""
    # Verify the game exists
    await _get_game_or_404(game_id, session)
    predictions = await _get_game_predictions(game_id, session)
    return PredictionResponse(game_id=game_id, predictions=predictions)


@router.get(
    "/{game_id}/features",
    response_model=FeatureResponse,
    summary="Get computed features for a game",
)
async def get_game_features(
    game_id: int,
    session: AsyncSession = Depends(get_session),
):
    """
    Compute and return the analytical feature vector for a game.

    Uses the FeatureEngine to build features suitable for model input.
    Falls back to a basic feature set if the analytics module is unavailable.
    """
    game = await _get_game_or_404(game_id, session)

    # Try the full feature engine first
    try:
        from app.analytics.features import FeatureEngine

        engine = FeatureEngine()
        features = await engine.build_game_features(session, game.id)
        return FeatureResponse(game_id=game_id, features=features)
    except (ImportError, AttributeError):
        pass
    except Exception as exc:
        logger.error("Error computing features: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Error computing features",
        )

    # Fallback: construct a basic feature dict from available data
    home_form = await _get_team_form(game.home_team, session)
    away_form = await _get_team_form(game.away_team, session)
    h2h = await _get_head_to_head(game.home_team_id, game.away_team_id, session)

    features: Dict[str, Any] = {
        "game_id": game.id,
        "game_date": str(game.date),
        "home_team_id": game.home_team_id,
        "away_team_id": game.away_team_id,
        "home_wins": home_form.wins,
        "home_losses": home_form.losses,
        "home_ot_losses": home_form.ot_losses,
        "away_wins": away_form.wins,
        "away_losses": away_form.losses,
        "away_ot_losses": away_form.ot_losses,
        "home_gf_per_game": home_form.goals_for_per_game,
        "home_ga_per_game": home_form.goals_against_per_game,
        "away_gf_per_game": away_form.goals_for_per_game,
        "away_ga_per_game": away_form.goals_against_per_game,
        "home_pp_pct": home_form.power_play_pct,
        "home_pk_pct": home_form.penalty_kill_pct,
        "away_pp_pct": away_form.power_play_pct,
        "away_pk_pct": away_form.penalty_kill_pct,
        "home_shots_for_pg": home_form.shots_for_per_game,
        "home_shots_against_pg": home_form.shots_against_per_game,
        "away_shots_for_pg": away_form.shots_for_per_game,
        "away_shots_against_pg": away_form.shots_against_per_game,
        "home_record_last_5": home_form.record_last_5,
        "away_record_last_5": away_form.record_last_5,
        "home_record_last_10": home_form.record_last_10,
        "away_record_last_10": away_form.record_last_10,
    }

    if h2h:
        features["h2h_games_played"] = h2h.games_played
        features["h2h_team1_wins"] = h2h.team1_wins
        features["h2h_team2_wins"] = h2h.team2_wins
        features["h2h_team1_goals"] = h2h.team1_goals
        features["h2h_team2_goals"] = h2h.team2_goals

    return FeatureResponse(game_id=game_id, features=features)


# ---------------------------------------------------------------------------
# Line movement endpoint
# ---------------------------------------------------------------------------

class OddsSnapshotResponse(BaseModel):
    """Single point-in-time odds snapshot."""

    captured_at: str
    source: Optional[str] = None
    home_moneyline: Optional[float] = None
    away_moneyline: Optional[float] = None
    over_under_line: Optional[float] = None
    over_price: Optional[float] = None
    under_price: Optional[float] = None
    home_spread_line: Optional[float] = None
    away_spread_line: Optional[float] = None
    home_spread_price: Optional[float] = None
    away_spread_price: Optional[float] = None


class LineMovementResponse(BaseModel):
    """Line movement history for a game."""

    game_id: int
    snapshots: List[OddsSnapshotResponse]
    opening: Optional[OddsSnapshotResponse] = None
    current: Optional[OddsSnapshotResponse] = None


@router.get(
    "/{game_id}/line-movement",
    response_model=LineMovementResponse,
    summary="Get line movement history for a game",
)
async def get_line_movement(
    game_id: int,
    session: AsyncSession = Depends(get_session),
):
    """
    Return historical odds snapshots for a game, ordered chronologically.

    Includes opening and current (latest) snapshots for quick comparison.
    """
    await _get_game_or_404(game_id, session)

    result = await session.execute(
        select(OddsSnapshot)
        .where(OddsSnapshot.game_id == game_id)
        .order_by(OddsSnapshot.captured_at.asc())
    )
    snapshots = result.scalars().all()

    snap_list = [
        OddsSnapshotResponse(
            captured_at=s.captured_at.isoformat() if s.captured_at else "",
            source=s.source,
            home_moneyline=s.home_moneyline,
            away_moneyline=s.away_moneyline,
            over_under_line=s.over_under_line,
            over_price=s.over_price,
            under_price=s.under_price,
            home_spread_line=s.home_spread_line,
            away_spread_line=s.away_spread_line,
            home_spread_price=s.home_spread_price,
            away_spread_price=s.away_spread_price,
        )
        for s in snapshots
    ]

    opening = snap_list[0] if snap_list else None
    current = snap_list[-1] if snap_list else None

    return LineMovementResponse(
        game_id=game_id,
        snapshots=snap_list,
        opening=opening,
        current=current,
    )


# ---------------------------------------------------------------------------
# Injury report endpoint
# ---------------------------------------------------------------------------

class InjuryInfo(BaseModel):
    """Injury information for a single player."""

    player_name: str
    position: Optional[str] = None
    status: str
    injury_type: Optional[str] = None
    detail: Optional[str] = None
    reported_at: Optional[str] = None


class GameInjuryResponse(BaseModel):
    """Injury reports for both teams in a game."""

    game_id: int
    home_injuries: List[InjuryInfo]
    away_injuries: List[InjuryInfo]


@router.get(
    "/{game_id}/injuries",
    response_model=GameInjuryResponse,
    summary="Get injury reports for both teams in a game",
)
async def get_game_injuries(
    game_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Return active injury reports for both teams in the specified game."""
    game = await _get_game_or_404(game_id, session)

    async def _team_injuries(team_id: int) -> List[InjuryInfo]:
        result = await session.execute(
            select(InjuryReport)
            .join(Player, InjuryReport.player_id == Player.id)
            .where(
                and_(
                    InjuryReport.team_id == team_id,
                    InjuryReport.active.is_(True),
                )
            )
        )
        reports = result.scalars().all()
        infos = []
        for r in reports:
            player_result = await session.execute(
                select(Player).where(Player.id == r.player_id)
            )
            player = player_result.scalar_one_or_none()
            infos.append(InjuryInfo(
                player_name=player.name if player else "Unknown",
                position=player.position if player else None,
                status=r.status,
                injury_type=r.injury_type,
                detail=r.detail,
                reported_at=r.reported_at.isoformat() if r.reported_at else None,
            ))
        return infos

    home_inj = await _team_injuries(game.home_team_id)
    away_inj = await _team_injuries(game.away_team_id)

    return GameInjuryResponse(
        game_id=game_id,
        home_injuries=home_inj,
        away_injuries=away_inj,
    )
