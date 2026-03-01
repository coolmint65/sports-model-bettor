"""
API route definitions for the sports betting application.

Provides endpoints for:
- Health check and application status
- Teams and team statistics
- Games and schedules
- Predictions and bet results
"""

from datetime import date, datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.database import get_session
from app.models.game import Game, HeadToHead
from app.models.player import GoalieStats, Player, PlayerStats
from app.models.prediction import BetResult, Prediction
from app.models.team import Team, TeamStats

# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str
    app_name: str
    version: str
    sport: str
    timestamp: str


class TeamResponse(BaseModel):
    id: int
    external_id: str
    name: str
    abbreviation: str
    city: Optional[str] = None
    division: Optional[str] = None
    conference: Optional[str] = None
    sport: str
    logo_url: Optional[str] = None
    active: bool

    model_config = {"from_attributes": True}


class TeamStatsResponse(BaseModel):
    id: int
    team_id: int
    season: str
    games_played: int
    wins: int
    losses: int
    ot_losses: int
    points: int
    goals_for: int
    goals_against: int
    goals_for_per_game: Optional[float] = None
    goals_against_per_game: Optional[float] = None
    power_play_pct: Optional[float] = None
    penalty_kill_pct: Optional[float] = None
    shots_for_per_game: Optional[float] = None
    shots_against_per_game: Optional[float] = None
    faceoff_win_pct: Optional[float] = None
    record_last_5: Optional[str] = None
    record_last_10: Optional[str] = None
    record_last_20: Optional[str] = None
    home_record: Optional[str] = None
    away_record: Optional[str] = None

    model_config = {"from_attributes": True}


class GameResponse(BaseModel):
    id: int
    external_id: str
    sport: str
    season: str
    game_type: Optional[str] = None
    date: date
    start_time: Optional[datetime] = None
    home_team_id: int
    away_team_id: int
    venue: Optional[str] = None
    status: str
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    home_shots: Optional[int] = None
    away_shots: Optional[int] = None
    went_to_overtime: Optional[bool] = None
    winning_team_id: Optional[int] = None

    model_config = {"from_attributes": True}


class PlayerResponse(BaseModel):
    id: int
    external_id: str
    name: str
    team_id: Optional[int] = None
    position: Optional[str] = None
    jersey_number: Optional[int] = None
    shoots_catches: Optional[str] = None
    height: Optional[int] = None
    weight: Optional[int] = None
    birth_date: Optional[date] = None
    sport: str
    active: bool

    model_config = {"from_attributes": True}


class PredictionResponse(BaseModel):
    id: int
    game_id: int
    bet_type: str
    prediction_value: str
    confidence: float
    odds_implied_prob: Optional[float] = None
    edge: Optional[float] = None
    recommended: bool
    best_bet: bool
    reasoning: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class BetResultResponse(BaseModel):
    id: int
    prediction_id: int
    actual_outcome: str
    was_correct: bool
    profit_loss: float
    settled_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class StandingsEntry(BaseModel):
    team: TeamResponse
    stats: TeamStatsResponse


class HeadToHeadResponse(BaseModel):
    id: int
    team1_id: int
    team2_id: int
    season: str
    games_played: int
    team1_wins: int
    team2_wins: int
    ot_games: int
    team1_goals: int
    team2_goals: int
    last_meeting_date: Optional[date] = None
    last_meeting_winner_id: Optional[int] = None

    model_config = {"from_attributes": True}


class ModelPerformanceResponse(BaseModel):
    total_predictions: int
    settled_predictions: int
    correct_predictions: int
    accuracy: Optional[float] = None
    total_profit_loss: float
    roi: Optional[float] = None
    best_bet_accuracy: Optional[float] = None


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

health_router = APIRouter(tags=["health"])
teams_router = APIRouter(prefix="/api/teams", tags=["teams"])
games_router = APIRouter(prefix="/api/games", tags=["games"])
players_router = APIRouter(prefix="/api/players", tags=["players"])
predictions_router = APIRouter(prefix="/api/predictions", tags=["predictions"])


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@health_router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Return application health status."""
    return HealthResponse(
        status="healthy",
        app_name=settings.app_name,
        version=settings.app_version,
        sport=settings.default_sport,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------


@teams_router.get("/", response_model=List[TeamResponse])
async def list_teams(
    sport: str = Query(default="nhl", description="Sport filter"),
    active: Optional[bool] = Query(default=None, description="Active status filter"),
    session: AsyncSession = Depends(get_session),
) -> List[TeamResponse]:
    """List all teams, optionally filtered by sport and active status."""
    stmt = select(Team).where(Team.sport == sport).order_by(Team.name)
    if active is not None:
        stmt = stmt.where(Team.active == active)
    result = await session.execute(stmt)
    teams = result.scalars().all()
    return [TeamResponse.model_validate(t) for t in teams]


@teams_router.get("/{team_id}", response_model=TeamResponse)
async def get_team(
    team_id: int,
    session: AsyncSession = Depends(get_session),
) -> TeamResponse:
    """Get a single team by ID."""
    team = await session.get(Team, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")
    return TeamResponse.model_validate(team)


@teams_router.get("/{team_id}/stats", response_model=List[TeamStatsResponse])
async def get_team_stats(
    team_id: int,
    season: Optional[str] = Query(default=None, description="Season filter"),
    session: AsyncSession = Depends(get_session),
) -> List[TeamStatsResponse]:
    """Get statistics for a team, optionally filtered by season."""
    team = await session.get(Team, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")

    stmt = select(TeamStats).where(TeamStats.team_id == team_id)
    if season:
        stmt = stmt.where(TeamStats.season == season)
    stmt = stmt.order_by(TeamStats.season.desc())
    result = await session.execute(stmt)
    stats = result.scalars().all()
    return [TeamStatsResponse.model_validate(s) for s in stats]


@teams_router.get("/standings/{season}", response_model=List[StandingsEntry])
async def get_standings(
    season: str,
    sport: str = Query(default="nhl", description="Sport filter"),
    session: AsyncSession = Depends(get_session),
) -> List[StandingsEntry]:
    """Get standings for a season, ordered by points descending."""
    stmt = (
        select(TeamStats)
        .join(Team, TeamStats.team_id == Team.id)
        .where(TeamStats.season == season, Team.sport == sport)
        .options(selectinload(TeamStats.team))
        .order_by(TeamStats.points.desc(), TeamStats.wins.desc())
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()

    standings = []
    for ts in rows:
        standings.append(
            StandingsEntry(
                team=TeamResponse.model_validate(ts.team),
                stats=TeamStatsResponse.model_validate(ts),
            )
        )
    return standings


# ---------------------------------------------------------------------------
# Games
# ---------------------------------------------------------------------------


@games_router.get("/", response_model=List[GameResponse])
async def list_games(
    sport: str = Query(default="nhl", description="Sport filter"),
    season: Optional[str] = Query(default=None, description="Season filter"),
    status: Optional[str] = Query(default=None, description="Game status filter"),
    game_date: Optional[date] = Query(default=None, description="Game date filter"),
    team_id: Optional[int] = Query(default=None, description="Filter by team"),
    limit: int = Query(default=50, ge=1, le=200, description="Max results"),
    offset: int = Query(default=0, ge=0, description="Offset for pagination"),
    session: AsyncSession = Depends(get_session),
) -> List[GameResponse]:
    """List games with optional filters."""
    stmt = select(Game).where(Game.sport == sport)
    if season:
        stmt = stmt.where(Game.season == season)
    if status:
        stmt = stmt.where(Game.status == status)
    if game_date:
        stmt = stmt.where(Game.date == game_date)
    if team_id:
        stmt = stmt.where(
            (Game.home_team_id == team_id) | (Game.away_team_id == team_id)
        )
    stmt = stmt.order_by(Game.date.desc(), Game.start_time.desc())
    stmt = stmt.offset(offset).limit(limit)

    result = await session.execute(stmt)
    games = result.scalars().all()
    return [GameResponse.model_validate(g) for g in games]


@games_router.get("/{game_id}", response_model=GameResponse)
async def get_game(
    game_id: int,
    session: AsyncSession = Depends(get_session),
) -> GameResponse:
    """Get a single game by ID."""
    game = await session.get(Game, game_id)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    return GameResponse.model_validate(game)


@games_router.get("/today/", response_model=List[GameResponse])
async def get_todays_games(
    sport: str = Query(default="nhl", description="Sport filter"),
    session: AsyncSession = Depends(get_session),
) -> List[GameResponse]:
    """Get all games scheduled for today."""
    today = date.today()
    stmt = (
        select(Game)
        .where(Game.sport == sport, Game.date == today)
        .order_by(Game.start_time)
    )
    result = await session.execute(stmt)
    games = result.scalars().all()
    return [GameResponse.model_validate(g) for g in games]


@games_router.get("/head-to-head/{team1_id}/{team2_id}", response_model=List[HeadToHeadResponse])
async def get_head_to_head(
    team1_id: int,
    team2_id: int,
    season: Optional[str] = Query(default=None, description="Season filter"),
    session: AsyncSession = Depends(get_session),
) -> List[HeadToHeadResponse]:
    """Get head-to-head records between two teams."""
    # Normalize ordering so team1_id < team2_id
    t1, t2 = min(team1_id, team2_id), max(team1_id, team2_id)
    stmt = select(HeadToHead).where(
        HeadToHead.team1_id == t1, HeadToHead.team2_id == t2
    )
    if season:
        stmt = stmt.where(HeadToHead.season == season)
    stmt = stmt.order_by(HeadToHead.season.desc())

    result = await session.execute(stmt)
    records = result.scalars().all()
    return [HeadToHeadResponse.model_validate(r) for r in records]


# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------


@players_router.get("/", response_model=List[PlayerResponse])
async def list_players(
    sport: str = Query(default="nhl", description="Sport filter"),
    team_id: Optional[int] = Query(default=None, description="Team filter"),
    position: Optional[str] = Query(default=None, description="Position filter"),
    active: Optional[bool] = Query(default=None, description="Active status filter"),
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    offset: int = Query(default=0, ge=0, description="Offset for pagination"),
    session: AsyncSession = Depends(get_session),
) -> List[PlayerResponse]:
    """List players with optional filters."""
    stmt = select(Player).where(Player.sport == sport)
    if team_id is not None:
        stmt = stmt.where(Player.team_id == team_id)
    if position:
        stmt = stmt.where(Player.position == position)
    if active is not None:
        stmt = stmt.where(Player.active == active)
    stmt = stmt.order_by(Player.name).offset(offset).limit(limit)

    result = await session.execute(stmt)
    players = result.scalars().all()
    return [PlayerResponse.model_validate(p) for p in players]


@players_router.get("/{player_id}", response_model=PlayerResponse)
async def get_player(
    player_id: int,
    session: AsyncSession = Depends(get_session),
) -> PlayerResponse:
    """Get a single player by ID."""
    player = await session.get(Player, player_id)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    return PlayerResponse.model_validate(player)


# ---------------------------------------------------------------------------
# Predictions
# ---------------------------------------------------------------------------


@predictions_router.get("/", response_model=List[PredictionResponse])
async def list_predictions(
    game_id: Optional[int] = Query(default=None, description="Game filter"),
    bet_type: Optional[str] = Query(default=None, description="Bet type filter"),
    recommended: Optional[bool] = Query(default=None, description="Recommended only"),
    best_bet: Optional[bool] = Query(default=None, description="Best bets only"),
    limit: int = Query(default=50, ge=1, le=200, description="Max results"),
    offset: int = Query(default=0, ge=0, description="Offset for pagination"),
    session: AsyncSession = Depends(get_session),
) -> List[PredictionResponse]:
    """List predictions with optional filters."""
    stmt = select(Prediction)
    if game_id is not None:
        stmt = stmt.where(Prediction.game_id == game_id)
    if bet_type:
        stmt = stmt.where(Prediction.bet_type == bet_type)
    if recommended is not None:
        stmt = stmt.where(Prediction.recommended == recommended)
    if best_bet is not None:
        stmt = stmt.where(Prediction.best_bet == best_bet)
    stmt = stmt.order_by(Prediction.created_at.desc()).offset(offset).limit(limit)

    result = await session.execute(stmt)
    predictions = result.scalars().all()
    return [PredictionResponse.model_validate(p) for p in predictions]


@predictions_router.get("/today/", response_model=List[PredictionResponse])
async def get_todays_predictions(
    recommended: Optional[bool] = Query(default=None, description="Recommended only"),
    best_bet: Optional[bool] = Query(default=None, description="Best bets only"),
    session: AsyncSession = Depends(get_session),
) -> List[PredictionResponse]:
    """Get predictions for today's games."""
    today = date.today()
    stmt = (
        select(Prediction)
        .join(Game, Prediction.game_id == Game.id)
        .where(Game.date == today)
    )
    if recommended is not None:
        stmt = stmt.where(Prediction.recommended == recommended)
    if best_bet is not None:
        stmt = stmt.where(Prediction.best_bet == best_bet)
    stmt = stmt.order_by(Prediction.confidence.desc())

    result = await session.execute(stmt)
    predictions = result.scalars().all()
    return [PredictionResponse.model_validate(p) for p in predictions]


@predictions_router.get("/performance", response_model=ModelPerformanceResponse)
async def get_model_performance(
    bet_type: Optional[str] = Query(default=None, description="Bet type filter"),
    session: AsyncSession = Depends(get_session),
) -> ModelPerformanceResponse:
    """Get overall model performance metrics."""
    # Total predictions
    pred_stmt = select(func.count(Prediction.id))
    if bet_type:
        pred_stmt = pred_stmt.where(Prediction.bet_type == bet_type)
    total_result = await session.execute(pred_stmt)
    total_predictions = total_result.scalar() or 0

    # Settled results
    result_stmt = select(BetResult)
    if bet_type:
        result_stmt = result_stmt.join(
            Prediction, BetResult.prediction_id == Prediction.id
        ).where(Prediction.bet_type == bet_type)
    result_rows = await session.execute(result_stmt)
    results = result_rows.scalars().all()

    settled = len(results)
    correct = sum(1 for r in results if r.was_correct)
    total_pl = sum(r.profit_loss for r in results)

    accuracy = (correct / settled) if settled > 0 else None
    roi = (total_pl / settled) if settled > 0 else None

    # Best bet accuracy
    best_stmt = (
        select(BetResult)
        .join(Prediction, BetResult.prediction_id == Prediction.id)
        .where(Prediction.best_bet.is_(True))
    )
    if bet_type:
        best_stmt = best_stmt.where(Prediction.bet_type == bet_type)
    best_result = await session.execute(best_stmt)
    best_results = best_result.scalars().all()
    best_correct = sum(1 for r in best_results if r.was_correct)
    best_bet_accuracy = (
        (best_correct / len(best_results)) if len(best_results) > 0 else None
    )

    return ModelPerformanceResponse(
        total_predictions=total_predictions,
        settled_predictions=settled,
        correct_predictions=correct,
        accuracy=accuracy,
        total_profit_loss=total_pl,
        roi=roi,
        best_bet_accuracy=best_bet_accuracy,
    )


@predictions_router.get("/{prediction_id}", response_model=PredictionResponse)
async def get_prediction(
    prediction_id: int,
    session: AsyncSession = Depends(get_session),
) -> PredictionResponse:
    """Get a single prediction by ID."""
    pred = await session.get(Prediction, prediction_id)
    if not pred:
        raise HTTPException(status_code=404, detail="Prediction not found")
    return PredictionResponse.model_validate(pred)


@predictions_router.get("/{prediction_id}/result", response_model=BetResultResponse)
async def get_prediction_result(
    prediction_id: int,
    session: AsyncSession = Depends(get_session),
) -> BetResultResponse:
    """Get the bet result for a specific prediction."""
    stmt = select(BetResult).where(BetResult.prediction_id == prediction_id)
    result = await session.execute(stmt)
    bet_result = result.scalar_one_or_none()
    if not bet_result:
        raise HTTPException(status_code=404, detail="Bet result not found")
    return BetResultResponse.model_validate(bet_result)
