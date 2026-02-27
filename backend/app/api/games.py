"""
Games API routes.

Provides endpoints for retrieving detailed game information, including
team form, head-to-head records, goalie stats, predictions, and computed
analytical features for a specific game.
"""

import json
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_session
from app.models.game import Game, GameGoalieStats, GamePlayerStats, HeadToHead
from app.models.player import GoalieStats, Player, PlayerStats
from app.models.prediction import Prediction
from app.models.team import Team, TeamStats

router = APIRouter(prefix="/api/games", tags=["games"])


# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------

class TeamForm(BaseModel):
    """Recent form / record information for a team."""

    team_id: int
    team_name: str
    abbreviation: str
    wins: int = 0
    losses: int = 0
    ot_losses: int = 0
    points: int = 0
    record_last_5: Optional[str] = None
    record_last_10: Optional[str] = None
    home_record: Optional[str] = None
    away_record: Optional[str] = None
    goals_for_per_game: Optional[float] = None
    goals_against_per_game: Optional[float] = None
    power_play_pct: Optional[float] = None
    penalty_kill_pct: Optional[float] = None
    shots_for_per_game: Optional[float] = None
    shots_against_per_game: Optional[float] = None

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
    prediction_type: Optional[str] = None
    predicted_winner_id: Optional[int] = None
    confidence: Optional[float] = None
    predicted_home_score: Optional[float] = None
    predicted_away_score: Optional[float] = None
    predicted_total: Optional[float] = None
    edge: Optional[float] = None
    result: Optional[str] = None
    created_at: Optional[str] = None

    model_config = {"from_attributes": True}


class GameDetailResponse(BaseModel):
    """Full game details response with analytics context."""

    id: int
    external_id: str
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

    home_team_form: TeamForm
    away_team_form: TeamForm

    head_to_head: Optional[HeadToHeadRecord] = None

    home_period_scoring: PeriodScoring
    away_period_scoring: PeriodScoring

    home_goalies: List[GoalieInfo] = []
    away_goalies: List[GoalieInfo] = []

    predictions: List[GamePredictionBrief] = []


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
    )
    if stats:
        form.wins = stats.wins
        form.losses = stats.losses
        form.ot_losses = stats.ot_losses
        form.points = stats.points
        form.record_last_5 = stats.record_last_5
        form.record_last_10 = stats.record_last_10
        form.home_record = stats.home_record
        form.away_record = stats.away_record
        form.goals_for_per_game = stats.goals_for_per_game
        form.goals_against_per_game = stats.goals_against_per_game
        form.power_play_pct = stats.power_play_pct
        form.penalty_kill_pct = stats.penalty_kill_pct
        form.shots_for_per_game = stats.shots_for_per_game
        form.shots_against_per_game = stats.shots_against_per_game

    return form


async def _get_head_to_head(
    team1_id: int, team2_id: int, session: AsyncSession
) -> Optional[HeadToHeadRecord]:
    """Retrieve the most recent H2H record (convention: team1_id < team2_id)."""
    lo, hi = sorted([team1_id, team2_id])
    result = await session.execute(
        select(HeadToHead)
        .where(HeadToHead.team1_id == lo, HeadToHead.team2_id == hi)
        .order_by(HeadToHead.season.desc())
        .limit(1)
    )
    h2h = result.scalar_one_or_none()
    if h2h is None:
        return None
    return HeadToHeadRecord(
        team1_id=h2h.team1_id,
        team2_id=h2h.team2_id,
        season=h2h.season,
        games_played=h2h.games_played,
        team1_wins=h2h.team1_wins,
        team2_wins=h2h.team2_wins,
        draws=h2h.draws,
        team1_goals=h2h.team1_goals,
        team2_goals=h2h.team2_goals,
        last_meeting=h2h.last_meeting,
    )


async def _get_team_goalies(
    team_id: int, session: AsyncSession
) -> List[GoalieInfo]:
    """Get goalies for a team with their latest season stats."""
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
        stats_result = await session.execute(
            select(GoalieStats)
            .where(GoalieStats.player_id == goalie.id)
            .order_by(GoalieStats.season.desc())
            .limit(1)
        )
        gs: Optional[GoalieStats] = stats_result.scalar_one_or_none()

        info = GoalieInfo(
            player_id=goalie.id,
            name=goalie.name,
            team_id=goalie.team_id,
        )
        if gs:
            info.games_played = gs.games_played
            info.games_started = gs.games_started
            info.wins = gs.wins
            info.losses = gs.losses
            info.ot_losses = gs.ot_losses
            info.save_pct = gs.save_pct
            info.gaa = gs.gaa
            info.shutouts = gs.shutouts

        goalie_infos.append(info)

    return goalie_infos


async def _compute_period_scoring(
    team_id: int, session: AsyncSession
) -> PeriodScoring:
    """
    Compute average period-by-period scoring for a team from completed games.

    Parses the JSON `period_scores` field stored on each Game. The expected
    format is: {"home": [1, 2, 0], "away": [0, 1, 1]}.
    """
    result = await session.execute(
        select(Game).where(
            or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
            Game.status == "final",
            Game.period_scores.isnot(None),
        )
        .order_by(Game.game_date.desc())
        .limit(20)
    )
    games = result.scalars().all()

    if not games:
        return PeriodScoring()

    period_totals = [0.0, 0.0, 0.0]
    counted = 0
    for game in games:
        try:
            data = json.loads(game.period_scores) if isinstance(game.period_scores, str) else game.period_scores
        except (json.JSONDecodeError, TypeError):
            continue

        side = "home" if game.home_team_id == team_id else "away"
        scores = data.get(side, [])
        if len(scores) >= 3:
            for i in range(3):
                period_totals[i] += scores[i]
            counted += 1

    if counted == 0:
        return PeriodScoring()

    return PeriodScoring(
        period_1_avg=round(period_totals[0] / counted, 2),
        period_2_avg=round(period_totals[1] / counted, 2),
        period_3_avg=round(period_totals[2] / counted, 2),
    )


async def _get_game_predictions(
    game_id: int, session: AsyncSession
) -> List[GamePredictionBrief]:
    """Return all predictions associated with a game."""
    result = await session.execute(
        select(Prediction).where(Prediction.game_id == game_id)
    )
    preds = result.scalars().all()

    briefs: List[GamePredictionBrief] = []
    for p in preds:
        briefs.append(
            GamePredictionBrief(
                id=p.id,
                prediction_type=getattr(p, "prediction_type", None),
                predicted_winner_id=getattr(p, "predicted_winner_id", None),
                confidence=getattr(p, "confidence", None),
                predicted_home_score=getattr(p, "predicted_home_score", None),
                predicted_away_score=getattr(p, "predicted_away_score", None),
                predicted_total=getattr(p, "predicted_total", None),
                edge=getattr(p, "edge", None),
                result=getattr(p, "result", None),
                created_at=str(p.created_at) if hasattr(p, "created_at") else None,
            )
        )
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

    # Gather all analytics data concurrently (async-serial here, but logically separate)
    home_form = await _get_team_form(game.home_team, session)
    away_form = await _get_team_form(game.away_team, session)
    h2h = await _get_head_to_head(game.home_team_id, game.away_team_id, session)
    home_period = await _compute_period_scoring(game.home_team_id, session)
    away_period = await _compute_period_scoring(game.away_team_id, session)
    home_goalies = await _get_team_goalies(game.home_team_id, session)
    away_goalies = await _get_team_goalies(game.away_team_id, session)
    predictions = await _get_game_predictions(game.id, session)

    # Parse period_scores JSON if present
    parsed_period_scores = None
    if game.period_scores:
        try:
            parsed_period_scores = json.loads(game.period_scores) if isinstance(game.period_scores, str) else game.period_scores
        except (json.JSONDecodeError, TypeError):
            parsed_period_scores = None

    return GameDetailResponse(
        id=game.id,
        external_id=game.external_id,
        game_date=game.game_date,
        start_time=str(game.start_time) if game.start_time else None,
        venue=game.venue,
        status=game.status,
        game_type=game.game_type,
        season=game.season,
        home_score=game.home_score,
        away_score=game.away_score,
        total_goals=game.total_goals,
        overtime=game.overtime,
        shootout=game.shootout,
        period_scores=parsed_period_scores,
        home_team_form=home_form,
        away_team_form=away_form,
        head_to_head=h2h,
        home_period_scoring=home_period,
        away_period_scoring=away_period,
        home_goalies=home_goalies,
        away_goalies=away_goalies,
        predictions=predictions,
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

        engine = FeatureEngine(session)
        features = await engine.build_game_features(game.id)
        return FeatureResponse(game_id=game_id, features=features)
    except (ImportError, AttributeError):
        pass
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error computing features: {exc}",
        )

    # Fallback: construct a basic feature dict from available data
    home_form = await _get_team_form(game.home_team, session)
    away_form = await _get_team_form(game.away_team, session)
    h2h = await _get_head_to_head(game.home_team_id, game.away_team_id, session)

    features: Dict[str, Any] = {
        "game_id": game.id,
        "game_date": str(game.game_date),
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
