"""
Predictions API routes.

Provides endpoints for generating, retrieving, and evaluating model
predictions for NHL games, including best-bet recommendations and
historical performance tracking.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_session
from app.models.game import Game
from app.models.prediction import Prediction
from app.models.team import Team

router = APIRouter(prefix="/api/predictions", tags=["predictions"])


# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------

class TeamSnapshot(BaseModel):
    """Minimal team reference used inside prediction payloads."""

    id: int
    name: str
    abbreviation: str

    model_config = {"from_attributes": True}


class PredictionDetail(BaseModel):
    """Full prediction record for an individual game."""

    id: int
    game_id: int
    game_date: Optional[date] = None
    home_team: Optional[TeamSnapshot] = None
    away_team: Optional[TeamSnapshot] = None

    prediction_type: Optional[str] = None
    predicted_winner_id: Optional[int] = None
    predicted_winner_name: Optional[str] = None
    confidence: Optional[float] = None
    predicted_home_score: Optional[float] = None
    predicted_away_score: Optional[float] = None
    predicted_total: Optional[float] = None
    edge: Optional[float] = None

    result: Optional[str] = None
    actual_home_score: Optional[int] = None
    actual_away_score: Optional[int] = None

    created_at: Optional[str] = None

    model_config = {"from_attributes": True}


class TodayPredictionsResponse(BaseModel):
    """Predictions for today's games."""

    date: date
    prediction_count: int
    predictions: List[PredictionDetail]


class BestBet(BaseModel):
    """A single best-bet recommendation."""

    prediction_id: int
    game_id: int
    game_date: Optional[date] = None
    home_team: Optional[TeamSnapshot] = None
    away_team: Optional[TeamSnapshot] = None
    prediction_type: Optional[str] = None
    predicted_winner_name: Optional[str] = None
    confidence: Optional[float] = None
    edge: Optional[float] = None
    predicted_home_score: Optional[float] = None
    predicted_away_score: Optional[float] = None
    predicted_total: Optional[float] = None
    reasoning: Optional[str] = None


class BestBetsResponse(BaseModel):
    """Top best-bet picks for today."""

    date: date
    bet_count: int
    best_bets: List[BestBet]


class HistoryEntry(BaseModel):
    """One day's worth of prediction performance."""

    date: date
    total_predictions: int
    correct: int
    incorrect: int
    pending: int
    hit_rate: Optional[float] = None


class HistoryResponse(BaseModel):
    """Historical prediction performance over time."""

    entries: List[HistoryEntry]
    total_predictions: int
    total_correct: int
    total_incorrect: int
    overall_hit_rate: Optional[float] = None


class GenerateResult(BaseModel):
    """Outcome of a prediction generation request."""

    success: bool
    message: str
    predictions_generated: int = 0


class ModelPerformanceStats(BaseModel):
    """Overall model performance summary."""

    total_predictions: int = 0
    total_graded: int = 0
    total_correct: int = 0
    total_incorrect: int = 0
    total_pending: int = 0
    hit_rate: Optional[float] = None
    roi: Optional[float] = None

    # Breakdowns
    moneyline_total: int = 0
    moneyline_correct: int = 0
    moneyline_hit_rate: Optional[float] = None

    over_under_total: int = 0
    over_under_correct: int = 0
    over_under_hit_rate: Optional[float] = None

    avg_confidence: Optional[float] = None
    avg_edge: Optional[float] = None

    best_streak: int = 0
    current_streak: int = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _build_prediction_detail(
    pred: Prediction, session: AsyncSession
) -> PredictionDetail:
    """Convert a Prediction ORM object into a fully-populated response model."""
    # Load the associated game with teams
    game_result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.id == pred.game_id)
    )
    game: Optional[Game] = game_result.scalar_one_or_none()

    home_team = None
    away_team = None
    game_date = None
    predicted_winner_name = None
    actual_home_score = None
    actual_away_score = None

    if game:
        game_date = game.game_date
        actual_home_score = game.home_score
        actual_away_score = game.away_score
        if game.home_team:
            home_team = TeamSnapshot(
                id=game.home_team.id,
                name=game.home_team.name,
                abbreviation=game.home_team.abbreviation,
            )
        if game.away_team:
            away_team = TeamSnapshot(
                id=game.away_team.id,
                name=game.away_team.name,
                abbreviation=game.away_team.abbreviation,
            )

        winner_id = getattr(pred, "predicted_winner_id", None)
        if winner_id and game.home_team and winner_id == game.home_team.id:
            predicted_winner_name = game.home_team.name
        elif winner_id and game.away_team and winner_id == game.away_team.id:
            predicted_winner_name = game.away_team.name

    return PredictionDetail(
        id=pred.id,
        game_id=pred.game_id,
        game_date=game_date,
        home_team=home_team,
        away_team=away_team,
        prediction_type=getattr(pred, "prediction_type", None),
        predicted_winner_id=getattr(pred, "predicted_winner_id", None),
        predicted_winner_name=predicted_winner_name,
        confidence=getattr(pred, "confidence", None),
        predicted_home_score=getattr(pred, "predicted_home_score", None),
        predicted_away_score=getattr(pred, "predicted_away_score", None),
        predicted_total=getattr(pred, "predicted_total", None),
        edge=getattr(pred, "edge", None),
        result=getattr(pred, "result", None),
        actual_home_score=actual_home_score,
        actual_away_score=actual_away_score,
        created_at=str(pred.created_at) if hasattr(pred, "created_at") else None,
    )


async def _get_predictions_for_date(
    target_date: date, session: AsyncSession
) -> List[PredictionDetail]:
    """Fetch all predictions whose game falls on the given date."""
    result = await session.execute(
        select(Prediction)
        .join(Game, Game.id == Prediction.game_id)
        .where(Game.game_date == target_date)
        .order_by(Prediction.id.asc())
    )
    predictions = result.scalars().all()

    details: List[PredictionDetail] = []
    for pred in predictions:
        detail = await _build_prediction_detail(pred, session)
        details.append(detail)
    return details


async def _try_generate_predictions(
    session: AsyncSession, target_date: Optional[date] = None
) -> int:
    """
    Attempt to generate predictions using the PredictionManager.

    Returns the count of new predictions created.
    """
    try:
        from app.analytics.predictions import PredictionManager

        manager = PredictionManager(session)
        results = await manager.generate_predictions(target_date=target_date)
        return len(results) if isinstance(results, list) else (results or 0)
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="Prediction engine is not available. Analytics module may not be installed.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate predictions: {exc}",
        )


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

@router.get(
    "/today",
    response_model=TodayPredictionsResponse,
    summary="Get today's predictions",
)
async def get_today_predictions(
    session: AsyncSession = Depends(get_session),
):
    """
    Return predictions for today's games.

    If no predictions exist yet, an automatic generation attempt is made
    using the PredictionManager before returning results.
    """
    today = date.today()
    predictions = await _get_predictions_for_date(today, session)

    # Auto-generate if none exist
    if not predictions:
        try:
            await _try_generate_predictions(session, target_date=today)
            await session.flush()
            predictions = await _get_predictions_for_date(today, session)
        except HTTPException:
            # Generation unavailable -- return empty list
            pass

    return TodayPredictionsResponse(
        date=today,
        prediction_count=len(predictions),
        predictions=predictions,
    )


@router.get(
    "/best-bets",
    response_model=BestBetsResponse,
    summary="Get top best bets for today",
)
async def get_best_bets(
    session: AsyncSession = Depends(get_session),
):
    """
    Return the top 1-3 best-bet picks for today, ranked by edge/confidence.

    Tries the PredictionManager.get_best_bets() first; falls back to
    selecting from existing predictions ordered by edge descending.
    """
    today = date.today()

    # Try the dedicated best-bets method first
    try:
        from app.analytics.predictions import PredictionManager

        manager = PredictionManager(session)
        bets = await manager.get_best_bets(target_date=today)
        if isinstance(bets, list) and bets:
            best: List[BestBet] = []
            for b in bets[:3]:
                # bets may be dicts or Prediction objects
                if isinstance(b, dict):
                    best.append(BestBet(**b))
                else:
                    detail = await _build_prediction_detail(b, session)
                    best.append(
                        BestBet(
                            prediction_id=detail.id,
                            game_id=detail.game_id,
                            game_date=detail.game_date,
                            home_team=detail.home_team,
                            away_team=detail.away_team,
                            prediction_type=detail.prediction_type,
                            predicted_winner_name=detail.predicted_winner_name,
                            confidence=detail.confidence,
                            edge=detail.edge,
                            predicted_home_score=detail.predicted_home_score,
                            predicted_away_score=detail.predicted_away_score,
                            predicted_total=detail.predicted_total,
                        )
                    )
            return BestBetsResponse(
                date=today, bet_count=len(best), best_bets=best
            )
    except (ImportError, AttributeError):
        pass
    except Exception:
        pass

    # Fallback: pick top predictions by edge from the DB
    result = await session.execute(
        select(Prediction)
        .join(Game, Game.id == Prediction.game_id)
        .where(Game.game_date == today)
        .order_by(
            Prediction.edge.desc().nulls_last(),
            Prediction.confidence.desc().nulls_last(),
        )
        .limit(3)
    )
    top_preds = result.scalars().all()

    best_bets: List[BestBet] = []
    for pred in top_preds:
        detail = await _build_prediction_detail(pred, session)
        best_bets.append(
            BestBet(
                prediction_id=detail.id,
                game_id=detail.game_id,
                game_date=detail.game_date,
                home_team=detail.home_team,
                away_team=detail.away_team,
                prediction_type=detail.prediction_type,
                predicted_winner_name=detail.predicted_winner_name,
                confidence=detail.confidence,
                edge=detail.edge,
                predicted_home_score=detail.predicted_home_score,
                predicted_away_score=detail.predicted_away_score,
                predicted_total=detail.predicted_total,
            )
        )

    return BestBetsResponse(
        date=today, bet_count=len(best_bets), best_bets=best_bets
    )


@router.get(
    "/history",
    response_model=HistoryResponse,
    summary="Get historical prediction performance",
)
async def get_prediction_history(
    days: int = 30,
    session: AsyncSession = Depends(get_session),
):
    """
    Return daily prediction performance for the last N days.

    Each entry includes the number of correct, incorrect, and pending
    predictions for that date plus the daily hit rate.
    """
    # We rely on safe getattr for 'result' because the Prediction model
    # may not yet define the column.
    result = await session.execute(
        select(
            Game.game_date,
            func.count(Prediction.id).label("total"),
        )
        .join(Game, Game.id == Prediction.game_id)
        .group_by(Game.game_date)
        .order_by(Game.game_date.desc())
        .limit(days)
    )
    date_rows = result.all()

    entries: List[HistoryEntry] = []
    grand_total = 0
    grand_correct = 0
    grand_incorrect = 0

    for row in date_rows:
        game_date = row[0]
        total = row[1]

        # Count results for this date
        correct_result = await session.execute(
            select(func.count(Prediction.id))
            .join(Game, Game.id == Prediction.game_id)
            .where(
                Game.game_date == game_date,
                Prediction.result == "correct",
            )
        )
        correct = correct_result.scalar() or 0

        incorrect_result = await session.execute(
            select(func.count(Prediction.id))
            .join(Game, Game.id == Prediction.game_id)
            .where(
                Game.game_date == game_date,
                Prediction.result == "incorrect",
            )
        )
        incorrect = incorrect_result.scalar() or 0

        pending = total - correct - incorrect
        hit_rate = round(correct / (correct + incorrect), 4) if (correct + incorrect) > 0 else None

        entries.append(
            HistoryEntry(
                date=game_date,
                total_predictions=total,
                correct=correct,
                incorrect=incorrect,
                pending=pending,
                hit_rate=hit_rate,
            )
        )
        grand_total += total
        grand_correct += correct
        grand_incorrect += incorrect

    overall_hit_rate = (
        round(grand_correct / (grand_correct + grand_incorrect), 4)
        if (grand_correct + grand_incorrect) > 0
        else None
    )

    return HistoryResponse(
        entries=entries,
        total_predictions=grand_total,
        total_correct=grand_correct,
        total_incorrect=grand_incorrect,
        overall_hit_rate=overall_hit_rate,
    )


@router.post(
    "/generate",
    response_model=GenerateResult,
    summary="Manually trigger prediction generation",
)
async def generate_predictions(
    session: AsyncSession = Depends(get_session),
):
    """
    Manually trigger the prediction engine to generate predictions
    for today's upcoming games.
    """
    today = date.today()
    count = await _try_generate_predictions(session, target_date=today)
    return GenerateResult(
        success=True,
        message=f"Generated {count} predictions for {today}.",
        predictions_generated=count,
    )


@router.get(
    "/stats",
    response_model=ModelPerformanceStats,
    summary="Overall model performance stats",
)
async def get_model_stats(
    session: AsyncSession = Depends(get_session),
):
    """
    Return aggregate model performance statistics including hit rate,
    ROI estimate, and breakdowns by prediction type (moneyline, over/under).
    """
    # Try dedicated evaluation method first
    try:
        from app.analytics.predictions import PredictionManager

        manager = PredictionManager(session)
        stats = await manager.evaluate_predictions()
        if isinstance(stats, dict):
            return ModelPerformanceStats(**stats)
    except (ImportError, AttributeError):
        pass
    except Exception:
        pass

    # Fallback: compute stats from DB
    total_result = await session.execute(select(func.count(Prediction.id)))
    total = total_result.scalar() or 0

    correct_result = await session.execute(
        select(func.count(Prediction.id)).where(Prediction.result == "correct")
    )
    correct = correct_result.scalar() or 0

    incorrect_result = await session.execute(
        select(func.count(Prediction.id)).where(Prediction.result == "incorrect")
    )
    incorrect = incorrect_result.scalar() or 0

    graded = correct + incorrect
    pending = total - graded
    hit_rate = round(correct / graded, 4) if graded > 0 else None

    # Simple ROI estimate: assume flat $100 bets, +100 odds (even money)
    # correct = +$100, incorrect = -$100
    roi = round((correct - incorrect) / total, 4) if total > 0 else None

    # Moneyline breakdown
    ml_total_result = await session.execute(
        select(func.count(Prediction.id)).where(
            Prediction.prediction_type == "moneyline",
            Prediction.result.in_(["correct", "incorrect"]),
        )
    )
    ml_total = ml_total_result.scalar() or 0

    ml_correct_result = await session.execute(
        select(func.count(Prediction.id)).where(
            Prediction.prediction_type == "moneyline",
            Prediction.result == "correct",
        )
    )
    ml_correct = ml_correct_result.scalar() or 0
    ml_hit_rate = round(ml_correct / ml_total, 4) if ml_total > 0 else None

    # Over/under breakdown
    ou_total_result = await session.execute(
        select(func.count(Prediction.id)).where(
            Prediction.prediction_type == "over_under",
            Prediction.result.in_(["correct", "incorrect"]),
        )
    )
    ou_total = ou_total_result.scalar() or 0

    ou_correct_result = await session.execute(
        select(func.count(Prediction.id)).where(
            Prediction.prediction_type == "over_under",
            Prediction.result == "correct",
        )
    )
    ou_correct = ou_correct_result.scalar() or 0
    ou_hit_rate = round(ou_correct / ou_total, 4) if ou_total > 0 else None

    # Average confidence and edge
    avg_conf_result = await session.execute(
        select(func.avg(Prediction.confidence))
    )
    avg_conf = avg_conf_result.scalar()

    avg_edge_result = await session.execute(
        select(func.avg(Prediction.edge))
    )
    avg_edge = avg_edge_result.scalar()

    return ModelPerformanceStats(
        total_predictions=total,
        total_graded=graded,
        total_correct=correct,
        total_incorrect=incorrect,
        total_pending=pending,
        hit_rate=hit_rate,
        roi=roi,
        moneyline_total=ml_total,
        moneyline_correct=ml_correct,
        moneyline_hit_rate=ml_hit_rate,
        over_under_total=ou_total,
        over_under_correct=ou_correct,
        over_under_hit_rate=ou_hit_rate,
        avg_confidence=round(avg_conf, 4) if avg_conf is not None else None,
        avg_edge=round(avg_edge, 4) if avg_edge is not None else None,
    )
