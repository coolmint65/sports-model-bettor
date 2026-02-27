"""
Predictions API routes.

Provides endpoints for generating, retrieving, and evaluating model
predictions for NHL games, including best-bet recommendations and
historical performance tracking.
"""

from datetime import date, datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_session
from app.models.game import Game
from app.models.prediction import BetResult, Prediction
from app.models.team import Team

router = APIRouter(prefix="/api/predictions", tags=["predictions"])


# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------

class TeamSnapshot(BaseModel):
    id: int
    name: str
    abbreviation: str
    model_config = {"from_attributes": True}


class PredictionDetail(BaseModel):
    id: int
    game_id: int
    game_date: Optional[date] = None
    home_team: Optional[TeamSnapshot] = None
    away_team: Optional[TeamSnapshot] = None
    bet_type: Optional[str] = None
    prediction_value: Optional[str] = None
    confidence: Optional[float] = None
    edge: Optional[float] = None
    recommended: bool = False
    best_bet: bool = False
    reasoning: Optional[str] = None
    was_correct: Optional[bool] = None
    actual_home_score: Optional[int] = None
    actual_away_score: Optional[int] = None
    created_at: Optional[str] = None
    model_config = {"from_attributes": True}


class TodayPredictionsResponse(BaseModel):
    date: date
    prediction_count: int
    predictions: List[PredictionDetail]


class BestBet(BaseModel):
    prediction_id: int
    game_id: int
    game_date: Optional[date] = None
    home_team: Optional[TeamSnapshot] = None
    away_team: Optional[TeamSnapshot] = None
    bet_type: Optional[str] = None
    prediction_value: Optional[str] = None
    confidence: Optional[float] = None
    edge: Optional[float] = None
    reasoning: Optional[str] = None


class BestBetsResponse(BaseModel):
    date: date
    bet_count: int
    best_bets: List[BestBet]


class HistoryEntry(BaseModel):
    date: date
    total_predictions: int
    correct: int
    incorrect: int
    pending: int
    hit_rate: Optional[float] = None


class HistoryResponse(BaseModel):
    entries: List[HistoryEntry]
    total_predictions: int
    total_correct: int
    total_incorrect: int
    overall_hit_rate: Optional[float] = None


class GenerateResult(BaseModel):
    success: bool
    message: str
    predictions_generated: int = 0


class ModelPerformanceStats(BaseModel):
    total_predictions: int = 0
    total_graded: int = 0
    total_correct: int = 0
    total_incorrect: int = 0
    total_pending: int = 0
    hit_rate: Optional[float] = None
    roi: Optional[float] = None
    avg_confidence: Optional[float] = None
    avg_edge: Optional[float] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _build_prediction_detail(
    pred: Prediction, session: AsyncSession
) -> PredictionDetail:
    game_result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.id == pred.game_id)
    )
    game: Optional[Game] = game_result.scalar_one_or_none()

    home_team = None
    away_team = None
    game_date = None
    actual_home_score = None
    actual_away_score = None
    was_correct = None

    if game:
        game_date = game.date
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

    # Check bet result if exists
    if pred.result:
        was_correct = pred.result.was_correct

    return PredictionDetail(
        id=pred.id,
        game_id=pred.game_id,
        game_date=game_date,
        home_team=home_team,
        away_team=away_team,
        bet_type=pred.bet_type,
        prediction_value=pred.prediction_value,
        confidence=pred.confidence,
        edge=pred.edge,
        recommended=pred.recommended,
        best_bet=pred.best_bet,
        reasoning=pred.reasoning,
        was_correct=was_correct,
        actual_home_score=actual_home_score,
        actual_away_score=actual_away_score,
        created_at=str(pred.created_at) if pred.created_at else None,
    )


async def _get_predictions_for_date(
    target_date: date, session: AsyncSession
) -> List[PredictionDetail]:
    result = await session.execute(
        select(Prediction)
        .options(selectinload(Prediction.result))
        .join(Game, Game.id == Prediction.game_id)
        .where(Game.date == target_date)
        .order_by(Prediction.confidence.desc().nulls_last())
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
    try:
        from app.analytics.predictions import PredictionManager

        manager = PredictionManager()
        results = await manager.generate_predictions(session, target_date)
        return len(results) if isinstance(results, list) else 0
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="Prediction engine is not available.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate predictions: {exc}",
        )


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

@router.get("/today", response_model=TodayPredictionsResponse)
async def get_today_predictions(
    session: AsyncSession = Depends(get_session),
):
    today = date.today()
    predictions = await _get_predictions_for_date(today, session)

    if not predictions:
        try:
            await _try_generate_predictions(session, target_date=today)
            await session.flush()
            predictions = await _get_predictions_for_date(today, session)
        except HTTPException:
            pass

    return TodayPredictionsResponse(
        date=today,
        prediction_count=len(predictions),
        predictions=predictions,
    )


@router.get("/best-bets", response_model=BestBetsResponse)
async def get_best_bets(
    session: AsyncSession = Depends(get_session),
):
    today = date.today()

    # Get top predictions by edge for today
    result = await session.execute(
        select(Prediction)
        .options(selectinload(Prediction.result))
        .join(Game, Game.id == Prediction.game_id)
        .where(Game.date == today, Prediction.recommended == True)
        .order_by(
            Prediction.best_bet.desc(),
            Prediction.edge.desc().nulls_last(),
            Prediction.confidence.desc().nulls_last(),
        )
        .limit(3)
    )
    top_preds = result.scalars().all()

    # If no recommended preds, fall back to top confidence
    if not top_preds:
        result = await session.execute(
            select(Prediction)
            .options(selectinload(Prediction.result))
            .join(Game, Game.id == Prediction.game_id)
            .where(Game.date == today)
            .order_by(
                Prediction.confidence.desc().nulls_last(),
            )
            .limit(3)
        )
        top_preds = result.scalars().all()

    # If still no predictions, auto-generate them
    if not top_preds:
        try:
            await _try_generate_predictions(session, target_date=today)
            await session.flush()
            result = await session.execute(
                select(Prediction)
                .options(selectinload(Prediction.result))
                .join(Game, Game.id == Prediction.game_id)
                .where(Game.date == today, Prediction.recommended == True)
                .order_by(
                    Prediction.best_bet.desc(),
                    Prediction.edge.desc().nulls_last(),
                    Prediction.confidence.desc().nulls_last(),
                )
                .limit(3)
            )
            top_preds = result.scalars().all()
            if not top_preds:
                result = await session.execute(
                    select(Prediction)
                    .options(selectinload(Prediction.result))
                    .join(Game, Game.id == Prediction.game_id)
                    .where(Game.date == today)
                    .order_by(Prediction.confidence.desc().nulls_last())
                    .limit(3)
                )
                top_preds = result.scalars().all()
        except HTTPException:
            pass

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
                bet_type=detail.bet_type,
                prediction_value=detail.prediction_value,
                confidence=detail.confidence,
                edge=detail.edge,
                reasoning=detail.reasoning,
            )
        )

    return BestBetsResponse(
        date=today, bet_count=len(best_bets), best_bets=best_bets
    )


@router.get("/history", response_model=HistoryResponse)
async def get_prediction_history(
    days: int = 30,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(
            Game.date,
            func.count(Prediction.id).label("total"),
        )
        .join(Game, Game.id == Prediction.game_id)
        .group_by(Game.date)
        .order_by(Game.date.desc())
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

        # Count settled results for this date via BetResult join
        correct_result = await session.execute(
            select(func.count(BetResult.id))
            .join(Prediction, Prediction.id == BetResult.prediction_id)
            .join(Game, Game.id == Prediction.game_id)
            .where(Game.date == game_date, BetResult.was_correct == True)
        )
        correct = correct_result.scalar() or 0

        incorrect_result = await session.execute(
            select(func.count(BetResult.id))
            .join(Prediction, Prediction.id == BetResult.prediction_id)
            .join(Game, Game.id == Prediction.game_id)
            .where(Game.date == game_date, BetResult.was_correct == False)
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


@router.post("/generate", response_model=GenerateResult)
async def generate_predictions(
    session: AsyncSession = Depends(get_session),
):
    today = date.today()
    count = await _try_generate_predictions(session, target_date=today)
    return GenerateResult(
        success=True,
        message=f"Generated {count} predictions for {today}.",
        predictions_generated=count,
    )


@router.get("/stats", response_model=ModelPerformanceStats)
async def get_model_stats(
    session: AsyncSession = Depends(get_session),
):
    total_result = await session.execute(select(func.count(Prediction.id)))
    total = total_result.scalar() or 0

    correct_result = await session.execute(
        select(func.count(BetResult.id)).where(BetResult.was_correct == True)
    )
    correct = correct_result.scalar() or 0

    incorrect_result = await session.execute(
        select(func.count(BetResult.id)).where(BetResult.was_correct == False)
    )
    incorrect = incorrect_result.scalar() or 0

    graded = correct + incorrect
    pending = total - graded
    hit_rate = round(correct / graded, 4) if graded > 0 else None

    # ROI from BetResult.profit_loss
    roi_result = await session.execute(select(func.sum(BetResult.profit_loss)))
    total_pl = roi_result.scalar() or 0.0
    roi = round(total_pl / graded, 4) if graded > 0 else None

    avg_conf_result = await session.execute(select(func.avg(Prediction.confidence)))
    avg_conf = avg_conf_result.scalar()

    avg_edge_result = await session.execute(select(func.avg(Prediction.edge)))
    avg_edge = avg_edge_result.scalar()

    return ModelPerformanceStats(
        total_predictions=total,
        total_graded=graded,
        total_correct=correct,
        total_incorrect=incorrect,
        total_pending=pending,
        hit_rate=hit_rate,
        roi=roi,
        avg_confidence=round(avg_conf, 4) if avg_conf is not None else None,
        avg_edge=round(avg_edge, 4) if avg_edge is not None else None,
    )
