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
from sqlalchemy import delete, func, select
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
    odds_implied_prob: Optional[float] = None
    reasoning: Optional[str] = None
    game_status: Optional[str] = None
    odds_display: Optional[float] = None


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
    """Generate predictions and persist them to the database.

    Deletes any stale predictions for the target date first, then uses
    ``PredictionManager.generate_predictions`` to build fresh prediction
    dicts for every game on *target_date* and persists each one via
    ``_persist_prediction``.
    """
    try:
        from app.analytics.predictions import PredictionManager

        td = target_date or date.today()

        # Delete stale predictions for today so fresh ones (with correct
        # sportsbook lines) replace them.
        today_game_ids = select(Game.id).where(Game.date == td)
        await session.execute(
            delete(Prediction).where(Prediction.game_id.in_(today_game_ids))
        )
        await session.flush()

        manager = PredictionManager()
        results = await manager.generate_predictions(session, target_date)

        # Persist every generated prediction to the database so that
        # downstream queries (e.g. best-bets filtered by market type)
        # can find them.
        count = 0
        for game_data in results or []:
            game_id = game_data.get("game_id")
            for pred in game_data.get("predictions", []):
                confidence = pred.get("confidence", 0)
                implied_prob = pred.get("implied_probability")
                if implied_prob is None:
                    implied_prob = 0.5
                edge = round(confidence - implied_prob, 4)

                await manager._persist_prediction(session, {
                    "game_id": game_id,
                    "bet_type": pred["bet_type"],
                    "prediction": pred["prediction"],
                    "confidence": confidence,
                    "probability": pred.get("probability", confidence),
                    "implied_probability": round(implied_prob, 4),
                    "odds": pred.get("odds"),
                    "edge": edge,
                    "reasoning": pred.get("reasoning", ""),
                    "is_best_bet": False,
                })
                count += 1

        if count > 0:
            await session.flush()

        return count
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

    # Only show bet types that have real market odds so we can calculate
    # genuine edge.  Props (both_score/BTTS, first_goal, overtime, odd_even,
    # period_winner, period_total) don't carry market odds and would show
    # inflated/fake edges.
    MARKET_BET_TYPES = ("ml", "total", "spread")

    # Refresh odds from The Odds API to get live/current lines before
    # generating predictions.  This ensures in-play games show live odds
    # and pre-match games always have the latest lines.
    try:
        from app.scrapers.odds_api import OddsScraper

        odds_scraper = OddsScraper()
        try:
            await odds_scraper.sync_odds(session)
            await session.flush()
        finally:
            await odds_scraper.close()
    except Exception:
        pass  # Non-critical; proceed with existing odds

    # Always regenerate predictions fresh so they use current sportsbook
    # lines instead of returning stale cached predictions.
    try:
        await _try_generate_predictions(session, target_date=today)
        await session.flush()
    except HTTPException:
        pass

    # Exclude games that are already final — those bets can't be placed.
    FINAL_STATUSES = ("final", "completed", "off", "official")

    # Get top predictions by edge for today (non-final games only)
    result = await session.execute(
        select(Prediction)
        .options(selectinload(Prediction.result))
        .join(Game, Game.id == Prediction.game_id)
        .where(
            Game.date == today,
            ~func.lower(Game.status).in_(FINAL_STATUSES),
            Prediction.recommended == True,
            Prediction.bet_type.in_(MARKET_BET_TYPES),
        )
        .order_by(
            Prediction.best_bet.desc(),
            Prediction.edge.desc().nulls_last(),
            Prediction.confidence.desc().nulls_last(),
        )
        .limit(3)
    )
    top_preds = result.scalars().all()

    # If no recommended preds, fall back to top confidence (still filtered)
    if not top_preds:
        result = await session.execute(
            select(Prediction)
            .options(selectinload(Prediction.result))
            .join(Game, Game.id == Prediction.game_id)
            .where(
                Game.date == today,
                ~func.lower(Game.status).in_(FINAL_STATUSES),
                Prediction.bet_type.in_(MARKET_BET_TYPES),
            )
            .order_by(
                Prediction.confidence.desc().nulls_last(),
            )
            .limit(3)
        )
        top_preds = result.scalars().all()

    best_bets: List[BestBet] = []
    for pred in top_preds:
        detail = await _build_prediction_detail(pred, session)

        # Fetch the game to get status and live odds
        game_result = await session.execute(
            select(Game).where(Game.id == pred.game_id)
        )
        game_obj = game_result.scalar_one_or_none()
        game_status = game_obj.status if game_obj else None

        # Resolve the actual sportsbook odds for this specific bet
        live_odds = None
        if game_obj:
            if pred.bet_type == "ml":
                # Figure out which side the prediction is for
                home_team_result = await session.execute(
                    select(Team).where(Team.id == game_obj.home_team_id)
                )
                home_team_obj = home_team_result.scalar_one_or_none()
                if home_team_obj and pred.prediction_value == home_team_obj.abbreviation:
                    live_odds = game_obj.home_moneyline
                else:
                    live_odds = game_obj.away_moneyline
            elif pred.bet_type == "total":
                if pred.prediction_value and "over" in pred.prediction_value:
                    live_odds = game_obj.over_price or -110.0
                else:
                    live_odds = game_obj.under_price or -110.0
            elif pred.bet_type == "spread":
                # Check if it's home or away side spread
                home_team_result = await session.execute(
                    select(Team).where(Team.id == game_obj.home_team_id)
                )
                home_team_obj = home_team_result.scalar_one_or_none()
                if home_team_obj and pred.prediction_value and home_team_obj.abbreviation in pred.prediction_value:
                    live_odds = game_obj.home_spread_price or -110.0
                else:
                    live_odds = game_obj.away_spread_price or -110.0

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
                odds_implied_prob=pred.odds_implied_prob,
                reasoning=detail.reasoning,
                game_status=game_status,
                odds_display=live_odds,
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
