"""
Predictions API routes.

Provides endpoints for generating, retrieving, and evaluating model
predictions for NHL games, including best-bet recommendations and
historical performance tracking.
"""

import logging
from datetime import date, datetime
from typing import List, Optional

logger = logging.getLogger(__name__)

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
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
    ml_bets: List[BestBet] = []
    spread_bets: List[BestBet] = []
    total_bets: List[BestBet] = []


class HistoryBet(BaseModel):
    id: int
    game_id: int
    game_date: Optional[date] = None
    home_team: Optional[TeamSnapshot] = None
    away_team: Optional[TeamSnapshot] = None
    bet_type: Optional[str] = None
    prediction_value: Optional[str] = None
    confidence: Optional[float] = None
    edge: Optional[float] = None
    odds_display: Optional[float] = None
    outcome: Optional[str] = None
    profit: Optional[float] = None


class HistoryResponse(BaseModel):
    bets: List[HistoryBet]
    total_bets: int
    wins: int
    losses: int
    pending: int
    win_rate: Optional[float] = None
    total_profit: float = 0.0


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

        # Delete stale predictions only for non-final games so fresh ones
        # (with correct sportsbook lines) replace them.  Keep predictions
        # for final/completed games — those are historical records and
        # won't be regenerated (generate_predictions skips final games).
        non_final_game_ids = select(Game.id).where(
            Game.date == td,
            Game.status.notin_(["final", "completed", "off"]),
        )
        await session.execute(
            delete(Prediction).where(Prediction.game_id.in_(non_final_game_ids))
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
                has_real_odds = implied_prob is not None
                # Only compute edge when we have real sportsbook odds.
                # Without real odds there is no market to compare against,
                # so edge is meaningless and should not be used for ranking.
                if has_real_odds:
                    edge = round(confidence - implied_prob, 4)
                else:
                    edge = None

                await manager._persist_prediction(session, {
                    "game_id": game_id,
                    "bet_type": pred["bet_type"],
                    "prediction": pred["prediction"],
                    "confidence": confidence,
                    "probability": pred.get("probability", confidence),
                    "implied_probability": round(implied_prob, 4) if has_real_odds else None,
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
            async with session.begin_nested():
                await _try_generate_predictions(session, target_date=today)
                await session.flush()
            predictions = await _get_predictions_for_date(today, session)
        except Exception:
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
    # Each sync step is isolated in a savepoint so that a failed flush
    # (e.g. SQLite lock contention with the schedule endpoint) doesn't
    # corrupt the session for subsequent queries.
    try:
        async with session.begin_nested():
            from app.scrapers.odds_multi import MultiSourceOddsScraper

            odds_scraper = MultiSourceOddsScraper()
            try:
                matched = await odds_scraper.sync_odds(session)
                logger.info(
                    "Multi-source odds sync matched %d games before prediction generation",
                    len(matched) if matched else 0,
                )
                await session.flush()
                session.expire_all()
            finally:
                await odds_scraper.close()
    except Exception as exc:
        logger.warning("Odds sync failed before best-bets generation: %s", exc)

    # Always regenerate predictions fresh so they use current sportsbook
    # lines instead of returning stale cached predictions.
    try:
        async with session.begin_nested():
            pred_count = await _try_generate_predictions(session, target_date=today)
            await session.flush()
            logger.info("Regenerated %d predictions for best-bets", pred_count)
    except Exception as exc:
        logger.warning(
            "Prediction generation failed: %s",
            getattr(exc, 'detail', str(exc)),
        )

    # Exclude games that are already final — those bets can't be placed.
    FINAL_STATUSES = ("final", "completed", "off", "official")

    # Get top predictions by edge for today (non-final games only).
    # Only include predictions with real sportsbook odds so we can
    # calculate genuine edge.  Predictions without odds_implied_prob
    # had their edge set to None in _try_generate_predictions.
    # Juice filter: exclude heavy chalk from best bets.
    # odds_implied_prob reflects the market juice for ANY bet type
    # (moneyline, spread/puck-line, totals).  A -278 puck line has
    # implied prob ~0.735 which far exceeds our 0.63 ceiling.
    max_implied = settings.best_bet_max_implied

    # Base filter conditions shared across all queries
    base_conditions = [
        Game.date == today,
        ~func.lower(Game.status).in_(FINAL_STATUSES),
        Prediction.odds_implied_prob.isnot(None),
        Prediction.odds_implied_prob < max_implied,
    ]

    base_order = [
        Prediction.best_bet.desc(),
        Prediction.edge.desc().nulls_last(),
        Prediction.confidence.desc().nulls_last(),
    ]

    # Query top bets per category (3 per type)
    categorized: dict[str, list] = {"ml": [], "spread": [], "total": []}
    for bet_type in MARKET_BET_TYPES:
        result = await session.execute(
            select(Prediction)
            .options(selectinload(Prediction.result))
            .join(Game, Game.id == Prediction.game_id)
            .where(
                *base_conditions,
                Prediction.recommended == True,
                Prediction.bet_type == bet_type,
            )
            .order_by(*base_order)
            .limit(3)
        )
        preds = result.scalars().all()
        # Fallback: without recommended filter
        if not preds:
            result = await session.execute(
                select(Prediction)
                .options(selectinload(Prediction.result))
                .join(Game, Game.id == Prediction.game_id)
                .where(
                    *base_conditions,
                    Prediction.bet_type == bet_type,
                )
                .order_by(*base_order)
                .limit(3)
            )
            preds = result.scalars().all()
        categorized[bet_type] = preds

    # Overall top 3 for the legacy best_bets field
    result = await session.execute(
        select(Prediction)
        .options(selectinload(Prediction.result))
        .join(Game, Game.id == Prediction.game_id)
        .where(
            *base_conditions,
            Prediction.recommended == True,
            Prediction.bet_type.in_(MARKET_BET_TYPES),
        )
        .order_by(*base_order)
        .limit(3)
    )
    top_preds = result.scalars().all()
    if not top_preds:
        result = await session.execute(
            select(Prediction)
            .options(selectinload(Prediction.result))
            .join(Game, Game.id == Prediction.game_id)
            .where(
                *base_conditions,
                Prediction.bet_type.in_(MARKET_BET_TYPES),
            )
            .order_by(*base_order)
            .limit(3)
        )
        top_preds = result.scalars().all()

    logger.info(
        "Best-bets: ml=%d, spread=%d, total=%d, overall=%d",
        len(categorized["ml"]),
        len(categorized["spread"]),
        len(categorized["total"]),
        len(top_preds),
    )

    # Build all unique predictions that need detail resolution
    all_preds = list(top_preds)
    for preds in categorized.values():
        for p in preds:
            if p not in all_preds:
                all_preds.append(p)

    best_bets: List[BestBet] = []
    ml_bets: List[BestBet] = []
    spread_bets: List[BestBet] = []
    total_bets: List[BestBet] = []

    async def _build_best_bet(pred: Prediction) -> BestBet:
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
                    live_odds = game_obj.over_price
                else:
                    live_odds = game_obj.under_price
            elif pred.bet_type == "spread":
                home_team_result = await session.execute(
                    select(Team).where(Team.id == game_obj.home_team_id)
                )
                home_team_obj = home_team_result.scalar_one_or_none()
                if home_team_obj and pred.prediction_value and home_team_obj.abbreviation in pred.prediction_value:
                    live_odds = game_obj.home_spread_price
                else:
                    live_odds = game_obj.away_spread_price

        return BestBet(
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

    # Build categorized lists
    for pred in top_preds:
        best_bets.append(await _build_best_bet(pred))
    for bet_type, preds in categorized.items():
        target = {"ml": ml_bets, "spread": spread_bets, "total": total_bets}[bet_type]
        for pred in preds:
            target.append(await _build_best_bet(pred))

    return BestBetsResponse(
        date=today,
        bet_count=len(best_bets),
        best_bets=best_bets,
        ml_bets=ml_bets,
        spread_bets=spread_bets,
        total_bets=total_bets,
    )


@router.get("/history", response_model=HistoryResponse)
async def get_prediction_history(
    days: int = 90,
    session: AsyncSession = Depends(get_session),
):
    """Return the single best bet per game (highest edge) for recent games.

    This shows the model's top pick per game along with its result and
    profit/loss, giving a clear track record of the model's best calls.
    """
    MARKET_BET_TYPES = ("ml", "total", "spread")

    from sqlalchemy import and_

    # Subquery: max edge per game (only market bet types with real odds)
    max_edge_sub = (
        select(
            Prediction.game_id,
            func.max(Prediction.edge).label("max_edge"),
        )
        .where(
            Prediction.bet_type.in_(MARKET_BET_TYPES),
            Prediction.edge.isnot(None),
        )
        .group_by(Prediction.game_id)
        .subquery()
    )

    # Get the best prediction per game
    result = await session.execute(
        select(Prediction)
        .options(selectinload(Prediction.result))
        .join(Game, Game.id == Prediction.game_id)
        .join(
            max_edge_sub,
            and_(
                Prediction.game_id == max_edge_sub.c.game_id,
                Prediction.edge == max_edge_sub.c.max_edge,
            ),
        )
        .where(
            Prediction.bet_type.in_(MARKET_BET_TYPES),
            Prediction.edge.isnot(None),
        )
        .order_by(Game.date.desc(), Prediction.edge.desc())
        .limit(days)
    )
    preds = result.scalars().all()

    # Deduplicate: one prediction per game_id (keep highest edge)
    seen_games = set()
    unique_preds = []
    for pred in preds:
        if pred.game_id not in seen_games:
            seen_games.add(pred.game_id)
            unique_preds.append(pred)

    bets: List[HistoryBet] = []
    wins = 0
    losses = 0
    pending = 0
    total_profit = 0.0

    for pred in unique_preds:
        # Resolve game info
        game_result = await session.execute(
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(Game.id == pred.game_id)
        )
        game = game_result.scalar_one_or_none()
        if not game:
            continue

        home_team = None
        away_team = None
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

        # Resolve the sportsbook odds for display
        odds_display = None
        if pred.bet_type == "ml":
            if game.home_team and pred.prediction_value == game.home_team.abbreviation:
                odds_display = game.home_moneyline
            else:
                odds_display = game.away_moneyline
        elif pred.bet_type == "total":
            if pred.prediction_value and "over" in pred.prediction_value:
                odds_display = game.over_price
            else:
                odds_display = game.under_price
        elif pred.bet_type == "spread":
            if game.home_team and pred.prediction_value and game.home_team.abbreviation in pred.prediction_value:
                odds_display = game.home_spread_price
            else:
                odds_display = game.away_spread_price

        # Determine outcome from BetResult
        outcome = None
        profit = None
        if pred.result:
            if pred.result.was_correct:
                outcome = "Win"
                wins += 1
            else:
                outcome = "Loss"
                losses += 1
            profit = pred.result.profit_loss
            total_profit += profit or 0.0
        else:
            # Check if game is final — mark as pending
            status_lower = (game.status or "").lower()
            if status_lower in ("final", "completed", "off", "official"):
                outcome = "Pending"
            pending += 1

        bets.append(
            HistoryBet(
                id=pred.id,
                game_id=pred.game_id,
                game_date=game.date,
                home_team=home_team,
                away_team=away_team,
                bet_type=pred.bet_type,
                prediction_value=pred.prediction_value,
                confidence=pred.confidence,
                edge=pred.edge,
                odds_display=odds_display,
                outcome=outcome,
                profit=profit,
            )
        )

    total_graded = wins + losses
    win_rate = round(wins / total_graded, 4) if total_graded > 0 else None

    return HistoryResponse(
        bets=bets,
        total_bets=len(bets),
        wins=wins,
        losses=losses,
        pending=pending,
        win_rate=win_rate,
        total_profit=round(total_profit, 2),
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
