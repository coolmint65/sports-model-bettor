"""
Predictions API routes.

Provides endpoints for generating, retrieving, and evaluating model
predictions for NHL games, including best-bet recommendations,
user-tracked bet management, and historical performance tracking.

Predictions are split into two phases:
  - **prematch**: Generated once before the game starts.  These are
    locked and never regenerated so the user always sees the original
    pre-game pick.
  - **live**: Generated (and updated) while a game is in progress.
    Updates are throttled so the pick doesn't flip-flop on every sync.
"""

import logging
from datetime import date, datetime
from typing import List, Optional

logger = logging.getLogger(__name__)

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import and_, delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.constants import GAME_FINAL_STATUSES, MARKET_BET_TYPES, composite_pick_score
from app.database import get_session
from app.models.game import Game
from app.models.prediction import BetResult, Prediction, TrackedBet
from app.models.team import Team

router = APIRouter(prefix="/api/predictions", tags=["predictions"])

# Minimum change in confidence required to update a live prediction.
# Prevents noisy flip-flopping on every sync.
LIVE_UPDATE_THRESHOLD = 0.05


# ---------------------------------------------------------------------------
# Unit sizing
# ---------------------------------------------------------------------------

def calculate_units(edge: Optional[float], confidence: Optional[float]) -> float:
    """Return recommended unit size based on edge.

    Tiered approach:
      edge <  3% → 0.5u  (lean)
      edge  3-5% → 1u    (standard)
      edge  5-8% → 1.5u
      edge 8-12% → 2u
      edge  12%+ → 3u    (max play)
    """
    if edge is None:
        return 1.0
    e = edge * 100  # convert to percentage points
    if e < 3:
        return 0.5
    if e < 5:
        return 1.0
    if e < 8:
        return 1.5
    if e < 12:
        return 2.0
    return 3.0


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
    phase: Optional[str] = None
    units: Optional[float] = None


class BestBetsResponse(BaseModel):
    date: date
    bet_count: int
    best_bets: List[BestBet]
    ml_bets: List[BestBet] = []
    spread_bets: List[BestBet] = []
    total_bets: List[BestBet] = []


class TrackedBetRequest(BaseModel):
    prediction_id: int
    units: Optional[float] = None  # override auto-calculated units


class TrackedBetResponse(BaseModel):
    id: int
    prediction_id: Optional[int] = None
    game_id: int
    bet_type: str
    prediction_value: str
    confidence: Optional[float] = None
    odds: Optional[float] = None
    implied_probability: Optional[float] = None
    edge: Optional[float] = None
    units: float = 1.0
    phase: Optional[str] = None
    reasoning: Optional[str] = None
    home_team_name: Optional[str] = None
    away_team_name: Optional[str] = None
    home_team_abbr: Optional[str] = None
    away_team_abbr: Optional[str] = None
    game_date: Optional[date] = None
    result: Optional[str] = None
    profit_loss: Optional[float] = None
    settled_at: Optional[str] = None
    created_at: Optional[str] = None
    model_config = {"from_attributes": True}


class TrackedBetsListResponse(BaseModel):
    bets: List[TrackedBetResponse]
    total_bets: int
    wins: int
    losses: int
    pushes: int
    pending: int
    win_rate: Optional[float] = None
    total_profit: float = 0.0
    total_units_wagered: float = 0.0


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
    """Generate predictions with prematch/live phase separation.

    **Prematch** predictions are generated once for scheduled/preview
    games.  If prematch rows already exist for a game they are kept
    untouched (locked).

    **Live** predictions are generated for in-progress games.  If a
    live prediction already exists and the new confidence is within
    ``LIVE_UPDATE_THRESHOLD`` of the old value, the old prediction is
    kept to prevent noisy flip-flopping.
    """
    try:
        from app.analytics.predictions import PredictionManager

        td = target_date or date.today()

        manager = PredictionManager()
        results = await manager.generate_predictions(session, td)

        if not results:
            logger.warning(
                "_try_generate: model returned 0 game results for %s", td
            )

        count = 0
        for game_data in results or []:
            game_id = game_data.get("game_id")
            game_status = game_data.get("status", "scheduled")
            is_live = game_status == "in_progress"
            phase = "live" if is_live else "prematch"

            if not is_live:
                # ---- Prematch: only generate if none exist for this game ----
                existing_result = await session.execute(
                    select(func.count(Prediction.id)).where(
                        Prediction.game_id == game_id,
                        Prediction.phase == "prematch",
                    )
                )
                existing_count = existing_result.scalar() or 0
                if existing_count > 0:
                    # Prematch predictions already locked — skip
                    continue

            if is_live:
                # ---- Live: load existing live predictions for comparison ----
                existing_live_result = await session.execute(
                    select(Prediction).where(
                        Prediction.game_id == game_id,
                        Prediction.phase == "live",
                    )
                )
                existing_live = {
                    (p.bet_type, p.prediction_value): p
                    for p in existing_live_result.scalars().all()
                }

            for pred in game_data.get("predictions", []):
                confidence = pred.get("confidence", 0)
                implied_prob = pred.get("implied_probability")
                has_real_odds = implied_prob is not None
                if has_real_odds:
                    edge = round(confidence - implied_prob, 4)
                else:
                    edge = None

                bet_type = pred["bet_type"]
                prediction_value = pred["prediction"]

                if is_live:
                    # Check if existing live prediction is still close enough
                    key = (bet_type, prediction_value)
                    old = existing_live.get(key)
                    if old is not None:
                        delta = abs(confidence - (old.confidence or 0))
                        if delta < LIVE_UPDATE_THRESHOLD:
                            # Small change — keep the old prediction as-is
                            existing_live.pop(key, None)
                            continue
                        # Significant change — update in-place
                        old.confidence = confidence
                        old.odds_implied_prob = (
                            round(implied_prob, 4) if has_real_odds else None
                        )
                        old.edge = edge
                        old.reasoning = pred.get("reasoning", old.reasoning or "")
                        old.recommended = (
                            confidence >= settings.min_confidence
                            and (edge or 0) >= settings.min_edge
                        )
                        existing_live.pop(key, None)
                        count += 1
                        continue

                    # Check if the *side flipped* (e.g., old was home ML,
                    # new is away ML).  In that case only flip if the new
                    # side is clearly better.
                    flipped_key = None
                    for ek in list(existing_live.keys()):
                        if ek[0] == bet_type and ek[1] != prediction_value:
                            flipped_key = ek
                            break
                    if flipped_key:
                        old_flipped = existing_live[flipped_key]
                        # Only flip if the new pick has notably higher conf
                        if confidence - (old_flipped.confidence or 0) < LIVE_UPDATE_THRESHOLD:
                            existing_live.pop(flipped_key, None)
                            continue
                        # Delete the old flipped prediction
                        await session.delete(old_flipped)
                        existing_live.pop(flipped_key, None)

                await manager._persist_prediction(session, {
                    "game_id": game_id,
                    "bet_type": bet_type,
                    "prediction": prediction_value,
                    "confidence": confidence,
                    "probability": pred.get("probability", confidence),
                    "implied_probability": (
                        round(implied_prob, 4) if has_real_odds else None
                    ),
                    "odds": pred.get("odds"),
                    "edge": edge,
                    "reasoning": pred.get("reasoning", ""),
                    "is_best_bet": False,
                    "phase": phase,
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

    # Check if predictions already exist (e.g., from a recent regeneration).
    # If they do, skip the expensive odds sync + regeneration to avoid
    # timeouts and redundant API calls.
    existing_pred_count = await session.execute(
        select(func.count(Prediction.id))
        .join(Game, Game.id == Prediction.game_id)
        .where(
            Game.date == today,
            ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
            Prediction.phase == "prematch",
        )
    )
    has_predictions = (existing_pred_count.scalar() or 0) > 0

    if not has_predictions:
        # No predictions yet — sync odds and generate
        try:
            async with session.begin_nested():
                from app.scrapers.odds_multi import MultiSourceOddsScraper

                async with MultiSourceOddsScraper() as odds_scraper:
                    matched = await odds_scraper.sync_odds(session)
                    logger.info(
                        "Multi-source odds sync matched %d games before prediction generation",
                        len(matched) if matched else 0,
                    )
                    await session.flush()
                    session.expire_all()
        except Exception as exc:
            logger.warning("Odds sync failed before best-bets generation: %s", exc)

        # Generate / update predictions (respects prematch locks)
        try:
            async with session.begin_nested():
                pred_count = await _try_generate_predictions(session, target_date=today)
                await session.flush()
                logger.info("Generated/updated %d predictions for best-bets", pred_count)
        except Exception as exc:
            logger.warning(
                "Prediction generation failed: %s",
                getattr(exc, 'detail', str(exc)),
            )

    max_implied = settings.best_bet_max_implied

    base_conditions = [
        Game.date == today,
        ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
        Prediction.odds_implied_prob.isnot(None),
        Prediction.odds_implied_prob < max_implied,
    ]

    def _score(p: Prediction) -> float:
        """Composite score for ranking: confidence + edge + juice."""
        return composite_pick_score(p.confidence, p.edge, p.odds_implied_prob)

    # Fetch all eligible predictions in one query, then rank in Python
    # using the composite score (confidence + edge + juice).
    result = await session.execute(
        select(Prediction)
        .options(selectinload(Prediction.result))
        .join(Game, Game.id == Prediction.game_id)
        .where(
            *base_conditions,
            Prediction.bet_type.in_(MARKET_BET_TYPES),
        )
    )
    all_eligible = result.scalars().all()

    # Split into recommended and fallback pools
    recommended = [p for p in all_eligible if p.recommended]
    fallback = [p for p in all_eligible if not p.recommended]

    # Sort each pool by composite score
    recommended.sort(key=_score, reverse=True)
    fallback.sort(key=_score, reverse=True)

    # Pick top 3 per category (prefer recommended, fall back if empty)
    categorized: dict[str, list] = {"ml": [], "spread": [], "total": []}
    for bet_type in MARKET_BET_TYPES:
        typed = [p for p in recommended if p.bet_type == bet_type][:3]
        if not typed:
            typed = [p for p in fallback if p.bet_type == bet_type][:3]
        categorized[bet_type] = typed

    # Overall top 3 (prefer recommended, fall back if empty)
    top_preds = recommended[:3]
    if not top_preds:
        top_preds = fallback[:3]

    logger.info(
        "Best-bets: ml=%d, spread=%d, total=%d, overall=%d",
        len(categorized["ml"]),
        len(categorized["spread"]),
        len(categorized["total"]),
        len(top_preds),
    )

    best_bets: List[BestBet] = []
    ml_bets: List[BestBet] = []
    spread_bets: List[BestBet] = []
    total_bets: List[BestBet] = []

    async def _build_best_bet(pred: Prediction) -> Optional[BestBet]:
        """Build a BestBet response, or return None if display odds exceed juice threshold."""
        detail = await _build_prediction_detail(pred, session)

        game_result = await session.execute(
            select(Game).where(Game.id == pred.game_id)
        )
        game_obj = game_result.scalar_one_or_none()
        game_status = game_obj.status if game_obj else None

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

        # Filter out bets whose displayed odds exceed the juice threshold.
        # The model may have evaluated using a better price from another
        # sportsbook, but if the primary line is heavy juice, don't show it.
        if live_odds is not None:
            from app.analytics.models import american_odds_to_implied_prob
            display_implied = american_odds_to_implied_prob(live_odds)
            if display_implied >= max_implied:
                logger.debug(
                    "Filtering heavy-juice best bet: %s %s (display odds %s, implied %.3f)",
                    pred.bet_type, pred.prediction_value, live_odds, display_implied,
                )
                return None

        units = calculate_units(pred.edge, pred.confidence)
        # Use the actual game status to determine phase — a prediction
        # created prematch is effectively "live" once the game starts.
        phase = "live" if game_status == "in_progress" else getattr(pred, "phase", "prematch")

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
            phase=phase,
            units=units,
        )

    for pred in top_preds:
        bet = await _build_best_bet(pred)
        if bet is not None:
            best_bets.append(bet)
    for bet_type, preds in categorized.items():
        target = {"ml": ml_bets, "spread": spread_bets, "total": total_bets}[bet_type]
        for pred in preds:
            bet = await _build_best_bet(pred)
            if bet is not None:
                target.append(bet)

    return BestBetsResponse(
        date=today,
        bet_count=len(best_bets),
        best_bets=best_bets,
        ml_bets=ml_bets,
        spread_bets=spread_bets,
        total_bets=total_bets,
    )


# ---------------------------------------------------------------------------
# Tracked bets endpoints
# ---------------------------------------------------------------------------

@router.post("/tracked", response_model=TrackedBetResponse)
async def track_bet(
    body: TrackedBetRequest,
    session: AsyncSession = Depends(get_session),
):
    """Add a prediction to the user's tracked bets."""
    pred_result = await session.execute(
        select(Prediction).where(Prediction.id == body.prediction_id)
    )
    pred = pred_result.scalar_one_or_none()
    if not pred:
        raise HTTPException(status_code=404, detail="Prediction not found")

    # Check for duplicate
    dup_result = await session.execute(
        select(TrackedBet).where(
            TrackedBet.prediction_id == pred.id,
        )
    )
    if dup_result.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Bet already tracked")

    # Resolve game info
    game_result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.id == pred.game_id)
    )
    game = game_result.scalar_one_or_none()

    units = body.units or calculate_units(pred.edge, pred.confidence)

    # Resolve sportsbook odds for snapshot
    odds_val = None
    if game:
        if pred.bet_type == "ml":
            if game.home_team and pred.prediction_value == game.home_team.abbreviation:
                odds_val = game.home_moneyline
            else:
                odds_val = game.away_moneyline
        elif pred.bet_type == "total":
            if pred.prediction_value and "over" in pred.prediction_value:
                odds_val = game.over_price
            else:
                odds_val = game.under_price
        elif pred.bet_type == "spread":
            if game.home_team and pred.prediction_value and game.home_team.abbreviation in pred.prediction_value:
                odds_val = game.home_spread_price
            else:
                odds_val = game.away_spread_price

    tracked = TrackedBet(
        prediction_id=pred.id,
        game_id=pred.game_id,
        bet_type=pred.bet_type,
        prediction_value=pred.prediction_value,
        confidence=pred.confidence,
        odds=odds_val,
        implied_probability=pred.odds_implied_prob,
        edge=pred.edge,
        units=units,
        phase=getattr(pred, "phase", "prematch"),
        reasoning=pred.reasoning,
        home_team_name=game.home_team.name if game and game.home_team else None,
        away_team_name=game.away_team.name if game and game.away_team else None,
        home_team_abbr=game.home_team.abbreviation if game and game.home_team else None,
        away_team_abbr=game.away_team.abbreviation if game and game.away_team else None,
        game_date=game.date if game else None,
    )
    session.add(tracked)
    await session.flush()

    return _tracked_bet_to_response(tracked)


@router.get("/tracked", response_model=TrackedBetsListResponse)
async def list_tracked_bets(
    session: AsyncSession = Depends(get_session),
):
    """Return all tracked bets with aggregate stats."""
    result = await session.execute(
        select(TrackedBet).order_by(TrackedBet.created_at.desc())
    )
    bets = result.scalars().all()

    items: List[TrackedBetResponse] = []
    wins = losses = pushes = pending = 0
    total_profit = 0.0
    total_units = 0.0

    for tb in bets:
        items.append(_tracked_bet_to_response(tb))
        total_units += tb.units or 1.0
        if tb.result == "win":
            wins += 1
            total_profit += tb.profit_loss or 0.0
        elif tb.result == "loss":
            losses += 1
            total_profit += tb.profit_loss or 0.0
        elif tb.result == "push":
            pushes += 1
        else:
            pending += 1

    graded = wins + losses
    win_rate = round(wins / graded, 4) if graded > 0 else None

    return TrackedBetsListResponse(
        bets=items,
        total_bets=len(items),
        wins=wins,
        losses=losses,
        pushes=pushes,
        pending=pending,
        win_rate=win_rate,
        total_profit=round(total_profit, 2),
        total_units_wagered=round(total_units, 2),
    )


@router.delete("/tracked/{tracked_id}")
async def delete_tracked_bet(
    tracked_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Remove a tracked bet."""
    result = await session.execute(
        select(TrackedBet).where(TrackedBet.id == tracked_id)
    )
    tb = result.scalar_one_or_none()
    if not tb:
        raise HTTPException(status_code=404, detail="Tracked bet not found")
    await session.delete(tb)
    await session.flush()
    return {"ok": True}


@router.post("/tracked/settle")
async def settle_tracked_bets(
    session: AsyncSession = Depends(get_session),
):
    """Auto-settle tracked bets for games that are final."""
    result = await session.execute(
        select(TrackedBet)
        .join(Game, Game.id == TrackedBet.game_id)
        .where(
            TrackedBet.result.is_(None),
            func.lower(Game.status).in_(GAME_FINAL_STATUSES),
        )
    )
    unsettled = result.scalars().all()
    settled_count = 0

    for tb in unsettled:
        game_result = await session.execute(
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(Game.id == tb.game_id)
        )
        game = game_result.scalar_one_or_none()
        if not game or game.home_score is None or game.away_score is None:
            continue

        was_correct = _grade_tracked_bet(tb, game)
        if was_correct is None:
            continue

        tb.result = "win" if was_correct else "loss"
        if was_correct:
            # Calculate profit from American odds
            odds = tb.odds
            if odds and odds > 0:
                tb.profit_loss = round((odds / 100) * tb.units, 2)
            elif odds and odds < 0:
                tb.profit_loss = round((100 / abs(odds)) * tb.units, 2)
            else:
                tb.profit_loss = round(1.0 * tb.units, 2)
        else:
            tb.profit_loss = round(-1.0 * tb.units, 2)
        tb.settled_at = datetime.utcnow()
        settled_count += 1

    if settled_count > 0:
        await session.flush()

    return {"settled": settled_count}


@router.delete("/tracked/all")
async def clear_all_tracked_bets(
    session: AsyncSession = Depends(get_session),
):
    """Clear all tracked bets (reset history)."""
    await session.execute(delete(TrackedBet))
    await session.flush()
    return {"ok": True}


def _tracked_bet_to_response(tb: TrackedBet) -> TrackedBetResponse:
    return TrackedBetResponse(
        id=tb.id,
        prediction_id=tb.prediction_id,
        game_id=tb.game_id,
        bet_type=tb.bet_type,
        prediction_value=tb.prediction_value,
        confidence=tb.confidence,
        odds=tb.odds,
        implied_probability=tb.implied_probability,
        edge=tb.edge,
        units=tb.units,
        phase=tb.phase,
        reasoning=tb.reasoning,
        home_team_name=tb.home_team_name,
        away_team_name=tb.away_team_name,
        home_team_abbr=tb.home_team_abbr,
        away_team_abbr=tb.away_team_abbr,
        game_date=tb.game_date,
        result=tb.result,
        profit_loss=tb.profit_loss,
        settled_at=str(tb.settled_at) if tb.settled_at else None,
        created_at=str(tb.created_at) if tb.created_at else None,
    )


def _grade_tracked_bet(tb: TrackedBet, game: Game) -> Optional[bool]:
    """Determine if a tracked bet won. Returns True/False or None if unknown."""
    hs = game.home_score
    aws = game.away_score
    val = tb.prediction_value

    if tb.bet_type == "ml":
        home_abbr = game.home_team.abbreviation if game.home_team else ""
        if val == home_abbr:
            return hs > aws
        else:
            return aws > hs

    elif tb.bet_type == "total":
        total = hs + aws
        if "over" in val:
            try:
                line = float(val.split("_")[1])
            except (IndexError, ValueError):
                return None
            return total > line
        elif "under" in val:
            try:
                line = float(val.split("_")[1])
            except (IndexError, ValueError):
                return None
            return total < line

    elif tb.bet_type == "spread":
        try:
            parts = val.split("_")
            team_abbr = parts[0]
            spread_val = float(parts[1])
        except (IndexError, ValueError):
            return None
        margin = hs - aws
        home_abbr = game.home_team.abbreviation if game.home_team else ""
        if team_abbr == home_abbr:
            return margin + spread_val > 0
        else:
            return -margin + spread_val > 0

    return None


# ---------------------------------------------------------------------------
# Legacy endpoints (kept for compatibility)
# ---------------------------------------------------------------------------

@router.get("/history", response_model=TrackedBetsListResponse)
async def get_prediction_history(
    session: AsyncSession = Depends(get_session),
):
    """Return tracked bets as the performance history."""
    return await list_tracked_bets(session=session)


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


@router.post("/regenerate", response_model=GenerateResult)
async def regenerate_predictions(
    session: AsyncSession = Depends(get_session),
):
    """Delete ALL of today's predictions and regenerate from scratch.

    Full pipeline:
      1. Sync today's schedule from NHL API (pick up status changes)
      2. Delete ALL existing predictions for today
      3. Sync fresh odds from sportsbooks
      4. Regenerate predictions with latest data
    """
    today = date.today()
    steps: list[str] = []

    # Step 1: Sync today's schedule from NHL API so game statuses,
    # scores, and any newly-added games are up to date.
    schedule_synced = 0
    try:
        async with session.begin_nested():
            from app.scrapers.nhl_api import NHLScraper

            scraper = NHLScraper()
            try:
                synced_games = await scraper.sync_schedule(session, str(today))
                schedule_synced = len(synced_games) if synced_games else 0
                await session.flush()
                session.expire_all()
                logger.info("Regenerate: schedule sync updated %s games", schedule_synced)
            finally:
                await scraper.close()
        steps.append(f"schedule synced ({schedule_synced} games)")
    except Exception as exc:
        logger.warning("Regenerate: schedule sync failed: %s", exc)
        steps.append(f"schedule sync failed: {exc}")

    # Step 2: Delete ALL predictions for today's games (not just prematch,
    # not just market types — nuke everything so the prematch lock is
    # fully cleared).  BetResults cascade-delete via the ORM relationship,
    # but bulk delete bypasses that, so delete BetResults first.
    today_pred_ids = await session.execute(
        select(Prediction.id)
        .join(Game, Game.id == Prediction.game_id)
        .where(Game.date == today)
    )
    pred_ids = [row[0] for row in today_pred_ids.all()]

    deleted = 0
    if pred_ids:
        # Delete child BetResults first (FK constraint)
        await session.execute(
            delete(BetResult).where(BetResult.prediction_id.in_(pred_ids))
        )
        # Then delete the predictions themselves
        await session.execute(
            delete(Prediction).where(Prediction.id.in_(pred_ids))
        )
        deleted = len(pred_ids)
        await session.flush()

    logger.info(
        "Regenerate: deleted %d predictions for %s", deleted, today,
    )
    steps.append(f"cleared {deleted} predictions")

    # Step 3: Sync fresh odds from sportsbooks
    odds_matched = 0
    try:
        async with session.begin_nested():
            from app.scrapers.odds_multi import MultiSourceOddsScraper

            async with MultiSourceOddsScraper() as odds_scraper:
                matched = await odds_scraper.sync_odds(session)
                odds_matched = len(matched) if matched else 0
                logger.info("Regenerate: odds sync matched %d games", odds_matched)
                await session.flush()
                session.expire_all()
        steps.append(f"odds synced ({odds_matched} games)")
    except Exception as exc:
        logger.warning("Regenerate: odds sync failed: %s", exc)
        steps.append(f"odds sync failed: {exc}")

    # Step 4: Generate fresh predictions (prematch lock is fully cleared)
    try:
        count = await _try_generate_predictions(session, target_date=today)
    except HTTPException:
        # _try_generate_predictions raises HTTPException on failure.
        # If we deleted predictions but failed to regenerate, the
        # transaction rollback from the exception will restore the
        # old predictions — which is what we want.
        logger.error("Regenerate: prediction generation failed, rolling back deletes")
        raise

    steps.append(f"generated {count} predictions")

    # Safety: if we deleted predictions but generated 0, log a warning.
    # The best-bets endpoint will try to generate on the next fetch.
    if deleted > 0 and count == 0:
        logger.warning(
            "Regenerate: deleted %d predictions but generated 0. "
            "Games may have changed status or feature extraction failed.",
            deleted,
        )

    msg = " → ".join(steps)
    logger.info("Regenerate complete: %s", msg)

    return GenerateResult(
        success=True,
        message=msg,
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
