"""
Predictions API routes.

Provides endpoints for generating, retrieving, and evaluating model
predictions for NHL games, including user-tracked bet management
and historical performance tracking.

Predictions are split into two phases:
  - **prematch**: Generated once before the game starts.  These are
    locked and never regenerated so the user always sees the original
    pre-game pick.
  - **live**: Generated (and updated) while a game is in progress.
    Updates are throttled so the pick doesn't flip-flop on every sync.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional

logger = logging.getLogger(__name__)

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import and_, delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.constants import GAME_FINAL_STATUSES, MARKET_BET_TYPES, composite_pick_score
from app.database import get_session, get_session_context, get_write_session_context
from app.services.odds import american_to_implied, fresh_implied_prob, implied_to_american as implied_prob_to_american
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



# ---------------------------------------------------------------------------
# Backfill prediction odds from fresh Game records
# ---------------------------------------------------------------------------

async def _backfill_prediction_odds(
    session: AsyncSession, target_date: date
) -> int:
    """Update all predictions for the given date with fresh odds from Game records.

    After odds are synced to Game records, the corresponding Prediction rows
    may still carry stale (or NULL) ``odds_implied_prob`` and ``edge`` values.
    This function reads the current Game odds and writes them back onto the
    Prediction records so the DB-level filters work correctly.

    Handles ALL bet types — market bets (ML, totals, spreads) AND prop bets
    (BTTS, first goal, overtime, period markets, etc.) — via the centralized
    ``fresh_implied_prob()`` helper.

    Returns the number of predictions updated.
    """
    # Fetch all non-final games for the date with their teams
    game_result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(
            Game.date == target_date,
            ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
        )
    )
    games = {g.id: g for g in game_result.scalars().all()}
    if not games:
        return 0

    # Fetch ALL predictions for these games (market + prop types)
    pred_result = await session.execute(
        select(Prediction).where(
            Prediction.game_id.in_(list(games.keys())),
        )
    )
    predictions = pred_result.scalars().all()

    updated = 0
    for pred in predictions:
        game = games.get(pred.game_id)
        if not game:
            continue

        fresh_implied = fresh_implied_prob(pred, game)
        if fresh_implied is None:
            continue

        # Sanity check: implied probability must be in (0, 1)
        if not (0 < fresh_implied < 1):
            logger.warning(
                "Skipping invalid implied prob %.4f for prediction %d",
                fresh_implied, pred.id,
            )
            continue

        # Skip if nothing changed (within rounding tolerance)
        if (
            pred.odds_implied_prob is not None
            and abs(pred.odds_implied_prob - fresh_implied) < 0.0001
        ):
            continue

        fresh_edge = round(pred.confidence - fresh_implied, 4) if pred.confidence else None

        # Update the prediction record
        pred.odds_implied_prob = fresh_implied
        pred.edge = fresh_edge
        pred.recommended = (
            (pred.confidence or 0) >= settings.min_confidence
            and (fresh_edge or 0) >= settings.min_edge
            and fresh_implied < settings.best_bet_max_implied
        )
        updated += 1

    if updated > 0:
        await session.flush()
        logger.info(
            "Backfilled odds for %d/%d predictions on %s",
            updated, len(predictions), target_date,
        )

    return updated


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



class TrackedBetRequest(BaseModel):
    prediction_id: int


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
    phase: Optional[str] = None
    reasoning: Optional[str] = None
    home_team_name: Optional[str] = None
    away_team_name: Optional[str] = None
    home_team_abbr: Optional[str] = None
    away_team_abbr: Optional[str] = None
    game_date: Optional[date] = None
    locked: bool = False
    locked_at: Optional[str] = None
    result: Optional[str] = None
    profit_loss: Optional[float] = None
    settled_at: Optional[str] = None
    created_at: Optional[str] = None
    game_start_time: Optional[str] = None
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

def _build_prediction_detail_from_game(
    pred: Prediction, game: Optional[Game]
) -> PredictionDetail:
    """Build a PredictionDetail from a pre-loaded Prediction and Game."""
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


async def _build_prediction_detail(
    pred: Prediction, session: AsyncSession
) -> PredictionDetail:
    game_result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.id == pred.game_id)
    )
    game: Optional[Game] = game_result.scalar_one_or_none()
    return _build_prediction_detail_from_game(pred, game)


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

    # Batch-load all games to avoid N+1
    game_ids = {p.game_id for p in predictions if p.game_id}
    games_by_id = {}
    if game_ids:
        games_result = await session.execute(
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(Game.id.in_(game_ids))
        )
        games_by_id = {g.id: g for g in games_result.scalars().all()}

    details: List[PredictionDetail] = []
    for pred in predictions:
        detail = _build_prediction_detail_from_game(pred, games_by_id.get(pred.game_id))
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
            is_live = game_status.lower() in ("in_progress", "live") if game_status else False
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
        logger.error("Failed to generate predictions: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to generate predictions",
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
        except Exception as exc:
            logger.warning("Today predictions: auto-generation failed: %s", exc)

    return TodayPredictionsResponse(
        date=today,
        prediction_count=len(predictions),
        predictions=predictions,
    )


# ---------------------------------------------------------------------------
# Tracked bets helpers
# ---------------------------------------------------------------------------

def _resolve_odds(
    bet_type: str, prediction_value: str, game: Game
) -> Optional[float]:
    """Resolve current sportsbook odds for a bet from the Game record."""
    if bet_type == "ml":
        if game.home_team and prediction_value == game.home_team.abbreviation:
            return game.home_moneyline
        return game.away_moneyline
    elif bet_type == "total":
        if prediction_value and "over" in prediction_value:
            return game.over_price
        return game.under_price
    elif bet_type == "spread":
        is_home = (
            game.home_team
            and prediction_value
            and prediction_value.startswith(game.home_team.abbreviation)
        )
        if is_home:
            return game.home_spread_price
        return game.away_spread_price
    return None


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

    # Resolve sportsbook odds for snapshot
    odds_val = _resolve_odds(pred.bet_type, pred.prediction_value, game) if game else None

    tracked = TrackedBet(
        prediction_id=pred.id,
        game_id=pred.game_id,
        bet_type=pred.bet_type,
        prediction_value=pred.prediction_value,
        confidence=pred.confidence,
        odds=odds_val,
        implied_probability=pred.odds_implied_prob,
        edge=pred.edge,
        units=1.0,
        phase=getattr(pred, "phase", "prematch"),
        reasoning=pred.reasoning,
        home_team_name=game.home_team.name if game and game.home_team else None,
        away_team_name=game.away_team.name if game and game.away_team else None,
        home_team_abbr=game.home_team.abbreviation if game and game.home_team else None,
        away_team_abbr=game.away_team.abbreviation if game and game.away_team else None,
        game_date=game.date if game else None,
    )
    # Auto-lock if game has already started
    if game and game.status and game.status.lower() in (
        *GAME_FINAL_STATUSES, "in_progress", "live"
    ):
        tracked.locked_at = datetime.now(timezone.utc)

    session.add(tracked)
    await session.flush()

    return _tracked_bet_to_response(tracked, game)


@router.get("/tracked", response_model=TrackedBetsListResponse)
async def list_tracked_bets(
    session: AsyncSession = Depends(get_session),
):
    """Return all tracked bets with aggregate stats.

    For unlocked (pre-game) bets, automatically syncs the latest
    prediction data so the user always sees current model output.
    Once a game starts (or is final), the bet is locked permanently.
    """
    result = await session.execute(
        select(TrackedBet).order_by(TrackedBet.created_at.desc())
    )
    bets = result.scalars().all()

    # Batch-load all games and predictions to avoid N+1 queries
    game_ids = {tb.game_id for tb in bets if tb.game_id}
    pred_ids = {tb.prediction_id for tb in bets if tb.prediction_id}

    games_by_id = {}
    if game_ids:
        games_result = await session.execute(
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(Game.id.in_(game_ids))
        )
        games_by_id = {g.id: g for g in games_result.scalars().all()}

    preds_by_id = {}
    if pred_ids:
        preds_result = await session.execute(
            select(Prediction).where(Prediction.id.in_(pred_ids))
        )
        preds_by_id = {p.id: p for p in preds_result.scalars().all()}

    items: List[TrackedBetResponse] = []
    wins = losses = pushes = pending = 0
    total_profit = 0.0
    dirty = False

    for tb in bets:
        game = games_by_id.get(tb.game_id)

        # Auto-lock: once the game starts or is final, freeze the bet
        if tb.locked_at is None and game:
            game_started = (
                game.status
                and game.status.lower() in (*GAME_FINAL_STATUSES, "in_progress", "live")
            )
            if game_started:
                tb.locked_at = datetime.now(timezone.utc)
                dirty = True

        # Auto-refresh: for unlocked, unsettled bets, sync from
        # the latest Prediction data so the user sees current values
        if tb.locked_at is None and tb.result is None and tb.prediction_id is not None:
            pred = preds_by_id.get(tb.prediction_id)
            if pred:
                # Sync snapshot from the prediction
                tb.bet_type = pred.bet_type
                tb.prediction_value = pred.prediction_value
                tb.confidence = pred.confidence
                tb.implied_probability = pred.odds_implied_prob
                tb.edge = pred.edge
                tb.phase = getattr(pred, "phase", "prematch")
                tb.reasoning = pred.reasoning
                # Refresh odds from game
                if game:
                    odds_val = _resolve_odds(pred.bet_type, pred.prediction_value, game)
                    if odds_val is not None:
                        tb.odds = odds_val
                dirty = True

        items.append(_tracked_bet_to_response(tb, game))
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

    if dirty:
        await session.flush()

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
    )


class TrackedBetUpdateRequest(BaseModel):
    prediction_id: Optional[int] = None  # swap to a different prediction


@router.put("/tracked/{tracked_id}", response_model=TrackedBetResponse)
async def update_tracked_bet(
    tracked_id: int,
    body: TrackedBetUpdateRequest,
    session: AsyncSession = Depends(get_session),
):
    """Update a tracked bet (only allowed before the game starts)."""
    result = await session.execute(
        select(TrackedBet).where(TrackedBet.id == tracked_id)
    )
    tb = result.scalar_one_or_none()
    if not tb:
        raise HTTPException(status_code=404, detail="Tracked bet not found")
    if tb.locked_at is not None:
        raise HTTPException(
            status_code=409,
            detail="Bet is locked — game has started or is final",
        )

    game_result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.id == tb.game_id)
    )
    game = game_result.scalar_one_or_none()

    # If swapping to a new prediction, refresh all snapshot data
    if body.prediction_id is not None:
        pred_result = await session.execute(
            select(Prediction).where(Prediction.id == body.prediction_id)
        )
        pred = pred_result.scalar_one_or_none()
        if not pred:
            raise HTTPException(status_code=404, detail="Prediction not found")
        tb.prediction_id = pred.id
        tb.game_id = pred.game_id
        tb.bet_type = pred.bet_type
        tb.prediction_value = pred.prediction_value
        tb.confidence = pred.confidence
        tb.implied_probability = pred.odds_implied_prob
        tb.edge = pred.edge
        tb.phase = getattr(pred, "phase", "prematch")
        tb.reasoning = pred.reasoning
        if game:
            odds_val = _resolve_odds(pred.bet_type, pred.prediction_value, game)
            if odds_val is not None:
                tb.odds = odds_val

    await session.flush()
    return _tracked_bet_to_response(tb, game)


@router.delete("/tracked/all")
async def clear_all_tracked_bets(
    session: AsyncSession = Depends(get_session),
):
    """Clear all tracked bets (reset history)."""
    await session.execute(delete(TrackedBet))
    await session.flush()
    return {"ok": True}


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
    """Auto-settle tracked bets and predictions for games that are final."""
    from app.services.settlement import settle_completed_games

    result = await settle_completed_games(session)
    return {
        "settled": result["tracked_bets_settled"],
        "predictions_graded": result["predictions_graded"],
    }


def _tracked_bet_to_response(
    tb: TrackedBet, game: Optional[Game] = None
) -> TrackedBetResponse:
    locked = tb.locked_at is not None
    game_start_time = None
    if game and game.start_time:
        game_start_time = game.start_time.isoformat()
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
        phase=tb.phase,
        reasoning=tb.reasoning,
        home_team_name=tb.home_team_name,
        away_team_name=tb.away_team_name,
        home_team_abbr=tb.home_team_abbr,
        away_team_abbr=tb.away_team_abbr,
        game_date=tb.game_date,
        locked=locked,
        locked_at=str(tb.locked_at) if tb.locked_at else None,
        result=tb.result,
        profit_loss=tb.profit_loss,
        settled_at=str(tb.settled_at) if tb.settled_at else None,
        created_at=str(tb.created_at) if tb.created_at else None,
        game_start_time=game_start_time,
    )


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


@router.get("/debug")
async def debug_pipeline(
    session: AsyncSession = Depends(get_session),
):
    """Diagnostic endpoint: show the full state of the prediction pipeline.

    Returns raw counts, statuses, and filter results at each stage so you
    can see exactly where prediction generation breaks down.
    """
    today = date.today()
    info: dict = {"date": str(today), "steps": []}

    # 1. Games for today
    game_result = await session.execute(
        select(
            Game.id, Game.status, Game.home_moneyline, Game.away_moneyline,
            Game.over_under_line, Game.home_spread_line, Game.odds_updated_at,
        ).where(Game.date == today)
    )
    games_raw = game_result.all()
    game_rows = []
    for g in games_raw:
        game_rows.append({
            "id": g[0], "status": g[1],
            "home_ml": g[2], "away_ml": g[3],
            "ou": g[4], "spread": g[5],
            "odds_updated": str(g[6]) if g[6] else None,
        })
    non_final = [g for g in game_rows if g["status"] and g["status"].lower() not in GAME_FINAL_STATUSES]
    info["games_total"] = len(game_rows)
    info["games_non_final"] = len(non_final)
    info["games"] = game_rows
    info["steps"].append(f"Found {len(game_rows)} games ({len(non_final)} non-final)")

    # 2. Games with odds
    with_ml = sum(1 for g in non_final if g["home_ml"] is not None)
    with_ou = sum(1 for g in non_final if g["ou"] is not None)
    with_spread = sum(1 for g in non_final if g["spread"] is not None)
    info["games_with_moneyline"] = with_ml
    info["games_with_ou"] = with_ou
    info["games_with_spread"] = with_spread
    info["steps"].append(f"Odds: {with_ml} have ML, {with_ou} have O/U, {with_spread} have spread")

    # 3. Predictions
    pred_result = await session.execute(
        select(
            Prediction.id, Prediction.game_id, Prediction.bet_type,
            Prediction.prediction_value, Prediction.confidence,
            Prediction.odds_implied_prob, Prediction.edge,
            Prediction.recommended, Prediction.phase,
        )
        .join(Game, Game.id == Prediction.game_id)
        .where(Game.date == today)
    )
    preds_raw = pred_result.all()
    pred_rows = []
    for p in preds_raw:
        pred_rows.append({
            "id": p[0], "game_id": p[1], "bet_type": p[2],
            "prediction_value": p[3],
            "confidence": round(p[4], 4) if p[4] else None,
            "odds_implied_prob": round(p[5], 4) if p[5] else None,
            "edge": round(p[6], 4) if p[6] else None,
            "recommended": p[7], "phase": p[8],
        })
    info["predictions_total"] = len(pred_rows)
    info["steps"].append(f"Found {len(pred_rows)} predictions total")

    # 4. Filter breakdown
    non_final_preds = [p for p in pred_rows if any(
        g["id"] == p["game_id"] for g in non_final
    )]
    with_odds = [p for p in non_final_preds if p["odds_implied_prob"] is not None]
    null_odds = [p for p in non_final_preds if p["odds_implied_prob"] is None]
    market_type = [p for p in with_odds if p["bet_type"] in MARKET_BET_TYPES]
    under_juice = [p for p in market_type if p["odds_implied_prob"] < settings.best_bet_max_implied]
    recommended_preds = [p for p in under_juice if p["recommended"]]

    info["filter_breakdown"] = {
        "non_final_preds": len(non_final_preds),
        "with_odds_implied": len(with_odds),
        "null_odds_implied": len(null_odds),
        "market_bet_types": len(market_type),
        "under_juice_ceiling": len(under_juice),
        "recommended": len(recommended_preds),
    }
    info["steps"].append(
        f"Filters: {len(non_final_preds)} non-final -> "
        f"{len(with_odds)} have odds -> "
        f"{len(market_type)} market types -> "
        f"{len(under_juice)} under juice -> "
        f"{len(recommended_preds)} recommended"
    )

    # 5. Show null-odds predictions (the most common failure mode)
    info["null_odds_predictions"] = null_odds[:10]

    # 6. Settings
    info["settings"] = {
        "min_confidence": settings.min_confidence,
        "min_edge": settings.min_edge,
        "best_bet_max_implied": settings.best_bet_max_implied,
        "best_bet_max_favorite": settings.best_bet_max_favorite,
        "odds_api_key_set": bool(settings.odds_api_key),
    }

    # 7. All predictions (capped at 30)
    info["predictions"] = pred_rows[:30]

    return info


@router.post("/regenerate", response_model=GenerateResult)
async def regenerate_predictions():
    """Delete ALL of today's predictions and regenerate from scratch.

    Full pipeline:
      1. Sync today's schedule from NHL API (pick up status changes)
      2. Delete ALL existing predictions for today
      3. Sync fresh odds from sportsbooks
      4. Regenerate predictions with latest data

    NOTE: Uses explicit session management (not Depends) to avoid holding
    idle connections during slow HTTP calls — the root cause of QueuePool
    exhaustion when multiple requests overlap.
    """
    today = date.today()
    steps: list[str] = []

    # Skip schedule sync here — it runs every 30 min via the background
    # scheduler and every time via _run_full_sync.  Removing this shaves
    # 5-15 seconds off regeneration, helping stay within the 120s timeout.
    steps.append("schedule sync skipped (runs via scheduler)")

    # Step 2: Delete ALL predictions for today's games (not just prematch,
    # not just market types — nuke everything so the prematch lock is
    # fully cleared).  BetResults cascade-delete via the ORM relationship,
    # but bulk delete bypasses that, so delete BetResults first.
    deleted = 0
    async with get_write_session_context() as session:
        today_pred_ids = await session.execute(
            select(Prediction.id)
            .join(Game, Game.id == Prediction.game_id)
            .where(Game.date == today)
        )
        pred_ids = [row[0] for row in today_pred_ids.all()]

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

    # Step 3: Sync fresh odds (slow HTTP call — own session).
    odds_matched = 0
    try:
        async with get_write_session_context() as write_session:
            from app.scrapers.odds_multi import MultiSourceOddsScraper

            async with MultiSourceOddsScraper() as odds_scraper:
                matched = await odds_scraper.sync_odds(write_session)
                odds_matched = len(matched) if matched else 0
                logger.info("Regenerate: odds sync matched %d games", odds_matched)
        steps.append(f"odds synced ({odds_matched} games)")
    except Exception as exc:
        logger.warning("Regenerate: odds sync failed: %s", exc)
        steps.append(f"odds sync failed: {exc}")

    # Step 4: Generate fresh predictions (prematch lock is fully cleared)
    try:
        async with get_write_session_context() as session:
            count = await _try_generate_predictions(session, target_date=today)
    except HTTPException:
        logger.error("Regenerate: prediction generation failed")
        raise

    steps.append(f"generated {count} predictions")

    # Step 5: Backfill odds onto freshly generated predictions.
    try:
        async with get_write_session_context() as session:
            backfilled = await _backfill_prediction_odds(session, today)
            if backfilled:
                steps.append(f"backfilled odds on {backfilled} predictions")
    except Exception as exc:
        logger.warning("Regenerate: odds backfill failed: %s", exc)

    # Player props sync skipped from regeneration — it runs every 30 min
    # via the background scheduler and is the slowest step (per-event API
    # calls).  Removing it prevents the 120s frontend timeout.
    steps.append("player props: uses cached data (syncs via scheduler)")

    # Safety: if we deleted predictions but generated 0, log a warning.
    if deleted > 0 and count == 0:
        logger.warning(
            "Regenerate: deleted %d predictions but generated 0. "
            "Games may have changed status or feature extraction failed.",
            deleted,
        )

    msg = " -> ".join(steps)
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
