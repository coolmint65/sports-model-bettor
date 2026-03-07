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
from app.database import get_session
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
# Backfill prediction odds from fresh Game records
# ---------------------------------------------------------------------------

async def _backfill_prediction_odds(
    session: AsyncSession, target_date: date
) -> int:
    """Update all predictions for the given date with fresh odds from Game records.

    After odds are synced to Game records, the corresponding Prediction rows
    may still carry stale (or NULL) ``odds_implied_prob`` and ``edge`` values.
    This function reads the current Game odds and writes them back onto the
    Prediction records so the DB-level filters in best-bets work correctly.

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
    line_display: Optional[str] = None
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
        except Exception as exc:
            logger.warning("Today predictions: auto-generation failed: %s", exc)

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
    logger.info("=== BEST-BETS START for %s ===", today)

    # Step 0: Ensure games exist for today.  If the schedule hasn't been
    # synced yet (e.g. user navigated directly to Best Bets before the
    # dashboard loaded), there are 0 games and nothing downstream works.
    game_count_result = await session.execute(
        select(func.count(Game.id)).where(
            Game.date == today,
            ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
        )
    )
    games_today = game_count_result.scalar() or 0
    logger.info("Best-bets step 0: %d non-final games for today", games_today)

    if games_today == 0:
        try:
            async with session.begin_nested():
                from app.scrapers.nhl_api import NHLScraper

                scraper = NHLScraper()
                try:
                    synced = await scraper.sync_schedule(session, str(today))
                    synced_count = len(synced) if synced else 0
                    logger.info(
                        "Best-bets step 0: schedule sync added %d games", synced_count
                    )
                    await session.flush()
                    session.expire_all()
                finally:
                    await scraper.close()
        except Exception as exc:
            logger.warning("Best-bets step 0: schedule sync failed: %s", exc)

        # Re-check
        game_count_result = await session.execute(
            select(func.count(Game.id)).where(
                Game.date == today,
                ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
            )
        )
        games_today = game_count_result.scalar() or 0
        logger.info("Best-bets step 0: after sync, %d non-final games", games_today)

    # Step 1: Always refresh odds when stale, even if predictions exist.
    # Without this, odds go stale after initial generation and the
    # displayed edge/confidence become unreliable ("fake edge").
    #
    # Use a shorter refresh interval when live games are in progress —
    # live odds move fast and 15-minute-old odds produce phantom edges.
    _LIVE_STATUSES = ("in_progress", "live")
    live_count_result = await session.execute(
        select(func.count(Game.id)).where(
            Game.date == today,
            func.lower(Game.status).in_(_LIVE_STATUSES),
        )
    )
    has_live_games = (live_count_result.scalar() or 0) > 0
    refresh_minutes = 2 if has_live_games else settings.odds_refresh_interval_minutes

    stale_cutoff = datetime.now(timezone.utc) - timedelta(minutes=refresh_minutes)
    stale_result = await session.execute(
        select(func.count(Game.id)).where(
            Game.date == today,
            ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
            (Game.odds_updated_at.is_(None)) | (Game.odds_updated_at < stale_cutoff),
        )
    )
    needs_odds_refresh = (stale_result.scalar() or 0) > 0
    logger.info(
        "Best-bets step 1: needs_odds_refresh=%s (interval=%dmin, live=%s)",
        needs_odds_refresh, refresh_minutes, has_live_games,
    )

    if needs_odds_refresh:
        try:
            async with session.begin_nested():
                from app.scrapers.odds_multi import MultiSourceOddsScraper

                async with MultiSourceOddsScraper() as odds_scraper:
                    matched = await odds_scraper.sync_odds(session)
                    logger.info(
                        "Best-bets step 1: odds refresh matched %d games",
                        len(matched) if matched else 0,
                    )
                    await session.flush()
                    session.expire_all()
        except Exception as exc:
            logger.warning("Best-bets step 1: odds refresh failed: %s", exc)

    # Log odds state after refresh
    odds_check = await session.execute(
        select(
            Game.id,
            Game.home_moneyline,
            Game.over_under_line,
            Game.home_spread_line,
            Game.odds_updated_at,
        ).where(
            Game.date == today,
            ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
        )
    )
    for row in odds_check.all():
        logger.debug(
            "Best-bets odds state: game=%d ml=%s ou=%s spread=%s updated=%s",
            row[0], row[1], row[2], row[3], row[4],
        )

    # Step 2: Generate predictions if none exist yet.
    # Check for ANY prediction phase (prematch or live) — not just
    # prematch — so that live-only games don't trigger unnecessary
    # regeneration that silently produces duplicates or empties.
    existing_pred_count = await session.execute(
        select(func.count(Prediction.id))
        .join(Game, Game.id == Prediction.game_id)
        .where(
            Game.date == today,
            ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
        )
    )
    pred_count_val = existing_pred_count.scalar() or 0
    has_predictions = pred_count_val > 0
    logger.info("Best-bets step 2: existing predictions=%d", pred_count_val)

    if not has_predictions:
        # Generate / update predictions (respects prematch locks)
        try:
            async with session.begin_nested():
                pred_count = await _try_generate_predictions(session, target_date=today)
                await session.flush()
                logger.info("Best-bets step 2: generated %d predictions", pred_count)
        except Exception as exc:
            logger.warning(
                "Best-bets step 2: prediction generation failed: %s",
                getattr(exc, 'detail', str(exc)),
            )

    # Step 3: Backfill prediction odds from fresh Game records.
    # Predictions may have been generated before odds were synced
    # (e.g., from /predictions/today or a failed odds fetch), leaving
    # odds_implied_prob as NULL.  This step reads the current odds on
    # the Game records and writes them back to every Prediction so the
    # DB-level filter below can find them.  Handles ML, totals, AND
    # spreads — keeping all bet types in sync.
    try:
        async with session.begin_nested():
            backfilled = await _backfill_prediction_odds(session, today)
            if backfilled:
                logger.info("Best-bets step 3: backfilled odds on %d predictions", backfilled)
    except Exception as exc:
        logger.warning("Best-bets step 3: odds backfill failed: %s", exc)

    # Log prediction state before filtering
    all_pred_result = await session.execute(
        select(
            Prediction.id,
            Prediction.bet_type,
            Prediction.prediction_value,
            Prediction.confidence,
            Prediction.odds_implied_prob,
            Prediction.edge,
            Prediction.recommended,
            Prediction.phase,
            Game.status,
        )
        .join(Game, Game.id == Prediction.game_id)
        .where(
            Game.date == today,
            ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
        )
    )
    all_pred_rows = all_pred_result.all()
    logger.info("Best-bets step 4: %d total predictions before filtering", len(all_pred_rows))
    null_odds = sum(1 for r in all_pred_rows if r[4] is None)
    has_odds = sum(1 for r in all_pred_rows if r[4] is not None)
    market_type = sum(1 for r in all_pred_rows if r[1] in MARKET_BET_TYPES)
    logger.info(
        "Best-bets step 4: null_odds=%d, has_odds=%d, market_types=%d",
        null_odds, has_odds, market_type,
    )
    for r in all_pred_rows[:20]:
        logger.debug(
            "  pred id=%s type=%s val=%s conf=%.3f impl=%s edge=%s rec=%s phase=%s gstatus=%s",
            r[0], r[1], r[2], r[3] or 0, r[4], r[5], r[6], r[7], r[8],
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
    logger.info("Best-bets step 4: %d eligible after filtering (implied<%.2f, not null, market type)", len(all_eligible), max_implied)

    # Fallback: when odds data is unavailable (scraper failed, lines not
    # posted yet, etc.), all predictions have NULL odds_implied_prob and
    # the strict filter above returns nothing.  Re-query without the odds
    # requirement so the dashboard still shows the top picks ranked by
    # confidence alone rather than an empty "No best bets" message.
    if not all_eligible:
        fallback_result = await session.execute(
            select(Prediction)
            .options(selectinload(Prediction.result))
            .join(Game, Game.id == Prediction.game_id)
            .where(
                Game.date == today,
                ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                Prediction.bet_type.in_(MARKET_BET_TYPES),
            )
        )
        all_eligible = fallback_result.scalars().all()
        logger.info(
            "Best-bets step 4 fallback: %d predictions without odds filter",
            len(all_eligible),
        )

    # Split into recommended and fallback pools
    recommended = [p for p in all_eligible if p.recommended]
    fallback = [p for p in all_eligible if not p.recommended]
    logger.info("Best-bets step 4: recommended=%d, fallback=%d", len(recommended), len(fallback))

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
        fresh_implied = pred.odds_implied_prob
        fresh_edge = pred.edge

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
                # Recompute edge from current sportsbook odds
                if live_odds is not None:
                    fresh_implied = american_to_implied(live_odds)
                    if fresh_implied is not None and pred.confidence is not None:
                        fresh_edge = round(pred.confidence - fresh_implied, 4)

            elif pred.bet_type == "total":
                # Parse the line from prediction_value (e.g., "over_4.5" → 4.5)
                is_over = pred.prediction_value and "over" in pred.prediction_value
                total_found = False
                if game_obj.all_total_lines and pred.prediction_value:
                    try:
                        parts = pred.prediction_value.split("_", 1)
                        if len(parts) == 2:
                            pred_line = float(parts[1])
                            all_tl = game_obj.all_total_lines
                            if isinstance(all_tl, str):
                                import json
                                all_tl = json.loads(all_tl)
                            for tl in (all_tl or []):
                                if abs(tl.get("line", 0) - pred_line) < 0.01:
                                    price_key = "over_price" if is_over else "under_price"
                                    live_odds = tl.get(price_key)
                                    if live_odds is not None:
                                        total_found = True
                                    break
                    except (ValueError, TypeError, KeyError):
                        pass

                # Fall back to the primary O/U prices on the Game.
                if not total_found:
                    if is_over:
                        live_odds = game_obj.over_price
                    else:
                        live_odds = game_obj.under_price

                # Recompute edge from current sportsbook odds
                if live_odds is not None:
                    fresh_implied = american_to_implied(live_odds)
                    if fresh_implied is not None and pred.confidence is not None:
                        fresh_edge = round(pred.confidence - fresh_implied, 4)

            elif pred.bet_type == "spread":
                # Read actual current sportsbook spread prices so odds,
                # edge, and the juice filter stay accurate as lines move.
                #
                # pred.prediction_value has the form "ABBR_+1.5" or
                # "ABBR_-1.5".  First determine which side (home/away)
                # the prediction is on.
                home_team_result = await session.execute(
                    select(Team).where(Team.id == game_obj.home_team_id)
                )
                home_team_obj = home_team_result.scalar_one_or_none()

                pred_is_home = (
                    home_team_obj
                    and pred.prediction_value
                    and pred.prediction_value.startswith(home_team_obj.abbreviation)
                )

                # Try to find the exact line in all_spread_lines (covers
                # both primary and alternate spread lines).
                spread_found = False
                if game_obj.all_spread_lines and pred.prediction_value:
                    try:
                        # Parse the spread value from prediction_value
                        # e.g. "LAK_-1.5" → spread_val = 1.5
                        parts = pred.prediction_value.rsplit("_", 1)
                        if len(parts) == 2:
                            spread_val = abs(float(parts[1]))
                            all_sl = game_obj.all_spread_lines
                            if isinstance(all_sl, str):
                                import json
                                all_sl = json.loads(all_sl)
                            for sl in (all_sl or []):
                                if abs(sl.get("line", 0) - spread_val) < 0.01:
                                    if pred_is_home:
                                        live_odds = sl.get("home_price")
                                    else:
                                        live_odds = sl.get("away_price")
                                    if live_odds is not None:
                                        spread_found = True
                                    break
                    except (ValueError, TypeError, KeyError):
                        pass

                # Fall back to the primary spread prices on the Game.
                if not spread_found:
                    if pred_is_home and game_obj.home_spread_price:
                        live_odds = game_obj.home_spread_price
                    elif not pred_is_home and game_obj.away_spread_price:
                        live_odds = game_obj.away_spread_price

                # Last resort: derive from stored implied probability.
                if live_odds is None:
                    live_odds = implied_prob_to_american(pred.odds_implied_prob)

                # Recompute edge from current sportsbook spread odds,
                # just like we already do for ML and totals.
                if live_odds is not None:
                    fresh_implied = american_to_implied(live_odds)
                    if fresh_implied is not None and pred.confidence is not None:
                        fresh_edge = round(pred.confidence - fresh_implied, 4)

        # Juice filter: exclude bets whose display odds are steeper than
        # the configured threshold (default -170). This catches cases
        # where the DB implied-prob filter passes but actual odds are bad.
        if live_odds is not None and live_odds < 0 and live_odds < settings.best_bet_max_favorite:
            return None

        units = calculate_units(fresh_edge, pred.confidence)
        # Use the actual game status to determine phase — a prediction
        # created prematch is effectively "live" once the game starts.
        phase = "live" if game_status and game_status.lower() in ("in_progress", "live") else getattr(pred, "phase", "prematch")

        # Build a human-readable line display for the bet card
        # e.g., "O 5.5 (-110)", "BOS -1.5 (-130)", "BOS +145"
        line_display = None
        if game_obj and live_odds is not None:
            odds_str = f"+{round(live_odds)}" if live_odds > 0 else str(round(live_odds))
            if pred.bet_type == "total":
                # Extract the actual line from prediction_value (e.g., "over_4.5" → 4.5)
                if pred.prediction_value:
                    side = "O" if "over" in pred.prediction_value else "U"
                    try:
                        pred_line = pred.prediction_value.split("_", 1)[1]
                        line_display = f"{side} {pred_line} ({odds_str})"
                    except (IndexError, ValueError):
                        pass
            elif pred.bet_type == "spread":
                if pred.prediction_value:
                    parts = pred.prediction_value.rsplit("_", 1)
                    if len(parts) == 2:
                        team_abbr = parts[0]
                        spread_val = parts[1]
                        if not spread_val.startswith("+") and not spread_val.startswith("-"):
                            spread_val = f"+{spread_val}"
                        line_display = f"{team_abbr} {spread_val} ({odds_str})"
            elif pred.bet_type == "ml":
                team_abbr = pred.prediction_value or ""
                line_display = f"{team_abbr} ML ({odds_str})"

        return BestBet(
            prediction_id=detail.id,
            game_id=detail.game_id,
            game_date=detail.game_date,
            home_team=detail.home_team,
            away_team=detail.away_team,
            bet_type=detail.bet_type,
            prediction_value=detail.prediction_value,
            confidence=detail.confidence,
            edge=fresh_edge,
            odds_implied_prob=fresh_implied,
            reasoning=detail.reasoning,
            game_status=game_status,
            odds_display=live_odds,
            line_display=line_display,
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

    units = body.units or calculate_units(pred.edge, pred.confidence)

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
        units=units,
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

    items: List[TrackedBetResponse] = []
    wins = losses = pushes = pending = 0
    total_profit = 0.0
    total_units = 0.0
    dirty = False

    for tb in bets:
        # Load the game for start_time and status
        game_result = await session.execute(
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(Game.id == tb.game_id)
        )
        game = game_result.scalar_one_or_none()

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
            pred_result = await session.execute(
                select(Prediction).where(Prediction.id == tb.prediction_id)
            )
            pred = pred_result.scalar_one_or_none()
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
                tb.units = calculate_units(tb.edge, tb.confidence)
                dirty = True

        items.append(_tracked_bet_to_response(tb, game))
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
        total_units_wagered=round(total_units, 2),
    )


class TrackedBetUpdateRequest(BaseModel):
    units: Optional[float] = None
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

    if body.units is not None:
        tb.units = body.units
    elif body.prediction_id is not None:
        # Recalculate units from new prediction data
        tb.units = calculate_units(tb.edge, tb.confidence)

    await session.flush()
    return _tracked_bet_to_response(tb, game)


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
        tb.settled_at = datetime.now(timezone.utc)
        if tb.locked_at is None:
            tb.locked_at = tb.settled_at
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
        units=tb.units,
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


@router.get("/debug")
async def debug_pipeline(
    session: AsyncSession = Depends(get_session),
):
    """Diagnostic endpoint: show the full state of the prediction pipeline.

    Returns raw counts, statuses, and filter results at each stage so you
    can see exactly where best bets generation breaks down.
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

    # Step 5: Backfill odds onto freshly generated predictions.
    # Even though odds were synced in step 3, the prediction generator
    # may have failed to pick up odds for some bet types, or odds may
    # have been missing for certain games.  This ensures every
    # prediction record has the latest odds data.
    try:
        async with session.begin_nested():
            backfilled = await _backfill_prediction_odds(session, today)
            if backfilled:
                steps.append(f"backfilled odds on {backfilled} predictions")
    except Exception as exc:
        logger.warning("Regenerate: odds backfill failed: %s", exc)

    # Safety: if we deleted predictions but generated 0, log a warning.
    # The best-bets endpoint will try to generate on the next fetch.
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
