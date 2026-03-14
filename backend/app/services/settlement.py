"""
Auto-settlement service — grades predictions and tracked bets
when games go final.

Called automatically by the background scheduler so users never
need to manually settle bets.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.constants import GAME_FINAL_STATUSES
from app.models.game import Game
from app.models.prediction import BetResult, Prediction, TrackedBet
from app.services.grading import (
    check_outcome,
    compute_tracked_bet_pl,
    determine_actual_outcome,
    get_home_abbr,
)

logger = logging.getLogger(__name__)


async def settle_completed_games(db: AsyncSession) -> Dict[str, Any]:
    """
    Grade all unsettled predictions and tracked bets for completed games.

    Returns:
        Dict with counts: predictions_graded, tracked_bets_settled.
    """
    predictions_graded = await _settle_predictions(db)
    tracked_bets_settled = await _settle_tracked_bets(db)

    if predictions_graded > 0 or tracked_bets_settled > 0:
        await db.flush()
        logger.info(
            "Settlement complete: %d predictions graded, %d tracked bets settled",
            predictions_graded,
            tracked_bets_settled,
        )

    return {
        "predictions_graded": predictions_graded,
        "tracked_bets_settled": tracked_bets_settled,
    }


# ------------------------------------------------------------------ #
#  Prediction grading (Prediction → BetResult)                        #
# ------------------------------------------------------------------ #

async def _settle_predictions(db: AsyncSession) -> int:
    """Grade ungraded predictions for completed games."""
    from app.analytics.models import american_odds_to_implied_prob

    # Only grade prematch predictions — live predictions are ephemeral
    # and regenerated constantly, so they shouldn't count toward stats.
    stmt = (
        select(Prediction)
        .join(Game, Prediction.game_id == Game.id)
        .outerjoin(BetResult, BetResult.prediction_id == Prediction.id)
        .where(
            and_(
                func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                BetResult.id.is_(None),
                Prediction.phase == "prematch",
            )
        )
    )
    result = await db.execute(stmt)
    ungraded = result.scalars().all()

    if not ungraded:
        return 0

    # Batch-load all needed games with team relationships
    game_ids = {p.game_id for p in ungraded}
    games_result = await db.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.id.in_(game_ids))
    )
    games_by_id = {g.id: g for g in games_result.scalars().all()}

    graded_count = 0
    for prediction in ungraded:
        game = games_by_id.get(prediction.game_id)
        if not game or game.home_score is None or game.away_score is None:
            continue

        actual_outcome = determine_actual_outcome(game, prediction.bet_type)
        if actual_outcome is None:
            continue

        home_abbr = get_home_abbr(game)
        was_correct = check_outcome(
            prediction.bet_type,
            prediction.prediction_value,
            game,
            home_abbr,
        )
        if was_correct is None:
            # Push — record as not correct with 0 P/L
            was_correct_flag = False
            profit_loss = 0.0
            actual_outcome = f"push_{actual_outcome}"
        else:
            was_correct_flag = was_correct
            profit_loss = 1.0 if was_correct else -1.0

        # Compute Closing Line Value
        closing_implied = _get_closing_implied_prob(game, prediction, home_abbr)
        clv = None
        if closing_implied is not None and prediction.odds_implied_prob is not None:
            clv = round(closing_implied - prediction.odds_implied_prob, 4)

        bet_result = BetResult(
            prediction_id=prediction.id,
            actual_outcome=actual_outcome,
            was_correct=was_correct_flag,
            profit_loss=profit_loss,
            settled_at=datetime.now(timezone.utc),
            closing_implied_prob=closing_implied,
            clv=clv,
        )
        db.add(bet_result)
        graded_count += 1

        logger.debug(
            "Graded prediction %d: %s (correct=%s, P/L=%.1f, clv=%s)",
            prediction.id, actual_outcome, was_correct_flag, profit_loss, clv,
        )

    return graded_count


# ------------------------------------------------------------------ #
#  Tracked bet settlement (TrackedBet → result/profit_loss)           #
# ------------------------------------------------------------------ #

async def _settle_tracked_bets(db: AsyncSession) -> int:
    """Settle unsettled tracked bets for completed games."""
    stmt = (
        select(TrackedBet)
        .join(Game, Game.id == TrackedBet.game_id)
        .where(
            TrackedBet.result.is_(None),
            func.lower(Game.status).in_(GAME_FINAL_STATUSES),
        )
    )
    result = await db.execute(stmt)
    unsettled = result.scalars().all()

    if not unsettled:
        return 0

    # Batch-load games
    game_ids = {tb.game_id for tb in unsettled}
    games_result = await db.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.id.in_(game_ids))
    )
    games_by_id = {g.id: g for g in games_result.scalars().all()}

    settled_count = 0
    for tb in unsettled:
        game = games_by_id.get(tb.game_id)
        if not game or game.home_score is None or game.away_score is None:
            continue

        home_abbr = get_home_abbr(game)
        was_correct = check_outcome(tb.bet_type, tb.prediction_value, game, home_abbr)

        if was_correct is None:
            tb.result = "push"
            tb.profit_loss = 0.0
        elif was_correct:
            tb.result = "win"
            tb.profit_loss = compute_tracked_bet_pl(True, tb.odds, tb.units)
        else:
            tb.result = "loss"
            tb.profit_loss = compute_tracked_bet_pl(False, tb.odds, tb.units)

        tb.settled_at = datetime.now(timezone.utc)
        if tb.locked_at is None:
            tb.locked_at = tb.settled_at
        settled_count += 1

        logger.debug(
            "Settled tracked bet %d: %s (P/L=%.2f)",
            tb.id, tb.result, tb.profit_loss,
        )

    return settled_count


# ------------------------------------------------------------------ #
#  CLV helper (shared with PredictionManager)                         #
# ------------------------------------------------------------------ #

def _get_closing_implied_prob(
    game: Game,
    prediction: Prediction,
    home_abbr: str,
) -> float | None:
    """Get the closing implied probability for a prediction's side."""
    from app.analytics.models import american_odds_to_implied_prob

    bt = prediction.bet_type
    pv = prediction.prediction_value

    if bt == "ml":
        if pv == "home" or pv == home_abbr:
            odds = game.closing_home_moneyline
        else:
            odds = game.closing_away_moneyline
        if odds is not None:
            return round(american_odds_to_implied_prob(odds), 4)

    elif bt == "total":
        if "over" in pv:
            odds = game.closing_over_price
        elif "under" in pv:
            odds = game.closing_under_price
        else:
            return None
        if odds is not None:
            return round(american_odds_to_implied_prob(odds), 4)

    elif bt == "spread":
        pred_team = pv.split("_", 1)[0] if "_" in pv else ""
        is_home = pred_team == home_abbr
        odds = game.closing_home_spread_price if is_home else game.closing_away_spread_price
        if odds is not None:
            return round(american_odds_to_implied_prob(float(odds)), 4)

    return None
