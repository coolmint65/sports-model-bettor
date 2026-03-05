"""
Odds service layer.

Single place for all odds-related operations: syncing from sportsbooks,
computing implied probabilities, and determining fresh odds for predictions.
Replaces the 7+ inline sync patterns scattered across route handlers.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.game import Game
from app.models.prediction import Prediction

logger = logging.getLogger(__name__)

# Throttle to avoid sportsbook API rate limits
_SYNC_MIN_INTERVAL = timedelta(minutes=2)
_sync_lock = asyncio.Lock()
_last_sync_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Odds conversion utilities (single source of truth)
# ---------------------------------------------------------------------------

def american_to_implied(odds: Optional[float]) -> Optional[float]:
    """Convert American odds to implied probability (0-1).

    Returns None for invalid/missing odds.
    """
    if odds is None or odds == 0:
        return None
    if odds > 0:
        return round(100.0 / (odds + 100.0), 4)
    return round(abs(odds) / (abs(odds) + 100.0), 4)


def implied_to_american(prob: Optional[float]) -> Optional[float]:
    """Convert implied probability (0-1) to American odds.

    Returns None for invalid/missing probabilities.
    """
    if prob is None or prob <= 0 or prob >= 1:
        return None
    if prob > 0.5:
        return round(-(prob / (1 - prob)) * 100)
    return round(((1 - prob) / prob) * 100)


# ---------------------------------------------------------------------------
# Fresh implied probability for a prediction (single implementation)
# ---------------------------------------------------------------------------

def fresh_implied_prob(pred: Prediction, game: Optional[Game]) -> Optional[float]:
    """Compute current implied probability from the Game's live odds.

    Uses the Game's current odds fields to compute a fresh implied probability
    for the given prediction. Returns None when the Game is unavailable or
    the relevant odds field is NULL.

    This replaces the duplicated _current_implied_for_pred (schedule.py) and
    _fresh_implied_for_pred (games.py) functions.
    """
    if game is None:
        return None

    live_odds: Optional[float] = None

    if pred.bet_type == "ml":
        home_abbr = game.home_team.abbreviation if game.home_team else ""
        if pred.prediction_value == home_abbr:
            live_odds = game.home_moneyline
        else:
            live_odds = game.away_moneyline

    elif pred.bet_type == "total":
        is_over = pred.prediction_value and "over" in pred.prediction_value
        # Look up the specific line in all_total_lines first
        if game.all_total_lines and pred.prediction_value:
            try:
                parts = pred.prediction_value.split("_", 1)
                if len(parts) == 2:
                    pred_line = float(parts[1])
                    all_tl = game.all_total_lines
                    if isinstance(all_tl, str):
                        all_tl = json.loads(all_tl)
                    for tl in (all_tl or []):
                        if abs(tl.get("line", 0) - pred_line) < 0.01:
                            price_key = "over_price" if is_over else "under_price"
                            live_odds = tl.get(price_key)
                            break
            except (ValueError, TypeError, KeyError):
                pass
        # Fall back to the primary O/U prices
        if live_odds is None:
            if is_over:
                live_odds = game.over_price
            else:
                live_odds = game.under_price

    elif pred.bet_type == "spread":
        home_abbr = game.home_team.abbreviation if game.home_team else ""
        pred_is_home = (
            pred.prediction_value
            and pred.prediction_value.startswith(home_abbr)
        )
        if pred_is_home:
            live_odds = game.home_spread_price
        else:
            live_odds = game.away_spread_price

    # --- Prop bet types ---
    elif pred.bet_type == "first_goal":
        if pred.prediction_value == "home":
            live_odds = game.first_goal_home_price
        else:
            live_odds = game.first_goal_away_price

    elif pred.bet_type == "both_score":
        if pred.prediction_value == "yes":
            live_odds = game.btts_yes_price
        else:
            live_odds = game.btts_no_price

    elif pred.bet_type == "overtime":
        if pred.prediction_value == "yes":
            live_odds = game.overtime_yes_price
        else:
            live_odds = game.overtime_no_price

    elif pred.bet_type == "odd_even":
        if pred.prediction_value == "odd":
            live_odds = game.total_odd_price
        else:
            live_odds = game.total_even_price

    elif pred.bet_type == "period_winner":
        # prediction_value like "p1_home", "p1_away", "p1_draw"
        pv = pred.prediction_value or ""
        if pv.startswith("p1_"):
            side = pv[3:]  # "home", "away", or "draw"
            if side == "home":
                live_odds = game.period1_home_ml
            elif side == "away":
                live_odds = game.period1_away_ml
            elif side == "draw":
                live_odds = game.period1_draw_price

    elif pred.bet_type == "period_total":
        # prediction_value like "p1_over_1.5", "p1_under_1.5"
        pv = pred.prediction_value or ""
        if pv.startswith("p1_"):
            if "over" in pv:
                live_odds = game.period1_over_price
            elif "under" in pv:
                live_odds = game.period1_under_price

    elif pred.bet_type == "period1_btts":
        if pred.prediction_value == "yes":
            live_odds = getattr(game, "period1_btts_yes_price", None)
        else:
            live_odds = getattr(game, "period1_btts_no_price", None)

    elif pred.bet_type == "period1_spread":
        pv = pred.prediction_value or ""
        if "home" in pv:
            live_odds = getattr(game, "period1_home_spread_price", None)
        else:
            live_odds = getattr(game, "period1_away_spread_price", None)

    elif pred.bet_type == "regulation_winner":
        if pred.prediction_value == "home":
            live_odds = getattr(game, "regulation_home_price", None)
        elif pred.prediction_value == "away":
            live_odds = getattr(game, "regulation_away_price", None)
        elif pred.prediction_value == "draw":
            live_odds = getattr(game, "regulation_draw_price", None)

    elif pred.bet_type == "team_total":
        pv = pred.prediction_value or ""
        if pv.startswith("home_over"):
            live_odds = getattr(game, "home_team_over_price", None)
        elif pv.startswith("home_under"):
            live_odds = getattr(game, "home_team_under_price", None)
        elif pv.startswith("away_over"):
            live_odds = getattr(game, "away_team_over_price", None)
        elif pv.startswith("away_under"):
            live_odds = getattr(game, "away_team_under_price", None)

    elif pred.bet_type == "highest_scoring_period":
        hp_map = {
            "p1": "highest_period_p1_price",
            "p2": "highest_period_p2_price",
            "p3": "highest_period_p3_price",
            "tie": "highest_period_tie_price",
        }
        field = hp_map.get(pred.prediction_value or "")
        if field:
            live_odds = getattr(game, field, None)

    return american_to_implied(live_odds)


# ---------------------------------------------------------------------------
# Centralized odds sync
# ---------------------------------------------------------------------------

async def sync_odds(session: AsyncSession, force: bool = False) -> List[Dict[str, Any]]:
    """Sync odds from all sportsbook sources.

    Throttled to avoid API rate limits. Use force=True to bypass throttle.
    Returns list of matched game dicts.
    """
    global _last_sync_at

    async with _sync_lock:
        now = datetime.now(timezone.utc)
        if not force and _last_sync_at is not None:
            elapsed = now - _last_sync_at
            if elapsed < _SYNC_MIN_INTERVAL:
                logger.debug(
                    "Odds sync throttled (last sync %s ago)", elapsed
                )
                return []

        try:
            from app.scrapers.odds_multi import MultiSourceOddsScraper

            async with MultiSourceOddsScraper() as scraper:
                matched = await scraper.sync_odds(session)
                await session.flush()
                session.expire_all()
                _last_sync_at = now
                logger.info("Odds sync: matched %d games", len(matched) if matched else 0)
                return matched or []
        except Exception as exc:
            _last_sync_at = now  # Still throttle on failure
            logger.error("Odds sync failed: %s", exc, exc_info=True)
            return []


async def sync_odds_and_regenerate(
    session: AsyncSession,
    force: bool = False,
) -> tuple[List[Dict[str, Any]], int]:
    """Sync odds then regenerate predictions for today's non-final games.

    Returns (matched_games, prediction_count).
    """
    from datetime import date

    from sqlalchemy import delete as sa_delete, func, select

    from app.constants import GAME_FINAL_STATUSES
    from app.models.prediction import Prediction

    matched = await sync_odds(session, force=force)

    today = date.today()
    pred_count = 0

    if matched:
        try:
            from app.analytics.predictions import PredictionManager

            async with session.begin_nested():
                non_final_ids = select(Game.id).where(
                    Game.date == today,
                    ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                )
                await session.execute(
                    sa_delete(Prediction).where(
                        Prediction.game_id.in_(non_final_ids)
                    )
                )
                await session.flush()
                pm = PredictionManager()
                bets = await pm.get_best_bets(session)
                pred_count = len(bets) if bets else 0
        except Exception as exc:
            logger.error("Prediction regeneration failed: %s", exc, exc_info=True)

    return matched, pred_count
