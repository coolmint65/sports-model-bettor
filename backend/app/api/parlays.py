"""
Parlays API routes.

Generates high-conviction 2-leg and 3-leg parlays from the model's
best game-line and player-prop picks for today's games.

Parlays are built from the HIGHEST conviction picks only — both legs
must individually meet best-bet criteria.
"""

import logging
from datetime import date
from itertools import combinations
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.constants import GAME_FINAL_STATUSES, MARKET_BET_TYPES, composite_pick_score
from app.database import get_session
from app.models.game import Game
from app.models.prediction import Prediction
from app.services.odds import american_to_implied, implied_to_american

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/parlays", tags=["parlays"])


def _parlay_odds(legs: List[Dict]) -> Optional[float]:
    """Compute combined American odds for a parlay from individual leg odds."""
    combined_decimal = 1.0
    for leg in legs:
        odds = leg.get("odds")
        if odds is None or odds == 0:
            return None
        if odds > 0:
            combined_decimal *= 1 + (odds / 100)
        else:
            combined_decimal *= 1 + (100 / abs(odds))
    if combined_decimal <= 1:
        return None
    # Convert back to American
    if combined_decimal >= 2.0:
        return round((combined_decimal - 1) * 100)
    else:
        return round(-100 / (combined_decimal - 1))


def _leg_from_prediction(pred: Prediction, game: Game) -> Optional[Dict]:
    """Build a parlay leg dict from a Prediction + Game."""
    home_abbr = game.home_team.abbreviation if game.home_team else ""
    away_abbr = game.away_team.abbreviation if game.away_team else ""

    # Determine display odds
    odds = None
    label = ""
    bt = pred.bet_type
    val = (pred.prediction_value or "").lower()

    if bt == "ml":
        if val == "home" or val == home_abbr.lower():
            odds = game.home_moneyline
            label = f"{home_abbr} ML"
        else:
            odds = game.away_moneyline
            label = f"{away_abbr} ML"
    elif bt == "total":
        is_over = "over" in val
        if is_over:
            odds = game.over_price
            line = game.over_under_line
            label = f"Over {line}" if line else "Over"
        else:
            odds = game.under_price
            line = game.over_under_line
            label = f"Under {line}" if line else "Under"
    elif bt == "spread":
        if val.startswith("home") or (home_abbr and val.startswith(home_abbr.lower())):
            odds = game.home_spread_price
            line = game.home_spread_line
            label = f"{home_abbr} {line:+.1f}" if line is not None else f"{home_abbr} Spread"
        else:
            odds = game.away_spread_price
            line = game.away_spread_line
            label = f"{away_abbr} {line:+.1f}" if line is not None else f"{away_abbr} Spread"

    if odds is None:
        return None

    score = composite_pick_score(pred.confidence, pred.edge, pred.odds_implied_prob)

    return {
        "type": "game",
        "game_id": game.id,
        "matchup": f"{away_abbr} @ {home_abbr}",
        "bet_type": bt,
        "label": label,
        "odds": round(odds),
        "confidence": pred.confidence,
        "edge": pred.edge,
        "score": score,
    }


def _leg_from_prop(pick: Dict) -> Optional[Dict]:
    """Build a parlay leg dict from a prop pick dict."""
    odds = pick.get("odds")
    if odds is None or odds == 0:
        return None

    market = pick.get("market", "")
    player = pick.get("player_name", "Unknown")
    side = pick.get("pick_side", "over")
    line = pick.get("line", 0)

    market_labels = {
        "player_goal_scorer_anytime": "ATG",
        "player_shots_on_goal": "SOG",
        "player_points": "PTS",
        "player_assists": "AST",
        "player_total_saves": "SVS",
        "player_rebounds": "REB",
        "player_threes": "3PM",
    }
    short = market_labels.get(market, market.replace("player_", "").upper())

    if market == "player_goal_scorer_anytime":
        label = f"{player} ATG"
    else:
        label = f"{player} {side.title()} {line} {short}"

    conf = pick.get("confidence", 0)
    edge = pick.get("edge", 0)
    impl = american_to_implied(odds) or 0.5
    score = composite_pick_score(conf, edge, impl)

    return {
        "type": "prop",
        "game_id": pick.get("game_id"),
        "matchup": pick.get("matchup", ""),
        "bet_type": market,
        "label": label,
        "odds": round(odds),
        "confidence": conf,
        "edge": edge,
        "score": score,
    }


def _build_best_parlays(
    all_legs: List[Dict], num_legs: int
) -> Optional[Dict]:
    """Pick the single best N-leg parlay from available legs.

    Selection criteria:
    1. All legs must be from different games
    2. Maximize minimum confidence across legs (weakest link)
    3. Break ties by total composite score
    """
    if len(all_legs) < num_legs:
        return None

    best_parlay = None
    best_min_conf = -1
    best_total_score = -1

    for combo in combinations(range(len(all_legs)), num_legs):
        legs = [all_legs[i] for i in combo]
        # All legs must be from different games
        game_ids = set(l["game_id"] for l in legs)
        if len(game_ids) < num_legs:
            continue

        min_conf = min(l["confidence"] for l in legs)
        total_score = sum(l["score"] for l in legs)

        if (min_conf > best_min_conf) or (
            min_conf == best_min_conf and total_score > best_total_score
        ):
            best_min_conf = min_conf
            best_total_score = total_score
            best_parlay = legs

    if best_parlay is None:
        return None

    parlay_odds = _parlay_odds(best_parlay)
    return {
        "legs": best_parlay,
        "combined_odds": parlay_odds,
        "min_confidence": best_min_conf,
        "avg_confidence": sum(l["confidence"] for l in best_parlay) / len(best_parlay),
    }


@router.get("/today")
async def get_todays_parlays(
    sport: str = Query(default="nhl", description="Sport filter"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Generate the highest conviction 2-leg and 3-leg parlays for today."""
    today = date.today()

    # Get today's non-final games with predictions
    games_result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(
            Game.date == today,
            Game.sport == sport,
            ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
        )
    )
    games = games_result.scalars().all()
    game_map = {g.id: g for g in games}

    if not games:
        return {"two_leg": None, "three_leg": None}

    # Get top predictions (prematch only, with edge)
    preds_result = await session.execute(
        select(Prediction).where(
            Prediction.game_id.in_([g.id for g in games]),
            Prediction.bet_type.in_(MARKET_BET_TYPES),
            Prediction.phase == "prematch",
            Prediction.edge.isnot(None),
            Prediction.odds_implied_prob.isnot(None),
            Prediction.edge >= settings.min_edge,
            Prediction.confidence >= settings.min_confidence,
        )
    )
    preds = preds_result.scalars().all()

    # Build game-line legs — take only the best pick per game
    game_legs: Dict[int, Dict] = {}
    for pred in preds:
        game = game_map.get(pred.game_id)
        if not game:
            continue
        leg = _leg_from_prediction(pred, game)
        if leg is None:
            continue
        existing = game_legs.get(pred.game_id)
        if existing is None or leg["score"] > existing["score"]:
            game_legs[pred.game_id] = leg

    # Build prop legs from frozen prop pick snapshots
    prop_legs: List[Dict] = []
    try:
        from app.models.prop_pick_snapshot import PropPickSnapshot

        snaps_result = await session.execute(
            select(PropPickSnapshot).where(
                PropPickSnapshot.game_id.in_([g.id for g in games]),
                PropPickSnapshot.edge >= 0.03,
                PropPickSnapshot.confidence >= 0.55,
            )
        )
        snaps = snaps_result.scalars().all()

        for snap in snaps:
            game = game_map.get(snap.game_id)
            if not game:
                continue
            home_abbr = game.home_team.abbreviation if game.home_team else ""
            away_abbr = game.away_team.abbreviation if game.away_team else ""
            pick_dict = {
                "game_id": snap.game_id,
                "matchup": f"{away_abbr} @ {home_abbr}",
                "player_name": snap.player_name,
                "market": snap.market,
                "pick_side": snap.pick_side,
                "line": snap.line,
                "odds": snap.odds,
                "confidence": snap.confidence,
                "edge": snap.edge,
            }
            leg = _leg_from_prop(pick_dict)
            if leg:
                prop_legs.append(leg)
    except Exception:
        logger.debug("Prop picks not available for parlay generation")

    # Combine all legs, sorted by score descending
    all_legs = list(game_legs.values()) + prop_legs
    all_legs.sort(key=lambda l: l["score"], reverse=True)

    # Take top legs only (limit search space)
    top_legs = all_legs[:12]

    two_leg = _build_best_parlays(top_legs, 2)
    three_leg = _build_best_parlays(top_legs, 3)

    return {
        "two_leg": two_leg,
        "three_leg": three_leg,
    }
