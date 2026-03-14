"""
API endpoints for player prop odds.

Provides endpoints to fetch player props grouped by game for today's
NHL schedule, and per-game prop details.
"""

from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.constants import GAME_FINAL_STATUSES
from app.database import get_session, get_write_session_context
from app.models.game import Game
from app.models.player_prop import PlayerPropOdds
from app.models.team import Team

router = APIRouter(prefix="/api/props", tags=["player-props"])


@router.get("/today")
async def get_todays_props(
    market: Optional[str] = Query(None, description="Filter by market key"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get all player props for today's games, grouped by game.

    Returns props grouped by game with team info and matchup context.
    Optionally filter by market (e.g. player_goal_scorer_anytime).
    """
    today = date.today()

    # Get today's non-final games with team info
    games_result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(
            Game.date == today,
            ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
        )
        .order_by(Game.start_time)
    )
    games = games_result.scalars().all()

    if not games:
        return {"games": [], "total_props": 0}

    game_ids = [g.id for g in games]

    # Fetch props for these games
    props_query = select(PlayerPropOdds).where(
        PlayerPropOdds.game_id.in_(game_ids)
    )
    if market:
        props_query = props_query.where(PlayerPropOdds.market == market)
    props_query = props_query.order_by(
        PlayerPropOdds.game_id,
        PlayerPropOdds.market,
        PlayerPropOdds.player_name,
    )

    props_result = await session.execute(props_query)
    all_props = props_result.scalars().all()

    # Group props by game_id
    props_by_game: Dict[int, List[Dict[str, Any]]] = {}
    for prop in all_props:
        if prop.game_id not in props_by_game:
            props_by_game[prop.game_id] = []
        props_by_game[prop.game_id].append({
            "id": prop.id,
            "player_name": prop.player_name,
            "player_id": prop.player_id,
            "market": prop.market,
            "line": prop.line,
            "over_price": prop.over_price,
            "under_price": prop.under_price,
            "bookmaker": prop.bookmaker,
            "odds_updated_at": (
                prop.odds_updated_at.isoformat()
                if prop.odds_updated_at else None
            ),
        })

    # Build response
    game_list = []
    for game in games:
        props = props_by_game.get(game.id, [])
        game_list.append({
            "game_id": game.id,
            "home_team": game.home_team.abbreviation if game.home_team else "",
            "away_team": game.away_team.abbreviation if game.away_team else "",
            "home_team_name": game.home_team.name if game.home_team else "",
            "away_team_name": game.away_team.name if game.away_team else "",
            "start_time": game.start_time.isoformat() if game.start_time else None,
            "status": game.status,
            "props": props,
            "prop_count": len(props),
        })

    return {
        "games": game_list,
        "total_props": len(all_props),
        "markets": sorted(set(p.market for p in all_props)),
    }


@router.get("/game/{game_id}")
async def get_game_props(
    game_id: int,
    market: Optional[str] = Query(None),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get player props for a specific game."""
    game_result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.id == game_id)
    )
    game = game_result.scalar_one_or_none()
    if not game:
        return {"error": "Game not found", "props": []}

    props_query = select(PlayerPropOdds).where(
        PlayerPropOdds.game_id == game_id
    )
    if market:
        props_query = props_query.where(PlayerPropOdds.market == market)
    props_query = props_query.order_by(
        PlayerPropOdds.market, PlayerPropOdds.player_name,
    )

    props_result = await session.execute(props_query)
    all_props = props_result.scalars().all()

    # Group by market
    by_market: Dict[str, List[Dict[str, Any]]] = {}
    for prop in all_props:
        if prop.market not in by_market:
            by_market[prop.market] = []
        by_market[prop.market].append({
            "id": prop.id,
            "player_name": prop.player_name,
            "player_id": prop.player_id,
            "line": prop.line,
            "over_price": prop.over_price,
            "under_price": prop.under_price,
            "bookmaker": prop.bookmaker,
            "odds_updated_at": (
                prop.odds_updated_at.isoformat()
                if prop.odds_updated_at else None
            ),
        })

    return {
        "game_id": game_id,
        "home_team": game.home_team.abbreviation if game.home_team else "",
        "away_team": game.away_team.abbreviation if game.away_team else "",
        "home_team_name": game.home_team.name if game.home_team else "",
        "away_team_name": game.away_team.name if game.away_team else "",
        "start_time": game.start_time.isoformat() if game.start_time else None,
        "status": game.status,
        "markets": by_market,
        "total_props": len(all_props),
    }


@router.post("/sync")
async def sync_props_now() -> Dict[str, Any]:
    """Manually trigger a player props sync from The Odds API.

    Bypasses the 30-minute cache to force a fresh fetch. Useful when
    props aren't appearing or need an immediate refresh.
    """
    import logging

    logger = logging.getLogger(__name__)

    # Clear the props cache so we get fresh data
    try:
        from app.scrapers.player_props import _props_cache, _props_cache_ts
        import app.scrapers.player_props as _pp_mod

        _pp_mod._props_cache = {}
        _pp_mod._props_cache_ts = 0.0
    except ImportError:
        pass

    try:
        async with get_write_session_context() as session:
            from app.services.odds import sync_player_props

            count = await sync_player_props(session)
            logger.info("Manual props sync: %d lines synced", count)
            return {"status": "ok", "props_synced": count}
    except Exception as exc:
        logger.error("Manual props sync failed: %s", exc, exc_info=True)
        return {"status": "error", "error": str(exc), "props_synced": 0}
