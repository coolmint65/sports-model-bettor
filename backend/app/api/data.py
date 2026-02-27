"""
Data management API routes.

Provides endpoints for triggering various data synchronisation operations
(teams, schedule, rosters, game results) and checking the overall status
of synced data in the database.
"""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.models.game import Game, GameGoalieStats, GamePlayerStats, HeadToHead
from app.models.player import GoalieStats, Player, PlayerStats
from app.models.prediction import Prediction
from app.models.team import Team, TeamStats

router = APIRouter(prefix="/api/data", tags=["data"])


# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------

class SyncResult(BaseModel):
    """Outcome of a sync operation."""

    success: bool
    message: str
    details: Optional[str] = None


class RecordCounts(BaseModel):
    """Number of rows in each key table."""

    teams: int = 0
    team_stats: int = 0
    players: int = 0
    player_stats: int = 0
    goalie_stats: int = 0
    games: int = 0
    game_player_stats: int = 0
    game_goalie_stats: int = 0
    head_to_head: int = 0
    predictions: int = 0


class DataStatusResponse(BaseModel):
    """Overall data-layer health and freshness."""

    last_team_sync: Optional[str] = None
    last_game_sync: Optional[str] = None
    last_player_sync: Optional[str] = None
    last_prediction_generated: Optional[str] = None
    record_counts: RecordCounts
    games_today: int = 0
    games_final_today: int = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_scraper(session: AsyncSession):
    """
    Import and instantiate the NHLScraper.

    Raises HTTPException 503 if the module is not available.
    """
    try:
        from app.scrapers.nhl_api import NHLScraper

        return NHLScraper(session)
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="NHL scraper module is not available. Ensure the scraper package is installed.",
        )


async def _latest_updated_at(session: AsyncSession, model) -> Optional[str]:
    """Return the most recent `updated_at` timestamp from a model table."""
    result = await session.execute(
        select(func.max(model.updated_at))
    )
    val = result.scalar()
    return str(val) if val else None


async def _count(session: AsyncSession, model) -> int:
    """Return the row count for a model table."""
    result = await session.execute(select(func.count(model.id)))
    return result.scalar() or 0


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

@router.post(
    "/sync/all",
    response_model=SyncResult,
    summary="Full data sync",
)
async def sync_all(
    session: AsyncSession = Depends(get_session),
):
    """
    Perform a full data synchronisation: teams, rosters, schedule, and
    game results. Delegates to NHLScraper.sync_all().
    """
    scraper = await _get_scraper(session)
    try:
        await scraper.sync_all()
        return SyncResult(
            success=True,
            message="Full data sync completed successfully.",
            details="Synced teams, rosters, schedule, and game results.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Full sync failed: {exc}",
        )


@router.post(
    "/sync/teams",
    response_model=SyncResult,
    summary="Sync teams only",
)
async def sync_teams(
    session: AsyncSession = Depends(get_session),
):
    """Fetch and upsert the latest team data from the NHL API."""
    scraper = await _get_scraper(session)
    try:
        await scraper.sync_teams()
        return SyncResult(
            success=True,
            message="Teams synced successfully.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Team sync failed: {exc}",
        )


@router.post(
    "/sync/schedule",
    response_model=SyncResult,
    summary="Sync schedule only",
)
async def sync_schedule(
    session: AsyncSession = Depends(get_session),
):
    """Fetch and upsert the latest schedule data from the NHL API."""
    scraper = await _get_scraper(session)
    try:
        await scraper.sync_schedule()
        return SyncResult(
            success=True,
            message="Schedule synced successfully.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Schedule sync failed: {exc}",
        )


@router.post(
    "/sync/results",
    response_model=SyncResult,
    summary="Sync game results",
)
async def sync_results(
    session: AsyncSession = Depends(get_session),
):
    """Fetch and update game results (scores, stats) from the NHL API."""
    scraper = await _get_scraper(session)
    try:
        await scraper.sync_game_results()
        return SyncResult(
            success=True,
            message="Game results synced successfully.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Results sync failed: {exc}",
        )


@router.get(
    "/status",
    response_model=DataStatusResponse,
    summary="Get data status",
)
async def get_data_status(
    session: AsyncSession = Depends(get_session),
):
    """
    Return a summary of the current data state: last sync timestamps,
    record counts per table, and today's game tallies.
    """
    from datetime import date as date_type

    today = date_type.today()

    # Record counts
    counts = RecordCounts(
        teams=await _count(session, Team),
        team_stats=await _count(session, TeamStats),
        players=await _count(session, Player),
        player_stats=await _count(session, PlayerStats),
        goalie_stats=await _count(session, GoalieStats),
        games=await _count(session, Game),
        game_player_stats=await _count(session, GamePlayerStats),
        game_goalie_stats=await _count(session, GameGoalieStats),
        head_to_head=await _count(session, HeadToHead),
        predictions=await _count(session, Prediction),
    )

    # Last sync timestamps (approximated by latest updated_at)
    last_team = await _latest_updated_at(session, Team)
    last_game = await _latest_updated_at(session, Game)
    last_player = await _latest_updated_at(session, Player)

    # Latest prediction timestamp
    pred_result = await session.execute(
        select(func.max(Prediction.created_at))
    )
    last_pred = pred_result.scalar()
    last_pred_str = str(last_pred) if last_pred else None

    # Today's games
    today_total_result = await session.execute(
        select(func.count(Game.id)).where(Game.game_date == today)
    )
    games_today = today_total_result.scalar() or 0

    today_final_result = await session.execute(
        select(func.count(Game.id)).where(
            Game.game_date == today, Game.status == "final"
        )
    )
    games_final_today = today_final_result.scalar() or 0

    return DataStatusResponse(
        last_team_sync=last_team,
        last_game_sync=last_game,
        last_player_sync=last_player,
        last_prediction_generated=last_pred_str,
        record_counts=counts,
        games_today=games_today,
        games_final_today=games_final_today,
    )
