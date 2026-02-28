"""
Data management API routes.

Provides endpoints for triggering various data synchronisation operations
(teams, schedule, rosters, game results) and checking the overall status
of synced data in the database.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session, get_session_context
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


class SyncStatusResponse(BaseModel):
    """Current state of the background sync task."""

    running: bool
    step: str
    error: Optional[str] = None


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
# Background sync state
# ---------------------------------------------------------------------------

_sync_state = {
    "running": False,
    "step": "idle",
    "error": None,
}


async def _run_full_sync():
    """Execute the full sync pipeline in the background."""
    global _sync_state
    _sync_state = {"running": True, "step": "Starting sync...", "error": None}

    try:
        from app.scrapers.nhl_api import NHLScraper

        scraper = NHLScraper()
        try:
            async with get_session_context() as session:
                # 1. Core sync: teams, rosters, schedule, game results
                _sync_state["step"] = "Syncing teams, rosters, schedule..."
                await scraper.sync_all(session)
                await session.flush()

                # 2. Historical H2H
                _sync_state["step"] = "Syncing historical H2H data..."
                try:
                    current = scraper.default_season
                    current_start = int(current[:4])
                except (ValueError, IndexError):
                    current_start = 2025

                h2h_games = 0
                try:
                    for i in range(0, 3):
                        start_year = current_start - i
                        season_str = f"{start_year}{start_year + 1}"
                        _sync_state["step"] = f"Syncing H2H season {season_str}..."
                        h2h_games += await scraper.sync_historical_season(
                            session, season_str
                        )
                except Exception as exc:
                    logger.warning("Historical H2H sync failed (non-critical): %s", exc)

                await session.flush()

                # 3. Odds
                _sync_state["step"] = "Syncing betting odds..."
                try:
                    from app.scrapers.odds_api import OddsScraper

                    odds_scraper = OddsScraper()
                    try:
                        await odds_scraper.sync_odds(session)
                    finally:
                        await odds_scraper.close()
                except Exception as exc:
                    logger.warning("Odds sync failed (non-critical): %s", exc)

                await session.flush()

                # 4. Predictions
                _sync_state["step"] = "Generating predictions..."
                try:
                    from app.analytics.predictions import PredictionManager

                    manager = PredictionManager()
                    await manager.get_best_bets(session)
                except Exception as exc:
                    logger.warning("Prediction generation failed (non-critical): %s", exc)

        finally:
            await scraper.close()

        _sync_state = {"running": False, "step": "Complete", "error": None}
        logger.info("Full background sync completed successfully.")

    except Exception as exc:
        logger.error("Background sync failed: %s", exc)
        _sync_state = {"running": False, "step": "Failed", "error": str(exc)}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_scraper():
    """
    Import and instantiate the NHLScraper.

    Raises HTTPException 503 if the module is not available.
    """
    try:
        from app.scrapers.nhl_api import NHLScraper

        return NHLScraper()
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
async def sync_all():
    """
    Kick off a full data sync in the background.

    Returns immediately. Poll GET /sync/status to track progress.
    """
    if _sync_state["running"]:
        return SyncResult(
            success=True,
            message="Sync already in progress.",
            details=_sync_state["step"],
        )

    asyncio.get_event_loop().create_task(_run_full_sync())

    return SyncResult(
        success=True,
        message="Sync started.",
        details="Poll /data/sync/status to track progress.",
    )


@router.get(
    "/sync/status",
    response_model=SyncStatusResponse,
    summary="Check sync progress",
)
async def sync_status():
    """Return the current state of the background sync task."""
    return SyncStatusResponse(**_sync_state)


@router.post(
    "/sync/teams",
    response_model=SyncResult,
    summary="Sync teams only",
)
async def sync_teams(
    session: AsyncSession = Depends(get_session),
):
    """Fetch and upsert the latest team data from the NHL API."""
    scraper = _get_scraper()
    try:
        await scraper.sync_teams(session)
        return SyncResult(
            success=True,
            message="Teams synced successfully.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Team sync failed: {exc}",
        )
    finally:
        await scraper.close()


@router.post(
    "/sync/schedule",
    response_model=SyncResult,
    summary="Sync schedule only",
)
async def sync_schedule(
    session: AsyncSession = Depends(get_session),
):
    """Fetch and upsert the latest schedule data from the NHL API."""
    scraper = _get_scraper()
    try:
        await scraper.sync_schedule(session)
        return SyncResult(
            success=True,
            message="Schedule synced successfully.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Schedule sync failed: {exc}",
        )
    finally:
        await scraper.close()


@router.post(
    "/sync/results",
    response_model=SyncResult,
    summary="Sync game results",
)
async def sync_results(
    session: AsyncSession = Depends(get_session),
):
    """Fetch and update game results (scores, stats) from the NHL API."""
    scraper = _get_scraper()
    try:
        await scraper.sync_recent_results(session)
        return SyncResult(
            success=True,
            message="Game results synced successfully.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Results sync failed: {exc}",
        )
    finally:
        await scraper.close()


@router.post(
    "/sync/odds",
    response_model=SyncResult,
    summary="Sync betting odds",
)
async def sync_odds(
    session: AsyncSession = Depends(get_session),
):
    """Fetch and update current betting odds from The Odds API."""
    try:
        from app.scrapers.odds_api import OddsScraper

        odds_scraper = OddsScraper()
        try:
            matched = await odds_scraper.sync_odds(session)
            return SyncResult(
                success=True,
                message=f"Odds synced for {len(matched)} games.",
                details=f"Updated moneyline, spread, and totals odds.",
            )
        finally:
            await odds_scraper.close()
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Odds sync failed: {exc}",
        )


@router.post(
    "/sync/history",
    response_model=SyncResult,
    summary="Sync historical season data for H2H",
)
async def sync_historical(
    seasons: int = 2,
    session: AsyncSession = Depends(get_session),
):
    """
    Sync previous seasons' game results to build head-to-head history.

    Fetches completed regular season games from past seasons so the
    prediction model has richer H2H data. Default syncs the last 2 seasons.

    Args:
        seasons: Number of past seasons to sync (1-3). Default 2.
    """
    scraper = _get_scraper()
    seasons = max(1, min(3, seasons))

    # Derive past season strings from current default season
    # e.g., if default is "20252026", previous is "20242025"
    try:
        current = scraper.default_season
        current_start = int(current[:4])
    except (ValueError, IndexError):
        current_start = 2025

    total_games = 0
    synced_seasons = []

    try:
        # Start from 0 to include current season's completed games
        for i in range(0, seasons + 1):
            start_year = current_start - i
            season_str = f"{start_year}{start_year + 1}"
            count = await scraper.sync_historical_season(session, season_str)
            total_games += count
            synced_seasons.append(season_str)
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Historical sync failed: {exc}",
        )
    finally:
        await scraper.close()

    return SyncResult(
        success=True,
        message=f"Synced {total_games} historical games from {len(synced_seasons)} season(s).",
        details=f"Seasons synced: {', '.join(synced_seasons)}",
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
        select(func.count(Game.id)).where(Game.date == today)
    )
    games_today = today_total_result.scalar() or 0

    today_final_result = await session.execute(
        select(func.count(Game.id)).where(
            Game.date == today, Game.status == "final"
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
