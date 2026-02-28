"""
Schedule API routes.

Provides endpoints for retrieving NHL game schedules by date,
including the ability to sync schedule data from the NHL API.
"""

import logging
from datetime import date, datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_session
from app.models.game import Game
from app.models.prediction import Prediction
from app.models.team import Team, TeamStats

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/schedule", tags=["schedule"])


class TeamBrief(BaseModel):
    id: int
    external_id: str
    name: str
    abbreviation: str
    logo_url: Optional[str] = None
    wins: Optional[int] = None
    losses: Optional[int] = None
    ot_losses: Optional[int] = None
    points: Optional[int] = None
    record: Optional[str] = None

    model_config = {"from_attributes": True}


class GameTopPick(BaseModel):
    bet_type: Optional[str] = None
    prediction_value: Optional[str] = None
    confidence: Optional[float] = None
    edge: Optional[float] = None


class ScheduleGame(BaseModel):
    id: int
    external_id: str
    game_date: date
    start_time: Optional[datetime] = None
    venue: Optional[str] = None
    status: str
    game_type: Optional[str] = None
    season: str
    home_team: TeamBrief
    away_team: TeamBrief
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    went_to_overtime: Optional[bool] = False
    # Live game info
    period: Optional[int] = None
    period_type: Optional[str] = None  # REG, OT, SO
    clock: Optional[str] = None  # e.g. "12:34"
    clock_running: Optional[bool] = None
    home_shots: Optional[int] = None
    away_shots: Optional[int] = None
    # Top prediction for this game
    top_pick: Optional[GameTopPick] = None

    model_config = {"from_attributes": True}


class ScheduleResponse(BaseModel):
    date: date
    game_count: int
    games: List[ScheduleGame]


class SyncResult(BaseModel):
    success: bool
    message: str
    games_synced: int = 0


async def _build_team_brief(team: Team, session: AsyncSession) -> TeamBrief:
    brief = TeamBrief(
        id=team.id,
        external_id=team.external_id,
        name=team.name,
        abbreviation=team.abbreviation,
        logo_url=team.logo_url,
    )
    stats_result = await session.execute(
        select(TeamStats)
        .where(TeamStats.team_id == team.id)
        .order_by(TeamStats.season.desc())
        .limit(1)
    )
    stats: Optional[TeamStats] = stats_result.scalar_one_or_none()
    if stats:
        brief.wins = stats.wins
        brief.losses = stats.losses
        brief.ot_losses = stats.ot_losses
        brief.points = stats.points
        brief.record = f"{stats.wins}-{stats.losses}-{stats.ot_losses}"
    return brief


async def _games_for_date(
    target_date: date, session: AsyncSession
) -> List[ScheduleGame]:
    result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.date == target_date)
        .order_by(Game.start_time.asc().nulls_last(), Game.id.asc())
    )
    games = result.scalars().all()

    # Pre-fetch best prediction per game (highest edge, market types only)
    MARKET_BET_TYPES = ("ml", "total", "spread")
    game_ids = [g.id for g in games]
    top_picks: dict[int, GameTopPick] = {}
    if game_ids:
        # Get the max edge per game
        max_edge_sub = (
            select(
                Prediction.game_id,
                func.max(Prediction.edge).label("max_edge"),
            )
            .where(
                Prediction.game_id.in_(game_ids),
                Prediction.bet_type.in_(MARKET_BET_TYPES),
                Prediction.edge.isnot(None),
            )
            .group_by(Prediction.game_id)
            .subquery()
        )
        pred_result = await session.execute(
            select(Prediction)
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
        )
        for pred in pred_result.scalars().all():
            if pred.game_id not in top_picks:
                top_picks[pred.game_id] = GameTopPick(
                    bet_type=pred.bet_type,
                    prediction_value=pred.prediction_value,
                    confidence=pred.confidence,
                    edge=pred.edge,
                )

    schedule_games: List[ScheduleGame] = []
    for game in games:
        home_brief = await _build_team_brief(game.home_team, session)
        away_brief = await _build_team_brief(game.away_team, session)

        schedule_games.append(
            ScheduleGame(
                id=game.id,
                external_id=game.external_id,
                game_date=game.date,
                start_time=game.start_time,
                venue=game.venue,
                status=game.status,
                game_type=game.game_type,
                season=game.season,
                home_team=home_brief,
                away_team=away_brief,
                home_score=game.home_score,
                away_score=game.away_score,
                went_to_overtime=game.went_to_overtime or False,
                period=getattr(game, "period", None),
                period_type=getattr(game, "period_type", None),
                clock=getattr(game, "clock", None),
                clock_running=getattr(game, "clock_running", None),
                home_shots=getattr(game, "home_shots", None),
                away_shots=getattr(game, "away_shots", None),
                top_pick=top_picks.get(game.id),
            )
        )
    return schedule_games


async def _try_sync_schedule(
    session: AsyncSession, target_date: Optional[date] = None
) -> int:
    try:
        from app.scrapers.nhl_api import NHLScraper

        scraper = NHLScraper()
        date_str = target_date.isoformat() if target_date else None
        games = await scraper.sync_schedule(session, date_str)
        return len(games) if isinstance(games, list) else 0
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="NHL scraper module is not available.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to sync schedule from NHL API: {exc}",
        )


@router.get("/live", response_model=ScheduleResponse)
async def get_live_games(
    session: AsyncSession = Depends(get_session),
):
    """Return all currently in-progress games across any date."""
    result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.status.in_(["in_progress", "live"]))
        .order_by(Game.start_time.asc().nulls_last(), Game.id.asc())
    )
    games = result.scalars().all()

    # Auto-sync if any live games to get latest scores/clock
    if games:
        try:
            for game in games:
                await _try_sync_schedule(session, target_date=game.date)
            await session.flush()
            # Re-query after sync
            result = await session.execute(
                select(Game)
                .options(selectinload(Game.home_team), selectinload(Game.away_team))
                .where(Game.status.in_(["in_progress", "live"]))
                .order_by(Game.start_time.asc().nulls_last(), Game.id.asc())
            )
            games = result.scalars().all()
        except HTTPException:
            pass

    schedule_games: List[ScheduleGame] = []
    for game in games:
        home_brief = await _build_team_brief(game.home_team, session)
        away_brief = await _build_team_brief(game.away_team, session)
        schedule_games.append(
            ScheduleGame(
                id=game.id,
                external_id=game.external_id,
                game_date=game.date,
                start_time=game.start_time,
                venue=game.venue,
                status=game.status,
                game_type=game.game_type,
                season=game.season,
                home_team=home_brief,
                away_team=away_brief,
                home_score=game.home_score,
                away_score=game.away_score,
                went_to_overtime=game.went_to_overtime or False,
                period=getattr(game, "period", None),
                period_type=getattr(game, "period_type", None),
                clock=getattr(game, "clock", None),
                clock_running=getattr(game, "clock_running", None),
                home_shots=getattr(game, "home_shots", None),
                away_shots=getattr(game, "away_shots", None),
            )
        )

    today = date.today()
    return ScheduleResponse(date=today, game_count=len(schedule_games), games=schedule_games)


@router.get("/today", response_model=ScheduleResponse)
async def get_today_schedule(
    session: AsyncSession = Depends(get_session),
):
    today = date.today()
    games = await _games_for_date(today, session)

    # Check if any games are live (in_progress) or if we have no games yet.
    # In either case, re-sync from the NHL API so we get the latest scores.
    has_live = any(g.status == "in_progress" for g in games)
    if not games or has_live:
        try:
            await _try_sync_schedule(session, target_date=today)
            await session.flush()
            games = await _games_for_date(today, session)
        except HTTPException:
            pass

    # If any non-final games are missing a top pick, sync odds and
    # generate predictions so every game card shows a real best bet.
    # This mirrors what the best-bets endpoint does: sync odds first so
    # predictions are calculated against real sportsbook lines (giving
    # them a real edge), rather than showing inflated fake confidence.
    missing_picks = [g for g in games if g.top_pick is None and g.status not in ("final", "completed", "off")]
    if missing_picks:
        try:
            # Step 1: Sync fresh odds from sportsbook API
            try:
                from app.scrapers.odds_multi import MultiSourceOddsScraper

                odds_scraper = MultiSourceOddsScraper()
                try:
                    matched = await odds_scraper.sync_odds(session)
                    logger.info("Schedule odds sync matched %d games", len(matched) if matched else 0)
                    await session.flush()
                    session.expire_all()
                finally:
                    await odds_scraper.close()
            except Exception as exc:
                logger.warning("Schedule odds sync failed: %s", exc)

            # Step 2: Generate predictions with real odds
            from app.api.predictions import _try_generate_predictions

            await _try_generate_predictions(session, target_date=today)
            await session.flush()
            games = await _games_for_date(today, session)
            logger.info("Schedule triggered prediction generation; %d/%d games now have picks",
                        sum(1 for g in games if g.top_pick is not None), len(games))
        except HTTPException:
            pass

    return ScheduleResponse(date=today, game_count=len(games), games=games)


@router.get("/{date_str}", response_model=ScheduleResponse)
async def get_schedule_by_date(
    date_str: str,
    session: AsyncSession = Depends(get_session),
):
    try:
        target_date = date.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format '{date_str}'. Expected YYYY-MM-DD.",
        )

    games = await _games_for_date(target_date, session)
    return ScheduleResponse(
        date=target_date, game_count=len(games), games=games
    )


@router.post("/sync", response_model=SyncResult)
async def sync_schedule(
    session: AsyncSession = Depends(get_session),
):
    count = await _try_sync_schedule(session)
    return SyncResult(
        success=True,
        message=f"Successfully synced {count} games from the NHL API.",
        games_synced=count,
    )
