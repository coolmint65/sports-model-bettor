"""
Schedule API routes.

Provides endpoints for retrieving NHL game schedules by date,
including the ability to sync schedule data from the NHL API.
"""

from datetime import date, datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_session
from app.models.game import Game
from app.models.team import Team, TeamStats

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


@router.get("/today", response_model=ScheduleResponse)
async def get_today_schedule(
    session: AsyncSession = Depends(get_session),
):
    today = date.today()
    games = await _games_for_date(today, session)

    if not games:
        try:
            await _try_sync_schedule(session, target_date=today)
            await session.flush()
            games = await _games_for_date(today, session)
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
