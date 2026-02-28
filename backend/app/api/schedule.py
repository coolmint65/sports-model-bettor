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
            async with session.begin_nested():
                for game in games:
                    await _try_sync_schedule(session, target_date=game.date)
                await session.flush()
        except Exception:
            pass
        # Always re-query to get fresh ORM objects (savepoint rollback
        # expires identity-mapped objects, which breaks async lazy loading).
        result = await session.execute(
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(Game.status.in_(["in_progress", "live"]))
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
            async with session.begin_nested():
                await _try_sync_schedule(session, target_date=today)
                await session.flush()
            games = await _games_for_date(today, session)
        except Exception:
            pass

    # If any non-final games are missing a top pick OR missing odds
    # data entirely, sync odds from sportsbooks and regenerate predictions.
    missing_picks = [g for g in games if g.top_pick is None and g.status not in ("final", "completed", "off")]

    # Also directly check if any DB games are missing odds — this catches
    # cases where predictions exist but lack edges because odds weren't
    # available when they were generated.
    needs_odds_result = await session.execute(
        select(Game.id).where(
            Game.date == today,
            Game.status.notin_(["final", "completed", "off"]),
            Game.home_moneyline.is_(None),
        )
    )
    games_missing_odds = needs_odds_result.scalars().all()

    if missing_picks or games_missing_odds:
        logger.info(
            "Schedule: %d games missing picks, %d games missing odds. "
            "Missing picks: %s | Missing odds (game IDs): %s",
            len(missing_picks), len(games_missing_odds),
            ", ".join(f"{g.away_team.abbreviation}@{g.home_team.abbreviation}(status={g.status})" for g in missing_picks) or "none",
            ", ".join(str(gid) for gid in games_missing_odds) or "none",
        )
        # Step 1: Sync fresh odds from sportsbook API (isolated in savepoint)
        try:
            async with session.begin_nested():
                from app.scrapers.odds_multi import MultiSourceOddsScraper

                odds_scraper = MultiSourceOddsScraper()
                try:
                    matched = await odds_scraper.sync_odds(session)
                    matched_pairs = [f"{m.get('away_abbrev','')}@{m.get('home_abbrev','')}" for m in (matched or [])]
                    logger.info(
                        "Schedule odds sync matched %d games: %s",
                        len(matched) if matched else 0,
                        ", ".join(matched_pairs) if matched_pairs else "none",
                    )
                    await session.flush()
                    session.expire_all()

                    # Log which games have/lack odds after sync
                    odds_check = await session.execute(
                        select(Game)
                        .options(selectinload(Game.home_team), selectinload(Game.away_team))
                        .where(Game.date == today)
                    )
                    for g in odds_check.scalars().all():
                        has_ml = g.home_moneyline is not None
                        has_ou = g.over_under_line is not None
                        ha = g.home_team.abbreviation if g.home_team else "?"
                        aa = g.away_team.abbreviation if g.away_team else "?"
                        if not has_ml or not has_ou:
                            logger.warning(
                                "MISSING ODDS for %s@%s: ml=%s, ou=%s, spread=%s",
                                aa, ha,
                                g.home_moneyline, g.over_under_line, g.home_spread_line,
                            )
                finally:
                    await odds_scraper.close()
        except Exception as exc:
            logger.warning("Schedule odds sync failed: %s", exc)

        # Step 2: Generate predictions with real odds (isolated in savepoint)
        try:
            async with session.begin_nested():
                from app.api.predictions import _try_generate_predictions

                await _try_generate_predictions(session, target_date=today)
                await session.flush()
        except Exception as exc:
            logger.warning(
                "Schedule: prediction generation failed: %s",
                getattr(exc, 'detail', str(exc)),
            )

        games = await _games_for_date(today, session)
        with_picks = [g for g in games if g.top_pick is not None]
        without_picks = [g for g in games if g.top_pick is None and g.status not in ("final", "completed", "off")]
        logger.info(
            "Schedule prediction result: %d/%d games have picks. "
            "Still missing: %s",
            len(with_picks), len(games),
            ", ".join(f"{g.away_team.abbreviation}@{g.home_team.abbreviation}" for g in without_picks) or "none",
        )

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


@router.post("/sync-odds")
async def force_sync_odds(
    session: AsyncSession = Depends(get_session),
):
    """
    Force-sync odds from all sportsbook sources and write to DB.

    Unlike the schedule endpoint's automatic sync, this endpoint ALWAYS
    runs the full odds pipeline regardless of prediction state.  Use this
    when games are missing odds despite being available in sportsbooks.
    """
    from app.scrapers.odds_multi import MultiSourceOddsScraper

    scraper = MultiSourceOddsScraper()
    try:
        matched = await scraper.sync_odds(session)
        await session.flush()
        session.expire_all()

        matched_pairs = [
            f"{m.get('away_abbrev', '')}@{m.get('home_abbrev', '')}"
            for m in (matched or [])
        ]

        # Also regenerate predictions so edges are computed with real odds
        from app.api.predictions import _try_generate_predictions

        today = date.today()
        pred_count = 0
        pred_error = None
        try:
            pred_count = await _try_generate_predictions(session, target_date=today)
            await session.flush()
        except HTTPException as exc:
            pred_error = exc.detail
            logger.warning("sync-odds: prediction generation failed: %s", exc.detail)
        except Exception as exc:
            pred_error = str(exc)
            logger.warning("sync-odds: prediction generation error: %s", exc)

        return {
            "status": "ok",
            "odds_matched": len(matched) if matched else 0,
            "predictions_generated": pred_count,
            "prediction_error": pred_error,
            "matched_games": matched_pairs,
        }
    except Exception as exc:
        logger.error("sync-odds endpoint failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Odds sync failed: {exc}",
        )
    finally:
        await scraper.close()
