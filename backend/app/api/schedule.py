"""
Schedule API routes.

Provides endpoints for retrieving NHL game schedules by date.
Odds syncing is handled by the background scheduler (app.live) —
GET endpoints are read-only.
"""

import logging
from datetime import date, datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.constants import GAME_FINAL_STATUSES, MARKET_BET_TYPES, composite_pick_score
from app.database import get_session
from app.models.game import Game
from app.models.prediction import Prediction
from app.models.team import Team, TeamStats
from app.services.odds import fresh_implied_prob

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
    is_fallback: bool = False


class GameOdds(BaseModel):
    """Snapshot of sportsbook odds for a game."""
    home_moneyline: Optional[float] = None
    away_moneyline: Optional[float] = None
    over_under_line: Optional[float] = None
    over_price: Optional[float] = None
    under_price: Optional[float] = None
    home_spread_line: Optional[float] = None
    away_spread_line: Optional[float] = None
    home_spread_price: Optional[float] = None
    away_spread_price: Optional[float] = None
    odds_updated_at: Optional[str] = None


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
    period_type: Optional[str] = None
    clock: Optional[str] = None
    clock_running: Optional[bool] = None
    in_intermission: Optional[bool] = None
    home_shots: Optional[int] = None
    away_shots: Optional[int] = None
    # Top prediction for this game
    top_pick: Optional[GameTopPick] = None
    # Sportsbook odds
    odds: Optional[GameOdds] = None
    pregame_odds: Optional[GameOdds] = None

    model_config = {"from_attributes": True}


class ScheduleResponse(BaseModel):
    date: date
    game_count: int
    games: List[ScheduleGame]


class SyncResult(BaseModel):
    success: bool
    message: str
    games_synced: int = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _batch_load_team_stats(
    team_ids: List[int], session: AsyncSession
) -> dict[int, Optional[TeamStats]]:
    """Batch-load the latest TeamStats for multiple teams in one query."""
    if not team_ids:
        return {}
    latest_season = (
        select(TeamStats.team_id, func.max(TeamStats.season).label("max_season"))
        .where(TeamStats.team_id.in_(team_ids))
        .group_by(TeamStats.team_id)
        .subquery()
    )
    result = await session.execute(
        select(TeamStats).join(
            latest_season,
            and_(
                TeamStats.team_id == latest_season.c.team_id,
                TeamStats.season == latest_season.c.max_season,
            ),
        )
    )
    return {ts.team_id: ts for ts in result.scalars().all()}


def _build_team_brief(team: Team, stats: Optional[TeamStats] = None) -> TeamBrief:
    brief = TeamBrief(
        id=team.id,
        external_id=team.external_id,
        name=team.name,
        abbreviation=team.abbreviation,
        logo_url=team.logo_url,
    )
    if stats:
        brief.wins = stats.wins
        brief.losses = stats.losses
        brief.ot_losses = stats.ot_losses
        brief.points = stats.points
        brief.record = f"{stats.wins}-{stats.losses}-{stats.ot_losses}"
    return brief


def _build_game_odds(game: Game) -> Optional[GameOdds]:
    if game.home_moneyline is None and game.away_moneyline is None:
        return None
    return GameOdds(
        home_moneyline=game.home_moneyline,
        away_moneyline=game.away_moneyline,
        over_under_line=game.over_under_line,
        over_price=game.over_price,
        under_price=game.under_price,
        home_spread_line=game.home_spread_line,
        away_spread_line=game.away_spread_line,
        home_spread_price=game.home_spread_price,
        away_spread_price=game.away_spread_price,
        odds_updated_at=str(game.odds_updated_at) if game.odds_updated_at else None,
    )


def _build_pregame_odds(game: Game) -> Optional[GameOdds]:
    if game.pregame_home_moneyline is None and game.pregame_away_moneyline is None:
        return None
    return GameOdds(
        home_moneyline=game.pregame_home_moneyline,
        away_moneyline=game.pregame_away_moneyline,
        over_under_line=game.pregame_over_under_line,
        over_price=game.pregame_over_price,
        under_price=game.pregame_under_price,
        home_spread_line=game.pregame_home_spread_line,
        away_spread_line=game.pregame_away_spread_line,
        home_spread_price=game.pregame_home_spread_price,
        away_spread_price=game.pregame_away_spread_price,
    )


def _build_schedule_game(
    game: Game,
    home_brief: TeamBrief,
    away_brief: TeamBrief,
    top_pick: Optional[GameTopPick] = None,
) -> ScheduleGame:
    return ScheduleGame(
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
        period=game.period,
        period_type=game.period_type,
        clock=game.clock,
        clock_running=game.clock_running,
        in_intermission=game.in_intermission,
        home_shots=game.home_shots,
        away_shots=game.away_shots,
        top_pick=top_pick,
        odds=_build_game_odds(game),
        pregame_odds=_build_pregame_odds(game),
    )


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

    # Pre-fetch best prediction per game using composite score
    max_implied = settings.best_bet_max_implied
    game_ids = [g.id for g in games]
    game_by_id = {g.id: g for g in games}
    top_picks: dict[int, GameTopPick] = {}
    if game_ids:
        all_preds_result = await session.execute(
            select(Prediction).where(
                Prediction.game_id.in_(game_ids),
                Prediction.bet_type.in_(MARKET_BET_TYPES),
                Prediction.phase == "prematch",
                Prediction.edge.isnot(None),
                Prediction.odds_implied_prob.isnot(None),
            )
        )
        all_preds = all_preds_result.scalars().all()

        # Compute fresh implied prob using the service layer
        fresh_map: dict[int, Optional[float]] = {}
        scoring_map: dict[int, Optional[float]] = {}
        for p in all_preds:
            game_obj = game_by_id.get(p.game_id)
            fresh_map[p.id] = fresh_implied_prob(p, game_obj)
            scoring_map[p.id] = (
                fresh_map[p.id]
                if fresh_map[p.id] is not None
                else p.odds_implied_prob
            )

        # --- Tier 1: strict best-bet criteria ---
        tier1 = [
            p for p in all_preds
            if (p.edge or 0) >= settings.min_edge
            and (p.confidence or 0) >= settings.min_confidence
            and (
                p.bet_type == "spread"
                or fresh_map.get(p.id) is None
                or fresh_map[p.id] < max_implied
            )
        ]
        for pred in sorted(
            tier1,
            key=lambda p: composite_pick_score(
                p.confidence, p.edge, scoring_map.get(p.id)
            ),
            reverse=True,
        ):
            if pred.game_id not in top_picks:
                top_picks[pred.game_id] = GameTopPick(
                    bet_type=pred.bet_type,
                    prediction_value=pred.prediction_value,
                    confidence=pred.confidence,
                    edge=pred.edge,
                    is_fallback=False,
                )

        # --- Tier 2: fallback for games still missing a pick ---
        missing_ids = set(gid for gid in game_ids if gid not in top_picks)
        if missing_ids:
            tier2 = [
                p for p in all_preds
                if p.game_id in missing_ids
                and (p.edge or 0) >= settings.min_edge
                and (p.confidence or 0) >= settings.min_confidence
            ]

            def _tier2_sort_key(p):
                score = composite_pick_score(
                    p.confidence, p.edge, scoring_map.get(p.id)
                )
                if (
                    p.bet_type == "spread"
                    and p.prediction_value
                    and "+" in p.prediction_value
                ):
                    score -= 0.10
                return score

            for pred in sorted(tier2, key=_tier2_sort_key, reverse=True):
                if pred.game_id not in top_picks:
                    cur_impl = fresh_map.get(pred.id)
                    actually_heavy = (
                        pred.bet_type in ("ml", "total")
                        and cur_impl is not None
                        and cur_impl >= max_implied
                    )
                    top_picks[pred.game_id] = GameTopPick(
                        bet_type=pred.bet_type,
                        prediction_value=pred.prediction_value,
                        confidence=pred.confidence,
                        edge=pred.edge,
                        is_fallback=actually_heavy,
                    )

        # --- Tier 3: confidence-only fallback when odds data is missing ---
        still_missing = set(gid for gid in game_ids if gid not in top_picks)
        if still_missing:
            no_odds_result = await session.execute(
                select(Prediction).where(
                    Prediction.game_id.in_(list(still_missing)),
                    Prediction.bet_type.in_(MARKET_BET_TYPES),
                    Prediction.phase == "prematch",
                )
            )
            no_odds_preds = no_odds_result.scalars().all()
            for pred in sorted(
                no_odds_preds,
                key=lambda p: p.confidence or 0,
                reverse=True,
            ):
                if pred.game_id not in top_picks:
                    top_picks[pred.game_id] = GameTopPick(
                        bet_type=pred.bet_type,
                        prediction_value=pred.prediction_value,
                        confidence=pred.confidence,
                        edge=pred.edge,
                        is_fallback=False,
                    )

    # Batch-load team stats
    all_team_ids = list({g.home_team_id for g in games} | {g.away_team_id for g in games})
    stats_map = await _batch_load_team_stats(all_team_ids, session)

    schedule_games: List[ScheduleGame] = []
    for game in games:
        home_brief = _build_team_brief(game.home_team, stats_map.get(game.home_team_id))
        away_brief = _build_team_brief(game.away_team, stats_map.get(game.away_team_id))
        schedule_games.append(
            _build_schedule_game(game, home_brief, away_brief, top_picks.get(game.id))
        )
    return schedule_games


async def _try_sync_schedule(
    session: AsyncSession, target_date: Optional[date] = None
) -> int:
    """Sync schedule from NHL API. Raises HTTPException on failure."""
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


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

@router.get("/live", response_model=ScheduleResponse)
async def get_live_games(
    session: AsyncSession = Depends(get_session),
):
    """Return all currently in-progress games.

    Syncs scores/clock from NHL API for live games. Odds syncing is
    handled by the background scheduler — not inline in GET requests.
    """
    result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(func.lower(Game.status).in_(("in_progress", "live")))
        .order_by(Game.start_time.asc().nulls_last(), Game.id.asc())
    )
    games = result.scalars().all()

    # Sync scores/clock from NHL API (not odds — scheduler handles that)
    if games:
        try:
            async with session.begin_nested():
                for game in games:
                    await _try_sync_schedule(session, target_date=game.date)
                await session.flush()
        except Exception as exc:
            logger.warning("Live schedule sync failed: %s", exc)

        # Re-query to get fresh ORM objects after savepoint
        result = await session.execute(
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(func.lower(Game.status).in_(("in_progress", "live")))
            .order_by(Game.start_time.asc().nulls_last(), Game.id.asc())
        )
        games = result.scalars().all()

    # Batch-load team stats
    all_team_ids = list({g.home_team_id for g in games} | {g.away_team_id for g in games})
    stats_map = await _batch_load_team_stats(all_team_ids, session)

    schedule_games: List[ScheduleGame] = []
    for game in games:
        home_brief = _build_team_brief(game.home_team, stats_map.get(game.home_team_id))
        away_brief = _build_team_brief(game.away_team, stats_map.get(game.away_team_id))
        schedule_games.append(_build_schedule_game(game, home_brief, away_brief))

    today = date.today()
    return ScheduleResponse(date=today, game_count=len(schedule_games), games=schedule_games)


@router.get("/today", response_model=ScheduleResponse)
async def get_today_schedule(
    session: AsyncSession = Depends(get_session),
):
    """Return today's schedule.

    Syncs scores/clock from NHL API for live games. Odds and predictions
    are kept fresh by the background scheduler — this endpoint only reads.
    """
    today = date.today()
    games = await _games_for_date(today, session)

    # Sync scores/clock if live or if we have no games yet
    has_live = any(g.status and g.status.lower() in ("in_progress", "live") for g in games)
    if not games or has_live:
        try:
            async with session.begin_nested():
                await _try_sync_schedule(session, target_date=today)
                await session.flush()
            games = await _games_for_date(today, session)
        except Exception as exc:
            logger.warning("Today schedule sync failed: %s", exc)

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
    """Force-sync odds from all sportsbook sources and regenerate predictions."""
    from app.services.odds import sync_odds_and_regenerate

    matched, pred_count = await sync_odds_and_regenerate(session, force=True)

    matched_pairs = [
        f"{m.get('away_abbrev', '')}@{m.get('home_abbrev', '')}"
        for m in matched
    ]

    # Broadcast to WebSocket clients
    try:
        from app.live import manager as ws_manager
        await ws_manager.broadcast({
            "type": "odds_update",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "changed_games": [],
            "predictions_updated": pred_count > 0,
            "source": "force_sync_odds",
        })
    except Exception as exc:
        logger.warning("WebSocket broadcast failed: %s", exc)

    return {
        "status": "ok",
        "odds_matched": len(matched),
        "predictions_generated": pred_count,
        "matched_games": matched_pairs,
    }
