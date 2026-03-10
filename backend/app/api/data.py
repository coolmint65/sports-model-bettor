"""
Data management API routes.

Provides endpoints for triggering various data synchronisation operations
(teams, schedule, rosters, game results) and checking the overall status
of synced data in the database.
"""

import asyncio
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional

logger = logging.getLogger(__name__)

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.constants import GAME_FINAL_STATUSES
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
    """Execute the full sync pipeline in the background.

    Each step uses its OWN database session so that writes are committed
    between steps.  This is critical for SQLite: a single long-lived
    session holds the write lock for its entire duration, blocking the
    scheduler's odds sync (which runs in a separate session).  By
    committing after each step we release the lock so the scheduler can
    interleave its fast odds updates.
    """
    global _sync_state
    _sync_state = {"running": True, "step": "Starting sync...", "error": None}

    try:
        from app.scrapers.nhl_api import NHLScraper

        scraper = NHLScraper()
        try:
            # 1. Core sync: teams, rosters, schedule, game results
            _sync_state["step"] = "Syncing teams, rosters, schedule..."
            try:
                async with get_session_context() as session:
                    await scraper.sync_all(session)
                    await session.flush()
            except Exception as exc:
                logger.warning("Core NHL sync failed (non-critical): %s", exc)

            # 2. Historical H2H — each season in its own session
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
                    async with get_session_context() as session:
                        h2h_games += await scraper.sync_historical_season(
                            session, season_str
                        )
            except Exception as exc:
                logger.warning("Historical H2H sync failed (non-critical): %s", exc)

            # 3. Odds via service layer (own session)
            _sync_state["step"] = "Syncing betting odds (multi-source)..."
            try:
                from app.services.odds import sync_odds as svc_sync_odds

                async with get_session_context() as session:
                    matched = await svc_sync_odds(session, force=True)
                    logger.info("Multi-source odds sync matched %d games", len(matched))
            except Exception as exc:
                logger.warning("Multi-source odds sync failed: %s", exc, exc_info=True)

            # 3.5. Injury reports (own session)
            _sync_state["step"] = "Syncing injury reports..."
            try:
                from app.scrapers.injuries import InjuryScraper

                inj_scraper = InjuryScraper()
                try:
                    async with get_session_context() as session:
                        inj_count = await inj_scraper.sync_injuries(session)
                        logger.info("Injury sync: %d active injuries", inj_count)
                finally:
                    await inj_scraper.close()
            except Exception as exc:
                logger.warning("Injury sync failed (non-critical): %s", exc)

            # 4. Predictions (own session with savepoint)
            _sync_state["step"] = "Generating predictions..."
            try:
                from datetime import date as date_type

                from sqlalchemy import delete as sa_delete

                from app.analytics.predictions import PredictionManager

                today = date_type.today()

                async with get_session_context() as session:
                    async with session.begin_nested():
                        non_final_game_ids = select(Game.id).where(
                            Game.date == today,
                            ~func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                        )
                        del_count_result = await session.execute(
                            select(func.count(Prediction.id)).where(
                                Prediction.game_id.in_(non_final_game_ids)
                            )
                        )
                        deleted_count = del_count_result.scalar() or 0

                        await session.execute(
                            sa_delete(Prediction).where(
                                Prediction.game_id.in_(non_final_game_ids)
                            )
                        )
                        await session.flush()

                        manager = PredictionManager()
                        new_bets = await manager.get_best_bets(session)

                        new_count_result = await session.execute(
                            select(func.count(Prediction.id)).where(
                                Prediction.game_id.in_(non_final_game_ids)
                            )
                        )
                        new_count = new_count_result.scalar() or 0

                        if deleted_count > 0 and new_count == 0:
                            logger.warning(
                                "Sync: deleted %d predictions but regenerated 0. "
                                "Best bets returned %d. Games may have unexpected statuses.",
                                deleted_count, len(new_bets),
                            )
                        else:
                            logger.info(
                                "Sync predictions: deleted %d old, created %d new, "
                                "best bets=%d",
                                deleted_count, new_count, len(new_bets),
                            )
            except Exception as exc:
                logger.warning("Prediction generation failed (non-critical): %s", exc)

        finally:
            await scraper.close()

        _sync_state = {"running": False, "step": "Complete", "error": None}
        logger.info("Full background sync completed successfully.")

        # Broadcast to WebSocket clients so frontend updates instantly
        try:
            from app.live import manager as ws_manager
            await ws_manager.broadcast({
                "type": "odds_update",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "changed_games": [],
                "predictions_updated": True,
                "source": "full_sync",
            })
        except Exception as exc:
            logger.warning("WebSocket broadcast after sync failed: %s", exc)

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
    summary="Sync betting odds (multi-source)",
)
async def sync_odds(
    session: AsyncSession = Depends(get_session),
):
    """
    Fetch and update current betting odds from multiple sportsbook sources.

    Sources: DraftKings, FanDuel, Kambi (BetRivers/Unibet), Bovada,
    The Odds API, and Hard Rock Bet.
    Best available lines are computed across all books.
    """
    try:
        from app.services.odds import sync_odds as svc_sync_odds

        matched = await svc_sync_odds(session, force=True)
        sources_seen = set()
        for m in matched:
            sources_seen.update(m.get("sources", []))
        return SyncResult(
            success=True,
            message=f"Odds synced for {len(matched)} games.",
            details=f"Sources: {', '.join(sorted(sources_seen)) or 'none'}. "
                    f"Updated moneyline, spread, and totals odds.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Multi-source odds sync failed: {exc}",
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


@router.get(
    "/odds/test",
    summary="Test all odds sources",
)
async def test_odds_sources():
    """
    Diagnostic endpoint: fetch odds from all sources and return raw results.

    Does NOT write to the database. Returns which sources succeeded,
    how many events each returned, and the merged best-odds output.
    Use this to verify odds sources are working before running a full sync.
    """
    from app.scrapers.odds_multi import MultiSourceOddsScraper

    async with MultiSourceOddsScraper() as scraper:
        merged = await scraper.fetch_best_odds()
        # Build a summary
        source_counts: dict = {}
        for game in merged:
            for src in game.get("sources", []):
                source_counts[src] = source_counts.get(src, 0) + 1

        return {
            "status": "ok" if merged else "no_data",
            "total_games": len(merged),
            "sources_active": source_counts,
            "games": [
                {
                    "matchup": f"{g['home_abbrev']} vs {g['away_abbrev']}",
                    "commence_time": g.get("commence_time"),
                    "sources": g.get("sources", []),
                    "best_odds": g.get("best_odds", {}),
                }
                for g in merged
            ],
        }


@router.get(
    "/odds/diagnose",
    summary="Diagnose odds-to-game matching",
)
async def diagnose_odds_matching(
    session: AsyncSession = Depends(get_session),
):
    """
    Diagnostic endpoint: fetch odds and compare with DB games to show
    exactly which games match and which don't (and why).

    Returns today's DB games, the sportsbook odds, and match results.
    """
    from datetime import date as date_type, timedelta
    from datetime import datetime as dt_cls
    from datetime import timezone as tz

    from app.scrapers.odds_multi import MultiSourceOddsScraper

    today = date_type.today()

    # 1. Get today's DB games
    db_result = await session.execute(
        select(Game)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
        )
        .where(Game.date == today)
    )
    db_games = db_result.scalars().all()
    db_games_info = []
    for g in db_games:
        db_games_info.append({
            "id": g.id,
            "date": str(g.date),
            "status": g.status,
            "home": g.home_team.abbreviation if g.home_team else "?",
            "away": g.away_team.abbreviation if g.away_team else "?",
            "has_moneyline": g.home_moneyline is not None,
            "home_ml": g.home_moneyline,
            "away_ml": g.away_moneyline,
        })

    # 2. Fetch odds
    async with MultiSourceOddsScraper() as scraper:
        odds_list = await scraper.fetch_best_odds()

    # 3. Try matching each odds event
    match_results = []
    for odds in odds_list:
        home_abbrev = odds.get("home_abbrev", "")
        away_abbrev = odds.get("away_abbrev", "")
        commence = odds.get("commence_time", "")

        # Compute game_date the same way sync_odds does
        game_date = None
        date_debug = ""
        if commence:
            try:
                if isinstance(commence, str):
                    ct = commence.replace("Z", "+00:00")
                    dt_val = dt_cls.fromisoformat(ct)
                else:
                    dt_val = commence
                dt_et = dt_val.astimezone(ZoneInfo("America/New_York"))
                game_date = dt_et.date()
                date_debug = f"UTC={dt_val.isoformat()} -> ET={dt_et.isoformat()} -> date={game_date}"
            except Exception as e:
                date_debug = f"parse_error: {e}"

        # Look up teams in DB
        home_result = await session.execute(
            select(Team).where(Team.abbreviation == home_abbrev)
        )
        home_team = home_result.scalar_one_or_none()
        away_result = await session.execute(
            select(Team).where(Team.abbreviation == away_abbrev)
        )
        away_team = away_result.scalar_one_or_none()

        # Try to find matching game
        matched_game = None
        search_dates = []
        if game_date and home_team and away_team:
            for candidate_date in (game_date, game_date - timedelta(days=1), game_date + timedelta(days=1)):
                search_dates.append(str(candidate_date))
                game_result = await session.execute(
                    select(Game).where(
                        Game.home_team_id == home_team.id,
                        Game.away_team_id == away_team.id,
                        Game.date == candidate_date,
                    )
                )
                matched_game = game_result.scalar_one_or_none()
                if matched_game:
                    break

        best = odds.get("best_odds", {})
        match_results.append({
            "matchup": f"{away_abbrev}@{home_abbrev}",
            "commence_time": commence,
            "date_conversion": date_debug,
            "computed_game_date": str(game_date) if game_date else None,
            "today": str(today),
            "date_matches_today": game_date == today if game_date else False,
            "home_team_found": home_team is not None,
            "away_team_found": away_team is not None,
            "searched_dates": search_dates,
            "game_found": matched_game is not None,
            "game_id": matched_game.id if matched_game else None,
            "game_status": matched_game.status if matched_game else None,
            "sources": odds.get("sources", []),
            "best_odds": {
                "home_ml": best.get("home_moneyline"),
                "away_ml": best.get("away_moneyline"),
                "over_under": best.get("over_under"),
                "over_price": best.get("over_price"),
                "under_price": best.get("under_price"),
                "home_spread": best.get("home_spread"),
                "away_spread": best.get("away_spread"),
                "home_spread_price": best.get("home_spread_price"),
                "away_spread_price": best.get("away_spread_price"),
            },
        })

    matched_count = sum(1 for m in match_results if m["game_found"])
    unmatched = [m for m in match_results if not m["game_found"]]

    return {
        "today": str(today),
        "db_games_today": len(db_games_info),
        "odds_events": len(odds_list),
        "matched": matched_count,
        "unmatched_count": len(unmatched),
        "db_games": db_games_info,
        "match_details": match_results,
        "unmatched_details": unmatched,
    }
