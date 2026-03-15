"""
API endpoints for player prop odds and picks.

Provides endpoints to fetch player props grouped by game for today's
NHL schedule, per-game prop details, and AI-generated player prop
predictions (ATG, SOG, Points, Assists, Saves).
"""

from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.constants import GAME_FINAL_STATUSES
from app.database import get_session, get_write_session_context
from app.models.game import Game, GameGoalieStats, GamePlayerStats
from app.models.player import Player
from app.models.player_prop import PlayerPropOdds
from app.models.prop_pick_snapshot import PropPickSnapshot
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
    """Manually trigger a player props sync with full diagnostics.

    Traces every step of the pipeline so we can see exactly where
    it breaks: API key → bulk fetch → event discovery → per-event
    props → game matching → DB upsert.
    """
    import logging

    import httpx

    from app.config import settings
    from app.scrapers.odds_multi import _fetch_odds_api_raw, _map_team

    logger = logging.getLogger(__name__)
    diag: Dict[str, Any] = {"steps": []}

    # Step 1: Check API key
    api_key = settings.odds_api_key
    if not api_key:
        diag["steps"].append("FAIL: ODDS_API_KEY is not set in settings")
        diag["status"] = "error"
        diag["props_synced"] = 0
        diag["fix"] = (
            "Ensure backend/.env contains ODDS_API_KEY=<your_key> "
            "with no BOM, no quotes, no leading spaces. "
            "Restart the backend after editing."
        )
        return diag
    masked = f"{api_key[:6]}...{api_key[-4:]}"
    diag["steps"].append(f"OK: API key loaded ({masked})")

    # Step 2: Clear props cache
    try:
        import app.scrapers.player_props as _pp_mod
        _pp_mod._props_cache = {}
        _pp_mod._props_cache_ts = 0.0
        diag["steps"].append("OK: Props cache cleared")
    except Exception as exc:
        diag["steps"].append(f"WARN: Could not clear cache: {exc}")

    # Step 3: Direct connectivity test to The Odds API
    # This bypasses _fetch_odds_api_raw to capture the exact error.
    test_url = "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds"
    test_params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": "h2h",
        "oddsFormat": "american",
    }
    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=20.0,
        limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
    ) as client:
        # Also clear the bulk odds cache so we get fresh data
        try:
            import app.scrapers.odds_multi as _om_mod
            _om_mod._odds_api_cache["data"] = None
            _om_mod._odds_api_cache["timestamp"] = 0.0
        except Exception:
            pass

        try:
            resp = await client.get(test_url, params=test_params, timeout=20.0)
            diag["steps"].append(
                f"OK: Odds API responded HTTP {resp.status_code} "
                f"(remaining: {resp.headers.get('x-requests-remaining', '?')}, "
                f"used: {resp.headers.get('x-requests-used', '?')})"
            )
            if resp.status_code != 200:
                try:
                    body = resp.text[:500]
                except Exception:
                    body = "(could not read body)"
                diag["steps"].append(f"FAIL: API returned HTTP {resp.status_code}: {body}")
                diag["status"] = "error"
                diag["props_synced"] = 0
                return diag
            # If we get here, the API is reachable — use this data
            raw = resp.json()
        except httpx.ConnectError as exc:
            diag["steps"].append(
                f"FAIL: Cannot connect to api.the-odds-api.com: {exc}. "
                "This is a network/firewall issue on your machine, not an API key problem."
            )
            diag["status"] = "error"
            diag["props_synced"] = 0
            return diag
        except httpx.TimeoutException as exc:
            diag["steps"].append(
                f"FAIL: Connection timed out after 20s: {exc}. "
                "Check firewall, VPN, or proxy settings."
            )
            diag["status"] = "error"
            diag["props_synced"] = 0
            return diag
        except Exception as exc:
            diag["steps"].append(
                f"FAIL: Unexpected error calling Odds API: {type(exc).__name__}: {exc}"
            )
            diag["status"] = "error"
            diag["props_synced"] = 0
            return diag

    if not raw or not isinstance(raw, list):
        diag["steps"].append("FAIL: Bulk API returned empty list — no NHL events found")
        diag["status"] = "error"
        diag["props_synced"] = 0
        return diag

    event_ids = [ev.get("id") for ev in raw if ev.get("id")]
    event_teams = {
        ev.get("id"): f"{_map_team(ev.get('away_team',''))}@{_map_team(ev.get('home_team',''))}"
        for ev in raw if ev.get("id")
    }
    diag["steps"].append(
        f"OK: Bulk API returned {len(raw)} events: "
        + ", ".join(event_teams.get(eid, eid) for eid in event_ids[:8])
    )

    # Step 4: Fetch props for each event
    from app.scrapers.player_props import _fetch_event_props

    props_by_event: Dict[str, List] = {}
    async with httpx.AsyncClient(
        follow_redirects=True,
        limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
    ) as client:
        for eid in event_ids:
            try:
                props = await _fetch_event_props(client, eid)
                props_by_event[eid] = props
            except Exception as exc:
                diag["steps"].append(f"WARN: Props fetch failed for {event_teams.get(eid, eid)}: {exc}")

    total_fetched = sum(len(v) for v in props_by_event.values())
    per_event_summary = {
        event_teams.get(eid, eid): len(props)
        for eid, props in props_by_event.items()
    }
    diag["steps"].append(f"OK: Fetched {total_fetched} prop lines across {len(props_by_event)} events")
    diag["props_per_event"] = per_event_summary

    if total_fetched == 0:
        diag["steps"].append(
            "FAIL: No prop lines returned from any event. "
            "Possible causes: (1) API credits exhausted — check "
            "https://the-odds-api.com/account/, (2) props not yet "
            "available for today's games (usually posted 2-4h before game time), "
            "(3) per-event endpoint returning 401/403"
        )
        diag["status"] = "error"
        diag["props_synced"] = 0
        return diag

    # Step 5: Populate caches so sync_player_props doesn't re-fetch
    import time as _time_mod
    import app.scrapers.player_props as _pp_mod
    import app.scrapers.odds_multi as _om_mod

    # Seed the bulk odds cache (used by sync_player_props for event metadata)
    _om_mod._odds_api_cache["data"] = raw
    _om_mod._odds_api_cache["timestamp"] = _time_mod.monotonic()

    # Seed the props cache (used by fetch_all_player_props)
    _pp_mod._props_cache = props_by_event
    _pp_mod._props_cache_ts = _time_mod.monotonic()

    # Step 6: Match events to games and persist
    try:
        async with get_write_session_context() as session:
            from app.services.odds import sync_player_props
            count = await sync_player_props(session)
            diag["steps"].append(f"OK: Persisted {count} prop lines to database")
            diag["status"] = "ok"
            diag["props_synced"] = count
    except Exception as exc:
        logger.error("Props sync DB step failed: %s", exc, exc_info=True)
        diag["steps"].append(f"FAIL: DB persist error: {exc}")
        diag["status"] = "error"
        diag["props_synced"] = 0

    return diag


_PROP_STAT_FIELD = {
    "player_shots_on_goal": "shots",
    "player_points": "points",
    "player_assists": "assists",
    "player_goal_scorer_anytime": "goals",
}



async def _get_or_create_snapshots(
    session: AsyncSession,
    game_id: int,
) -> List["PropPickSnapshot"]:
    """Return frozen prop pick snapshots for a game.

    If snapshots already exist, return them (picks are frozen).
    Otherwise, generate fresh picks, persist as snapshots, and return.
    Uses a write session to save new snapshots.
    """
    from app.models.prop_pick_snapshot import PropPickSnapshot
    from app.services.player_prop_picks import generate_prop_picks

    # Check for existing snapshots
    existing = await session.execute(
        select(PropPickSnapshot)
        .where(PropPickSnapshot.game_id == game_id)
        .order_by(PropPickSnapshot.edge.desc())
    )
    snapshots = list(existing.scalars().all())
    if snapshots:
        return snapshots

    # No snapshots yet — generate and persist
    picks = await generate_prop_picks(session, game_id)
    if not picks:
        return []

    async with get_write_session_context() as write_session:
        new_snapshots = []
        for p in picks:
            snap = PropPickSnapshot(
                game_id=game_id,
                player_id=p.player_id,
                player_name=p.player_name,
                market=p.market,
                pick_side=p.pick_side,
                line=p.line,
                odds=p.odds,
                model_prob=round(p.model_prob, 4),
                implied_prob=round(p.implied_prob, 4),
                edge=round(p.edge, 4),
                confidence=round(p.confidence, 4),
                avg_rate=round(p.avg_rate, 2),
                games_sampled=p.games_sampled,
                reasoning=p.reasoning,
            )
            write_session.add(snap)
            new_snapshots.append(snap)
        await write_session.flush()

        # Re-read from the read session so the caller has attached objects
        result = await session.execute(
            select(PropPickSnapshot)
            .where(PropPickSnapshot.game_id == game_id)
            .order_by(PropPickSnapshot.edge.desc())
        )
        return list(result.scalars().all())


async def _grade_snapshots(
    session: AsyncSession,
    final_games: List[Game],
    snapshots_by_game: Dict[int, List["PropPickSnapshot"]],
) -> None:
    """Grade frozen snapshots for final games and persist outcomes.

    Only grades snapshots that haven't been graded yet (outcome is None).
    """
    from app.models.prop_pick_snapshot import PropPickSnapshot

    game_ids = [g.id for g in final_games if g.id in snapshots_by_game]
    if not game_ids:
        return

    # Check if any need grading
    ungraded_ids = set()
    for gid in game_ids:
        for snap in snapshots_by_game[gid]:
            if snap.outcome is None:
                ungraded_ids.add(gid)
                break
    if not ungraded_ids:
        return

    # Load actual stats for ungraded games
    stats_result = await session.execute(
        select(GamePlayerStats)
        .options(selectinload(GamePlayerStats.player))
        .where(GamePlayerStats.game_id.in_(ungraded_ids))
    )
    skater_stats = stats_result.scalars().all()

    goalie_result = await session.execute(
        select(GameGoalieStats)
        .options(selectinload(GameGoalieStats.player))
        .where(GameGoalieStats.game_id.in_(ungraded_ids))
    )
    goalie_stats = goalie_result.scalars().all()

    skater_by_game: Dict[tuple, GamePlayerStats] = {}
    for s in skater_stats:
        if s.player:
            skater_by_game[(s.game_id, s.player.name.lower())] = s

    goalie_by_game: Dict[tuple, GameGoalieStats] = {}
    for g in goalie_stats:
        if g.player:
            goalie_by_game[(g.game_id, g.player.name.lower())] = g

    # Grade and persist
    async with get_write_session_context() as write_session:
        for gid in ungraded_ids:
            for snap in snapshots_by_game[gid]:
                if snap.outcome is not None:
                    continue

                name_lower = snap.player_name.lower()
                outcome = None

                if snap.market == "player_total_saves":
                    gs = goalie_by_game.get((gid, name_lower))
                    if gs is not None:
                        if snap.pick_side == "over":
                            outcome = gs.saves > snap.line
                        else:
                            outcome = gs.saves < snap.line
                elif snap.market == "player_goal_scorer_anytime":
                    ss = skater_by_game.get((gid, name_lower))
                    if ss is not None:
                        outcome = ss.goals >= 1
                else:
                    ss = skater_by_game.get((gid, name_lower))
                    if ss is not None:
                        field = _PROP_STAT_FIELD.get(snap.market)
                        if field:
                            actual = getattr(ss, field, 0)
                            if snap.pick_side == "over":
                                outcome = actual > snap.line
                            else:
                                outcome = actual < snap.line

                if outcome is not None:
                    await write_session.execute(
                        select(PropPickSnapshot)
                        .where(PropPickSnapshot.id == snap.id)
                    )
                    # Update via direct SQL for efficiency
                    from sqlalchemy import update
                    await write_session.execute(
                        update(PropPickSnapshot)
                        .where(PropPickSnapshot.id == snap.id)
                        .values(outcome=outcome)
                    )
                    snap.outcome = outcome

        await write_session.flush()


def _snapshot_to_dict(snap, include_outcome: bool = False) -> dict:
    """Convert a PropPickSnapshot to API response dict."""
    d = {
        "player_name": snap.player_name,
        "player_id": snap.player_id,
        "market": snap.market,
        "pick_side": snap.pick_side,
        "line": snap.line,
        "odds": snap.odds,
        "model_prob": snap.model_prob,
        "implied_prob": snap.implied_prob,
        "edge": snap.edge,
        "confidence": snap.confidence,
        "avg_rate": snap.avg_rate,
        "games_sampled": snap.games_sampled,
        "reasoning": snap.reasoning,
    }
    if include_outcome:
        d["outcome"] = snap.outcome
    return d


@router.get("/picks/today")
async def get_todays_prop_picks(
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get AI-generated player prop picks for today's games.

    Picks are frozen on first generation and never re-computed,
    preventing look-ahead bias when games go final.  Outcomes
    (hit/miss) are graded against actual stats once available.
    """
    from app.models.prop_pick_snapshot import PropPickSnapshot

    today = date.today()

    games_result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.date == today)
        .order_by(Game.start_time)
    )
    games = games_result.scalars().all()

    if not games:
        return {"games": [], "total_picks": 0}

    # Get or create frozen snapshots for each game
    snapshots_by_game: Dict[int, List[PropPickSnapshot]] = {}
    for game in games:
        snapshots = await _get_or_create_snapshots(session, game.id)
        if snapshots:
            snapshots_by_game[game.id] = snapshots

    # Grade final games
    final_statuses = {s.lower() for s in GAME_FINAL_STATUSES}
    final_games = [g for g in games if (g.status or "").lower() in final_statuses]
    await _grade_snapshots(session, final_games, snapshots_by_game)

    game_list = []
    total_picks = 0
    for game in games:
        game_snaps = snapshots_by_game.get(game.id, [])
        total_picks += len(game_snaps)
        game_list.append({
            "game_id": game.id,
            "home_team": game.home_team.abbreviation if game.home_team else "",
            "away_team": game.away_team.abbreviation if game.away_team else "",
            "home_team_name": game.home_team.name if game.home_team else "",
            "away_team_name": game.away_team.name if game.away_team else "",
            "start_time": game.start_time.isoformat() if game.start_time else None,
            "status": game.status,
            "picks": [
                _snapshot_to_dict(s, include_outcome=True)
                for s in game_snaps
            ],
            "pick_count": len(game_snaps),
        })

    return {
        "games": game_list,
        "total_picks": total_picks,
    }


@router.get("/picks/game/{game_id}")
async def get_game_prop_picks(
    game_id: int,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get AI-generated player prop picks for a specific game.

    Picks are frozen on first generation.
    """
    game_result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.id == game_id)
    )
    game = game_result.scalar_one_or_none()
    if not game:
        return {"error": "Game not found", "picks": []}

    snapshots = await _get_or_create_snapshots(session, game_id)

    return {
        "game_id": game_id,
        "home_team": game.home_team.abbreviation if game.home_team else "",
        "away_team": game.away_team.abbreviation if game.away_team else "",
        "picks": [_snapshot_to_dict(s) for s in snapshots],
        "total_picks": len(snapshots),
    }
