"""
NHL confirmed starting goalie scraper.

Uses the NHL API game landing endpoint to detect confirmed starting goalies
before game time. The landing page includes a ``matchup.goalieComparison``
section with goalie info when lineups are set (typically a few hours before
puck drop).

Fallback: the ``/right-rail`` endpoint sometimes has starter info too.
"""

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.game import Game
from app.models.player import Player
from app.models.team import Team

logger = logging.getLogger(__name__)

NHL_API_BASE = settings.nhl_api_base


async def sync_confirmed_starters(db: AsyncSession) -> List[Dict[str, Any]]:
    """Fetch confirmed starting goalies for today's games.

    Queries the NHL API landing page for each upcoming game.  When a
    goalie is confirmed (``confirmed=True`` in the response), the
    result is returned with high confidence.

    Returns:
        List of dicts with keys: game_id, team_id, team_abbrev,
        goalie_name, goalie_id (external), confirmed (bool).
    """
    # Get today's games that haven't started yet
    today = date.today()
    stmt = (
        select(Game)
        .where(
            Game.date == today,
            Game.status.in_(("scheduled", "preview", "pre-game", "FUT", "PRE")),
        )
    )
    result = await db.execute(stmt)
    games = result.scalars().all()

    if not games:
        logger.debug("Starter scraper: no upcoming games today")
        return []

    starters: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        for game in games:
            try:
                game_starters = await _fetch_game_starters(client, db, game)
                starters.extend(game_starters)
            except Exception as exc:
                logger.warning(
                    "Failed to fetch starters for game %s: %s",
                    game.external_id, exc,
                )

    confirmed_count = sum(1 for s in starters if s["confirmed"])
    logger.info(
        "Starter scraper: %d starters found (%d confirmed) for %d games",
        len(starters), confirmed_count, len(games),
    )
    return starters


async def _fetch_game_starters(
    client: httpx.AsyncClient,
    db: AsyncSession,
    game: Game,
) -> List[Dict[str, Any]]:
    """Extract starting goalie info from a game's landing page."""
    game_ext_id = game.external_id
    url = f"{NHL_API_BASE}/gamecenter/{game_ext_id}/landing"

    resp = await client.get(url)
    if resp.status_code != 200:
        logger.debug("Landing page %d returned %d", game_ext_id, resp.status_code)
        return []

    data = resp.json()
    results: List[Dict[str, Any]] = []

    # Primary path: matchup.goalieComparison
    matchup = data.get("matchup", {})
    goalie_comp = matchup.get("goalieComparison", {})

    for side in ("homeTeam", "awayTeam"):
        team_data = goalie_comp.get(side, {})
        if not team_data:
            # Fallback: try top-level team blocks
            team_data = data.get(side, {})

        goalie_info = _extract_goalie_from_block(team_data)
        if not goalie_info:
            continue

        # Determine team_id from game
        is_home = side == "homeTeam"
        team_id = game.home_team_id if is_home else game.away_team_id

        # Resolve team abbreviation
        team_abbrev = ""
        team_block = data.get(side, {})
        team_abbrev = team_block.get("abbrev", "")
        if not team_abbrev:
            team_obj = await db.get(Team, team_id)
            team_abbrev = team_obj.abbreviation if team_obj else ""

        results.append({
            "game_id": game.id,
            "team_id": team_id,
            "team_abbrev": team_abbrev,
            "goalie_name": goalie_info["name"],
            "goalie_external_id": str(goalie_info["player_id"]),
            "confirmed": goalie_info["confirmed"],
        })

    return results


def _extract_goalie_from_block(block: dict) -> Optional[Dict[str, Any]]:
    """Extract goalie name, id, and confirmation status from an API block.

    The NHL API uses several different structures depending on game state:
    - ``matchup.goalieComparison.homeTeam`` has ``name``, ``playerId``
    - ``homeTeam.startingGoalie`` has ``name.default``, ``id``
    - Simple fallback: any block with a goalie-like structure
    """
    # Pattern 1: goalieComparison block
    name_obj = block.get("name", {})
    if isinstance(name_obj, dict):
        name = name_obj.get("default", "")
    elif isinstance(name_obj, str):
        name = name_obj
    else:
        name = ""

    player_id = block.get("playerId") or block.get("id")

    if not name and not player_id:
        # Pattern 2: startingGoalie sub-block
        starter = block.get("startingGoalie", {})
        if starter:
            name_obj = starter.get("name", {})
            if isinstance(name_obj, dict):
                name = name_obj.get("default", "")
            elif isinstance(name_obj, str):
                name = name_obj
            player_id = starter.get("playerId") or starter.get("id")

    if not name and not player_id:
        return None

    # The API sets ``confirmed`` when the team has officially announced.
    # If not present, treat as "projected" (lower confidence).
    confirmed = block.get("confirmed", False)
    if not confirmed:
        # Also check startingGoalie block
        confirmed = block.get("startingGoalie", {}).get("confirmed", False)

    return {
        "name": name,
        "player_id": player_id,
        "confirmed": bool(confirmed),
    }


async def get_confirmed_starter_for_team(
    db: AsyncSession,
    game_id: int,
    team_id: int,
) -> Optional[Dict[str, Any]]:
    """Look up the confirmed starter for a specific team in a game.

    This is a convenience wrapper used by the feature engine.
    Fetches from the NHL API on each call (results are cached by the
    scheduler which calls sync_confirmed_starters periodically).

    Returns:
        Dict with goalie_name, goalie_external_id, confirmed, or None.
    """
    game = await db.get(Game, game_id)
    if not game:
        return None

    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            starters = await _fetch_game_starters(client, db, game)
            for s in starters:
                if s["team_id"] == team_id:
                    return s
    except Exception as exc:
        logger.debug("Could not fetch starter for game %d team %d: %s", game_id, team_id, exc)

    return None
