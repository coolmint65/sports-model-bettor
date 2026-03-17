"""
NBA injury report scraper.

Fetches injury data from the ESPN NBA injuries page and stores it in
the InjuryReport table. The prediction model uses this to adjust
expected points for teams missing key players.
"""

import logging
import re
from datetime import date, datetime, timezone
from typing import Dict, Optional

import httpx
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.injury import InjuryReport
from app.models.player import Player
from app.models.team import Team
from app.scrapers.team_map import resolve_nba_team

logger = logging.getLogger(__name__)

ESPN_INJURIES_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"

# Map ESPN injury status to our standard statuses
_STATUS_MAP = {
    "out": "out",
    "day-to-day": "day-to-day",
    "questionable": "questionable",
    "probable": "probable",
    "doubtful": "questionable",
    "suspended": "out",
    "injured reserve": "ir",
    "not with team": "out",
}


async def fetch_nba_injury_reports(db: AsyncSession) -> int:
    """Fetch and store NBA injury reports from ESPN.

    Returns:
        Number of injury records created or updated.
    """
    updated_count = 0

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(ESPN_INJURIES_URL)
            if resp.status_code != 200:
                logger.error("Failed to fetch NBA injuries: %d", resp.status_code)
                return 0

            data = resp.json()

    except Exception as exc:
        logger.error("NBA injury fetch failed: %s", exc)
        return 0

    # ESPN returns injuries grouped by team
    for team_data in data.get("items", []):
        team_info = team_data.get("team", {})
        team_abbr = team_info.get("abbreviation", "")

        # Normalize team abbreviation
        if not team_abbr:
            team_name = team_info.get("displayName", "")
            team_abbr = resolve_nba_team(team_name)

        if not team_abbr:
            continue

        # Find team in DB
        team_result = await db.execute(
            select(Team).where(
                Team.abbreviation == team_abbr,
                Team.sport == "nba",
            )
        )
        team = team_result.scalar_one_or_none()
        if not team:
            continue

        for injury in team_data.get("injuries", []):
            athlete = injury.get("athlete", {})
            player_name = athlete.get("displayName", "")
            if not player_name:
                continue

            # Find player in DB by name and team
            player_result = await db.execute(
                select(Player).where(
                    Player.team_id == team.id,
                    Player.sport == "nba",
                    Player.name == player_name,
                )
            )
            player = player_result.scalar_one_or_none()

            # Try fuzzy match if exact name doesn't work
            if not player:
                player_result = await db.execute(
                    select(Player).where(
                        Player.team_id == team.id,
                        Player.sport == "nba",
                        Player.name.ilike(f"%{player_name.split()[-1]}%"),
                    )
                )
                player = player_result.scalars().first()

            if not player:
                logger.debug(
                    "NBA injury: player %s (%s) not found in DB",
                    player_name, team_abbr,
                )
                continue

            # Parse status
            status_raw = (injury.get("status", "") or "").lower().strip()
            status = _STATUS_MAP.get(status_raw, "day-to-day")

            # Determine impact description
            injury_type = injury.get("type", {}).get("text", "")
            details = injury.get("details", {})
            description = details.get("detail", "") or injury_type

            # Check for existing injury record
            existing_result = await db.execute(
                select(InjuryReport).where(
                    InjuryReport.player_id == player.id,
                )
            )
            existing = existing_result.scalar_one_or_none()

            if existing:
                existing.status = status
                existing.impact = description
                existing.date = date.today()
            else:
                report = InjuryReport(
                    player_id=player.id,
                    date=date.today(),
                    status=status,
                    impact=description,
                )
                db.add(report)

            updated_count += 1

    await db.flush()
    logger.info("NBA injury reports synced: %d records", updated_count)
    return updated_count
