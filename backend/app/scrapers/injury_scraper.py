"""
NHL injury report scraper.

Fetches injury data from the NHL API and stores it in the InjuryReport table.
Identifies injured players, their status, and snapshots their production
metrics so the prediction model can accurately assess lineup impact.
"""

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.game import GamePlayerStats
from app.models.injury import InjuryReport
from app.models.player import Player

logger = logging.getLogger(__name__)

# NHL API endpoints for injury/roster data
NHL_API_BASE = settings.nhl_api_base

# Map NHL API injury designations to our status values
_STATUS_MAP = {
    "injured reserve": "ir",
    "ir": "ir",
    "long-term injured reserve": "ir",
    "ltir": "ir",
    "day-to-day": "day-to-day",
    "dtd": "day-to-day",
    "out": "out",
    "questionable": "questionable",
    "probable": "probable",
    "suspended": "out",
}


async def fetch_injury_reports(db: AsyncSession) -> int:
    """Fetch and store injury reports from the NHL API.

    Queries each team's roster/prospect data to identify players with
    injury designations, then upserts InjuryReport records.

    Returns:
        Number of injury records created or updated.
    """
    updated_count = 0

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # Get all teams to iterate their rosters
            teams_resp = await client.get(f"{NHL_API_BASE}/standings/now")
            if teams_resp.status_code != 200:
                logger.error("Failed to fetch standings: %d", teams_resp.status_code)
                return 0

            standings = teams_resp.json()
            team_abbrs = []
            for entry in standings.get("standings", []):
                abbr = entry.get("teamAbbrev", {}).get("default", "")
                if abbr:
                    team_abbrs.append(abbr)

            for abbr in team_abbrs:
                try:
                    count = await _process_team_injuries(client, db, abbr)
                    updated_count += count
                except Exception as e:
                    logger.error("Error processing injuries for %s: %s", abbr, e)

    except Exception as e:
        logger.error("Injury scraper failed: %s", e)

    # Deactivate old reports that are no longer showing up
    await _deactivate_stale_reports(db)

    logger.info("Injury scraper complete: %d records updated", updated_count)
    return updated_count


async def _process_team_injuries(
    client: httpx.AsyncClient,
    db: AsyncSession,
    team_abbr: str,
) -> int:
    """Process injury reports for a single team.

    Uses the club stats roster endpoint to find players with injury
    designations in their status.

    Returns:
        Number of injury records created/updated for this team.
    """
    count = 0

    # Try the roster endpoint
    try:
        resp = await client.get(
            f"{NHL_API_BASE}/roster/{team_abbr}/current"
        )
        if resp.status_code != 200:
            return 0

        roster_data = resp.json()
    except Exception as e:
        logger.debug("Could not fetch roster for %s: %s", team_abbr, e)
        return 0

    # Process each position group
    for position_group in ["forwards", "defensemen", "goalies"]:
        players = roster_data.get(position_group, [])
        for player_data in players:
            player_id_ext = str(player_data.get("id", ""))
            injury_status = _extract_injury_status(player_data)

            if not injury_status:
                continue

            # Look up player in our DB
            player_stmt = select(Player).where(Player.external_id == player_id_ext)
            result = await db.execute(player_stmt)
            player = result.scalars().first()

            if not player:
                continue

            # Get player production metrics
            ppg, gpg, toi = await _get_player_metrics(db, player.id)

            # Upsert injury report
            existing_stmt = select(InjuryReport).where(
                and_(
                    InjuryReport.player_id == player.id,
                    InjuryReport.is_active.is_(True),
                )
            )
            existing_result = await db.execute(existing_stmt)
            existing = existing_result.scalars().first()

            injury_type = player_data.get("injuryType", None)
            if isinstance(injury_type, dict):
                injury_type = injury_type.get("default", "undisclosed")

            if existing:
                existing.status = injury_status
                existing.injury_type = injury_type
            else:
                report = InjuryReport(
                    player_id=player.id,
                    team_id=player.team_id,
                    status=injury_status,
                    injury_type=injury_type,
                    reported_at=datetime.now(timezone.utc),
                    source="nhl_api",
                    is_active=True,
                )
                db.add(report)

            count += 1

    return count


def _extract_injury_status(player_data: Dict[str, Any]) -> Optional[str]:
    """Extract and normalize injury status from NHL API player data.

    Returns None if the player is healthy.
    """
    # Check various fields where injury info might appear
    for field in ["injuryStatus", "status", "rosterStatus"]:
        raw = player_data.get(field)
        if raw is None:
            continue
        if isinstance(raw, dict):
            raw = raw.get("default", "")
        raw_lower = str(raw).lower().strip()

        if raw_lower in _STATUS_MAP:
            return _STATUS_MAP[raw_lower]

        # Check if the status contains injury keywords
        for keyword, status in _STATUS_MAP.items():
            if keyword in raw_lower:
                return status

    return None


async def _get_player_metrics(
    db: AsyncSession,
    player_id: int,
) -> tuple:
    """Get a player's recent per-game production metrics.

    Returns (ppg, gpg, avg_toi) from the last 20 games.
    """
    stmt = (
        select(
            func.avg(GamePlayerStats.points).label("ppg"),
            func.avg(GamePlayerStats.goals).label("gpg"),
            func.avg(GamePlayerStats.toi).label("avg_toi"),
        )
        .where(GamePlayerStats.player_id == player_id)
        .order_by(GamePlayerStats.id.desc())
        .limit(20)
    )

    # SQLAlchemy doesn't support LIMIT on aggregates directly,
    # so use a subquery approach
    from sqlalchemy import literal_column
    sub = (
        select(
            GamePlayerStats.points,
            GamePlayerStats.goals,
            GamePlayerStats.toi,
        )
        .where(GamePlayerStats.player_id == player_id)
        .order_by(GamePlayerStats.id.desc())
        .limit(20)
        .subquery()
    )
    agg_stmt = select(
        func.avg(sub.c.points).label("ppg"),
        func.avg(sub.c.goals).label("gpg"),
        func.avg(sub.c.toi).label("avg_toi"),
    )
    result = await db.execute(agg_stmt)
    row = result.one_or_none()

    if row:
        return (
            round(row.ppg or 0, 3),
            round(row.gpg or 0, 3),
            round(row.avg_toi or 0, 1),
        )
    return (0.0, 0.0, 0.0)


async def _deactivate_stale_reports(db: AsyncSession) -> None:
    """Mark injury reports as inactive if the player wasn't found
    in the latest scrape (they may have been activated).

    We use a conservative approach: only deactivate reports that
    haven't been updated in the last 48 hours.
    """
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)

    stmt = (
        select(InjuryReport)
        .where(
            and_(
                InjuryReport.is_active.is_(True),
                InjuryReport.updated_at < cutoff,
            )
        )
    )
    result = await db.execute(stmt)
    stale = result.scalars().all()

    for report in stale:
        report.is_active = False
        logger.info(
            "Deactivated stale injury report: player_id=%d, status=%s",
            report.player_id, report.status,
        )
