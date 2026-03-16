"""
NHL injury report scraper.

Fetches injury data from the NHL API's club endpoint and supplemental
sources. Updates the InjuryReport table with current player injury status.

The NHL API exposes injury info through the roster and landing endpoints:
  - /v1/roster/{team}/current → players may have injuryStatus fields
  - /v1/club-stats/{team}/now → may include absent players

As a supplemental source, we parse injury data from the NHL.com
daily status report JSON feed when available.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.injury import InjuryReport
from app.models.player import Player
from app.models.team import Team
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# NHL API roster endpoint includes injuryStatus for some players
_NHL_API_BASE = "https://api-web.nhle.com/v1"

# Map common injury status strings to normalized values
_STATUS_MAP = {
    "day-to-day": "Day-to-Day",
    "dtd": "Day-to-Day",
    "out": "Out",
    "injured reserve": "IR",
    "ir": "IR",
    "ir-lt": "IR-LT",
    "ir-nr": "IR-NR",
    "ltir": "IR-LT",
    "questionable": "Questionable",
    "probable": "Probable",
    "doubtful": "Doubtful",
    "suspended": "Suspended",
}


def _normalize_status(raw: str) -> str:
    """Normalize an injury status string to a standard value."""
    return _STATUS_MAP.get(raw.strip().lower(), raw.strip().title())


class InjuryScraper(BaseScraper):
    """
    Scraper for NHL injury reports.

    Fetches roster data from the NHL API and extracts injury information
    for players whose roster status indicates they are injured or unavailable.
    """

    def __init__(self, rate_limit: float = 0.5, **kwargs):
        super().__init__(
            base_url=_NHL_API_BASE,
            rate_limit=rate_limit,
            **kwargs,
        )

    async def sync_all(self, db: AsyncSession) -> None:
        """Sync injuries for all teams."""
        await self.sync_injuries(db)

    async def fetch_team_injuries(self, team_abbrev: str) -> List[Dict[str, Any]]:
        """
        Fetch injury data for a team from the NHL API roster endpoint.

        The /roster/{team}/current endpoint returns player objects that
        sometimes include injury-related fields like `injuries` or
        roster status indicators.

        Returns list of dicts with player injury info.
        """
        injuries: List[Dict[str, Any]] = []

        try:
            data = await self.fetch_json(f"/roster/{team_abbrev}/current")
        except Exception as exc:
            logger.warning("Failed to fetch roster for %s: %s", team_abbrev, exc)
            return injuries

        # The roster response has groups: forwards, defensemen, goalies
        for group_key in ("forwards", "defensemen", "goalies"):
            players = data.get(group_key, [])
            for player in players:
                player_id = player.get("id")
                first_name = player.get("firstName", {})
                last_name = player.get("lastName", {})
                name = f"{first_name.get('default', '')} {last_name.get('default', '')}".strip()

                # Check for injury indicators in the player data
                # The NHL API uses different fields across seasons
                injury_status = player.get("injuryStatus")
                injury_desc = player.get("injuryDescription")

                if injury_status:
                    injuries.append({
                        "player_external_id": str(player_id),
                        "player_name": name,
                        "status": _normalize_status(injury_status),
                        "injury_type": injury_desc or "Undisclosed",
                        "source": "nhl_roster",
                    })

        # Also try the /club-stats endpoint for additional injury data
        try:
            landing = await self.fetch_json(f"/club-stats/{team_abbrev}/now")
            # Some versions of the API include an injuries section
            if isinstance(landing, dict):
                for section in ("injuries", "injuredPlayers"):
                    for inj in landing.get(section, []):
                        pid = inj.get("playerId") or inj.get("id")
                        fname = inj.get("firstName", {})
                        lname = inj.get("lastName", {})
                        if isinstance(fname, dict):
                            fname = fname.get("default", "")
                        if isinstance(lname, dict):
                            lname = lname.get("default", "")
                        pname = f"{fname} {lname}".strip() or inj.get("name", "")

                        injuries.append({
                            "player_external_id": str(pid) if pid else None,
                            "player_name": pname,
                            "status": _normalize_status(inj.get("status", "Out")),
                            "injury_type": inj.get("injuryType") or inj.get("description") or "Undisclosed",
                            "detail": inj.get("detail") or inj.get("comment"),
                            "source": "nhl_club_stats",
                        })
        except Exception:
            # club-stats is supplemental — don't fail if unavailable
            pass

        return injuries

    async def sync_injuries(self, db: AsyncSession) -> int:
        """
        Sync injury reports for all teams in the database.

        Fetches roster data for each team, extracts injury information,
        and upserts InjuryReport records. Marks resolved injuries as
        inactive.

        Returns the total number of active injuries found.
        """
        # Get all active teams
        result = await db.execute(
            select(Team).where(Team.active.is_(True))
        )
        teams = result.scalars().all()

        if not teams:
            logger.warning("No active teams found for injury sync")
            return 0

        total_injuries = 0
        now = datetime.now(timezone.utc)

        for team in teams:
            try:
                injuries = await self.fetch_team_injuries(team.abbreviation)
            except Exception as exc:
                logger.warning(
                    "Injury fetch failed for %s: %s", team.abbreviation, exc
                )
                continue

            # Track which player IDs we find injuries for (to deactivate resolved ones)
            found_player_ids = set()

            for inj_data in injuries:
                ext_id = inj_data.get("player_external_id")
                if not ext_id:
                    continue

                # Look up the player in our database
                player_result = await db.execute(
                    select(Player).where(Player.external_id == ext_id)
                )
                player = player_result.scalar_one_or_none()

                if not player:
                    # Try matching by name as fallback
                    pname = inj_data.get("player_name", "")
                    if pname:
                        name_result = await db.execute(
                            select(Player).where(
                                and_(
                                    Player.name == pname,
                                    Player.team_id == team.id,
                                )
                            )
                        )
                        player = name_result.scalar_one_or_none()

                if not player:
                    logger.debug(
                        "Injury skip: player %s (ext_id=%s) not in DB",
                        inj_data.get("player_name"), ext_id,
                    )
                    continue

                found_player_ids.add(player.id)

                # Check for existing active injury report
                existing_result = await db.execute(
                    select(InjuryReport).where(
                        and_(
                            InjuryReport.player_id == player.id,
                            InjuryReport.active.is_(True),
                        )
                    )
                )
                existing = existing_result.scalar_one_or_none()

                status = inj_data.get("status", "Out")
                injury_type = inj_data.get("injury_type", "Undisclosed")
                detail = inj_data.get("detail")
                source = inj_data.get("source", "nhl")

                if existing:
                    # Update existing report if status changed
                    if existing.status != status or existing.injury_type != injury_type:
                        existing.status = status
                        existing.injury_type = injury_type
                        if detail:
                            existing.description = detail
                        existing.source = source
                        logger.info(
                            "Updated injury: %s (%s) -> %s: %s",
                            player.name, team.abbreviation, status, injury_type,
                        )
                else:
                    # Create new injury report
                    report = InjuryReport(
                        player_id=player.id,
                        team_id=team.id,
                        status=status,
                        injury_type=injury_type,
                        description=detail,
                        reported_date=now.date(),
                        source=source,
                        active=True,
                    )
                    db.add(report)
                    logger.info(
                        "New injury: %s (%s) - %s: %s",
                        player.name, team.abbreviation, status, injury_type,
                    )

                total_injuries += 1

            # Mark injuries as resolved for players on this team who are
            # no longer in the injury list
            if found_player_ids or injuries:
                stale_result = await db.execute(
                    select(InjuryReport).where(
                        and_(
                            InjuryReport.team_id == team.id,
                            InjuryReport.active.is_(True),
                            ~InjuryReport.player_id.in_(found_player_ids) if found_player_ids else True,
                        )
                    )
                )
                stale_reports = stale_result.scalars().all()
                for report in stale_reports:
                    # Only deactivate if we got a successful roster fetch
                    # (non-empty injuries list means we parsed the roster)
                    if injuries or found_player_ids:
                        report.active = False
                        logger.info(
                            "Resolved injury: player_id=%d (team %s)",
                            report.player_id, team.abbreviation,
                        )

        logger.info("Injury sync complete: %d active injuries across %d teams", total_injuries, len(teams))
        return total_injuries
