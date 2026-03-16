"""
MoneyPuck 5v5 possession data scraper.

Fetches team-level even-strength (5v5) advanced stats from MoneyPuck's
free CSV downloads. Data includes true Corsi, Fenwick, and expected goals
percentages at 5-on-5 — metrics that cannot be derived from boxscore data.

CSV source: https://moneypuck.com/moneypuck/playerData/seasonSummary/{YEAR}/regular/teams.csv
Updated nightly by MoneyPuck.
"""

import io
import logging
from datetime import date, datetime, timezone
from typing import Dict, Optional

import httpx
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.team import Team, TeamEVStats
from app.scrapers.team_map import MONEYPUCK_TEAM_MAP

logger = logging.getLogger(__name__)

MONEYPUCK_BASE = "https://moneypuck.com/moneypuck/playerData/seasonSummary"

# Re-export for backward compatibility
_TEAM_NAME_MAP = MONEYPUCK_TEAM_MAP


def _current_season_year() -> int:
    """Return the start year of the current NHL season.

    The NHL season spans two calendar years. If we're before September,
    the season started the previous year; otherwise it started this year.
    """
    today = date.today()
    return today.year if today.month >= 9 else today.year - 1


def _season_string(year: int) -> str:
    """Format season as '20252026' for database storage."""
    return f"{year}{year + 1}"


async def sync_moneypuck_ev_stats(db: AsyncSession) -> int:
    """Fetch and store 5v5 possession stats from MoneyPuck.

    Downloads the team-level CSV, filters to situation == '5on5',
    and upserts into TeamEVStats.

    Returns:
        Number of team records created or updated.
    """
    season_year = _current_season_year()
    url = f"{MONEYPUCK_BASE}/{season_year}/regular/teams.csv"
    season_str = _season_string(season_year)

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.error(
                    "MoneyPuck CSV fetch failed: HTTP %d from %s",
                    resp.status_code, url,
                )
                return 0

            csv_text = resp.text

    except Exception as e:
        logger.error("MoneyPuck fetch error: %s", e)
        return 0

    return await _parse_and_store(db, csv_text, season_str)


async def _parse_and_store(
    db: AsyncSession,
    csv_text: str,
    season_str: str,
) -> int:
    """Parse MoneyPuck CSV and upsert TeamEVStats rows."""
    import csv

    reader = csv.DictReader(io.StringIO(csv_text))

    # Build a lookup of team abbreviation -> Team from our DB
    team_stmt = select(Team).where(Team.active == True)
    result = await db.execute(team_stmt)
    teams = {t.abbreviation: t for t in result.scalars().all()}

    updated = 0
    today = date.today()

    for row in reader:
        # Filter to 5v5 situation only
        situation = row.get("situation", "").strip()
        if situation != "5on5":
            continue

        mp_abbr = row.get("team", "").strip()
        our_abbr = _TEAM_NAME_MAP.get(mp_abbr, mp_abbr)
        team = teams.get(our_abbr)
        if not team:
            logger.debug("MoneyPuck team '%s' -> '%s' not found in DB", mp_abbr, our_abbr)
            continue

        # Extract metrics
        ev_cf_pct = _safe_float(row.get("corsiPercentage"))
        ev_ff_pct = _safe_float(row.get("fenwickPercentage"))
        ev_xgf_pct = _safe_float(row.get("xGoalsPercentage"))
        ev_shots_pct = _safe_float(row.get("shotsOnGoalForPercentage"))
        games_played = _safe_int(row.get("games_played", row.get("gamesPlayed", "0")))

        # Convert from 0-1 to percentage if needed
        if ev_cf_pct is not None and ev_cf_pct <= 1.0:
            ev_cf_pct *= 100.0
        if ev_ff_pct is not None and ev_ff_pct <= 1.0:
            ev_ff_pct *= 100.0
        if ev_xgf_pct is not None and ev_xgf_pct <= 1.0:
            ev_xgf_pct *= 100.0
        if ev_shots_pct is not None and ev_shots_pct <= 1.0:
            ev_shots_pct *= 100.0

        # Upsert
        existing_stmt = select(TeamEVStats).where(
            and_(
                TeamEVStats.team_id == team.id,
                TeamEVStats.season == season_str,
            )
        )
        existing_result = await db.execute(existing_stmt)
        existing = existing_result.scalars().first()

        if existing:
            existing.ev_cf_pct = ev_cf_pct
            existing.ev_ff_pct = ev_ff_pct
            existing.ev_xgf_pct = ev_xgf_pct
            existing.ev_shots_for_pct = ev_shots_pct
            existing.games_played = games_played
            existing.scrape_date = today
        else:
            record = TeamEVStats(
                team_id=team.id,
                season=season_str,
                ev_cf_pct=ev_cf_pct,
                ev_ff_pct=ev_ff_pct,
                ev_xgf_pct=ev_xgf_pct,
                ev_shots_for_pct=ev_shots_pct,
                games_played=games_played,
                scrape_date=today,
            )
            db.add(record)

        updated += 1

    logger.info("MoneyPuck 5v5 sync: %d teams updated for season %s", updated, season_str)
    return updated


def _safe_float(val: Optional[str]) -> Optional[float]:
    """Safely convert a CSV value to float."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val: Optional[str]) -> int:
    """Safely convert a CSV value to int."""
    if val is None or val == "":
        return 0
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0
