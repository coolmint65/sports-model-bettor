"""
API endpoints for injury reports and their impact on predictions.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.models.injury import InjuryReport
from app.models.player import Player
from app.models.team import Team

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/injuries", tags=["injuries"])


@router.get("/{team_abbr}")
async def get_team_injuries(
    team_abbr: str,
    active_only: bool = True,
    db: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get injury reports for a team.

    Args:
        team_abbr: Team abbreviation (e.g., 'BOS', 'COL').
        active_only: If True, only return active (current) injuries.
    """
    # Look up team
    team_stmt = select(Team).where(Team.abbreviation == team_abbr.upper())
    team_result = await db.execute(team_stmt)
    team = team_result.scalars().first()

    if not team:
        return {"error": f"Team '{team_abbr}' not found", "injuries": []}

    # Get injuries
    stmt = (
        select(InjuryReport)
        .join(Player, InjuryReport.player_id == Player.id)
        .where(InjuryReport.team_id == team.id)
    )
    if active_only:
        stmt = stmt.where(InjuryReport.active == True)
    stmt = stmt.order_by(InjuryReport.reported_date.desc())

    result = await db.execute(stmt)
    injuries = result.scalars().all()

    # Get player names
    injury_list = []
    for inj in injuries:
        player_stmt = select(Player).where(Player.id == inj.player_id)
        p_result = await db.execute(player_stmt)
        player = p_result.scalars().first()

        injury_list.append({
            "player_name": player.name if player else "Unknown",
            "player_id": inj.player_id,
            "position": player.position if player else None,
            "status": inj.status,
            "injury_type": inj.injury_type,
            "body_part": inj.body_part,
            "reported_date": str(inj.reported_date),
            "expected_return": str(inj.expected_return_date) if inj.expected_return_date else None,
            "impact_ppg": inj.player_ppg,
            "impact_gpg": inj.player_gpg,
            "active": inj.active,
        })

    return {
        "team": team.name,
        "team_abbr": team.abbreviation,
        "total_injuries": len(injury_list),
        "injuries": injury_list,
    }


@router.post("/refresh")
async def refresh_injuries(
    db: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Manually trigger an injury report refresh from the NHL API."""
    from app.scrapers.injury_scraper import fetch_injury_reports

    count = await fetch_injury_reports(db)
    return {
        "status": "complete",
        "records_updated": count,
    }
