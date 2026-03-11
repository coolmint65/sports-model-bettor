"""
API endpoints for player and team matchup analytics.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.analytics.matchups import MatchupEngine
from app.database import get_session
from app.models.matchup import PlayerMatchupStats, TeamMatchupProfile
from app.models.player import Player
from app.models.team import Team

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/matchups", tags=["matchups"])

matchup_engine = MatchupEngine()


@router.get("/player/{player_id}/vs/{team_abbr}")
async def get_player_vs_team(
    player_id: int,
    team_abbr: str,
    db: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get a player's historical performance against a specific team.

    Shows games played, goals, assists, points, and how their production
    deviates from their overall average when facing this opponent.
    """
    # Look up team
    team_stmt = select(Team).where(Team.abbreviation == team_abbr.upper())
    team_result = await db.execute(team_stmt)
    team = team_result.scalars().first()

    if not team:
        return {"error": f"Team '{team_abbr}' not found"}

    # Look up player
    player_stmt = select(Player).where(Player.id == player_id)
    player_result = await db.execute(player_stmt)
    player = player_result.scalars().first()

    if not player:
        return {"error": f"Player {player_id} not found"}

    # Get or compute matchup stats
    stats_stmt = (
        select(PlayerMatchupStats)
        .where(
            and_(
                PlayerMatchupStats.player_id == player_id,
                PlayerMatchupStats.opponent_team_id == team.id,
            )
        )
        .order_by(PlayerMatchupStats.season.desc())
    )
    result = await db.execute(stats_stmt)
    records = result.scalars().all()

    if not records:
        # Compute on the fly
        record = await matchup_engine.compute_player_matchup(
            db, player_id, team.id
        )
        records = [record] if record else []

    seasons = []
    for r in records:
        seasons.append({
            "season": r.season,
            "games_played": r.games_played,
            "goals": r.goals,
            "assists": r.assists,
            "points": r.points,
            "shots": r.shots,
            "plus_minus": r.plus_minus,
            "ppg": r.ppg,
            "gpg": r.gpg,
            "overall_ppg": r.overall_ppg,
            "overall_gpg": r.overall_gpg,
            "ppg_deviation": r.ppg_deviation,
            "gpg_deviation": r.gpg_deviation,
            "avg_toi": r.avg_toi,
        })

    return {
        "player": player.name,
        "player_id": player_id,
        "opponent": team.name,
        "opponent_abbr": team.abbreviation,
        "seasons": seasons,
        "total_games": sum(s["games_played"] for s in seasons),
        "career_ppg_vs": (
            round(sum(s["ppg"] * s["games_played"] for s in seasons) /
                  max(sum(s["games_played"] for s in seasons), 1), 3)
            if seasons else None
        ),
    }


@router.get("/team/{team1_abbr}/vs/{team2_abbr}")
async def get_team_vs_team(
    team1_abbr: str,
    team2_abbr: str,
    db: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get enhanced head-to-head matchup profile between two teams.

    Shows scoring patterns, pace, period tendencies, OT frequency,
    and how these teams tend to play against each other.
    """
    # Look up teams
    t1_stmt = select(Team).where(Team.abbreviation == team1_abbr.upper())
    t1_result = await db.execute(t1_stmt)
    team1 = t1_result.scalars().first()

    t2_stmt = select(Team).where(Team.abbreviation == team2_abbr.upper())
    t2_result = await db.execute(t2_stmt)
    team2 = t2_result.scalars().first()

    if not team1 or not team2:
        return {"error": "One or both teams not found"}

    # Normalize order
    t1_id = min(team1.id, team2.id)
    t2_id = max(team1.id, team2.id)

    # Get or compute matchup profile
    profile_stmt = (
        select(TeamMatchupProfile)
        .where(
            and_(
                TeamMatchupProfile.team1_id == t1_id,
                TeamMatchupProfile.team2_id == t2_id,
            )
        )
        .order_by(TeamMatchupProfile.season.desc())
    )
    result = await db.execute(profile_stmt)
    profiles = result.scalars().all()

    if not profiles:
        # Compute on the fly
        profile = await matchup_engine.compute_team_matchup_profile(
            db, t1_id, t2_id
        )
        profiles = [profile] if profile else []

    # Also get player matchup highlights
    home_impact = await matchup_engine.get_team_player_matchup_impact(
        db, team1.id, team2.id
    )
    away_impact = await matchup_engine.get_team_player_matchup_impact(
        db, team2.id, team1.id
    )

    seasons = []
    for p in profiles:
        seasons.append({
            "season": p.season,
            "games_played": p.games_played,
            "avg_total_goals": p.avg_total_goals,
            "scoring_variance": p.scoring_variance,
            "avg_margin": p.avg_margin,
            "ot_rate": p.ot_rate,
            "team1_goals_pg": p.team1_goals_pg,
            "team2_goals_pg": p.team2_goals_pg,
            "pace_indicator": p.pace_indicator,
            "team1_p1_goals_avg": p.team1_p1_goals_avg,
            "team2_p1_goals_avg": p.team2_p1_goals_avg,
            "team1_p3_goals_avg": p.team1_p3_goals_avg,
            "team2_p3_goals_avg": p.team2_p3_goals_avg,
        })

    # Determine which team is team1 vs team2 in the profile
    t1_is_team1 = team1.id == t1_id

    return {
        "team1": team1.name if t1_is_team1 else team2.name,
        "team1_abbr": team1.abbreviation if t1_is_team1 else team2.abbreviation,
        "team2": team2.name if t1_is_team1 else team1.name,
        "team2_abbr": team2.abbreviation if t1_is_team1 else team1.abbreviation,
        "seasons": seasons,
        "player_matchup_highlights": {
            "team1_boost": home_impact.get("matchup_boost", 0),
            "team1_top_performers": home_impact.get("top_performers", []),
            "team2_boost": away_impact.get("matchup_boost", 0),
            "team2_top_performers": away_impact.get("top_performers", []),
        },
    }


@router.post("/refresh/{team1_abbr}/vs/{team2_abbr}")
async def refresh_matchup_data(
    team1_abbr: str,
    team2_abbr: str,
    db: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Manually refresh matchup data between two teams."""
    t1_stmt = select(Team).where(Team.abbreviation == team1_abbr.upper())
    t2_stmt = select(Team).where(Team.abbreviation == team2_abbr.upper())

    t1 = (await db.execute(t1_stmt)).scalars().first()
    t2 = (await db.execute(t2_stmt)).scalars().first()

    if not t1 or not t2:
        return {"error": "One or both teams not found"}

    await matchup_engine.refresh_matchup_data(db, t1.id, t2.id)
    return {"status": "complete", "team1": t1.name, "team2": t2.name}
