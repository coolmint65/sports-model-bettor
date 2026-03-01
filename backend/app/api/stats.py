"""
Stats API routes.

Provides endpoints for retrieving team and player statistics, including
season-level aggregates, goalie metrics, and roster-level data.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_session
from app.models.player import GoalieStats, Player, PlayerStats
from app.models.team import Team, TeamStats

router = APIRouter(prefix="/api/stats", tags=["stats"])


# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------

class TeamStatsDetail(BaseModel):
    """Season-level statistics for a team."""

    id: int
    team_id: int
    season: str
    games_played: int = 0
    wins: int = 0
    losses: int = 0
    ot_losses: int = 0
    points: int = 0
    goals_for: int = 0
    goals_against: int = 0
    goals_for_per_game: Optional[float] = None
    goals_against_per_game: Optional[float] = None
    power_play_pct: Optional[float] = None
    penalty_kill_pct: Optional[float] = None
    shots_for_per_game: Optional[float] = None
    shots_against_per_game: Optional[float] = None
    faceoff_win_pct: Optional[float] = None
    record_last_5: Optional[str] = None
    record_last_10: Optional[str] = None
    record_last_20: Optional[str] = None
    home_record: Optional[str] = None
    away_record: Optional[str] = None

    model_config = {"from_attributes": True}


class TeamSummary(BaseModel):
    """Team with its current season stats."""

    id: int
    external_id: str
    name: str
    abbreviation: str
    city: Optional[str] = None
    division: Optional[str] = None
    conference: Optional[str] = None
    logo_url: Optional[str] = None
    active: bool = True
    current_stats: Optional[TeamStatsDetail] = None

    model_config = {"from_attributes": True}


class TeamsListResponse(BaseModel):
    """List of all teams with stats."""

    team_count: int
    teams: List[TeamSummary]


class PlayerStatsSeason(BaseModel):
    """Season-level skater stats."""

    id: int
    player_id: int
    season: str
    games_played: int = 0
    goals: int = 0
    assists: int = 0
    points: int = 0
    plus_minus: int = 0
    pim: int = 0
    ppg: int = 0
    ppa: int = 0
    shg: int = 0
    sha: int = 0
    gwg: int = 0
    shots: int = 0
    shooting_pct: Optional[float] = None
    toi_per_game: Optional[float] = None
    faceoff_pct: Optional[float] = None

    model_config = {"from_attributes": True}


class GoalieStatsSeason(BaseModel):
    """Season-level goalie stats."""

    id: int
    player_id: int
    season: str
    games_played: int = 0
    games_started: int = 0
    wins: int = 0
    losses: int = 0
    ot_losses: int = 0
    save_pct: Optional[float] = None
    gaa: Optional[float] = None
    shutouts: int = 0
    saves: int = 0
    shots_against: int = 0
    toi: Optional[float] = None
    quality_starts: int = 0

    model_config = {"from_attributes": True}


class PlayerDetail(BaseModel):
    """Detailed player information with all season stats."""

    id: int
    external_id: str
    name: str
    team_id: Optional[int] = None
    team_name: Optional[str] = None
    team_abbreviation: Optional[str] = None
    position: Optional[str] = None
    jersey_number: Optional[int] = None
    shoots_catches: Optional[str] = None
    height: Optional[int] = None
    weight: Optional[int] = None
    birth_date: Optional[str] = None
    active: bool = True
    season_stats: List[PlayerStatsSeason] = []
    goalie_stats: List[GoalieStatsSeason] = []

    model_config = {"from_attributes": True}


class RosterPlayer(BaseModel):
    """Compact player entry for team roster listings."""

    id: int
    external_id: str
    name: str
    position: Optional[str] = None
    jersey_number: Optional[int] = None
    active: bool = True

    # Current season highlights (if skater)
    goals: Optional[int] = None
    assists: Optional[int] = None
    points: Optional[int] = None

    # Current season highlights (if goalie)
    wins: Optional[int] = None
    save_pct: Optional[float] = None
    gaa: Optional[float] = None

    model_config = {"from_attributes": True}


class TeamDetailResponse(BaseModel):
    """Full team detail including all season stats and roster."""

    team: TeamSummary
    all_stats: List[TeamStatsDetail] = []
    roster: List[RosterPlayer] = []


class SyncResult(BaseModel):
    """Outcome of a data sync."""

    success: bool
    message: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _build_team_summary(team: Team, session: AsyncSession) -> TeamSummary:
    """Build a TeamSummary with current season stats attached."""
    result = await session.execute(
        select(TeamStats)
        .where(TeamStats.team_id == team.id)
        .order_by(TeamStats.season.desc())
        .limit(1)
    )
    stats: Optional[TeamStats] = result.scalar_one_or_none()

    current_stats = None
    if stats:
        current_stats = TeamStatsDetail(
            id=stats.id,
            team_id=stats.team_id,
            season=stats.season,
            games_played=stats.games_played,
            wins=stats.wins,
            losses=stats.losses,
            ot_losses=stats.ot_losses,
            points=stats.points,
            goals_for=stats.goals_for,
            goals_against=stats.goals_against,
            goals_for_per_game=stats.goals_for_per_game,
            goals_against_per_game=stats.goals_against_per_game,
            power_play_pct=stats.power_play_pct,
            penalty_kill_pct=stats.penalty_kill_pct,
            shots_for_per_game=stats.shots_for_per_game,
            shots_against_per_game=stats.shots_against_per_game,
            faceoff_win_pct=stats.faceoff_win_pct,
            record_last_5=stats.record_last_5,
            record_last_10=stats.record_last_10,
            record_last_20=stats.record_last_20,
            home_record=stats.home_record,
            away_record=stats.away_record,
        )

    return TeamSummary(
        id=team.id,
        external_id=team.external_id,
        name=team.name,
        abbreviation=team.abbreviation,
        city=team.city,
        division=team.division,
        conference=team.conference,
        logo_url=team.logo_url,
        active=team.active,
        current_stats=current_stats,
    )


async def _build_roster_player(player: Player, session: AsyncSession) -> RosterPlayer:
    """Build a RosterPlayer with current season stat highlights."""
    rp = RosterPlayer(
        id=player.id,
        external_id=player.external_id,
        name=player.name,
        position=player.position,
        jersey_number=player.jersey_number,
        active=player.active,
    )

    if player.position == "G":
        gs_result = await session.execute(
            select(GoalieStats)
            .where(GoalieStats.player_id == player.id)
            .order_by(GoalieStats.season.desc())
            .limit(1)
        )
        gs = gs_result.scalar_one_or_none()
        if gs:
            rp.wins = gs.wins
            rp.save_pct = gs.save_pct
            rp.gaa = gs.gaa
    else:
        ps_result = await session.execute(
            select(PlayerStats)
            .where(PlayerStats.player_id == player.id)
            .order_by(PlayerStats.season.desc())
            .limit(1)
        )
        ps = ps_result.scalar_one_or_none()
        if ps:
            rp.goals = ps.goals
            rp.assists = ps.assists
            rp.points = ps.points

    return rp


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

@router.get(
    "/teams",
    response_model=TeamsListResponse,
    summary="Get all teams with current stats",
)
async def get_all_teams(
    session: AsyncSession = Depends(get_session),
):
    """Return all active NHL teams along with their current season statistics."""
    result = await session.execute(
        select(Team)
        .where(Team.active.is_(True))
        .order_by(Team.name.asc())
    )
    teams = result.scalars().all()

    summaries: List[TeamSummary] = []
    for team in teams:
        summary = await _build_team_summary(team, session)
        summaries.append(summary)

    return TeamsListResponse(team_count=len(summaries), teams=summaries)


@router.get(
    "/teams/{team_id}",
    response_model=TeamDetailResponse,
    summary="Get detailed team stats",
)
async def get_team_detail(
    team_id: int,
    session: AsyncSession = Depends(get_session),
):
    """
    Return full team details including all season stats and the current roster
    with per-player stat highlights.
    """
    result = await session.execute(select(Team).where(Team.id == team_id))
    team = result.scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail=f"Team with id {team_id} not found.")

    summary = await _build_team_summary(team, session)

    # All season stats
    all_stats_result = await session.execute(
        select(TeamStats)
        .where(TeamStats.team_id == team_id)
        .order_by(TeamStats.season.desc())
    )
    all_stats_rows = all_stats_result.scalars().all()
    all_stats = [
        TeamStatsDetail(
            id=s.id,
            team_id=s.team_id,
            season=s.season,
            games_played=s.games_played,
            wins=s.wins,
            losses=s.losses,
            ot_losses=s.ot_losses,
            points=s.points,
            goals_for=s.goals_for,
            goals_against=s.goals_against,
            goals_for_per_game=s.goals_for_per_game,
            goals_against_per_game=s.goals_against_per_game,
            power_play_pct=s.power_play_pct,
            penalty_kill_pct=s.penalty_kill_pct,
            shots_for_per_game=s.shots_for_per_game,
            shots_against_per_game=s.shots_against_per_game,
            faceoff_win_pct=s.faceoff_win_pct,
            record_last_5=s.record_last_5,
            record_last_10=s.record_last_10,
            record_last_20=s.record_last_20,
            home_record=s.home_record,
            away_record=s.away_record,
        )
        for s in all_stats_rows
    ]

    # Roster
    roster_result = await session.execute(
        select(Player)
        .where(Player.team_id == team_id, Player.active.is_(True))
        .order_by(Player.position.asc(), Player.name.asc())
    )
    players = roster_result.scalars().all()

    roster: List[RosterPlayer] = []
    for player in players:
        rp = await _build_roster_player(player, session)
        roster.append(rp)

    return TeamDetailResponse(team=summary, all_stats=all_stats, roster=roster)


@router.get(
    "/players/{player_id}",
    response_model=PlayerDetail,
    summary="Get player stats",
)
async def get_player_detail(
    player_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Return detailed player information including all season stats."""
    result = await session.execute(
        select(Player)
        .options(selectinload(Player.team))
        .where(Player.id == player_id)
    )
    player = result.scalar_one_or_none()
    if player is None:
        raise HTTPException(
            status_code=404, detail=f"Player with id {player_id} not found."
        )

    # Team info
    team_name = None
    team_abbreviation = None
    if player.team:
        team_name = player.team.name
        team_abbreviation = player.team.abbreviation

    # Skater season stats
    ps_result = await session.execute(
        select(PlayerStats)
        .where(PlayerStats.player_id == player.id)
        .order_by(PlayerStats.season.desc())
    )
    player_stats = ps_result.scalars().all()
    season_stats = [
        PlayerStatsSeason(
            id=ps.id,
            player_id=ps.player_id,
            season=ps.season,
            games_played=ps.games_played,
            goals=ps.goals,
            assists=ps.assists,
            points=ps.points,
            plus_minus=ps.plus_minus,
            pim=ps.pim,
            ppg=ps.ppg,
            ppa=ps.ppa,
            shg=ps.shg,
            sha=ps.sha,
            gwg=ps.gwg,
            shots=ps.shots,
            shooting_pct=ps.shooting_pct,
            toi_per_game=ps.toi_per_game,
            faceoff_pct=ps.faceoff_pct,
        )
        for ps in player_stats
    ]

    # Goalie season stats
    gs_result = await session.execute(
        select(GoalieStats)
        .where(GoalieStats.player_id == player.id)
        .order_by(GoalieStats.season.desc())
    )
    goalie_stats_rows = gs_result.scalars().all()
    goalie_stats = [
        GoalieStatsSeason(
            id=gs.id,
            player_id=gs.player_id,
            season=gs.season,
            games_played=gs.games_played,
            games_started=gs.games_started,
            wins=gs.wins,
            losses=gs.losses,
            ot_losses=gs.ot_losses,
            save_pct=gs.save_pct,
            gaa=gs.gaa,
            shutouts=gs.shutouts,
            saves=gs.saves,
            shots_against=gs.shots_against,
            toi=gs.toi,
            quality_starts=gs.quality_starts,
        )
        for gs in goalie_stats_rows
    ]

    return PlayerDetail(
        id=player.id,
        external_id=player.external_id,
        name=player.name,
        team_id=player.team_id,
        team_name=team_name,
        team_abbreviation=team_abbreviation,
        position=player.position,
        jersey_number=player.jersey_number,
        shoots_catches=player.shoots_catches,
        height=player.height,
        weight=player.weight,
        birth_date=str(player.birth_date) if player.birth_date else None,
        active=player.active,
        season_stats=season_stats,
        goalie_stats=goalie_stats,
    )


@router.post(
    "/sync",
    response_model=SyncResult,
    summary="Trigger data sync",
)
async def sync_stats(
    session: AsyncSession = Depends(get_session),
):
    """
    Trigger a data sync to update team rosters and stats from the NHL API.

    Delegates to the NHLScraper.sync_rosters() and any available stats
    refresh methods.
    """
    try:
        from app.scrapers.nhl_api import NHLScraper

        scraper = NHLScraper(session)
        await scraper.sync_rosters()
        return SyncResult(
            success=True,
            message="Successfully synced rosters and stats from the NHL API.",
        )
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="NHL scraper module is not available.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to sync stats: {exc}",
        )
