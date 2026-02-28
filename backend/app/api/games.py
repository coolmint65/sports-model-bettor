"""
Games API routes.

Provides endpoints for retrieving detailed game information, including
team form, head-to-head records, goalie stats, predictions, and computed
analytical features for a specific game.
"""

import json
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_session
from app.models.game import Game, GameGoalieStats, GamePlayerStats, HeadToHead
from app.models.player import GoalieStats, Player, PlayerStats
from app.models.prediction import Prediction
from app.models.team import Team, TeamStats

router = APIRouter(prefix="/api/games", tags=["games"])


# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------

class TeamForm(BaseModel):
    """Recent form / record information for a team."""

    team_id: int
    team_name: str
    abbreviation: str
    logo_url: Optional[str] = None
    wins: int = 0
    losses: int = 0
    ot_losses: int = 0
    points: int = 0
    games_played: int = 0
    points_pct: Optional[float] = None
    goal_diff: Optional[int] = None
    record_last_5: Optional[str] = None
    record_last_10: Optional[str] = None
    record_last_20: Optional[str] = None
    home_record: Optional[str] = None
    away_record: Optional[str] = None
    goals_for_per_game: Optional[float] = None
    goals_against_per_game: Optional[float] = None
    power_play_pct: Optional[float] = None
    penalty_kill_pct: Optional[float] = None
    shots_for_per_game: Optional[float] = None
    shots_against_per_game: Optional[float] = None
    faceoff_win_pct: Optional[float] = None

    model_config = {"from_attributes": True}


class HeadToHeadRecord(BaseModel):
    """Head-to-head summary between two teams."""

    team1_id: int
    team2_id: int
    season: str
    games_played: int = 0
    team1_wins: int = 0
    team2_wins: int = 0
    draws: int = 0
    team1_goals: int = 0
    team2_goals: int = 0
    last_meeting: Optional[date] = None

    model_config = {"from_attributes": True}


class GoalieInfo(BaseModel):
    """Goalie information and season stats."""

    player_id: int
    name: str
    team_id: Optional[int] = None

    games_played: int = 0
    games_started: int = 0
    wins: int = 0
    losses: int = 0
    ot_losses: int = 0
    save_pct: Optional[float] = None
    gaa: Optional[float] = None
    shutouts: int = 0

    model_config = {"from_attributes": True}


class PeriodScoring(BaseModel):
    """Period-by-period scoring averages for a team."""

    period_1_avg: Optional[float] = None
    period_2_avg: Optional[float] = None
    period_3_avg: Optional[float] = None


class GamePredictionBrief(BaseModel):
    """Compact prediction info embedded in game details."""

    id: int
    bet_type: Optional[str] = None
    prediction_value: Optional[str] = None
    confidence: Optional[float] = None
    edge: Optional[float] = None
    recommended: bool = False
    best_bet: bool = False
    reasoning: Optional[str] = None
    created_at: Optional[str] = None

    model_config = {"from_attributes": True}


class RecentGameResult(BaseModel):
    """A single recent game result for a team."""

    game_date: date
    opponent_abbrev: str
    opponent_name: str
    home_away: str  # "home" or "away"
    goals_for: int = 0
    goals_against: int = 0
    result: str  # "W", "L", "OTL"
    score_display: str = ""  # e.g. "4-2"
    overtime: bool = False


class OddsInfo(BaseModel):
    """Current betting odds for a game."""

    home_moneyline: Optional[float] = None
    away_moneyline: Optional[float] = None
    over_under_line: Optional[float] = None
    home_spread_line: Optional[float] = None
    away_spread_line: Optional[float] = None
    home_spread_price: Optional[float] = None
    away_spread_price: Optional[float] = None
    over_price: Optional[float] = None
    under_price: Optional[float] = None
    odds_updated_at: Optional[str] = None


class GameDetailResponse(BaseModel):
    """Full game details response with analytics context."""

    id: int
    external_id: str
    game_date: date
    start_time: Optional[str] = None
    venue: Optional[str] = None
    status: str
    game_type: str
    season: str

    home_score: Optional[int] = None
    away_score: Optional[int] = None
    total_goals: Optional[int] = None
    overtime: bool = False
    shootout: bool = False
    period_scores: Optional[Dict[str, Any]] = None

    odds: Optional[OddsInfo] = None

    home_team_form: TeamForm
    away_team_form: TeamForm

    home_recent_games: List[RecentGameResult] = []
    away_recent_games: List[RecentGameResult] = []

    head_to_head: Optional[HeadToHeadRecord] = None

    home_period_scoring: PeriodScoring
    away_period_scoring: PeriodScoring

    home_goalies: List[GoalieInfo] = []
    away_goalies: List[GoalieInfo] = []

    predictions: List[GamePredictionBrief] = []


class PredictionResponse(BaseModel):
    """Prediction list response for a game."""

    game_id: int
    predictions: List[GamePredictionBrief]


class FeatureResponse(BaseModel):
    """Computed analytical features for a game."""

    game_id: int
    features: Dict[str, Any]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_game_or_404(game_id: int, session: AsyncSession) -> Game:
    """Load a Game by its primary key or raise 404."""
    result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.id == game_id)
    )
    game = result.scalar_one_or_none()
    if game is None:
        raise HTTPException(status_code=404, detail=f"Game with id {game_id} not found.")
    return game


async def _get_team_form(team: Team, session: AsyncSession) -> TeamForm:
    """Build a TeamForm from the latest TeamStats row for the given team."""
    result = await session.execute(
        select(TeamStats)
        .where(TeamStats.team_id == team.id)
        .order_by(TeamStats.season.desc())
        .limit(1)
    )
    stats: Optional[TeamStats] = result.scalar_one_or_none()

    form = TeamForm(
        team_id=team.id,
        team_name=team.name,
        abbreviation=team.abbreviation,
        logo_url=team.logo_url,
    )
    if stats:
        form.wins = stats.wins
        form.losses = stats.losses
        form.ot_losses = stats.ot_losses
        form.points = stats.points
        form.games_played = stats.games_played
        gd = stats.goals_for - stats.goals_against if stats.goals_for and stats.goals_against else None
        form.goal_diff = gd
        total_possible = stats.games_played * 2 if stats.games_played else 0
        form.points_pct = round(stats.points / total_possible, 3) if total_possible > 0 else None
        form.record_last_5 = stats.record_last_5
        form.record_last_10 = stats.record_last_10
        form.record_last_20 = stats.record_last_20
        form.home_record = stats.home_record
        form.away_record = stats.away_record
        form.goals_for_per_game = stats.goals_for_per_game
        form.goals_against_per_game = stats.goals_against_per_game
        form.power_play_pct = stats.power_play_pct
        form.penalty_kill_pct = stats.penalty_kill_pct
        form.shots_for_per_game = stats.shots_for_per_game
        form.shots_against_per_game = stats.shots_against_per_game
        form.faceoff_win_pct = stats.faceoff_win_pct

    return form


async def _get_recent_games(
    team_id: int, session: AsyncSession, limit: int = 10
) -> List[RecentGameResult]:
    """Return the last N completed games for a team, most recent first."""
    FINISHED_STATUSES = ("final", "completed", "off")
    result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(
            or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
            func.lower(Game.status).in_(FINISHED_STATUSES),
            Game.home_score.isnot(None),
        )
        .order_by(Game.date.desc())
        .limit(limit)
    )
    games = result.scalars().all()

    results: List[RecentGameResult] = []
    for game in games:
        is_home = game.home_team_id == team_id
        gf = game.home_score if is_home else game.away_score
        ga = game.away_score if is_home else game.home_score
        opponent = game.away_team if is_home else game.home_team
        won = gf > ga
        ot = bool(game.went_to_overtime)
        if won:
            res = "W"
        elif ot:
            res = "OTL"
        else:
            res = "L"

        results.append(RecentGameResult(
            game_date=game.date,
            opponent_abbrev=opponent.abbreviation if opponent else "???",
            opponent_name=opponent.name if opponent else "Unknown",
            home_away="home" if is_home else "away",
            goals_for=gf or 0,
            goals_against=ga or 0,
            result=res,
            score_display=f"{gf}-{ga}",
            overtime=ot,
        ))
    return results


async def _get_head_to_head(
    team1_id: int, team2_id: int, session: AsyncSession
) -> Optional[HeadToHeadRecord]:
    """Compute H2H from the last 10 completed regular-season Game records.

    This is more accurate than the HeadToHead table because it includes
    ALL games in the database regardless of how they were synced.
    """
    lo, hi = sorted([team1_id, team2_id])
    FINISHED = ("final", "completed", "off")

    result = await session.execute(
        select(Game)
        .where(
            or_(
                and_(Game.home_team_id == lo, Game.away_team_id == hi),
                and_(Game.home_team_id == hi, Game.away_team_id == lo),
            ),
            func.lower(Game.status).in_(FINISHED),
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
            Game.game_type == "regular",
        )
        .order_by(Game.date.desc())
        .limit(10)
    )
    games = result.scalars().all()

    if not games:
        # Fall back to the HeadToHead table if no Game records found
        h2h_result = await session.execute(
            select(HeadToHead)
            .where(HeadToHead.team1_id == lo, HeadToHead.team2_id == hi)
            .order_by(HeadToHead.season.desc())
        )
        records = h2h_result.scalars().all()
        if not records:
            return None

        total_gp = sum(r.games_played for r in records)
        total_t1w = sum(r.team1_wins for r in records)
        total_t2w = sum(r.team2_wins for r in records)
        total_t1g = sum(r.team1_goals for r in records)
        total_t2g = sum(r.team2_goals for r in records)
        last_meeting = records[0].last_meeting_date

        return HeadToHeadRecord(
            team1_id=lo, team2_id=hi, season="All Time",
            games_played=total_gp, team1_wins=total_t1w,
            team2_wins=total_t2w, draws=0,
            team1_goals=total_t1g, team2_goals=total_t2g,
            last_meeting=last_meeting,
        )

    # Compute directly from Game records
    total_gp = len(games)
    t1_wins = 0
    t2_wins = 0
    t1_goals = 0
    t2_goals = 0

    for game in games:
        if game.home_team_id == lo:
            g1, g2 = game.home_score, game.away_score
        else:
            g1, g2 = game.away_score, game.home_score
        t1_goals += g1
        t2_goals += g2
        if g1 > g2:
            t1_wins += 1
        elif g2 > g1:
            t2_wins += 1

    return HeadToHeadRecord(
        team1_id=lo,
        team2_id=hi,
        season="Last 10",
        games_played=total_gp,
        team1_wins=t1_wins,
        team2_wins=t2_wins,
        draws=0,
        team1_goals=t1_goals,
        team2_goals=t2_goals,
        last_meeting=games[0].date,
    )


async def _get_team_goalies(
    team_id: int, session: AsyncSession
) -> List[GoalieInfo]:
    """Get goalies for a team with their latest season stats."""
    result = await session.execute(
        select(Player).where(
            Player.team_id == team_id,
            Player.position == "G",
            Player.active.is_(True),
        )
    )
    goalies = result.scalars().all()

    goalie_infos: List[GoalieInfo] = []
    for goalie in goalies:
        stats_result = await session.execute(
            select(GoalieStats)
            .where(GoalieStats.player_id == goalie.id)
            .order_by(GoalieStats.season.desc())
            .limit(1)
        )
        gs: Optional[GoalieStats] = stats_result.scalar_one_or_none()

        info = GoalieInfo(
            player_id=goalie.id,
            name=goalie.name,
            team_id=goalie.team_id,
        )
        if gs:
            info.games_played = gs.games_played
            info.games_started = gs.games_started
            info.wins = gs.wins
            info.losses = gs.losses
            info.ot_losses = gs.ot_losses
            info.save_pct = gs.save_pct
            info.gaa = gs.gaa
            info.shutouts = gs.shutouts

        goalie_infos.append(info)

    return goalie_infos


async def _compute_period_scoring(
    team_id: int, session: AsyncSession
) -> PeriodScoring:
    """
    Compute average period-by-period scoring for a team from completed games.

    Tries three approaches in order:
      1. Games with detailed per-period scores (from boxscore sync).
      2. Games with total scores (from historical sync) — estimates period
         breakdown using the typical NHL 32/33/35% distribution.
      3. TeamStats ``goals_for_per_game`` as a final fallback.
    """
    FINISHED_STATUSES = ("final", "completed", "off")

    # ---- Approach 1: per-period scores from boxscore data ----
    result = await session.execute(
        select(Game).where(
            or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
            func.lower(Game.status).in_(FINISHED_STATUSES),
            Game.home_score_p1.isnot(None),
        )
        .order_by(Game.date.desc())
        .limit(20)
    )
    games = result.scalars().all()

    if games:
        period_totals = [0.0, 0.0, 0.0]
        counted = 0
        for game in games:
            is_home = game.home_team_id == team_id
            p1 = (game.home_score_p1 if is_home else game.away_score_p1) or 0
            p2 = (game.home_score_p2 if is_home else game.away_score_p2) or 0
            p3 = (game.home_score_p3 if is_home else game.away_score_p3) or 0
            period_totals[0] += p1
            period_totals[1] += p2
            period_totals[2] += p3
            counted += 1

        if counted > 0:
            return PeriodScoring(
                period_1_avg=round(period_totals[0] / counted, 2),
                period_2_avg=round(period_totals[1] / counted, 2),
                period_3_avg=round(period_totals[2] / counted, 2),
            )

    # ---- Approach 2: estimate from total game scores ----
    result = await session.execute(
        select(Game).where(
            or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
            func.lower(Game.status).in_(FINISHED_STATUSES),
            Game.home_score.isnot(None),
        )
        .order_by(Game.date.desc())
        .limit(20)
    )
    games_total = result.scalars().all()

    if games_total:
        total_goals = 0.0
        counted = 0
        for game in games_total:
            is_home = game.home_team_id == team_id
            goals = (game.home_score if is_home else game.away_score) or 0
            total_goals += goals
            counted += 1

        if counted > 0:
            avg_per_game = total_goals / counted
            # NHL scoring is roughly evenly distributed across periods
            # with a slight uptick in the 3rd period
            return PeriodScoring(
                period_1_avg=round(avg_per_game * 0.32, 2),
                period_2_avg=round(avg_per_game * 0.33, 2),
                period_3_avg=round(avg_per_game * 0.35, 2),
            )

    # ---- Approach 3: fall back to TeamStats goals_for_per_game ----
    ts_result = await session.execute(
        select(TeamStats)
        .where(TeamStats.team_id == team_id)
        .order_by(TeamStats.season.desc())
        .limit(1)
    )
    ts = ts_result.scalar_one_or_none()
    if ts and ts.goals_for_per_game:
        gf = ts.goals_for_per_game
        return PeriodScoring(
            period_1_avg=round(gf * 0.32, 2),
            period_2_avg=round(gf * 0.33, 2),
            period_3_avg=round(gf * 0.35, 2),
        )

    return PeriodScoring()


async def _get_game_predictions(
    game_id: int, session: AsyncSession
) -> List[GamePredictionBrief]:
    """Return all predictions associated with a game."""
    result = await session.execute(
        select(Prediction).where(Prediction.game_id == game_id)
    )
    preds = result.scalars().all()

    briefs: List[GamePredictionBrief] = []
    for p in preds:
        briefs.append(
            GamePredictionBrief(
                id=p.id,
                bet_type=p.bet_type,
                prediction_value=p.prediction_value,
                confidence=p.confidence,
                edge=p.edge,
                recommended=p.recommended,
                best_bet=p.best_bet,
                reasoning=p.reasoning,
                created_at=str(p.created_at) if p.created_at else None,
            )
        )
    return briefs


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

@router.get(
    "/{game_id}",
    response_model=GameDetailResponse,
    summary="Get full game details",
)
async def get_game_details(
    game_id: int,
    session: AsyncSession = Depends(get_session),
):
    """
    Return comprehensive details for a single game.

    Includes both teams' recent form (last-5, last-10 records), head-to-head
    record, period-by-period scoring averages, goalie season stats, and any
    model predictions that have been generated.
    """
    game = await _get_game_or_404(game_id, session)

    # Gather all analytics data
    home_form = await _get_team_form(game.home_team, session)
    away_form = await _get_team_form(game.away_team, session)
    h2h = await _get_head_to_head(game.home_team_id, game.away_team_id, session)
    home_period = await _compute_period_scoring(game.home_team_id, session)
    away_period = await _compute_period_scoring(game.away_team_id, session)
    home_goalies = await _get_team_goalies(game.home_team_id, session)
    away_goalies = await _get_team_goalies(game.away_team_id, session)
    predictions = await _get_game_predictions(game.id, session)
    home_recent = await _get_recent_games(game.home_team_id, session, limit=20)
    away_recent = await _get_recent_games(game.away_team_id, session, limit=20)

    # Build period scores from individual columns
    parsed_period_scores = None
    if game.home_score_p1 is not None:
        parsed_period_scores = {
            "home": [game.home_score_p1 or 0, game.home_score_p2 or 0, game.home_score_p3 or 0],
            "away": [game.away_score_p1 or 0, game.away_score_p2 or 0, game.away_score_p3 or 0],
        }
        if game.home_score_ot is not None:
            parsed_period_scores["home"].append(game.home_score_ot)
            parsed_period_scores["away"].append(game.away_score_ot or 0)

    total_goals = None
    if game.home_score is not None and game.away_score is not None:
        total_goals = game.home_score + game.away_score

    # Build odds info from Game model fields (populated by OddsScraper)
    odds_info = None
    if any([game.home_moneyline, game.away_moneyline, game.over_under_line, game.home_spread_line]):
        odds_updated = None
        if hasattr(game, "odds_updated_at") and game.odds_updated_at:
            odds_updated = str(game.odds_updated_at)
        odds_info = OddsInfo(
            home_moneyline=game.home_moneyline,
            away_moneyline=game.away_moneyline,
            over_under_line=game.over_under_line,
            home_spread_line=game.home_spread_line,
            away_spread_line=getattr(game, "away_spread_line", None),
            home_spread_price=getattr(game, "home_spread_price", None),
            away_spread_price=getattr(game, "away_spread_price", None),
            over_price=getattr(game, "over_price", None),
            under_price=getattr(game, "under_price", None),
            odds_updated_at=odds_updated,
        )

    return GameDetailResponse(
        id=game.id,
        external_id=game.external_id,
        game_date=game.date,
        start_time=game.start_time.isoformat() if game.start_time else None,
        venue=game.venue,
        status=game.status,
        game_type=game.game_type,
        season=game.season,
        home_score=game.home_score,
        away_score=game.away_score,
        total_goals=total_goals,
        overtime=game.went_to_overtime or False,
        shootout=False,
        period_scores=parsed_period_scores,
        odds=odds_info,
        home_team_form=home_form,
        away_team_form=away_form,
        home_recent_games=home_recent,
        away_recent_games=away_recent,
        head_to_head=h2h,
        home_period_scoring=home_period,
        away_period_scoring=away_period,
        home_goalies=home_goalies,
        away_goalies=away_goalies,
        predictions=predictions,
    )


@router.get(
    "/{game_id}/predictions",
    response_model=PredictionResponse,
    summary="Get predictions for a game",
)
async def get_game_predictions(
    game_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Return all model predictions generated for the specified game."""
    # Verify the game exists
    await _get_game_or_404(game_id, session)
    predictions = await _get_game_predictions(game_id, session)
    return PredictionResponse(game_id=game_id, predictions=predictions)


@router.get(
    "/{game_id}/features",
    response_model=FeatureResponse,
    summary="Get computed features for a game",
)
async def get_game_features(
    game_id: int,
    session: AsyncSession = Depends(get_session),
):
    """
    Compute and return the analytical feature vector for a game.

    Uses the FeatureEngine to build features suitable for model input.
    Falls back to a basic feature set if the analytics module is unavailable.
    """
    game = await _get_game_or_404(game_id, session)

    # Try the full feature engine first
    try:
        from app.analytics.features import FeatureEngine

        engine = FeatureEngine()
        features = await engine.build_game_features(session, game.id)
        return FeatureResponse(game_id=game_id, features=features)
    except (ImportError, AttributeError):
        pass
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error computing features: {exc}",
        )

    # Fallback: construct a basic feature dict from available data
    home_form = await _get_team_form(game.home_team, session)
    away_form = await _get_team_form(game.away_team, session)
    h2h = await _get_head_to_head(game.home_team_id, game.away_team_id, session)

    features: Dict[str, Any] = {
        "game_id": game.id,
        "game_date": str(game.date),
        "home_team_id": game.home_team_id,
        "away_team_id": game.away_team_id,
        "home_wins": home_form.wins,
        "home_losses": home_form.losses,
        "home_ot_losses": home_form.ot_losses,
        "away_wins": away_form.wins,
        "away_losses": away_form.losses,
        "away_ot_losses": away_form.ot_losses,
        "home_gf_per_game": home_form.goals_for_per_game,
        "home_ga_per_game": home_form.goals_against_per_game,
        "away_gf_per_game": away_form.goals_for_per_game,
        "away_ga_per_game": away_form.goals_against_per_game,
        "home_pp_pct": home_form.power_play_pct,
        "home_pk_pct": home_form.penalty_kill_pct,
        "away_pp_pct": away_form.power_play_pct,
        "away_pk_pct": away_form.penalty_kill_pct,
        "home_shots_for_pg": home_form.shots_for_per_game,
        "home_shots_against_pg": home_form.shots_against_per_game,
        "away_shots_for_pg": away_form.shots_for_per_game,
        "away_shots_against_pg": away_form.shots_against_per_game,
        "home_record_last_5": home_form.record_last_5,
        "away_record_last_5": away_form.record_last_5,
        "home_record_last_10": home_form.record_last_10,
        "away_record_last_10": away_form.record_last_10,
    }

    if h2h:
        features["h2h_games_played"] = h2h.games_played
        features["h2h_team1_wins"] = h2h.team1_wins
        features["h2h_team2_wins"] = h2h.team2_wins
        features["h2h_team1_goals"] = h2h.team1_goals
        features["h2h_team2_goals"] = h2h.team2_goals

    return FeatureResponse(game_id=game_id, features=features)
