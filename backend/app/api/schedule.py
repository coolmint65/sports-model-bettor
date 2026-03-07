"""
Schedule API routes.

Provides endpoints for retrieving NHL game schedules by date.
Odds syncing is handled by the background scheduler (app.live) —
GET endpoints are read-only.
"""

import logging
from datetime import date, datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.constants import GAME_FINAL_STATUSES, MARKET_BET_TYPES, PROP_BET_TYPES, composite_pick_score, is_heavy_juice
from app.database import get_session
from app.models.game import Game
from app.models.prediction import Prediction
from app.models.team import Team, TeamStats
from app.services.odds import fresh_implied_prob
from app.utils import serialize_utc_datetime

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/schedule", tags=["schedule"])


class TeamBrief(BaseModel):
    id: int
    external_id: str
    name: str
    abbreviation: str
    logo_url: Optional[str] = None
    wins: Optional[int] = None
    losses: Optional[int] = None
    ot_losses: Optional[int] = None
    points: Optional[int] = None
    record: Optional[str] = None

    model_config = {"from_attributes": True}


class GameTopPick(BaseModel):
    bet_type: Optional[str] = None
    prediction_value: Optional[str] = None
    confidence: Optional[float] = None
    edge: Optional[float] = None
    is_fallback: bool = False
    outcome: Optional[str] = None  # "win", "loss", or None (pending/in-progress)


class GameOdds(BaseModel):
    """Snapshot of sportsbook odds for a game."""
    home_moneyline: Optional[float] = None
    away_moneyline: Optional[float] = None
    over_under_line: Optional[float] = None
    over_price: Optional[float] = None
    under_price: Optional[float] = None
    home_spread_line: Optional[float] = None
    away_spread_line: Optional[float] = None
    home_spread_price: Optional[float] = None
    away_spread_price: Optional[float] = None
    odds_updated_at: Optional[str] = None


class ScheduleGame(BaseModel):
    id: int
    external_id: str
    game_date: date
    start_time: Optional[datetime] = None
    venue: Optional[str] = None
    status: str
    game_type: Optional[str] = None
    season: str
    home_team: TeamBrief
    away_team: TeamBrief
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    went_to_overtime: Optional[bool] = False
    # Live game info
    period: Optional[int] = None
    period_type: Optional[str] = None
    clock: Optional[str] = None
    clock_running: Optional[bool] = None
    in_intermission: Optional[bool] = None
    home_shots: Optional[int] = None
    away_shots: Optional[int] = None
    # Top prediction for this game
    top_pick: Optional[GameTopPick] = None
    # Top prop prediction for this game (non-market bet)
    top_prop: Optional[GameTopPick] = None
    # Sportsbook odds
    odds: Optional[GameOdds] = None
    pregame_odds: Optional[GameOdds] = None

    model_config = {"from_attributes": True}


class ScheduleResponse(BaseModel):
    date: date
    game_count: int
    games: List[ScheduleGame]


class SyncResult(BaseModel):
    success: bool
    message: str
    games_synced: int = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _batch_load_team_stats(
    team_ids: List[int], session: AsyncSession
) -> dict[int, Optional[TeamStats]]:
    """Batch-load the latest TeamStats for multiple teams in one query."""
    if not team_ids:
        return {}
    latest_season = (
        select(TeamStats.team_id, func.max(TeamStats.season).label("max_season"))
        .where(TeamStats.team_id.in_(team_ids))
        .group_by(TeamStats.team_id)
        .subquery()
    )
    result = await session.execute(
        select(TeamStats).join(
            latest_season,
            and_(
                TeamStats.team_id == latest_season.c.team_id,
                TeamStats.season == latest_season.c.max_season,
            ),
        )
    )
    return {ts.team_id: ts for ts in result.scalars().all()}


def _build_team_brief(team: Team, stats: Optional[TeamStats] = None) -> TeamBrief:
    brief = TeamBrief(
        id=team.id,
        external_id=team.external_id,
        name=team.name,
        abbreviation=team.abbreviation,
        logo_url=team.logo_url,
    )
    if stats:
        brief.wins = stats.wins
        brief.losses = stats.losses
        brief.ot_losses = stats.ot_losses
        brief.points = stats.points
        brief.record = f"{stats.wins}-{stats.losses}-{stats.ot_losses}"
    return brief


def _build_game_odds(game: Game) -> Optional[GameOdds]:
    if game.home_moneyline is None and game.away_moneyline is None:
        return None
    return GameOdds(
        home_moneyline=game.home_moneyline,
        away_moneyline=game.away_moneyline,
        over_under_line=game.over_under_line,
        over_price=game.over_price,
        under_price=game.under_price,
        home_spread_line=game.home_spread_line,
        away_spread_line=game.away_spread_line,
        home_spread_price=game.home_spread_price,
        away_spread_price=game.away_spread_price,
        odds_updated_at=serialize_utc_datetime(game.odds_updated_at),
    )


def _build_pregame_odds(game: Game) -> Optional[GameOdds]:
    if game.pregame_home_moneyline is None and game.pregame_away_moneyline is None:
        return None
    return GameOdds(
        home_moneyline=game.pregame_home_moneyline,
        away_moneyline=game.pregame_away_moneyline,
        over_under_line=game.pregame_over_under_line,
        over_price=game.pregame_over_price,
        under_price=game.pregame_under_price,
        home_spread_line=game.pregame_home_spread_line,
        away_spread_line=game.pregame_away_spread_line,
        home_spread_price=game.pregame_home_spread_price,
        away_spread_price=game.pregame_away_spread_price,
    )


def _build_schedule_game(
    game: Game,
    home_brief: TeamBrief,
    away_brief: TeamBrief,
    top_pick: Optional[GameTopPick] = None,
    top_prop: Optional[GameTopPick] = None,
) -> ScheduleGame:
    return ScheduleGame(
        id=game.id,
        external_id=game.external_id,
        game_date=game.date,
        start_time=game.start_time,
        venue=game.venue,
        status=game.status,
        game_type=game.game_type,
        season=game.season,
        home_team=home_brief,
        away_team=away_brief,
        home_score=game.home_score,
        away_score=game.away_score,
        went_to_overtime=game.went_to_overtime or False,
        period=game.period,
        period_type=game.period_type,
        clock=game.clock,
        clock_running=game.clock_running,
        in_intermission=game.in_intermission,
        home_shots=game.home_shots,
        away_shots=game.away_shots,
        top_pick=top_pick,
        top_prop=top_prop,
        odds=_build_game_odds(game),
        pregame_odds=_build_pregame_odds(game),
    )


def _grade_top_pick(pick: GameTopPick, game: Game) -> Optional[str]:
    """Grade a top pick against final scores. Returns 'win', 'loss', or None."""
    if game.home_score is None or game.away_score is None:
        return None
    hs = game.home_score
    aws = game.away_score
    val = pick.prediction_value

    if pick.bet_type == "ml":
        home_abbr = game.home_team.abbreviation if game.home_team else ""
        if val == home_abbr:
            return "win" if hs > aws else "loss"
        else:
            return "win" if aws > hs else "loss"

    elif pick.bet_type == "total":
        total = hs + aws
        if "over" in val:
            try:
                line = float(val.split("_")[1])
            except (IndexError, ValueError):
                return None
            if total == line:
                return "push"
            return "win" if total > line else "loss"
        elif "under" in val:
            try:
                line = float(val.split("_")[1])
            except (IndexError, ValueError):
                return None
            if total == line:
                return "push"
            return "win" if total < line else "loss"

    elif pick.bet_type == "spread":
        try:
            parts = val.split("_")
            team_abbr = parts[0]
            spread_val = float(parts[1])
        except (IndexError, ValueError):
            return None
        margin = hs - aws
        home_abbr = game.home_team.abbreviation if game.home_team else ""
        if team_abbr == home_abbr:
            adjusted = margin + spread_val
        else:
            adjusted = -margin + spread_val
        if adjusted == 0:
            return "push"
        return "win" if adjusted > 0 else "loss"

    elif pick.bet_type == "both_score":
        both = hs > 0 and aws > 0
        return "win" if (val == "yes") == both else "loss"

    elif pick.bet_type == "first_goal":
        if hs > 0 or aws > 0:
            actual = "home" if hs > 0 else "away"
            return "win" if val == actual else "loss"

    elif pick.bet_type == "overtime":
        if game.went_to_overtime is None:
            return None  # OT status not yet synced
        return "win" if (val == "yes") == game.went_to_overtime else "loss"

    elif pick.bet_type == "odd_even":
        total = hs + aws
        actual = "odd" if total % 2 == 1 else "even"
        return "win" if val == actual else "loss"

    elif pick.bet_type == "regulation_winner":
        if game.went_to_overtime is None:
            return None  # OT data not yet synced
        if game.went_to_overtime:
            # Game went to OT — regulation ended in a draw
            actual = "draw"
        else:
            actual = "home" if hs > aws else "away"
        return "win" if val == actual else "loss"

    elif pick.bet_type == "team_total":
        try:
            line = float(val.split("_")[-1])
            team_goals = hs if val.startswith("home") else aws
            if "over" in val:
                if team_goals == line:
                    return "push"
                return "win" if team_goals > line else "loss"
            else:
                if team_goals == line:
                    return "push"
                return "win" if team_goals < line else "loss"
        except (ValueError, IndexError):
            return None

    elif pick.bet_type == "highest_scoring_period":
        if game.home_score_p1 is None or game.away_score_p1 is None:
            return None  # period scores not yet synced
        hp1 = game.home_score_p1 or 0
        ap1 = game.away_score_p1 or 0
        hp2 = game.home_score_p2 or 0
        ap2 = game.away_score_p2 or 0
        hp3 = game.home_score_p3 or 0
        ap3 = game.away_score_p3 or 0
        periods = [hp1 + ap1, hp2 + ap2, hp3 + ap3]
        max_p = max(periods)
        if periods.count(max_p) > 1:
            actual = "tie"
        else:
            actual = ["p1", "p2", "p3"][periods.index(max_p)]
        return "win" if val == actual else "loss"

    elif pick.bet_type == "period1_btts":
        hp1 = game.home_score_p1
        ap1 = game.away_score_p1
        if hp1 is not None and ap1 is not None:
            both = hp1 > 0 and ap1 > 0
            return "win" if (val == "yes") == both else "loss"

    elif pick.bet_type == "period1_spread":
        hp1 = game.home_score_p1
        ap1 = game.away_score_p1
        if hp1 is not None and ap1 is not None:
            try:
                spread_val = float(val.split("_")[-1])
                margin = hp1 - ap1
                if val.startswith("home"):
                    adjusted = margin + spread_val
                else:
                    adjusted = -margin + spread_val
                if adjusted == 0:
                    return "push"
                return "win" if adjusted > 0 else "loss"
            except (ValueError, IndexError):
                return None

    elif pick.bet_type == "period_winner":
        hp1 = game.home_score_p1
        ap1 = game.away_score_p1
        if hp1 is not None and ap1 is not None and val and val.startswith("p1_"):
            if hp1 > ap1:
                actual = "p1_home"
            elif ap1 > hp1:
                actual = "p1_away"
            else:
                actual = "p1_draw"
            return "win" if val == actual else "loss"

    elif pick.bet_type == "period_total":
        hp1 = game.home_score_p1
        ap1 = game.away_score_p1
        if hp1 is not None and ap1 is not None and val and val.startswith("p1_"):
            p1_total = hp1 + ap1
            try:
                line = float(val.split("_")[-1])
                if "over" in val:
                    if p1_total == line:
                        return "push"
                    return "win" if p1_total > line else "loss"
                elif "under" in val:
                    if p1_total == line:
                        return "push"
                    return "win" if p1_total < line else "loss"
            except (ValueError, IndexError):
                return None

    return None


async def _compute_top_picks(
    games: List[Game], session: AsyncSession, *, prefer_live: bool = False,
) -> dict[int, GameTopPick]:
    """Compute the best top_pick prediction for each game.

    Shared by /today, /live, and date-specific schedule endpoints.

    When *prefer_live* is False (default, used by /today), only prematch
    predictions are considered so the original pre-game recommendation is
    frozen and never changes once a game goes live.

    When *prefer_live* is True (used by /live), live-phase predictions are
    preferred; prematch is used as fallback when no live prediction exists.
    """
    max_implied = settings.best_bet_max_implied
    game_ids = [g.id for g in games]
    game_by_id = {g.id: g for g in games}
    top_picks: dict[int, GameTopPick] = {}
    if not game_ids:
        return top_picks

    # Choose which prediction phases to include
    if prefer_live:
        phase_filter = ("prematch", "live")
    else:
        phase_filter = ("prematch",)

    all_preds_result = await session.execute(
        select(Prediction).where(
            Prediction.game_id.in_(game_ids),
            Prediction.bet_type.in_(MARKET_BET_TYPES),
            Prediction.phase.in_(phase_filter),
            Prediction.edge.isnot(None),
            Prediction.odds_implied_prob.isnot(None),
        )
    )
    all_preds = all_preds_result.scalars().all()

    # Deduplicate when both phases are present.
    # prefer_live=True  → keep live over prematch
    # prefer_live=False → only prematch is queried, so no dedup needed
    if prefer_live:
        _seen: dict[tuple, Prediction] = {}
        for p in all_preds:
            key = (p.game_id, p.bet_type, p.prediction_value)
            existing = _seen.get(key)
            if existing is None or (existing.phase == "prematch" and p.phase == "live"):
                _seen[key] = p
        all_preds = list(_seen.values())

    # Compute fresh implied prob for heavy-juice detection only.
    # Ranking uses the stored (snapshot) edge/implied_prob so that
    # the top pick doesn't flip every time live odds shift.
    fresh_map: dict[int, Optional[float]] = {}
    for p in all_preds:
        game_obj = game_by_id.get(p.game_id)
        fresh = fresh_implied_prob(p, game_obj)
        fresh_map[p.id] = fresh if fresh is not None else p.odds_implied_prob

    # --- Tier 1: best-bet criteria (edge + confidence) ---
    tier1 = [
        p for p in all_preds
        if (p.edge or 0) >= settings.min_edge
        and (p.confidence or 0) >= settings.min_confidence
        and not is_heavy_juice(fresh_map.get(p.id) or p.odds_implied_prob, max_implied)
    ]

    def _tier1_sort_key(p):
        # Use the STORED edge and implied_prob (snapshot from prediction
        # generation) so the ranking is stable across odds updates.
        score = composite_pick_score(
            p.confidence, p.edge, p.odds_implied_prob
        )
        if (
            p.bet_type == "spread"
            and p.prediction_value
            and "+" in p.prediction_value
        ):
            score -= 0.10
        return score

    for pred in sorted(tier1, key=_tier1_sort_key, reverse=True):
        if pred.game_id not in top_picks:
            cur_impl = fresh_map.get(pred.id)
            if pred.bet_type == "spread":
                game_obj = game_by_id.get(pred.game_id)
                logger.debug(
                    "Spread top pick: game=%d val=%s cur_impl=%.4f "
                    "max_implied=%.4f "
                    "home_spread_price=%s away_spread_price=%s "
                    "stored_impl=%s",
                    pred.game_id,
                    pred.prediction_value,
                    cur_impl if cur_impl is not None else -1,
                    max_implied,
                    getattr(game_obj, "home_spread_price", "N/A") if game_obj else "no_game",
                    getattr(game_obj, "away_spread_price", "N/A") if game_obj else "no_game",
                    pred.odds_implied_prob,
                )
            top_picks[pred.game_id] = GameTopPick(
                bet_type=pred.bet_type,
                prediction_value=pred.prediction_value,
                confidence=pred.confidence,
                edge=pred.edge,
                is_fallback=False,

            )

    # --- Tier 3: confidence-only fallback when odds data is missing ---
    still_missing = set(gid for gid in game_ids if gid not in top_picks)
    if still_missing:
        no_odds_result = await session.execute(
            select(Prediction).where(
                Prediction.game_id.in_(list(still_missing)),
                Prediction.bet_type.in_(MARKET_BET_TYPES),
                Prediction.phase.in_(phase_filter),
            )
        )
        no_odds_preds_raw = no_odds_result.scalars().all()
        # Deduplicate based on prefer_live setting
        _seen_t3: dict[tuple, Prediction] = {}
        for p in no_odds_preds_raw:
            key = (p.game_id, p.bet_type, p.prediction_value)
            existing = _seen_t3.get(key)
            if prefer_live:
                if existing is None or (existing.phase == "prematch" and p.phase == "live"):
                    _seen_t3[key] = p
            else:
                if existing is None:
                    _seen_t3[key] = p
        no_odds_preds = list(_seen_t3.values())
        for pred in sorted(
            no_odds_preds,
            key=lambda p: p.confidence or 0,
            reverse=True,
        ):
            if pred.game_id not in top_picks:
                # Exclude heavy-juice bets — if we have odds and they
                # exceed the ceiling, skip entirely rather than show a
                # bet with terrible risk/reward.
                if is_heavy_juice(pred.odds_implied_prob, max_implied):
                    continue
                top_picks[pred.game_id] = GameTopPick(
                    bet_type=pred.bet_type,
                    prediction_value=pred.prediction_value,
                    confidence=pred.confidence,
                    edge=pred.edge,
                    is_fallback=pred.odds_implied_prob is None,
                )

    # Grade outcomes for final games
    for game_id, pick in top_picks.items():
        game_obj = game_by_id.get(game_id)
        if game_obj and game_obj.status and game_obj.status.lower() in GAME_FINAL_STATUSES:
            pick.outcome = _grade_top_pick(pick, game_obj)

    return top_picks


async def _compute_top_props(
    games: List[Game], session: AsyncSession, *, prefer_live: bool = False,
) -> dict[int, GameTopPick]:
    """Select the best prop prediction for each game (non-market bet types).

    Uses a tiered approach matching _compute_top_picks():
      Tier 1: Props with real sportsbook odds AND positive edge (composite score).
      Tier 2: Props with real odds, any edge (composite score).
      Tier 3: No-odds fallback — only if nothing else exists for a game.
              Skips trivially high-confidence bets (e.g., BTTS No at 93%)
              that provide no useful betting signal.

    When *prefer_live* is False (default), only prematch predictions are used
    so the original recommendation stays frozen.  When True, live-phase
    predictions are preferred with prematch as fallback.
    """
    game_ids = [g.id for g in games]
    game_by_id = {g.id: g for g in games}
    top_props: dict[int, GameTopPick] = {}
    if not game_ids:
        return top_props

    phase_filter = ("prematch", "live") if prefer_live else ("prematch",)

    result = await session.execute(
        select(Prediction).where(
            Prediction.game_id.in_(game_ids),
            Prediction.bet_type.in_(PROP_BET_TYPES),
            Prediction.phase.in_(phase_filter),
        )
    )
    all_props = result.scalars().all()

    # Deduplicate when both phases are present
    if prefer_live:
        _seen: dict[tuple, Prediction] = {}
        for p in all_props:
            key = (p.game_id, p.bet_type, p.prediction_value)
            existing = _seen.get(key)
            if existing is None or (existing.phase == "prematch" and p.phase == "live"):
                _seen[key] = p
        deduped = list(_seen.values())
    else:
        deduped = list(all_props)

    # Build a map of the best available implied prob per prediction.
    # Fresh odds are used for tier promotion (does it have odds?), but
    # the STORED edge/implied_prob is used for ranking so the top prop
    # doesn't flip every time live odds shift.
    effective_impl: dict[int, Optional[float]] = {}
    effective_edge: dict[int, Optional[float]] = {}
    for pred in deduped:
        game_obj = game_by_id.get(pred.game_id)
        fresh_ip = fresh_implied_prob(pred, game_obj)
        # For tier checks, prefer fresh (detects newly available odds)
        # then fall back to stored value.
        ip = fresh_ip if fresh_ip is not None else pred.odds_implied_prob
        effective_impl[pred.id] = ip
        if ip is not None:
            effective_edge[pred.id] = (pred.confidence or 0) - ip
        else:
            effective_edge[pred.id] = pred.edge

    max_implied = settings.best_bet_max_implied

    # --- Tier 1: props with real odds AND positive edge ---
    tier1 = [
        p for p in deduped
        if effective_impl.get(p.id) is not None
        and (effective_edge.get(p.id) or 0) > 0
        and not is_heavy_juice(effective_impl.get(p.id), max_implied)
    ]
    for pred in sorted(
        tier1,
        key=lambda p: composite_pick_score(
            p.confidence, p.edge, p.odds_implied_prob
        ),
        reverse=True,
    ):
        if pred.game_id not in top_props:
            top_props[pred.game_id] = GameTopPick(
                bet_type=pred.bet_type,
                prediction_value=pred.prediction_value,
                confidence=pred.confidence,
                edge=effective_edge.get(pred.id, pred.edge),
                is_fallback=False,

            )

    # --- Tier 2: props with real odds, any edge ---
    still_missing = set(gid for gid in game_ids if gid not in top_props)
    if still_missing:
        tier2 = [
            p for p in deduped
            if p.game_id in still_missing
            and effective_impl.get(p.id) is not None
            and not is_heavy_juice(effective_impl.get(p.id), max_implied)
        ]
        for pred in sorted(
            tier2,
            key=lambda p: composite_pick_score(
                p.confidence, p.edge or 0, p.odds_implied_prob
            ),
            reverse=True,
        ):
            if pred.game_id not in top_props:
                top_props[pred.game_id] = GameTopPick(
                    bet_type=pred.bet_type,
                    prediction_value=pred.prediction_value,
                    confidence=pred.confidence,
                    edge=effective_edge.get(pred.id, pred.edge),
                    is_fallback=False,
    
                )

    # --- Tier 3: no-odds fallback (confidence-only) ---
    # Only for games that have NO odds-backed props at all.
    # Skip trivially-confident bets that aren't useful betting signals
    # (e.g., BTTS No at 90%+ — always true, not insightful).
    still_missing = set(gid for gid in game_ids if gid not in top_props)
    if still_missing:
        tier3 = [
            p for p in deduped
            if p.game_id in still_missing
            and (p.confidence or 0) < 0.88  # skip trivially obvious bets
        ]
        for pred in sorted(
            tier3,
            key=lambda p: p.confidence or 0,
            reverse=True,
        ):
            if pred.game_id not in top_props:
                # Exclude heavy-juice bets even in fallback tier
                if is_heavy_juice(pred.odds_implied_prob, max_implied):
                    continue
                top_props[pred.game_id] = GameTopPick(
                    bet_type=pred.bet_type,
                    prediction_value=pred.prediction_value,
                    confidence=pred.confidence,
                    edge=pred.edge,
                    is_fallback=pred.odds_implied_prob is None,
    
                )

    # Grade outcomes for final games
    for game_id, prop in top_props.items():
        game_obj = game_by_id.get(game_id)
        if game_obj and game_obj.status and game_obj.status.lower() in GAME_FINAL_STATUSES:
            prop.outcome = _grade_top_pick(prop, game_obj)

    return top_props


async def _games_for_date(
    target_date: date, session: AsyncSession
) -> List[ScheduleGame]:
    result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(Game.date == target_date)
        .order_by(Game.start_time.asc().nulls_last(), Game.id.asc())
    )
    games = result.scalars().all()

    # Pre-fetch best prediction per game using composite score
    top_picks = await _compute_top_picks(games, session)
    top_props = await _compute_top_props(games, session)

    # Batch-load team stats
    all_team_ids = list({g.home_team_id for g in games} | {g.away_team_id for g in games})
    stats_map = await _batch_load_team_stats(all_team_ids, session)

    schedule_games: List[ScheduleGame] = []
    for game in games:
        home_brief = _build_team_brief(game.home_team, stats_map.get(game.home_team_id))
        away_brief = _build_team_brief(game.away_team, stats_map.get(game.away_team_id))
        schedule_games.append(
            _build_schedule_game(
                game, home_brief, away_brief,
                top_picks.get(game.id), top_props.get(game.id),
            )
        )
    return schedule_games


async def _try_sync_schedule(
    session: AsyncSession, target_date: Optional[date] = None
) -> int:
    """Sync schedule from NHL API. Raises HTTPException on failure."""
    try:
        from app.scrapers.nhl_api import NHLScraper

        scraper = NHLScraper()
        date_str = target_date.isoformat() if target_date else None
        games = await scraper.sync_schedule(session, date_str)
        return len(games) if isinstance(games, list) else 0
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="NHL scraper module is not available.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to sync schedule from NHL API: {exc}",
        )


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

@router.get("/live", response_model=ScheduleResponse)
async def get_live_games(
    session: AsyncSession = Depends(get_session),
):
    """Return all currently in-progress games.

    Syncs scores/clock from NHL API for live games. Odds syncing is
    handled by the background scheduler — not inline in GET requests.
    """
    result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(func.lower(Game.status).in_(("in_progress", "live")))
        .order_by(Game.start_time.asc().nulls_last(), Game.id.asc())
    )
    games = result.scalars().all()

    # Sync scores/clock from NHL API (not odds — scheduler handles that)
    if games:
        try:
            async with session.begin_nested():
                for game in games:
                    await _try_sync_schedule(session, target_date=game.date)
                await session.flush()
        except Exception as exc:
            logger.warning("Live schedule sync failed: %s", exc)

        # Re-query to get fresh ORM objects after savepoint
        result = await session.execute(
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(func.lower(Game.status).in_(("in_progress", "live")))
            .order_by(Game.start_time.asc().nulls_last(), Game.id.asc())
        )
        games = result.scalars().all()

    # Compute top picks and props for live games — prefer live-phase
    # predictions so the "Live Now" section shows updated recommendations.
    top_picks = await _compute_top_picks(games, session, prefer_live=True)
    top_props = await _compute_top_props(games, session, prefer_live=True)

    # Batch-load team stats
    all_team_ids = list({g.home_team_id for g in games} | {g.away_team_id for g in games})
    stats_map = await _batch_load_team_stats(all_team_ids, session)

    schedule_games: List[ScheduleGame] = []
    for game in games:
        home_brief = _build_team_brief(game.home_team, stats_map.get(game.home_team_id))
        away_brief = _build_team_brief(game.away_team, stats_map.get(game.away_team_id))
        schedule_games.append(_build_schedule_game(
            game, home_brief, away_brief,
            top_picks.get(game.id), top_props.get(game.id),
        ))

    today = date.today()
    return ScheduleResponse(date=today, game_count=len(schedule_games), games=schedule_games)


@router.get("/today", response_model=ScheduleResponse)
async def get_today_schedule(
    session: AsyncSession = Depends(get_session),
):
    """Return today's schedule.

    Syncs scores/clock from NHL API for live games. Odds and predictions
    are kept fresh by the background scheduler — this endpoint only reads.
    """
    today = date.today()
    games = await _games_for_date(today, session)

    # Sync scores/clock if live or if we have no games yet
    has_live = any(g.status and g.status.lower() in ("in_progress", "live") for g in games)
    if not games or has_live:
        try:
            async with session.begin_nested():
                await _try_sync_schedule(session, target_date=today)
                await session.flush()
            games = await _games_for_date(today, session)
        except Exception as exc:
            logger.warning("Today schedule sync failed: %s", exc)

    return ScheduleResponse(date=today, game_count=len(games), games=games)


@router.get("/{date_str}", response_model=ScheduleResponse)
async def get_schedule_by_date(
    date_str: str,
    session: AsyncSession = Depends(get_session),
):
    try:
        target_date = date.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format '{date_str}'. Expected YYYY-MM-DD.",
        )

    games = await _games_for_date(target_date, session)
    return ScheduleResponse(
        date=target_date, game_count=len(games), games=games
    )


@router.post("/sync", response_model=SyncResult)
async def sync_schedule(
    session: AsyncSession = Depends(get_session),
):
    count = await _try_sync_schedule(session)
    return SyncResult(
        success=True,
        message=f"Successfully synced {count} games from the NHL API.",
        games_synced=count,
    )


@router.post("/sync-odds")
async def force_sync_odds(
    session: AsyncSession = Depends(get_session),
):
    """Force-sync odds from all sportsbook sources and regenerate predictions."""
    from app.services.odds import sync_odds_and_regenerate

    matched, pred_count = await sync_odds_and_regenerate(session, force=True)

    matched_pairs = [
        f"{m.get('away_abbrev', '')}@{m.get('home_abbrev', '')}"
        for m in matched
    ]

    # Broadcast to WebSocket clients
    try:
        from app.live import manager as ws_manager
        await ws_manager.broadcast({
            "type": "odds_update",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "changed_games": [],
            "predictions_updated": pred_count > 0,
            "source": "force_sync_odds",
        })
    except Exception as exc:
        logger.warning("WebSocket broadcast failed: %s", exc)

    return {
        "status": "ok",
        "odds_matched": len(matched),
        "predictions_generated": pred_count,
        "matched_games": matched_pairs,
    }


@router.get("/odds-usage")
async def odds_api_usage():
    """Check remaining Odds API quota."""
    from app.scrapers.odds_api import OddsAPIScraper

    async with OddsAPIScraper() as scraper:
        usage = await scraper.get_usage()

    if usage is None:
        raise HTTPException(status_code=503, detail="Could not fetch API usage (key missing or API unreachable)")

    return usage
