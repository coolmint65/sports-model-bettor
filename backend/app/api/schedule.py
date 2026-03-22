"""
Schedule API routes.

Provides endpoints for retrieving game schedules by date.
Odds syncing is handled by the background scheduler (app.live) —
GET endpoints are read-only.
"""

import logging
import time as _time
from datetime import date, datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.constants import GAME_FINAL_STATUSES, MARKET_BET_TYPES, composite_pick_score, is_heavy_juice
from app.database import get_session, get_session_context, get_write_session_context
from app.models.game import Game, GameGoalieStats
from app.models.player import Player
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
    prediction_id: Optional[int] = None
    bet_type: Optional[str] = None
    prediction_value: Optional[str] = None
    confidence: Optional[float] = None
    bet_confidence: Optional[float] = None
    edge: Optional[float] = None
    composite_score: Optional[float] = None
    is_fallback: bool = False
    outcome: Optional[str] = None  # "win", "loss", or None (pending/in-progress)
    reasoning: Optional[str] = None
    odds_display: Optional[float] = None


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


class GoalieStarter(BaseModel):
    """Confirmed or projected starting goalie for a team."""
    name: Optional[str] = None
    confirmed: bool = False
    status: Optional[str] = None  # e.g. "Confirmed", "Expected", "Likely", "Projected"


class ScheduleGame(BaseModel):
    id: int
    external_id: str
    sport: str = "nhl"
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
    # Top prediction for this game (single best across all markets)
    top_pick: Optional[GameTopPick] = None
    # Best pick per market type (ML, Spread, O/U) — up to 3
    top_picks: Optional[List[GameTopPick]] = None
    # Top prop prediction for this game
    top_prop: Optional[GameTopPick] = None
    # Sportsbook odds
    odds: Optional[GameOdds] = None
    pregame_odds: Optional[GameOdds] = None
    # Starting goalies
    home_starter: Optional[GoalieStarter] = None
    away_starter: Optional[GoalieStarter] = None

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
        # NBA uses W-L; NHL (and other OTL sports) uses W-L-OTL
        sport = getattr(team, "sport", "nhl") or "nhl"
        if sport == "nba":
            brief.record = f"{stats.wins}-{stats.losses}"
        else:
            brief.record = f"{stats.wins}-{stats.losses}-{stats.ot_losses}"
    return brief


def _build_game_odds(game: Game) -> Optional[GameOdds]:
    if game.home_moneyline is None and game.away_moneyline is None:
        return None

    hsp = game.home_spread_price
    asp = game.away_spread_price
    hsl = game.home_spread_line

    # Fall back to all_spread_lines data when primary spread prices are missing
    if (hsp is None or asp is None) and game.all_spread_lines:
        try:
            alt_lines = game.all_spread_lines if isinstance(game.all_spread_lines, list) else []
            target_line = abs(hsl) if hsl is not None else 1.5
            for alt in alt_lines:
                alt_line = abs(alt.get("line", alt.get("home_spread", 0)))
                if abs(alt_line - target_line) < 0.01:
                    if hsp is None and alt.get("home_price") is not None:
                        hsp = alt["home_price"]
                    if asp is None and alt.get("away_price") is not None:
                        asp = alt["away_price"]
                    break
        except Exception:
            pass

    # Sanity check: ensure spread prices match spread direction.
    # Negative spread (favorite) → positive price; positive spread (underdog) → negative price.
    # If swapped, correct them in the API response.
    if hsl is not None and hsp is not None and asp is not None:
        if hsl < 0 and hsp < 0 and asp > 0:
            hsp, asp = asp, hsp
        elif hsl > 0 and hsp > 0 and asp < 0:
            hsp, asp = asp, hsp

    return GameOdds(
        home_moneyline=game.home_moneyline,
        away_moneyline=game.away_moneyline,
        over_under_line=game.over_under_line,
        over_price=game.over_price,
        under_price=game.under_price,
        home_spread_line=game.home_spread_line,
        away_spread_line=game.away_spread_line,
        home_spread_price=hsp,
        away_spread_price=asp,
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
    home_starter: Optional[GoalieStarter] = None,
    away_starter: Optional[GoalieStarter] = None,
    top_picks: Optional[List[GameTopPick]] = None,
) -> ScheduleGame:
    # When no market pick exists but a prop does, promote the prop
    # to top_pick so the dashboard still shows a recommendation.
    effective_pick = top_pick if top_pick is not None else top_prop

    return ScheduleGame(
        id=game.id,
        external_id=game.external_id,
        sport=game.sport,
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
        top_pick=effective_pick,
        top_picks=top_picks,
        top_prop=top_prop,
        odds=_build_game_odds(game),
        pregame_odds=_build_pregame_odds(game),
        home_starter=home_starter,
        away_starter=away_starter,
    )


def _pick_odds_display(pred: Prediction, game: Optional[Game]) -> Optional[float]:
    """Extract the American odds for the specific pick from the game's odds.

    For spreads, validates that the price matches the spread direction:
    - Negative spread (favorite, e.g. -1.5) → positive price (hard to cover)
    - Positive spread (underdog, e.g. +1.5) → negative price (easy to cover)
    If the prices are swapped, returns the opposite side's price.
    """
    if game is None:
        return None
    bt = pred.bet_type
    val = (pred.prediction_value or "").lower()

    home_abbr = ""
    away_abbr = ""
    if game.home_team:
        home_abbr = getattr(game.home_team, "abbreviation", "").lower()
    if game.away_team:
        away_abbr = getattr(game.away_team, "abbreviation", "").lower()

    if bt == "ml":
        if val == "home" or val == home_abbr:
            return game.home_moneyline
        if val == "away" or val == away_abbr:
            return game.away_moneyline
    elif bt == "total":
        if "over" in val:
            return game.over_price
        if "under" in val:
            return game.under_price
    elif bt == "spread":
        is_home_pick = val.startswith("home") or (home_abbr and val.startswith(home_abbr))
        if is_home_pick:
            price = game.home_spread_price
            alt_price = game.away_spread_price
            line = game.home_spread_line
        elif val.startswith("away") or (away_abbr and val.startswith(away_abbr)):
            price = game.away_spread_price
            alt_price = game.home_spread_price
            line = game.away_spread_line
        else:
            return None

        # Sanity check: spread direction must match price sign.
        # Negative spread (favorite) should have positive price;
        # Positive spread (underdog) should have negative price.
        if (
            line is not None
            and price is not None
            and alt_price is not None
        ):
            if line < 0 and price < 0 and alt_price > 0:
                return alt_price
            if line > 0 and price > 0 and alt_price < 0:
                return alt_price
        return price
    return None


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

    else:
        # Dispatch to prop grading
        from app.props.grading import check_prop_outcome
        home_abbr = game.home_team.abbreviation if game.home_team else ""
        result = check_prop_outcome(pick.bet_type, val, game, home_abbr)
        if result is True:
            return "win"
        elif result is False:
            return "loss"
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
    #
    # When prefer_live=False (/today endpoint), use ONLY the stored
    # snapshot implied prob.  Using live odds would cause valid prematch
    # recommendations to vanish once a game goes live and the line moves
    # to heavy juice (e.g., prematch -130 → live -400).
    fresh_map: dict[int, Optional[float]] = {}
    for p in all_preds:
        if prefer_live:
            game_obj = game_by_id.get(p.game_id)
            fresh = fresh_implied_prob(p, game_obj)
            fresh_map[p.id] = fresh if fresh is not None else p.odds_implied_prob
        else:
            fresh_map[p.id] = p.odds_implied_prob

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
                prediction_id=pred.id,
                bet_type=pred.bet_type,
                prediction_value=pred.prediction_value,
                confidence=pred.confidence,
                bet_confidence=pred.bet_confidence,
                edge=pred.edge,
                is_fallback=False,
                reasoning=pred.reasoning,
                odds_display=_pick_odds_display(pred, game_by_id.get(pred.game_id)),
            )

    # --- Tier 3: fallback for games with no Tier 1 pick ---
    # Uses composite scoring (same as Tier 1 and game detail page) to
    # pick the best available prediction even when edge or confidence is
    # below the strict Tier 1 thresholds.
    #
    # Heavy-juice picks are still filtered here — showing a -238 spread
    # as the top pick is misleading even when it's the only prediction
    # for a game.  Better to show no pick than a heavy-juice one.
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

        # Populate fresh_map for Tier 3 predictions (they come from a
        # separate query and aren't covered by the initial fresh_map pass).
        for p in no_odds_preds:
            if p.id not in fresh_map:
                if prefer_live:
                    game_obj = game_by_id.get(p.game_id)
                    fresh = fresh_implied_prob(p, game_obj)
                    fresh_map[p.id] = fresh if fresh is not None else p.odds_implied_prob
                else:
                    fresh_map[p.id] = p.odds_implied_prob

        def _tier3_sort_key(p):
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

        # Tier 3 uses the same juice ceiling as Tier 1.  The fallback
        # tier already relaxes edge/confidence requirements — allowing
        # heavier juice on top of that leads to misleading picks like
        # a -246 spread showing as the "best pick".
        _TIER3_MAX_IMPLIED = max_implied
        for pred in sorted(no_odds_preds, key=_tier3_sort_key, reverse=True):
            if pred.game_id not in top_picks:
                cur_impl = fresh_map.get(pred.id) or pred.odds_implied_prob
                if is_heavy_juice(cur_impl, _TIER3_MAX_IMPLIED):
                    continue
                top_picks[pred.game_id] = GameTopPick(
                    prediction_id=pred.id,
                    bet_type=pred.bet_type,
                    prediction_value=pred.prediction_value,
                    confidence=pred.confidence,
                    bet_confidence=pred.bet_confidence,
                    edge=pred.edge,
                    is_fallback=pred.odds_implied_prob is None,
                    reasoning=pred.reasoning,
                    odds_display=_pick_odds_display(pred, game_by_id.get(pred.game_id)),
                )

    # Grade outcomes for final games
    for game_id, pick in top_picks.items():
        game_obj = game_by_id.get(game_id)
        if game_obj and game_obj.status and game_obj.status.lower() in GAME_FINAL_STATUSES:
            pick.outcome = _grade_top_pick(pick, game_obj)

    return top_picks


async def _compute_top_picks_by_market(
    games: List[Game], session: AsyncSession, *, prefer_live: bool = False,
) -> dict[int, List[GameTopPick]]:
    """Compute the best prediction per market type (ML, Spread, O/U) for each game.

    Returns a dict of game_id → list of up to 3 GameTopPick objects,
    one for each market type that has a qualifying prediction.
    """
    max_implied = settings.best_bet_max_implied
    game_ids = [g.id for g in games]
    game_by_id = {g.id: g for g in games}

    if not game_ids:
        return {}

    phase_filter = ("prematch", "live") if prefer_live else ("prematch",)

    all_preds_result = await session.execute(
        select(Prediction).where(
            Prediction.game_id.in_(game_ids),
            Prediction.bet_type.in_(MARKET_BET_TYPES),
            Prediction.phase.in_(phase_filter),
        )
    )
    all_preds = all_preds_result.scalars().all()

    # Dedup: prefer live over prematch when both exist
    if prefer_live:
        _seen: dict[tuple, Prediction] = {}
        for p in all_preds:
            key = (p.game_id, p.bet_type, p.prediction_value)
            existing = _seen.get(key)
            if existing is None or (existing.phase == "prematch" and p.phase == "live"):
                _seen[key] = p
        all_preds = list(_seen.values())

    # Group by (game_id, bet_type) and pick the best per market
    from collections import defaultdict
    by_game_market: dict[int, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for p in all_preds:
        by_game_market[p.game_id][p.bet_type].append(p)

    result: dict[int, List[GameTopPick]] = {}
    for game_id, markets in by_game_market.items():
        game_obj = game_by_id.get(game_id)
        picks_for_game = []
        for bet_type in ("ml", "total", "spread"):
            preds = markets.get(bet_type, [])
            if not preds:
                continue

            # Sort by composite score
            def _sort_key(p):
                return composite_pick_score(
                    p.confidence, p.edge, p.odds_implied_prob
                )

            preds.sort(key=_sort_key, reverse=True)
            best = preds[0]

            picks_for_game.append(GameTopPick(
                prediction_id=best.id,
                bet_type=best.bet_type,
                prediction_value=best.prediction_value,
                confidence=best.confidence,
                bet_confidence=best.bet_confidence,
                edge=best.edge,
                is_fallback=best.odds_implied_prob is None,
                reasoning=best.reasoning,
                odds_display=_pick_odds_display(best, game_obj),
            ))

        # Grade outcomes for final games
        if game_obj and game_obj.status and game_obj.status.lower() in GAME_FINAL_STATUSES:
            for pick in picks_for_game:
                pick.outcome = _grade_top_pick(pick, game_obj)

        if picks_for_game:
            result[game_id] = picks_for_game

    return result


def _prop_signal_strength(confidence: float, baseline: float) -> float:
    """Compute how far a prop's confidence deviates from the league norm.

    Raw confidence is not comparable across prop types: 48% for
    regulation-winner is unremarkable, while 30% for overtime-yes is a
    strong signal.  Signal strength normalises them:

        signal = (confidence - baseline) / baseline

    Positive → model sees something above average for this prop type.
    Higher → more interesting / more likely to represent real value.
    """
    if baseline <= 0:
        return confidence
    return (confidence - baseline) / baseline


async def _compute_top_props(
    games: List[Game], session: AsyncSession,
) -> dict[int, GameTopPick]:
    """Pick the single best prop prediction per game.

    Ranking order:
    1. Real edge from sportsbook odds (when available).
    2. Signal strength — how far the model's confidence deviates from
       the league-average baseline for that prop type.  This makes
       "OT Yes at 30%"  (signal +0.30 vs 23% baseline) beat
       "Reg Winner at 48%" (signal +0.14 vs 42% baseline) because the
       OT pick is genuinely surprising while the reg-winner pick is
       just restating the obvious.

    Deduplicates to one candidate per bet_type before comparing across
    types.  Uses the same GameTopPick schema so the frontend can render
    it identically to a market top_pick.
    """
    from app.props.types import PROP_BY_BET_TYPE

    prop_bet_types = tuple(PROP_BY_BET_TYPE.keys())
    # Build baseline lookup from each prop class.
    baselines: dict[str, float] = {
        bt: cls.baseline for bt, cls in PROP_BY_BET_TYPE.items()
    }

    game_ids = [g.id for g in games]
    game_by_id = {g.id: g for g in games}
    top_props: dict[int, GameTopPick] = {}
    if not game_ids:
        return top_props

    result = await session.execute(
        select(Prediction).where(
            Prediction.game_id.in_(game_ids),
            Prediction.bet_type.in_(prop_bet_types),
            Prediction.phase == "prematch",
        )
    )
    all_props = result.scalars().all()

    # Deduplicate: keep only the best candidate per bet_type per game
    # (e.g., only the strongest reg_winner side, not all three).
    by_game: dict[int, dict[str, Prediction]] = {}
    for pred in all_props:
        existing = by_game.setdefault(pred.game_id, {}).get(pred.bet_type)
        if existing is None or (pred.confidence or 0) > (existing.confidence or 0):
            by_game[pred.game_id][pred.bet_type] = pred

    for game_id, type_map in by_game.items():
        preds = list(type_map.values())

        # Prefer props with real edge from sportsbook odds.
        with_edge = [p for p in preds if p.edge is not None and p.edge > 0]
        if with_edge:
            best = max(with_edge, key=lambda p: (p.edge, p.confidence or 0))
        else:
            # No odds available — rank by signal strength so that a
            # genuinely surprising pick (high deviation from its
            # baseline) beats a structurally high-confidence but
            # obvious pick.
            best = max(
                preds,
                key=lambda p: _prop_signal_strength(
                    p.confidence or 0,
                    baselines.get(p.bet_type, 0.50),
                ),
            )

        top_props[game_id] = GameTopPick(
            prediction_id=best.id,
            bet_type=best.bet_type,
            prediction_value=best.prediction_value,
            confidence=best.confidence,
            bet_confidence=best.bet_confidence,
            edge=best.edge,
            is_fallback=False,
            reasoning=best.reasoning,
        )

    # Grade outcomes for final games
    for game_id, pick in top_props.items():
        game_obj = game_by_id.get(game_id)
        if game_obj and game_obj.status and game_obj.status.lower() in GAME_FINAL_STATUSES:
            pick.outcome = _grade_top_pick(pick, game_obj)

    return top_props


async def _fetch_starters_for_games(
    games: List[Game], session: AsyncSession,
) -> dict[int, dict[str, GoalieStarter]]:
    """Batch-fetch starting goalies for upcoming games.

    Returns {game_id: {"home": GoalieStarter, "away": GoalieStarter}}.
    Only fetches for games that haven't started yet.

    Uses a three-tier strategy:
    1. External scrapers (DailyFaceoff, RotoWire, NHL API)
    2. DB fallback — each team's #1 goalie by games started this season
    """
    upcoming = [
        g for g in games
        if g.status and g.status.lower() in (
            "scheduled", "preview", "pre-game", "pregame", "fut", "pre",
        )
    ]
    if not upcoming:
        return {}

    starters_map: dict[int, dict[str, GoalieStarter]] = {}

    # --- Tier 1: external scrapers ---
    try:
        from app.scrapers.starter_scraper import sync_confirmed_starters
        raw_starters = await sync_confirmed_starters(session)
        for s in raw_starters:
            gid = s["game_id"]
            if gid not in starters_map:
                starters_map[gid] = {}
            side = "home" if s["team_id"] == next(
                (g.home_team_id for g in upcoming if g.id == gid), None
            ) else "away"
            starters_map[gid][side] = GoalieStarter(
                name=s["goalie_name"],
                confirmed=s["confirmed"],
                status=s.get("status"),
            )
    except Exception as exc:
        logger.warning("Could not fetch starters from scrapers: %s", exc)

    # --- Tier 2: DB fallback for any games missing starters ---
    missing_team_ids: set[int] = set()
    for g in upcoming:
        game_starters = starters_map.get(g.id, {})
        if "home" not in game_starters:
            missing_team_ids.add(g.home_team_id)
        if "away" not in game_starters:
            missing_team_ids.add(g.away_team_id)

    if missing_team_ids:
        try:
            db_goalie_map = await _db_fallback_starters(session, missing_team_ids)
            logger.info(
                "Goalie DB fallback: filled %d/%d teams missing from scrapers",
                len(db_goalie_map), len(missing_team_ids),
            )
            for g in upcoming:
                if g.id not in starters_map:
                    starters_map[g.id] = {}
                if "home" not in starters_map[g.id] and g.home_team_id in db_goalie_map:
                    starters_map[g.id]["home"] = db_goalie_map[g.home_team_id]
                if "away" not in starters_map[g.id] and g.away_team_id in db_goalie_map:
                    starters_map[g.id]["away"] = db_goalie_map[g.away_team_id]
        except Exception as exc:
            logger.warning("Goalie DB fallback error: %s", exc, exc_info=True)
    else:
        logger.info("Goalie starters: no missing teams (all covered by scrapers or no upcoming games)")

    # Persist resolved starters to Game model so the prediction engine
    # can read them without re-scraping DFO.
    for g in upcoming:
        gs = starters_map.get(g.id, {})
        changed = False
        home_s = gs.get("home")
        away_s = gs.get("away")
        if home_s and home_s.name and g.home_starter_name != home_s.name:
            g.home_starter_name = home_s.name
            g.home_starter_status = home_s.status or ("Confirmed" if home_s.confirmed else "Expected")
            changed = True
        if away_s and away_s.name and g.away_starter_name != away_s.name:
            g.away_starter_name = away_s.name
            g.away_starter_status = away_s.status or ("Confirmed" if away_s.confirmed else "Expected")
            changed = True
        if changed:
            session.add(g)
    try:
        await session.flush()
    except Exception as exc:
        logger.warning("Failed to persist starters to Game model: %s", exc)

    return starters_map


async def _db_fallback_starters(
    session: AsyncSession, team_ids: set[int],
) -> dict[int, GoalieStarter]:
    """Look up the likely starter for each team from GameGoalieStats.

    Determines team association via Game.home_team_id/away_team_id
    (not Player.team_id, which may be null). Picks the goalie with
    the most game appearances per team.
    Returns {team_id: GoalieStarter}.
    """
    # Build a union of home-side and away-side goalie appearances.
    # For each finished game, a goalie in GameGoalieStats either
    # played for the home team or away team. We use the Game's
    # team IDs to figure out which side.
    from sqlalchemy import literal

    # Subquery: for each GameGoalieStats row, determine the team
    # by checking if the goalie's Player.team_id matches home or away.
    # But Player.team_id may be stale/null, so we use a heuristic:
    # the goalie's current team (from Player) OR infer from Game.
    #
    # Simplest reliable approach: query all GameGoalieStats for
    # recent finished games involving these teams, then pair in Python.
    finished_games_result = await session.execute(
        select(Game)
        .where(
            func.lower(Game.status).in_(GAME_FINAL_STATUSES),
            (Game.home_team_id.in_(team_ids)) | (Game.away_team_id.in_(team_ids)),
        )
        .order_by(Game.date.desc())
        .limit(200)  # recent games only
    )
    finished_games = finished_games_result.scalars().all()

    if not finished_games:
        logger.info("DB fallback: no finished games found for teams %s", team_ids)
        return {}

    game_ids = [g.id for g in finished_games]
    game_map = {g.id: g for g in finished_games}

    # Get all goalie stats for these games
    ggs_result = await session.execute(
        select(GameGoalieStats)
        .options(selectinload(GameGoalieStats.player))
        .where(GameGoalieStats.game_id.in_(game_ids))
    )
    all_ggs = ggs_result.scalars().all()

    # Count appearances per (team_id, player) using game context
    # to determine which team the goalie played for.
    # Heuristic: a goalie belongs to the team their Player.team_id
    # points to. If null, check which side of the game they're on
    # by comparing with other goalies in the same game.
    from collections import Counter
    team_goalie_counts: dict[int, Counter] = {tid: Counter() for tid in team_ids}
    goalie_names: dict[int, str] = {}

    for ggs in all_ggs:
        game = game_map.get(ggs.game_id)
        if not game:
            continue
        player = ggs.player
        if not player:
            continue
        goalie_names[player.id] = player.name

        # Determine which team this goalie played for
        ptid = player.team_id
        if ptid and ptid in team_ids:
            team_goalie_counts[ptid][player.id] += 1
        else:
            # Infer: if only one side of the game is in our target teams,
            # assume this goalie played for that side
            home_in = game.home_team_id in team_ids
            away_in = game.away_team_id in team_ids
            if home_in and not away_in:
                team_goalie_counts[game.home_team_id][player.id] += 1
            elif away_in and not home_in:
                team_goalie_counts[game.away_team_id][player.id] += 1

    team_starters: dict[int, GoalieStarter] = {}
    for tid in team_ids:
        counts = team_goalie_counts[tid]
        if counts:
            top_goalie_id = counts.most_common(1)[0][0]
            name = goalie_names.get(top_goalie_id, "Unknown")
            appearances = counts[top_goalie_id]
            team_starters[tid] = GoalieStarter(
                name=name,
                confirmed=False,
                status="Expected",
            )
            logger.info(
                "DB fallback goalie: team_id=%d -> %s (%d games)",
                tid, name, appearances,
            )
    return team_starters


async def _games_for_date(
    target_date: date, session: AsyncSession, sport: Optional[str] = None,
) -> List[ScheduleGame]:
    filters = [Game.date == target_date]
    if sport:
        filters.append(Game.sport == sport)
    result = await session.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(*filters)
        .order_by(Game.start_time.asc().nulls_last(), Game.id.asc())
    )
    games = result.scalars().all()

    # Pre-fetch best prediction per game using composite score
    top_picks = await _compute_top_picks(games, session)
    top_picks_by_market = await _compute_top_picks_by_market(games, session)
    top_props = await _compute_top_props(games, session)

    # Batch-load team stats
    all_team_ids = list({g.home_team_id for g in games} | {g.away_team_id for g in games})
    stats_map = await _batch_load_team_stats(all_team_ids, session)

    # Read starting goalies from persisted Game model fields.
    # External scraping is handled by the background scheduler
    # (_sync_confirmed_starters in app.live) every 15 minutes —
    # no need to call external APIs inline on every request.
    schedule_games: List[ScheduleGame] = []
    for game in games:
        home_brief = _build_team_brief(game.home_team, stats_map.get(game.home_team_id))
        away_brief = _build_team_brief(game.away_team, stats_map.get(game.away_team_id))
        home_starter = (
            GoalieStarter(
                name=game.home_starter_name,
                confirmed=game.home_starter_status in ("Confirmed",),
                status=game.home_starter_status,
            )
            if game.home_starter_name
            else None
        )
        away_starter = (
            GoalieStarter(
                name=game.away_starter_name,
                confirmed=game.away_starter_status in ("Confirmed",),
                status=game.away_starter_status,
            )
            if game.away_starter_name
            else None
        )
        schedule_games.append(
            _build_schedule_game(
                game, home_brief, away_brief,
                top_picks.get(game.id),
                top_props.get(game.id),
                home_starter=home_starter,
                away_starter=away_starter,
                top_picks=top_picks_by_market.get(game.id),
            )
        )
    return schedule_games


_last_schedule_sync: float = 0.0
_SCHEDULE_SYNC_THROTTLE = 30  # seconds — prevent rapid-fire syncs


async def _try_sync_schedule(
    session: AsyncSession, target_date: Optional[date] = None,
    sport: Optional[str] = None,
) -> int:
    """Sync schedule from the sport's API. Throttled to once per 30s.

    Multiple endpoints (``/live``, ``/today``) call this inline on every
    request.  Without throttling, 2 WebSocket clients cause rapid-fire
    schedule syncs (8+ in 5 seconds) which hammers the API and
    slows down response times.
    """
    global _last_schedule_sync
    now = _time.monotonic()
    if now - _last_schedule_sync < _SCHEDULE_SYNC_THROTTLE:
        return 0
    _last_schedule_sync = now

    # NBA schedule sync uses the NBA scraper
    if sport == "nba":
        try:
            from app.scrapers.nba_api import NBAScraper

            scraper = NBAScraper()
            try:
                date_str = target_date.isoformat() if target_date else None
                games = await scraper.sync_schedule(session, target_date=date_str)
                return len(games) if isinstance(games, list) else 0
            finally:
                await scraper.close()
        except ImportError:
            raise HTTPException(
                status_code=503,
                detail="NBA scraper module is not available.",
            )
        except Exception as exc:
            logger.error("Failed to sync schedule from NBA API: %s", exc, exc_info=True)
            raise HTTPException(
                status_code=502,
                detail="Failed to sync schedule from NBA API",
            )

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
        logger.error("Failed to sync schedule from NHL API: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=502,
            detail="Failed to sync schedule from NHL API",
        )


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

@router.get("/live", response_model=ScheduleResponse)
async def get_live_games(sport: Optional[str] = None):
    """Return all currently in-progress games — pure read from local DB.

    All external syncing (scores, clock, odds) is handled by the
    background scheduler.  This endpoint only reads committed data.
    """
    today = date.today()
    live_filters = [
        func.lower(Game.status).in_(("in_progress", "live")),
        Game.date == today,
    ]
    if sport:
        live_filters.append(Game.sport == sport)

    async with get_session_context() as session:
        result = await session.execute(
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(*live_filters)
            .order_by(Game.start_time.asc().nulls_last(), Game.id.asc())
        )
        games = result.scalars().all()

        # Compute top picks for live games — prefer live-phase
        # predictions so the "Live Now" section shows updated recommendations.
        top_picks = await _compute_top_picks(games, session, prefer_live=True)
        top_picks_by_market = await _compute_top_picks_by_market(games, session, prefer_live=True)
        top_props = await _compute_top_props(games, session)

        # Batch-load team stats
        all_team_ids = list({g.home_team_id for g in games} | {g.away_team_id for g in games})
        stats_map = await _batch_load_team_stats(all_team_ids, session)

        schedule_games: List[ScheduleGame] = []
        for game in games:
            home_brief = _build_team_brief(game.home_team, stats_map.get(game.home_team_id))
            away_brief = _build_team_brief(game.away_team, stats_map.get(game.away_team_id))
            schedule_games.append(_build_schedule_game(
                game, home_brief, away_brief,
                top_picks.get(game.id),
                top_props.get(game.id),
                top_picks=top_picks_by_market.get(game.id),
            ))

    today = date.today()
    return ScheduleResponse(date=today, game_count=len(schedule_games), games=schedule_games)


@router.get("/today", response_model=ScheduleResponse)
async def get_today_schedule(sport: Optional[str] = None):
    """Return today's schedule — pure read from local DB.

    All external syncing (schedule, odds, starters, predictions) is
    handled by the background scheduler in app.live.  GET endpoints
    never call external APIs or acquire the write lock, so they
    respond instantly regardless of what the scheduler is doing.
    """
    today = date.today()

    async with get_session_context() as session:
        games = await _games_for_date(today, session, sport=sport)

    return ScheduleResponse(date=today, game_count=len(games), games=games)


@router.get("/{date_str}", response_model=ScheduleResponse)
async def get_schedule_by_date(
    date_str: str,
    sport: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
):
    try:
        target_date = date.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format '{date_str}'. Expected YYYY-MM-DD.",
        )

    games = await _games_for_date(target_date, session, sport=sport)
    return ScheduleResponse(
        date=target_date, game_count=len(games), games=games
    )


@router.post("/sync", response_model=SyncResult)
async def sync_schedule():
    async with get_write_session_context() as session:
        count = await _try_sync_schedule(session)
    return SyncResult(
        success=True,
        message=f"Successfully synced {count} games from the NHL API.",
        games_synced=count,
    )


@router.post("/sync-odds")
async def force_sync_odds():
    """Force-sync odds from all sportsbook sources and regenerate predictions."""
    from app.services.odds import sync_odds_and_regenerate

    async with get_write_session_context() as session:
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
