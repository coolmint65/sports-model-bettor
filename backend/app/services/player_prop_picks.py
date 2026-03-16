"""
Player prop prediction engine.

Analyzes player performance data against sportsbook prop lines
to identify value bets. Compares recent per-game rates (goals,
shots, points, assists, saves) to the prop line and implied
probability to calculate edge.

Factors considered:
  - Historical per-game rates (last 15 games)
  - Opponent-specific matchup history (via PlayerMatchupStats or live query)
  - Home/away performance splits
  - Rest/fatigue (back-to-back detection, days rest)
  - Time-on-ice consistency and workload
  - Opponent defensive quality (goals/shots against vs league avg)
  - Line movement (opening vs current odds)
  - Special teams and physical play context
  - Injury status filtering

Supported markets:
  - player_goal_scorer_anytime (ATG): Poisson P(goals >= 1)
  - player_shots_on_goal (SOG): compare avg shots vs line
  - player_points: compare avg points vs line
  - player_assists: compare avg assists vs line
  - player_total_saves: compare avg saves vs line
"""

import logging
import math
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.game import Game, GameGoalieStats, GamePlayerStats
from app.models.injury import InjuryReport
from app.models.matchup import PlayerMatchupStats
from app.models.player import Player
from app.models.player_prop import PlayerPropOdds
from app.models.team import Team, TeamStats
from app.services.odds import american_to_implied as _svc_american_to_implied

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Number of recent games to use for per-game rate calculation
RECENT_GAMES_WINDOW = 15
# Minimum games required to generate a prediction
MIN_GAMES = 5
# Minimum games vs opponent to apply matchup adjustment
MIN_MATCHUP_GAMES = 3
# Minimum edge to consider a pick worth recommending
MIN_PICK_EDGE = 0.05

# Calibration shrinkage for player props. Poisson/Normal models
# structurally overestimate player prop probabilities because they
# don't model variance from ice time changes, scratches, blowouts,
# coaching decisions, etc. Same shrinkage level as game totals.
PROP_CALIBRATION_SHRINKAGE = 0.35
# Minimum confidence to include a pick
MIN_PICK_CONFIDENCE = 0.55
# Maximum picks to return per game (top N by edge)
MAX_PICKS_PER_GAME = 5
# Maximum implied probability (vig-free) for a prop to be considered.
# Props above this are too heavily juiced to offer real value.
# 0.65 ≈ -186 American odds.
MAX_PROP_IMPLIED = 0.65
# Max matchup adjustment to prevent extreme swings from small samples
MAX_MATCHUP_ADJUSTMENT = 0.25

# Home/away adjustment constants
HOME_BOOST = 0.04   # +4% rate boost for home games
AWAY_PENALTY = -0.03  # -3% rate penalty for away games
MIN_LOCATION_GAMES = 3  # Minimum games at a location for split

# Rest/fatigue constants
BACK_TO_BACK_PENALTY = -0.06  # -6% rate adjustment for B2B
WELL_RESTED_BOOST = 0.03      # +3% for 3+ days rest
NORMAL_REST_DAYS = 2           # 2 days rest is normal (no adjustment)

# Opponent quality constants
LEAGUE_AVG_GA_PG = 3.0   # Approximate league average goals against/game
LEAGUE_AVG_SA_PG = 30.0  # Approximate league average shots against/game
MAX_OPP_ADJUSTMENT = 0.15  # Cap opponent quality adjustment at ±15%

# TOI confidence constants
LOW_TOI_THRESHOLD = 12.0   # Below this, reduce confidence
HIGH_TOI_THRESHOLD = 18.0  # Above this, boost confidence
TOI_VARIANCE_PENALTY = 0.05  # Penalty per unit of TOI std dev above 3 min

# Line movement constants
SHARP_MOVE_THRESHOLD = 15  # American odds points of movement to flag


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def _calibrate_prop_prob(raw_prob: float) -> float:
    """Apply calibration shrinkage to a raw model probability.

    Pulls the probability toward 50% to correct for structural
    overconfidence in Poisson/Normal models when applied to player props.
    """
    calibrated = raw_prob * (1.0 - PROP_CALIBRATION_SHRINKAGE) + 0.5 * PROP_CALIBRATION_SHRINKAGE
    return max(0.01, min(0.99, calibrated))


def _american_to_implied(american: Optional[float]) -> Optional[float]:
    """Convert American odds to implied probability (includes vig).

    Delegates to the canonical implementation in services.odds.
    """
    return _svc_american_to_implied(american)


def _remove_vig(
    over_price: Optional[float], under_price: Optional[float]
) -> tuple[Optional[float], Optional[float]]:
    """Remove vig from over/under prices to get true probabilities.

    Sportsbooks bake in ~4-8% vig (overround) by making both sides
    sum to >100%.  To get the "true" probability, we normalize each
    side by the total implied probability.
    """
    impl_over = _american_to_implied(over_price)
    impl_under = _american_to_implied(under_price)

    if impl_over is not None and impl_under is not None:
        total = impl_over + impl_under
        if total > 0:
            return (impl_over / total, impl_under / total)

    # If only one side available, can't remove vig — return raw
    return (impl_over, impl_under)


def _poisson_at_least_one(avg_rate: float) -> float:
    """P(X >= 1) for Poisson distribution = 1 - P(X = 0) = 1 - e^(-lambda)."""
    if avg_rate <= 0:
        return 0.0
    return 1.0 - math.exp(-avg_rate)


def _poisson_over(avg_rate: float, line: float) -> float:
    """P(X > line) for Poisson distribution.

    For a line of 2.5, this is P(X >= 3).
    For a line of 0.5, this is P(X >= 1).
    """
    if avg_rate <= 0:
        return 0.0
    threshold = int(line) + 1 if line == int(line) + 0.5 else int(line + 1)
    # P(X >= threshold) = 1 - sum(P(X=k) for k in 0..threshold-1)
    cumulative = 0.0
    for k in range(threshold):
        cumulative += (avg_rate ** k) * math.exp(-avg_rate) / math.factorial(k)
    return 1.0 - cumulative


def _normal_over(values: list, line: float) -> float:
    """P(X > line) using normal distribution.

    Better than Poisson for high-count stats like goalie saves where
    the distribution is approximately normal.  Uses the actual sample
    standard deviation rather than assuming variance == mean (Poisson).
    """
    n = len(values)
    if n < 2:
        return 0.5
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / (n - 1)
    std = math.sqrt(variance) if variance > 0 else 1.0
    # P(X > line) using the complementary error function
    z = (line - mean) / (std * math.sqrt(2))
    return 0.5 * math.erfc(z)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class MatchupContext:
    """Opponent-specific performance context for a player."""

    opponent_team_id: int
    opponent_abbrev: Optional[str]
    vs_opponent_rate: Optional[float]  # per-game rate vs this opponent
    vs_opponent_games: int  # games played vs this opponent
    overall_rate: float  # overall per-game rate
    adjustment: float  # multiplier applied to model prob (e.g., 1.10 = +10%)
    reasoning: str  # human-readable matchup note


@dataclass
class GameContext:
    """Contextual factors for the current game being analyzed."""

    is_home: bool = True
    days_rest: Optional[int] = None  # None = unknown
    is_back_to_back: bool = False
    opp_defensive_factor: float = 1.0  # >1 = weak defense, <1 = strong
    opp_abbrev: Optional[str] = None
    line_moved_against: bool = False  # True if sharp money moved against pick
    line_movement_pts: float = 0.0  # Signed movement in American odds pts


@dataclass
class PropPick:
    """A single player prop recommendation."""

    player_name: str
    player_id: Optional[int]
    market: str
    pick_side: str  # "over", "under", or "yes"
    line: Optional[float]
    odds: Optional[float]  # American odds for the picked side
    model_prob: float  # Our estimated probability
    implied_prob: float  # Sportsbook implied probability
    edge: float  # model_prob - implied_prob
    confidence: float  # How confident we are (0-1)
    avg_rate: float  # Player's recent per-game rate
    games_sampled: int  # How many games used
    reasoning: str
    matchup: Optional[MatchupContext] = None  # opponent-specific context


# ---------------------------------------------------------------------------
# Stat queries (with Game eager loading for home/away tagging)
# ---------------------------------------------------------------------------

async def _get_skater_game_stats(
    session: AsyncSession,
    player_id: int,
    team_id: int,
    exclude_game_id: Optional[int] = None,
) -> List[GamePlayerStats]:
    """Get recent game stats for a skater, ordered by most recent.

    Eager-loads the Game relationship so callers can determine
    home/away and compute rest days.  Excludes the current game
    to prevent look-ahead bias when re-analyzing after final.
    """
    query = (
        select(GamePlayerStats)
        .join(Game, Game.id == GamePlayerStats.game_id)
        .options(selectinload(GamePlayerStats.game))
        .where(
            GamePlayerStats.player_id == player_id,
            Game.status == "final",
            Game.home_team_id.in_([team_id])
            | Game.away_team_id.in_([team_id]),
        )
    )
    if exclude_game_id is not None:
        query = query.where(Game.id != exclude_game_id)
    result = await session.execute(
        query.order_by(desc(Game.date)).limit(RECENT_GAMES_WINDOW)
    )
    return list(result.scalars().all())


async def _get_goalie_game_stats(
    session: AsyncSession,
    player_id: int,
    exclude_game_id: Optional[int] = None,
) -> List[GameGoalieStats]:
    """Get recent game stats for a goalie, ordered by most recent.

    Filters to completed games and meaningful appearances (>= 30 min TOI)
    to exclude relief appearances that would skew save averages.
    Eager-loads the Game relationship for home/away and rest tracking.
    Excludes the current game to prevent look-ahead bias.
    """
    query = (
        select(GameGoalieStats)
        .join(Game, Game.id == GameGoalieStats.game_id)
        .options(selectinload(GameGoalieStats.game))
        .where(
            GameGoalieStats.player_id == player_id,
            Game.status == "final",
            # Only include starts / full appearances (>= 30 min TOI).
            (GameGoalieStats.toi >= 30.0) | (GameGoalieStats.toi.is_(None)),
        )
    )
    if exclude_game_id is not None:
        query = query.where(Game.id != exclude_game_id)
    result = await session.execute(
        query.order_by(desc(Game.date)).limit(RECENT_GAMES_WINDOW)
    )
    return list(result.scalars().all())


async def _get_skater_vs_opponent_stats(
    session: AsyncSession,
    player_id: int,
    team_id: int,
    opponent_team_id: int,
) -> List[GamePlayerStats]:
    """Get a skater's game stats filtered to games against a specific opponent."""
    result = await session.execute(
        select(GamePlayerStats)
        .join(Game, Game.id == GamePlayerStats.game_id)
        .where(
            GamePlayerStats.player_id == player_id,
            Game.status == "final",
            Game.home_team_id.in_([team_id])
            | Game.away_team_id.in_([team_id]),
            Game.home_team_id.in_([opponent_team_id])
            | Game.away_team_id.in_([opponent_team_id]),
        )
        .order_by(desc(Game.date))
        .limit(RECENT_GAMES_WINDOW)
    )
    return list(result.scalars().all())


async def _get_goalie_vs_opponent_stats(
    session: AsyncSession,
    player_id: int,
    opponent_team_id: int,
) -> List[GameGoalieStats]:
    """Get a goalie's game stats filtered to games against a specific opponent."""
    result = await session.execute(
        select(GameGoalieStats)
        .join(Game, Game.id == GameGoalieStats.game_id)
        .where(
            GameGoalieStats.player_id == player_id,
            Game.status == "final",
            (GameGoalieStats.toi >= 30.0) | (GameGoalieStats.toi.is_(None)),
            Game.home_team_id.in_([opponent_team_id])
            | Game.away_team_id.in_([opponent_team_id]),
        )
        .order_by(desc(Game.date))
        .limit(RECENT_GAMES_WINDOW)
    )
    return list(result.scalars().all())


# ---------------------------------------------------------------------------
# Home/away splits (#1)
# ---------------------------------------------------------------------------

def _compute_location_adjustment(
    game_stats: list,
    stat_extractor,
    player_team_id: int,
    is_home: bool,
) -> float:
    """Compute how the player's rate differs at home vs away.

    Returns a rate multiplier (e.g. 1.08 means +8% at this location).
    Falls back to the generic HOME_BOOST / AWAY_PENALTY if insufficient
    split data.
    """
    home_values = []
    away_values = []

    for g in game_stats:
        game_obj = getattr(g, "game", None)
        if game_obj is None:
            continue
        val = stat_extractor(g)
        if game_obj.home_team_id == player_team_id:
            home_values.append(val)
        else:
            away_values.append(val)

    # Need enough data at each location
    if len(home_values) < MIN_LOCATION_GAMES or len(away_values) < MIN_LOCATION_GAMES:
        # Fall back to league-average home/away effect
        return HOME_BOOST if is_home else AWAY_PENALTY

    overall_avg = (sum(home_values) + sum(away_values)) / (len(home_values) + len(away_values))
    if overall_avg <= 0:
        return 0.0

    if is_home:
        location_avg = sum(home_values) / len(home_values)
    else:
        location_avg = sum(away_values) / len(away_values)

    deviation = (location_avg - overall_avg) / overall_avg
    # Cap to prevent extreme swings
    return max(-0.15, min(0.15, deviation))


# ---------------------------------------------------------------------------
# Rest/fatigue (#3)
# ---------------------------------------------------------------------------

def _compute_rest_days(game_stats: list, game_date: date) -> Optional[int]:
    """Calculate days since the player's most recent game.

    Uses the eager-loaded Game relationship on each stat row.
    """
    for g in game_stats:
        game_obj = getattr(g, "game", None)
        if game_obj and game_obj.date:
            delta = (game_date - game_obj.date).days
            if delta > 0:
                return delta
    return None


def _rest_adjustment(days_rest: Optional[int]) -> float:
    """Return a rate adjustment factor based on rest days.

    Back-to-back (1 day): penalty
    Normal (2 days): neutral
    Well-rested (3+ days): small boost
    """
    if days_rest is None:
        return 0.0
    if days_rest <= 1:
        return BACK_TO_BACK_PENALTY
    if days_rest >= 3:
        return WELL_RESTED_BOOST
    return 0.0


# ---------------------------------------------------------------------------
# TOI weighting (#4)
# ---------------------------------------------------------------------------

def _toi_confidence_modifier(game_stats: List[GamePlayerStats]) -> float:
    """Compute a confidence modifier based on time-on-ice patterns.

    High, consistent TOI → slight boost.
    Low or volatile TOI → penalty.
    Returns a value typically in [-0.10, +0.05].
    """
    toi_values = [g.toi for g in game_stats if g.toi is not None]
    if len(toi_values) < 3:
        return 0.0

    avg_toi = sum(toi_values) / len(toi_values)
    toi_std = math.sqrt(
        sum((t - avg_toi) ** 2 for t in toi_values) / len(toi_values)
    )

    modifier = 0.0

    # Low average TOI → likely lower-line player
    if avg_toi < LOW_TOI_THRESHOLD:
        modifier -= 0.05
    elif avg_toi >= HIGH_TOI_THRESHOLD:
        modifier += 0.03

    # High TOI variance → unpredictable role/usage
    if toi_std > 3.0:
        modifier -= min(TOI_VARIANCE_PENALTY * (toi_std - 3.0), 0.10)

    # Check recent trend (last 3 vs overall)
    recent_toi = toi_values[:3]
    if recent_toi:
        recent_avg = sum(recent_toi) / len(recent_toi)
        if recent_avg < avg_toi * 0.85:
            # TOI declining — player may be losing minutes
            modifier -= 0.04

    return modifier


# ---------------------------------------------------------------------------
# Opponent defensive quality (#5)
# ---------------------------------------------------------------------------

async def _get_opponent_defensive_factor(
    session: AsyncSession,
    opponent_team_id: int,
    market: str,
    season: str,
    opp_defense_cache: Dict[tuple, float],
) -> float:
    """Compute how the opponent's defense compares to league average.

    Returns a factor: >1.0 = weak defense (good for props), <1.0 = strong.
    Uses goals_against_per_game for scoring props, shots_against_per_game
    for SOG props.
    """
    cache_key = (opponent_team_id, market)
    if cache_key in opp_defense_cache:
        return opp_defense_cache[cache_key]

    result = await session.execute(
        select(TeamStats).where(
            TeamStats.team_id == opponent_team_id,
            TeamStats.season == season,
        )
    )
    ts = result.scalar_one_or_none()

    factor = 1.0
    if ts:
        if market == "player_shots_on_goal" and ts.shots_against_per_game:
            # More shots allowed = weaker defense for SOG props
            factor = ts.shots_against_per_game / LEAGUE_AVG_SA_PG
        elif market == "player_total_saves" and ts.shots_for_per_game:
            # More shots by opponent = more saves needed
            factor = ts.shots_for_per_game / LEAGUE_AVG_SA_PG
        elif ts.goals_against_per_game:
            # Scoring props: goals, points, assists
            factor = ts.goals_against_per_game / LEAGUE_AVG_GA_PG

        # Cap the adjustment
        factor = max(1.0 - MAX_OPP_ADJUSTMENT, min(1.0 + MAX_OPP_ADJUSTMENT, factor))

    opp_defense_cache[cache_key] = factor
    return factor


# ---------------------------------------------------------------------------
# Matchup system (#7 — leverage PlayerMatchupStats when available)
# ---------------------------------------------------------------------------

def _compute_matchup_adjustment(
    overall_rate: float,
    vs_opponent_stats: list,
    stat_extractor,
    opponent_team_id: int,
    opponent_abbrev: Optional[str],
) -> Optional[MatchupContext]:
    """Compute a matchup adjustment from opponent-specific game stats.

    Compares the player's per-game rate against this specific opponent
    to their overall rate. The adjustment is scaled by sample size —
    more games vs the opponent means we trust the signal more.

    Returns None if insufficient opponent data.
    """
    if len(vs_opponent_stats) < MIN_MATCHUP_GAMES:
        return None

    vs_values = [stat_extractor(g) for g in vs_opponent_stats]
    vs_rate = sum(vs_values) / len(vs_values)
    vs_games = len(vs_values)

    if overall_rate <= 0:
        return None

    # Raw deviation: how much does the player over/under-perform vs this team?
    raw_deviation = (vs_rate - overall_rate) / overall_rate

    # Scale by sample size confidence: ramps from 0.3 at 3 games to 1.0 at 10+
    sample_weight = min(vs_games / 10.0, 1.0)
    adjustment = raw_deviation * sample_weight

    # Cap to prevent extreme swings
    adjustment = max(-MAX_MATCHUP_ADJUSTMENT, min(MAX_MATCHUP_ADJUSTMENT, adjustment))

    opp_label = opponent_abbrev or f"team {opponent_team_id}"
    direction = "+" if raw_deviation >= 0 else ""

    return MatchupContext(
        opponent_team_id=opponent_team_id,
        opponent_abbrev=opponent_abbrev,
        vs_opponent_rate=round(vs_rate, 2),
        vs_opponent_games=vs_games,
        overall_rate=round(overall_rate, 2),
        adjustment=round(adjustment, 4),
        reasoning=(
            f"vs {opp_label}: {vs_rate:.2f}/game over {vs_games} games "
            f"({direction}{raw_deviation:.0%} vs overall {overall_rate:.2f})"
        ),
    )


async def _get_matchup_from_cache(
    session: AsyncSession,
    player_id: int,
    opponent_team_id: int,
    season: str,
    stat_field: str,
) -> Optional[MatchupContext]:
    """Try to build matchup from the pre-computed PlayerMatchupStats table.

    Falls back to None if no cached data exists, letting the caller
    use the live query path instead.
    """
    result = await session.execute(
        select(PlayerMatchupStats).where(
            PlayerMatchupStats.player_id == player_id,
            PlayerMatchupStats.opponent_team_id == opponent_team_id,
            PlayerMatchupStats.season == season,
        )
    )
    pms = result.scalar_one_or_none()
    if not pms or pms.games_played < MIN_MATCHUP_GAMES:
        return None

    # Map stat field to the PlayerMatchupStats columns
    if stat_field == "goals" and pms.gpg is not None and pms.overall_gpg:
        vs_rate = pms.gpg
        overall_rate = pms.overall_gpg
    elif stat_field == "points" and pms.ppg is not None and pms.overall_ppg:
        vs_rate = pms.ppg
        overall_rate = pms.overall_ppg
    elif stat_field == "shots" and pms.shots and pms.games_played:
        vs_rate = pms.shots / pms.games_played
        overall_rate = None  # Not stored — fall back to live query
    else:
        return None

    if overall_rate is None or overall_rate <= 0:
        return None

    raw_deviation = (vs_rate - overall_rate) / overall_rate
    sample_weight = min(pms.games_played / 10.0, 1.0)
    adjustment = raw_deviation * sample_weight
    adjustment = max(-MAX_MATCHUP_ADJUSTMENT, min(MAX_MATCHUP_ADJUSTMENT, adjustment))

    opp_team_result = await session.execute(
        select(Team.abbreviation).where(Team.id == opponent_team_id)
    )
    opp_abbrev = opp_team_result.scalar_one_or_none() or f"team {opponent_team_id}"
    direction = "+" if raw_deviation >= 0 else ""

    return MatchupContext(
        opponent_team_id=opponent_team_id,
        opponent_abbrev=opp_abbrev,
        vs_opponent_rate=round(vs_rate, 2),
        vs_opponent_games=pms.games_played,
        overall_rate=round(overall_rate, 2),
        adjustment=round(adjustment, 4),
        reasoning=(
            f"vs {opp_abbrev}: {vs_rate:.2f}/game over {pms.games_played} games "
            f"({direction}{raw_deviation:.0%} vs overall {overall_rate:.2f})"
        ),
    )


async def _get_opponent_abbrev(
    session: AsyncSession,
    team_id: int,
    team_abbrev_cache: Dict[int, str],
) -> Optional[str]:
    """Look up a team's abbreviation, with caching."""
    if team_id in team_abbrev_cache:
        return team_abbrev_cache[team_id]
    result = await session.execute(
        select(Team.abbreviation).where(Team.id == team_id)
    )
    row = result.scalar_one_or_none()
    if row:
        team_abbrev_cache[team_id] = row
    return row


async def _build_skater_matchup(
    session: AsyncSession,
    player_id: int,
    team_id: int,
    opponent_team_id: int,
    opponent_abbrev: Optional[str],
    overall_stats: List[GamePlayerStats],
    market: str,
    stat_extractor,
    season: str,
    stat_field: str,
    matchup_stats_cache: Dict[tuple, Optional[MatchupContext]],
) -> Optional[MatchupContext]:
    """Build matchup context for a skater prop.

    First tries the pre-computed PlayerMatchupStats table (#7),
    then falls back to a live game-by-game query.
    """
    cache_key = (player_id, stat_field)
    if cache_key in matchup_stats_cache:
        return matchup_stats_cache[cache_key]

    # Try cached PlayerMatchupStats first
    ctx = await _get_matchup_from_cache(
        session, player_id, opponent_team_id, season, stat_field,
    )
    if ctx is not None:
        matchup_stats_cache[cache_key] = ctx
        return ctx

    # Fall back to live query
    vs_stats = await _get_skater_vs_opponent_stats(
        session, player_id, team_id, opponent_team_id,
    )
    if len(vs_stats) < MIN_MATCHUP_GAMES:
        matchup_stats_cache[cache_key] = None
        return None

    overall_rate = (
        sum(stat_extractor(g) for g in overall_stats) / len(overall_stats)
        if overall_stats else 0
    )
    ctx = _compute_matchup_adjustment(
        overall_rate, vs_stats, stat_extractor,
        opponent_team_id, opponent_abbrev,
    )
    matchup_stats_cache[cache_key] = ctx
    return ctx


async def _build_goalie_matchup(
    session: AsyncSession,
    player_id: int,
    opponent_team_id: int,
    opponent_abbrev: Optional[str],
    overall_stats: List[GameGoalieStats],
    stat_extractor,
) -> Optional[MatchupContext]:
    """Build matchup context for a goalie prop by querying games vs opponent."""
    vs_stats = await _get_goalie_vs_opponent_stats(
        session, player_id, opponent_team_id,
    )
    if len(vs_stats) < MIN_MATCHUP_GAMES:
        return None

    overall_rate = (
        sum(stat_extractor(g) for g in overall_stats) / len(overall_stats)
        if overall_stats else 0
    )
    return _compute_matchup_adjustment(
        overall_rate, vs_stats, stat_extractor,
        opponent_team_id, opponent_abbrev,
    )


# ---------------------------------------------------------------------------
# Line movement (#8)
# ---------------------------------------------------------------------------

def _check_line_movement(prop: PlayerPropOdds, pick_side: str) -> tuple[bool, float]:
    """Check if the line has moved against our pick since opening.

    Returns (moved_against: bool, movement_pts: float).
    movement_pts is signed: positive = line moved in our favor,
    negative = moved against us.
    """
    if pick_side == "over" or pick_side == "yes":
        opening = prop.opening_over_price
        current = prop.over_price
    else:
        opening = prop.opening_under_price
        current = prop.under_price

    if opening is None or current is None:
        return (False, 0.0)

    # Movement: more negative = worse for bettor (higher juice)
    # e.g. opening -130 → current -150 = moved against us by 20 pts
    movement = current - opening  # for negative odds, more negative = worse

    # Normalize: for negative odds, a decrease is bad. For positive, a decrease is bad.
    if opening < 0:
        # e.g. -130 → -150: movement = -20, which is bad (against)
        moved_against = movement < -SHARP_MOVE_THRESHOLD
    else:
        # e.g. +150 → +130: movement = -20, which is bad (against)
        moved_against = movement < -SHARP_MOVE_THRESHOLD

    return (moved_against, movement)


# ---------------------------------------------------------------------------
# Injury filtering (#2)
# ---------------------------------------------------------------------------

async def _get_injured_player_ids(
    session: AsyncSession,
    team_ids: List[int],
) -> Dict[int, str]:
    """Get player IDs with active injuries and their status.

    Returns {player_id: status} for players who are out, on IR,
    or day-to-day.
    """
    result = await session.execute(
        select(InjuryReport.player_id, InjuryReport.status).where(
            InjuryReport.team_id.in_(team_ids),
            InjuryReport.active == True,
        )
    )
    return {row[0]: row[1] for row in result.all()}


# ---------------------------------------------------------------------------
# Enhanced confidence model (#6)
# ---------------------------------------------------------------------------

def _compute_confidence(
    model_prob: float,
    values: list,
    avg_rate: float,
    games_sampled: int,
    toi_modifier: float = 0.0,
    rest_adj: float = 0.0,
    location_adj: float = 0.0,
    opp_factor: float = 1.0,
    line_moved_against: bool = False,
    injury_status: Optional[str] = None,
) -> float:
    """Compute confidence using an additive model with multiple factors.

    Unlike the old multiplicative form_factor approach, this properly
    accounts for variance, sample size, and contextual factors.
    """
    # Start with the raw model probability as base
    base = model_prob

    # 1. Recent form adjustment (last 5 games vs overall)
    if values and len(values) >= 5:
        recent_5 = values[:5]
        recent_avg = sum(recent_5) / len(recent_5)
        if avg_rate > 0:
            form_deviation = (recent_avg - avg_rate) / max(avg_rate, 0.1)
            # Additive: ±5% max from form
            form_adj = max(-0.05, min(0.05, form_deviation * 0.15))
            base += form_adj

    # 2. Sample size — more games = more confidence
    if games_sampled >= 12:
        base += 0.02
    elif games_sampled <= 6:
        base -= 0.03

    # 3. Variance penalty — high variance in recent stats = less predictable
    if values and len(values) >= 5:
        mean_val = sum(values) / len(values)
        variance = sum((v - mean_val) ** 2 for v in values) / len(values)
        cv = math.sqrt(variance) / max(mean_val, 0.1)  # coefficient of variation
        if cv > 0.8:
            base -= 0.04  # High variance stat
        elif cv < 0.3:
            base += 0.02  # Very consistent

    # 4. TOI modifier (skaters only, 0 for goalies)
    base += toi_modifier

    # 5. Rest/fatigue
    base += rest_adj * 0.3  # Scale down — rest is secondary signal

    # 6. Location (home/away)
    base += location_adj * 0.2  # Scale down

    # 7. Opponent quality — strong opponent reduces confidence
    if opp_factor < 0.95:
        base -= 0.02  # Strong defense
    elif opp_factor > 1.05:
        base += 0.02  # Weak defense

    # 8. Line movement against us — sharp money signal
    if line_moved_against:
        base -= 0.04

    # 9. Injury concern — day-to-day/questionable still generates
    # but with reduced confidence
    if injury_status in ("day-to-day", "questionable", "probable"):
        base -= 0.05

    return min(max(base, 0.0), 1.0)


# ---------------------------------------------------------------------------
# Supplementary stats context (#9)
# ---------------------------------------------------------------------------

def _build_supplementary_reasoning(
    game_stats: List[GamePlayerStats],
    market: str,
) -> str:
    """Build extra reasoning from underused stat fields.

    Adds context about PP production, physical play, etc.
    """
    if not game_stats:
        return ""

    games = len(game_stats)
    parts = []

    # Power play production
    total_pp_goals = sum(g.pp_goals for g in game_stats)
    if total_pp_goals > 0:
        pp_rate = total_pp_goals / games
        parts.append(f"PP goals: {pp_rate:.2f}/game")

    # For scoring/shooting props, hits and blocks show engagement
    if market in ("player_shots_on_goal", "player_points", "player_goal_scorer_anytime"):
        total_hits = sum(g.hits for g in game_stats)
        if total_hits > 0:
            hit_rate = total_hits / games
            if hit_rate >= 2.0:
                parts.append(f"physical ({hit_rate:.1f} hits/game)")

        total_blocks = sum(g.blocked_shots for g in game_stats)
        if total_blocks > 0:
            block_rate = total_blocks / games
            if block_rate >= 1.5:
                parts.append(f"active defensively ({block_rate:.1f} blocks/game)")

    if not parts:
        return ""
    return " | " + ", ".join(parts)


# ---------------------------------------------------------------------------
# Analysis functions (enhanced with all factors)
# ---------------------------------------------------------------------------

def _analyze_atg(
    player_name: str,
    player_id: Optional[int],
    prop: PlayerPropOdds,
    game_stats: List[GamePlayerStats],
    matchup: Optional[MatchupContext] = None,
    ctx: Optional[GameContext] = None,
    injury_status: Optional[str] = None,
    player_team_id: int = 0,
) -> Optional[PropPick]:
    """Analyze anytime goal scorer prop.

    Uses Poisson model: P(goals >= 1) = 1 - e^(-avg_goals_per_game).
    Adjusts the goal rate based on matchup, home/away, rest, and
    opponent defensive quality.
    """
    if not game_stats or len(game_stats) < MIN_GAMES:
        return None

    ctx = ctx or GameContext()

    total_goals = sum(g.goals for g in game_stats)
    games = len(game_stats)
    avg_goals = total_goals / games

    # Apply all rate adjustments
    adjusted_goals = avg_goals

    # Matchup adjustment
    if matchup and matchup.adjustment != 0:
        adjusted_goals *= (1.0 + matchup.adjustment)

    # Home/away adjustment (#1)
    location_adj = _compute_location_adjustment(
        game_stats, lambda g: g.goals, player_team_id, ctx.is_home,
    )
    adjusted_goals *= (1.0 + location_adj)

    # Rest adjustment (#3)
    rest_adj = _rest_adjustment(ctx.days_rest)
    adjusted_goals *= (1.0 + rest_adj)

    # Opponent defensive quality (#5)
    adjusted_goals *= ctx.opp_defensive_factor

    adjusted_goals = max(adjusted_goals, 0.0)

    model_prob = _poisson_at_least_one(adjusted_goals)

    # Apply calibration shrinkage — Poisson models structurally overestimate
    # player prop probabilities just like game totals/spreads.
    model_prob = _calibrate_prop_prob(model_prob)

    # Raw implied probability is what you bet against (includes vig).
    implied = _american_to_implied(prop.over_price)
    if implied is None or implied <= 0:
        return None

    edge = model_prob - implied

    # Use vig-free probability ONLY for juice filtering.
    true_over, _ = _remove_vig(prop.over_price, prop.under_price)
    juice_check = true_over if true_over is not None else implied
    if juice_check > MAX_PROP_IMPLIED:
        return None

    # Line movement check (#8)
    line_moved_against, line_movement = _check_line_movement(prop, "yes")

    # Enhanced confidence (#6)
    goal_values = [g.goals for g in game_stats]
    toi_mod = _toi_confidence_modifier(game_stats)
    confidence = _compute_confidence(
        model_prob, goal_values, avg_goals, games,
        toi_modifier=toi_mod,
        rest_adj=rest_adj,
        location_adj=location_adj,
        opp_factor=ctx.opp_defensive_factor,
        line_moved_against=line_moved_against,
        injury_status=injury_status,
    )

    # Only recommend if sufficient edge and confidence
    if edge < MIN_PICK_EDGE or confidence < MIN_PICK_CONFIDENCE:
        return None

    goals_in_last = sum(1 for g in game_stats if g.goals >= 1)
    hit_rate = goals_in_last / games

    # Build reasoning with all factors
    reasoning = (
        f"{avg_goals:.2f} goals/game over {games} games "
        f"({hit_rate:.0%} scored in). "
        f"Model: {model_prob:.1%} vs line {implied:.1%}"
    )
    if matchup and matchup.adjustment != 0:
        reasoning += f" | Matchup: {matchup.reasoning}"
    # Home/away context
    reasoning += f" | {'At home' if ctx.is_home else 'On the road'}"
    # Rest context
    if ctx.days_rest is not None:
        if ctx.days_rest <= 1:
            reasoning += " | B2B fatigue"
        elif ctx.days_rest >= 3:
            reasoning += f" | {ctx.days_rest} days rest"
    # Opponent defense
    if ctx.opp_defensive_factor > 1.05:
        pct_above = (ctx.opp_defensive_factor - 1.0) * 100
        reasoning += f" | Opp {ctx.opp_abbrev} allows {pct_above:.0f}% more goals than avg"
    elif ctx.opp_defensive_factor < 0.95:
        pct_below = (1.0 - ctx.opp_defensive_factor) * 100
        reasoning += f" | Opp {ctx.opp_abbrev} allows {pct_below:.0f}% fewer goals than avg"
    # Line movement
    if line_moved_against:
        reasoning += " | Line moved against"
    # Supplementary stats (#9)
    reasoning += _build_supplementary_reasoning(game_stats, "player_goal_scorer_anytime")

    return PropPick(
        player_name=player_name,
        player_id=player_id,
        market="player_goal_scorer_anytime",
        pick_side="yes",
        line=None,
        odds=prop.over_price,
        model_prob=model_prob,
        implied_prob=implied,
        edge=edge,
        confidence=confidence,
        avg_rate=avg_goals,
        games_sampled=games,
        reasoning=reasoning,
        matchup=matchup,
    )


def _analyze_over_under(
    player_name: str,
    player_id: Optional[int],
    prop: PlayerPropOdds,
    game_stats: list,
    market: str,
    stat_extractor,
    matchup: Optional[MatchupContext] = None,
    ctx: Optional[GameContext] = None,
    injury_status: Optional[str] = None,
    player_team_id: int = 0,
    is_goalie: bool = False,
) -> Optional[PropPick]:
    """Analyze an over/under prop (SOG, Points, Assists, Saves).

    Uses Poisson model for low-count stats (goals, assists, shots, points)
    and normal distribution for high-count stats (saves) where Poisson
    dramatically overstates edge.

    Integrates matchup, home/away, rest, opponent quality, TOI, and
    line movement adjustments.
    """
    if not game_stats or len(game_stats) < MIN_GAMES:
        return None
    if prop.line is None:
        return None

    ctx = ctx or GameContext()

    values = [stat_extractor(g) for g in game_stats]
    games = len(values)
    avg_rate = sum(values) / games

    # Apply all rate adjustments
    adjusted_rate = avg_rate

    # Matchup adjustment
    if matchup and matchup.adjustment != 0:
        adjusted_rate *= (1.0 + matchup.adjustment)

    # Home/away adjustment (#1)
    location_adj = _compute_location_adjustment(
        game_stats, stat_extractor, player_team_id, ctx.is_home,
    )
    adjusted_rate *= (1.0 + location_adj)

    # Rest adjustment (#3)
    rest_adj = _rest_adjustment(ctx.days_rest)
    adjusted_rate *= (1.0 + rest_adj)

    # Opponent defensive quality (#5)
    adjusted_rate *= ctx.opp_defensive_factor

    adjusted_rate = max(adjusted_rate, 0.0)

    # Use normal distribution for high-count stats (saves avg 25-35/game).
    if market == "player_total_saves":
        shift = adjusted_rate - avg_rate
        adjusted_values = [v + shift for v in values]
        p_over = _normal_over(adjusted_values, prop.line)
    else:
        p_over = _poisson_over(adjusted_rate, prop.line)
    p_under = 1.0 - p_over

    # Apply calibration shrinkage before computing edge — Poisson/Normal
    # models structurally overestimate player prop probabilities.
    p_over_cal = _calibrate_prop_prob(p_over)
    p_under_cal = _calibrate_prop_prob(p_under)

    implied_over = _american_to_implied(prop.over_price)
    implied_under = _american_to_implied(prop.under_price)

    # Compare both sides and pick the one with more edge
    best_side = None
    best_edge = -1.0
    best_model_prob = 0.0
    best_implied = 0.0
    best_odds = None

    if implied_over and implied_over > 0:
        over_edge = p_over_cal - implied_over
        if over_edge > best_edge:
            best_side = "over"
            best_edge = over_edge
            best_model_prob = p_over_cal
            best_implied = implied_over
            best_odds = prop.over_price

    if implied_under and implied_under > 0:
        under_edge = p_under_cal - implied_under
        if under_edge > best_edge:
            best_side = "under"
            best_edge = under_edge
            best_model_prob = p_under_cal
            best_implied = implied_under
            best_odds = prop.under_price

    if best_side is None or best_edge < MIN_PICK_EDGE:
        return None

    # Use vig-free probability for juice filtering only.
    true_over, true_under = _remove_vig(prop.over_price, prop.under_price)
    juice_implied = (
        true_over if best_side == "over" and true_over is not None
        else true_under if best_side == "under" and true_under is not None
        else best_implied
    )
    if juice_implied > MAX_PROP_IMPLIED:
        return None

    # Line movement check (#8)
    line_moved_against, line_movement = _check_line_movement(prop, best_side)

    # Enhanced confidence (#6)
    toi_mod = 0.0
    if not is_goalie and isinstance(game_stats[0], GamePlayerStats):
        toi_mod = _toi_confidence_modifier(game_stats)

    confidence = _compute_confidence(
        best_model_prob, values, avg_rate, games,
        toi_modifier=toi_mod,
        rest_adj=rest_adj,
        location_adj=location_adj,
        opp_factor=ctx.opp_defensive_factor,
        line_moved_against=line_moved_against,
        injury_status=injury_status,
    )

    if confidence < MIN_PICK_CONFIDENCE:
        return None

    # Count how many times they went over the line
    over_count = sum(1 for v in values if v > prop.line)
    hit_pct = over_count / games

    stat_label = {
        "player_shots_on_goal": "SOG",
        "player_points": "points",
        "player_assists": "assists",
        "player_total_saves": "saves",
    }.get(market, "stat")

    # Build reasoning with all factors
    reasoning = (
        f"{avg_rate:.1f} {stat_label}/game over {games} games "
        f"(over {prop.line} in {hit_pct:.0%}). "
        f"Model: {best_model_prob:.1%} vs line {best_implied:.1%}"
    )
    if matchup and matchup.adjustment != 0:
        reasoning += f" | Matchup: {matchup.reasoning}"
    reasoning += f" | {'At home' if ctx.is_home else 'On the road'}"
    if ctx.days_rest is not None:
        if ctx.days_rest <= 1:
            reasoning += " | B2B fatigue"
        elif ctx.days_rest >= 3:
            reasoning += f" | {ctx.days_rest} days rest"
    if ctx.opp_defensive_factor > 1.05:
        pct_above = (ctx.opp_defensive_factor - 1.0) * 100
        defense_stat = "shots" if market == "player_shots_on_goal" else "goals"
        if market == "player_total_saves":
            reasoning += f" | Opp {ctx.opp_abbrev} generates {pct_above:.0f}% more shots than avg"
        else:
            reasoning += f" | Opp {ctx.opp_abbrev} allows {pct_above:.0f}% more {defense_stat} than avg"
    elif ctx.opp_defensive_factor < 0.95:
        pct_below = (1.0 - ctx.opp_defensive_factor) * 100
        defense_stat = "shots" if market == "player_shots_on_goal" else "goals"
        if market == "player_total_saves":
            reasoning += f" | Opp {ctx.opp_abbrev} generates {pct_below:.0f}% fewer shots than avg"
        else:
            reasoning += f" | Opp {ctx.opp_abbrev} allows {pct_below:.0f}% fewer {defense_stat} than avg"
    if line_moved_against:
        reasoning += " | Line moved against"
    # Supplementary stats (#9) for skaters only
    if not is_goalie and isinstance(game_stats[0], GamePlayerStats):
        reasoning += _build_supplementary_reasoning(game_stats, market)

    return PropPick(
        player_name=player_name,
        player_id=player_id,
        market=market,
        pick_side=best_side,
        line=prop.line,
        odds=best_odds,
        model_prob=best_model_prob,
        implied_prob=best_implied,
        edge=best_edge,
        confidence=confidence,
        avg_rate=avg_rate,
        games_sampled=games,
        reasoning=reasoning,
        matchup=matchup,
    )


# ---------------------------------------------------------------------------
# Main generation pipeline
# ---------------------------------------------------------------------------

# Position groups for player disambiguation
_FORWARD_POSITIONS = {"C", "LW", "RW", "F"}
_SKATER_POSITIONS = {"C", "LW", "RW", "F", "D"}


def _disambiguate_player(candidates: List[Player], market: str) -> Player:
    """Pick the most likely player when multiple share the same name.

    For scoring/shooting props (goals, points, assists, SOG), prefer
    forwards over defensemen — sportsbooks almost always mean the
    higher-scoring forward when the name is ambiguous.

    For saves props, prefer goalies.

    As a final tiebreaker, prefer the player with the lower jersey
    number or higher external_id (typically the more established player
    who was registered first).
    """
    if market == "player_total_saves":
        goalies = [p for p in candidates if (p.position or "").upper() == "G"]
        if goalies:
            return goalies[0]

    # Scoring / shooting props — prefer forwards over D
    if market in (
        "player_goal_scorer_anytime",
        "player_points",
        "player_assists",
        "player_shots_on_goal",
    ):
        forwards = [
            p for p in candidates
            if (p.position or "").upper() in _FORWARD_POSITIONS
        ]
        if forwards:
            return forwards[0]
        skaters = [
            p for p in candidates
            if (p.position or "").upper() in _SKATER_POSITIONS
        ]
        if skaters:
            return skaters[0]

    # Fallback: return the first candidate (arbitrary but stable)
    return candidates[0]


async def generate_prop_picks(
    session: AsyncSession,
    game_id: int,
) -> List[PropPick]:
    """Generate player prop picks for a specific game.

    Reads the PlayerPropOdds for the game, matches players to their
    historical game stats, computes opponent-specific matchup adjustments,
    and runs the analysis with all contextual factors.

    Returns a list of PropPick objects sorted by edge (best first).
    """
    # Get all prop odds for this game
    props_result = await session.execute(
        select(PlayerPropOdds).where(PlayerPropOdds.game_id == game_id)
    )
    all_props = props_result.scalars().all()
    if not all_props:
        return []

    # Get the game for team info
    game_result = await session.execute(
        select(Game).where(Game.id == game_id)
    )
    game = game_result.scalar_one_or_none()
    if not game:
        return []

    # Build player name → Player lookup for both teams
    team_ids = [
        tid for tid in [game.home_team_id, game.away_team_id] if tid
    ]
    if not team_ids:
        return []

    players_result = await session.execute(
        select(Player).where(
            Player.team_id.in_(team_ids),
            Player.active == True,
        )
    )
    all_players = players_result.scalars().all()

    # Build lookup by player ID (most reliable) and by name
    player_by_id: Dict[int, Player] = {p.id: p for p in all_players}
    # For name-based lookup, track duplicates so we can disambiguate
    player_by_name: Dict[str, List[Player]] = {}
    for p in all_players:
        key = p.name.lower()
        player_by_name.setdefault(key, []).append(p)
        parts = p.name.split()
        if len(parts) >= 2:
            last = parts[-1].lower()
            player_by_name.setdefault(last, []).append(p)

    # --- Injury filtering (#2) ---
    injured_players = await _get_injured_player_ids(session, team_ids)

    # Cache game stats per player to avoid duplicate queries
    skater_stats_cache: Dict[int, List[GamePlayerStats]] = {}
    goalie_stats_cache: Dict[int, List[GameGoalieStats]] = {}
    # Cache matchup contexts per (player_id, stat_field)
    matchup_cache: Dict[tuple, Optional[MatchupContext]] = {}
    team_abbrev_cache: Dict[int, str] = {}
    # Opponent defensive quality cache
    opp_defense_cache: Dict[tuple, float] = {}

    picks: List[PropPick] = []

    for prop in all_props:
        # Match prop player to our Player record.
        # Prefer the explicit player_id FK when available (most reliable).
        player = None
        if prop.player_id and prop.player_id in player_by_id:
            player = player_by_id[prop.player_id]

        if player is None:
            # Fall back to name-based matching with disambiguation
            candidates = player_by_name.get(prop.player_name.lower(), [])
            if not candidates:
                # Try last name
                parts = prop.player_name.split()
                if len(parts) >= 2:
                    candidates = player_by_name.get(parts[-1].lower(), [])

            if len(candidates) == 1:
                player = candidates[0]
            elif len(candidates) > 1:
                # Disambiguate: for scoring/shooting props, prefer forwards
                # (C, LW, RW) over defensemen (D). For save props, prefer G.
                player = _disambiguate_player(candidates, prop.market)

        if player is None:
            continue

        # --- Injury check (#2) ---
        player_injury = injured_players.get(player.id)
        if player_injury and player_injury.lower() in ("out", "ir"):
            # Skip players who are definitely out
            continue
        # day-to-day, questionable, probable still generate but with
        # reduced confidence (handled in _compute_confidence)

        # Determine the opponent team for this player
        player_team_id = player.team_id or 0
        is_home = player_team_id == game.home_team_id
        if player_team_id == game.home_team_id:
            opponent_team_id = game.away_team_id
        elif player_team_id == game.away_team_id:
            opponent_team_id = game.home_team_id
        else:
            opponent_team_id = None

        opponent_abbrev = None
        if opponent_team_id:
            opponent_abbrev = await _get_opponent_abbrev(
                session, opponent_team_id, team_abbrev_cache,
            )

        market = prop.market

        # --- Shared context computation ---
        # Opponent defensive factor (#5)
        opp_factor = 1.0
        if opponent_team_id:
            opp_factor = await _get_opponent_defensive_factor(
                session, opponent_team_id, market,
                game.season, opp_defense_cache,
            )

        if market == "player_goal_scorer_anytime":
            # ATG — skater stat
            if player.id not in skater_stats_cache:
                skater_stats_cache[player.id] = await _get_skater_game_stats(
                    session, player.id, player_team_id,
                    exclude_game_id=game_id,
                )
            stats = skater_stats_cache[player.id]

            # Rest days (#3)
            days_rest = _compute_rest_days(stats, game.date)

            # Game context
            game_ctx = GameContext(
                is_home=is_home,
                days_rest=days_rest,
                is_back_to_back=(days_rest is not None and days_rest <= 1),
                opp_defensive_factor=opp_factor,
                opp_abbrev=opponent_abbrev,
            )

            # Matchup (#7 — try cached first)
            matchup = None
            if opponent_team_id and stats:
                matchup = await _build_skater_matchup(
                    session, player.id, player_team_id, opponent_team_id,
                    opponent_abbrev, stats, market, lambda g: g.goals,
                    game.season, "goals", matchup_cache,
                )

            pick = _analyze_atg(
                prop.player_name, player.id, prop, stats, matchup,
                ctx=game_ctx, injury_status=player_injury,
                player_team_id=player_team_id,
            )
            if pick:
                picks.append(pick)

        elif market == "player_shots_on_goal":
            if player.id not in skater_stats_cache:
                skater_stats_cache[player.id] = await _get_skater_game_stats(
                    session, player.id, player_team_id,
                    exclude_game_id=game_id,
                )
            stats = skater_stats_cache[player.id]

            days_rest = _compute_rest_days(stats, game.date)
            game_ctx = GameContext(
                is_home=is_home,
                days_rest=days_rest,
                is_back_to_back=(days_rest is not None and days_rest <= 1),
                opp_defensive_factor=opp_factor,
                opp_abbrev=opponent_abbrev,
            )

            matchup = None
            if opponent_team_id and stats:
                matchup = await _build_skater_matchup(
                    session, player.id, player_team_id, opponent_team_id,
                    opponent_abbrev, stats, market, lambda g: g.shots,
                    game.season, "shots", matchup_cache,
                )

            pick = _analyze_over_under(
                prop.player_name, player.id, prop, stats, market,
                lambda g: g.shots, matchup,
                ctx=game_ctx, injury_status=player_injury,
                player_team_id=player_team_id,
            )
            if pick:
                picks.append(pick)

        elif market == "player_points":
            if player.id not in skater_stats_cache:
                skater_stats_cache[player.id] = await _get_skater_game_stats(
                    session, player.id, player_team_id,
                    exclude_game_id=game_id,
                )
            stats = skater_stats_cache[player.id]

            days_rest = _compute_rest_days(stats, game.date)
            game_ctx = GameContext(
                is_home=is_home,
                days_rest=days_rest,
                is_back_to_back=(days_rest is not None and days_rest <= 1),
                opp_defensive_factor=opp_factor,
                opp_abbrev=opponent_abbrev,
            )

            matchup = None
            if opponent_team_id and stats:
                matchup = await _build_skater_matchup(
                    session, player.id, player_team_id, opponent_team_id,
                    opponent_abbrev, stats, market, lambda g: g.points,
                    game.season, "points", matchup_cache,
                )

            pick = _analyze_over_under(
                prop.player_name, player.id, prop, stats, market,
                lambda g: g.points, matchup,
                ctx=game_ctx, injury_status=player_injury,
                player_team_id=player_team_id,
            )
            if pick:
                picks.append(pick)

        elif market == "player_assists":
            if player.id not in skater_stats_cache:
                skater_stats_cache[player.id] = await _get_skater_game_stats(
                    session, player.id, player_team_id,
                    exclude_game_id=game_id,
                )
            stats = skater_stats_cache[player.id]

            days_rest = _compute_rest_days(stats, game.date)
            game_ctx = GameContext(
                is_home=is_home,
                days_rest=days_rest,
                is_back_to_back=(days_rest is not None and days_rest <= 1),
                opp_defensive_factor=opp_factor,
                opp_abbrev=opponent_abbrev,
            )

            matchup = None
            if opponent_team_id and stats:
                matchup = await _build_skater_matchup(
                    session, player.id, player_team_id, opponent_team_id,
                    opponent_abbrev, stats, market, lambda g: g.assists,
                    game.season, "assists", matchup_cache,
                )

            pick = _analyze_over_under(
                prop.player_name, player.id, prop, stats, market,
                lambda g: g.assists, matchup,
                ctx=game_ctx, injury_status=player_injury,
                player_team_id=player_team_id,
            )
            if pick:
                picks.append(pick)

        elif market == "player_total_saves":
            if player.id not in goalie_stats_cache:
                goalie_stats_cache[player.id] = await _get_goalie_game_stats(
                    session, player.id,
                    exclude_game_id=game_id,
                )
            stats = goalie_stats_cache[player.id]

            days_rest = _compute_rest_days(stats, game.date)
            game_ctx = GameContext(
                is_home=is_home,
                days_rest=days_rest,
                is_back_to_back=(days_rest is not None and days_rest <= 1),
                opp_defensive_factor=opp_factor,
                opp_abbrev=opponent_abbrev,
            )

            matchup = None
            if opponent_team_id and stats:
                matchup = await _build_goalie_matchup(
                    session, player.id, opponent_team_id,
                    opponent_abbrev, stats, lambda g: g.saves,
                )

            pick = _analyze_over_under(
                prop.player_name, player.id, prop, stats, market,
                lambda g: g.saves, matchup,
                ctx=game_ctx, injury_status=player_injury,
                player_team_id=player_team_id,
                is_goalie=True,
            )
            if pick:
                picks.append(pick)

    # Sort by edge descending and cap to best picks per game
    picks.sort(key=lambda p: p.edge, reverse=True)
    return picks[:MAX_PICKS_PER_GAME]


async def generate_all_prop_picks(
    session: AsyncSession,
    game_ids: List[int],
) -> Dict[int, List[PropPick]]:
    """Generate player prop picks for multiple games.

    Returns {game_id: [PropPick, ...]} with picks sorted by edge.
    """
    result: Dict[int, List[PropPick]] = {}
    for gid in game_ids:
        picks = await generate_prop_picks(session, gid)
        if picks:
            result[gid] = picks
    return result
