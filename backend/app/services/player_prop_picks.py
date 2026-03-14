"""
Player prop prediction engine.

Analyzes player performance data against sportsbook prop lines
to identify value bets. Compares recent per-game rates (goals,
shots, points, assists, saves) to the prop line and implied
probability to calculate edge.

Supported markets:
  - player_goal_scorer_anytime (ATG): Poisson P(goals >= 1)
  - player_shots_on_goal (SOG): compare avg shots vs line
  - player_points: compare avg points vs line
  - player_assists: compare avg assists vs line
  - player_total_saves: compare avg saves vs line
"""

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.game import Game, GameGoalieStats, GamePlayerStats
from app.models.matchup import PlayerMatchupStats
from app.models.player import Player
from app.models.player_prop import PlayerPropOdds
from app.models.team import Team

logger = logging.getLogger(__name__)

# Number of recent games to use for per-game rate calculation
RECENT_GAMES_WINDOW = 15
# Minimum games required to generate a prediction
MIN_GAMES = 5
# Minimum games vs opponent to apply matchup adjustment
MIN_MATCHUP_GAMES = 3
# Minimum edge to consider a pick worth recommending
MIN_PICK_EDGE = 0.05
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


def _american_to_implied(american: Optional[float]) -> Optional[float]:
    """Convert American odds to implied probability (includes vig)."""
    if american is None:
        return None
    if american >= 100:
        return 100 / (american + 100)
    else:
        return abs(american) / (abs(american) + 100)


def _remove_vig(
    over_price: Optional[float], under_price: Optional[float]
) -> tuple[Optional[float], Optional[float]]:
    """Remove vig from over/under prices to get true probabilities.

    Sportsbooks bake in ~4-8% vig (overround) by making both sides
    sum to >100%.  To get the "true" probability, we normalize each
    side by the total implied probability.

    Example: Over -130 (56.5%) + Under +100 (50%) = 106.5%
             True over = 56.5/106.5 = 53.1%, true under = 50/106.5 = 46.9%
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
    # erfc(x) = 1 - erf(x), and P(X > line) = 0.5 * erfc((line - mean) / (std * sqrt(2)))
    z = (line - mean) / (std * math.sqrt(2))
    return 0.5 * math.erfc(z)


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


async def _get_skater_game_stats(
    session: AsyncSession,
    player_id: int,
    team_id: int,
) -> List[GamePlayerStats]:
    """Get recent game stats for a skater, ordered by most recent."""
    result = await session.execute(
        select(GamePlayerStats)
        .join(Game, Game.id == GamePlayerStats.game_id)
        .where(
            GamePlayerStats.player_id == player_id,
            Game.status == "final",
            Game.home_team_id.in_([team_id])
            | Game.away_team_id.in_([team_id]),
        )
        .order_by(desc(Game.date))
        .limit(RECENT_GAMES_WINDOW)
    )
    return list(result.scalars().all())


async def _get_goalie_game_stats(
    session: AsyncSession,
    player_id: int,
) -> List[GameGoalieStats]:
    """Get recent game stats for a goalie, ordered by most recent.

    Filters to completed games and meaningful appearances (>= 30 min TOI)
    to exclude relief appearances that would skew save averages.
    """
    result = await session.execute(
        select(GameGoalieStats)
        .join(Game, Game.id == GameGoalieStats.game_id)
        .where(
            GameGoalieStats.player_id == player_id,
            Game.status == "final",
            # Only include starts / full appearances (>= 30 min TOI).
            # Relief goalies with 5-10 min and a handful of saves
            # would badly skew the per-game average downward.
            (GameGoalieStats.toi >= 30.0) | (GameGoalieStats.toi.is_(None)),
        )
        .order_by(desc(Game.date))
        .limit(RECENT_GAMES_WINDOW)
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


def _analyze_atg(
    player_name: str,
    player_id: Optional[int],
    prop: PlayerPropOdds,
    game_stats: List[GamePlayerStats],
    matchup: Optional[MatchupContext] = None,
) -> Optional[PropPick]:
    """Analyze anytime goal scorer prop.

    Uses Poisson model: P(goals >= 1) = 1 - e^(-avg_goals_per_game).
    Adjusts the goal rate based on opponent-specific matchup data when available.
    """
    if not game_stats or len(game_stats) < MIN_GAMES:
        return None

    total_goals = sum(g.goals for g in game_stats)
    games = len(game_stats)
    avg_goals = total_goals / games

    # Apply matchup adjustment to the goal rate
    adjusted_goals = avg_goals
    if matchup and matchup.adjustment != 0:
        adjusted_goals = avg_goals * (1.0 + matchup.adjustment)
        adjusted_goals = max(adjusted_goals, 0.0)

    model_prob = _poisson_at_least_one(adjusted_goals)

    # Raw implied probability is what you bet against (includes vig).
    # Edge = model_prob - raw_implied.  This is the TRUE edge — how much
    # our model beats the actual line you'd bet.
    implied = _american_to_implied(prop.over_price)
    if implied is None or implied <= 0:
        return None

    edge = model_prob - implied

    # Use vig-free probability ONLY for juice filtering.
    # A prop at -210 raw (67.7%) might be 64% vig-free.  We filter on
    # vig-free so the juice check reflects the TRUE probability, not
    # the inflated sportsbook number.
    true_over, _ = _remove_vig(prop.over_price, prop.under_price)
    juice_check = true_over if true_over is not None else implied
    if juice_check > MAX_PROP_IMPLIED:
        return None

    # Weight recent games more heavily for confidence
    recent_5 = game_stats[:5]
    recent_goals = sum(g.goals for g in recent_5)
    recent_rate = recent_goals / len(recent_5) if recent_5 else 0
    form_factor = 1.0 + 0.2 * (recent_rate - avg_goals) / max(avg_goals, 0.1)
    confidence = min(max(model_prob * form_factor, 0.0), 1.0)

    # Only recommend if sufficient edge and confidence
    if edge < MIN_PICK_EDGE or confidence < MIN_PICK_CONFIDENCE:
        return None

    goals_in_last = sum(1 for g in game_stats if g.goals >= 1)
    hit_rate = goals_in_last / games

    reasoning = (
        f"{avg_goals:.2f} goals/game over {games} games "
        f"({hit_rate:.0%} scored in). "
        f"Model: {model_prob:.1%} vs line {implied:.1%}"
    )
    if matchup and matchup.adjustment != 0:
        reasoning += f" | Matchup: {matchup.reasoning}"

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
) -> Optional[PropPick]:
    """Analyze an over/under prop (SOG, Points, Assists, Saves).

    Uses Poisson model for low-count stats (goals, assists, shots, points)
    and normal distribution for high-count stats (saves) where Poisson
    dramatically overstates edge.

    When matchup data is available, adjusts the player's rate based on
    their historical performance against the specific opponent.
    """
    if not game_stats or len(game_stats) < MIN_GAMES:
        return None
    if prop.line is None:
        return None

    values = [stat_extractor(g) for g in game_stats]
    games = len(values)
    avg_rate = sum(values) / games

    # Apply matchup adjustment to the rate
    adjusted_rate = avg_rate
    if matchup and matchup.adjustment != 0:
        adjusted_rate = avg_rate * (1.0 + matchup.adjustment)
        adjusted_rate = max(adjusted_rate, 0.0)

    # Use normal distribution for high-count stats (saves avg 25-35/game).
    # Poisson assumes variance == mean, but saves have much higher variance
    # due to opponent shot volume, leading to wildly inflated edges.
    if market == "player_total_saves":
        # For saves with matchup adjustment, shift the values to reflect
        # the adjusted mean while preserving variance
        if matchup and matchup.adjustment != 0:
            shift = adjusted_rate - avg_rate
            adjusted_values = [v + shift for v in values]
            p_over = _normal_over(adjusted_values, prop.line)
        else:
            p_over = _normal_over(values, prop.line)
    else:
        p_over = _poisson_over(adjusted_rate, prop.line)
    p_under = 1.0 - p_over

    # Use RAW implied probabilities for edge calculation — this is the
    # actual line you bet against, including vig.  Edge = model - raw.
    implied_over = _american_to_implied(prop.over_price)
    implied_under = _american_to_implied(prop.under_price)

    # Compare both sides and pick the one with more edge
    best_side = None
    best_edge = -1.0
    best_model_prob = 0.0
    best_implied = 0.0
    best_odds = None

    if implied_over and implied_over > 0:
        over_edge = p_over - implied_over
        if over_edge > best_edge:
            best_side = "over"
            best_edge = over_edge
            best_model_prob = p_over
            best_implied = implied_over
            best_odds = prop.over_price

    if implied_under and implied_under > 0:
        under_edge = p_under - implied_under
        if under_edge > best_edge:
            best_side = "under"
            best_edge = under_edge
            best_model_prob = p_under
            best_implied = implied_under
            best_odds = prop.under_price

    if best_side is None or best_edge < MIN_PICK_EDGE:
        return None

    # Use vig-free probability for juice filtering only.
    # This checks the TRUE probability (not inflated by vig)
    # against our max threshold.
    true_over, true_under = _remove_vig(prop.over_price, prop.under_price)
    juice_implied = (
        true_over if best_side == "over" and true_over is not None
        else true_under if best_side == "under" and true_under is not None
        else best_implied
    )
    if juice_implied > MAX_PROP_IMPLIED:
        return None

    # Recent form factor
    recent_5 = values[:5]
    recent_avg = sum(recent_5) / len(recent_5) if recent_5 else 0
    form_factor = 1.0 + 0.15 * (recent_avg - avg_rate) / max(avg_rate, 0.1)
    confidence = min(max(best_model_prob * form_factor, 0.0), 1.0)

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

    reasoning = (
        f"{avg_rate:.1f} {stat_label}/game over {games} games "
        f"(over {prop.line} in {hit_pct:.0%}). "
        f"Model: {best_model_prob:.1%} vs line {best_implied:.1%}"
    )
    if matchup and matchup.adjustment != 0:
        reasoning += f" | Matchup: {matchup.reasoning}"

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
) -> Optional[MatchupContext]:
    """Build matchup context for a skater prop by querying games vs opponent."""
    vs_stats = await _get_skater_vs_opponent_stats(
        session, player_id, team_id, opponent_team_id,
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


async def generate_prop_picks(
    session: AsyncSession,
    game_id: int,
) -> List[PropPick]:
    """Generate player prop picks for a specific game.

    Reads the PlayerPropOdds for the game, matches players to their
    historical game stats, computes opponent-specific matchup adjustments,
    and runs the analysis.

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

    # Build lookup by lowercase name and last name
    player_by_name: Dict[str, Player] = {}
    for p in all_players:
        player_by_name[p.name.lower()] = p
        parts = p.name.split()
        if len(parts) >= 2:
            player_by_name[parts[-1].lower()] = p

    # Cache game stats per player to avoid duplicate queries
    skater_stats_cache: Dict[int, List[GamePlayerStats]] = {}
    goalie_stats_cache: Dict[int, List[GameGoalieStats]] = {}
    # Cache matchup contexts per (player_id, market) to avoid re-querying
    matchup_cache: Dict[tuple, Optional[MatchupContext]] = {}
    team_abbrev_cache: Dict[int, str] = {}

    picks: List[PropPick] = []

    for prop in all_props:
        # Match prop player to our Player record
        player = player_by_name.get(prop.player_name.lower())
        if player is None:
            # Try last name
            parts = prop.player_name.split()
            if len(parts) >= 2:
                player = player_by_name.get(parts[-1].lower())
        if player is None:
            continue

        # Determine the opponent team for this player
        player_team_id = player.team_id or 0
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

        if market == "player_goal_scorer_anytime":
            # ATG — skater stat
            if player.id not in skater_stats_cache:
                skater_stats_cache[player.id] = await _get_skater_game_stats(
                    session, player.id, player_team_id,
                )
            stats = skater_stats_cache[player.id]

            matchup = None
            cache_key = (player.id, "goals")
            if cache_key not in matchup_cache and opponent_team_id and stats:
                matchup_cache[cache_key] = await _build_skater_matchup(
                    session, player.id, player_team_id, opponent_team_id,
                    opponent_abbrev, stats, market, lambda g: g.goals,
                )
            matchup = matchup_cache.get(cache_key)

            pick = _analyze_atg(
                prop.player_name, player.id, prop, stats, matchup,
            )
            if pick:
                picks.append(pick)

        elif market == "player_shots_on_goal":
            if player.id not in skater_stats_cache:
                skater_stats_cache[player.id] = await _get_skater_game_stats(
                    session, player.id, player_team_id,
                )
            stats = skater_stats_cache[player.id]

            matchup = None
            cache_key = (player.id, "shots")
            if cache_key not in matchup_cache and opponent_team_id and stats:
                matchup_cache[cache_key] = await _build_skater_matchup(
                    session, player.id, player_team_id, opponent_team_id,
                    opponent_abbrev, stats, market, lambda g: g.shots,
                )
            matchup = matchup_cache.get(cache_key)

            pick = _analyze_over_under(
                prop.player_name, player.id, prop, stats, market,
                lambda g: g.shots, matchup,
            )
            if pick:
                picks.append(pick)

        elif market == "player_points":
            if player.id not in skater_stats_cache:
                skater_stats_cache[player.id] = await _get_skater_game_stats(
                    session, player.id, player_team_id,
                )
            stats = skater_stats_cache[player.id]

            matchup = None
            cache_key = (player.id, "points")
            if cache_key not in matchup_cache and opponent_team_id and stats:
                matchup_cache[cache_key] = await _build_skater_matchup(
                    session, player.id, player_team_id, opponent_team_id,
                    opponent_abbrev, stats, market, lambda g: g.points,
                )
            matchup = matchup_cache.get(cache_key)

            pick = _analyze_over_under(
                prop.player_name, player.id, prop, stats, market,
                lambda g: g.points, matchup,
            )
            if pick:
                picks.append(pick)

        elif market == "player_assists":
            if player.id not in skater_stats_cache:
                skater_stats_cache[player.id] = await _get_skater_game_stats(
                    session, player.id, player_team_id,
                )
            stats = skater_stats_cache[player.id]

            matchup = None
            cache_key = (player.id, "assists")
            if cache_key not in matchup_cache and opponent_team_id and stats:
                matchup_cache[cache_key] = await _build_skater_matchup(
                    session, player.id, player_team_id, opponent_team_id,
                    opponent_abbrev, stats, market, lambda g: g.assists,
                )
            matchup = matchup_cache.get(cache_key)

            pick = _analyze_over_under(
                prop.player_name, player.id, prop, stats, market,
                lambda g: g.assists, matchup,
            )
            if pick:
                picks.append(pick)

        elif market == "player_total_saves":
            if player.id not in goalie_stats_cache:
                goalie_stats_cache[player.id] = await _get_goalie_game_stats(
                    session, player.id,
                )
            stats = goalie_stats_cache[player.id]

            matchup = None
            cache_key = (player.id, "saves")
            if cache_key not in matchup_cache and opponent_team_id and stats:
                matchup_cache[cache_key] = await _build_goalie_matchup(
                    session, player.id, opponent_team_id,
                    opponent_abbrev, stats, lambda g: g.saves,
                )
            matchup = matchup_cache.get(cache_key)

            pick = _analyze_over_under(
                prop.player_name, player.id, prop, stats, market,
                lambda g: g.saves, matchup,
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
