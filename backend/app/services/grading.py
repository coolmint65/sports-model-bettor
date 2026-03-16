"""
Shared bet grading logic — single source of truth for determining
whether a bet won, lost, or pushed.

Used by:
- SettlementService (auto-settlement of predictions and tracked bets)
- PredictionManager (historical stats)
- Backtester (parameter evaluation)
"""

from typing import Optional

from app.models.game import Game


def check_ml_outcome(
    prediction_value: str,
    game: Game,
    home_abbr: str = "",
) -> Optional[bool]:
    """Check moneyline prediction against final score."""
    hs, aws = game.home_score, game.away_score
    if hs is None or aws is None:
        return None
    if hs == aws:
        return None  # Push (OT decides in NHL, but guard against it)
    if prediction_value == home_abbr or prediction_value == "home":
        return hs > aws
    return aws > hs


def check_total_outcome(
    prediction_value: str,
    game: Game,
) -> Optional[bool]:
    """Check over/under prediction against final score total."""
    hs, aws = game.home_score, game.away_score
    if hs is None or aws is None:
        return None
    total = hs + aws
    try:
        parts = prediction_value.split("_")
        direction = parts[0]
        line = float(parts[1])
    except (IndexError, ValueError):
        return None

    if direction == "over":
        if total == line:
            return None  # Push
        return total > line
    elif direction == "under":
        if total == line:
            return None  # Push
        return total < line
    return None


def check_spread_outcome(
    prediction_value: str,
    game: Game,
    home_abbr: str = "",
) -> Optional[bool]:
    """Check spread prediction against final score margin."""
    hs, aws = game.home_score, game.away_score
    if hs is None or aws is None:
        return None
    margin = hs - aws
    try:
        pred_parts = prediction_value.split("_", 1)
        team_abbr = pred_parts[0] if len(pred_parts) > 1 else ""
        spread_str = pred_parts[1] if len(pred_parts) > 1 else prediction_value
        spread_val = float(spread_str)
    except (IndexError, ValueError):
        return None

    is_home = (team_abbr == home_abbr) or ("home" in prediction_value.lower())
    adjusted_margin = margin if is_home else -margin
    result = adjusted_margin + spread_val
    if result == 0:
        return None  # Push
    return result > 0


def check_outcome(
    bet_type: str,
    prediction_value: str,
    game: Game,
    home_abbr: str = "",
) -> Optional[bool]:
    """
    Determine if a bet won given the final game state.

    Returns True (win), False (loss), or None (push / ungradeable).

    Args:
        bet_type: "ml", "total", or "spread"
        prediction_value: The predicted value string (e.g. "home", "over_5.5", "EDM_-1.5")
        game: Game ORM object with home_score, away_score populated
        home_abbr: Home team abbreviation (needed for ml/spread grading)
    """
    if game.home_score is None or game.away_score is None:
        return None

    if bet_type == "ml":
        return check_ml_outcome(prediction_value, game, home_abbr)
    elif bet_type == "total":
        return check_total_outcome(prediction_value, game)
    elif bet_type == "spread":
        return check_spread_outcome(prediction_value, game, home_abbr)

    # Dispatch to prop engine for all other bet types
    from app.props.grading import check_prop_outcome
    return check_prop_outcome(bet_type, prediction_value, game, home_abbr)


def determine_actual_outcome(
    game: Game,
    bet_type: str,
) -> Optional[str]:
    """
    Build the actual outcome string for a bet type.

    Used by BetResult records to store the canonical outcome.
    """
    hs, aws = game.home_score, game.away_score
    if hs is None or aws is None:
        return None

    if bet_type == "ml":
        return "home" if hs > aws else "away"
    elif bet_type == "total":
        return f"total_{hs + aws}"
    elif bet_type == "spread":
        return f"margin_{hs - aws}"

    # Dispatch to prop engine for all other bet types
    from app.props.grading import determine_prop_outcome
    return determine_prop_outcome(game, bet_type)


def compute_tracked_bet_pl(
    was_correct: bool,
    odds: Optional[float],
    units: float,
) -> float:
    """
    Calculate profit/loss for a tracked bet using American odds.

    Win: pays out based on odds and units staked.
    Loss: loses the units staked.
    """
    if not was_correct:
        return round(-1.0 * units, 2)

    if odds and odds > 0:
        return round((odds / 100) * units, 2)
    elif odds and odds < 0:
        return round((100 / abs(odds)) * units, 2)
    return round(1.0 * units, 2)


def get_home_abbr(game: Game) -> str:
    """Extract home team abbreviation from a Game with loaded relationships."""
    home_team = getattr(game, "home_team", None)
    return getattr(home_team, "abbreviation", "") if home_team else ""
