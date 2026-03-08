"""Prop grading dispatch — routes to each prop type's grade/outcome methods."""

from typing import Optional

from app.models.game import Game
from app.props.types import PROP_BY_BET_TYPE


def check_prop_outcome(
    bet_type: str,
    prediction_value: str,
    game: Game,
    home_abbr: str = "",
) -> Optional[bool]:
    """
    Grade a prop prediction.

    Returns True (win), False (loss), or None (push/ungradeable).
    Dispatches to the prop type's grade() method.
    """
    prop_cls = PROP_BY_BET_TYPE.get(bet_type)
    if prop_cls is None:
        return None
    return prop_cls().grade(prediction_value, game, home_abbr)


def determine_prop_outcome(
    game: Game,
    bet_type: str,
) -> Optional[str]:
    """
    Build the actual outcome string for a prop bet type.

    Dispatches to the prop type's determine_outcome() method.
    """
    prop_cls = PROP_BY_BET_TYPE.get(bet_type)
    if prop_cls is None:
        return None
    return prop_cls().determine_outcome(game)
