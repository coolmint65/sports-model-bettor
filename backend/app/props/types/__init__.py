"""Prop type registry — add new props here."""

from app.props.types.btts import BTTSProp
from app.props.types.first_goal import FirstGoalProp
from app.props.types.overtime import OvertimeProp
from app.props.types.period_total import PeriodTotalProp
from app.props.types.period_winner import PeriodWinnerProp
from app.props.types.regulation import RegulationWinnerProp

# Master list of all prop types. Adding a new prop = adding one class
# and appending it here.
PROP_REGISTRY = [
    BTTSProp,
    PeriodTotalProp,
    PeriodWinnerProp,
    FirstGoalProp,
    OvertimeProp,
    RegulationWinnerProp,
]

# Lookup by bet_type string for grading dispatch
PROP_BY_BET_TYPE = {cls.bet_type: cls for cls in PROP_REGISTRY}
