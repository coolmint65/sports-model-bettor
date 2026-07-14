"""Legacy path - moved to ``engine.sports.nba.full_backtest`` (A6 STUB)."""
from engine.sports.nba.full_backtest import *  # noqa: F401, F403
from engine.sports.nba import full_backtest as _mod  # noqa: F401

globals().update({k: v for k, v in vars(_mod).items()
                   if k.startswith("_") and not k.startswith("__")})
