"""Legacy path - moved to ``engine.sports.nba.nhl_props_train`` (A6 STUB)."""
from engine.sports.nba.nhl_props_train import *  # noqa: F401, F403
from engine.sports.nba import nhl_props_train as _mod  # noqa: F401

globals().update({k: v for k, v in vars(_mod).items()
                   if k.startswith("_") and not k.startswith("__")})
