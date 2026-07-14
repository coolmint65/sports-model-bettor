"""Legacy path - moved to ``engine.sports.nba.minutes_projector`` (A6 STUB)."""
from engine.sports.nba.minutes_projector import *  # noqa: F401, F403
from engine.sports.nba import minutes_projector as _mod  # noqa: F401

globals().update({k: v for k, v in vars(_mod).items()
                   if k.startswith("_") and not k.startswith("__")})
