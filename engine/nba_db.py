"""Legacy path - moved to ``engine.sports.nba.db`` (A6 STUB)."""
from engine.sports.nba.db import *  # noqa: F401, F403
from engine.sports.nba import db as _mod  # noqa: F401

globals().update({k: v for k, v in vars(_mod).items()
                   if k.startswith("_") and not k.startswith("__")})
