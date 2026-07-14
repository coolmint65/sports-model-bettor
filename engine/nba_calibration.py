"""Legacy path - moved to ``engine.sports.nba.calibration`` (A6 STUB)."""
from engine.sports.nba.calibration import *  # noqa: F401, F403
from engine.sports.nba import calibration as _mod  # noqa: F401

globals().update({k: v for k, v in vars(_mod).items()
                   if k.startswith("_") and not k.startswith("__")})
