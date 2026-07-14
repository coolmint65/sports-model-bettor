"""Legacy path - moved to ``engine.sports.mlb.player_logs`` (A6 STUB).

Old callers using ``engine.mlb_player_logs`` keep working via this
re-export. Update your imports when convenient.
"""
from engine.sports.mlb.player_logs import *  # noqa: F401, F403
from engine.sports.mlb import player_logs as _mod  # noqa: F401

# Re-export underscore-prefixed names that wildcard skips.
globals().update({k: v for k, v in vars(_mod).items()
                   if k.startswith("_") and not k.startswith("__")})
