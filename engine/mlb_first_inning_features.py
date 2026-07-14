"""Legacy path - moved to ``engine.sports.mlb.first_inning_features`` (A6 STUB).

Old callers using ``engine.mlb_first_inning_features`` keep working via this
re-export. Update your imports when convenient.
"""
from engine.sports.mlb.first_inning_features import *  # noqa: F401, F403
from engine.sports.mlb import first_inning_features as _mod  # noqa: F401

# Re-export underscore-prefixed names that wildcard skips.
globals().update({k: v for k, v in vars(_mod).items()
                   if k.startswith("_") and not k.startswith("__")})
