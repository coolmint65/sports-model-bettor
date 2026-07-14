"""Legacy path - moved to ``engine.sports.mlb.props_train_all`` (A6 STUB).

Old callers using ``engine.mlb_props_train_all`` keep working via this
re-export. Update your imports when convenient.
"""
from engine.sports.mlb.props_train_all import *  # noqa: F401, F403
from engine.sports.mlb import props_train_all as _mod  # noqa: F401

# Re-export underscore-prefixed names that wildcard skips.
globals().update({k: v for k, v in vars(_mod).items()
                   if k.startswith("_") and not k.startswith("__")})
