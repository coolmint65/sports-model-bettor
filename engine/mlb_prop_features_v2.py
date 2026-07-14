"""Legacy path - moved to ``engine.sports.mlb.prop_features_v2`` (A6 STUB).

Old callers using ``engine.mlb_prop_features_v2`` keep working via this
re-export. Update your imports when convenient.
"""
from engine.sports.mlb.prop_features_v2 import *  # noqa: F401, F403
from engine.sports.mlb import prop_features_v2 as _mod  # noqa: F401

# Re-export underscore-prefixed names that wildcard skips.
globals().update({k: v for k, v in vars(_mod).items()
                   if k.startswith("_") and not k.startswith("__")})
