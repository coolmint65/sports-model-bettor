"""Legacy path — moved to ``engine.sports.nhl.predict`` (A6 STUB).

Old callers using ``engine.nhl_predict`` keep working via this
re-export. Update your imports when convenient — the legacy path
will be removed once no in-tree callers reference it.
"""
from engine.sports.nhl.predict import *  # noqa: F401, F403
from engine.sports.nhl import predict as _mod  # noqa: F401

# Explicit private re-exports (wildcard import skips _-prefixed names).
# Add specific names below if external callers reference them.
globals().update({k: v for k, v in vars(_mod).items()
                   if k.startswith("_") and not k.startswith("__")})
