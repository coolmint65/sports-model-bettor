"""Legacy path — moved to ``engine.sports.nhl.explain`` (A6 STUB).

Old callers using ``engine.nhl_explain`` keep working via this
re-export. Update your imports when convenient — the legacy path
will be removed once no in-tree callers reference it.
"""
from engine.sports.nhl.explain import *  # noqa: F401, F403
from engine.sports.nhl import explain as _mod  # noqa: F401

# Explicit private re-exports (wildcard import skips _-prefixed names).
# Add specific names below if external callers reference them.
globals().update({k: v for k, v in vars(_mod).items()
                   if k.startswith("_") and not k.startswith("__")})
