"""MLB sport module (A6 namespace shim).

Re-exports the legacy ``engine.mlb_*`` modules + ``engine.db`` (which
holds the MLB tracker tables) under the canonical ``engine.sports.mlb``
namespace. See ``engine.sports.nhl`` docstring for the rationale.

Note: MLB is the odd-sport-out — its primary DB is ``engine.db`` (not
``engine.mlb_db``) for historical reasons, so ``engine.sports.mlb.db``
re-exports from ``engine.db``.
"""
from __future__ import annotations

from ...predictor import MLBFactorPredictor  # noqa: F401

__all__ = [
    "MLBFactorPredictor",
    "predict",
    "db",
    "picks",
    "factors",
    "scoring",
]


def __getattr__(name: str):
    if name in ("predict", "db", "picks", "factors", "scoring"):
        import importlib
        return importlib.import_module(__name__ + "." + name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
