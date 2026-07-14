"""NBA sport module (A6 namespace shim).

Re-exports the legacy ``engine.nba_*`` modules under the canonical
``engine.sports.nba`` namespace. See ``engine.sports.nhl`` docstring
for the rationale and migration plan.
"""
from __future__ import annotations

from ...predictor import NBAFactorPredictor  # noqa: F401

__all__ = [
    "NBAFactorPredictor",
    "predict",
    "db",
    "picks",
    "calibration",
    "injuries",
]


def __getattr__(name: str):
    if name in ("predict", "db", "picks", "calibration", "injuries"):
        import importlib
        return importlib.import_module(__name__ + "." + name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
