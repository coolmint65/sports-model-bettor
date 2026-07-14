"""NHL sport module (A6 namespace shim).

Establishes ``engine.sports.nhl`` as the canonical import root for NHL
work. Submodules (``predict``, ``db``, ``picks``, ``tracker``,
``calibration``) re-export from the legacy ``engine.nhl_*`` files —
zero behavior change, but new code can target this namespace today and
future sessions can move actual file contents in without breaking
callers.

Why a shim instead of a hard move: NHL has 15 legacy files and ~128
import sites across engine/, backend/, services/, scripts/, tests/. A
single-PR move would silently break live production paths. Shim now,
move later (per-sport dedicated session per the A6 plan).

Predictor protocol registration also lives here — ``register('nhl',
NHLFactorPredictor())`` already runs in ``engine.predictor._bootstrap``,
so the protocol works regardless of whether callers use the new
namespace or the legacy module path.
"""
from __future__ import annotations

# Re-export the Predictor protocol's NHL adapter under the canonical
# namespace, mirroring engine.sports.tennis.TennisPredictor.
from ...predictor import NHLFactorPredictor  # noqa: F401

__all__ = [
    "NHLFactorPredictor",
    "predict",
    "db",
    "picks",
    "calibration",
]


def __getattr__(name: str):
    """PEP 562 lazy submodule loader. importlib avoids __getattr__
    recursion that ``from . import X`` would trigger."""
    if name in ("predict", "db", "picks", "calibration"):
        import importlib
        return importlib.import_module(__name__ + "." + name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
