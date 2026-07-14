"""Sport-keyed modules (A6).

The new home for per-sport logic. Each sport lives at
``engine/sports/{sport}/`` with a uniform layout:

    predict.py    — Predictor protocol implementations
    picks.py      — picks generation (sport-specific market shapes)
    tracker.py    — record/settle/list — wraps the per-sport tracker
    db.py         — schema + connection (per-sport SQLite)

Migration plan (one sport at a time, decreasing risk if something breaks):

    1. WNBA / NCAAM / Euroleague / etc — already consolidated under
       ``engine.basketball`` (parameterized by league). Auto-registered
       under the Predictor protocol via ``engine.predictor`` at import.
    2. Tennis — port pending here. Adapter wraps the legacy
       ``engine.tennis_*`` modules so callers can switch progressively.
    3. NHL / NBA / MLB — each gets a dedicated session, with full
       parity-test before the legacy modules are deleted.

Until a sport is fully ported, the shim layer here re-exports adapters
that wrap the legacy modules. Callers can switch to the new path
incrementally; no big-bang import churn required.
"""

# Lazy imports — don't pull in per-sport DBs until something asks.

__all__ = [
    "tennis",
    "nhl",
    "nba",
    "mlb",
    "ahl",
    "pwhl",
]

_LAZY_SUBPKGS = {"tennis", "nhl", "nba", "mlb", "ahl", "pwhl"}


def __getattr__(name):
    """PEP 562 module-level __getattr__ so sub-packages load on demand.

    Use importlib.import_module — ``from . import X as _X`` would
    re-enter this hook (Python invokes __getattr__ when the attribute
    isn't yet bound on the module), recursing until the import stack
    overflows.
    """
    if name in _LAZY_SUBPKGS:
        import importlib
        return importlib.import_module(__name__ + "." + name)
    raise AttributeError(f"module 'engine.sports' has no attribute {name!r}")
