"""PWHL (Professional Women's Hockey League) — small but established
North-American women's pro league. Sourced from theScore. Same shape
as engine.sports.ahl; both share _thescore_ingest.

Status: beta until ROI is proven on calibrated sample.
"""
from __future__ import annotations

__all__ = ["PWHLPredictor", "db", "ingest", "predict"]

LEAGUE_SLUG = "pwhl"
DB_PATH = "data/hockey/pwhl.db"
TEAMS_TABLE = "teams"
GAMES_TABLE = "games"


def __getattr__(name):
    if name in ("db", "ingest", "predict"):
        import importlib
        return importlib.import_module(__name__ + "." + name)
    if name == "PWHLPredictor":
        from .predict import PWHLPredictor as _P
        return _P
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
