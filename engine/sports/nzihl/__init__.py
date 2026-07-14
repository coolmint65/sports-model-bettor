"""NZIHL (New Zealand Ice Hockey League) — Oceania-region hockey league.

Sourced from SofaScore (theScore doesn't cover Oceania). Schema reuses
the shared theScore DDL.

Status: pending_calibration — predictor falls back to default Poisson
rates until enough samples accumulate.

Public surface:
    db.get_conn()      — sqlite handle to data/hockey/nzihl.db
    ingest.refresh()   — pull latest SofaScore events into the DB
    NZIHLPredictor     — Predictor protocol impl (Poisson scorer).
"""
from __future__ import annotations

__all__ = ["NZIHLPredictor", "db", "ingest", "predict"]

LEAGUE_SLUG = "nzihl"
DB_PATH = "data/hockey/nzihl.db"
TEAMS_TABLE = "teams"
GAMES_TABLE = "games"
SOFASCORE_TOURNAMENT_ID = 11133


def __getattr__(name):
    if name in ("db", "ingest", "predict"):
        import importlib
        return importlib.import_module(__name__ + "." + name)
    if name == "NZIHLPredictor":
        from .predict import NZIHLPredictor as _P
        return _P
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
