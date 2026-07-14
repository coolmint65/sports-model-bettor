"""AIHL (Australian Ice Hockey League) — Oceania-region hockey league.

Sourced from SofaScore (theScore doesn't cover Oceania). Schema reuses
the shared theScore DDL so the predictor / picks / tracker stay
sport-agnostic.

Status: pending_calibration — first ingest will seed the team table +
season-to-date results. Predictor falls back to default Poisson rates
until enough samples land.

Public surface:
    db.get_conn()      — sqlite handle to data/hockey/aihl.db
    ingest.refresh()   — pull latest SofaScore events into the DB
    AIHLPredictor      — Predictor protocol impl (Poisson scorer).
"""
from __future__ import annotations

__all__ = ["AIHLPredictor", "db", "ingest", "predict"]

LEAGUE_SLUG = "aihl"
DB_PATH = "data/hockey/aihl.db"
TEAMS_TABLE = "teams"
GAMES_TABLE = "games"
SOFASCORE_TOURNAMENT_ID = 11059


def __getattr__(name):
    if name in ("db", "ingest", "predict"):
        import importlib
        return importlib.import_module(__name__ + "." + name)
    if name == "AIHLPredictor":
        from .predict import AIHLPredictor as _P
        return _P
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
