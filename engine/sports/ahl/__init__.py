"""AHL (American Hockey League) — NHL minor league.

Sourced from theScore (free unauthenticated JSON). Schema matches the
PWHL package because both leagues share the same upstream API. NHL
itself uses a separate ESPN/NHL Stats API path and stays in
``engine/sports/nhl/``.

Status: beta (paper-bet only) until ROI proven on a calibrated sample.

Public surface:
    AHLPredictor   — Predictor protocol implementation registered for
                       sport='ahl'. Uses a Poisson scoring model fit
                       to backfilled team off/def goals-per-game.
    db.get_conn()  — sqlite handle to data/hockey/ahl.db
    ingest.refresh() — pull latest theScore events into the DB.
"""
from __future__ import annotations

__all__ = ["AHLPredictor", "db", "ingest", "predict"]

LEAGUE_SLUG = "ahl"
DB_PATH = "data/hockey/ahl.db"
TEAMS_TABLE = "teams"
GAMES_TABLE = "games"


def __getattr__(name):
    if name in ("db", "ingest", "predict"):
        import importlib
        return importlib.import_module(__name__ + "." + name)
    if name == "AHLPredictor":
        from .predict import AHLPredictor as _P
        return _P
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
