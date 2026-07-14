"""Golf framework — outright + top-N picks per tour.

Public surface mirrors ``engine.motorsports`` for consistency. Single
sport key ('pga' for v1) extensible by adding TOUR_REGISTRY entries.
"""

from ._config import TOUR_REGISTRY, get_tour_config
from ._db import get_conn
from ._ingest import ingest_today, ingest_summary, backfill
from ._predict import predict_field, fit_skill
from ._odds import fetch_tournament_odds
from ._picks import generate_picks
from ._tracker import record_picks, settle_picks, list_history

__all__ = [
    "TOUR_REGISTRY",
    "get_tour_config",
    "get_conn",
    "ingest_today",
    "ingest_summary",
    "backfill",
    "predict_field",
    "fit_skill",
    "fetch_tournament_odds",
    "generate_picks",
    "record_picks",
    "settle_picks",
    "list_history",
]
