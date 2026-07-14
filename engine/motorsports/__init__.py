"""Motorsports framework.

Per-series predictive stack for outright-style markets (race winner,
podium top-3). Structurally distinct from the team-sports frameworks:

  - Markets are 1-of-N outright, not 2-team H2H. ``picks_core.score_pick``
    doesn't apply — motorsports has its own picks engine in ``_picks``.
  - Predictor outputs a *field-normalized* probability vector across
    all entries, not a single home/away win prob.

Supported series live in ``SERIES_REGISTRY``. F1 ships first (Ergast
historical data is public + clean); IndyCar follows once a reliable
data source is in place.
"""
from ._config import SERIES_REGISTRY, get_series_config

__all__ = ["SERIES_REGISTRY", "get_series_config"]
