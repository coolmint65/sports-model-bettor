"""Multi-league soccer framework.

Soccer is fundamentally different from the basketball/hockey frameworks
in two ways that drive every design decision below:

  1. Three-outcome match — 1X2 (home / draw / away) instead of binary
     ML. Pricing models that ignore draw probability mis-rate every
     close match, so the predictor emits a full 3-way distribution.

  2. Low-scoring + Poisson-shaped scoring distributions. Goal counts
     cluster tightly around the league mean (1.0–1.8 goals/team/match)
     so a Normal-margin model would mis-fit. We use a Dixon-Coles
     bivariate Poisson with low-score adjustments instead.

Entry points:

    from engine.soccer import (
        LEAGUE_REGISTRY,        # all known leagues + metadata
        get_league_config,      # one league's config dict
        active_leagues,         # leagues currently in season
        predict_match,          # 1X2 + OU + BTTS probs per match
        generate_picks,         # picks for tonight's matches
    )

Per-league DBs live at ``data/soccer/<league>.db`` with a uniform
schema (``teams`` / ``matches`` / ``picks``). The framework reads/writes
through the same conventions as engine.basketball.

Status taxonomy mirrors basketball:

    active              — backfilled, calibrated, live picks
    beta                — calibrated but ROI unproven; paper-bet only
    pending_calibration — data source identified, constants unfit
    pending_data        — HR offers it but no clean ingest yet
    offseason           — empty slate; framework returns no picks
"""

from ._config import (
    LEAGUE_REGISTRY,
    get_league_config,
    active_leagues,
)
from ._predict import predict_match, predict_slate
from ._picks import generate_picks_for_match, generate_picks_for_slate
from ._tracker import record_picks, settle_picks, list_history

__all__ = [
    "LEAGUE_REGISTRY",
    "get_league_config",
    "active_leagues",
    "predict_match",
    "predict_slate",
    "generate_picks_for_match",
    "generate_picks_for_slate",
    "record_picks",
    "settle_picks",
    "list_history",
]
