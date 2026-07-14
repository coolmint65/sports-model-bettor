"""Multi-league basketball framework.

Parameterizes every market the per-sport NBA modules already handle, so
adding a new league is config + data ingest, not a code fork.

Entry points:

    from engine.basketball import (
        LEAGUE_REGISTRY,        # all known leagues + metadata
        get_league_config,      # one league's config dict
        active_leagues,         # leagues currently in season
        predict_full,           # generic factor predictor
        generate_picks,         # generic ML/SPREAD/TOTAL picker
    )

Per-league DBs live at ``data/basketball/<league>.db`` with a uniform
schema (``teams`` + ``games`` tables, no league prefix). Existing NBA
keeps ``data/nba.db`` to avoid a destructive cutover; the basketball
package treats NBA as one of N leagues but reads/writes its DB through
a compatibility shim (see ``_db.get_conn``).

Status is tracked per-league in LEAGUE_REGISTRY:

    active            — game-level model live, picks emit, tracker settles
    beta              — onboarded but not yet validated; paper-bet only
    pending_data      — HR offers it but no clean data source identified
    pending_calibration — data ingest works; model constants not yet fitted
    offseason         — currently no events; framework returns empty slate
"""

from ._config import (
    LEAGUE_REGISTRY,
    get_league_config,
    active_leagues,
)
from ._predict import predict_full
from ._picks import generate_picks
from ._tracker import (
    record_pick,
    settle_picks,
    list_pending,
    list_history,
)

__all__ = [
    "LEAGUE_REGISTRY",
    "get_league_config",
    "active_leagues",
    "predict_full",
    "generate_picks",
    "record_pick",
    "settle_picks",
    "list_pending",
    "list_history",
]
