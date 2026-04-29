"""
Live data worker — Phase 3a entry point.

Runs as a separate process (NOT a uvicorn background task). Polls
ESPN scoreboard + HR live-markets and writes to the shared
`engine.live._store._LIVE_STORE` cache that the API server reads
from when serving /api/{sport}/live-bets.

Cadence per spec:
    NBA: 15s
    NHL: 30s

Run separately:
    python -m services.live_worker.main

Skeleton — fills in during Phase 3a. Public API the worker drives:
    while True:
        - poll ESPN scoreboard for active NBA + NHL games
        - for each in-progress game:
            - fetch HR live odds for that game
            - hand current state + odds to engine.live._predict
            - update engine.live._store._LIVE_STORE[(sport, game_id)]
        - sleep until next tick (sport-specific)

Failure modes to handle:
    - ESPN scoreboard 5xx → keep last known state, log and retry
    - HR live odds empty → no live picks for that game (don't cache stale)
    - Single game's poll throws → don't kill the loop; isolate per-game
    - Process crash → systemd / supervisor restart
"""

from __future__ import annotations
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] live_worker: %(message)s",
)
logger = logging.getLogger("live_worker")


def main() -> None:
    """Entry point. Implementation lands in Phase 3a."""
    logger.info("live_worker scaffold — Phase 3a entry point. "
                "Implementation pending. See project_sports_model_roadmap.md.")
    raise NotImplementedError("Phase 3a — start here tomorrow")


if __name__ == "__main__":
    main()
