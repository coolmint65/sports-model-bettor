"""
Live data worker — Phase 3a entry point.

Runs as a separate process from the FastAPI server. Polls ESPN
scoreboard for NBA + NHL, normalizes each in-progress game's state,
and writes to `engine.live._store` (SQLite-backed shared cache).

Cadence per spec:
    NBA: 15s
    NHL: 30s

Two staggered loops to honor the per-sport cadence without bloating
the polling burden — NBA polls every 15s, NHL every 30s. Both share
one main thread; we just track per-sport `next_due_at` timestamps.

Failure handling:
    - ESPN returns None → keep last-known state (we don't overwrite
      with empty), log at warning level.
    - One sport's parse throws → don't kill the loop; isolate per-sport.
    - Single game's parse throws → don't kill that sport's tick;
      isolate per-game inside _state.fetch_states.

Run separately:
    python -m services.live_worker.main

Stop:
    Ctrl+C — KeyboardInterrupt is caught, store-flush is unnecessary
    (writes are durable per upsert).
"""

from __future__ import annotations
import logging
import signal
import time
from typing import Any

from engine.live._state import fetch_states
from engine.live._store import upsert_state, purge_stale, ensure_table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] live_worker: %(message)s",
)
logger = logging.getLogger("live_worker")

# Per spec — see project_sports_model_roadmap.md Phase 3 spec.
_POLL_INTERVAL_S = {"nba": 15, "nhl": 30}

# Garbage-collect rows older than this so completed games don't pile
# up in the cache. 30 min is generous — gives the API time to read
# final-state rows before they vanish.
_PURGE_AFTER_S = 1800
_PURGE_EVERY_S = 300


_running = True


def _handle_signal(signum: int, _frame: Any) -> None:
    global _running
    logger.info("live_worker received signal %s — shutting down", signum)
    _running = False


def _poll_sport(sport: str) -> int:
    """Fetch and persist the live state for one sport. Returns the
    number of games written. Per-sport try/except so an outage in one
    sport's ESPN feed doesn't take down the other."""
    try:
        states = fetch_states(sport)
    except Exception as e:
        logger.warning("fetch_states(%s) crashed: %s", sport, e)
        return 0
    written = 0
    for s in states:
        try:
            upsert_state(sport, s["game_id"], s)
            written += 1
        except Exception as e:
            logger.warning("live_state upsert failed for %s/%s: %s",
                           sport, s.get("game_id"), e)
    if written:
        logger.info("polled %s — %d in-progress game(s) written",
                    sport, written)
    return written


def main() -> None:
    """Main loop — staggered per-sport polling, runs until signaled."""
    ensure_table()

    # Trap SIGINT/SIGTERM so a clean Ctrl+C doesn't leave a half-written
    # row.
    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    next_due_at = {sport: 0.0 for sport in _POLL_INTERVAL_S}
    next_purge_at = 0.0

    logger.info("live_worker started — NBA every %ss, NHL every %ss",
                _POLL_INTERVAL_S["nba"], _POLL_INTERVAL_S["nhl"])

    while _running:
        now = time.monotonic()

        for sport, interval in _POLL_INTERVAL_S.items():
            if now >= next_due_at[sport]:
                _poll_sport(sport)
                next_due_at[sport] = time.monotonic() + interval

        if now >= next_purge_at:
            try:
                deleted = purge_stale(_PURGE_AFTER_S)
                if deleted:
                    logger.info("purged %d stale live_state row(s)", deleted)
            except Exception as e:
                logger.warning("live_state purge failed: %s", e)
            next_purge_at = time.monotonic() + _PURGE_EVERY_S

        # Sleep until the next due tick across all sports + purge.
        # Cap at 1s so the signal handler responds fast on Ctrl+C.
        upcoming = [v for v in next_due_at.values()] + [next_purge_at]
        sleep_s = max(0.5, min(1.0, min(upcoming) - time.monotonic()))
        time.sleep(sleep_s)

    logger.info("live_worker exited cleanly")


if __name__ == "__main__":
    main()
