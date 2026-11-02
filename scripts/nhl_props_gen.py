"""Standalone NHL player-props generator (decoupled 2026-10-01).

Runs ``engine.sports.nhl.prop_picks.generate_picks()`` on its own
Windows scheduled task (NhlPropsGen, hourly at :05 around the clock since
2026-10-26 so the card locks as soon as HR posts a game's props -- see
``schtasks /query /tn NhlPropsGen /xml``) instead of piggybacking on
the NHL best-bets refresh thread, which GIL-contended with the best-
bets compute (measured ~109s cold) for the ~44-50s a 10k-sim MC pass
over ~1,400 props takes.

Idempotent -- safe to re-run anytime, not just on a timer:
  * ``generate_picks()`` is a card lock (2026-10-26): each game date keeps the
    first picks recorded for it (up to the per-game / per-date caps) and a
    later run never voids or swaps them -- it only fills empty slots.
  * ``insert_pick()`` is ``INSERT OR IGNORE`` on
    ``UNIQUE(game_id, player_id, bet_type, pick)`` -- an identical
    pick seen again this run is a no-op, never a duplicate row.
  * Only ``result IS NULL`` rows are ever touched -- settled history
    is never voided or rewritten by a later run.
No separate "clear today's picks" step needed.

Lookback is adaptive (season-aware staircase, see
``prop_picks._adaptive_lookback``) -- this script intentionally does
NOT pass ``lookback_days`` so ``generate_picks()`` picks it.

Usage (matches the other scripts\\*.py convention -- see
scripts\\dedupe_nhl_teams.py):
    .venv\\Scripts\\python.exe -m scripts.nhl_props_gen
"""
from __future__ import annotations

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("nhl_props_gen")


def main(argv: list[str]) -> int:
    from engine.sports.nhl.prop_picks import generate_picks

    try:
        counts = generate_picks()
    except Exception:
        logger.exception("nhl_props_gen failed")
        return 1

    logger.info("nhl_props_gen done: %s", counts)
    print(counts)
    # A clean run with nothing picked is still exit 0 (e.g. an
    # all-day gap with no NHL slate) -- only an exception is a
    # failure. The log line above is what a human/alert should watch.
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
