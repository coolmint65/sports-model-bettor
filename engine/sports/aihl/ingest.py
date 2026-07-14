"""AIHL ingest — pulls teams + events from SofaScore into the DB."""
from __future__ import annotations

import logging

from .._sofascore_hockey_ingest import backfill as _backfill
from . import LEAGUE_SLUG, TEAMS_TABLE, GAMES_TABLE, SOFASCORE_TOURNAMENT_ID
from .db import get_conn

logger = logging.getLogger(__name__)


def refresh(*, status: str | None = None,
             seasons: int | list[int] | None = None) -> dict:
    """Pull AIHL games from SofaScore. ``seasons=None`` = current season
    only (worker default). Pass an int N to walk N most recent seasons
    for backfill / calibration."""
    return _backfill(LEAGUE_SLUG, get_conn(),
                      tournament_id=SOFASCORE_TOURNAMENT_ID,
                      teams_table=TEAMS_TABLE, games_table=GAMES_TABLE,
                      status=status, seasons=seasons)


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.sports.aihl.ingest")
    ap.add_argument("--status", default=None,
                    help="filter to status (e.g. 'final')")
    ap.add_argument("--seasons", type=int, default=None,
                    help="N most recent seasons (default 1)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    print(refresh(status=args.status, seasons=args.seasons))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
