"""AHL ingest — pulls teams + final games from theScore into the local DB."""
from __future__ import annotations

import logging

from .._thescore_ingest import backfill as _backfill
from . import LEAGUE_SLUG, TEAMS_TABLE, GAMES_TABLE
from .db import get_conn

logger = logging.getLogger(__name__)


def refresh(*, status: str | None = None) -> dict:
    """Pull every team + event from theScore into the local DB.
    Idempotent (INSERT OR REPLACE). Returns aggregate counts."""
    return _backfill(LEAGUE_SLUG, get_conn(),
                      teams_table=TEAMS_TABLE, games_table=GAMES_TABLE,
                      status=status)


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.sports.ahl.ingest")
    ap.add_argument("--status", default=None,
                    help="filter to status (e.g. 'final')")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    res = refresh(status=args.status)
    print(res)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
