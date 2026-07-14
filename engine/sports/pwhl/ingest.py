"""PWHL ingest — same theScore wrapper as AHL."""
from __future__ import annotations

import logging

from .._thescore_ingest import backfill as _backfill
from . import LEAGUE_SLUG, TEAMS_TABLE, GAMES_TABLE
from .db import get_conn

logger = logging.getLogger(__name__)


def refresh(*, status: str | None = None) -> dict:
    return _backfill(LEAGUE_SLUG, get_conn(),
                      teams_table=TEAMS_TABLE, games_table=GAMES_TABLE,
                      status=status)


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.sports.pwhl.ingest")
    ap.add_argument("--status", default=None)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    print(refresh(status=args.status))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
