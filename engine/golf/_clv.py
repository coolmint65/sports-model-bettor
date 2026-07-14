"""Closing-line value capture for golf outrights.

Golf picks are outright markets (WINNER / TOP_5 / TOP_10 / TOP_20 /
MAKE_CUT) keyed by player_id. The HR fetcher
``fetch_tournament_odds(tour, tournament_id)`` returns one dict per
tournament: ``{bet_type: {player_id: int_odds, ...}, _event_*: ...}``.

CLV path: group pending picks by (tour, tournament_id), fetch each
tournament's odds once, then walk per-pick matching on player_id.

Public:
    capture_closing_odds(tour) -> int  # rows updated/refreshed
"""
from __future__ import annotations

import logging

from ._db import get_conn
from ._odds import fetch_tournament_odds

logger = logging.getLogger(__name__)


def capture_closing_odds(tour: str) -> int:
    """Snapshot HR closing odds for every pending pick on ``tour``.
    Returns rows updated. Idempotent."""
    conn = get_conn(tour)
    pending = conn.execute(
        "SELECT id, tournament_id, bet_type, player_id, closing_odds "
        "FROM picks WHERE result IS NULL"
    ).fetchall()
    if not pending:
        return 0

    # Group by tournament_id so we fetch HR once per tournament instead
    # of once per pick.
    by_tournament: dict[str, list] = {}
    for row in pending:
        by_tournament.setdefault(str(row["tournament_id"]), []).append(row)

    updated = refreshed = 0
    for tournament_id, rows in by_tournament.items():
        try:
            odds = fetch_tournament_odds(tour, tournament_id) or {}
        except Exception as e:
            logger.warning("HR golf fetch failed (%s/%s): %s",
                           tour, tournament_id, e)
            continue
        if not odds:
            continue
        for row in rows:
            pick = dict(row)
            bet_type = pick["bet_type"]
            market = odds.get(bet_type) or {}
            if not market:
                continue
            closing = market.get(int(pick["player_id"]))
            if closing is None:
                continue
            prior = pick.get("closing_odds")
            if prior is None:
                conn.execute(
                    "UPDATE picks SET closing_odds = ? WHERE id = ?",
                    (int(closing), pick["id"]),
                )
                updated += 1
            elif int(prior) != int(closing):
                conn.execute(
                    "UPDATE picks SET closing_odds = ? WHERE id = ?",
                    (int(closing), pick["id"]),
                )
                refreshed += 1
    conn.commit()
    if updated or refreshed:
        logger.info("golf:%s CLV: %d new, %d refreshed (of %d pending)",
                    tour, updated, refreshed, len(pending))
    return updated + refreshed


def capture_all() -> dict:
    """Run capture across every active golf tour. Returns {tour: n}."""
    out: dict[str, int] = {}
    for tour in ("pga", "lpga", "kornferry"):
        try:
            out[tour] = capture_closing_odds(tour)
        except Exception as e:
            logger.warning("golf CLV %s crashed: %s", tour, e)
            out[tour] = 0
    return out


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.golf._clv")
    ap.add_argument("tour", nargs="?", default=None,
                     choices=("pga", "lpga", "kornferry", "all"))
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    if args.tour in (None, "all"):
        res = capture_all()
        print(res)
    else:
        n = capture_closing_odds(args.tour)
        print(f"{args.tour}: {n} rows updated")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
