"""Closing-line value capture for motorsports outrights (F1 + future
series). Picks are outright winner / podium markets keyed by driver_id.

HR shape from ``fetch_series_odds(series)``:
    {race_id: {winner: {driver_id: {odds, name, hr_ext_id}}, podium: {...}, ...}}

Bet types persisted in picks table: ``WINNER`` and ``PODIUM``. The
extractor maps each to the matching HR market.

Public:
    capture_closing_odds(series) -> int  # rows updated/refreshed
"""
from __future__ import annotations

import logging

from ._db import get_conn
from ._odds import fetch_series_odds

logger = logging.getLogger(__name__)


_BET_TYPE_TO_MARKET = {
    "WINNER": "winner",
    "PODIUM": "podium",
}


def capture_closing_odds(series: str) -> int:
    """Snapshot HR closing odds for every pending motorsports pick on
    ``series``. Returns rows updated. Idempotent."""
    conn = get_conn(series)
    pending = conn.execute(
        "SELECT id, race_id, bet_type, driver_id, closing_odds "
        "FROM picks WHERE result IS NULL"
    ).fetchall()
    if not pending:
        return 0

    try:
        odds_by_race = fetch_series_odds(series) or {}
    except Exception as e:
        logger.warning("HR motorsports fetch failed (%s): %s", series, e)
        return 0
    if not odds_by_race:
        return 0

    updated = refreshed = 0
    for row in pending:
        pick = dict(row)
        race = odds_by_race.get(str(pick["race_id"]))
        if not race:
            continue
        market_key = _BET_TYPE_TO_MARKET.get(pick["bet_type"])
        if not market_key:
            continue
        market = race.get(market_key) or {}
        entry = market.get(int(pick["driver_id"]))
        if not entry:
            continue
        closing = entry.get("odds")
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
        logger.info("motorsports:%s CLV: %d new, %d refreshed (of %d pending)",
                    series, updated, refreshed, len(pending))
    return updated + refreshed


def capture_all() -> dict:
    """Run capture across every registered motorsports series. Currently
    F1 only; IndyCar/NASCAR will register through ``get_series_config``
    as those frameworks land."""
    out: dict[str, int] = {}
    for series in ("f1",):
        try:
            out[series] = capture_closing_odds(series)
        except Exception as e:
            logger.warning("motorsports CLV %s crashed: %s", series, e)
            out[series] = 0
    return out


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.motorsports._clv")
    ap.add_argument("series", nargs="?", default="f1")
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    n = capture_closing_odds(args.series)
    print(f"{args.series}: {n} rows updated")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
