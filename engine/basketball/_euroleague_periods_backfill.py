"""Backfill per-quarter scores into the Euroleague games table.

The standard Euroleague ingest (`_euroleague_ingest._upsert_games_for_gameday`)
only pulls full-game scores from the `/schedules` + `/results` endpoints —
those don't carry per-quarter breakdowns. Quarter data lives at
`api-live.euroleague.net/v1/games?seasonCode=&gameCode=`, in the
`<partials Partial1="" Partial2="" ... />` attribute on each team's
`<localclub>` / `<roadclub>` block.

This module backfills those columns for every finalized game in the
DB. Idempotent: skips rows that already have `home_q1` populated.

CLI::

    python -m engine.basketball._euroleague_periods_backfill
    python -m engine.basketball._euroleague_periods_backfill --throttle 0.3
    python -m engine.basketball._euroleague_periods_backfill --force
"""
from __future__ import annotations

import argparse
import logging
import time
import urllib.request
import xml.etree.ElementTree as ET

from ._db import get_conn

logger = logging.getLogger(__name__)

_BASE = "https://api-live.euroleague.net/v1"
_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "SportsBettor/1.0"),
    "Accept": "text/xml,application/xml",
}


def _season_code(season: int) -> str:
    """DB season is the calendar year the regular season starts in;
    Euroleague's seasonCode is the same digit ('E2024' = 2024-25 season)."""
    return f"E{int(season)}"


def _strip_season_prefix(gamecode: str) -> str:
    """DB stores game_id as 'E2025_7' but the live API expects raw
    integer gameCodes ('7'). Strip the season prefix before the call."""
    s = str(gamecode)
    if "_" in s:
        # 'E2025_7' -> '7'
        s = s.rsplit("_", 1)[-1]
    return s


def _fetch_game(season: int, gamecode: str) -> ET.Element | None:
    raw_gc = _strip_season_prefix(gamecode)
    url = (f"{_BASE}/games?seasonCode={_season_code(season)}"
           f"&gameCode={raw_gc}")
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as r:
            return ET.fromstring(r.read())
    except Exception as e:
        logger.debug("fetch failed %s: %s", url, e)
        return None


def _partials_for(side: ET.Element | None) -> dict[int, int] | None:
    """Return {1: q1, 2: q2, 3: q3, 4: q4} from a localclub/roadclub
    element's <partials> child. None when the element is missing or
    has no partial attributes."""
    if side is None:
        return None
    p = side.find("partials")
    if p is None:
        return None
    out: dict[int, int] = {}
    for i in (1, 2, 3, 4):
        v = p.attrib.get(f"Partial{i}")
        if v is None:
            return None
        try:
            out[i] = int(v)
        except (TypeError, ValueError):
            return None
    return out


def backfill(force: bool = False, throttle_s: float = 0.25,
             limit: int | None = None) -> dict:
    """Walk every finalized Euroleague game and pull per-quarter scores.

    Args:
        force: when False (default), skips games that already have
               `home_q1` populated.
        throttle_s: delay between API calls. Euroleague's free endpoint
                    handles ~5 req/s comfortably; we go slower to be safe.
        limit: stop after N updates (None = process everything).

    Returns aggregate counts.
    """
    conn = get_conn("euroleague")
    where = ["status = 'final'", "game_id IS NOT NULL", "season IS NOT NULL"]
    if not force:
        where.append("home_q1 IS NULL")
    sql = (f"SELECT game_id, season FROM games "
           f"WHERE {' AND '.join(where)} "
           f"ORDER BY season, game_id")
    rows = conn.execute(sql).fetchall()
    out = {"candidates": len(rows), "updated": 0,
           "missing_partials": 0, "fetch_errors": 0}
    for i, r in enumerate(rows):
        if limit is not None and out["updated"] >= limit:
            break
        if i and i % 100 == 0:
            logger.info("euroleague Q1 backfill: %d/%d, updated=%d",
                        i, len(rows), out["updated"])
        gamecode = str(r[0])
        season = int(r[1])
        doc = _fetch_game(season, gamecode)
        if doc is None:
            out["fetch_errors"] += 1
            continue
        home_partials = _partials_for(doc.find("localclub"))
        away_partials = _partials_for(doc.find("roadclub"))
        if not home_partials or not away_partials:
            out["missing_partials"] += 1
            continue
        conn.execute(
            "UPDATE games SET "
            "  home_q1 = ?, home_q2 = ?, home_q3 = ?, home_q4 = ?, "
            "  away_q1 = ?, away_q2 = ?, away_q3 = ?, away_q4 = ? "
            "WHERE game_id = ?",
            (home_partials[1], home_partials[2],
             home_partials[3], home_partials[4],
             away_partials[1], away_partials[2],
             away_partials[3], away_partials[4],
             gamecode),
        )
        out["updated"] += 1
        if throttle_s > 0:
            time.sleep(throttle_s)
    conn.commit()
    logger.info("euroleague Q1 backfill done: %s", out)
    return out


def _cli() -> int:
    ap = argparse.ArgumentParser(prog="engine.basketball._euroleague_periods_backfill")
    ap.add_argument("--force", action="store_true",
                    help="re-fetch games that already have home_q1")
    ap.add_argument("--throttle", type=float, default=0.25)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    res = backfill(force=args.force, throttle_s=args.throttle, limit=args.limit)
    print(res)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
