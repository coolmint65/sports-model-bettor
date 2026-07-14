"""Per-period score backfill for theScore-sourced hockey leagues.

theScore's ``/{league}/events`` endpoint ships totals only — per-period
scores live behind a second hop at ``/{league}/box_scores/{id}``.
We pay that cost lazily (only once per game, skipping games already
populated) and write to the new home_p1/away_p1/...p3 columns.

Used by AHL/PWHL/AIHL/NZIHL state-MC fits — those leagues had no
quarter splits before (theScore is single-fetch by default), which
blocked the WNBA-style quarter-splits state-MC sampler. After backfill
the existing _state_mc fitter just reads them.

Public:
    backfill_periods(league_slug, conn, *, games_table='games',
                     limit=None, throttle=0.3)
"""
from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

_API = "https://api.thescore.com"
_HEADERS = {"User-Agent": "SportsBettor/1.0 (period-backfill)",
            "Accept": "application/json"}


def _fetch(url: str, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            logger.debug("theScore fetch %s HTTP %d (attempt %d)",
                          url, e.code, attempt + 1)
        except Exception as e:
            logger.debug("theScore fetch %s failed (attempt %d): %s",
                          url, attempt + 1, e)
        time.sleep(0.5 * (attempt + 1))
    return None


def _line_scores_for_game(league_slug: str, event_id: int
                           ) -> dict[str, list[int | None]] | None:
    """Fetch per-period line_scores for one theScore event. Returns
    ``{'home': [p1, p2, p3], 'away': [p1, p2, p3]}`` or None when the
    box_score isn't available."""
    # theScore exposes box_scores by event id directly on hockey
    # leagues — same endpoint as /box_scores/{box_id} but we need
    # the box_score id from /events. Simpler: fetch the event, read
    # box_score.api_uri, follow it.
    ev = _fetch(f"{_API}/{league_slug}/events/{event_id}")
    if not ev:
        return None
    bs = ev.get("box_score") or {}
    bs_uri = bs.get("api_uri")
    if not bs_uri:
        return None
    bs_full = _fetch(f"{_API}{bs_uri}")
    if not bs_full:
        return None
    ls = bs_full.get("line_scores") or {}
    out: dict[str, list[int | None]] = {"home": [None, None, None],
                                          "away": [None, None, None]}
    for side in ("home", "away"):
        rows = ls.get(side) or []
        for row in rows:
            seg = row.get("segment")
            if not isinstance(seg, int) or seg < 1 or seg > 3:
                continue
            out[side][seg - 1] = row.get("score")
    return out


def backfill_periods(league_slug: str, conn, *,
                      games_table: str = "games",
                      limit: int | None = None,
                      throttle: float = 0.3,
                      only_missing: bool = True) -> dict:
    """Walk every finalized game in ``games_table`` and fill the
    home_p1..p3 / away_p1..p3 columns from theScore.

    ``only_missing=True`` skips games whose home_p1 is already set —
    cheap incremental refresh. Returns counts. Each game costs 2 HTTP
    hits (event + box_score) so backfill is throttled.
    """
    sql = (f"SELECT id FROM {games_table} "
           f"WHERE status='final' AND home_score IS NOT NULL ")
    if only_missing:
        sql += "AND home_p1 IS NULL "
    sql += "ORDER BY date DESC"
    if limit:
        sql += f" LIMIT {int(limit)}"
    rows = conn.execute(sql).fetchall()
    total = len(rows)
    if not total:
        logger.info("[%s] period backfill: nothing to do", league_slug)
        return {"games": 0, "filled": 0, "skipped": 0}

    filled = skipped = 0
    started = time.monotonic()
    for idx, r in enumerate(rows, start=1):
        gid = int(r["id"] if hasattr(r, "__getitem__") else r[0])
        try:
            ls = _line_scores_for_game(league_slug, gid)
        except Exception as e:
            logger.debug("line_scores fetch %s/%s failed: %s",
                          league_slug, gid, e)
            skipped += 1
            time.sleep(throttle)
            continue
        if not ls:
            skipped += 1
            time.sleep(throttle)
            continue
        try:
            conn.execute(
                f"UPDATE {games_table} SET "
                f"  home_p1=?, away_p1=?, "
                f"  home_p2=?, away_p2=?, "
                f"  home_p3=?, away_p3=? "
                f"WHERE id = ?",
                (ls["home"][0], ls["away"][0],
                 ls["home"][1], ls["away"][1],
                 ls["home"][2], ls["away"][2],
                 gid),
            )
            filled += 1
        except Exception as e:
            logger.warning("period upsert %s/%s failed: %s",
                            league_slug, gid, e)
            skipped += 1
        if idx % 100 == 0:
            conn.commit()
            elapsed = time.monotonic() - started
            rate = idx / max(0.1, elapsed)
            eta = (total - idx) / max(0.1, rate)
            logger.info("[%s] periods: %d/%d (%.1f g/s, ~%.0fs left, "
                        "%d filled)",
                        league_slug, idx, total, rate, eta, filled)
        time.sleep(throttle)
    conn.commit()
    logger.info("[%s] period backfill done: %d filled, %d skipped",
                league_slug, filled, skipped)
    return {"games": total, "filled": filled, "skipped": skipped}


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.sports._thescore_periods")
    ap.add_argument("league", choices=("ahl", "pwhl"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--throttle", type=float, default=0.25)
    ap.add_argument("--all", action="store_true",
                     help="re-fetch even games that already have periods")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    if args.league == "ahl":
        from .ahl.db import get_conn
    else:
        from .pwhl.db import get_conn
    conn = get_conn()
    # Trigger schema migration via init_schema-like path (touching the
    # table works because get_conn() runs init_schema in module init).
    from . import _thescore_ingest as _ti
    _ti.init_schema(conn,
                     teams_table=f"teams" if args.league != "ahl" else "teams",
                     games_table="games")
    res = backfill_periods(args.league, conn, games_table="games",
                            limit=args.limit, throttle=args.throttle,
                            only_missing=not args.all)
    print(res)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
