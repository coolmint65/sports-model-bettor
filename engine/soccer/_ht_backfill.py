"""Half-time score backfill from ESPN's summary endpoint.

The ``/scoreboard`` endpoint we ingest from doesn't include period-by-
period scores for soccer (``competitor.linescores`` is null). The
``/summary`` endpoint does — each finalized event ships
``header.competitions[0].competitors[*].linescores = [H1, H2, …]``.

Backfill pattern mirrors ``engine.basketball._euroleague_periods_backfill``:
walk every finalized match missing ``home_score_ht``, hit the summary
endpoint per match, write H1 totals into ``home_score_ht`` and
``away_score_ht``. One HTTP call per match; throttled.

CLI::

    python -m engine.soccer._ht_backfill mls
    python -m engine.soccer._ht_backfill mls --limit 100
    python -m engine.soccer._ht_backfill --all
"""
from __future__ import annotations

import argparse
import json
import logging
import time
import urllib.request
from pathlib import Path

from ._config import LEAGUE_REGISTRY, get_league_config
from ._db import get_conn

logger = logging.getLogger(__name__)


def _summary_url(league: str, event_id: str) -> str:
    cfg = get_league_config(league)
    espn_path = (cfg.get("espn_league_path") or cfg.get("espn_slug")
                  or cfg.get("slug"))
    if not espn_path:
        raise ValueError(f"no espn_league_path for {league}")
    return (f"https://site.api.espn.com/apis/site/v2/sports/soccer/"
            f"{espn_path}/summary?event={event_id}")


def _fetch_summary(league: str, event_id: str, timeout: float = 15.0) -> dict | None:
    url = _summary_url(league, event_id)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        logger.debug("summary fetch failed %s/%s: %s", league, event_id, e)
        return None


def _extract_ht(summary: dict) -> tuple[int, int] | None:
    """Pull (home_h1, away_h1) from a summary payload. Returns None
    when linescores are missing or malformed."""
    header = summary.get("header") or {}
    comps = header.get("competitions") or [{}]
    if not comps:
        return None
    competitors = comps[0].get("competitors") or []
    home_h1 = away_h1 = None
    for c in competitors:
        ls = c.get("linescores") or []
        if not ls:
            continue
        try:
            v = int((ls[0] or {}).get("displayValue") or 0)
        except (TypeError, ValueError):
            continue
        if c.get("homeAway") == "home":
            home_h1 = v
        elif c.get("homeAway") == "away":
            away_h1 = v
    if home_h1 is None or away_h1 is None:
        return None
    return home_h1, away_h1


def _extract_periods(summary: dict) -> dict | None:
    """Pull all period totals + shootout from the summary linescores.
    Returns ``{home_ht, away_ht, home_et, away_et, home_pens, away_pens,
    has_extra_time, has_shootout}`` — any missing field is None. Used
    by the KO-stage ``To Advance`` settler when the match went past
    regulation.

    ESPN's linescores array for soccer indexes:
      [0]=H1 total, [1]=H2 total, [2]=ET1 total, [3]=ET2 total,
      [4]=shootout (only present when it went to pens)
    """
    header = summary.get("header") or {}
    comps = header.get("competitions") or [{}]
    if not comps:
        return None
    competitors = comps[0].get("competitors") or []
    out: dict = {}
    for c in competitors:
        side = c.get("homeAway")
        if side not in ("home", "away"):
            continue
        ls = c.get("linescores") or []
        def _v(idx: int) -> int | None:
            if idx >= len(ls):
                return None
            try:
                return int((ls[idx] or {}).get("displayValue") or 0)
            except (TypeError, ValueError):
                return None
        # H1 + H2 always present on finalized games; ET1/ET2 only when
        # regulation was tied and the fixture went past 90'. Shootout
        # slot only when ET was also tied.
        h1, h2 = _v(0), _v(1)
        et1, et2 = _v(2), _v(3)
        pens = _v(4)
        out[f"{side}_ht"] = h1
        # cumulative after ET = H1 + H2 + ET1 + ET2
        et_total = None
        if h1 is not None and h2 is not None and et1 is not None and et2 is not None:
            et_total = h1 + h2 + et1 + et2
        out[f"{side}_et"] = et_total
        out[f"{side}_pens"] = pens
    out["has_extra_time"] = 1 if (out.get("home_et") is not None
                                    and out.get("away_et") is not None) else 0
    out["has_shootout"] = 1 if (out.get("home_pens") is not None
                                  and out.get("away_pens") is not None) else 0
    return out


def backfill(league: str, *, limit: int | None = None,
              throttle: float = 0.1,
              include_live: bool = True) -> dict:
    """Walk every match in ``league`` with NULL ``home_score_ht``,
    fetch the summary endpoint, persist the HT scores. Returns counts.

    By default also walks live (in-2H) matches so the live halftime
    predictor has HT scores available the moment 2H starts. Set
    ``include_live=False`` to restrict to finals only (backfill jobs).

    ``throttle`` is the per-match sleep — at the default 0.1s a full
    1272-match league takes ~2 minutes."""
    if league not in LEAGUE_REGISTRY:
        raise KeyError(f"unknown soccer league {league!r}")
    conn = get_conn(league)
    if include_live:
        # 'final' OR 'live'/'halftime' with at least one period of play
        # behind it (HT-or-later). Use home_score not NULL as a proxy
        # for "the match has started scoring" — matches in 1st half
        # show NULL home_score in scoreboard data until first goal.
        sql = (
            "SELECT id, external_id FROM matches "
            "WHERE home_score_ht IS NULL "
            "  AND status IN ('final', 'live', 'halftime') "
            "ORDER BY date DESC"
        )
    else:
        sql = (
            "SELECT id, external_id FROM matches "
            "WHERE status = 'final' AND home_score IS NOT NULL "
            "  AND home_score_ht IS NULL "
            "ORDER BY date DESC"
        )
    if limit:
        sql += f" LIMIT {int(limit)}"
    rows = conn.execute(sql).fetchall()
    totals = {"checked": len(rows), "written": 0, "skipped": 0, "errors": 0}
    if not rows:
        logger.info("[%s] ht_backfill: no rows to process", league)
        return totals
    t0 = time.time()
    for i, r in enumerate(rows):
        mid = r["id"]
        ext = r["external_id"] or str(mid)
        summary = _fetch_summary(league, str(ext))
        if not summary:
            totals["errors"] += 1
        else:
            ht = _extract_ht(summary)
            if ht is None:
                totals["skipped"] += 1
            else:
                # Grab HT + any ET/PEN data in a single UPDATE so KO
                # settle for the ADVANCE market doesn't defer on ET
                # games. Regulation-only matches leave the ET/PEN
                # columns as their existing values (NULL for finals
                # that never went past 90').
                periods = _extract_periods(summary) or {}
                conn.execute(
                    "UPDATE matches SET "
                    "  home_score_ht = ?, away_score_ht = ?, "
                    "  home_score_et = COALESCE(?, home_score_et), "
                    "  away_score_et = COALESCE(?, away_score_et), "
                    "  home_pens     = COALESCE(?, home_pens), "
                    "  away_pens     = COALESCE(?, away_pens), "
                    "  has_extra_time = COALESCE(?, has_extra_time), "
                    "  has_shootout   = COALESCE(?, has_shootout) "
                    "WHERE id = ?",
                    (
                        int(ht[0]), int(ht[1]),
                        periods.get("home_et"),
                        periods.get("away_et"),
                        periods.get("home_pens"),
                        periods.get("away_pens"),
                        periods.get("has_extra_time"),
                        periods.get("has_shootout"),
                        int(mid),
                    ),
                )
                totals["written"] += 1
        if i and i % 100 == 0:
            conn.commit()
            logger.info("[%s] ht_backfill: %d/%d  w=%d s=%d e=%d (%.0fs)",
                        league, i, len(rows), totals["written"],
                        totals["skipped"], totals["errors"], time.time() - t0)
        time.sleep(throttle)
    conn.commit()
    logger.info("[%s] ht_backfill done: %s (%.0fs)", league, totals,
                time.time() - t0)
    return totals


def _cli() -> int:
    ap = argparse.ArgumentParser(prog="engine.soccer._ht_backfill")
    ap.add_argument("league", nargs="?")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--throttle", type=float, default=0.1)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    if args.all:
        leagues = list(LEAGUE_REGISTRY.keys())
    elif args.league:
        leagues = [args.league]
    else:
        ap.error("specify --all or a league name")
        return 1
    for lg in leagues:
        try:
            totals = backfill(lg, limit=args.limit, throttle=args.throttle)
            print(f"[{lg}] {totals}")
        except Exception as e:
            print(f"[{lg}] FAILED: {e}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
