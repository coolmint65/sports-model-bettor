"""OWGR (Official World Golf Ranking) scraper.

Discovered 2026-05-14 via Playwright network capture — the public API
under ``apiweb.owgr.com`` is accessible to a browser-impersonating
client (curl_cffi w/ firefox133). No auth required; just needs Origin /
Referer headers to match the public site.

OWGR is the closest thing to a global skill prior for the men's pro
game — combines results across PGA Tour, DP World, Asian, etc., into
a single weighted points average per player. Used as a Bayesian prior
in our predictor for players with thin PGA-only history (LIV defectors,
Korn Ferry crossovers, international entrants at majors).

Public entry: fetch_rankings(limit=300) → list of dicts with rank,
player name, country, points_average.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_API = "https://apiweb.owgr.com/api/owgr/rankings/getRankings"
_HEADERS = {
    "Origin": "https://www.owgr.com",
    "Referer": "https://www.owgr.com/",
    "Accept": "application/json",
}


def fetch_rankings(limit: int = 300, *, region_id: int = 0,
                   country_id: int = 0) -> list[dict]:
    """Returns the top ``limit`` players from the current OWGR week.

    Output shape (subset of OWGR's blob):
        [{
          'rank', 'player_id', 'name', 'first_name', 'last_name',
          'country', 'country_code', 'points_average',
          'points_total', 'last_week_rank', 'week_id',
        }, ...]
    """
    try:
        from curl_cffi import requests as _cc
    except ImportError:
        logger.warning("OWGR: curl_cffi not installed")
        return []
    params = {
        "regionId": region_id,
        "countryId": country_id,
        "pageSize": int(limit),
        "pageNumber": 1,
        "sortString": "Rank ASC",
    }
    try:
        r = _cc.get(_API, params=params, headers=_HEADERS,
                     impersonate="firefox133", timeout=15)
    except Exception as e:
        logger.warning("OWGR fetch failed: %s", e)
        return []
    if r.status_code != 200:
        logger.warning("OWGR HTTP %s", r.status_code)
        return []
    try:
        data = r.json()
    except Exception as e:
        logger.warning("OWGR bad JSON: %s", e)
        return []
    rows = data.get("rankingsList") or []
    out: list[dict] = []
    for row in rows:
        player = row.get("player") or {}
        country = player.get("country") or {}
        out.append({
            "rank":            row.get("rank"),
            "player_id":       player.get("id"),
            "name":            player.get("fullName") or "",
            "first_name":      player.get("firstName") or "",
            "last_name":       player.get("lastName") or "",
            "is_amateur":      bool(player.get("isAmateur")),
            "country":         country.get("name") or "",
            "country_code":    country.get("iocCode") or "",
            "points_average":  row.get("pointsAverage"),
            "points_total":    row.get("pointsTotal"),
            "last_week_rank":  row.get("lastWeekRank"),
            "week_id":         row.get("weekId"),
            "week_end_date":   row.get("weekEndDate") or "",
        })
    return out
