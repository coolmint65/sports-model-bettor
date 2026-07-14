"""PGA Tour schedule scraper — extracts course names + tournament
metadata from pgatour.com that ESPN strips on its year-level
scoreboard endpoint.

Why this exists: our golf framework backfills 11 years from ESPN, but
the year-level `?dates=YYYY` scoreboard response doesn't include
`competition.course`. Without course attribution, we can't do
course-history slicing in the predictor. PGA Tour publishes a full
schedule with `courseData.name` per tournament via the Next.js page
JSON. Free, no auth, accessible via curl_cffi (Cloudflare-gated).

Output shape:
    [{ "name", "year", "month", "displayDate", "courseName", "city",
       "stateCode", "country" }, ...]

Coverage caveat: pgatour.com's schedule page always returns the
current PGA Tour season (their `?season=YYYY` param is ignored). For
historical seasons we'd need per-tournament leaderboard URLs (deferred
to a v2 backfill pass). For current-season tournaments — including
tonight's PGA Championship — this gives clean course attribution.
"""
from __future__ import annotations

import json
import logging
import re

logger = logging.getLogger(__name__)

_SCHEDULE_URL = "https://www.pgatour.com/schedule.html"


def fetch_schedule() -> list[dict]:
    """Returns the current PGA Tour season's tournaments. Each row
    has at minimum ``name``, ``courseName``; ``city`` / ``stateCode``
    / ``country`` may be populated."""
    try:
        from curl_cffi import requests as _cc
    except ImportError:
        logger.warning("pgatour: curl_cffi not installed")
        return []
    try:
        r = _cc.get(_SCHEDULE_URL, impersonate="firefox133", timeout=15)
    except Exception as e:
        logger.warning("pgatour schedule fetch failed: %s", e)
        return []
    if r.status_code != 200:
        logger.warning("pgatour schedule HTTP %s", r.status_code)
        return []
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>([\s\S]+?)</script>',
                   r.text)
    if not m:
        logger.warning("pgatour schedule: no __NEXT_DATA__ blob")
        return []
    try:
        data = json.loads(m.group(1))
    except json.JSONDecodeError as e:
        logger.warning("pgatour schedule: bad JSON (%s)", e)
        return []
    queries = (data.get("props", {})
                   .get("pageProps", {})
                   .get("dehydratedState", {})
                   .get("queries") or [])
    sched_q = next(
        (q for q in queries
         if (q.get("queryKey") or [None])[0] == "schedule"),
        None,
    )
    if not sched_q:
        return []
    tournaments = (sched_q.get("state", {}).get("data", {})
                          .get("tournaments") or [])
    out: list[dict] = []
    for t in tournaments:
        course = t.get("courseData") or {}
        out.append({
            "name": t.get("name") or "",
            "year": t.get("year") or "",
            "month": t.get("month") or "",
            "displayDate": t.get("displayDate") or "",
            "courseName": course.get("name") or "",
            "city": course.get("city") or "",
            "stateCode": course.get("stateCode") or "",
            "country": course.get("country") or "",
            "tournamentId": t.get("tournamentId") or "",
        })
    return out
