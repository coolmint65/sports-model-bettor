"""Horses tab (PAPER, display-only, added 2026-11-01).

Proxies the Pi's ``GET /api/horses-v2`` (pl-server :8899), which serves the summary written every
2 min by ``~/pibot/scanners/racing/horses-v2-paper.py``: FanDuel Racing PLACE/SHOW pool-value plays
("Dr. Z" style, discounted Harville from the win pool), graded at the official FDR payouts, plus the
favourite baselines and data coverage. PAPER ONLY -- nothing here places a bet.

READ-ONLY. 5 s timeout, 45 s cache; if the Pi is down the last good payload is served with
``stale: True``, or an empty ``ok: False`` payload if there never was one.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.request

from fastapi import APIRouter

logger = logging.getLogger(__name__)
router = APIRouter()

PI_URL = os.getenv("HORSES_V2_URL", "http://100.118.96.46:8899/api/horses-v2")
CACHE_SECONDS = 45
TIMEOUT_SECONDS = 5

_lock = threading.Lock()
_cache: dict = {"t": 0.0, "data": None}

_EMPTY = {"plays_today": [], "upcoming": [], "record": [], "baselines": [], "coverage": None, "backtest": None}


def _fetch() -> dict:
    req = urllib.request.Request(PI_URL, headers={"User-Agent": "pickmint/1.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as r:
        return json.loads(r.read().decode("utf-8"))


@router.get("/api/horses")
def api_horses() -> dict:
    now = time.time()
    with _lock:
        if _cache["data"] is not None and now - _cache["t"] < CACHE_SECONDS:
            return {**_cache["data"], "ok": True, "stale": False, "proxy_age_seconds": round(now - _cache["t"])}
        try:
            data = _fetch()
            if data.get("ok") is False or ("record" not in data and not data.get("building")):
                raise RuntimeError(data.get("error") or "Pi returned no record")
            _cache["data"], _cache["t"] = data, now
            return {**data, "ok": True, "stale": False, "proxy_age_seconds": 0}
        except Exception as e:  # Pi down / timeout / bad JSON -> degrade
            logger.warning("horses: Pi fetch failed: %s", e)
            if _cache["data"] is not None:
                return {**_cache["data"], "ok": True, "stale": True, "proxy_age_seconds": round(now - _cache["t"]),
                        "error": str(e)[:200]}
            return {**_EMPTY, "ok": False, "stale": True, "error": "Pi horses feed unreachable: %s" % str(e)[:200]}
