"""Public Betting tab (display-only, added 2026-11-01).

Proxies the Pi's ``GET /api/hr-public`` (pl-server :8899), which reads the Hard Rock
public-betting ("Follow or Fade") snapshots that ``~/pibot/scanners/hr-public-snapshot.py``
logs every 30 min: lopsided spreads (bet counts per side), most-bet games by $ handle,
most-bet leagues, popular overs / underdogs / props, and the fade-the-public record.

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

PI_URL = os.getenv("HR_PUBLIC_URL", "http://100.118.96.46:8899/api/hr-public")
CACHE_SECONDS = 45
TIMEOUT_SECONDS = 5

_lock = threading.Lock()
_cache: dict = {"t": 0.0, "data": None}


def _fetch() -> dict:
    req = urllib.request.Request(PI_URL, headers={"User-Agent": "pickmint/1.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as r:
        return json.loads(r.read().decode("utf-8"))


@router.get("/api/public-betting")
def api_public_betting() -> dict:
    now = time.time()
    with _lock:
        if _cache["data"] is not None and now - _cache["t"] < CACHE_SECONDS:
            return {**_cache["data"], "stale": False, "proxy_age_seconds": round(now - _cache["t"])}
        try:
            data = _fetch()
            if not data.get("ok"):
                raise RuntimeError(data.get("error") or "Pi returned ok=false")
            _cache["data"], _cache["t"] = data, now
            return {**data, "stale": False, "proxy_age_seconds": 0}
        except Exception as e:  # Pi down / timeout / bad JSON -> degrade
            logger.warning("public-betting: Pi fetch failed: %s", e)
            if _cache["data"] is not None:
                return {**_cache["data"], "stale": True, "proxy_age_seconds": round(now - _cache["t"]),
                        "error": str(e)[:200]}
            return {"ok": False, "stale": True, "error": "Pi public-betting feed unreachable: %s" % str(e)[:200],
                    "latest": None, "history": {}, "record": None}
