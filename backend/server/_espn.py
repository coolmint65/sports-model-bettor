"""Shared ESPN helpers used by every per-sport router.

Lives outside ``__init__.py`` so the per-sport route modules
(``routes_nba``, ``routes_nhl``, ``routes_tennis`` ...) can import it
without circular dependency on the FastAPI app instance.

ESPN_BASE: root for ESPN's public site API.
_fetch_espn_json: defensive JSON fetch with one retry + logging.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.request


logger = logging.getLogger(__name__)

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"


def _fetch_espn_json(url: str) -> dict | None:
    for attempt in range(2):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            # Both attempts log so an ESPN outage is visible. Without
            # this the only symptom is empty scoreboards / fallbacks
            # that look like "the model thinks no games tonight".
            level = logging.DEBUG if attempt == 0 else logging.WARNING
            logger.log(level, "ESPN fetch failed (attempt %d) for %s: %s",
                       attempt + 1, url, e)
            if attempt == 0:
                time.sleep(1)
    return None
