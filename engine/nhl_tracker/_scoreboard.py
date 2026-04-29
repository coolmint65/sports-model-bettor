"""ESPN NHL scoreboard fetcher."""

from __future__ import annotations
import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)


def _fetch_nhl_scoreboard(date: str) -> list[dict]:
    """Fetch NHL scoreboard from ESPN for a given date."""
    espn_date = date.replace("-", "")
    url = f"https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates={espn_date}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("events", [])
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to fetch NHL scoreboard for %s: %s", date, e)
        return []
