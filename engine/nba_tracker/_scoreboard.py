"""ESPN NBA scoreboard fetcher + Q1 score parser."""

from __future__ import annotations
import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)


def _fetch_nba_scoreboard(date: str) -> list[dict]:
    """Fetch NBA scoreboard from ESPN for a given date (YYYY-MM-DD)."""
    espn_date = date.replace("-", "")
    url = (
        "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
        f"/scoreboard?dates={espn_date}"
    )
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "curl/8.4.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("events", [])
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to fetch NBA scoreboard for %s: %s", date, e)
        return []


def _parse_q1_scores(event: dict) -> dict | None:
    """Parse Q1 scores and metadata from an ESPN event.

    Returns dict with home_abbr, away_abbr, home_q1, away_q1, etc.
    Returns None when Q1 isn't LOCKED yet (game in progress, still in
    Q1 with seconds left — score can still change). Q1 is locked when
    either:
      - state == "post" (game complete), OR
      - state == "in" AND current period > 1 (Q2+ has tipped off)
    """
    comp = event.get("competitions", [{}])[0]
    status = comp.get("status", {})
    status_type = status.get("type", {})
    state = status_type.get("state", "pre")
    cur_period = status.get("period") or 0

    if state == "pre":
        return None
    q1_locked = (state == "post") or (state == "in" and cur_period > 1)
    if not q1_locked:
        return None

    is_completed = (state == "post")
    result = {"game_id": event.get("id", ""), "is_completed": is_completed}

    for team_entry in comp.get("competitors", []):
        team = team_entry.get("team", {})
        abbr = team.get("abbreviation", "")
        is_home = team_entry.get("homeAway") == "home"
        score = 0
        raw_score = team_entry.get("score", "0")
        if isinstance(raw_score, (int, str)) and str(raw_score).isdigit():
            score = int(raw_score)

        linescores = team_entry.get("linescores", [])
        q1 = None
        if linescores:
            val = linescores[0].get("value")
            if val is not None:
                q1 = int(val)
        # 1st half = Q1+Q2, locked once Q3 has tipped (or the game is over). Feeds H1_* grading and,
        # with the final, H2_* (2nd half incl. OT = final - 1H). Added 2026-11-01: H1/H2 picks reached
        # nba_paper_picks via first-pick-wins and sat ungraded.
        h1 = None
        if len(linescores) >= 2 and (state == "post" or cur_period > 2):
            v1, v2 = linescores[0].get("value"), linescores[1].get("value")
            if v1 is not None and v2 is not None:
                h1 = int(v1) + int(v2)

        if is_home:
            result["home_abbr"] = abbr
            result["home_score"] = score
            result["home_q1"] = q1
            result["home_h1"] = h1
        else:
            result["away_abbr"] = abbr
            result["away_score"] = score
            result["away_q1"] = q1
            result["away_h1"] = h1

    if result.get("home_q1") is None or result.get("away_q1") is None:
        return None

    result["q1_total"] = result["home_q1"] + result["away_q1"]
    result["q1_margin"] = result["home_q1"] - result["away_q1"]

    return result
