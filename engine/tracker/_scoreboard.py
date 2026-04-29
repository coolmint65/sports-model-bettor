"""ESPN scoreboard fallback for the MLB tracker.

Used when the games table doesn't yet have today's slate (sync ran late
or the user is calling tracker before fetch_schedule completed). The
scoreboard fetch returns enough information for `record_picks` to feed
the picker — abbreviations, team_ids, probable pitchers, venue.
"""

from __future__ import annotations
import json
import logging
import urllib.error
import urllib.request

from ._helpers import ESPN_BASE

logger = logging.getLogger(__name__)


def _fetch_espn_scoreboard(date: str) -> list[dict]:
    """Fetch MLB scoreboard from ESPN for a given date."""
    espn_date = date.replace("-", "")
    url = f"{ESPN_BASE}/baseball/mlb/scoreboard?dates={espn_date}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.warning("ESPN scoreboard fetch failed: %s", e)
        return []

    games = []
    for event in data.get("events", []):
        comps = event.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue

        home_team, away_team = None, None
        for c in competitors:
            team = c.get("team", {})
            entry = {
                "abbreviation": team.get("abbreviation", ""),
                "name": team.get("displayName", ""),
                "team_id": None,
            }
            # Resolve team_id from DB. ESPN sometimes reports abbrs that
            # differ from what MLB Stats API seeds into the teams table
            # (CWS vs CHW, AZ vs ARI, ATH vs OAK). Walk every known alias
            # so the fallback finds the right team_id instead of leaving
            # it None and dropping the game.
            from ..db import get_conn as _gc
            from ..abbr import aliases_for as _aliases
            conn = _gc()
            row = None
            for candidate in _aliases(entry["abbreviation"], sport="mlb"):
                row = conn.execute(
                    "SELECT mlb_id FROM teams WHERE abbreviation = ?",
                    (candidate,),
                ).fetchone()
                if row:
                    break
            # Last-resort: match by displayName.
            if not row and entry["name"]:
                row = conn.execute(
                    "SELECT mlb_id FROM teams WHERE name = ? OR name LIKE ?",
                    (entry["name"], f"%{entry['name']}%"),
                ).fetchone()
            if row:
                entry["team_id"] = row["mlb_id"]
            else:
                logger.warning(
                    "ESPN fallback: could not resolve team_id for '%s' (%s) — add to engine/abbr.py",
                    entry["abbreviation"], entry["name"],
                )

            if c.get("homeAway") == "home":
                home_team = entry
            else:
                away_team = entry

        if not home_team or not away_team:
            continue

        status = comp.get("status", {}).get("type", {})

        # Probable pitchers
        home_pid, away_pid = None, None
        for c in competitors:
            pp = c.get("probables", [])
            if pp:
                pid = pp[0].get("athlete", {}).get("id")
                if c.get("homeAway") == "home":
                    home_pid = pid
                else:
                    away_pid = pid

        games.append({
            "id": event.get("id", ""),
            "home": home_team,
            "away": away_team,
            "home_pitcher": {"id": home_pid} if home_pid else {},
            "away_pitcher": {"id": away_pid} if away_pid else {},
            "venue": comp.get("venue", {}).get("fullName", ""),
            "status": {
                "state": status.get("state", "pre"),
                "completed": status.get("completed", False),
            },
        })

    return games
