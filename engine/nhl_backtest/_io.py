"""Backtest data loading: historical odds + completed games."""

from __future__ import annotations
import json
import logging
import urllib.request
from datetime import datetime, timedelta

from ._helpers import SEASON

logger = logging.getLogger(__name__)


def _load_odds_map(games: list[dict]) -> dict:
    """Pre-load all historical odds for the games being backtested.

    Returns a dict keyed by (game_date, home_abbr, away_abbr) -> odds row dict.
    """
    try:
        from ..nhl_db import get_conn
    except Exception:
        return {}

    conn = get_conn()
    try:
        rows = conn.execute("SELECT * FROM nhl_odds").fetchall()
    except Exception:
        return {}

    odds_map = {}
    for r in rows:
        row = dict(r)
        key = (row.get("game_date", ""), row.get("home_abbr", "").upper(),
               row.get("away_abbr", "").upper())
        odds_map[key] = row
    return odds_map


def _lookup_game_odds(odds_map: dict, game_date: str,
                      home_abbr: str, away_abbr: str) -> dict | None:
    """Look up real historical odds for a specific game.
    Returns the odds row dict if found, else None."""
    key = (game_date, home_abbr.upper(), away_abbr.upper())
    return odds_map.get(key)


def _load_games_from_db(days: int | None = None,
                        season: int | None = None) -> list[dict]:
    """Load completed NHL games from the nhl_games DB table."""
    try:
        from ..nhl_db import get_conn
    except Exception:
        return []

    conn = get_conn()
    yr = season or SEASON

    # NHL API stores season as YYYYYYYY (e.g. 20252026)
    season_ids = [yr]
    if yr < 10000:
        season_ids.append(yr * 10000 + yr + 1)
        season_ids.append((yr - 1) * 10000 + yr)

    if days and days > 0:
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT g.*,
                   ht.abbreviation as home_abbr, ht.name as home_name,
                   at.abbreviation as away_abbr, at.name as away_name
            FROM nhl_games g
            LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
            LEFT JOIN nhl_teams at ON g.away_team_id = at.id
            WHERE g.status = 'final' AND g.date >= ?
            ORDER BY g.date
        """, (start_date,)).fetchall()
    else:
        placeholders = ",".join("?" for _ in season_ids)
        rows = conn.execute(f"""
            SELECT g.*,
                   ht.abbreviation as home_abbr, ht.name as home_name,
                   at.abbreviation as away_abbr, at.name as away_name
            FROM nhl_games g
            LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
            LEFT JOIN nhl_teams at ON g.away_team_id = at.id
            WHERE g.status = 'final' AND g.season IN ({placeholders})
            ORDER BY g.date
        """, season_ids).fetchall()

    return [dict(r) for r in rows]


def _load_games_from_api(days: int = 30) -> list[dict]:
    """Fetch recent completed games from the ESPN API when DB is empty."""
    games = []
    today = datetime.utcnow().date()

    for day_offset in range(days, 0, -1):
        check_date = today - timedelta(days=day_offset)
        espn_date = check_date.strftime("%Y%m%d")
        date_str = check_date.strftime("%Y-%m-%d")
        url = (
            "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl"
            f"/scoreboard?dates={espn_date}"
        )
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())

            for event in data.get("events", []):
                status_type = (event.get("status", {})
                               .get("type", {}).get("name", ""))
                if status_type != "STATUS_FINAL":
                    continue

                for comp in event.get("competitions", []):
                    home_team = away_team = None
                    home_score = away_score = None

                    for team_entry in comp.get("competitors", []):
                        t = team_entry.get("team", {})
                        abbr = t.get("abbreviation", "")
                        score = int(team_entry.get("score", 0))
                        if team_entry.get("homeAway") == "home":
                            home_team = abbr
                            home_score = score
                        else:
                            away_team = abbr
                            away_score = score

                    if home_team and away_team and home_score is not None:
                        games.append({
                            "date": date_str,
                            "home_abbr": home_team,
                            "away_abbr": away_team,
                            "home_score": home_score,
                            "away_score": away_score,
                            "status": "final",
                        })
        except Exception as e:
            logger.debug("Failed to fetch ESPN scoreboard for %s: %s",
                         date_str, e)
            continue

    return games
