"""
Local data layer.

Team stats are stored as JSON files under data/teams/{LEAGUE}/{team_key}.json.
This module handles loading, saving, and searching team data.
"""

import json
import os
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
TEAMS_DIR = DATA_DIR / "teams"


def _team_path(league: str, team_key: str) -> Path:
    return TEAMS_DIR / league.upper() / f"{team_key.lower()}.json"


def load_team(league: str, team_key: str) -> dict | None:
    """Load a team's data from local JSON. Returns None if not found."""
    path = _team_path(league, team_key)
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def save_team(league: str, team_key: str, data: dict) -> None:
    """Save team data to local JSON."""
    path = _team_path(league, team_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def list_teams(league: str) -> list[dict]:
    """List all teams for a league, returning [{key, name, record}]."""
    league_dir = TEAMS_DIR / league.upper()
    if not league_dir.exists():
        return []
    teams = []
    for f in sorted(league_dir.glob("*.json")):
        with open(f) as fh:
            data = json.load(fh)
            teams.append({
                "key": f.stem,
                "name": data.get("name", f.stem),
                "record": data.get("record", ""),
            })
    return teams


def search_teams(league: str, query: str) -> list[dict]:
    """Fuzzy search teams by name, abbreviation, or city."""
    query = query.lower().strip()
    results = []
    for team in list_teams(league):
        full = load_team(league, team["key"])
        searchable = " ".join([
            full.get("name", ""),
            full.get("abbreviation", ""),
            full.get("city", ""),
            full.get("short_name", ""),
            team["key"],
        ]).lower()
        if query in searchable:
            results.append(team)
    return results


def get_team_stats(league: str, team_key: str) -> dict:
    """Get the stats block for a team, with safe defaults."""
    team = load_team(league, team_key)
    if not team:
        return {}
    return team.get("stats", {})


def get_league_averages(league: str) -> dict:
    """Compute league averages from all team data."""
    teams = list_teams(league)
    if not teams:
        return {}

    totals = {}
    count = 0
    for t in teams:
        stats = get_team_stats(league, t["key"])
        if not stats:
            continue
        count += 1
        for k, v in stats.items():
            if isinstance(v, (int, float)):
                totals[k] = totals.get(k, 0) + v

    if count == 0:
        return {}
    return {k: round(v / count, 3) for k, v in totals.items()}
