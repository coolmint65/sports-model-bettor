"""
FastAPI backend for Sports Matchup Engine.
Serves league/team data, scoreboard, and predictions via REST API.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import json
import time
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from engine.leagues import LEAGUES, list_leagues
from engine.data import list_teams, search_teams, load_team
from engine.predict import predict_matchup

logger = logging.getLogger(__name__)

app = FastAPI(title="Sports Matchup Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── ESPN sport/league slug mapping (mirrors scrapers/config.py) ──
ESPN_SLUGS = {
    "NFL": ("football", "nfl"),
    "CFB": ("football", "college-football"),
    "NBA": ("basketball", "nba"),
    "NCAAB": ("basketball", "mens-college-basketball"),
    "NCAAW": ("basketball", "womens-college-basketball"),
    "MLB": ("baseball", "mlb"),
    "NHL": ("hockey", "nhl"),
    "EPL": ("soccer", "eng.1"),
    "UCL": ("soccer", "uefa.champions"),
    "LALIGA": ("soccer", "esp.1"),
    "BUNDESLIGA": ("soccer", "ger.1"),
    "MLS": ("soccer", "usa.1"),
    "NWSL": ("soccer", "usa.nwsl"),
    "LIGAMX": ("soccer", "mex.1"),
}

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"

# Simple in-memory scoreboard cache: {league_key: (timestamp, data)}
_scoreboard_cache: dict[str, tuple[float, list]] = {}
CACHE_TTL = 120  # seconds


def _fetch_espn_json(url: str) -> dict | None:
    """Fetch JSON from ESPN with a single retry."""
    for attempt in range(2):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode())
        except Exception:
            if attempt == 0:
                time.sleep(1)
    return None


def _team_key_from_name(name: str) -> str:
    """Derive a team_key from display name (mirrors scrapers logic)."""
    import re
    import unicodedata
    key = name.lower().strip()
    key = unicodedata.normalize("NFKD", key)
    key = "".join(c for c in key if not unicodedata.combining(c))
    key = key.replace("&", "and")
    key = re.sub(r"[^a-z0-9]+", "_", key)
    return key.strip("_")


def _parse_scoreboard(data: dict, league_key: str) -> list[dict]:
    """Parse ESPN scoreboard response into a clean games list."""
    events = data.get("events", [])
    games = []

    for event in events:
        comps = event.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue

        status = comp.get("status", {})
        status_type = status.get("type", {})

        home_team = None
        away_team = None
        for c in competitors:
            team = c.get("team", {})
            raw_score = c.get("score", "0")
            if isinstance(raw_score, dict):
                score = raw_score.get("displayValue", raw_score.get("value", "0"))
            else:
                score = str(raw_score)

            short_name = team.get("shortDisplayName", team.get("name", ""))
            entry = {
                "id": team.get("id", ""),
                "name": team.get("displayName", team.get("name", "")),
                "short_name": short_name,
                "abbreviation": team.get("abbreviation", ""),
                "key": _team_key_from_name(short_name),
                "score": score,
                "record": c.get("records", [{}])[0].get("summary", "") if c.get("records") else "",
                "logo": team.get("logo", team.get("logos", [{}])[0].get("href", "")) if team.get("logos") else "",
                "winner": c.get("winner", False),
            }

            if c.get("homeAway") == "home":
                home_team = entry
            else:
                away_team = entry

        if not home_team or not away_team:
            continue

        game = {
            "id": event.get("id", ""),
            "date": event.get("date", ""),
            "name": event.get("name", ""),
            "short_name": event.get("shortName", ""),
            "home": home_team,
            "away": away_team,
            "status": {
                "state": status_type.get("state", "pre"),  # pre, in, post
                "detail": status.get("type", {}).get("shortDetail",
                          status.get("type", {}).get("detail", "")),
                "description": status_type.get("description", ""),
                "completed": status_type.get("completed", False),
                "period": status.get("period", 0),
                "clock": status.get("displayClock", ""),
            },
            "venue": comp.get("venue", {}).get("fullName", ""),
            "broadcast": "",
        }

        # Extract broadcast info
        broadcasts = comp.get("broadcasts", [])
        if broadcasts:
            names = []
            for b in broadcasts:
                for n in b.get("names", []):
                    names.append(n)
            game["broadcast"] = ", ".join(names[:2])

        # Extract odds if available
        odds = comp.get("odds", [])
        if odds:
            o = odds[0]
            game["odds"] = {
                "spread": o.get("details", ""),
                "over_under": o.get("overUnder", None),
            }

        games.append(game)

    return games


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/leagues")
def get_leagues():
    """Return all leagues grouped by sport."""
    result = []
    for key in list_leagues():
        league = LEAGUES[key]
        result.append({
            "key": key,
            "name": league["name"],
            "sport": league["sport"],
        })
    return result


@app.get("/api/leagues/{league_key}/teams")
def get_teams(league_key: str):
    """Return all teams for a league."""
    key = league_key.upper()
    if key not in LEAGUES:
        raise HTTPException(status_code=404, detail=f"League '{key}' not found")
    return list_teams(key)


@app.get("/api/leagues/{league_key}/teams/search")
def search(league_key: str, q: str = ""):
    """Search teams by name/city/abbreviation."""
    key = league_key.upper()
    if key not in LEAGUES:
        raise HTTPException(status_code=404, detail=f"League '{key}' not found")
    if not q.strip():
        return list_teams(key)
    return search_teams(key, q)


@app.get("/api/leagues/{league_key}/teams/{team_key}")
def get_team(league_key: str, team_key: str):
    """Return full team data."""
    team = load_team(league_key.upper(), team_key.lower())
    if not team:
        raise HTTPException(status_code=404, detail=f"Team '{team_key}' not found")
    return team


@app.get("/api/leagues/{league_key}/scoreboard")
def get_scoreboard(league_key: str, dates: str = Query(default="")):
    """
    Return today's games for a league from ESPN.
    Optional dates param in YYYYMMDD format.
    Cached for 2 minutes.
    """
    key = league_key.upper()
    if key not in ESPN_SLUGS:
        raise HTTPException(status_code=404, detail=f"League '{key}' not found")

    cache_key = f"{key}:{dates}"
    now = time.time()
    if cache_key in _scoreboard_cache:
        ts, cached = _scoreboard_cache[cache_key]
        if now - ts < CACHE_TTL:
            return cached

    sport, league_slug = ESPN_SLUGS[key]
    url = f"{ESPN_BASE}/{sport}/{league_slug}/scoreboard"
    if dates:
        url += f"?dates={dates}"

    data = _fetch_espn_json(url)
    if not data:
        return []

    games = _parse_scoreboard(data, key)
    _scoreboard_cache[cache_key] = (now, games)
    return games


class MatchupRequest(BaseModel):
    league: str
    home: str
    away: str


@app.post("/api/predict")
def predict(req: MatchupRequest):
    """Run a matchup prediction."""
    result = predict_matchup(req.league, req.home, req.away)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result
