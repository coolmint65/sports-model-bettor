"""
FastAPI backend for Sports Matchup Engine.
Serves league/team data and predictions via REST API.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from engine.leagues import LEAGUES, list_leagues
from engine.data import list_teams, search_teams, load_team
from engine.predict import predict_matchup

app = FastAPI(title="Sports Matchup Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)


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
