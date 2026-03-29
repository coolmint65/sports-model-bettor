"""
MLB Prediction Engine API.

Serves MLB schedule, team data, and game predictions.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import json
import time
import logging
import urllib.request
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from engine.db import (
    get_conn, get_all_teams, get_team_by_id, get_team_by_abbr,
    get_today_games, get_team_record, get_pitcher_season,
    get_bullpen, get_recent_games,
)
from engine.mlb_predict import predict_matchup

logger = logging.getLogger(__name__)

SEASON = datetime.now().year

app = FastAPI(title="MLB Prediction Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── ESPN integration for live scoreboard ────────────────────

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"
_scoreboard_cache: dict[str, tuple[float, list]] = {}
CACHE_TTL = 120


def _fetch_espn_json(url: str) -> dict | None:
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


# ── Endpoints ───────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/teams")
def api_teams():
    """Return all 30 MLB teams."""
    teams = get_all_teams()
    result = []
    for t in teams:
        record = get_team_record(t["mlb_id"], SEASON)
        result.append({
            "id": t["mlb_id"],
            "name": t["name"],
            "abbreviation": t["abbreviation"],
            "city": t.get("city", ""),
            "venue": t.get("venue", ""),
            "league": t.get("league", ""),
            "division": t.get("division", ""),
            "record": f"{record['wins']}-{record['losses']}" if record else "",
            "streak": record.get("streak", "") if record else "",
            "last_10": f"{record.get('last_10_wins', 0)}-{record.get('last_10_losses', 0)}" if record else "",
            "run_diff": record.get("run_diff", 0) if record else 0,
        })
    return result


@app.get("/api/teams/{team_id}")
def api_team_detail(team_id: int):
    """Return full team data with stats."""
    team = get_team_by_id(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")

    record = get_team_record(team_id, SEASON) or {}
    bp = get_bullpen(team_id, SEASON) or {}
    recent = get_recent_games(team_id, 10)

    return {
        "team": team,
        "record": record,
        "bullpen": bp,
        "recent_games": recent,
    }


@app.get("/api/scoreboard")
def api_scoreboard(date: str = Query(default="")):
    """
    Return today's MLB games.
    Combines ESPN live data with our DB data (probable pitchers, records).
    """
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    espn_date = target_date.replace("-", "")

    cache_key = f"mlb:{espn_date}"
    now = time.time()
    if cache_key in _scoreboard_cache:
        ts, cached = _scoreboard_cache[cache_key]
        if now - ts < CACHE_TTL:
            return cached

    # Fetch from ESPN — try with and without date param
    url = f"{ESPN_BASE}/baseball/mlb/scoreboard?dates={espn_date}"
    logger.info("Fetching scoreboard: %s", url)
    espn_data = _fetch_espn_json(url)

    games = []
    if espn_data:
        events = espn_data.get("events", [])
        logger.info("ESPN returned %d events for date %s", len(events), espn_date)
        games = _parse_espn_scoreboard(espn_data)
    else:
        logger.warning("ESPN returned no data for %s", url)

    # If no games found for the specific date, try without date param
    # (ESPN defaults to today's games in their timezone)
    if not games and date == "":
        fallback_url = f"{ESPN_BASE}/baseball/mlb/scoreboard"
        logger.info("Trying fallback (no date): %s", fallback_url)
        espn_data = _fetch_espn_json(fallback_url)
        if espn_data:
            events = espn_data.get("events", [])
            logger.info("Fallback returned %d events", len(events))
            games = _parse_espn_scoreboard(espn_data)

    # Enrich with our DB data
    games = _enrich_games(games, target_date)

    _scoreboard_cache[cache_key] = (now, games)
    return games


def _parse_espn_scoreboard(data: dict) -> list[dict]:
    """Parse ESPN scoreboard into clean game objects."""
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

            entry = {
                "espn_id": team.get("id", ""),
                "name": team.get("displayName", team.get("name", "")),
                "abbreviation": team.get("abbreviation", ""),
                "score": score,
                "record": (c.get("records", [{}])[0].get("summary", "")
                          if c.get("records") else ""),
                "logo": "",
                "winner": c.get("winner", False),
            }
            logos = team.get("logos", [])
            if logos:
                entry["logo"] = logos[0].get("href", "")

            if c.get("homeAway") == "home":
                home_team = entry
            else:
                away_team = entry

        if not home_team or not away_team:
            continue

        # Probable pitchers from ESPN
        home_pp = None
        away_pp = None
        for c in competitors:
            pp = c.get("probables", [])
            if pp:
                pitcher = pp[0].get("athlete", {})
                pitcher_info = {
                    "name": pitcher.get("displayName", "TBD"),
                    "id": pitcher.get("id"),
                    "headshot": pitcher.get("headshot", "") if isinstance(pitcher.get("headshot"), str) else pitcher.get("headshot", {}).get("href", ""),
                    "stats": [],
                }
                # Extract pitcher stats from ESPN
                for s in pp[0].get("statistics", []):
                    pitcher_info["stats"].append({
                        "name": s.get("abbreviation", s.get("name", "")),
                        "value": s.get("displayValue", ""),
                    })
                if c.get("homeAway") == "home":
                    home_pp = pitcher_info
                else:
                    away_pp = pitcher_info

        game = {
            "id": event.get("id", ""),
            "game_pk": int(event.get("uid", "0").split("~")[-1]) if "~" in event.get("uid", "") else 0,
            "date": event.get("date", ""),
            "name": event.get("name", ""),
            "short_name": event.get("shortName", ""),
            "home": home_team,
            "away": away_team,
            "home_pitcher": home_pp,
            "away_pitcher": away_pp,
            "status": {
                "state": status_type.get("state", "pre"),
                "detail": status_type.get("shortDetail",
                          status_type.get("detail", "")),
                "description": status_type.get("description", ""),
                "completed": status_type.get("completed", False),
                "inning": status.get("period", 0),
                "inning_half": status.get("type", {}).get("description", ""),
            },
            "venue": comp.get("venue", {}).get("fullName", ""),
            "broadcast": "",
            "odds": None,
        }

        # Broadcast
        broadcasts = comp.get("broadcasts", [])
        if broadcasts:
            names = []
            for b in broadcasts:
                for n in b.get("names", []):
                    names.append(n)
            game["broadcast"] = ", ".join(names[:2])

        # Odds
        odds = comp.get("odds", [])
        if odds:
            o = odds[0]
            game["odds"] = {
                "spread": o.get("details", ""),
                "over_under": o.get("overUnder"),
                "home_ml": o.get("homeTeamOdds", {}).get("moneyLine"),
                "away_ml": o.get("awayTeamOdds", {}).get("moneyLine"),
            }

        games.append(game)

    return games


def _enrich_games(games: list[dict], date: str) -> list[dict]:
    """Enrich ESPN game data with our DB records/stats."""
    for game in games:
        # Try to match teams to our DB
        home_abbr = game["home"].get("abbreviation", "")
        away_abbr = game["away"].get("abbreviation", "")

        home_db = get_team_by_abbr(home_abbr)
        away_db = get_team_by_abbr(away_abbr)

        if home_db:
            game["home"]["team_id"] = home_db["mlb_id"]
            rec = get_team_record(home_db["mlb_id"], SEASON)
            if rec:
                game["home"]["db_record"] = f"{rec['wins']}-{rec['losses']}"
                game["home"]["streak"] = rec.get("streak", "")
                game["home"]["last_10"] = f"{rec.get('last_10_wins', 0)}-{rec.get('last_10_losses', 0)}"

        if away_db:
            game["away"]["team_id"] = away_db["mlb_id"]
            rec = get_team_record(away_db["mlb_id"], SEASON)
            if rec:
                game["away"]["db_record"] = f"{rec['wins']}-{rec['losses']}"
                game["away"]["streak"] = rec.get("streak", "")
                game["away"]["last_10"] = f"{rec.get('last_10_wins', 0)}-{rec.get('last_10_losses', 0)}"

    return games


class PredictRequest(BaseModel):
    home_team_id: int
    away_team_id: int
    home_pitcher_id: int | None = None
    away_pitcher_id: int | None = None
    venue: str | None = None


@app.post("/api/predict")
def api_predict(req: PredictRequest):
    """Run a game prediction."""
    result = predict_matchup(
        home_team_id=req.home_team_id,
        away_team_id=req.away_team_id,
        home_pitcher_id=req.home_pitcher_id,
        away_pitcher_id=req.away_pitcher_id,
        venue=req.venue,
    )
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@app.get("/api/standings")
def api_standings():
    """Return MLB standings grouped by division."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT t.mlb_id, t.name, t.abbreviation, t.league, t.division,
               ts.wins, ts.losses, ts.run_diff, ts.streak,
               ts.last_10_wins, ts.last_10_losses,
               ts.home_wins, ts.home_losses, ts.away_wins, ts.away_losses,
               ts.era, ts.ops, ts.wrc_plus
        FROM teams t
        LEFT JOIN team_stats ts ON t.mlb_id = ts.team_id AND ts.season = ?
        ORDER BY t.league, t.division, ts.wins DESC
    """, (SEASON,)).fetchall()

    divisions = {}
    for r in rows:
        div_key = f"{r['league']} {r['division']}"
        if div_key not in divisions:
            divisions[div_key] = {
                "league": r["league"],
                "division": r["division"],
                "teams": [],
            }
        w = r["wins"] or 0
        l = r["losses"] or 0
        divisions[div_key]["teams"].append({
            "id": r["mlb_id"],
            "name": r["name"],
            "abbreviation": r["abbreviation"],
            "wins": w,
            "losses": l,
            "pct": f".{int(w / (w + l) * 1000):03d}" if (w + l) > 0 else ".000",
            "run_diff": r["run_diff"] or 0,
            "streak": r["streak"] or "",
            "last_10": f"{r['last_10_wins'] or 0}-{r['last_10_losses'] or 0}",
            "home": f"{r['home_wins'] or 0}-{r['home_losses'] or 0}",
            "away": f"{r['away_wins'] or 0}-{r['away_losses'] or 0}",
            "era": r["era"],
            "ops": r["ops"],
            "wrc_plus": r["wrc_plus"],
        })

    return list(divisions.values())


@app.get("/api/pitcher/{pitcher_id}")
def api_pitcher(pitcher_id: int):
    """Return pitcher stats and recent starts."""
    from engine.db import get_pitcher_recent_starts
    conn = get_conn()

    player = conn.execute(
        "SELECT * FROM players WHERE mlb_id = ?", (pitcher_id,)
    ).fetchone()
    if not player:
        raise HTTPException(status_code=404, detail="Pitcher not found")

    stats = get_pitcher_season(pitcher_id, SEASON)
    recent = get_pitcher_recent_starts(pitcher_id, 5)

    return {
        "player": dict(player),
        "stats": dict(stats) if stats else None,
        "recent_starts": recent,
    }
