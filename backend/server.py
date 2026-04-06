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
    conn = get_conn()
    teams = conn.execute("SELECT COUNT(*) as c FROM teams").fetchone()["c"]
    stats = conn.execute("SELECT COUNT(*) as c FROM team_stats").fetchone()["c"]
    from engine.db import DB_PATH
    return {
        "status": "ok",
        "db_path": str(DB_PATH),
        "teams": teams,
        "team_stats": stats,
    }


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


def _get_scoreboard(date: str = "") -> list[dict]:
    """Core scoreboard logic — reusable by other endpoints."""
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    espn_date = target_date.replace("-", "")

    cache_key = f"mlb:{espn_date}"
    now = time.time()
    if cache_key in _scoreboard_cache:
        ts, cached = _scoreboard_cache[cache_key]
        if now - ts < CACHE_TTL:
            return cached

    games = []

    # Primary: ESPN
    url = f"{ESPN_BASE}/baseball/mlb/scoreboard?dates={espn_date}"
    logger.info("Fetching scoreboard: %s", url)
    espn_data = _fetch_espn_json(url)

    games = []
    if espn_data:
        events = espn_data.get("events", [])
        logger.info("ESPN returned %d events for date %s", len(events), espn_date)
        # Debug: log first team's keys to see logo format
        if events:
            try:
                first_comp = events[0].get("competitions", [{}])[0]
                first_team = first_comp.get("competitors", [{}])[0].get("team", {})
                logger.info("ESPN team keys: %s", list(first_team.keys()))
            except Exception:
                pass
        games = _parse_espn_scoreboard(espn_data)
    else:
        logger.warning("ESPN returned no data for %s", url)

    # ESPN fallback: try without date param
    if not games and date == "":
        fallback_url = f"{ESPN_BASE}/baseball/mlb/scoreboard"
        logger.info("ESPN fallback (no date): %s", fallback_url)
        espn_data = _fetch_espn_json(fallback_url)
        if espn_data:
            events = espn_data.get("events", [])
            logger.info("Fallback returned %d events", len(events))
            games = _parse_espn_scoreboard(espn_data)

    # Secondary fallback: MLB Stats API (if ESPN is completely down)
    if not games:
        logger.warning("ESPN unavailable, falling back to MLB Stats API")
        games = _mlb_api_scoreboard(target_date)

    # Enrich with our DB data
    games = _enrich_games(games, target_date)

    # Fetch real odds — try The Odds API first (has all lines with juice),
    # then ESPN per-game as fallback
    odds_matched = 0
    try:
        from scrapers.odds_api import fetch_odds
        logger.info("Calling Odds API...")
        api_odds = fetch_odds()
        logger.info("Odds API returned %d games", len(api_odds) if api_odds else 0)
        if api_odds:
            # Log what keys we're trying to match
            api_keys = set(api_odds.keys())
            for game in games:
                h_abbr = game["home"].get("abbreviation", "")
                a_abbr = game["away"].get("abbreviation", "")
                key = f"{a_abbr}@{h_abbr}"
                if key in api_odds:
                    game["odds"] = api_odds[key]
                    odds_matched += 1
                else:
                    # Try reverse key or alternate abbreviations
                    alt_keys = [
                        f"{a_abbr}@{h_abbr}",
                        f"{_alt_abbr(a_abbr)}@{_alt_abbr(h_abbr)}",
                        f"{_alt_abbr(a_abbr)}@{h_abbr}",
                        f"{a_abbr}@{_alt_abbr(h_abbr)}",
                    ]
                    matched = False
                    for ak in alt_keys:
                        if ak in api_odds:
                            game["odds"] = api_odds[ak]
                            odds_matched += 1
                            matched = True
                            break
                    if not matched:
                        logger.debug("No odds match for %s (tried %s, available: %s)",
                                    key, alt_keys[:2], list(api_keys)[:3])
            logger.info("Odds API: matched %d/%d games", odds_matched, len(games))
    except Exception as e:
        logger.warning("Odds API failed: %s", e, exc_info=True)

    # Fallback: ESPN per-game odds for games without API odds
    if odds_matched < len(games):
        try:
            from scrapers.espn_odds import fetch_all_game_odds
            games_needing_odds = [g for g in games
                                  if not g.get("odds") or not g["odds"].get("home_ml")]
            if games_needing_odds:
                espn_odds = fetch_all_game_odds(games_needing_odds)
                for game in games:
                    gid = game.get("id")
                    if gid and gid in espn_odds and (
                        not game.get("odds") or not game["odds"].get("home_ml")):
                        game["odds"] = espn_odds[gid]
                        game["odds"]["provider"] = "ESPN"
        except Exception as e:
            logger.warning("ESPN per-game odds failed: %s", e)

    _scoreboard_cache[cache_key] = (now, games)
    return games


@app.get("/api/scoreboard")
def api_scoreboard(date: str = Query(default="")):
    """Return today's MLB games."""
    return _get_scoreboard(date)


def _mlb_api_scoreboard(date: str) -> list[dict]:
    """Fallback scoreboard using MLB Stats API when ESPN is down."""
    MLB_API = "https://statsapi.mlb.com/api/v1"
    url = (f"{MLB_API}/schedule?sportId=1&date={date}"
           f"&hydrate=probablePitcher,linescore,team")
    data = _fetch_espn_json(url)  # Reuse the fetch helper
    if not data:
        return []

    games = []
    for date_entry in data.get("dates", []):
        for g in date_entry.get("games", []):
            status_code = g.get("status", {}).get("abstractGameCode", "")
            home = g.get("teams", {}).get("home", {})
            away = g.get("teams", {}).get("away", {})

            home_team = home.get("team", {})
            away_team = away.get("team", {})

            home_pp = home.get("probablePitcher", {})
            away_pp = away.get("probablePitcher", {})

            state = {"P": "pre", "S": "pre", "L": "in", "I": "in", "F": "post"}.get(status_code, "pre")

            game = {
                "id": str(g.get("gamePk", "")),
                "game_pk": g.get("gamePk", 0),
                "date": g.get("gameDate", ""),
                "name": f"{away_team.get('name', '')} @ {home_team.get('name', '')}",
                "short_name": f"{away_team.get('abbreviation', '')} @ {home_team.get('abbreviation', '')}",
                "home": {
                    "espn_id": str(home_team.get("id", "")),
                    "name": home_team.get("name", ""),
                    "abbreviation": home_team.get("abbreviation", ""),
                    "score": str(home.get("score", "0")),
                    "record": f"{home.get('leagueRecord', {}).get('wins', 0)}-{home.get('leagueRecord', {}).get('losses', 0)}",
                    "logo": "",
                    "winner": False,
                },
                "away": {
                    "espn_id": str(away_team.get("id", "")),
                    "name": away_team.get("name", ""),
                    "abbreviation": away_team.get("abbreviation", ""),
                    "score": str(away.get("score", "0")),
                    "record": f"{away.get('leagueRecord', {}).get('wins', 0)}-{away.get('leagueRecord', {}).get('losses', 0)}",
                    "logo": "",
                    "winner": False,
                },
                "home_pitcher": {
                    "name": home_pp.get("fullName", "TBD"),
                    "id": home_pp.get("id"),
                } if home_pp else None,
                "away_pitcher": {
                    "name": away_pp.get("fullName", "TBD"),
                    "id": away_pp.get("id"),
                } if away_pp else None,
                "status": {
                    "state": state,
                    "detail": g.get("status", {}).get("detailedState", ""),
                    "description": "",
                    "completed": state == "post",
                    "inning": g.get("linescore", {}).get("currentInning", 0),
                    "inning_half": g.get("linescore", {}).get("inningHalf", ""),
                },
                "venue": g.get("venue", {}).get("name", ""),
                "broadcast": "",
                "odds": None,
            }
            games.append(game)

    logger.info("MLB API fallback returned %d games", len(games))
    return games


def _safe_game_pk(uid: str, event_id: str) -> int:
    """Extract a numeric game PK from ESPN uid or event id."""
    # Try uid formats: "s:1~l:10~e:401814725" or "e:401814725"
    for part in uid.split("~"):
        if part.startswith("e:"):
            try:
                return int(part[2:])
            except ValueError:
                pass
    # Fall back to event id
    try:
        return int(event_id)
    except (ValueError, TypeError):
        return 0


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
            # ESPN sends logos in different formats
            logo = team.get("logo", "")
            if isinstance(logo, str) and logo:
                entry["logo"] = logo
            elif isinstance(logo, dict):
                entry["logo"] = logo.get("href", "")
            else:
                logos = team.get("logos", [])
                if logos and isinstance(logos, list):
                    first = logos[0]
                    if isinstance(first, str):
                        entry["logo"] = first
                    elif isinstance(first, dict):
                        entry["logo"] = first.get("href", "")

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
            "game_pk": _safe_game_pk(event.get("uid", ""), event.get("id", "")),
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
            home_odds = o.get("homeTeamOdds", {}) or {}
            away_odds = o.get("awayTeamOdds", {}) or {}

            # Log raw odds structure on first game for debugging
            if not games:
                logger.info("ESPN odds top-level keys: %s", list(o.keys()))
                logger.info("ESPN homeTeamOdds: %s", dict(home_odds))
                logger.info("ESPN awayTeamOdds: %s", dict(away_odds))
                # Log any additional odds entries (some have spread/total as separate items)
                if len(odds) > 1:
                    logger.info("ESPN odds[1]: %s", odds[1])

            game["odds"] = {
                "spread": o.get("details", ""),
                "over_under": o.get("overUnder"),
                # Moneyline
                "home_ml": home_odds.get("moneyLine"),
                "away_ml": away_odds.get("moneyLine"),
                # Run line (spread odds)
                "home_spread": home_odds.get("spreadOdds") or home_odds.get("spread"),
                "away_spread": away_odds.get("spreadOdds") or away_odds.get("spread"),
                "home_spread_line": home_odds.get("spreadLine") or home_odds.get("line"),
                "away_spread_line": away_odds.get("spreadLine") or away_odds.get("line"),
                # Over/Under odds
                "over_odds": o.get("overOdds") or home_odds.get("overOdds"),
                "under_odds": o.get("underOdds") or away_odds.get("underOdds"),
            }

        games.append(game)

    return games


# ESPN uses different abbreviations than MLB Stats API for some teams
_ESPN_ABBR_MAP = {
    "CHW": "CWS",   # White Sox
    "WSH": "WSH",   # Nationals (sometimes WAS)
    "WAS": "WSH",
    "AZ": "ARI",    # Diamondbacks
    "SF": "SF",      # Giants
    "SD": "SD",      # Padres
    "TB": "TB",      # Rays
    "KC": "KC",      # Royals
}


def _resolve_abbr(espn_abbr: str):
    """Try to find a team by ESPN abbreviation, with fallback mapping."""
    team = get_team_by_abbr(espn_abbr)
    if team:
        return team
    # Try mapped abbreviation
    mapped = _ESPN_ABBR_MAP.get(espn_abbr)
    if mapped and mapped != espn_abbr:
        team = get_team_by_abbr(mapped)
        if team:
            return team
    # Try by team name substring
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM teams WHERE name LIKE ? LIMIT 1",
        (f"%{espn_abbr}%",)
    ).fetchone()
    return dict(row) if row else None


# Abbreviation mapping: ESPN ↔ Odds API differences
_ABBR_ALTS = {
    "ARI": "AZ", "AZ": "ARI",
    "CHW": "CWS", "CWS": "CHW",
    "WSH": "WAS", "WAS": "WSH",
    "ATH": "OAK", "OAK": "ATH",
}

def _alt_abbr(abbr: str) -> str:
    return _ABBR_ALTS.get(abbr, abbr)


def _enrich_games(games: list[dict], date: str) -> list[dict]:
    """Enrich ESPN game data with our DB records/stats."""
    for game in games:
        home_abbr = game["home"].get("abbreviation", "")
        away_abbr = game["away"].get("abbreviation", "")

        home_db = _resolve_abbr(home_abbr)
        away_db = _resolve_abbr(away_abbr)

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


@app.get("/api/best-bets")
def api_best_bets():
    """
    Run predictions on all today's games using the unified picks engine.
    """
    games = _get_scoreboard()

    from engine.picks import generate_picks, get_best_pick, match_odds, fetch_real_odds_for_games

    all_odds = fetch_real_odds_for_games()

    bets = []
    logger.info("Best bets: analyzing %d games", len(games))

    for game in games:
        home_id = game["home"].get("team_id")
        away_id = game["away"].get("team_id")
        if not home_id or not away_id:
            continue

        if game["status"].get("completed") or game["status"].get("state") == "post":
            continue

        home_pid = game.get("home_pitcher") or {}
        away_pid = game.get("away_pitcher") or {}
        try:
            h_pitcher_id = int(home_pid["id"]) if home_pid.get("id") else None
            a_pitcher_id = int(away_pid["id"]) if away_pid.get("id") else None
        except (ValueError, TypeError):
            h_pitcher_id = None
            a_pitcher_id = None

        h_abbr = game["home"]["abbreviation"]
        a_abbr = game["away"]["abbreviation"]

        game_odds = game.get("odds") or match_odds(h_abbr, a_abbr, all_odds)

        try:
            picks = generate_picks(
                home_team_id=home_id,
                away_team_id=away_id,
                home_pitcher_id=h_pitcher_id,
                away_pitcher_id=a_pitcher_id,
                venue=game.get("venue"),
                odds=game_odds,
            )
        except Exception as e:
            logger.error("  Prediction failed for %s: %s", game.get("short_name", "?"), e)
            continue

        if not picks:
            continue

        best = get_best_pick(picks)
        if not best:
            continue

        bets.append({
            "game_id": game["id"],
            "matchup": f"{a_abbr} @ {h_abbr}",
            "home": game["home"],
            "away": game["away"],
            "time": game["date"],
            "venue": game.get("venue", ""),
            "best_pick": best,
            "all_picks": picks[:4],
            "confidence": best.get("confidence", "lean"),
        })

    bets.sort(key=lambda b: b["best_pick"]["edge"], reverse=True)
    return bets


def _implied(ml: int) -> float:
    """Convert American odds to implied probability."""
    if ml < 0:
        return abs(ml) / (abs(ml) + 100)
    return 100 / (ml + 100)


def _find_ou(ou_lines, vegas_total):
    """Find the O/U entry closest to the Vegas total."""
    vt = float(vegas_total)
    for fmt in [str(vt), f"{vt:.1f}", str(int(vt))]:
        if fmt in ou_lines:
            return ou_lines[fmt]
    # Closest
    best_key = min(ou_lines.keys(), key=lambda k: abs(float(k) - vt), default=None)
    return ou_lines.get(best_key) if best_key else None


@app.get("/api/tracker/history")
def api_pick_history():
    """Return recent pick history with results."""
    conn = get_conn()
    picks = conn.execute("""
        SELECT * FROM picks ORDER BY created_at DESC LIMIT 50
    """).fetchall()
    return [dict(p) for p in picks]


@app.get("/api/teams/{team_id}/profile")
def api_team_profile(team_id: int):
    """Full team profile with stats, recent games, and form."""
    team = get_team_by_id(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")

    record = get_team_record(team_id, SEASON) or {}
    recent = get_recent_games(team_id, 15)
    bp = get_bullpen(team_id, SEASON) or {}

    # PIT stats for deeper analysis
    from engine.pit_stats import compute_team_stats_at_date
    today = datetime.now().strftime("%Y-%m-%d")
    pit = compute_team_stats_at_date(team_id, today, SEASON)

    # Get roster pitchers
    conn = get_conn()
    pitchers = conn.execute("""
        SELECT p.mlb_id, p.name, p.throws,
               ps.era, ps.whip, ps.k_per_9, ps.wins, ps.losses, ps.innings
        FROM players p
        LEFT JOIN pitcher_stats ps ON p.mlb_id = ps.player_id AND ps.season = ?
        WHERE p.team_id = ? AND p.position = 'P' AND p.active = 1
        ORDER BY ps.innings DESC
    """, (SEASON, team_id)).fetchall()

    return {
        "team": team,
        "record": record,
        "pit_stats": pit,
        "bullpen": bp,
        "recent_games": recent,
        "pitchers": [dict(p) for p in pitchers],
    }


@app.post("/api/calibrate")
def api_calibrate(days: int = Query(default=30)):
    """Run model self-calibration on recent games."""
    from engine.calibration import calibrate
    from engine.mlb_predict import reload_weights
    report = calibrate(days=days)
    reload_weights()  # Refresh cached weights
    return report


@app.get("/api/calibration/status")
def api_calibration_status():
    """Return current model weights and calibration info."""
    from engine.calibration import get_calibration_status
    return get_calibration_status()


@app.post("/api/calibrate/teams")
def api_calibrate_teams():
    """Run per-team calibration."""
    from engine.team_calibration import calibrate_teams
    return calibrate_teams()


@app.get("/api/debug/odds")
def api_debug_odds():
    """Test all odds sources."""
    result = {}

    # Test The Odds API
    try:
        from scrapers.odds_api import fetch_odds, _get_api_key, KEY_FILE
        key = _get_api_key()
        result["odds_api"] = {
            "key_found": key is not None,
            "key_file": str(KEY_FILE),
            "key_file_exists": KEY_FILE.exists(),
            "key_preview": key[:8] + "..." if key else None,
        }
        if key:
            odds = fetch_odds()
            result["odds_api"]["games_found"] = len(odds)
            result["odds_api"]["sample"] = dict(list(odds.items())[:1]) if odds else None
    except Exception as e:
        result["odds_api"] = {"error": str(e)}

    # Test ESPN
    try:
        from scrapers.espn_odds import fetch_game_odds
        games = _get_scoreboard()
        if games:
            espn = fetch_game_odds(games[0].get("id"))
            result["espn"] = {"game": games[0].get("short_name", ""), "odds": espn}
    except Exception as e:
        result["espn"] = {"error": str(e)}

    return result


@app.get("/api/debug/teams")
def api_debug_teams():
    """Debug: dump raw team data to see league/division values."""
    conn = get_conn()
    rows = conn.execute("SELECT mlb_id, abbreviation, name, league, division FROM teams").fetchall()
    return [dict(r) for r in rows]


@app.get("/api/backtest")
def api_backtest(days: int = Query(default=0), min_edge: float = Query(default=3),
                 season: int = Query(default=0)):
    """Run model backtest against historical games."""
    from engine.backtest import run_backtest

    yr = season if season > 0 else None

    # If requesting a past season, auto-load the data if not already present
    if yr:
        conn = get_conn()
        game_count = conn.execute(
            "SELECT COUNT(*) as c FROM games WHERE season = ? AND status = 'final'",
            (yr,)
        ).fetchone()["c"]
        if game_count < 100:
            # Need to fetch this season's data first
            logger.info("Loading %d season data for backtest...", yr)
            from scrapers.mlb_stats import fetch_teams, fetch_season_results
            fetch_teams()
            fetch_season_results(season=yr)
            game_count = conn.execute(
                "SELECT COUNT(*) as c FROM games WHERE season = ? AND status = 'final'",
                (yr,)
            ).fetchone()["c"]
            logger.info("Loaded %d games for %d", game_count, yr)

    results = run_backtest(
        season=yr,
        days=days if days > 0 else None,
        min_edge=min_edge,
    )
    # Don't send full game log to frontend (too large)
    results.pop("game_log", None)
    return results


@app.post("/api/tracker/record")
def api_record_picks():
    """Record today's model picks."""
    from engine.tracker import record_picks
    picks = record_picks()
    return {"recorded": len(picks), "picks": picks}


@app.post("/api/tracker/settle")
def api_settle_picks():
    """Settle completed picks against final scores."""
    from engine.tracker import settle_picks
    return settle_picks()


@app.get("/api/tracker/summary")
def api_pick_summary():
    """Get running pick totals."""
    from engine.tracker import get_pick_summary
    return get_pick_summary()


@app.get("/api/standings")
def api_standings():
    """Return MLB standings grouped by division."""
    conn = get_conn()

    # Debug: check what's in the DB
    team_count = conn.execute("SELECT COUNT(*) as c FROM teams").fetchone()["c"]
    ts_count = conn.execute("SELECT COUNT(*) as c FROM team_stats").fetchone()["c"]
    with_league = conn.execute(
        "SELECT COUNT(*) as c FROM teams WHERE league IS NOT NULL AND league != ''"
    ).fetchone()["c"]
    sample = conn.execute(
        "SELECT abbreviation, league, division FROM teams LIMIT 3"
    ).fetchall()
    logger.info("Standings: %d teams, %d stats, %d with league. Sample: %s",
                team_count, ts_count, with_league,
                [(dict(r)) for r in sample])

    if team_count == 0:
        return []

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

    logger.info("Standings query returned %d rows", len(rows))
    # Log first few rows to see actual values
    for r in rows[:3]:
        logger.info("  Row: %s league='%s' division='%s' wins=%s",
                    r["abbreviation"], r["league"], r["division"], r["wins"])

    if not rows:
        return []

    divisions = {}
    for r in rows:
        league = (r["league"] or "").strip()
        division = (r["division"] or "").strip()
        if not league or not division:
            logger.warning("  Skipping %s: league='%s' division='%s'",
                          r["abbreviation"], league, division)
            continue
        div_key = f"{league} {division}"
        if div_key not in divisions:
            divisions[div_key] = {
                "league": league,
                "division": division,
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


# ══════════════════════════════════════════════════════════════
#  NHL ENDPOINTS
# ══════════════════════════════════════════════════════════════

# NHL team name → abbreviation (for Odds API matching)
_NHL_TEAM_ABBR = {
    "Anaheim Ducks": "ANA", "Arizona Coyotes": "ARI", "Utah Hockey Club": "UTA",
    "Boston Bruins": "BOS", "Buffalo Sabres": "BUF",
    "Calgary Flames": "CGY", "Carolina Hurricanes": "CAR",
    "Chicago Blackhawks": "CHI", "Colorado Avalanche": "COL",
    "Columbus Blue Jackets": "CBJ", "Dallas Stars": "DAL",
    "Detroit Red Wings": "DET", "Edmonton Oilers": "EDM",
    "Florida Panthers": "FLA", "Los Angeles Kings": "LAK",
    "Minnesota Wild": "MIN", "Montreal Canadiens": "MTL",
    "Nashville Predators": "NSH", "New Jersey Devils": "NJD",
    "New York Islanders": "NYI", "New York Rangers": "NYR",
    "Ottawa Senators": "OTT", "Philadelphia Flyers": "PHI",
    "Pittsburgh Penguins": "PIT", "San Jose Sharks": "SJS",
    "Seattle Kraken": "SEA", "St. Louis Blues": "STL",
    "Tampa Bay Lightning": "TBL", "Toronto Maple Leafs": "TOR",
    "Vancouver Canucks": "VAN", "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH", "Winnipeg Jets": "WPG",
}

_NHL_ABBR_ALTS = {
    "TB": "TBL", "TBL": "TB",
    "NJ": "NJD", "NJD": "NJ",
    "SJ": "SJS", "SJS": "SJ",
    "LA": "LAK", "LAK": "LA",
    "WAS": "WSH", "WSH": "WAS",
    "CLB": "CBJ", "CBJ": "CLB",
    "MON": "MTL", "MTL": "MON",
    "NAS": "NSH", "NSH": "NAS",
    "AZ": "UTA", "UTA": "AZ",
    "UTAH": "UTA",
}

# Map ESPN abbreviation to team JSON key
_NHL_ESPN_TO_KEY = {}  # populated lazily

def _nhl_espn_to_key() -> dict:
    """Build mapping from ESPN abbreviation to JSON file key."""
    if _NHL_ESPN_TO_KEY:
        return _NHL_ESPN_TO_KEY
    from engine.data import list_teams, load_team
    for t in list_teams("NHL"):
        team = load_team("NHL", t["key"])
        if team:
            abbr = team.get("abbreviation", "")
            if abbr:
                _NHL_ESPN_TO_KEY[abbr] = t["key"]
                # Add alternate abbreviations
                alt = _NHL_ABBR_ALTS.get(abbr)
                if alt:
                    _NHL_ESPN_TO_KEY[alt] = t["key"]
            _NHL_ESPN_TO_KEY[t["key"]] = t["key"]
            # Also map by short name (e.g. "Bruins" -> "bruins")
            short = team.get("short_name", "")
            if short:
                _NHL_ESPN_TO_KEY[short.lower()] = t["key"]
    return _NHL_ESPN_TO_KEY


def _nhl_alt_abbr(abbr: str) -> str:
    return _NHL_ABBR_ALTS.get(abbr, abbr)


def _get_nhl_scoreboard(date: str = "") -> list[dict]:
    """Fetch NHL scoreboard from ESPN."""
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    espn_date = target_date.replace("-", "")

    cache_key = f"nhl:{espn_date}"
    now = time.time()
    if cache_key in _scoreboard_cache:
        ts, cached = _scoreboard_cache[cache_key]
        if now - ts < CACHE_TTL:
            return cached

    url = f"{ESPN_BASE}/hockey/nhl/scoreboard?dates={espn_date}"
    logger.info("Fetching NHL scoreboard: %s", url)
    espn_data = _fetch_espn_json(url)

    games = []
    if espn_data:
        events = espn_data.get("events", [])
        logger.info("ESPN NHL returned %d events", len(events))
        games = _parse_nhl_scoreboard(espn_data)

    # Fallback without date
    if not games and date == "":
        espn_data = _fetch_espn_json(f"{ESPN_BASE}/hockey/nhl/scoreboard")
        if espn_data:
            games = _parse_nhl_scoreboard(espn_data)

    # Fetch NHL odds from The Odds API
    try:
        nhl_odds = _fetch_nhl_odds()
        if nhl_odds:
            matched = 0
            for game in games:
                h = game["home"]["abbreviation"]
                a = game["away"]["abbreviation"]
                key = f"{a}@{h}"
                alt_keys = [
                    key,
                    f"{_nhl_alt_abbr(a)}@{_nhl_alt_abbr(h)}",
                    f"{_nhl_alt_abbr(a)}@{h}",
                    f"{a}@{_nhl_alt_abbr(h)}",
                ]
                for k in alt_keys:
                    if k in nhl_odds:
                        game["odds"] = nhl_odds[k]
                        matched += 1
                        break
            logger.info("NHL odds: matched %d/%d games", matched, len(games))
    except Exception as e:
        logger.warning("NHL odds failed: %s", e)

    _scoreboard_cache[cache_key] = (now, games)
    return games


def _parse_nhl_scoreboard(data: dict) -> list[dict]:
    """Parse ESPN NHL scoreboard into game objects."""
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

            # Parse record — NHL has W-L-OTL format
            record = ""
            if c.get("records"):
                record = c["records"][0].get("summary", "")

            entry = {
                "espn_id": team.get("id", ""),
                "name": team.get("displayName", team.get("name", "")),
                "abbreviation": team.get("abbreviation", ""),
                "score": score,
                "record": record,
                "logo": "",
                "winner": c.get("winner", False),
            }

            logo = team.get("logo", "")
            if isinstance(logo, str) and logo:
                entry["logo"] = logo
            elif isinstance(logo, dict):
                entry["logo"] = logo.get("href", "")
            else:
                logos = team.get("logos", [])
                if logos and isinstance(logos, list):
                    first = logos[0]
                    entry["logo"] = first.get("href", "") if isinstance(first, dict) else (first if isinstance(first, str) else "")

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
                "state": status_type.get("state", "pre"),
                "detail": status_type.get("shortDetail",
                          status_type.get("detail", "")),
                "description": status_type.get("description", ""),
                "completed": status_type.get("completed", False),
                "period": status.get("period", 0),
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

        # ESPN inline odds
        odds = comp.get("odds", [])
        if odds:
            o = odds[0]
            home_odds = o.get("homeTeamOdds", {}) or {}
            away_odds = o.get("awayTeamOdds", {}) or {}
            game["odds"] = {
                "spread": o.get("details", ""),
                "over_under": o.get("overUnder"),
                "home_ml": home_odds.get("moneyLine"),
                "away_ml": away_odds.get("moneyLine"),
                "home_spread_odds": home_odds.get("spreadOdds"),
                "away_spread_odds": away_odds.get("spreadOdds"),
                "home_spread_point": home_odds.get("spreadLine") or home_odds.get("line"),
                "away_spread_point": away_odds.get("spreadLine") or away_odds.get("line"),
                "over_odds": o.get("overOdds"),
                "under_odds": o.get("underOdds"),
                "provider": "ESPN",
            }

        games.append(game)

    return games


# NHL odds cache (separate from MLB)
_nhl_odds_cache: dict | None = None
_nhl_odds_cache_time: float = 0

def _fetch_nhl_odds() -> dict:
    """Fetch NHL odds from The Odds API."""
    global _nhl_odds_cache, _nhl_odds_cache_time

    if _nhl_odds_cache and (time.time() - _nhl_odds_cache_time) < 600:
        return _nhl_odds_cache

    import os
    from pathlib import Path
    key_file = Path(__file__).resolve().parent.parent / "data" / "odds_api_key.txt"
    api_key = os.environ.get("ODDS_API_KEY") or (key_file.read_text().strip() if key_file.exists() else None)
    if not api_key:
        return {}

    url = (f"https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds/"
           f"?apiKey={api_key}"
           f"&regions=us"
           f"&markets=h2h,spreads,totals"
           f"&oddsFormat=american"
           f"&bookmakers=draftkings")

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "NHLPredictionEngine/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            remaining = resp.headers.get("x-requests-remaining", "?")
            logger.info("NHL Odds API: %s requests remaining", remaining)
    except Exception as e:
        logger.warning("NHL Odds API failed: %s", e)
        return {}

    if not data or not isinstance(data, list):
        return {}

    odds_map = {}
    for game in data:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        h_abbr = _NHL_TEAM_ABBR.get(home, home[:3].upper())
        a_abbr = _NHL_TEAM_ABBR.get(away, away[:3].upper())
        key = f"{a_abbr}@{h_abbr}"

        result = {"provider": "DraftKings"}
        bookmakers = game.get("bookmakers", [])
        if not bookmakers:
            continue

        book = bookmakers[0]
        for market in book.get("markets", []):
            mkey = market.get("key", "")
            outcomes = market.get("outcomes", [])

            if mkey == "h2h":
                for o in outcomes:
                    if o.get("name") == home:
                        result["home_ml"] = o.get("price", 0)
                    elif o.get("name") == away:
                        result["away_ml"] = o.get("price", 0)
            elif mkey == "spreads":
                for o in outcomes:
                    if o.get("name") == home:
                        result["home_spread_odds"] = o.get("price", 0)
                        result["home_spread_point"] = o.get("point", 0)
                    elif o.get("name") == away:
                        result["away_spread_odds"] = o.get("price", 0)
                        result["away_spread_point"] = o.get("point", 0)
            elif mkey == "totals":
                for o in outcomes:
                    name = o.get("name", "").lower()
                    if "over" in name:
                        result["over_odds"] = o.get("price", 0)
                        result["over_under"] = o.get("point", 0)
                    elif "under" in name:
                        result["under_odds"] = o.get("price", 0)

        if result.get("home_ml"):
            odds_map[key] = result

    _nhl_odds_cache = odds_map
    _nhl_odds_cache_time = time.time()
    return odds_map


@app.post("/api/nhl/sync")
def api_nhl_sync():
    """Refresh NHL team data from ESPN."""
    try:
        from scrapers.espn import scrape_league
        updated = scrape_league("hockey", "nhl", "NHL")
        # Clear the key cache so it rebuilds
        _NHL_ESPN_TO_KEY.clear()
        return {"status": "ok", "updated": len(updated)}
    except Exception as e:
        logger.error("NHL sync failed: %s", e)
        return {"status": "error", "message": str(e)}


@app.get("/api/nhl/scoreboard")
def api_nhl_scoreboard(date: str = Query(default="")):
    """Return today's NHL games."""
    return _get_nhl_scoreboard(date)


@app.get("/api/nhl/standings")
def api_nhl_standings():
    """Return NHL standings from ESPN."""
    url = f"{ESPN_BASE}/hockey/nhl/standings"
    # Add season param
    now = datetime.now(timezone.utc)
    season = now.year if now.month >= 8 else now.year - 1
    url += f"?season={season}"

    data = _fetch_espn_json(url)
    if not data:
        return []

    divisions = {}
    # ESPN NHL standings: children = conferences, sub-children = divisions
    groups = []

    for child in data.get("children", []):
        if not isinstance(child, dict):
            continue
        if "standings" in child:
            groups.append(child)
        for sub in child.get("children", []):
            if isinstance(sub, dict) and "standings" in sub:
                groups.append(sub)

    for group in groups:
        div_name = group.get("name", group.get("abbreviation", "Unknown"))
        standings_data = group.get("standings", {})
        if not isinstance(standings_data, dict):
            continue

        teams = []
        for entry in standings_data.get("entries", []):
            if not isinstance(entry, dict):
                continue
            team = entry.get("team", {})
            if not isinstance(team, dict):
                continue

            stats_map = {}
            for s in entry.get("stats", []):
                if isinstance(s, dict):
                    stats_map[s.get("name", "")] = s.get("displayValue", s.get("value", ""))

            teams.append({
                "name": team.get("displayName", team.get("name", "")),
                "abbreviation": team.get("abbreviation", ""),
                "logo": "",
                "record": stats_map.get("overall", ""),
                "wins": int(float(stats_map.get("wins", 0))),
                "losses": int(float(stats_map.get("losses", 0))),
                "otl": int(float(stats_map.get("OTLosses", stats_map.get("otLosses", 0)))),
                "points": int(float(stats_map.get("points", 0))),
                "gf": int(float(stats_map.get("pointsFor", stats_map.get("goalsFor", 0)))),
                "ga": int(float(stats_map.get("pointsAgainst", stats_map.get("goalsAgainst", 0)))),
                "diff": int(float(stats_map.get("pointDifferential", stats_map.get("differential", 0)))),
                "streak": stats_map.get("streak", ""),
                "home": stats_map.get("Home", stats_map.get("home", "")),
                "away": stats_map.get("Road", stats_map.get("away", stats_map.get("road", ""))),
                "l10": stats_map.get("Last Ten", stats_map.get("lastTen", "")),
            })

            # Get logo
            logo = team.get("logo", "")
            if isinstance(logo, str) and logo:
                teams[-1]["logo"] = logo
            elif isinstance(logo, dict):
                teams[-1]["logo"] = logo.get("href", "")
            else:
                logos = team.get("logos", [])
                if logos and isinstance(logos, list):
                    first = logos[0]
                    teams[-1]["logo"] = first.get("href", "") if isinstance(first, dict) else (first if isinstance(first, str) else "")

        teams.sort(key=lambda t: t["points"], reverse=True)

        divisions[div_name] = {
            "name": div_name,
            "teams": teams,
        }

    return list(divisions.values())


@app.get("/api/nhl/predict")
def api_nhl_predict(home: str = Query(...), away: str = Query(...)):
    """
    Run NHL prediction. home/away are team keys (e.g. 'bruins', 'maple_leafs')
    or abbreviations (e.g. 'BOS', 'TOR').
    """
    from engine.nhl_predict import predict_matchup as nhl_predict

    key_map = _nhl_espn_to_key()

    # Resolve to JSON keys
    home_key = key_map.get(home, home.lower())
    away_key = key_map.get(away, away.lower())

    result = nhl_predict(home_key, away_key)
    if not result:
        raise HTTPException(status_code=400, detail=f"Could not predict {away} @ {home}")
    return result


@app.get("/api/nhl/best-bets")
def api_nhl_best_bets():
    """Run predictions on all today's NHL games and find edges."""
    from engine.nhl_predict import generate_nhl_picks
    from engine.data import list_teams, load_team

    games = _get_nhl_scoreboard()
    key_map = _nhl_espn_to_key()

    bets = []
    for game in games:
        if game["status"].get("completed") or game["status"].get("state") == "post":
            continue

        h_abbr = game["home"]["abbreviation"]
        a_abbr = game["away"]["abbreviation"]

        h_key = key_map.get(h_abbr)
        a_key = key_map.get(a_abbr)

        if not h_key or not a_key:
            # Try lowercase name matching
            h_name = game["home"]["name"].split()[-1].lower()
            a_name = game["away"]["name"].split()[-1].lower()
            h_key = h_key or key_map.get(h_name, h_name)
            a_key = a_key or key_map.get(a_name, a_name)

        odds = game.get("odds")

        try:
            picks = generate_nhl_picks(h_key, a_key, odds)
        except Exception as e:
            logger.error("NHL prediction failed for %s @ %s: %s", a_abbr, h_abbr, e)
            continue

        if not picks:
            continue

        best = picks[0]  # Already sorted by edge

        bets.append({
            "game_id": game["id"],
            "matchup": f"{a_abbr} @ {h_abbr}",
            "home": game["home"],
            "away": game["away"],
            "time": game["date"],
            "venue": game.get("venue", ""),
            "best_pick": best,
            "all_picks": picks[:4],
            "confidence": best.get("confidence", "lean"),
        })

    bets.sort(key=lambda b: b["best_pick"]["edge"], reverse=True)
    return bets
