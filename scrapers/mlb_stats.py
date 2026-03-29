"""
MLB data pipeline using the MLB Stats API (statsapi.mlb.com).

Fetches teams, rosters, schedules, game results, standings, and
probable pitchers.  This is the primary real-time data source.

Usage:
    python -m scrapers.mlb_stats              # Full sync
    python -m scrapers.mlb_stats --today      # Today's games only
    python -m scrapers.mlb_stats --season     # Full season results
    python -m scrapers.mlb_stats --rosters    # Rosters only
"""

import json
import logging
import time
import urllib.request
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

MLB_API = "https://statsapi.mlb.com/api/v1"
SEASON = datetime.now().year

# ── HTTP helpers ────────────────────────────────────────────

def _fetch(url: str, retries: int = 3) -> dict | None:
    """Fetch JSON from MLB Stats API with retry."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "SportsBettor/1.0",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            logger.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


# ── Teams ───────────────────────────────────────────────────

def fetch_teams() -> list[dict]:
    """Fetch all 30 MLB teams."""
    data = _fetch(f"{MLB_API}/teams?sportId=1&season={SEASON}")
    if not data:
        return []

    from engine.db import upsert_team

    teams = []
    for t in data.get("teams", []):
        team = {
            "mlb_id": t["id"],
            "name": t.get("name", ""),
            "abbreviation": t.get("abbreviation", ""),
            "city": t.get("locationName", ""),
            "venue": t.get("venue", {}).get("name", ""),
            "league": t.get("league", {}).get("abbreviation", ""),
            "division": t.get("division", {}).get("name", "").replace(
                "American League ", "").replace("National League ", ""),
        }
        upsert_team(**team)
        teams.append(team)
        logger.info("Team: %s (%s)", team["name"], team["abbreviation"])

    logger.info("Loaded %d teams", len(teams))
    return teams


# ── Rosters ─────────────────────────────────────────────────

def fetch_roster(team_id: int) -> list[dict]:
    """Fetch 40-man roster for a team."""
    data = _fetch(f"{MLB_API}/teams/{team_id}/roster?rosterType=40Man&season={SEASON}")
    if not data:
        return []

    from engine.db import upsert_player

    players = []
    for entry in data.get("roster", []):
        person = entry.get("person", {})
        pos = entry.get("position", {})

        player = {
            "mlb_id": person.get("id"),
            "name": person.get("fullName", ""),
            "team_id": team_id,
            "position": pos.get("abbreviation", ""),
            "bats": "",
            "throws": "",
        }

        # Fetch player detail for bats/throws
        detail = _fetch(f"{MLB_API}/people/{player['mlb_id']}")
        if detail and detail.get("people"):
            p = detail["people"][0]
            player["bats"] = p.get("batSide", {}).get("code", "")
            player["throws"] = p.get("pitchHand", {}).get("code", "")

        upsert_player(**player)
        players.append(player)

    logger.info("  Roster for team %d: %d players", team_id, len(players))
    return players


def fetch_all_rosters():
    """Fetch rosters for all 30 teams."""
    from engine.db import get_all_teams
    teams = get_all_teams()
    for i, t in enumerate(teams):
        logger.info("[%d/%d] Fetching roster: %s", i + 1, len(teams), t["name"])
        fetch_roster(t["mlb_id"])
        time.sleep(0.5)


# ── Schedule & Games ────────────────────────────────────────

def fetch_schedule(start_date: str, end_date: str) -> list[dict]:
    """
    Fetch games between two dates (YYYY-MM-DD format).
    Includes probable pitchers, scores, venue, weather.
    """
    url = (f"{MLB_API}/schedule?sportId=1"
           f"&startDate={start_date}&endDate={end_date}"
           f"&hydrate=probablePitcher,linescore,weather,venue,team")
    data = _fetch(url)
    if not data:
        return []

    from engine.db import upsert_game

    games = []
    for date_entry in data.get("dates", []):
        for g in date_entry.get("games", []):
            game_id = g.get("gamePk")
            status_code = g.get("status", {}).get("abstractGameCode", "")

            home_team = g.get("teams", {}).get("home", {})
            away_team = g.get("teams", {}).get("away", {})

            home_pp = home_team.get("probablePitcher", {})
            away_pp = away_team.get("probablePitcher", {})

            # Weather
            weather = g.get("weather", {})

            game = {
                "mlb_game_id": game_id,
                "date": g.get("officialDate", g.get("gameDate", "")[:10]),
                "home_team_id": home_team.get("team", {}).get("id"),
                "away_team_id": away_team.get("team", {}).get("id"),
                "home_score": home_team.get("score"),
                "away_score": away_team.get("score"),
                "status": _map_status(status_code),
                "home_pitcher_id": home_pp.get("id"),
                "away_pitcher_id": away_pp.get("id"),
                "venue": g.get("venue", {}).get("name", ""),
                "day_night": g.get("dayNight", ""),
                "weather_temp": _safe_float(weather.get("temp")),
                "weather_wind": weather.get("wind", ""),
                "season": SEASON,
            }

            # If final, extract winning/losing/save pitchers
            if game["status"] == "final":
                decisions = g.get("decisions", {})
                game["winning_pitcher"] = decisions.get("winner", {}).get("id")
                game["losing_pitcher"] = decisions.get("loser", {}).get("id")
                game["save_pitcher"] = decisions.get("save", {}).get("id")

            upsert_game(**game)
            games.append(game)

    logger.info("Fetched %d games from %s to %s", len(games), start_date, end_date)
    return games


def fetch_today() -> list[dict]:
    """Fetch today's schedule."""
    today = datetime.now().strftime("%Y-%m-%d")
    return fetch_schedule(today, today)


def fetch_season_results(season: int | None = None) -> list[dict]:
    """Fetch all games for a season (or current season)."""
    yr = season or SEASON
    # MLB regular season: late March through September
    start = f"{yr}-03-20"
    end = f"{yr}-10-01"
    today = datetime.now().strftime("%Y-%m-%d")
    if end > today:
        end = today
    return fetch_schedule(start, end)


# ── Standings ───────────────────────────────────────────────

def fetch_standings() -> dict:
    """Fetch current standings for all divisions."""
    data = _fetch(f"{MLB_API}/standings?leagueId=103,104&season={SEASON}"
                  f"&standingsTypes=regularSeason&hydrate=team")
    if not data:
        return {}

    from engine.db import get_conn

    conn = get_conn()
    standings = {}

    for record in data.get("records", []):
        for entry in record.get("teamRecords", []):
            team_id = entry.get("team", {}).get("id")
            if not team_id:
                continue

            wins = entry.get("wins", 0)
            losses = entry.get("losses", 0)
            streak_data = entry.get("streak", {})
            streak = f"{streak_data.get('streakType', 'W')}{streak_data.get('streakNumber', 0)}"
            run_diff = entry.get("runDifferential", 0)

            # Home/away records
            split_records = entry.get("records", {}).get("splitRecords", [])
            home_w, home_l, away_w, away_l = 0, 0, 0, 0
            for sr in split_records:
                if sr.get("type") == "home":
                    home_w = sr.get("wins", 0)
                    home_l = sr.get("losses", 0)
                elif sr.get("type") == "away":
                    away_w = sr.get("wins", 0)
                    away_l = sr.get("losses", 0)

            # Last 10
            l10_w, l10_l = 0, 0
            for sr in split_records:
                if sr.get("type") == "lastTen":
                    l10_w = sr.get("wins", 0)
                    l10_l = sr.get("losses", 0)

            conn.execute("""
                INSERT INTO team_stats (team_id, season, wins, losses, run_diff,
                    home_wins, home_losses, away_wins, away_losses,
                    last_10_wins, last_10_losses, streak)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(team_id, season) DO UPDATE SET
                    wins=excluded.wins, losses=excluded.losses,
                    run_diff=excluded.run_diff,
                    home_wins=excluded.home_wins, home_losses=excluded.home_losses,
                    away_wins=excluded.away_wins, away_losses=excluded.away_losses,
                    last_10_wins=excluded.last_10_wins,
                    last_10_losses=excluded.last_10_losses,
                    streak=excluded.streak,
                    updated_at=datetime('now')
            """, (team_id, SEASON, wins, losses, run_diff,
                  home_w, home_l, away_w, away_l, l10_w, l10_l, streak))

            standings[team_id] = {
                "wins": wins, "losses": losses, "streak": streak,
                "run_diff": run_diff, "l10": f"{l10_w}-{l10_l}",
            }

    conn.commit()
    logger.info("Updated standings for %d teams", len(standings))
    return standings


# ── Player Stats (season-level from MLB API) ────────────────

def fetch_player_stats(player_id: int, season: int | None = None,
                       group: str = "pitching") -> dict:
    """Fetch season stats for a player from MLB Stats API."""
    yr = season or SEASON
    url = (f"{MLB_API}/people/{player_id}/stats"
           f"?stats=season&season={yr}&group={group}")
    data = _fetch(url)
    if not data:
        return {}

    stats_list = data.get("stats", [])
    if not stats_list:
        return {}

    splits = stats_list[0].get("splits", [])
    if not splits:
        return {}

    return splits[0].get("stat", {})


def sync_pitcher_stats(player_id: int, team_id: int | None = None,
                        season: int | None = None) -> dict | None:
    """Fetch and store pitcher stats for a season."""
    yr = season or SEASON
    raw = fetch_player_stats(player_id, yr, "pitching")
    if not raw:
        return None

    from engine.db import get_conn

    conn = get_conn()
    conn.execute("""
        INSERT INTO pitcher_stats (player_id, season, team_id,
            games, games_started, wins, losses, saves,
            innings, hits, runs, earned_runs, walks, strikeouts, home_runs,
            era, whip, k_per_9, bb_per_9, hr_per_9)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_id, season) DO UPDATE SET
            team_id=excluded.team_id,
            games=excluded.games, games_started=excluded.games_started,
            wins=excluded.wins, losses=excluded.losses, saves=excluded.saves,
            innings=excluded.innings, hits=excluded.hits, runs=excluded.runs,
            earned_runs=excluded.earned_runs, walks=excluded.walks,
            strikeouts=excluded.strikeouts, home_runs=excluded.home_runs,
            era=excluded.era, whip=excluded.whip,
            k_per_9=excluded.k_per_9, bb_per_9=excluded.bb_per_9,
            hr_per_9=excluded.hr_per_9,
            updated_at=datetime('now')
    """, (
        player_id, yr, team_id,
        _safe_int(raw.get("gamesPlayed")),
        _safe_int(raw.get("gamesStarted")),
        _safe_int(raw.get("wins")),
        _safe_int(raw.get("losses")),
        _safe_int(raw.get("saves")),
        _safe_float(raw.get("inningsPitched")),
        _safe_int(raw.get("hits")),
        _safe_int(raw.get("runs")),
        _safe_int(raw.get("earnedRuns")),
        _safe_int(raw.get("baseOnBalls")),
        _safe_int(raw.get("strikeOuts")),
        _safe_int(raw.get("homeRuns")),
        _safe_float(raw.get("era")),
        _safe_float(raw.get("whip")),
        _safe_float(raw.get("strikeoutsPer9Inn")),
        _safe_float(raw.get("walksPer9Inn")),
        _safe_float(raw.get("homeRunsPer9")),
    ))
    conn.commit()
    return raw


def sync_batter_stats(player_id: int, team_id: int | None = None,
                       season: int | None = None) -> dict | None:
    """Fetch and store batter stats for a season."""
    yr = season or SEASON
    raw = fetch_player_stats(player_id, yr, "hitting")
    if not raw:
        return None

    from engine.db import get_conn

    conn = get_conn()
    conn.execute("""
        INSERT INTO batter_stats (player_id, season, team_id,
            games, plate_appearances, at_bats, hits, doubles, triples,
            home_runs, rbi, stolen_bases, walks, strikeouts,
            avg, obp, slg, ops)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_id, season) DO UPDATE SET
            team_id=excluded.team_id,
            games=excluded.games, plate_appearances=excluded.plate_appearances,
            at_bats=excluded.at_bats, hits=excluded.hits,
            doubles=excluded.doubles, triples=excluded.triples,
            home_runs=excluded.home_runs, rbi=excluded.rbi,
            stolen_bases=excluded.stolen_bases, walks=excluded.walks,
            strikeouts=excluded.strikeouts,
            avg=excluded.avg, obp=excluded.obp, slg=excluded.slg,
            ops=excluded.ops,
            updated_at=datetime('now')
    """, (
        player_id, yr, team_id,
        _safe_int(raw.get("gamesPlayed")),
        _safe_int(raw.get("plateAppearances")),
        _safe_int(raw.get("atBats")),
        _safe_int(raw.get("hits")),
        _safe_int(raw.get("doubles")),
        _safe_int(raw.get("triples")),
        _safe_int(raw.get("homeRuns")),
        _safe_int(raw.get("rbi")),
        _safe_int(raw.get("stolenBases")),
        _safe_int(raw.get("baseOnBalls")),
        _safe_int(raw.get("strikeOuts")),
        _safe_float(raw.get("avg")),
        _safe_float(raw.get("obp")),
        _safe_float(raw.get("slg")),
        _safe_float(raw.get("ops")),
    ))
    conn.commit()
    return raw


def sync_all_player_stats(season: int | None = None):
    """Sync stats for all rostered players."""
    from engine.db import get_conn

    conn = get_conn()
    players = conn.execute(
        "SELECT mlb_id, name, team_id, position FROM players WHERE active = 1"
    ).fetchall()

    total = len(players)
    for i, p in enumerate(players):
        pid = p["mlb_id"]
        pos = p["position"]
        name = p["name"]

        if pos == "P" or pos == "TWP":
            logger.info("[%d/%d] Pitcher stats: %s", i + 1, total, name)
            sync_pitcher_stats(pid, p["team_id"], season)
        else:
            logger.info("[%d/%d] Batter stats: %s", i + 1, total, name)
            sync_batter_stats(pid, p["team_id"], season)

        # Be polite
        if (i + 1) % 10 == 0:
            time.sleep(0.5)


# ── Helpers ─────────────────────────────────────────────────

def _map_status(code: str) -> str:
    return {"P": "scheduled", "S": "scheduled", "L": "live",
            "I": "live", "F": "final"}.get(code, "scheduled")


def _safe_float(val, default=None) -> float | None:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=0) -> int:
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


# ── Full Sync ───────────────────────────────────────────────

def full_sync():
    """Run a complete data sync: teams, rosters, season games, standings, stats."""
    logger.info("=== MLB Full Data Sync ===")
    start = time.time()

    logger.info("--- Step 1/5: Teams ---")
    fetch_teams()

    logger.info("--- Step 2/5: Rosters ---")
    fetch_all_rosters()

    logger.info("--- Step 3/5: Season Games ---")
    fetch_season_results()

    logger.info("--- Step 4/5: Standings ---")
    fetch_standings()

    logger.info("--- Step 5/5: Player Stats ---")
    sync_all_player_stats()

    elapsed = time.time() - start
    logger.info("=== Sync complete in %.0f seconds ===", elapsed)


def daily_sync():
    """Quick daily update: today's games, standings, probable pitchers."""
    logger.info("=== MLB Daily Sync ===")

    logger.info("--- Today's games ---")
    fetch_today()

    logger.info("--- Standings ---")
    fetch_standings()

    # Also fetch tomorrow for probable pitchers
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info("--- Tomorrow's schedule ---")
    fetch_schedule(tomorrow, tomorrow)

    logger.info("=== Daily sync complete ===")


# ── CLI entry point ─────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("data/logs/mlb_sync.log", mode="a"),
        ]
    )

    args = set(sys.argv[1:])

    if "--today" in args or "--daily" in args:
        daily_sync()
    elif "--season" in args:
        fetch_teams()
        fetch_season_results()
        fetch_standings()
    elif "--rosters" in args:
        fetch_teams()
        fetch_all_rosters()
    elif "--standings" in args:
        fetch_teams()
        fetch_standings()
    elif "--stats" in args:
        sync_all_player_stats()
    else:
        full_sync()
