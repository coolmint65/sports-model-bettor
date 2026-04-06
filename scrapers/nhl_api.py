"""
NHL data pipeline using the NHL Stats API (api-web.nhle.com).

Fetches teams, rosters, schedules, game results, standings,
goalie stats, skater stats, and team stats.  No authentication required.

Usage:
    python -m scrapers.nhl_api              # Quick sync (today + standings)
    python -m scrapers.nhl_api --full       # Full sync (all data)
    python -m scrapers.nhl_api --history 20242025  # Historical season
"""

import json
import logging
import time
import urllib.request
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

NHL_API = "https://api-web.nhle.com/v1"

# All 32 NHL team abbreviations
NHL_TEAMS = [
    "ANA", "BOS", "BUF", "CAR", "CBJ", "CGY", "CHI", "COL",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NJD",
    "NSH", "NYI", "NYR", "OTT", "PHI", "PIT", "SEA", "SJS",
    "STL", "TBL", "TOR", "UTA", "VAN", "VGK", "WPG", "WSH",
]


def _current_season_str() -> str:
    """Return current season string like '20252026'."""
    now = datetime.now()
    # NHL season spans two calendar years; if before September, we're
    # in the season that started the previous year.
    if now.month >= 9:
        return f"{now.year}{now.year + 1}"
    else:
        return f"{now.year - 1}{now.year}"


def _season_start_year(season_str: str) -> int:
    """Extract start year from season string like '20252026' -> 2025."""
    return int(season_str[:4])


# ── HTTP helpers ────────────────────────────────────────────


def _fetch(url: str, retries: int = 3) -> dict | list | None:
    """Fetch JSON from NHL API with retry."""
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


def _nhl_str(obj: dict | None, *keys: str, default: str = "") -> str:
    """
    Safely extract a string from nested NHL API dicts.

    The NHL API often uses a pattern like:
        {"default": "Connor McDavid", "fr": "Connor McDavid"}
    This helper traverses the keys and grabs .default if the value is a dict.
    """
    val = obj
    for key in keys:
        if not isinstance(val, dict):
            return default
        val = val.get(key)
        if val is None:
            return default
    # If the final value is a dict with a "default" key, extract it
    if isinstance(val, dict):
        return str(val.get("default", default))
    return str(val) if val is not None else default


def _safe_float(val, default=None) -> float | None:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _progress(msg: str) -> None:
    """Print + log so output is always visible."""
    print(msg, flush=True)
    logger.info(msg)


# ── Teams (via standings) ──────────────────────────────────


def fetch_teams() -> list[dict]:
    """
    Fetch all 32 NHL teams from the standings endpoint and save to DB.
    The standings endpoint conveniently provides team metadata.
    """
    data = _fetch(f"{NHL_API}/standings/now")
    if not data:
        _progress("WARNING: NHL standings API returned no data")
        return []

    from engine.nhl_db import upsert_nhl_team

    teams = []
    for entry in data.get("standings", []):
        team_abbr = entry.get("teamAbbrev", {})
        abbr = team_abbr.get("default", "") if isinstance(team_abbr, dict) else str(team_abbr)
        team_name = _nhl_str(entry, "teamName")
        team_common = _nhl_str(entry, "teamCommonName")
        place_name = _nhl_str(entry, "placeName")

        # Build full name from place + common (e.g. "Toronto Maple Leafs")
        full_name = f"{place_name} {team_common}".strip() if place_name and team_common else team_name

        team = {
            "team_id": _safe_int(entry.get("teamId", 0)),
            "name": full_name,
            "abbreviation": abbr,
            "city": place_name,
            "division": _nhl_str(entry, "divisionName"),
            "conference": _nhl_str(entry, "conferenceName"),
            "venue": "",  # not in standings; filled by roster if needed
        }

        if team["team_id"] and team["abbreviation"]:
            upsert_nhl_team(**team)
            teams.append(team)

    _progress(f"Loaded {len(teams)} NHL teams")
    return teams


# ── Rosters ────────────────────────────────────────────────


def fetch_rosters(team_abbr: str | None = None) -> int:
    """
    Fetch rosters for all teams (or one specific team).
    Returns total number of players saved.
    """
    from engine.nhl_db import upsert_nhl_player, get_nhl_team_by_abbr

    abbrs = [team_abbr.upper()] if team_abbr else NHL_TEAMS
    total_players = 0

    for i, abbr in enumerate(abbrs):
        url = f"{NHL_API}/roster/{abbr}/current"
        data = _fetch(url)
        if not data:
            logger.warning("No roster data for %s", abbr)
            time.sleep(1)
            continue

        # Look up team_id from DB
        team = get_nhl_team_by_abbr(abbr)
        tid = team["id"] if team else None

        count = 0
        for group in ["forwards", "defensemen", "goalies"]:
            for p in data.get(group, []):
                pid = p.get("id")
                name = _nhl_str(p, "firstName") + " " + _nhl_str(p, "lastName")
                name = name.strip()
                position = p.get("positionCode", "")
                shoots = p.get("shootsCatches", "")

                if pid and name:
                    upsert_nhl_player(pid, name, tid, position, shoots)
                    count += 1

        total_players += count
        if len(abbrs) > 1 and (i + 1) % 8 == 0:
            _progress(f"  [{i+1}/{len(abbrs)}] rosters fetched...")

        time.sleep(1)

    _progress(f"Loaded {total_players} NHL players")
    return total_players


# ── Schedule & Games ───────────────────────────────────────


def fetch_schedule(start_date: str, end_date: str) -> list[dict]:
    """
    Fetch games between two dates (YYYY-MM-DD).
    Iterates day by day using the NHL schedule endpoint.
    """
    from engine.nhl_db import upsert_nhl_game

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start

    all_games = []
    total_days = (end - start).days + 1
    day_count = 0
    last_month = ""
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")

        # Show monthly progress
        month_str = current.strftime("%Y-%m")
        if month_str != last_month:
            _progress(f"       {month_str}...")
            last_month = month_str

        data = _fetch(f"{NHL_API}/score/{date_str}")
        day_count += 1
        if not data:
            current += timedelta(days=1)
            time.sleep(0.5)
            continue

        for g in data.get("games", []):
            game_id = g.get("id")
            if not game_id:
                continue

            state = g.get("gameState", "FUT")
            if state in ("FUT", "PRE"):
                status = "scheduled"
            elif state in ("LIVE", "CRIT"):
                status = "live"
            else:
                status = "final"

            home = g.get("homeTeam", {})
            away = g.get("awayTeam", {})

            # Determine season from the game date
            game_date = g.get("gameDate", date_str)[:10]
            yr = int(game_date[:4])
            month = int(game_date[5:7])
            season_start = yr if month >= 9 else yr - 1

            game = {
                "date": game_date,
                "home_team_id": home.get("id"),
                "away_team_id": away.get("id"),
                "home_score": _safe_int(home.get("score")) if status != "scheduled" else None,
                "away_score": _safe_int(away.get("score")) if status != "scheduled" else None,
                "status": status,
                "season": season_start,
                "game_type": g.get("gameType", 2),
            }

            upsert_nhl_game(game_id, **game)
            all_games.append({"game_id": game_id, **game})

        current += timedelta(days=1)
        time.sleep(1)

    _progress(f"Fetched {len(all_games)} NHL games from {start_date} to {end_date}")
    return all_games


def fetch_boxscore(game_id: int) -> dict | None:
    """
    Fetch detailed boxscore for a single game.
    Extracts goalie info, shots, PP, faceoffs, hits, blocks.
    """
    data = _fetch(f"{NHL_API}/gamecenter/{game_id}/boxscore")
    if not data:
        return None

    from engine.nhl_db import upsert_nhl_game

    game_state = data.get("gameState", "")
    if game_state not in ("FINAL", "OFF"):
        return None  # only process completed games

    home_stats = data.get("homeTeam", {})
    away_stats = data.get("awayTeam", {})
    boxscore = data.get("boxscore", {})

    # Team-level stats from boxscore
    home_team_stats = boxscore.get("teamGameStats", {}) if isinstance(boxscore, dict) else {}

    # Try extracting from playerByGameStats
    home_goalies = []
    away_goalies = []

    # Parse player-by-game stats to find starting goalies
    pbs = data.get("playerByGameStats", {})
    if not pbs:
        pbs = boxscore.get("playerByGameStats", {}) if isinstance(boxscore, dict) else {}

    home_players = pbs.get("homeTeam", {})
    away_players = pbs.get("awayTeam", {})

    if isinstance(home_players, dict):
        for section in ["goalies"]:
            for gp in home_players.get(section, []):
                home_goalies.append(gp)
    elif isinstance(home_players, list):
        for gp in home_players:
            if gp.get("position", "") == "G":
                home_goalies.append(gp)

    if isinstance(away_players, dict):
        for section in ["goalies"]:
            for gp in away_players.get(section, []):
                away_goalies.append(gp)
    elif isinstance(away_players, list):
        for gp in away_players:
            if gp.get("position", "") == "G":
                away_goalies.append(gp)

    # Starting goalie = goalie with most saves or first listed
    home_goalie_id = home_goalies[0].get("playerId") if home_goalies else None
    away_goalie_id = away_goalies[0].get("playerId") if away_goalies else None

    # Team game stats (the NHL API may nest these differently)
    home_shots = _safe_int(home_stats.get("sog"))
    away_shots = _safe_int(away_stats.get("sog"))

    # Power play: look in teamGameStats
    tgs = boxscore.get("teamGameStats", []) if isinstance(boxscore, dict) else []
    home_pp_goals, home_pp_opps = 0, 0
    away_pp_goals, away_pp_opps = 0, 0
    home_faceoff_pct, away_faceoff_pct = None, None
    home_hits_val, away_hits_val = 0, 0
    home_blocks_val, away_blocks_val = 0, 0

    if isinstance(tgs, list):
        for stat_entry in tgs:
            cat = stat_entry.get("category", "")
            hv = stat_entry.get("homeValue", "")
            av = stat_entry.get("awayValue", "")

            if cat == "sog":
                home_shots = _safe_int(hv) or home_shots
                away_shots = _safe_int(av) or away_shots
            elif cat == "powerPlay":
                # Format: "1/3" (goals/opportunities)
                home_pp = str(hv).split("/")
                away_pp = str(av).split("/")
                if len(home_pp) == 2:
                    home_pp_goals = _safe_int(home_pp[0])
                    home_pp_opps = _safe_int(home_pp[1])
                if len(away_pp) == 2:
                    away_pp_goals = _safe_int(away_pp[0])
                    away_pp_opps = _safe_int(away_pp[1])
            elif cat == "faceoffWinningPctg":
                home_faceoff_pct = _safe_float(hv)
                away_faceoff_pct = _safe_float(av)
            elif cat == "hits":
                home_hits_val = _safe_int(hv)
                away_hits_val = _safe_int(av)
            elif cat == "blockedShots":
                home_blocks_val = _safe_int(hv)
                away_blocks_val = _safe_int(av)

    updates = {
        "home_goalie_id": home_goalie_id,
        "away_goalie_id": away_goalie_id,
        "home_shots": home_shots,
        "away_shots": away_shots,
        "home_pp_goals": home_pp_goals,
        "home_pp_opps": home_pp_opps,
        "away_pp_goals": away_pp_goals,
        "away_pp_opps": away_pp_opps,
        "home_faceoff_pct": home_faceoff_pct,
        "away_faceoff_pct": away_faceoff_pct,
        "home_hits": home_hits_val,
        "away_hits": away_hits_val,
        "home_blocks": home_blocks_val,
        "away_blocks": away_blocks_val,
    }

    upsert_nhl_game(game_id, **updates)
    return updates


def fetch_boxscores_for_date(date_str: str) -> int:
    """Fetch boxscores for all final games on a date. Returns count."""
    from engine.nhl_db import get_conn

    conn = get_conn()
    rows = conn.execute(
        "SELECT game_id FROM nhl_games WHERE date = ? AND status = 'final' AND home_shots IS NULL",
        (date_str,)
    ).fetchall()

    count = 0
    for row in rows:
        result = fetch_boxscore(row["game_id"])
        if result:
            count += 1
        time.sleep(1)

    return count


# ── Goalie Stats ───────────────────────────────────────────


def fetch_goalie_stats(season: str | None = None) -> int:
    """
    Fetch goalie stats from club-stats for every team.
    Returns total number of goalie stat rows saved.
    """
    from engine.nhl_db import upsert_goalie_stats, get_all_nhl_teams

    season_str = season or _current_season_str()
    season_int = _season_start_year(season_str)
    teams = get_all_nhl_teams()
    total = 0

    for team in teams:
        abbr = team["abbreviation"]
        url = f"{NHL_API}/club-stats/{abbr}/now"
        data = _fetch(url)
        if not data:
            time.sleep(1)
            continue

        for g in data.get("goalies", []):
            pid = g.get("playerId")
            if not pid:
                continue

            upsert_goalie_stats(
                player_id=pid,
                season=season_int,
                games=_safe_int(g.get("gamesPlayed")),
                wins=_safe_int(g.get("wins")),
                losses=_safe_int(g.get("losses")),
                ot_losses=_safe_int(g.get("otLosses")),
                save_pct=_safe_float(g.get("savePctg")),
                gaa=_safe_float(g.get("goalsAgainstAvg")),
                shutouts=_safe_int(g.get("shutouts")),
                saves=_safe_int(g.get("saves")),
                shots_against=_safe_int(g.get("shotsAgainst")),
            )
            total += 1

        time.sleep(1)

    _progress(f"Loaded goalie stats for {total} goalies (season {season_str})")
    return total


# ── Skater Stats ───────────────────────────────────────────


def fetch_skater_stats(season: str | None = None) -> int:
    """
    Fetch skater stats from club-stats for every team.
    Returns total number of skater stat rows saved.
    """
    from engine.nhl_db import upsert_skater_stats, get_all_nhl_teams

    season_str = season or _current_season_str()
    season_int = _season_start_year(season_str)
    teams = get_all_nhl_teams()
    total = 0

    for team in teams:
        abbr = team["abbreviation"]
        url = f"{NHL_API}/club-stats/{abbr}/now"
        data = _fetch(url)
        if not data:
            time.sleep(1)
            continue

        for s in data.get("skaters", []):
            pid = s.get("playerId")
            if not pid:
                continue

            upsert_skater_stats(
                player_id=pid,
                season=season_int,
                games=_safe_int(s.get("gamesPlayed")),
                goals=_safe_int(s.get("goals")),
                assists=_safe_int(s.get("assists")),
                points=_safe_int(s.get("points")),
                plus_minus=_safe_int(s.get("plusMinus")),
                pim=_safe_int(s.get("penaltyMinutes")),
                shots=_safe_int(s.get("shots")),
                hits=_safe_int(s.get("hits", 0)),
                blocks=_safe_int(s.get("blockedShots", 0)),
            )
            total += 1

        time.sleep(1)

    _progress(f"Loaded skater stats for {total} skaters (season {season_str})")
    return total


# ── Team Stats (from standings) ────────────────────────────


def fetch_team_stats(season: str | None = None) -> int:
    """
    Fetch team-level stats from the standings endpoint.
    Captures W-L-OTL, points, GF, GA, PP%, PK%, etc.
    Returns count of teams updated.
    """
    from engine.nhl_db import upsert_nhl_team_stats

    data = _fetch(f"{NHL_API}/standings/now")
    if not data:
        _progress("WARNING: NHL standings returned no data")
        return 0

    season_str = season or _current_season_str()
    season_int = _season_start_year(season_str)
    count = 0

    for entry in data.get("standings", []):
        tid = entry.get("teamId")
        if not tid:
            continue

        wins = _safe_int(entry.get("wins"))
        losses = _safe_int(entry.get("losses"))
        ot_losses = _safe_int(entry.get("otLosses"))
        pts = _safe_int(entry.get("points"))
        gf = _safe_int(entry.get("goalFor"))
        ga = _safe_int(entry.get("goalAgainst"))
        gp = _safe_int(entry.get("gamesPlayed")) or 1

        # PP/PK percentages from standings (if available)
        pp_pct = _safe_float(entry.get("powerPlayPctg"))
        pk_pct = _safe_float(entry.get("penaltyKillPctg"))

        # Shots per game (not always in standings; use GF/GA as proxy if needed)
        shots_pg = _safe_float(entry.get("shotsForPerGame"))
        shots_ag = _safe_float(entry.get("shotsAgainstPerGame"))
        fo_pct = _safe_float(entry.get("faceoffWinPctg"))

        upsert_nhl_team_stats(
            team_id=tid,
            season=season_int,
            wins=wins,
            losses=losses,
            ot_losses=ot_losses,
            points=pts,
            goals_for=gf,
            goals_against=ga,
            pp_pct=pp_pct,
            pk_pct=pk_pct,
            shots_per_game=shots_pg,
            shots_against_per_game=shots_ag,
            faceoff_pct=fo_pct,
        )
        count += 1

    _progress(f"Updated team stats for {count} teams (season {season_str})")
    return count


# ── Orchestrators ──────────────────────────────────────────


def sync_nhl(full: bool = False) -> None:
    """
    Main sync orchestrator.
    - Quick mode: teams + standings + today's games
    - Full mode: + rosters + season games + boxscores + player stats
    """
    _progress("=== NHL Data Sync ===")
    start = time.time()

    _progress("[1] Fetching teams + standings...")
    fetch_teams()
    fetch_team_stats()

    if full:
        _progress("[2] Fetching rosters (32 teams, ~1 req/sec)...")
        fetch_rosters()

        _progress("[3] Fetching season games...")
        season = _current_season_str()
        yr = _season_start_year(season)
        start_date = f"{yr}-10-01"
        end_date = datetime.now().strftime("%Y-%m-%d")
        fetch_schedule(start_date, end_date)

        _progress("[4] Fetching boxscores for recent games...")
        # Fetch boxscores for the last 7 days
        for d in range(7):
            dt = (datetime.now() - timedelta(days=d)).strftime("%Y-%m-%d")
            n = fetch_boxscores_for_date(dt)
            if n:
                _progress(f"       {dt}: {n} boxscores")

        _progress("[5] Fetching goalie stats...")
        fetch_goalie_stats()

        _progress("[6] Fetching skater stats...")
        fetch_skater_stats()
    else:
        _progress("[2] Fetching today's games...")
        today = datetime.now().strftime("%Y-%m-%d")
        fetch_schedule(today, today)

        _progress("[3] Fetching yesterday's boxscores...")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        fetch_boxscores_for_date(yesterday)

    elapsed = time.time() - start
    _progress(f"=== NHL sync complete in {elapsed:.0f}s ===")


def sync_history(season_str: str) -> None:
    """
    Load a full historical season.
    Season format: '20242025' for the 2024-25 season.
    """
    _progress(f"=== Loading NHL {season_str} Season ===")
    start = time.time()

    yr = _season_start_year(season_str)

    _progress("[1] Fetching teams...")
    fetch_teams()

    _progress(f"[2] Fetching {season_str} schedule...")
    start_date = f"{yr}-10-01"
    end_date = f"{yr + 1}-06-30"
    today = datetime.now().strftime("%Y-%m-%d")
    if end_date > today:
        end_date = today
    games = fetch_schedule(start_date, end_date)
    _progress(f"       Loaded {len(games)} games")

    _progress("[3] Fetching boxscores (this may take a while)...")
    from engine.nhl_db import get_conn
    conn = get_conn()
    rows = conn.execute(
        "SELECT game_id FROM nhl_games WHERE season = ? AND status = 'final' AND home_shots IS NULL",
        (yr,)
    ).fetchall()

    for i, row in enumerate(rows):
        fetch_boxscore(row["game_id"])
        if (i + 1) % 50 == 0:
            _progress(f"       [{i+1}/{len(rows)}] boxscores...")
        time.sleep(1)

    _progress("[4] Fetching team stats...")
    fetch_team_stats(season_str)

    elapsed = time.time() - start
    _progress(f"=== History load complete in {elapsed:.0f}s ===")


# ── CLI entry point ────────────────────────────────────────

if __name__ == "__main__":
    import os
    import sys

    # Ensure data/logs directory exists
    os.makedirs("data/logs", exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("data/logs/nhl_sync.log", mode="a"),
        ],
    )

    args = sys.argv[1:]
    args_set = set(args)

    # Parse --history SEASON
    history_season = None
    for i, a in enumerate(args):
        if a == "--history" and i + 1 < len(args):
            history_season = args[i + 1]

    if history_season:
        sync_history(history_season)
    elif "--full" in args_set:
        sync_nhl(full=True)
    else:
        sync_nhl(full=False)
