"""
NBA data pipeline using ESPN's public API.

Fetches teams, schedules, game results with quarter-by-quarter scores,
standings, and basic team stats.  Focused on extracting Q1 scoring data
for 1st quarter spread prediction.

Usage:
    python -m scrapers.nba_espn              # Quick sync (today + standings)
    python -m scrapers.nba_espn --full       # Full sync (all data)
    python -m scrapers.nba_espn --history 2025  # Historical season (start year)
"""

import json
import logging
import time
import urllib.request
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

ESPN_API = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"

# All 30 NBA team abbreviations
NBA_TEAMS = [
    "ATL", "BOS", "BKN", "CHA", "CHI", "CLE", "DAL", "DEN", "DET", "GSW",
    "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN", "NOP", "NYK",
    "OKC", "ORL", "PHI", "PHX", "POR", "SAC", "SAS", "TOR", "UTA", "WAS",
]


def _current_season_year() -> int:
    """
    Return the start year of the current NBA season.
    NBA season spans two calendar years; if before September,
    we're in the season that started the previous year.
    """
    now = datetime.now()
    if now.month >= 9:
        return now.year
    else:
        return now.year - 1


# -- HTTP helpers ----------------------------------------------------------


def _fetch(url: str, retries: int = 3) -> dict | list | None:
    """Fetch JSON from ESPN API with retry."""
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


def _safe_int(val, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_float(val, default=None) -> float | None:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _progress(msg: str) -> None:
    """Print + log so output is always visible."""
    print(msg, flush=True)
    logger.info(msg)


# -- Teams & Standings -----------------------------------------------------


def fetch_standings() -> list[dict]:
    """
    Fetch current NBA standings from ESPN.
    Also upserts team records into the nba_teams table.
    Returns list of team dicts.
    """
    data = _fetch(f"{ESPN_API}/standings")
    if not data:
        _progress("WARNING: ESPN standings returned no data")
        return []

    from engine.nba_db import upsert_nba_team

    teams = []
    for group in data.get("children", []):
        conf = group.get("name", "")  # "Eastern Conference" / "Western Conference"
        conference = "Eastern" if "East" in conf else "Western"

        for div_group in group.get("children", []):
            division = div_group.get("name", "")

            for entry in div_group.get("standings", {}).get("entries", []):
                team_info = entry.get("team", {})
                team_id = _safe_int(team_info.get("id"))
                if not team_id:
                    continue

                abbr = team_info.get("abbreviation", "")
                name = team_info.get("displayName", "")
                city = team_info.get("location", "")
                venue = ""

                # Try to extract venue from logos/links (not always present)
                for link in team_info.get("links", []):
                    if "venue" in str(link.get("text", "")).lower():
                        venue = link.get("text", "")
                        break

                team = {
                    "team_id": team_id,
                    "name": name,
                    "abbreviation": abbr,
                    "city": city,
                    "conference": conference,
                    "division": division,
                    "venue": venue,
                }

                if abbr:
                    upsert_nba_team(**team)
                    teams.append(team)

    _progress(f"Loaded {len(teams)} NBA teams from standings")
    return teams


# -- Scoreboard & Games ---------------------------------------------------


def fetch_scoreboard(date: str = "") -> list[dict]:
    """
    Fetch games for a specific date (YYYYMMDD format) from ESPN scoreboard.
    If date is empty, fetches today's games.
    Returns list of game dicts with quarter scores extracted.
    """
    url = f"{ESPN_API}/scoreboard"
    if date:
        url += f"?dates={date}"

    data = _fetch(url)
    if not data:
        return []

    from engine.nba_db import upsert_nba_game

    games = []
    for event in data.get("events", []):
        competition = event.get("competitions", [{}])[0]
        game_id = str(event.get("id", ""))
        if not game_id:
            continue

        # Parse date
        game_date_raw = event.get("date", "")
        if game_date_raw:
            # ESPN dates are ISO format: "2025-01-15T00:00Z"
            game_date = game_date_raw[:10]
        else:
            game_date = date[:4] + "-" + date[4:6] + "-" + date[6:8] if len(date) == 8 else ""

        # Determine status
        status_obj = event.get("status", {})
        status_type = status_obj.get("type", {}).get("name", "")
        if status_type == "STATUS_FINAL":
            status = "final"
        elif status_type in ("STATUS_IN_PROGRESS", "STATUS_HALFTIME",
                             "STATUS_END_PERIOD"):
            status = "live"
        else:
            status = "scheduled"

        # Determine season from date
        if game_date:
            yr = int(game_date[:4])
            month = int(game_date[5:7])
            season_start = yr if month >= 9 else yr - 1
        else:
            season_start = _current_season_year()

        # Parse competitors
        home_team_id = away_team_id = None
        home_score = away_score = None
        home_q1 = away_q1 = None
        home_q2 = away_q2 = None
        home_q3 = away_q3 = None
        home_q4 = away_q4 = None

        for comp in competition.get("competitors", []):
            team_obj = comp.get("team", {})
            tid = _safe_int(team_obj.get("id"))
            score = _safe_int(comp.get("score")) if status != "scheduled" else None
            is_home = comp.get("homeAway") == "home"

            # Extract quarter scores from linescores
            linescores = comp.get("linescores", [])
            q_scores = [None, None, None, None]
            for qi, ls in enumerate(linescores[:4]):
                q_scores[qi] = _safe_int(ls.get("value")) if ls.get("value") is not None else None

            if is_home:
                home_team_id = tid
                home_score = score
                home_q1, home_q2, home_q3, home_q4 = q_scores
            else:
                away_team_id = tid
                away_score = score
                away_q1, away_q2, away_q3, away_q4 = q_scores

        game = {
            "date": game_date,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "home_score": home_score,
            "away_score": away_score,
            "home_q1": home_q1,
            "away_q1": away_q1,
            "home_q2": home_q2,
            "away_q2": away_q2,
            "home_q3": home_q3,
            "away_q3": away_q3,
            "home_q4": home_q4,
            "away_q4": away_q4,
            "status": status,
            "season": season_start,
        }

        upsert_nba_game(game_id, **game)
        games.append({"game_id": game_id, **game})

    return games


def fetch_schedule(start_date: str, end_date: str) -> list[dict]:
    """
    Fetch games between two dates (YYYY-MM-DD).
    Iterates day by day using the ESPN scoreboard endpoint.
    Stores games WITH quarter scores when available.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start

    all_games = []
    last_month = ""

    while current <= end:
        date_str = current.strftime("%Y%m%d")

        # Show monthly progress
        month_str = current.strftime("%Y-%m")
        if month_str != last_month:
            _progress(f"       {month_str}...")
            last_month = month_str

        day_games = fetch_scoreboard(date_str)
        all_games.extend(day_games)

        current += timedelta(days=1)
        time.sleep(1)

    _progress(f"Fetched {len(all_games)} NBA games from {start_date} to {end_date}")
    return all_games


# -- Team Stats ------------------------------------------------------------


def fetch_team_stats() -> int:
    """
    Fetch basic team-level stats (pace, off/def rating if available)
    from the ESPN team statistics endpoint.
    Updates nba_q1_stats with pace/rating data where available.
    Returns count of teams updated.
    """
    from engine.nba_db import get_all_nba_teams, get_team_q1_stats, upsert_q1_stats

    teams = get_all_nba_teams()
    if not teams:
        _progress("WARNING: No NBA teams in DB. Run standings first.")
        return 0

    season = _current_season_year()
    count = 0

    for team in teams:
        tid = team["id"]
        url = f"{ESPN_API}/teams/{tid}/statistics"
        data = _fetch(url)
        if not data:
            time.sleep(1)
            continue

        # Parse stats from ESPN response
        pace = None
        off_rating = None
        def_rating = None
        fg_pct = None
        three_pct = None
        ft_rate = None
        reb_rate = None

        # ESPN team stats are in splits -> categories -> stats
        splits = data.get("results", {}).get("stats", {}).get("splits", {})
        if not splits:
            # Try alternate structure
            splits = data.get("statistics", {}).get("splits", {})

        categories = []
        if isinstance(splits, dict):
            categories = splits.get("categories", [])
        elif isinstance(splits, list):
            for split in splits:
                categories.extend(split.get("categories", []))

        for cat in categories:
            cat_name = cat.get("name", "").lower()
            stats_list = cat.get("stats", [])

            for stat in stats_list:
                stat_name = stat.get("name", "").lower()
                stat_val = stat.get("value")

                if stat_name == "pace":
                    pace = _safe_float(stat_val)
                elif stat_name in ("offensiverating", "offrating"):
                    off_rating = _safe_float(stat_val)
                elif stat_name in ("defensiverating", "defrating"):
                    def_rating = _safe_float(stat_val)
                elif stat_name in ("fieldgoalpct", "fgpct"):
                    fg_pct = _safe_float(stat_val)
                elif stat_name in ("threepointfieldgoalpct", "threepointpct", "3ptpct"):
                    three_pct = _safe_float(stat_val)
                elif stat_name in ("freethrowrate", "ftrate"):
                    ft_rate = _safe_float(stat_val)
                elif stat_name in ("reboundingrate", "rebrate", "totalreboundpct"):
                    reb_rate = _safe_float(stat_val)

        # Merge with existing Q1 stats if they exist
        existing = get_team_q1_stats(tid, season)
        if existing:
            update = {
                "games": existing["games"],
                "q1_ppg": existing["q1_ppg"],
                "q1_opp_ppg": existing["q1_opp_ppg"],
                "q1_margin": existing["q1_margin"],
                "q1_home_ppg": existing["q1_home_ppg"],
                "q1_home_opp_ppg": existing["q1_home_opp_ppg"],
                "q1_away_ppg": existing["q1_away_ppg"],
                "q1_away_opp_ppg": existing["q1_away_opp_ppg"],
                "q1_cover_pct": existing["q1_cover_pct"],
                "q1_over_pct": existing["q1_over_pct"],
                "pace": pace or existing["pace"],
                "off_rating": off_rating or existing["off_rating"],
                "def_rating": def_rating or existing["def_rating"],
                "fg_pct": fg_pct or existing["fg_pct"],
                "three_pct": three_pct or existing["three_pct"],
                "ft_rate": ft_rate or existing["ft_rate"],
                "reb_rate": reb_rate or existing["reb_rate"],
                "fast_start_pct": existing["fast_start_pct"],
                "slow_start_pct": existing["slow_start_pct"],
            }
            upsert_q1_stats(tid, season, **update)
        elif any(v is not None for v in [pace, off_rating, def_rating]):
            # Create a new entry with just the team stats
            upsert_q1_stats(tid, season,
                            pace=pace, off_rating=off_rating,
                            def_rating=def_rating,
                            fg_pct=fg_pct, three_pct=three_pct,
                            ft_rate=ft_rate, reb_rate=reb_rate)

        count += 1
        time.sleep(1)

    _progress(f"Updated team stats for {count} NBA teams")
    return count


# -- Q1 Computation --------------------------------------------------------


def compute_all_q1_stats(season: int) -> int:
    """
    Iterate through all completed games for the season and compute
    Q1 profiles for every team.  Returns count of teams updated.
    """
    from engine.nba_db import get_all_nba_teams, compute_q1_stats_from_games

    teams = get_all_nba_teams()
    count = 0

    for team in teams:
        result = compute_q1_stats_from_games(team["id"], season)
        if result:
            count += 1

    _progress(f"Computed Q1 stats for {count} NBA teams (season {season})")
    return count


# -- Orchestrators ---------------------------------------------------------


def sync_nba(full: bool = False) -> None:
    """
    Main sync orchestrator.
    - Quick mode: standings + today's games
    - Full mode: + full season schedule + team stats + Q1 computation
    """
    _progress("=== NBA Data Sync ===")
    start = time.time()

    _progress("[1] Fetching standings + teams...")
    fetch_standings()

    season = _current_season_year()

    if full:
        _progress("[2] Fetching full season schedule...")
        start_date = f"{season}-10-01"
        end_date = datetime.now().strftime("%Y-%m-%d")
        fetch_schedule(start_date, end_date)

        _progress("[3] Fetching team stats...")
        fetch_team_stats()

        _progress("[4] Computing Q1 stats for all teams...")
        compute_all_q1_stats(season)
    else:
        _progress("[2] Fetching today's games...")
        today = datetime.now().strftime("%Y%m%d")
        games = fetch_scoreboard(today)
        _progress(f"       {len(games)} games today")

        _progress("[3] Fetching yesterday's results...")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        yesterday_games = fetch_scoreboard(yesterday)
        _progress(f"       {len(yesterday_games)} games yesterday")

    elapsed = time.time() - start
    _progress(f"=== NBA sync complete in {elapsed:.0f}s ===")


def sync_history(season: int) -> None:
    """
    Load a full historical season.
    Season is the start year (e.g. 2024 for the 2024-25 season).
    """
    _progress(f"=== Loading NBA {season}-{season + 1} Season ===")
    start = time.time()

    _progress("[1] Fetching standings + teams...")
    fetch_standings()

    _progress(f"[2] Fetching {season}-{season + 1} schedule...")
    start_date = f"{season}-10-01"
    end_date = f"{season + 1}-06-30"
    today = datetime.now().strftime("%Y-%m-%d")
    if end_date > today:
        end_date = today
    games = fetch_schedule(start_date, end_date)
    _progress(f"       Loaded {len(games)} games")

    _progress("[3] Computing Q1 stats for all teams...")
    compute_all_q1_stats(season)

    _progress("[4] Fetching team stats...")
    fetch_team_stats()

    elapsed = time.time() - start
    _progress(f"=== History load complete in {elapsed:.0f}s ===")


# -- CLI entry point -------------------------------------------------------

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
            logging.FileHandler("data/logs/nba_sync.log", mode="a"),
        ],
    )

    args = sys.argv[1:]
    args_set = set(args)

    # Parse --history YEAR
    history_season = None
    for i, a in enumerate(args):
        if a == "--history" and i + 1 < len(args):
            history_season = int(args[i + 1])

    if history_season:
        sync_history(history_season)
    elif "--full" in args_set:
        sync_nba(full=True)
    else:
        sync_nba(full=False)
