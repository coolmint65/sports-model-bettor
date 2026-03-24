"""
Scraper configuration: ESPN sport/league mappings and data source URLs.

ESPN's public API requires no authentication and returns JSON.
Base: https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/...
"""

# ESPN sport+league slugs
# Format: (espn_sport, espn_league, our_league_key)
ESPN_LEAGUES = [
    # American Football
    ("football", "nfl", "NFL"),
    ("football", "college-football", "CFB"),

    # Basketball
    ("basketball", "nba", "NBA"),
    ("basketball", "mens-college-basketball", "NCAAB"),
    ("basketball", "womens-college-basketball", "NCAAW"),

    # Baseball
    ("baseball", "mlb", "MLB"),

    # Hockey
    ("hockey", "nhl", "NHL"),

    # Soccer
    ("soccer", "eng.1", "EPL"),
    ("soccer", "uefa.champions", "UCL"),
    ("soccer", "esp.1", "LALIGA"),
    ("soccer", "ger.1", "BUNDESLIGA"),
    ("soccer", "usa.1", "MLS"),
    ("soccer", "usa.nwsl", "NWSL"),
    ("soccer", "mex.1", "LIGAMX"),
]

# Per-league team fetch limits and ESPN group IDs for division filtering.
# ESPN defaults to 50 teams; college leagues need higher limits.
# groups: ESPN group ID to filter by division (e.g. 80 = FBS, 50 = D1 basketball)
LEAGUE_SETTINGS = {
    "NFL":        {"limit": 50},
    "CFB":        {"limit": 200, "groups": 80},     # FBS (130 teams)
    "NBA":        {"limit": 50},
    "NCAAB":      {"limit": 400, "groups": 50},     # D1 (362 teams)
    "NCAAW":      {"limit": 400, "groups": 50},     # D1
    "MLB":        {"limit": 50},
    "NHL":        {"limit": 50},
    "EPL":        {"limit": 50},
    "UCL":        {"limit": 50},
    "LALIGA":     {"limit": 50},
    "BUNDESLIGA": {"limit": 50},
    "MLS":        {"limit": 50},
    "NWSL":       {"limit": 50},
    "LIGAMX":     {"limit": 50},
}

# ESPN API endpoints
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"


def espn_teams_url(sport: str, league: str, limit: int = 50,
                    groups: int | None = None) -> str:
    url = f"{ESPN_BASE}/{sport}/{league}/teams?limit={limit}"
    if groups is not None:
        url += f"&groups={groups}"
    return url


def espn_team_stats_url(sport: str, league: str, team_id: str) -> str:
    return f"{ESPN_BASE}/{sport}/{league}/teams/{team_id}/statistics"


def espn_standings_url(sport: str, league: str) -> str:
    """Standings URL with auto-detected season year.

    ESPN often returns empty standings without a season param.
    Season year = the year the season started:
      - Fall sports (football, basketball, hockey, soccer): current year if Aug+, else last year
      - Spring sports (baseball): current calendar year
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    month, year = now.month, now.year

    if sport == "baseball":
        # MLB season runs within a calendar year
        season = year
    else:
        # Fall-start leagues: season started in Aug-Oct of prior year if we're in Jan-Jul
        season = year if month >= 8 else year - 1

    return f"{ESPN_BASE}/{sport}/{league}/standings?season={season}"


def espn_scoreboard_url(sport: str, league: str) -> str:
    return f"{ESPN_BASE}/{sport}/{league}/scoreboard"


def espn_team_schedule_url(sport: str, league: str, team_id: str) -> str:
    return f"{ESPN_BASE}/{sport}/{league}/teams/{team_id}/schedule"


def espn_team_record_url(sport: str, league: str, team_id: str) -> str:
    return f"{ESPN_BASE}/{sport}/{league}/teams/{team_id}"
