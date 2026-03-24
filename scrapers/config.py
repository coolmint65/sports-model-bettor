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

# ESPN API endpoints
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"


def espn_teams_url(sport: str, league: str) -> str:
    return f"{ESPN_BASE}/{sport}/{league}/teams"


def espn_team_stats_url(sport: str, league: str, team_id: str) -> str:
    return f"{ESPN_BASE}/{sport}/{league}/teams/{team_id}/statistics"


def espn_standings_url(sport: str, league: str) -> str:
    return f"{ESPN_BASE}/{sport}/{league}/standings"


def espn_scoreboard_url(sport: str, league: str) -> str:
    return f"{ESPN_BASE}/{sport}/{league}/scoreboard"


def espn_team_schedule_url(sport: str, league: str, team_id: str) -> str:
    return f"{ESPN_BASE}/{sport}/{league}/teams/{team_id}/schedule"


def espn_team_record_url(sport: str, league: str, team_id: str) -> str:
    return f"{ESPN_BASE}/{sport}/{league}/teams/{team_id}"
