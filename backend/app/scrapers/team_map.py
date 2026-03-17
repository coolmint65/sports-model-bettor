"""
Canonical NHL team-name mapping — single source of truth.

Every scraper and service that needs to resolve a sportsbook/API team name
to a 3-letter NHL abbreviation should import from here instead of
maintaining its own mapping dict.

The map covers:
  - Official full names (e.g. "Boston Bruins" -> "BOS")
  - Common sportsbook variants (e.g. "LA Kings" -> "LAK")
  - MoneyPuck CSV abbreviations (e.g. "L.A" -> "LAK")
  - Accent/punctuation variants (e.g. "Montréal Canadiens" -> "MTL")
"""

import logging
from typing import Dict

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# Full-name -> abbreviation (primary)
# -------------------------------------------------------------------------
NHL_TEAM_MAP: Dict[str, str] = {
    # Full names
    "Anaheim Ducks": "ANA",
    "Arizona Coyotes": "ARI",
    "Boston Bruins": "BOS",
    "Buffalo Sabres": "BUF",
    "Calgary Flames": "CGY",
    "Carolina Hurricanes": "CAR",
    "Chicago Blackhawks": "CHI",
    "Colorado Avalanche": "COL",
    "Columbus Blue Jackets": "CBJ",
    "Dallas Stars": "DAL",
    "Detroit Red Wings": "DET",
    "Edmonton Oilers": "EDM",
    "Florida Panthers": "FLA",
    "Los Angeles Kings": "LAK",
    "Minnesota Wild": "MIN",
    "Montreal Canadiens": "MTL",
    "Nashville Predators": "NSH",
    "New Jersey Devils": "NJD",
    "New York Islanders": "NYI",
    "New York Rangers": "NYR",
    "Ottawa Senators": "OTT",
    "Philadelphia Flyers": "PHI",
    "Pittsburgh Penguins": "PIT",
    "San Jose Sharks": "SJS",
    "Seattle Kraken": "SEA",
    "St. Louis Blues": "STL",
    "Tampa Bay Lightning": "TBL",
    "Toronto Maple Leafs": "TOR",
    "Utah Hockey Club": "UTA",
    "Vancouver Canucks": "VAN",
    "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH",
    "Winnipeg Jets": "WPG",
    # ----- Sportsbook / API variants -----
    "LA Kings": "LAK",
    "L.A. Kings": "LAK",
    "Montréal Canadiens": "MTL",
    "NY Islanders": "NYI",
    "NY Rangers": "NYR",
    "N.Y. Islanders": "NYI",
    "N.Y. Rangers": "NYR",
    "St Louis Blues": "STL",
    "Saint Louis Blues": "STL",
    "Utah HC": "UTA",
    "Utah Mammoth": "UTA",
    "Utah": "UTA",
    "Vegas": "VGK",
    "Golden Knights": "VGK",
    "Tampa Bay": "TBL",
    "Lightning": "TBL",
    "Maple Leafs": "TOR",
    "Blue Jackets": "CBJ",
    "Red Wings": "DET",
}

# -------------------------------------------------------------------------
# MoneyPuck CSV abbreviation -> standard abbreviation
# Most are identity mappings; only the non-obvious ones matter.
# -------------------------------------------------------------------------
MONEYPUCK_TEAM_MAP: Dict[str, str] = {
    "ANA": "ANA", "ARI": "ARI", "BOS": "BOS", "BUF": "BUF",
    "CAR": "CAR", "CBJ": "CBJ", "CGY": "CGY", "CHI": "CHI",
    "COL": "COL", "DAL": "DAL", "DET": "DET", "EDM": "EDM",
    "FLA": "FLA", "MIN": "MIN", "MTL": "MTL", "NSH": "NSH",
    "NYI": "NYI", "NYR": "NYR", "OTT": "OTT", "PHI": "PHI",
    "PIT": "PIT", "SEA": "SEA", "STL": "STL", "TOR": "TOR",
    "UTA": "UTA", "VAN": "VAN", "VGK": "VGK", "WPG": "WPG",
    "WSH": "WSH",
    # Non-obvious mappings
    "L.A": "LAK",
    "N.J": "NJD",
    "S.J": "SJS",
    "T.B": "TBL",
}

# -------------------------------------------------------------------------
# Set of valid 3-letter abbreviations (for fast membership checks)
# -------------------------------------------------------------------------
NHL_ABBREVIATIONS = set(NHL_TEAM_MAP.values()) | set(MONEYPUCK_TEAM_MAP.values())

# -------------------------------------------------------------------------
# NBA full-name -> abbreviation
# -------------------------------------------------------------------------
NBA_TEAM_MAP: Dict[str, str] = {
    # Full names
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
    # ----- Sportsbook / API variants -----
    "LA Clippers": "LAC",
    "LA Lakers": "LAL",
    "L.A. Clippers": "LAC",
    "L.A. Lakers": "LAL",
    "NY Knicks": "NYK",
    "N.Y. Knicks": "NYK",
    "GS Warriors": "GSW",
    "Golden State": "GSW",
    "OKC Thunder": "OKC",
    "Oklahoma City": "OKC",
    "San Antonio": "SAS",
    "New Orleans": "NOP",
    "New York": "NYK",
    "Portland": "POR",
    "Trail Blazers": "POR",
    "Timberwolves": "MIN",
    "76ers": "PHI",
    "Sixers": "PHI",
}

NBA_ABBREVIATIONS = set(NBA_TEAM_MAP.values())

# Track unmapped names to avoid log flooding
_unmapped_logged: set = set()


def _resolve_against_map(
    name: str, team_map: Dict[str, str], abbreviations: set
) -> str:
    """Resolve a team name against a specific team map.

    Tries in order:
      1. Direct abbreviation match (e.g. "BOS" -> "BOS")
      2. Exact map lookup (e.g. "Boston Bruins" -> "BOS")
      3. Fuzzy substring match
      4. Mascot-only match (last word)

    Returns empty string if no match is found.
    """
    if not name:
        return ""
    stripped = name.strip()

    # Already a valid abbreviation?
    upper = stripped.upper()
    if upper in abbreviations:
        return upper

    # Direct lookup
    abbr = team_map.get(stripped, "")
    if abbr:
        return abbr

    # Fuzzy: substring matching
    name_lower = stripped.lower()
    for full_name, code in team_map.items():
        if full_name.lower() in name_lower or name_lower in full_name.lower():
            return code

    # Mascot matching (last word)
    mascot = stripped.split()[-1] if stripped else ""
    if mascot:
        mascot_lower = mascot.lower()
        for full_name, code in team_map.items():
            if full_name.split()[-1].lower() == mascot_lower:
                return code

    return ""


def resolve_team(name: str) -> str:
    """Resolve a team name to its 3-letter NHL abbreviation.

    Returns empty string if no match is found.
    """
    result = _resolve_against_map(name, NHL_TEAM_MAP, NHL_ABBREVIATIONS)
    if result:
        return result

    # Log unmapped name once
    stripped = name.strip() if name else ""
    if stripped and stripped not in _unmapped_logged:
        _unmapped_logged.add(stripped)
        logger.debug("UNMAPPED TEAM NAME: %r — add to NHL_TEAM_MAP", stripped)

    return ""


def resolve_nba_team(name: str) -> str:
    """Resolve a team name to its 3-letter NBA abbreviation.

    Returns empty string if no match is found.
    """
    result = _resolve_against_map(name, NBA_TEAM_MAP, NBA_ABBREVIATIONS)
    if result:
        return result

    # Log unmapped name once
    stripped = name.strip() if name else ""
    if stripped and stripped not in _unmapped_logged:
        _unmapped_logged.add(stripped)
        logger.debug("UNMAPPED NBA TEAM NAME: %r — add to NBA_TEAM_MAP", stripped)

    return ""


def resolve_team_for_sport(name: str, sport: str = "nhl") -> str:
    """Resolve a team name using the correct sport-specific map."""
    if sport == "nba":
        return resolve_nba_team(name)
    return resolve_team(name)
