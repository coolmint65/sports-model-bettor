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

# Track unmapped names to avoid log flooding
_unmapped_logged: set = set()


def resolve_team(name: str) -> str:
    """Resolve a team name to its 3-letter NHL abbreviation.

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
    if upper in NHL_ABBREVIATIONS:
        return upper

    # Direct lookup
    abbr = NHL_TEAM_MAP.get(stripped, "")
    if abbr:
        return abbr

    # Fuzzy: substring matching
    name_lower = stripped.lower()
    for full_name, code in NHL_TEAM_MAP.items():
        if full_name.lower() in name_lower or name_lower in full_name.lower():
            return code

    # Mascot matching (last word)
    mascot = stripped.split()[-1] if stripped else ""
    if mascot:
        mascot_lower = mascot.lower()
        for full_name, code in NHL_TEAM_MAP.items():
            if full_name.split()[-1].lower() == mascot_lower:
                return code

    # Log unmapped name once
    if stripped not in _unmapped_logged:
        _unmapped_logged.add(stripped)
        logger.debug("UNMAPPED TEAM NAME: %r — add to NHL_TEAM_MAP", stripped)

    return ""
