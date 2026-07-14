"""
Canonical team-abbreviation aliases.

ESPN, MLB Stats API, and the various sportsbooks all use slightly
different abbreviations for the same team. Historically each module
that joined two data sources kept its own ad-hoc ALT dict, which is
how ARI/AZ inconsistencies sneak in when one consumer adds a new
sportsbook.

Single source of truth lives here. Consumers that need to look up the
"other" form of an abbreviation call ``alt_abbr(abbr)``; consumers
that need every known form call ``aliases_for(abbr)``.

The mapping is bidirectional -- adding ``"ARI": "AZ"`` automatically
makes ``alt_abbr("AZ")`` return ``"ARI"``.
"""

# MLB ESPN ↔ Odds API differences (the universe most code touches)
MLB_ESPN_ODDS_API_ALIASES = {
    "ARI": "AZ",
    "CHW": "CWS",
    "WSH": "WAS",
    "ATH": "OAK",
}

# Additional MLB variations seen in the wild from other data sources
# (MLB Stats API and some injury feeds use the longer 3-letter forms).
MLB_LONG_FORM_ALIASES = {
    "SF": "SFG",
    "SD": "SDP",
    "TB": "TBR",
    "KC": "KCR",
}

# NHL aliases live alongside their MLB peers so anything that needs to
# disambiguate by sport can branch on a single import.
NHL_ALIASES = {
    "TB":  "TBL",
    "SJ":  "SJS",
    "LA":  "LAK",
    "NJ":  "NJD",
    "WSH": "WAS",
}

# NBA aliases (a few teams have multiple short forms in different feeds).
NBA_ALIASES = {
    "BKN": "BRK",
    "PHX": "PHO",
    "CHA": "CHO",
    "NY":  "NYK",
    "GS":  "GSW",
    "NO":  "NOP",
    "SA":  "SAS",
    "UTAH": "UTA",
    "WSH": "WAS",
}


def _build_bidirectional(*maps: dict) -> dict:
    out: dict[str, str] = {}
    for m in maps:
        for a, b in m.items():
            out[a] = b
            out.setdefault(b, a)
    return out


_MLB_BIDIR = _build_bidirectional(MLB_ESPN_ODDS_API_ALIASES, MLB_LONG_FORM_ALIASES)
_NHL_BIDIR = _build_bidirectional(NHL_ALIASES)
_NBA_BIDIR = _build_bidirectional(NBA_ALIASES)

_BY_SPORT = {
    "mlb": _MLB_BIDIR,
    "nhl": _NHL_BIDIR,
    "nba": _NBA_BIDIR,
}


def alt_abbr(abbr: str, sport: str = "mlb") -> str:
    """Return the alternate form of ``abbr`` for the given sport.

    Falls back to the input when the abbreviation is unknown, so this is
    safe to drop in anywhere a team abbreviation is being keyed against
    a foreign data source.
    """
    if not abbr:
        return abbr
    return _BY_SPORT.get(sport, _MLB_BIDIR).get(abbr, abbr)


def aliases_for(abbr: str, sport: str = "mlb") -> list[str]:
    """Return all known forms of ``abbr`` (canonical + alternate + full
    team name), de-duplicated. Useful when iterating possible keys in a
    foreign map.

    Full names matter for the closing-line-value capture path: Hard
    Rock ships its MLB odds keyed by full team name (e.g. "Arizona
    Diamondbacks@San Diego Padres"), and the identity-tag fallback
    inside ``_find_odds_by_team_pair`` compares ``odds_home`` /
    ``odds_away`` — which HR populates with the full name — against
    our alias set. Without the full name here, MLB CLV coverage
    silently drops to 0% (root cause of June+July 2026 gap)."""
    if not abbr:
        return []
    table = _BY_SPORT.get(sport, _MLB_BIDIR)
    other = table.get(abbr)
    out = [abbr] if other is None else [abbr, other]
    # Enrich with full team names for whichever sport the caller asked
    # about. Adds every known variant (canonical + alt) so the caller
    # doesn't need to also know which form our teams-table stored.
    names = _FULL_NAMES.get(sport, {})
    for form in list(out):
        full = names.get(form)
        if full:
            out.append(full)
    # Dedup while preserving order.
    seen: set[str] = set()
    deduped: list[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            deduped.append(x)
    return deduped


# ── Full team names ──────────────────────────────────────────
#
# abbr -> full name. Keyed by the canonical (ESPN-style) abbreviation
# AND every alternate form we ship — so aliases_for("SD") and
# aliases_for("SDP") both surface "San Diego Padres". This is the
# lookup the CLV identity-tag fallback needs when the sportsbook
# keys its odds by full name instead of abbr.

_MLB_NAMES = {
    "ARI": "Arizona Diamondbacks", "AZ": "Arizona Diamondbacks",
    "ATL": "Atlanta Braves",
    "BAL": "Baltimore Orioles",
    "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs",
    "CHW": "Chicago White Sox", "CWS": "Chicago White Sox",
    "CIN": "Cincinnati Reds",
    "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies",
    "DET": "Detroit Tigers",
    "HOU": "Houston Astros",
    "KC": "Kansas City Royals", "KCR": "Kansas City Royals",
    "LAA": "Los Angeles Angels",
    "LAD": "Los Angeles Dodgers",
    "MIA": "Miami Marlins",
    "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins",
    "NYM": "New York Mets",
    "NYY": "New York Yankees",
    "OAK": "Oakland Athletics", "ATH": "Oakland Athletics",
    "PHI": "Philadelphia Phillies",
    "PIT": "Pittsburgh Pirates",
    "SD": "San Diego Padres", "SDP": "San Diego Padres",
    "SEA": "Seattle Mariners",
    "SF": "San Francisco Giants", "SFG": "San Francisco Giants",
    "STL": "St. Louis Cardinals",
    "TB": "Tampa Bay Rays", "TBR": "Tampa Bay Rays",
    "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays",
    "WSH": "Washington Nationals", "WAS": "Washington Nationals",
}

_NHL_NAMES = {
    "ANA": "Anaheim Ducks",
    "BOS": "Boston Bruins",
    "BUF": "Buffalo Sabres",
    "CGY": "Calgary Flames",
    "CAR": "Carolina Hurricanes",
    "CHI": "Chicago Blackhawks",
    "COL": "Colorado Avalanche",
    "CBJ": "Columbus Blue Jackets",
    "DAL": "Dallas Stars",
    "DET": "Detroit Red Wings",
    "EDM": "Edmonton Oilers",
    "FLA": "Florida Panthers",
    "LA": "Los Angeles Kings", "LAK": "Los Angeles Kings",
    "MIN": "Minnesota Wild",
    "MTL": "Montreal Canadiens",
    "NSH": "Nashville Predators",
    "NJ": "New Jersey Devils", "NJD": "New Jersey Devils",
    "NYI": "New York Islanders",
    "NYR": "New York Rangers",
    "OTT": "Ottawa Senators",
    "PHI": "Philadelphia Flyers",
    "PIT": "Pittsburgh Penguins",
    "SEA": "Seattle Kraken",
    "SJ": "San Jose Sharks", "SJS": "San Jose Sharks",
    "STL": "St. Louis Blues",
    "TB": "Tampa Bay Lightning", "TBL": "Tampa Bay Lightning",
    "TOR": "Toronto Maple Leafs",
    "UTA": "Utah Mammoth",
    "VAN": "Vancouver Canucks",
    "VGK": "Vegas Golden Knights",
    "WSH": "Washington Capitals", "WAS": "Washington Capitals",
    "WPG": "Winnipeg Jets",
}

_NBA_NAMES = {
    "ATL": "Atlanta Hawks",
    "BOS": "Boston Celtics",
    "BKN": "Brooklyn Nets", "BRK": "Brooklyn Nets",
    "CHA": "Charlotte Hornets", "CHO": "Charlotte Hornets",
    "CHI": "Chicago Bulls",
    "CLE": "Cleveland Cavaliers",
    "DAL": "Dallas Mavericks",
    "DEN": "Denver Nuggets",
    "DET": "Detroit Pistons",
    "GS": "Golden State Warriors", "GSW": "Golden State Warriors",
    "HOU": "Houston Rockets",
    "IND": "Indiana Pacers",
    "LAC": "LA Clippers",
    "LAL": "Los Angeles Lakers",
    "MEM": "Memphis Grizzlies",
    "MIA": "Miami Heat",
    "MIL": "Milwaukee Bucks",
    "MIN": "Minnesota Timberwolves",
    "NO": "New Orleans Pelicans", "NOP": "New Orleans Pelicans",
    "NY": "New York Knicks", "NYK": "New York Knicks",
    "OKC": "Oklahoma City Thunder",
    "ORL": "Orlando Magic",
    "PHI": "Philadelphia 76ers",
    "PHX": "Phoenix Suns", "PHO": "Phoenix Suns",
    "POR": "Portland Trail Blazers",
    "SAC": "Sacramento Kings",
    "SA": "San Antonio Spurs", "SAS": "San Antonio Spurs",
    "TOR": "Toronto Raptors",
    "UTAH": "Utah Jazz", "UTA": "Utah Jazz",
    "WSH": "Washington Wizards", "WAS": "Washington Wizards",
}

_FULL_NAMES = {
    "mlb": _MLB_NAMES,
    "nhl": _NHL_NAMES,
    "nba": _NBA_NAMES,
}
