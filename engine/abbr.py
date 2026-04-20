"""
Canonical team-abbreviation aliases.

ESPN, the Odds API, MLB Stats API, and DraftKings all use slightly
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
    """Return all known forms of ``abbr`` (canonical + alternate),
    de-duplicated. Useful when iterating possible keys in a foreign map."""
    if not abbr:
        return []
    table = _BY_SPORT.get(sport, _MLB_BIDIR)
    other = table.get(abbr)
    return [abbr] if other is None else [abbr, other]
