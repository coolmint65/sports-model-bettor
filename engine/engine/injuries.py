"""
Unified injury tracking for NHL and MLB.

Fetches current injury data from ESPN's public API, caches it for 2 hours,
and computes expected-goals / expected-runs adjustments based on which
players are out and their estimated impact tier.

Usage (CLI):
    python -m engine.injuries          # Print all injuries for both sports
    python -m engine.injuries --nhl    # NHL only
    python -m engine.injuries --mlb    # MLB only
"""

import json
import logging
import time
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

# ── ESPN endpoints ──────────────────────────────────────────

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"

_NHL_INJURIES_URL = f"{ESPN_BASE}/hockey/nhl/injuries"
_MLB_INJURIES_URL = f"{ESPN_BASE}/baseball/mlb/injuries"

_NHL_TEAM_INJURIES_URL = f"{ESPN_BASE}/hockey/nhl/teams/{{team_id}}/injuries"
_MLB_TEAM_INJURIES_URL = f"{ESPN_BASE}/baseball/mlb/teams/{{team_id}}/injuries"

# ── Module-level cache ──────────────────────────────────────

CACHE_TTL = 7200  # 2 hours in seconds

_cache: dict[str, tuple[float, dict]] = {}


def _cache_get(key: str) -> dict | None:
    """Return cached value if it exists and hasn't expired."""
    entry = _cache.get(key)
    if entry is None:
        return None
    ts, data = entry
    if time.time() - ts > CACHE_TTL:
        del _cache[key]
        return None
    return data


def _cache_set(key: str, data: dict) -> None:
    _cache[key] = (time.time(), data)


# ── HTTP helper ─────────────────────────────────────────────

def _fetch_json(url: str) -> dict | None:
    """Fetch JSON from ESPN. Returns None on any failure (403, timeout, etc.)."""
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
                time.sleep(0.5)
    return None


# ── ESPN abbreviation normalization ─────────────────────────
# ESPN sometimes uses different abbreviations than the rest of the codebase.

_NHL_ABBR_MAP: dict[str, str] = {
    "TBL": "TB", "NJD": "NJ", "SJS": "SJ", "LAK": "LA",
    "WSH": "WAS", "CBJ": "CLB", "MTL": "MON", "NSH": "NAS",
    "UTAH": "UTA",
}

_MLB_ABBR_MAP: dict[str, str] = {
    "CHW": "CWS", "WAS": "WSH", "AZ": "ARI",
}

# ESPN team IDs (1-based) for per-team fallback fetching.
# NHL: 32 teams  MLB: 30 teams
_NHL_TEAM_IDS = list(range(1, 36))  # ESPN IDs aren't contiguous; overshoot a bit
_MLB_TEAM_IDS = list(range(1, 31))


_TEAM_NAME_TO_ABBR: dict[str, str] = {
    # NHL
    "Anaheim Ducks": "ANA", "Boston Bruins": "BOS", "Buffalo Sabres": "BUF",
    "Calgary Flames": "CGY", "Carolina Hurricanes": "CAR", "Chicago Blackhawks": "CHI",
    "Colorado Avalanche": "COL", "Columbus Blue Jackets": "CBJ", "Dallas Stars": "DAL",
    "Detroit Red Wings": "DET", "Edmonton Oilers": "EDM", "Florida Panthers": "FLA",
    "Los Angeles Kings": "LAK", "Minnesota Wild": "MIN", "Montreal Canadiens": "MTL",
    "Nashville Predators": "NSH", "New Jersey Devils": "NJD", "New York Islanders": "NYI",
    "New York Rangers": "NYR", "Ottawa Senators": "OTT", "Philadelphia Flyers": "PHI",
    "Pittsburgh Penguins": "PIT", "San Jose Sharks": "SJS", "Seattle Kraken": "SEA",
    "St. Louis Blues": "STL", "Tampa Bay Lightning": "TBL", "Toronto Maple Leafs": "TOR",
    "Utah Hockey Club": "UTA", "Vancouver Canucks": "VAN", "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH", "Winnipeg Jets": "WPG",
    # MLB
    "Arizona Diamondbacks": "ARI", "Atlanta Braves": "ATL", "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS", "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE", "Colorado Rockies": "COL",
    "Detroit Tigers": "DET", "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD", "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL", "Minnesota Twins": "MIN", "New York Mets": "NYM",
    "New York Yankees": "NYY", "Oakland Athletics": "OAK", "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT", "San Diego Padres": "SD", "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL", "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX", "Toronto Blue Jays": "TOR", "Washington Nationals": "WSH",
}


def _normalize_abbr(abbr: str, sport: str) -> str:
    """Map ESPN abbreviation or team name to the codebase's canonical form."""
    # Try full team name lookup first
    if abbr in _TEAM_NAME_TO_ABBR:
        return _TEAM_NAME_TO_ABBR[abbr]

    abbr = abbr.upper().strip()
    mapping = _NHL_ABBR_MAP if sport == "nhl" else _MLB_ABBR_MAP
    return mapping.get(abbr, abbr)


# ── Response parsing ────────────────────────────────────────

def _safe_get(d: Any, *keys: str, default: Any = None) -> Any:
    """Safely traverse nested dicts/lists."""
    current = d
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key, default)
        else:
            return default
        if current is None:
            return default
    return current


def _parse_injury_entry(entry: dict) -> dict:
    """Parse a single injury entry from ESPN's response into our format."""
    # ESPN structures vary; try common key paths
    athlete = entry.get("athlete") or entry.get("player") or {}
    name = (
        athlete.get("displayName")
        or athlete.get("fullName")
        or athlete.get("name", "Unknown")
    )

    position_obj = athlete.get("position") or {}
    position = (
        position_obj.get("abbreviation")
        or position_obj.get("name", "")
    )

    # Status: "Out", "Day-To-Day", "Questionable", "10-Day IL", etc.
    status = entry.get("status") or entry.get("injuryStatus") or "Unknown"
    if isinstance(status, dict):
        status = status.get("type", status.get("description", "Unknown"))

    # Injury type / description
    inj_type = entry.get("type") or entry.get("injuryType") or ""
    if isinstance(inj_type, dict):
        inj_type = inj_type.get("description", inj_type.get("name", ""))

    details_obj = entry.get("details") or {}
    detail = ""
    if isinstance(details_obj, dict):
        detail = details_obj.get("detail", details_obj.get("returnDate", ""))
    elif isinstance(details_obj, str):
        detail = details_obj

    # Fallback: some responses put the description at top level
    if not inj_type:
        inj_type = entry.get("description", "")
    if not detail:
        detail = entry.get("longComment") or entry.get("shortComment") or ""

    return {
        "name": name,
        "position": position,
        "status": status,
        "type": inj_type,
        "detail": detail,
    }


def _parse_team_block(team_block: dict, sport: str) -> tuple[str, list[dict]]:
    """Parse a team's injury block, returning (abbreviation, [injury_entries])."""
    # ESPN structure: each block has "displayName" (team name) and "injuries" list.
    # Team abbreviation can be found in:
    # 1. block.team.abbreviation (old format)
    # 2. block.injuries[0].athlete.team.abbreviation (current format)
    # 3. block.displayName -> map to abbreviation

    abbr = "UNK"

    # Try block.team first (old format)
    team_info = team_block.get("team") or {}
    if team_info.get("abbreviation"):
        abbr = team_info["abbreviation"]

    # Try getting from first injury's athlete.team
    if abbr == "UNK":
        raw_injuries = team_block.get("injuries") or team_block.get("items") or []
        for inj in raw_injuries:
            athlete = inj.get("athlete") or {}
            athlete_team = athlete.get("team") or {}
            if athlete_team.get("abbreviation"):
                abbr = athlete_team["abbreviation"]
                break

    # Fall back to displayName -> abbreviation mapping
    if abbr == "UNK":
        display = team_block.get("displayName", "")
        if display:
            abbr = display  # _normalize_abbr will try to map full names

    abbr = _normalize_abbr(abbr, sport)

    raw_injuries = team_block.get("injuries") or team_block.get("items") or []
    parsed = []
    for entry in raw_injuries:
        parsed.append(_parse_injury_entry(entry))
    return abbr, parsed


# ── Public fetchers ─────────────────────────────────────────

def fetch_nhl_injuries() -> dict[str, list[dict]]:
    """Fetch all NHL injuries from ESPN.

    Returns:
        {team_abbr: [{"name": str, "position": str, "status": str,
                       "type": str, "detail": str}, ...]}
    """
    cached = _cache_get("nhl_injuries")
    if cached is not None:
        return cached

    result: dict[str, list[dict]] = {}

    # Try league-wide endpoint first
    data = _fetch_json(_NHL_INJURIES_URL)
    if data:
        # ESPN wraps teams at top level under various keys
        team_blocks = (
            data.get("injuries")
            or data.get("teams")
            or data.get("items")
            or []
        )
        # Sometimes the response is a flat dict with a "season" key and
        # injuries nested under it — handle that too.
        if not team_blocks and isinstance(data, dict):
            for key in data:
                if isinstance(data[key], list) and len(data[key]) > 0:
                    # Heuristic: a list of dicts that have "team" keys
                    if isinstance(data[key][0], dict) and "team" in data[key][0]:
                        team_blocks = data[key]
                        break

        for block in team_blocks:
            if not isinstance(block, dict):
                continue
            abbr, injuries = _parse_team_block(block, "nhl")
            if injuries:
                result[abbr] = injuries

    # If league-wide returned nothing, try per-team endpoints as fallback
    if not result:
        logger.info("League-wide NHL injuries empty; trying per-team endpoints")
        for tid in _NHL_TEAM_IDS:
            url = _NHL_TEAM_INJURIES_URL.format(team_id=tid)
            team_data = _fetch_json(url)
            if not team_data:
                continue
            # Per-team response may have the team at the top level
            team_info = team_data.get("team") or {}
            abbr = _normalize_abbr(
                team_info.get("abbreviation", ""), "nhl"
            )
            raw = (
                team_data.get("injuries")
                or team_data.get("items")
                or []
            )
            parsed = [_parse_injury_entry(e) for e in raw if isinstance(e, dict)]
            if abbr and parsed:
                result[abbr] = parsed

    _cache_set("nhl_injuries", result)
    logger.info("Fetched NHL injuries for %d teams", len(result))
    return result


def fetch_mlb_injuries() -> dict[str, list[dict]]:
    """Fetch all MLB injuries from ESPN.

    Returns:
        {team_abbr: [{"name": str, "position": str, "status": str,
                       "type": str, "detail": str}, ...]}
    """
    cached = _cache_get("mlb_injuries")
    if cached is not None:
        return cached

    result: dict[str, list[dict]] = {}

    # Try league-wide endpoint first
    data = _fetch_json(_MLB_INJURIES_URL)
    if data:
        team_blocks = (
            data.get("injuries")
            or data.get("teams")
            or data.get("items")
            or []
        )
        if not team_blocks and isinstance(data, dict):
            for key in data:
                if isinstance(data[key], list) and len(data[key]) > 0:
                    if isinstance(data[key][0], dict) and "team" in data[key][0]:
                        team_blocks = data[key]
                        break

        for block in team_blocks:
            if not isinstance(block, dict):
                continue
            abbr, injuries = _parse_team_block(block, "mlb")
            if injuries:
                result[abbr] = injuries

    # Fallback: per-team endpoints
    if not result:
        logger.info("League-wide MLB injuries empty; trying per-team endpoints")
        for tid in _MLB_TEAM_IDS:
            url = _MLB_TEAM_INJURIES_URL.format(team_id=tid)
            team_data = _fetch_json(url)
            if not team_data:
                continue
            team_info = team_data.get("team") or {}
            abbr = _normalize_abbr(
                team_info.get("abbreviation", ""), "mlb"
            )
            raw = (
                team_data.get("injuries")
                or team_data.get("items")
                or []
            )
            parsed = [_parse_injury_entry(e) for e in raw if isinstance(e, dict)]
            if abbr and parsed:
                result[abbr] = parsed

    _cache_set("mlb_injuries", result)
    logger.info("Fetched MLB injuries for %d teams", len(result))
    return result


# ── Impact computation: NHL ─────────────────────────────────

# Positions considered "goalie"
_GOALIE_POSITIONS = {"G"}
# Positions considered forward
_FORWARD_POSITIONS = {"C", "LW", "RW", "F", "W"}
# Positions considered defenseman
_DEFENSE_POSITIONS = {"D", "LD", "RD"}

# Statuses that indicate the player is actually out / unavailable
_OUT_STATUSES = {
    "out", "injured reserve", "ir", "day-to-day", "d2d", "dtd",
    "10-day il", "15-day il", "60-day il", "suspended", "paternity",
    "bereavement", "concussion protocol", "long-term injured reserve",
    "ltir",
}


def _is_player_out(status: str) -> bool:
    """Determine if the player's status means they are unavailable."""
    s = status.lower().strip()
    # Exact match
    if s in _OUT_STATUSES:
        return True
    # Partial match for things like "Out - Upper Body"
    for keyword in ("out", "il", "ir", "suspended", "injured"):
        if keyword in s:
            return True
    return False


def _nhl_position_tier(position: str) -> str:
    """Classify an NHL player into a tier: goalie, forward, defense, depth."""
    pos = position.upper().strip()
    if pos in _GOALIE_POSITIONS:
        return "goalie"
    if pos in _FORWARD_POSITIONS:
        return "forward"
    if pos in _DEFENSE_POSITIONS:
        return "defense"
    # Unknown position — treat as depth
    return "depth"


def compute_nhl_injury_impact(team_abbr: str, injuries: list[dict]) -> float:
    """Compute expected goals multiplier based on injured players.

    Impact tiers:
    - Star goalie out (starter with .915+ SV%): opponent gets +0.4 xG
      (~13% increase on a ~3.0 xG baseline)
    - Top-6 forward out: -0.08 xG per player
    - Top-4 defenseman out: -0.06 xG per player
    - Bottom-6 / depth: -0.02 xG per player

    We don't have individual player stats from ESPN injuries alone, so we
    use heuristics: the first goalie listed is assumed to be the starter,
    the first few forwards are top-6, first few D are top-4.

    Args:
        team_abbr: Team abbreviation (used for logging).
        injuries: List of injury dicts from fetch_nhl_injuries().

    Returns:
        Multiplier (e.g. 0.95 means 5% reduction in team's expected goals).
        For goalie injuries, the *opponent* benefits — so we return a
        multiplier > 1.0 representing the opponent's xG boost converted
        to a same-team reduction.  Callers should apply this to the
        injured team's own xG as a penalty.
    """
    if not injuries:
        return 1.0

    baseline_xg = 3.0  # approximate league-average goals per game
    total_adjustment = 0.0  # negative means fewer goals for this team

    goalie_count = 0
    forward_count = 0
    defense_count = 0

    for inj in injuries:
        if not _is_player_out(inj.get("status", "")):
            continue

        tier = _nhl_position_tier(inj.get("position", ""))

        if tier == "goalie":
            goalie_count += 1
            if goalie_count == 1:
                # Assume the first goalie listed is the starter.
                # Losing the starter means the team allows ~0.4 more goals,
                # which we model as a penalty on the team's own output
                # (opponent is effectively stronger).
                total_adjustment -= 0.4
                logger.debug(
                    "%s: starter goalie %s out -> -0.40 xG adjustment",
                    team_abbr, inj.get("name"),
                )
            else:
                # Backup goalie out is minor
                total_adjustment -= 0.05
        elif tier == "forward":
            forward_count += 1
            if forward_count <= 6:
                # Top-6 forward
                total_adjustment -= 0.08
                logger.debug(
                    "%s: top-6 F %s out -> -0.08 xG", team_abbr, inj.get("name"),
                )
            else:
                total_adjustment -= 0.02
        elif tier == "defense":
            defense_count += 1
            if defense_count <= 4:
                # Top-4 defenseman
                total_adjustment -= 0.06
                logger.debug(
                    "%s: top-4 D %s out -> -0.06 xG", team_abbr, inj.get("name"),
                )
            else:
                total_adjustment -= 0.02
        else:
            total_adjustment -= 0.02

    if total_adjustment == 0.0:
        return 1.0

    multiplier = max(0.70, 1.0 + total_adjustment / baseline_xg)
    logger.info(
        "%s injury impact: %.3f xG adjustment -> %.3f multiplier "
        "(%d G, %d F, %d D out)",
        team_abbr, total_adjustment, multiplier,
        goalie_count, forward_count, defense_count,
    )
    return round(multiplier, 4)


# ── Impact computation: MLB ─────────────────────────────────

# MLB positions that indicate a pitcher
_PITCHER_POSITIONS = {"P", "SP", "RP", "CL"}

# Positions that indicate a position player (batter)
_BATTER_POSITIONS = {"C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "DH", "OF", "IF", "UT"}


def _mlb_player_tier(position: str) -> str:
    """Classify an MLB player: pitcher, batter, or bench."""
    pos = position.upper().strip()
    if pos in _PITCHER_POSITIONS:
        return "pitcher"
    if pos in _BATTER_POSITIONS:
        return "batter"
    return "bench"


def compute_mlb_injury_impact(team_id: int, injuries: list[dict]) -> float:
    """Compute expected runs multiplier based on injured players.

    Impact tiers:
    - Ace pitcher on IL: -0.3 runs (but typically handled by pitcher matchup
      module already, so we only apply -0.1 here as residual pen/rotation effect)
    - Star batter (first 4 batters out): -0.15 runs/game each
    - Regular starter (batters 5-9): -0.08 runs/game each
    - Bench / depth player: -0.02 runs/game each

    Args:
        team_id: Team's MLB ID (used for potential DB lookups in the future).
        injuries: List of injury dicts from fetch_mlb_injuries().

    Returns:
        Multiplier (e.g. 0.97 means 3% reduction in expected runs).
    """
    if not injuries:
        return 1.0

    baseline_runs = 4.5  # approximate league-average runs per game
    total_adjustment = 0.0

    pitcher_count = 0
    batter_count = 0

    for inj in injuries:
        if not _is_player_out(inj.get("status", "")):
            continue

        tier = _mlb_player_tier(inj.get("position", ""))

        if tier == "pitcher":
            pitcher_count += 1
            if pitcher_count == 1:
                # Ace / top starter — residual impact beyond pitcher matchup
                total_adjustment -= 0.10
                logger.debug(
                    "team %s: ace pitcher %s on IL -> -0.10 runs residual",
                    team_id, inj.get("name"),
                )
            else:
                # Additional pitchers on IL hurt rotation depth
                total_adjustment -= 0.03
        elif tier == "batter":
            batter_count += 1
            if batter_count <= 4:
                # Star / cleanup hitter
                total_adjustment -= 0.15
                logger.debug(
                    "team %s: star batter %s out -> -0.15 runs",
                    team_id, inj.get("name"),
                )
            elif batter_count <= 9:
                # Regular starter
                total_adjustment -= 0.08
                logger.debug(
                    "team %s: starter %s out -> -0.08 runs",
                    team_id, inj.get("name"),
                )
            else:
                total_adjustment -= 0.02
        else:
            total_adjustment -= 0.02

    if total_adjustment == 0.0:
        return 1.0

    multiplier = max(0.70, 1.0 + total_adjustment / baseline_runs)
    logger.info(
        "Team %s injury impact: %.3f runs adjustment -> %.3f multiplier "
        "(%d P, %d batters out)",
        team_id, total_adjustment, multiplier,
        pitcher_count, batter_count,
    )
    return round(multiplier, 4)


# ── CLI ─────────────────────────────────────────────────────

def _print_injuries(sport: str, injuries: dict[str, list[dict]]) -> None:
    """Pretty-print injuries for a sport."""
    header = f"{'=' * 60}\n  {sport.upper()} INJURIES\n{'=' * 60}"
    print(header)
    if not injuries:
        print("  No injury data available (ESPN may be unreachable).\n")
        return

    total_players = 0
    for team_abbr in sorted(injuries.keys()):
        players = injuries[team_abbr]
        out_players = [p for p in players if _is_player_out(p.get("status", ""))]
        if not out_players and not players:
            continue
        total_players += len(players)

        print(f"\n  {team_abbr} ({len(players)} injured)")
        print(f"  {'-' * 40}")
        for p in players:
            status_flag = "*" if _is_player_out(p.get("status", "")) else " "
            pos = p.get("position", "??")
            name = p.get("name", "Unknown")
            status = p.get("status", "?")
            inj_type = p.get("type", "")
            detail = p.get("detail", "")
            extra = f" ({inj_type})" if inj_type else ""
            extra += f" - {detail}" if detail else ""
            print(f"  {status_flag} [{pos:>3}] {name:<25} {status}{extra}")

    print(f"\n  Total: {len(injuries)} teams, {total_players} players listed\n")


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    show_nhl = True
    show_mlb = True

    if "--nhl" in sys.argv:
        show_mlb = False
    elif "--mlb" in sys.argv:
        show_nhl = False

    if show_nhl:
        nhl = fetch_nhl_injuries()
        _print_injuries("NHL", nhl)
        # Show impact calculations for teams with injuries
        if nhl:
            print("  NHL Injury Impact Multipliers:")
            print(f"  {'-' * 40}")
            for abbr in sorted(nhl.keys()):
                mult = compute_nhl_injury_impact(abbr, nhl[abbr])
                if mult != 1.0:
                    print(f"    {abbr}: {mult:.4f}x expected goals")
            print()

    if show_mlb:
        mlb = fetch_mlb_injuries()
        _print_injuries("MLB", mlb)
        if mlb:
            print("  MLB Injury Impact Multipliers:")
            print(f"  {'-' * 40}")
            for abbr in sorted(mlb.keys()):
                mult = compute_mlb_injury_impact(0, mlb[abbr])
                if mult != 1.0:
                    print(f"    {abbr}: {mult:.4f}x expected runs")
            print()
