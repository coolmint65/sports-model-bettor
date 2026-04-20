"""
Hard Rock Bet odds scraper (MLB / NHL / NBA).

Hostname + shape discovered from the user's browser Network tab:
- Host: ``api.hardrocksportsbook.com``
- Primary odds endpoint: ``POST /graphql?type=event_tree``
  (41 kB response -- full sports tree with events + markets + outcomes)
- Supporting endpoints (not used yet): ``/getRootLadder``,
  ``/sbk.home?language=en-us``, ``/searchLayout?route=...``
- Brand/segment context visible in the URLs: ``brand=hrd_online``,
  ``segment=fl`` (Florida online)

The GraphQL query body isn't public -- the Hard Rock JS bundle
constructs it client-side. We ship with a reasonable default query
shape + the ability to override it via ``data/hardrock_query.json``
for when the user pastes the exact request body from DevTools.
Same for headers via ``data/hardrock_headers.json`` -- Hard Rock's
bot check needs specific ``X-*`` tokens + cookies that vary per
session, so the scraper reads them from that file when present.

Returns the same dict shape every other odds scraper in this repo
does, keyed by "AWAY@HOME":

    {
        "MTL@TB": {
            "provider": "HardRock",
            "home_ml": -185, "away_ml": 154,
            "over_under": 6.5, "over_odds": -110, "under_odds": -110,
            "home_spread_point": -1.5, "home_spread_odds": 150,
            "away_spread_point":  1.5, "away_spread_odds": -180,
        },
        ...
    }

Empty dict on any failure -- all callers fall through to the next
source in their chain, so a broken Hard Rock path degrades gracefully.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────
HARDROCK_HOST = "https://api.hardrocksportsbook.com"
# Real path (confirmed from user's DevTools cURL) -- note the
# /java-graphql/ prefix. Without it the server returns 404.
EVENT_TREE_URL = f"{HARDROCK_HOST}/java-graphql/graphql?type=event_tree"

# Files the user can drop to override the default request body + headers.
# data/hardrock_query.json   - the GraphQL JSON body (from DevTools cURL)
# data/hardrock_headers.json - request header map (from DevTools cURL)
_REPO_DATA = Path(__file__).resolve().parent.parent / "data"
QUERY_FILE = _REPO_DATA / "hardrock_query.json"
HEADERS_FILE = _REPO_DATA / "hardrock_headers.json"

# Caching
_cache: dict[str, tuple[float, dict]] = {}
CACHE_TTL = 60
EMPTY_CACHE_TTL = 120  # short TTL when we got nothing -- Hard Rock may
                       # just be momentarily 403'ing and we don't want to
                       # hammer the endpoint from every best-bets tick.


# ── Default request body / headers ─────────────────────────
#
# Default GraphQL query. When we don't know the exact query the Hard
# Rock JS bundle sends, try a reasonable event-tree shape most
# sportsbook GraphQL schemas expose. Overriden by hardrock_query.json
# once the user pastes a real request body.

_DEFAULT_LOCALE = "en-us"
_DEFAULT_CHANNEL = "web"
_DEFAULT_REGION = "fl"
_DEFAULT_CMS_SEGMENT = "fl"

_DEFAULT_QUERY_TEMPLATE = {
    "operationName": "BetSync",
    "query": (
        "query BetSync($locale: String!, $channel: String!, "
        "$language: String!, $region: String!, $cmsSegment: String!) { "
        "  betSync(locale: $locale, channel: $channel, language: $language, "
        "          region: $region, cmsSegment: $cmsSegment) { "
        "    numEvents "
        "    sports { id name code "
        "      competitions { id name "
        "        events { data { "
        "          id name sport startTime "
        "          participants { id name shortName position } "
        "          markets { id name type line spread period "
        "            selection { id name type odds rootIdx } "
        "          } "
        "        } } "
        "      } "
        "    } "
        "  } "
        "}"
    ),
    "variables": {
        "locale": _DEFAULT_LOCALE,
        "channel": _DEFAULT_CHANNEL,
        "language": _DEFAULT_LOCALE,
        "region": _DEFAULT_REGION,
        "cmsSegment": _DEFAULT_CMS_SEGMENT,
    },
}

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://app.hardrocksportsbook.com",
    "Referer": "https://app.hardrocksportsbook.com/",
}


def _load_query() -> dict:
    """Prefer user-pasted query body if present, else fall back to
    our best-effort default."""
    if QUERY_FILE.exists():
        try:
            return json.loads(QUERY_FILE.read_text())
        except Exception as e:
            logger.warning("Hard Rock query file %s is not valid JSON: %s",
                           QUERY_FILE, e)
    return _DEFAULT_QUERY_TEMPLATE


def _load_headers() -> dict:
    """Merge default headers with anything in hardrock_headers.json.
    The file wins on conflict -- that's how the user injects their
    session cookie + any brand/device tokens Hard Rock requires."""
    headers = dict(_DEFAULT_HEADERS)
    if HEADERS_FILE.exists():
        try:
            extra = json.loads(HEADERS_FILE.read_text())
            if isinstance(extra, dict):
                headers.update({str(k): str(v) for k, v in extra.items()})
        except Exception as e:
            logger.warning("Hard Rock headers file %s not valid JSON: %s",
                           HEADERS_FILE, e)
    return headers


# ── Team normalization ────────────────────────────────────

try:
    from .dk_odds import _normalize_team as _mlb_to_abbr  # type: ignore
except Exception:
    _mlb_to_abbr = lambda n: (n or "").strip()  # noqa: E731

try:
    from .nba_dk_odds import _NBA_NAME_TO_ABBR as _NBA_MAP_FULL  # type: ignore
except Exception:
    _NBA_MAP_FULL = {}

# NBA short-name map (Hard Rock uses nicknames only, no city)
_NBA_SHORT_TO_ABBR: dict[str, str] = {
    "Hawks": "ATL", "Celtics": "BOS", "Nets": "BKN",
    "Hornets": "CHA", "Bulls": "CHI", "Cavaliers": "CLE",
    "Mavericks": "DAL", "Nuggets": "DEN", "Pistons": "DET",
    "Warriors": "GS", "Rockets": "HOU", "Pacers": "IND",
    "Clippers": "LAC", "Lakers": "LAL", "Grizzlies": "MEM",
    "Heat": "MIA", "Bucks": "MIL", "Timberwolves": "MIN",
    "Pelicans": "NO", "Knicks": "NYK", "Thunder": "OKC",
    "Magic": "ORL", "76ers": "PHI", "Suns": "PHX",
    "Trail Blazers": "POR", "Kings": "SAC", "Spurs": "SA",
    "Raptors": "TOR", "Jazz": "UTA", "Wizards": "WAS",
}
# Merge full names from DK map
_NBA_MAP: dict[str, str] = {**_NBA_MAP_FULL, **_NBA_SHORT_TO_ABBR}

_NHL_NAME_TO_ABBR: dict[str, str] = {
    # Full names
    "Anaheim Ducks": "ANA", "Boston Bruins": "BOS", "Buffalo Sabres": "BUF",
    "Calgary Flames": "CGY", "Carolina Hurricanes": "CAR", "Chicago Blackhawks": "CHI",
    "Colorado Avalanche": "COL", "Columbus Blue Jackets": "CBJ", "Dallas Stars": "DAL",
    "Detroit Red Wings": "DET", "Edmonton Oilers": "EDM", "Florida Panthers": "FLA",
    "Los Angeles Kings": "LA", "Minnesota Wild": "MIN", "Montreal Canadiens": "MTL",
    "Nashville Predators": "NSH", "New Jersey Devils": "NJ", "New York Islanders": "NYI",
    "New York Rangers": "NYR", "Ottawa Senators": "OTT", "Philadelphia Flyers": "PHI",
    "Pittsburgh Penguins": "PIT", "San Jose Sharks": "SJ", "Seattle Kraken": "SEA",
    "St. Louis Blues": "STL", "Tampa Bay Lightning": "TB", "Toronto Maple Leafs": "TOR",
    "Utah Mammoth": "UTA", "Utah Hockey Club": "UTA",
    "Vancouver Canucks": "VAN", "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH", "Winnipeg Jets": "WPG",
    # Short names (Hard Rock format)
    "Ducks": "ANA", "Bruins": "BOS", "Sabres": "BUF",
    "Flames": "CGY", "Hurricanes": "CAR", "Blackhawks": "CHI",
    "Avalanche": "COL", "Blue Jackets": "CBJ", "Stars": "DAL",
    "Red Wings": "DET", "Oilers": "EDM", "Panthers": "FLA",
    "Kings": "LA", "Wild": "MIN", "Canadiens": "MTL",
    "Predators": "NSH", "Devils": "NJ", "Islanders": "NYI",
    "Rangers": "NYR", "Senators": "OTT", "Flyers": "PHI",
    "Penguins": "PIT", "Sharks": "SJ", "Kraken": "SEA",
    "Blues": "STL", "Lightning": "TB", "Maple Leafs": "TOR",
    "Mammoth": "UTA",
    "Canucks": "VAN", "Golden Knights": "VGK",
    "Capitals": "WSH", "Jets": "WPG",
}

# MLB short-name map (Hard Rock uses nicknames only, no city)
_MLB_SHORT_TO_ABBR: dict[str, str] = {
    "Diamondbacks": "ARI", "D-backs": "ARI", "Braves": "ATL",
    "Orioles": "BAL", "Red Sox": "BOS", "Cubs": "CHC",
    "White Sox": "CWS", "Reds": "CIN", "Guardians": "CLE",
    "Rockies": "COL", "Tigers": "DET", "Astros": "HOU",
    "Royals": "KC", "Angels": "LAA", "Dodgers": "LAD",
    "Brewers": "MIL", "Twins": "MIN", "Mets": "NYM",
    "Yankees": "NYY", "Athletics": "OAK", "Phillies": "PHI",
    "Pirates": "PIT", "Padres": "SD", "Giants": "SF",
    "Mariners": "SEA", "Cardinals": "STL", "Rays": "TB",
    "Rangers": "TEX", "Blue Jays": "TOR", "Nationals": "WSH",
    "Marlins": "MIA",
}

# Sport identifiers seen in Hard Rock's sports tree. We don't know the
# exact codes yet -- these are the common variants sportsbook APIs use.
# Matcher falls back to substring checks on the sport name so any of
# ("MLB", "Baseball - MLB", "baseball_mlb", etc.) will match.
_SPORT_HINTS = {
    "mlb": ("mlb", "baseball"),
    "nhl": ("nhl", "hockey", "ice_hockey", "icehockey"),
    "nba": ("nba", "basketball"),
}


def _matches_sport(sport_name: str, sport: str) -> bool:
    name = (sport_name or "").lower()
    return any(h in name for h in _SPORT_HINTS.get(sport, ()))


def _team_abbr(sport: str, name: str) -> str:
    if not name:
        return ""
    name = name.strip()
    if sport == "mlb":
        # Try short-name map first (Hard Rock), then full-name normalizer (DK)
        if name in _MLB_SHORT_TO_ABBR:
            return _MLB_SHORT_TO_ABBR[name]
        return _mlb_to_abbr(name)
    if sport == "nhl":
        return _NHL_NAME_TO_ABBR.get(name, name)
    if sport == "nba":
        return _NBA_MAP.get(name, name)
    return name


# ── HTTP ───────────────────────────────────────────────────

def _decompress_body(body: bytes, encoding: str) -> bytes:
    """Hard Rock serves responses with Accept-Encoding: gzip, br, zstd.
    urllib doesn't auto-decompress; handle the common cases here so
    the parser gets plain JSON regardless of what the server sent."""
    if not body or not encoding:
        return body
    enc = encoding.lower().strip()
    try:
        if enc == "gzip":
            import gzip
            return gzip.decompress(body)
        if enc == "deflate":
            import zlib
            return zlib.decompress(body)
        if enc == "br":
            try:
                import brotli  # type: ignore
                return brotli.decompress(body)
            except ImportError:
                logger.warning("Hard Rock returned brotli but 'brotli' is not installed; "
                               "install it or strip Accept-Encoding: br from the headers file.")
                return body
        if enc == "zstd":
            try:
                import zstandard  # type: ignore
                return zstandard.ZstdDecompressor().decompress(body)
            except ImportError:
                logger.warning("Hard Rock returned zstd but 'zstandard' is not installed; "
                               "install it or strip Accept-Encoding: zstd from the headers file.")
                return body
    except Exception as e:
        logger.warning("Hard Rock: decompress %s failed: %s", enc, e)
    return body


def _graphql_post(url: str, body: dict, headers: dict,
                  timeout: float = 15.0) -> tuple[int, bytes | None, str | None]:
    try:
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            raw = _decompress_body(raw, resp.headers.get("Content-Encoding", ""))
            return resp.status, raw, None
    except urllib.error.HTTPError as e:
        try:
            raw = e.read()
            raw = _decompress_body(raw, e.headers.get("Content-Encoding", "") if e.headers else "")
        except Exception:
            raw = None
        return e.code, raw, f"HTTPError {e.code}"
    except Exception as e:
        return 0, None, str(e)


# ── Parsing ───────────────────────────────────────────────
#
# The GraphQL response shape isn't public so we probe for common
# patterns. The top-level data key is either ``eventTree``,
# ``sports``, or just ``data``. Events live at
# data.<root>.sports[].events[] (most likely) or data.<root>.events[]
# flat.

def _pick(d: dict | None, *keys: str, default: Any = None) -> Any:
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def _int_odds(val: Any) -> int | None:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            return int(val)
        s = str(val).replace("+", "").strip()
        return int(s) if s else None
    except (ValueError, TypeError):
        return None


def _float_line(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _decimal_to_american(dec: float | None) -> int | None:
    if dec is None or dec <= 1.0:
        return None
    return int(round((dec - 1) * 100)) if dec >= 2.0 else int(round(-100 / (dec - 1)))


def _walk_sports(root: Any) -> list[dict]:
    """Find a list of sport dicts anywhere in the response tree."""
    stack = [root]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            sports = cur.get("sports")
            if isinstance(sports, list) and sports and isinstance(sports[0], dict):
                return sports
            for v in cur.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(cur, list):
            for v in cur:
                if isinstance(v, (dict, list)):
                    stack.append(v)
    return []


def _walk_events_flat(root: Any) -> list[dict]:
    """Find an events list at any depth when the response isn't
    grouped under sports[]."""
    stack = [root]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for key in ("events", "eventList", "items"):
                v = cur.get(key)
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    # Heuristic: must look like an event, not some other
                    # list (has participants / markets / home-name field)
                    e0 = v[0]
                    if any(k in e0 for k in
                           ("markets", "participants", "home",
                            "homeTeam", "homeName")):
                        return v
            for v in cur.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(cur, list):
            for v in cur:
                if isinstance(v, (dict, list)):
                    stack.append(v)
    return []


def _extract_teams(event: dict) -> tuple[str, str]:
    """Return (away_name, home_name) best-effort.

    Hard Rock's betSync schema puts teams in ``participants`` with a
    ``position`` field (typically 1 = away, 2 = home in US sports) and
    the event ``name`` as "Away @ Home" or "Away at Home".
    """
    # ── Direct home/away fields (legacy / user-pasted query shapes) ──
    home = _pick(event, "homeTeam", "home", "homeTeamName", "homeName")
    away = _pick(event, "awayTeam", "away", "awayTeamName", "awayName")
    if isinstance(home, dict):
        home = _pick(home, "name", "displayName", "fullName", "label")
    if isinstance(away, dict):
        away = _pick(away, "name", "displayName", "fullName", "label")
    if home and away:
        return str(away), str(home)

    # ── Parse event name first — it's the most reliable signal ──
    # Hard Rock formats: "Away vs Home", "Away vs. Home", "Away @ Home"
    title = str(_pick(event, "name", "title", "displayName") or "")
    if " @ " in title:
        a, h = title.split(" @ ", 1)
        return a.strip(), h.strip()
    if " at " in title.lower():
        low = title.lower()
        idx = low.find(" at ")
        return title[:idx].strip(), title[idx + 4:].strip()
    # "vs." before "vs" to avoid partial match
    for sep in (" vs. ", " vs ", " v "):
        if sep in title.lower():
            idx = title.lower().find(sep)
            return title[:idx].strip(), title[idx + len(sep):].strip()

    # ── Participants list ──
    parts = _pick(event, "participants", "competitors", "teams")
    if isinstance(parts, list) and len(parts) >= 2:
        # Hard Rock uses position: 1 and 2. Try role/homeAway first.
        role_map: dict[str, str] = {}
        pos_map: dict[int, str] = {}
        for p in parts:
            if not isinstance(p, dict):
                continue
            name = str(_pick(p, "name", "displayName", "fullName",
                             "label", "teamName") or "")
            role = (_pick(p, "role", "homeAway", "side", "type") or "").lower()
            pos = _pick(p, "position")
            if role in ("home", "away") and name:
                role_map[role] = name
            if isinstance(pos, int) and name:
                pos_map[pos] = name
        if "home" in role_map and "away" in role_map:
            return role_map["away"], role_map["home"]
        # position convention: 1 = away, 2 = home (US sportsbook standard)
        if 1 in pos_map and 2 in pos_map:
            return pos_map[1], pos_map[2]
        names = [str(_pick(p, "name", "displayName", "fullName",
                           "label", "teamName") or "")
                 for p in parts if isinstance(p, dict)]
        if len(names) >= 2:
            return names[0], names[1]
    return "", ""


# Whitelist of exact market type suffixes that represent game-level
# moneyline, spread, and total markets. Discovered via introspection
# of the Hard Rock GraphQL schema + live data inspection.
#
# NHL:  ICE_HOCKEY:FTOT:ML / :SPRD / :OU
# MLB:  BASEBALL:FTEI:ML / :SPRD / :OU   (FTEI = includes extra innings)
# NBA:  BASKETBALL:FTOT:ML / :SPRD / :OU
#
# Everything else (team totals, period totals, stat totals, props,
# futures/outrights) is excluded by not being in this map.
_GAME_MARKET_TYPES: dict[str, str] = {
    # Moneyline
    "FTOT:ML": "ml", "FTEI:ML": "ml",
    # Spread
    "FTOT:SPRD": "ml_spread", "FTEI:SPRD": "ml_spread",
    # Total
    "FTOT:OU": "total", "FTEI:OU": "total",
}

# Q1 markets use period-level type codes. We match these only when
# the market name or period confirms it's Q1 (not 2nd/3rd/4th quarter).
_Q1_MARKET_TYPES: dict[str, str] = {
    "P:DNB": "q1_ml",       # "1st Quarter Winner"
    "P:SPRD": "q1_spread",  # "1st Quarter Spread"
    "P:OU": "q1_total",     # "1st Quarter Total Points"
}


def _is_q1_market(label: str, period: str = "") -> bool:
    """Check if a period market is specifically Q1."""
    lower = label.lower()
    p = period.upper()
    # Period field: Q1, P1, 1Q, etc.
    if p in ("Q1", "1Q", "P1", "1"):
        return True
    # Label: "1st Quarter ..."
    if "1st quarter" in lower or "first quarter" in lower:
        return True
    return False


def _market_kind(label: str, market_type: str = "",
                 period: str = "") -> str | None:
    """Classify a market as ml, spread, total, or q1_* using the type code.

    Uses a strict whitelist of game-level market type suffixes.
    Q1 markets are identified by period-level type codes + label/period
    confirmation that it's specifically Q1 (not other quarters).
    Falls back to label-based heuristics only when type is missing.
    """
    mtu = market_type.upper()

    # ── Whitelist match on full-game type code suffix ──
    for suffix, kind in _GAME_MARKET_TYPES.items():
        if mtu.endswith(suffix):
            return "spread" if kind == "ml_spread" else kind

    # ── Q1 period markets (NBA only) ──
    if _is_q1_market(label, period):
        for suffix, kind in _Q1_MARKET_TYPES.items():
            if mtu.endswith(suffix):
                return kind

    # ── Fallback: label-based heuristics (for non-betSync schemas) ──
    if not market_type:
        s = label.lower()
        if "moneyline" in s or "money line" in s or "match winner" in s:
            return "ml"
        if "puck line" in s or "run line" in s or "spread" in s:
            return "spread"
        if s.startswith("total") and "total" in s:
            return "total"

    return None


def _is_q1(label: str) -> bool:
    s = (label or "").lower()
    return any(h in s for h in ("1st quarter", "first quarter", "1q", " q1", "q1 "))


def _parse_odds_string(raw: Any) -> int | None:
    """Parse the ``odds`` field from a Hard Rock Selection.

    Hard Rock always sends **decimal** odds as a string (e.g. "2.50",
    "1.90909091", "2", "41").  There is no American format in their
    GraphQL schema, so we always interpret the value as decimal and
    convert to American.

    Also handles edge cases from other providers / user-pasted queries:
      - Explicit American: "+150", "-110" (has +/- prefix)
      - Fractional: "3/2"
    """
    if raw is None:
        return None
    s = str(raw).strip().replace("\u2212", "-")  # unicode minus
    if not s:
        return None
    # Fractional: "3/2"
    if "/" in s:
        try:
            num, den = s.split("/", 1)
            dec = float(num) / float(den) + 1.0
            return _decimal_to_american(dec)
        except (ValueError, ZeroDivisionError):
            return None
    # Explicit American sign: "+150", "-110"
    if s.startswith("+") or s.startswith("-"):
        try:
            return int(s.replace("+", ""))
        except ValueError:
            pass
    # Everything else: treat as decimal odds (Hard Rock's format)
    try:
        dec = float(s)
        if dec > 1.0:
            return _decimal_to_american(dec)
    except ValueError:
        pass
    return None


def _extract_line_from_name(name: str) -> float | None:
    """Extract a numeric spread/handicap from a selection name.

    Hard Rock embeds the line in the name, e.g.:
      "Rangers +1.5", "Mariners -1.5", "Over 5.5"
    """
    import re
    m = re.search(r'[+-]?\d+\.?\d*\s*$', name.strip())
    if m:
        return _float_line(m.group(0).strip())
    return None


def _classify_outcome(outcome: dict, away_abbr: str, home_abbr: str,
                      sport: str,
                      participants: list[dict] | None = None,
                      ) -> tuple[str | None, int | None, float | None]:
    """Return (side, american_price, line) for a selection/outcome dict.

    Hard Rock betSync selections use:
      - ``type``: "A" (position 0 / away), "B" (position 1 / home),
        "AH"/"BH" (spread A/B), "Over", "Under"
      - ``odds``: decimal string like "2.50"
      - ``name``: team name + optional line, e.g. "Rangers +1.5"
    """
    label = (_pick(outcome, "name", "label", "participantName", "selection") or "").strip()
    sel_type = (_pick(outcome, "type", "side", "outcomeType") or "").strip()

    # Hard Rock selections use an ``odds`` string field
    american = _parse_odds_string(_pick(outcome, "odds"))
    if american is None:
        american = _int_odds(_pick(outcome, "oddsAmerican", "americanOdds", "priceUsOdds"))
    if american is None:
        dec = _float_line(_pick(outcome, "oddsDecimal", "decimalOdds", "price"))
        if dec is not None:
            american = _decimal_to_american(dec)

    line = _float_line(_pick(outcome, "line", "handicap", "points", "spread", "total"))
    if line is None:
        line = _extract_line_from_name(label)

    sel_lower = sel_type.lower()

    # ── 1. Selection type field (most reliable for Hard Rock) ──
    # "Over"/"Under" types are unambiguous
    if sel_lower == "over":
        return "over", american, line
    if sel_lower == "under":
        return "under", american, line
    # "A" = position 0 (away), "B" = position 1 (home)
    # "AH"/"BH" = spread for A/B
    if sel_type in ("A", "AH"):
        return "away", american, line
    if sel_type in ("B", "BH"):
        return "home", american, line

    # ── 2. Team abbreviation matching ──
    # Strip any trailing line numbers from label for matching
    # e.g. "Rangers +1.5" → try matching "Rangers"
    import re
    team_part = re.sub(r'\s*[+-]?\d+\.?\d*\s*$', '', label).strip()
    abbr = _team_abbr(sport, team_part) if team_part else ""
    if abbr == home_abbr:
        return "home", american, line
    if abbr == away_abbr:
        return "away", american, line

    # ── 3. Label-based over/under (standalone word only) ──
    # Must be a standalone "Over" or "Under" at the start of the label,
    # not a substring of a team name like "Thunder" or "Rovers".
    lower = label.lower()
    if lower.startswith("over ") or lower == "over":
        return "over", american, line
    if lower.startswith("under ") or lower == "under":
        return "under", american, line

    # ── 4. Participant name matching (fallback) ──
    if participants:
        for p in participants:
            pname = (p.get("name") or "").strip()
            pos = p.get("position")
            if pname and team_part and pname.lower() == team_part.lower():
                if pos == 0:
                    return "away", american, line
                if pos == 1:
                    return "home", american, line

    return None, american, line


def _apply_market(bucket: dict, kind: str, sides: dict, q1: bool) -> None:
    """Apply parsed market sides to the event bucket.

    For spread and total markets, the first market seen becomes the
    "primary" line (backwards-compatible top-level fields). Subsequent
    markets with different lines are appended to ``alt_spreads`` or
    ``alt_totals`` lists so the pick engine can shop for the best edge.

    Each alt entry is a dict:
        alt_spreads: [{"point": -2.5, "home_odds": 150, "away_odds": -180}, ...]
        alt_totals:  [{"line": 5.5, "over_odds": -110, "under_odds": -110}, ...]
    """
    if kind == "ml":
        if q1:
            if sides.get("home", {}).get("price") is not None:
                bucket["q1_home_ml"] = sides["home"]["price"]
            if sides.get("away", {}).get("price") is not None:
                bucket["q1_away_ml"] = sides["away"]["price"]
        else:
            if sides.get("home", {}).get("price") is not None:
                bucket["home_ml"] = sides["home"]["price"]
            if sides.get("away", {}).get("price") is not None:
                bucket["away_ml"] = sides["away"]["price"]
    elif kind == "spread":
        home_line = sides.get("home", {}).get("line")
        home_price = sides.get("home", {}).get("price")
        away_line = sides.get("away", {}).get("line")
        away_price = sides.get("away", {}).get("price")
        if home_line is None and away_line is None:
            return
        if q1:
            cur_q1 = bucket.get("q1_spread")
            if cur_q1 is None:
                if home_line is not None:
                    bucket["q1_spread"] = home_line
                if home_price is not None:
                    bucket["q1_spread_home_odds"] = home_price
                if away_price is not None:
                    bucket["q1_spread_away_odds"] = away_price
            elif home_line != cur_q1:
                point = home_line if home_line is not None else (
                    -away_line if away_line is not None else None)
                if point is not None:
                    alt = {"point": point,
                           "home_odds": home_price, "away_odds": away_price}
                    bucket.setdefault("q1_alt_spreads", []).append(alt)
        else:
            cur = bucket.get("home_spread_point")
            if cur is None:
                # First spread — set as primary
                if home_line is not None:
                    bucket["home_spread_point"] = home_line
                    bucket["home_spread_odds"] = home_price
                if away_line is not None:
                    bucket["away_spread_point"] = away_line
                    bucket["away_spread_odds"] = away_price
            elif home_line == cur or (away_line is not None and -away_line == cur):
                # Same line seen again (from a second comp) — overwrite
                # odds to pick up fresher pricing.
                if home_line is not None:
                    bucket["home_spread_point"] = home_line
                    bucket["home_spread_odds"] = home_price
                if away_line is not None:
                    bucket["away_spread_point"] = away_line
                    bucket["away_spread_odds"] = away_price
            else:
                # Different line — add as alt
                point = home_line if home_line is not None else (
                    -away_line if away_line is not None else None)
                if point is not None:
                    alt = {"point": point,
                           "home_odds": home_price, "away_odds": away_price}
                    bucket.setdefault("alt_spreads", []).append(alt)
    elif kind == "total":
        line = (sides.get("over", {}).get("line")
                or sides.get("under", {}).get("line"))
        over_price = sides.get("over", {}).get("price")
        under_price = sides.get("under", {}).get("price")
        if q1:
            cur_q1t = bucket.get("q1_total")
            if cur_q1t is None:
                if line is not None:
                    bucket["q1_total"] = line
                if over_price is not None:
                    bucket["q1_over_odds"] = over_price
                if under_price is not None:
                    bucket["q1_under_odds"] = under_price
            elif line != cur_q1t:
                if line is not None:
                    alt = {"line": line,
                           "over_odds": over_price, "under_odds": under_price}
                    bucket.setdefault("q1_alt_totals", []).append(alt)
        else:
            cur = bucket.get("over_under")
            if cur is None:
                # First total — set as primary
                if line is not None:
                    bucket["over_under"] = line
                if over_price is not None:
                    bucket["over_odds"] = over_price
                if under_price is not None:
                    bucket["under_odds"] = under_price
            elif line == cur:
                # Same line seen again — overwrite with fresher odds
                if over_price is not None:
                    bucket["over_odds"] = over_price
                if under_price is not None:
                    bucket["under_odds"] = under_price
            else:
                # Different line — add as alt
                if line is not None:
                    alt = {"line": line,
                           "over_odds": over_price, "under_odds": under_price}
                    bucket.setdefault("alt_totals", []).append(alt)


# Competition name filters — only pull events from the target league,
# not eSports, college, or international leagues.
_COMP_FILTERS: dict[str, tuple[str, ...]] = {
    "mlb": ("mlb",),
    "nhl": ("nhl",),
    "nba": ("nba",),
}


def _comp_matches_sport(comp_name: str, sport: str) -> bool:
    """Return True if a competition belongs to the target league.

    Uses word-boundary matching to avoid "nba" matching "wnba".
    """
    filters = _COMP_FILTERS.get(sport)
    if not filters:
        return True
    import re
    name_lower = comp_name.lower().strip()
    for f in filters:
        if name_lower == f:
            return True
        # Word-boundary match: \b ensures "nba" doesn't match "wnba"
        if re.search(r'\b' + re.escape(f) + r'\b', name_lower):
            return True
    return False


def _collect_events_from_sport(sport_node: dict,
                               sport: str = "") -> list[dict]:
    """Gather event dicts from a sport node.

    Hard Rock's betSync nests events under competitions:
        sport -> competitions[] -> events.data[]
    But also supports a flat sport -> events.data[] path.

    When ``sport`` is provided, filters competitions by league name
    (e.g. only "MLB" competitions, not college baseball or eSports).

    Hard Rock sometimes has duplicate competitions for the same league
    (e.g. two "NBA" comps — one for playoff series, one for individual
    games). When duplicates exist, we pick the one with more markets
    per event (the game-level comp) and skip the series/overview comp.
    """
    events: list[dict] = []

    # competitions[].events.data[]
    comps = sport_node.get("competitions")
    if isinstance(comps, list):
        # Hard Rock often has two competitions with the same name for a
        # league (e.g. two "NBA" comps). One is the site-facing comp
        # with correct primary lines and alts; the other is an extended
        # comp with extra/internal markets. The site-facing comp always
        # has fewer markets per event. When duplicates exist, use only
        # the smaller one.
        by_name: dict[str, list[dict]] = {}
        for comp in comps:
            if not isinstance(comp, dict):
                continue
            comp_name = comp.get("name", "")
            if sport and not _comp_matches_sport(comp_name, sport):
                continue
            by_name.setdefault(comp_name.lower(), []).append(comp)

        selected_comps: list[dict] = []
        for group in by_name.values():
            if len(group) > 1:
                def _avg_mkts(c: dict) -> float:
                    ev = c.get("events")
                    ev_list = (ev.get("data", []) if isinstance(ev, dict)
                               else ev if isinstance(ev, list) else [])
                    if not ev_list:
                        return 0.0
                    return sum(len(e.get("markets", []))
                               for e in ev_list if isinstance(e, dict)) / len(ev_list)
                # Site-facing comp (fewest markets) has correct primary
                # lines AND the Q1 alts the site actually displays.
                selected_comps.append(min(group, key=_avg_mkts))
            else:
                selected_comps.append(group[0])

        def _extract_events(comp_list: list[dict]) -> list[dict]:
            evts: list[dict] = []
            for comp in comp_list:
                ev_container = comp.get("events")
                if isinstance(ev_container, dict):
                    raw = ev_container.get("data")
                    if isinstance(raw, list):
                        evts.extend(e for e in raw if isinstance(e, dict))
                elif isinstance(ev_container, list):
                    evts.extend(e for e in ev_container if isinstance(e, dict))
            return evts

        events = _extract_events(selected_comps)

    # sport.events.data[] (flat)
    ev_container = sport_node.get("events")
    if isinstance(ev_container, dict):
        ev_list = ev_container.get("data")
        if isinstance(ev_list, list):
            events.extend(e for e in ev_list if isinstance(e, dict))
    elif isinstance(ev_container, list):
        events.extend(e for e in ev_container if isinstance(e, dict))
    return events


def _parse_response(sport: str, data: Any) -> dict[str, dict]:
    """Walk the Hard Rock response and extract {'AWAY@HOME': odds_dict}."""
    result: dict[str, dict] = {}

    # Descend into data.* -- GraphQL results are wrapped in {"data": {...}}
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
        data = data["data"]

    # Two layouts: sports[].competitions[].events.data[] (betSync tree)
    # or just events[] somewhere flat.
    events_to_process: list[tuple[str, list[dict]]] = []
    sports = _walk_sports(data)
    if sports:
        for s in sports:
            sport_name = str(_pick(s, "name", "displayName", "code", "id") or "")
            events = _collect_events_from_sport(s, sport)
            if events:
                events_to_process.append((sport_name, events))
    else:
        flat = _walk_events_flat(data)
        if flat:
            events_to_process.append(("", flat))

    for sport_name, events in events_to_process:
        # Filter by sport if the node is labeled
        if sport_name and not _matches_sport(sport_name, sport):
            continue

        # Sort events so those with MORE markets come first. When
        # duplicate events exist (same matchup from different comps),
        # the comp with fewer markets (typically the overview/series
        # comp with fresher MLs) processes last and overwrites.
        events = sorted(events,
                        key=lambda e: len(e.get("markets", [])),
                        reverse=True)

        for event in events:
            if not isinstance(event, dict):
                continue
            away_name, home_name = _extract_teams(event)
            away_abbr = _team_abbr(sport, away_name)
            home_abbr = _team_abbr(sport, home_name)
            if not (away_abbr and home_abbr):
                continue
            key = f"{away_abbr}@{home_abbr}"
            bucket = result.setdefault(key, {
                "provider": "HardRock",
                "odds_home": home_abbr,
                "odds_away": away_abbr,
            })

            event_participants = event.get("participants") or []

            markets = (_pick(event, "markets", "offers", "bets", "marketGroups") or [])
            if not isinstance(markets, list):
                continue

            for m in markets:
                if not isinstance(m, dict):
                    continue
                # Some schemas nest the real markets under a group
                inner = _pick(m, "markets")
                iterable = inner if isinstance(inner, list) else [m]
                for mkt in iterable:
                    if not isinstance(mkt, dict):
                        continue
                    label = str(_pick(mkt, "name", "label", "marketName") or "")
                    mtype = str(_pick(mkt, "type", "marketType") or "")
                    mkt_period = str(mkt.get("period") or "")
                    kind = _market_kind(label, mtype, mkt_period)
                    if kind is None:
                        continue

                    # Hard Rock bundles Asian lines (whole numbers) under
                    # the same type as standard half-point lines. The site
                    # only shows .5 lines, so skip whole-number markets.
                    if kind in ("spread", "total", "q1_spread", "q1_total"):
                        outcomes_peek = (_pick(mkt, "selection", "outcomes",
                                              "selections") or [])
                        if outcomes_peek:
                            sample_line = _extract_line_from_name(
                                str(_pick(outcomes_peek[0], "name") or ""))
                            if sample_line is not None and sample_line == int(sample_line):
                                continue
                    # Hard Rock betSync uses "selection"; other shapes
                    # use "outcomes" / "selections" / "runners".
                    outcomes = (_pick(mkt, "selection", "outcomes",
                                     "selections", "runners") or [])
                    if not isinstance(outcomes, list) or len(outcomes) < 2:
                        continue

                    # For spread/total markets, the line may live on the
                    # market itself rather than on each selection.
                    market_line = _float_line(_pick(mkt, "line", "spread"))

                    # Q1 flag for _apply_market: True if the kind
                    # starts with "q1_" (from whitelist) or the legacy
                    # label check matches.
                    q1 = kind.startswith("q1_")

                    sides: dict[str, dict] = {}
                    for o in outcomes:
                        if not isinstance(o, dict):
                            continue
                        side, price, line = _classify_outcome(
                            o, away_abbr, home_abbr, sport,
                            participants=event_participants)
                        if side is None:
                            continue
                        # Fall back to market-level line when the
                        # selection doesn't carry its own.
                        if line is None and market_line is not None:
                            line = market_line
                        sides[side] = {"price": price, "line": line}
                    # Normalize q1_* kinds to base kind for _apply_market
                    base_kind = kind.replace("q1_", "") if q1 else kind
                    _apply_market(bucket, base_kind, sides, q1)

    # Post-process: pick the best primary spread/total from all
    # collected lines. The "primary" should be the one closest to
    # even juice (-110/-110), which is the standard/consensus line.
    for v in result.values():
        _promote_best_primary(v)

    # Drop entries without real pricing -- an empty stub isn't useful.
    return {k: v for k, v in result.items()
            if any(x in v for x in (
                "home_ml", "away_ml", "over_under",
                "q1_spread", "q1_total", "q1_home_ml",
            ))}


def _juice_score(home_odds: int | None, away_odds: int | None,
                 line: float | None = None) -> tuple[float, float]:
    """Score how "standard" a spread/total line is.

    Returns a (primary_score, tiebreak) tuple for sorting. Lower is better.

    Primary score: distance of both sides' odds from even money (±100).
    The consensus line has both sides near -110, while alts have one
    side at long odds (+300/-400).

    Tiebreak: absolute value of the line. When two lines have identical
    juice (e.g. both -105/-115), the one closer to zero is more likely
    the consensus line the site displays.
    """
    if home_odds is None or away_odds is None:
        return (9999.0, abs(line) if line is not None else 9999.0)
    def _dist(odds: int) -> float:
        return abs(abs(odds) - 100)
    return (_dist(home_odds) + _dist(away_odds),
            abs(line) if line is not None else 0.0)


def _promote_best_primary(bucket: dict) -> None:
    """If alt_spreads or alt_totals exist, find the line with the most
    balanced juice and swap it into the primary slot."""
    # ── Spreads ──
    alts = bucket.get("alt_spreads")
    if alts and "home_spread_point" in bucket:
        # Collect all lines (primary + alts)
        all_spreads = [{
            "point": bucket["home_spread_point"],
            "home_odds": bucket.get("home_spread_odds"),
            "away_odds": bucket.get("away_spread_odds"),
        }] + list(alts)
        # Pick the one with most balanced juice, tiebreak by smaller line
        best = min(all_spreads, key=lambda s: _juice_score(s["home_odds"], s["away_odds"], s["point"]))
        if best["point"] != bucket["home_spread_point"]:
            # Swap: current primary becomes an alt
            old_primary = {
                "point": bucket["home_spread_point"],
                "home_odds": bucket.get("home_spread_odds"),
                "away_odds": bucket.get("away_spread_odds"),
            }
            bucket["home_spread_point"] = best["point"]
            bucket["home_spread_odds"] = best["home_odds"]
            bucket["away_spread_point"] = -best["point"] if best["point"] is not None else None
            bucket["away_spread_odds"] = best["away_odds"]
            # Rebuild alt list without the new primary
            new_alts = [s for s in all_spreads
                        if s["point"] != best["point"]]
            bucket["alt_spreads"] = new_alts if new_alts else []
        if not bucket.get("alt_spreads"):
            bucket.pop("alt_spreads", None)

    # ── Totals ──
    alts = bucket.get("alt_totals")
    if alts and "over_under" in bucket:
        all_totals = [{
            "line": bucket["over_under"],
            "over_odds": bucket.get("over_odds"),
            "under_odds": bucket.get("under_odds"),
        }] + list(alts)
        # Filter to reasonable game totals. Props and aggregates can
        # have lines like 275.5 (strikeouts). Game totals are:
        #   MLB: 3-20, NHL: 2-12, NBA: 150-300
        # We use a generous range to not clip unusual games.
        reasonable = [t for t in all_totals
                      if t["line"] is not None and t["line"] <= 300]
        if reasonable:
            best = min(reasonable, key=lambda t: _juice_score(
                t["over_odds"], t["under_odds"], t["line"]))
            bucket["over_under"] = best["line"]
            bucket["over_odds"] = best["over_odds"]
            bucket["under_odds"] = best["under_odds"]
            new_alts = [t for t in reasonable
                        if t["line"] != best["line"]]
            bucket["alt_totals"] = new_alts if new_alts else []
        if not bucket.get("alt_totals"):
            bucket.pop("alt_totals", None)


# ── Fetch / probe ─────────────────────────────────────────

def _fetch_event_tree() -> tuple[dict, str | None]:
    """Do one POST to /graphql?type=event_tree. Returns (parsed_json,
    error_string). error_string is None on 200."""
    body = _load_query()
    headers = _load_headers()
    status, raw, err = _graphql_post(EVENT_TREE_URL, body, headers)
    if status != 200 or not raw:
        return {}, err or f"HTTP {status}"
    try:
        return json.loads(raw), None
    except Exception as e:
        return {}, f"json parse: {e}"


def fetch_hardrock_odds(sport: str) -> dict:
    """Fetch Hard Rock odds for one sport.

    Sport in {"mlb", "nhl", "nba"}. Empty dict on failure (which is
    common right now until the user supplies a working query body +
    headers via data/hardrock_query.json and data/hardrock_headers.json).
    """
    now = time.time()
    cached = _cache.get(sport)
    if cached:
        age = now - cached[0]
        ttl = CACHE_TTL if cached[1] else EMPTY_CACHE_TTL
        if age < ttl:
            return cached[1]

    data, err = _fetch_event_tree()
    if err:
        logger.debug("Hard Rock %s: %s", sport, err)
        _cache[sport] = (now, {})
        return {}
    parsed = _parse_response(sport, data)
    if parsed:
        logger.info("Hard Rock %s: %d games", sport.upper(), len(parsed))
    _cache[sport] = (now, parsed)
    return parsed


def fetch_mlb() -> dict: return fetch_hardrock_odds("mlb")
def fetch_nhl() -> dict: return fetch_hardrock_odds("nhl")
def fetch_nba() -> dict: return fetch_hardrock_odds("nba")


# ── cURL ingestion (for when the user pastes a real DevTools cURL) ─

def load_from_curl(curl_text: str) -> tuple[Path, Path]:
    """Parse a cURL command from DevTools, extract the URL + headers +
    JSON body, and write them to the override files so the next fetch
    uses them. Returns (query_path, headers_path) of what was written.

    Accepts three cURL flavors DevTools produces:
      * Bash  - single quotes:  curl 'url' -H 'a: b' --data-raw '{...}'
      * Bash  - double quotes:  curl "url" -H "a: b" --data-raw "{...}"
      * CMD   - Windows escape: curl.exe ^"url^" ^ -H ^"a: b^"

    Tolerates line continuations in all three (backslash for bash,
    caret for cmd, backtick for PowerShell).
    """
    import re
    import shlex
    # Normalize line continuations. Windows cmd wraps with trailing ``^``
    # followed by a newline; DevTools 'Copy as cURL (cmd)' also
    # escape-quotes double quotes with ``^"`` inline.
    text = curl_text.replace("\r\n", "\n")
    # Strip line-continuation markers first
    text = text.replace("\\\n", " ").replace("^\n", " ").replace("`\n", " ")

    # Windows CMD cURL: the ``^`` character escapes the next character.
    # Firefox "Copy as cURL (Windows)" produces e.g.:
    #   ^"...^"  for quoted strings
    #   ^{, ^}, ^[, ^], ^\, ^$  inside --data-raw
    # Strip all ``^X`` → ``X`` (caret is the CMD escape char).
    text = re.sub(r'\^(.)', r'\1', text)

    try:
        tokens = shlex.split(text, posix=True)
    except ValueError:
        # Fallback for unbalanced quotes -- try posix=False which is
        # more permissive with Windows-style cmd.exe quoting.
        tokens = shlex.split(text, posix=False)
        # Strip surrounding quotes left by posix=False
        tokens = [t.strip('"').strip("'") for t in tokens]

    url = None
    headers: dict[str, str] = {}
    body_text: str | None = None

    i = 0
    while i < len(tokens):
        t = tokens[i]
        # Skip the cURL executable token. DevTools produces "curl" on
        # Unix and "curl.exe" on Windows.
        if t.lower() in ("curl", "curl.exe") and url is None and i + 1 < len(tokens):
            nxt = tokens[i + 1]
            if not nxt.startswith("-"):
                url = nxt
                i += 2
                continue
        elif t in ("-H", "--header") and i + 1 < len(tokens):
            h = tokens[i + 1]
            if ":" in h:
                k, v = h.split(":", 1)
                headers[k.strip()] = v.strip()
            i += 2
            continue
        elif t in ("--data-raw", "--data", "-d", "--data-binary") and i + 1 < len(tokens):
            body_text = tokens[i + 1]
            i += 2
            continue
        elif t in ("-b", "--cookie") and i + 1 < len(tokens):
            # Merge cookies into Cookie header so the scraper picks them up
            headers["Cookie"] = tokens[i + 1]
            i += 2
            continue
        elif t.startswith("http://") or t.startswith("https://"):
            if url is None:
                url = t
        i += 1

    if url is None:
        raise ValueError("Could not find URL in cURL command.")

    if body_text:
        try:
            body_json = json.loads(body_text)
        except Exception as e:
            raise ValueError(f"Body was not JSON: {e}")
        # Extract variables (channel, segment, region, language) from the
        # user's cURL, but replace the query with our own that fetches
        # full event data including markets + selections + odds.
        # The user's browser query often only fetches the sports tree
        # (counts/categories) without actual odds data.
        user_vars = body_json.get("variables", {})
        if user_vars:
            merged_vars = dict(_DEFAULT_QUERY_TEMPLATE["variables"])
            # Map the user's variable names to ours
            for src, dst in [("channel", "channel"), ("segment", "cmsSegment"),
                             ("region", "region"), ("language", "language"),
                             ("locale", "locale")]:
                if src in user_vars:
                    merged_vars[dst] = user_vars[src]
            body_json = dict(_DEFAULT_QUERY_TEMPLATE)
            body_json["variables"] = merged_vars
            logger.info("Hard Rock: extracted variables from user cURL: %s",
                        merged_vars)
    else:
        body_json = _DEFAULT_QUERY_TEMPLATE

    _REPO_DATA.mkdir(parents=True, exist_ok=True)
    QUERY_FILE.write_text(json.dumps(body_json, indent=2))
    HEADERS_FILE.write_text(json.dumps(headers, indent=2))
    logger.info("Hard Rock: wrote query -> %s (%d bytes) and headers -> %s (%d keys) from cURL (url=%s)",
                QUERY_FILE, len(json.dumps(body_json)),
                HEADERS_FILE, len(headers), url)
    return QUERY_FILE, HEADERS_FILE


# ── CLI ────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="mlb", choices=["mlb", "nhl", "nba"])
    ap.add_argument("--raw", action="store_true",
                    help="Dump the raw event_tree JSON to stdout and exit.")
    ap.add_argument("--load-curl", action="store_true",
                    help="Read a DevTools cURL command from stdin and "
                         "write it to data/hardrock_query.json + "
                         "data/hardrock_headers.json so the scraper can "
                         "use it on the next fetch. On Windows the stdin "
                         "terminator is Ctrl+Z then Enter. Prefer "
                         "--curl-file on Windows because cmd.exe mangles "
                         "multi-line pastes.")
    ap.add_argument("--curl-file",
                    help="Path to a text file containing a cURL command. "
                         "On Windows: save the cURL to a .txt file first "
                         "(open the file in Notepad, paste cURL, save) "
                         "then point this at it. Avoids the Ctrl+Z stdin "
                         "pain entirely.")
    args = ap.parse_args()

    if args.curl_file:
        curl_text = Path(args.curl_file).read_text(encoding="utf-8")
        q, h = load_from_curl(curl_text)
        print(f"Wrote:\n  query   -> {q}\n  headers -> {h}")
        print("\nNext: python -m scrapers.hardrock_odds --sport nhl")
        sys.exit(0)

    if args.load_curl:
        print("Paste the cURL command and press Ctrl+D (Unix) or Ctrl+Z "
              "then Enter (Windows):", file=sys.stderr)
        curl_text = sys.stdin.read()
        q, h = load_from_curl(curl_text)
        print(f"Wrote:\n  query   -> {q}\n  headers -> {h}")
        print("\nRun: python -m scrapers.hardrock_odds --sport nhl")
        sys.exit(0)

    if args.raw:
        data, err = _fetch_event_tree()
        if err:
            print(f"ERROR: {err}", file=sys.stderr)
            sys.exit(1)
        print(json.dumps(data, indent=2)[:5000])
        sys.exit(0)

    odds = fetch_hardrock_odds(args.sport)
    print(f"\n{'=' * 60}")
    print(f"  Hard Rock {args.sport.upper()} odds ({len(odds)} games)")
    print(f"{'=' * 60}")
    for key, v in sorted(odds.items()):
        print(f"  {key}: {v}")
    if not odds:
        print(
            "\nNo odds returned. Likely causes:\n"
            f"  1. {QUERY_FILE} is missing/stale -- paste the real cURL:\n"
            "       python -m scrapers.hardrock_odds --load-curl\n"
            "     (from DevTools, right-click the graphql?type=event_tree\n"
            "     request -> Copy as cURL -> paste here)\n"
            "  2. Session cookie in hardrock_headers.json expired -- refresh.\n"
            "  3. Geo/IP block -- Hard Rock Sportsbook is US-only.\n"
        )
