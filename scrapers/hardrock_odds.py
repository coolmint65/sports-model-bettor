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
CACHE_TTL = 600
EMPTY_CACHE_TTL = 120  # short TTL when we got nothing -- Hard Rock may
                       # just be momentarily 403'ing and we don't want to
                       # hammer the endpoint from every best-bets tick.


# ── Default request body / headers ─────────────────────────
#
# Default GraphQL query. When we don't know the exact query the Hard
# Rock JS bundle sends, try a reasonable event-tree shape most
# sportsbook GraphQL schemas expose. Overriden by hardrock_query.json
# once the user pastes a real request body.

_DEFAULT_BRAND = "hrd_online"
_DEFAULT_SEGMENT = "fl"
_DEFAULT_LANGUAGE = "en-us"

_DEFAULT_QUERY_TEMPLATE = {
    "operationName": "EventTree",
    "query": (
        "query EventTree($brand: String!, $segment: String!, "
        "$language: String!) { "
        "  eventTree(brand: $brand, segment: $segment, language: $language) { "
        "    sports { id name "
        "      events { id name startTime "
        "        participants { id name role } "
        "        markets { id name type "
        "          outcomes { id label line oddsAmerican oddsDecimal } } } } } }"
    ),
    "variables": {
        "brand": _DEFAULT_BRAND,
        "segment": _DEFAULT_SEGMENT,
        "language": _DEFAULT_LANGUAGE,
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
    from .nba_dk_odds import _NBA_NAME_TO_ABBR as _NBA_MAP  # type: ignore
except Exception:
    _NBA_MAP = {}

_NHL_NAME_TO_ABBR: dict[str, str] = {
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
}

# Sport identifiers seen in Hard Rock's sports tree. We don't know the
# exact codes yet -- these are the common variants sportsbook APIs use.
# Matcher falls back to substring checks on the sport name so any of
# ("MLB", "Baseball - MLB", "baseball_mlb", etc.) will match.
_SPORT_HINTS = {
    "mlb": ("mlb", "baseball"),
    "nhl": ("nhl", "hockey"),
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
    """Return (away_name, home_name) best-effort."""
    home = _pick(event, "homeTeam", "home", "homeTeamName", "homeName")
    away = _pick(event, "awayTeam", "away", "awayTeamName", "awayName")
    if isinstance(home, dict):
        home = _pick(home, "name", "displayName", "fullName", "label")
    if isinstance(away, dict):
        away = _pick(away, "name", "displayName", "fullName", "label")
    if home and away:
        return str(away), str(home)

    parts = _pick(event, "participants", "competitors", "teams")
    if isinstance(parts, list) and len(parts) >= 2:
        role_map = {}
        for p in parts:
            if not isinstance(p, dict):
                continue
            role = (_pick(p, "role", "homeAway", "side") or "").lower()
            name = _pick(p, "name", "displayName", "fullName", "label", "teamName")
            if role and name:
                role_map[role] = str(name)
        if "home" in role_map and "away" in role_map:
            return role_map["away"], role_map["home"]
        names = [str(_pick(p, "name", "displayName", "fullName", "label", "teamName") or "")
                 for p in parts if isinstance(p, dict)]
        if len(names) >= 2:
            # Sportsbook convention: participants[0] often home, [1] away.
            # Also try parsing "Away @ Home" from the event name as a backup.
            title = str(_pick(event, "name", "title", "displayName") or "")
            if " @ " in title:
                a, h = title.split(" @ ", 1)
                return a.strip(), h.strip()
            if " at " in title.lower():
                low = title.lower()
                idx = low.find(" at ")
                return title[:idx].strip(), title[idx + 4:].strip()
            return names[1], names[0]
    return "", ""


def _market_kind(label: str, market_type: str = "") -> str | None:
    s = f"{label} {market_type}".lower()
    if "moneyline" in s or "money line" in s or s.strip() == "ml" or "match winner" in s:
        return "ml"
    if "puck line" in s or "run line" in s or "spread" in s or "handicap" in s:
        return "spread"
    if "total" in s or "over/under" in s or "over under" in s:
        return "total"
    return None


def _is_q1(label: str) -> bool:
    s = (label or "").lower()
    return any(h in s for h in ("1st quarter", "first quarter", "1q", " q1", "q1 "))


def _classify_outcome(outcome: dict, away_abbr: str, home_abbr: str,
                      sport: str) -> tuple[str | None, int | None, float | None]:
    """Return (side, american_price, line) for an outcome dict."""
    label = (_pick(outcome, "label", "name", "participantName", "selection") or "").strip()
    side_hint = (_pick(outcome, "side", "type", "outcomeType") or "").strip().lower()

    american = _int_odds(_pick(outcome, "oddsAmerican", "americanOdds", "priceUsOdds"))
    if american is None:
        dec = _float_line(_pick(outcome, "oddsDecimal", "decimalOdds", "price"))
        if dec is not None:
            american = _decimal_to_american(dec)

    line = _float_line(_pick(outcome, "line", "handicap", "points", "spread", "total"))

    abbr = _team_abbr(sport, label) if label else ""
    if abbr == home_abbr:
        return "home", american, line
    if abbr == away_abbr:
        return "away", american, line
    lower = label.lower()
    if "over" in lower or side_hint == "over":
        return "over", american, line
    if "under" in lower or side_hint == "under":
        return "under", american, line
    return None, american, line


def _apply_market(bucket: dict, kind: str, sides: dict, q1: bool) -> None:
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
        if q1:
            if sides.get("home", {}).get("line") is not None:
                bucket["q1_spread"] = sides["home"]["line"]
            if sides.get("home", {}).get("price") is not None:
                bucket["q1_spread_home_odds"] = sides["home"]["price"]
            if sides.get("away", {}).get("price") is not None:
                bucket["q1_spread_away_odds"] = sides["away"]["price"]
        else:
            if sides.get("home", {}).get("line") is not None:
                bucket["home_spread_point"] = sides["home"]["line"]
                bucket["home_spread_odds"] = sides["home"]["price"]
            if sides.get("away", {}).get("line") is not None:
                bucket["away_spread_point"] = sides["away"]["line"]
                bucket["away_spread_odds"] = sides["away"]["price"]
    elif kind == "total":
        line = (sides.get("over", {}).get("line")
                or sides.get("under", {}).get("line"))
        if line is not None:
            bucket["q1_total" if q1 else "over_under"] = line
        if sides.get("over", {}).get("price") is not None:
            bucket["q1_over_odds" if q1 else "over_odds"] = sides["over"]["price"]
        if sides.get("under", {}).get("price") is not None:
            bucket["q1_under_odds" if q1 else "under_odds"] = sides["under"]["price"]


def _parse_response(sport: str, data: Any) -> dict[str, dict]:
    """Walk the Hard Rock response and extract {'AWAY@HOME': odds_dict}."""
    result: dict[str, dict] = {}

    # Descend into data.* -- GraphQL results are wrapped in {"data": {...}}
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
        data = data["data"]

    # Two layouts: sports[].events[] (tree) or just events[] (flat)
    events_to_process: list[tuple[str, list[dict]]] = []
    sports = _walk_sports(data)
    if sports:
        for s in sports:
            sport_name = str(_pick(s, "name", "displayName", "id") or "")
            events = _pick(s, "events", "eventList") or []
            if isinstance(events, list):
                events_to_process.append((sport_name, events))
    else:
        flat = _walk_events_flat(data)
        if flat:
            events_to_process.append(("", flat))

    for sport_name, events in events_to_process:
        # Filter by sport if the node is labeled
        if sport_name and not _matches_sport(sport_name, sport):
            continue

        for event in events:
            if not isinstance(event, dict):
                continue
            away_name, home_name = _extract_teams(event)
            away_abbr = _team_abbr(sport, away_name)
            home_abbr = _team_abbr(sport, home_name)
            if not (away_abbr and home_abbr):
                continue
            key = f"{away_abbr}@{home_abbr}"
            bucket = result.setdefault(key, {"provider": "HardRock"})

            markets = (_pick(event, "markets", "offers", "bets", "marketGroups") or [])
            if not isinstance(markets, list):
                continue

            for m in markets:
                if not isinstance(m, dict):
                    continue
                # Some GraphQL schemas nest the real markets under a group
                inner = _pick(m, "markets")
                iterable = inner if isinstance(inner, list) else [m]
                for mkt in iterable:
                    if not isinstance(mkt, dict):
                        continue
                    label = str(_pick(mkt, "name", "label", "marketName") or "")
                    mtype = str(_pick(mkt, "type", "marketType") or "")
                    kind = _market_kind(label, mtype)
                    if kind is None:
                        continue
                    outcomes = _pick(mkt, "outcomes", "selections", "runners") or []
                    if not isinstance(outcomes, list) or len(outcomes) < 2:
                        continue
                    q1 = _is_q1(label) if sport == "nba" else False

                    sides: dict[str, dict] = {}
                    for o in outcomes:
                        if not isinstance(o, dict):
                            continue
                        side, price, line = _classify_outcome(o, away_abbr, home_abbr, sport)
                        if side is None:
                            continue
                        sides[side] = {"price": price, "line": line}
                    _apply_market(bucket, kind, sides, q1)

    # Drop entries without real pricing -- an empty stub isn't useful.
    return {k: v for k, v in result.items()
            if any(x in v for x in (
                "home_ml", "away_ml", "over_under",
                "q1_spread", "q1_total", "q1_home_ml",
            ))}


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

    Tolerates line continuations in all three (``\`` for bash,
    ``^`` for cmd, ``` ` ``` for PowerShell).
    """
    import shlex
    # Normalize line continuations. Windows cmd wraps with trailing ``^``
    # followed by a newline; DevTools 'Copy as cURL (cmd)' also
    # escape-quotes double quotes with ``^"`` inline.
    text = curl_text.replace("\r\n", "\n")
    # Strip line-continuation markers first
    text = text.replace("\\\n", " ").replace("^\n", " ").replace("`\n", " ")
    # Windows cURL: ``^"..."^`` wraps quoted strings; unescape to plain "
    text = text.replace('^"', '"')

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
