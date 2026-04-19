"""
Hard Rock Bet odds scraper (MLB / NHL / NBA).

Goal: a FREE primary source so the model keeps getting live prices when
The Odds API plan is at/over its monthly credit limit.

Status: scaffolding. Hard Rock Bet is NOT on the Kambi platform (user
confirmed no ``kambicdn.org`` traffic in their browser's Network tab),
so we can't use the public Kambi offering API the way BetRivers /
Unibet consumers do. Hard Rock Digital built their own backend,
internally referred to as "Cerberus". Public-web URL patterns we've
observed / inferred (needs verification against the user's live site):

    https://app.hardrock.bet/api/sports-data/...
    https://app.hardrock.bet/api/v1/events/...
    https://api.app.hardrock.bet/...
    https://cerberus-service.app.hardrock.bet/graphql
    https://sbapi.app.hardrock.bet/...

The scraper tries each base URL in turn, logs what responds, and
attempts to parse using a generic "events -> markets -> outcomes"
shape that most sportsbook APIs expose. When the live response shape
doesn't match, the probe CLI (`python -m scrapers.hardrock_odds
--probe`) dumps the raw JSON so we can refine the parser.

Returns the same dict shape as scrapers.odds_api so callers can drop
it in transparently:

    {
        "AWAY@HOME": {
            "provider": "HardRock",
            "home_ml": -150, "away_ml": 128,
            "over_under": 8.5, "over_odds": -110, "under_odds": -110,
            "home_spread_point": -1.5, "home_spread_odds": 155,
            "away_spread_point":  1.5, "away_spread_odds": -180,
            # NBA adds Q1 markets:
            "q1_spread": 1.5, "q1_spread_home_odds": -110, ...
            # NHL adds puck line under home/away_spread_point (±1.5)
            # MLB adds run line under home/away_spread_point (±1.5)
        },
        ...
    }
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


# ── Candidate endpoints (probe order) ──────────────────────
#
# Listed most-likely first. ``{sport}`` is substituted per sport key.
# When one responds with 200 + parseable JSON we lock to it for the
# session (no point probing the next candidate every fetch).
#
# Sport codes used in the URL path -- again, GUESSED; Hard Rock may use
# "baseball", "mlb", "hockey", etc. The probe tries several.
_SPORT_KEYS = {
    "mlb": ["baseball-mlb", "mlb", "baseball"],
    "nhl": ["hockey-nhl", "nhl", "hockey"],
    "nba": ["basketball-nba", "nba", "basketball"],
}

_BASE_URL_TEMPLATES = [
    # Preferred (direct, no auth) app-scoped REST endpoints
    "https://app.hardrock.bet/api/sports-data/events/{sport}",
    "https://app.hardrock.bet/api/v1/sports/{sport}/events",
    "https://app.hardrock.bet/api/v1/events?sport={sport}",
    "https://app.hardrock.bet/api/events/{sport}",
    # Alternate hostnames the Hard Rock JS bundles have referenced
    "https://api.app.hardrock.bet/sports/{sport}/events",
    "https://api.app.hardrock.bet/v1/events/{sport}",
    "https://sbapi.app.hardrock.bet/sports/{sport}",
    # Cerberus GraphQL (probe body needed; handled separately)
    "https://cerberus-service.app.hardrock.bet/graphql",
]


# Browser-ish headers. Hard Rock checks Origin / Referer on some routes.
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://app.hardrock.bet",
    "Referer": "https://app.hardrock.bet/",
    "X-Requested-With": "XMLHttpRequest",
}


# Cache to stay polite even when we're only using this as a fallback.
# Separate cache per sport so one bad response doesn't poison others.
_cache: dict[str, tuple[float, dict]] = {}
CACHE_TTL = 600

# Which URL template worked for each sport, so we don't re-probe every
# minute after discovery. Populated at runtime by fetch_*.
_working_url: dict[str, str] = {}


# ── HTTP helpers ───────────────────────────────────────────

def _http_get(url: str, timeout: float = 10.0) -> tuple[int, bytes | None]:
    """Return (status, body-or-None). Never raises."""
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:
        logger.debug("HTTP GET failed %s: %s", url, e)
        return 0, None


def _http_post_json(url: str, payload: dict,
                    timeout: float = 10.0) -> tuple[int, bytes | None]:
    try:
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            url, data=body,
            headers={**_HEADERS, "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:
        logger.debug("HTTP POST failed %s: %s", url, e)
        return 0, None


# ── Team normalization ────────────────────────────────────
#
# Hard Rock is a US-licensed book so team names should match the full
# English names our other scrapers use. We share the maps defined in
# dk_odds / nba_dk_odds / nhl_dk_odds so normalization stays consistent
# across sources.

try:
    from .dk_odds import _normalize_team as _mlb_to_abbr  # type: ignore
except Exception:  # pragma: no cover - defensive during partial deploys
    _mlb_to_abbr = lambda n: n.strip()  # noqa: E731

try:
    from .nba_dk_odds import _NBA_NAME_TO_ABBR as _NBA_MAP  # type: ignore
except Exception:
    _NBA_MAP = {}


# NHL abbreviation map (Hard Rock uses full names; we key by our canonical abbrevs).
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


# ── Generic parser ────────────────────────────────────────
#
# Most sportsbook JSON responses expose a list of events, each with
# home/away participants and a list of markets, each with outcomes
# carrying American odds and (for spreads/totals) a line. We try the
# likely field names; anything unfound becomes None.


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


def _decimal_to_american(dec: float) -> int | None:
    """Convert decimal odds (e.g. 1.91) to American (-110)."""
    if dec is None or dec <= 1.0:
        return None
    if dec >= 2.0:
        return int(round((dec - 1) * 100))
    return int(round(-100 / (dec - 1)))


def _iter_events(data: Any) -> list[dict]:
    """Return a list of event-shaped dicts from an arbitrary response."""
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("events", "data", "items", "results", "eventList", "upcoming"):
        v = data.get(key)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v
        if isinstance(v, dict):
            # Nested {data: {events: [...]}} etc.
            nested = _iter_events(v)
            if nested:
                return nested
    return []


def _iter_markets(event: dict) -> list[dict]:
    for key in ("markets", "marketGroups", "offers", "bets"):
        v = event.get(key)
        if isinstance(v, list):
            out = []
            for item in v:
                if not isinstance(item, dict):
                    continue
                # Some APIs nest a further "markets" list inside a group
                inner = item.get("markets") if isinstance(item.get("markets"), list) else None
                if inner:
                    out.extend([m for m in inner if isinstance(m, dict)])
                else:
                    out.append(item)
            return out
    return []


def _market_kind(label: str) -> str | None:
    s = (label or "").lower()
    if "moneyline" in s or "money line" in s or s == "ml" or "winner" in s:
        return "ml"
    if "spread" in s or "puck line" in s or "run line" in s or "handicap" in s:
        return "spread"
    if "total" in s or "over/under" in s or "over under" in s or " ou " in s:
        return "total"
    return None


def _is_q1(label: str) -> bool:
    s = (label or "").lower()
    return any(h in s for h in (
        "1st quarter", "first quarter", "1q", " q1", "q1 ", "quarter 1",
    ))


def _extract_team_names(event: dict) -> tuple[str, str]:
    """Return (away_name, home_name) from an event dict, best-effort."""
    # Direct keys
    home = _pick(event, "homeTeam", "home", "homeTeamName")
    away = _pick(event, "awayTeam", "away", "awayTeamName")
    if isinstance(home, dict):
        home = _pick(home, "name", "displayName", "fullName", "label")
    if isinstance(away, dict):
        away = _pick(away, "name", "displayName", "fullName", "label")
    if home and away:
        return str(away), str(home)

    # Participants array pattern
    parts = _pick(event, "participants", "competitors")
    if isinstance(parts, list) and len(parts) >= 2:
        role_map = {}
        for p in parts:
            if not isinstance(p, dict):
                continue
            role = (_pick(p, "role", "homeAway", "side") or "").lower()
            name = _pick(p, "name", "displayName", "fullName", "label")
            if role and name:
                role_map[role] = str(name)
        if "home" in role_map and "away" in role_map:
            return role_map["away"], role_map["home"]
        # Fall back to order
        names = [str(_pick(p, "name", "displayName", "fullName", "label") or "")
                 for p in parts if isinstance(p, dict)]
        if len(names) >= 2:
            # Hard Rock convention (and most books): index 0 = home, 1 = away.
            # Without a role we can't be sure, so try the "away @ home"
            # text and split it.
            title = _pick(event, "name", "title", "displayName") or ""
            m = title.strip()
            if " @ " in m:
                a, h = m.split(" @ ", 1)
                return a.strip(), h.strip()
            if " at " in m.lower():
                low = m.lower()
                idx = low.find(" at ")
                return m[:idx].strip(), m[idx + 4:].strip()
            return names[1], names[0]
    # Give up
    return "", ""


def _extract_outcome_sides(sport: str, outcomes: list, away_abbr: str,
                            home_abbr: str) -> dict[str, dict]:
    """Map each outcome to {'home'|'away'|'over'|'under', payload}."""
    out: dict[str, dict] = {}
    for o in outcomes:
        if not isinstance(o, dict):
            continue
        label = (_pick(o, "label", "name", "participantName") or "").strip()
        side = (_pick(o, "side", "type", "outcomeType") or "").strip().lower()
        abbr = _team_abbr(sport, label) if label else ""

        price_am = _int_odds(_pick(o, "oddsAmerican", "americanOdds", "price"))
        if price_am is None:
            # Try decimal
            dec = _float_line(_pick(o, "decimalOdds", "oddsDecimal", "decimal"))
            if dec:
                price_am = _decimal_to_american(dec)

        line = _float_line(_pick(o, "line", "handicap", "points", "total", "spread"))

        data = {"price": price_am, "line": line, "label": label}

        if abbr == home_abbr:
            out["home"] = data
        elif abbr == away_abbr:
            out["away"] = data
        elif "over" in label.lower() or side == "over":
            out["over"] = data
        elif "under" in label.lower() or side == "under":
            out["under"] = data
    return out


def _apply_market(bucket: dict, kind: str, sides: dict, q1: bool) -> None:
    if kind == "ml":
        if q1:
            if (p := sides.get("home", {}).get("price")) is not None:
                bucket["q1_home_ml"] = p
            if (p := sides.get("away", {}).get("price")) is not None:
                bucket["q1_away_ml"] = p
        else:
            if (p := sides.get("home", {}).get("price")) is not None:
                bucket["home_ml"] = p
            if (p := sides.get("away", {}).get("price")) is not None:
                bucket["away_ml"] = p
    elif kind == "spread":
        if q1:
            if sides.get("home", {}).get("line") is not None:
                bucket["q1_spread"] = sides["home"]["line"]
            if (p := sides.get("home", {}).get("price")) is not None:
                bucket["q1_spread_home_odds"] = p
            if (p := sides.get("away", {}).get("price")) is not None:
                bucket["q1_spread_away_odds"] = p
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
        if (p := sides.get("over", {}).get("price")) is not None:
            bucket["q1_over_odds" if q1 else "over_odds"] = p
        if (p := sides.get("under", {}).get("price")) is not None:
            bucket["q1_under_odds" if q1 else "under_odds"] = p


def _parse_response(sport: str, data: Any) -> dict[str, dict]:
    """Parse a Hard Rock response into our canonical odds dict."""
    result: dict[str, dict] = {}
    events = _iter_events(data)
    if not events:
        return result

    for event in events:
        away_name, home_name = _extract_team_names(event)
        away_abbr = _team_abbr(sport, away_name)
        home_abbr = _team_abbr(sport, home_name)
        if not (away_abbr and home_abbr):
            continue
        key = f"{away_abbr}@{home_abbr}"
        bucket = result.setdefault(key, {"provider": "HardRock"})

        for market in _iter_markets(event):
            label = _pick(market, "name", "label", "marketName", "type") or ""
            kind = _market_kind(str(label))
            if kind is None:
                continue
            outcomes = (_pick(market, "outcomes", "selections", "runners") or [])
            if not isinstance(outcomes, list) or len(outcomes) < 2:
                continue
            sides = _extract_outcome_sides(sport, outcomes, away_abbr, home_abbr)
            q1 = _is_q1(str(label)) if sport == "nba" else False
            _apply_market(bucket, kind, sides, q1)

    # Drop entries that didn't collect any real pricing
    return {k: v for k, v in result.items()
            if any(x in v for x in (
                "home_ml", "away_ml", "over_under",
                "q1_spread", "q1_total", "q1_home_ml",
            ))}


# ── Probe + fetch ─────────────────────────────────────────

def _candidate_urls(sport: str) -> list[str]:
    out: list[str] = []
    for sport_key in _SPORT_KEYS.get(sport, [sport]):
        for tmpl in _BASE_URL_TEMPLATES:
            if "{sport}" in tmpl:
                out.append(tmpl.replace("{sport}", sport_key))
            elif tmpl not in out:
                out.append(tmpl)
    return out


def probe(sport: str = "mlb", verbose: bool = True) -> dict:
    """Try every candidate URL and report which ones respond.

    Returns:
        {
            "sport": "mlb",
            "attempts": [
                {"url": "...", "status": 200, "bytes": 12345,
                 "events_parsed": 3, "sample_keys": ["AWAY@HOME", ...]},
                ...
            ],
        }

    CLI: ``python -m scrapers.hardrock_odds --probe --sport nhl``
    """
    out = {"sport": sport, "attempts": []}
    for url in _candidate_urls(sport):
        if url.endswith("/graphql"):
            # Rough attempt at a GraphQL query shape. Real schema lives
            # inside the Hard Rock JS bundle -- this probe will 400
            # unless the shape matches; that's the signal to dig into
            # the site's actual query.
            query = {
                "query": "{ events(sport:\"%s\"){ id name home{name} away{name} "
                          "markets{ name outcomes{ label line oddsAmerican } } } }"
                          % sport,
            }
            status, body = _http_post_json(url, query)
        else:
            status, body = _http_get(url)

        rec: dict[str, Any] = {"url": url, "status": status}
        if body:
            rec["bytes"] = len(body)
            try:
                data = json.loads(body)
                events = _iter_events(data)
                rec["events_parsed"] = len(events)
                if events:
                    parsed = _parse_response(sport, data)
                    rec["matchups"] = len(parsed)
                    rec["sample_keys"] = list(parsed.keys())[:5]
                    if verbose:
                        # Dump first event for visibility
                        rec["first_event_keys"] = list(events[0].keys())[:20]
            except Exception as e:
                rec["parse_error"] = str(e)
                rec["head"] = body[:300].decode("utf-8", errors="replace")
        out["attempts"].append(rec)
        if verbose:
            logger.info("HardRock probe [%s] -> %s  bytes=%s  events=%s  matchups=%s",
                        url, status, rec.get("bytes"), rec.get("events_parsed"),
                        rec.get("matchups"))
    return out


def _fetch_one(sport: str) -> dict:
    """Try each candidate URL until one returns parseable odds."""
    # Fast path: reuse last working URL for this sport.
    urls = _candidate_urls(sport)
    if sport in _working_url:
        w = _working_url[sport]
        urls = [w] + [u for u in urls if u != w]

    for url in urls:
        if url.endswith("/graphql"):
            continue  # Needs real schema; skip in normal fetch flow.
        status, body = _http_get(url)
        if status != 200 or not body:
            continue
        try:
            data = json.loads(body)
        except Exception:
            continue
        parsed = _parse_response(sport, data)
        if parsed:
            if _working_url.get(sport) != url:
                logger.info("Hard Rock %s: locked onto %s (%d matchups)",
                            sport.upper(), url, len(parsed))
                _working_url[sport] = url
            return parsed
    return {}


def fetch_hardrock_odds(sport: str) -> dict:
    """Fetch Hard Rock odds for the given sport.

    Sport in {"mlb", "nhl", "nba"}. Returns dict keyed by "AWAY@HOME".
    Empty dict on any failure (all callers fall through to the next
    source in their chain -- this is a *fallback* path by design).
    """
    now = time.time()
    cached = _cache.get(sport)
    if cached and now - cached[0] < CACHE_TTL:
        return cached[1]
    try:
        result = _fetch_one(sport)
    except Exception as e:
        logger.warning("Hard Rock %s fetch crashed: %s", sport, e)
        result = {}
    _cache[sport] = (now, result)
    return result


# Convenience wrappers matching existing call sites.

def fetch_mlb() -> dict: return fetch_hardrock_odds("mlb")
def fetch_nhl() -> dict: return fetch_hardrock_odds("nhl")
def fetch_nba() -> dict: return fetch_hardrock_odds("nba")


# ── CLI ────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="mlb", choices=["mlb", "nhl", "nba"])
    ap.add_argument("--probe", action="store_true",
                    help="Try every candidate URL and print responses.")
    ap.add_argument("--url", help="Override: fetch a single URL and dump keys.")
    args = ap.parse_args()

    if args.url:
        status, body = _http_get(args.url)
        print(f"Status: {status}  Bytes: {len(body) if body else 0}")
        if body:
            try:
                data = json.loads(body)
                print("Top-level keys:", list(data.keys())[:20] if isinstance(data, dict) else type(data).__name__)
                print("\nFirst 1000 chars of JSON:")
                print(json.dumps(data, indent=2)[:1000])
                parsed = _parse_response(args.sport, data)
                print(f"\nParsed {len(parsed)} matchups:")
                for k, v in list(parsed.items())[:5]:
                    print(f"  {k}: {v}")
            except Exception as e:
                print(f"JSON parse failed: {e}")
                print("Body head:", body[:500].decode("utf-8", errors="replace"))
    elif args.probe:
        import pprint
        pprint.pprint(probe(args.sport))
    else:
        odds = fetch_hardrock_odds(args.sport)
        print(f"Got {len(odds)} Hard Rock {args.sport.upper()} matchups")
        for k, v in sorted(odds.items()):
            print(f"  {k}: {v}")
