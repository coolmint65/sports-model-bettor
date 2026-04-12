"""
DraftKings NBA odds scraper with Q1-market support.

Pulls full-game AND 1st-quarter markets (moneyline, spread, total)
straight from DraftKings' public sportsbook API. This is the fallback
when The Odds API plan tier doesn't expose h2h_q1/spreads_q1/totals_q1.

Endpoint: /api/v5/eventgroups/42648?format=json
Event Group 42648 = NBA

DraftKings organizes markets into categories (e.g. "Game Lines",
"Quarters") and subcategories. Q1 markets live under the "Quarters"
category (or sometimes "1st Quarter") with subcategories like
"1st Quarter - Moneyline", "1st Quarter - Point Spread", and
"1st Quarter - Total Points".

Returns dict keyed by "AWAY@HOME" with the same schema as
scrapers/nba_odds.py::fetch_nba_odds so downstream callers can drop it
in transparently.
"""

import json
import logging
import time
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

NBA_EVENT_GROUP = 42648

DK_URLS = [
    f"https://sportsbook-nash.draftkings.com//sites/US-SB/api/v5/eventgroups/{NBA_EVENT_GROUP}?format=json",
    f"https://sportsbook.draftkings.com//sites/US-SB/api/v5/eventgroups/{NBA_EVENT_GROUP}?format=json",
    f"https://sportsbook-us-nj.draftkings.com//sites/US-NJ-SB/api/v5/eventgroups/{NBA_EVENT_GROUP}?format=json",
    f"https://sportsbook-us-il.draftkings.com//sites/US-IL-SB/api/v5/eventgroups/{NBA_EVENT_GROUP}?format=json",
    f"https://sportsbook-us-co.draftkings.com//sites/US-CO-SB/api/v5/eventgroups/{NBA_EVENT_GROUP}?format=json",
    f"https://sportsbook-us-pa.draftkings.com//sites/US-PA-SB/api/v5/eventgroups/{NBA_EVENT_GROUP}?format=json",
]

# Cache for 10 minutes to stay polite to DK
_cache: dict | None = None
_cache_ts: float = 0
CACHE_TTL = 600


# ── HTTP ───────────────────────────────────────────────────

def _fetch_dk(url: str) -> dict | None:
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://sportsbook.draftkings.com/",
            "Origin": "https://sportsbook.draftkings.com",
        })
        with urllib.request.urlopen(req, timeout=12) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        logger.debug("DK fetch failed for %s: %s", url, e)
        return None


# ── Team normalization ────────────────────────────────────

# DK full team name -> our canonical abbreviation (must match nba_odds.py)
_NBA_NAME_TO_ABBR: dict[str, str] = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "LA Clippers": "LAC", "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM", "Miami Heat": "MIA", "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN", "New Orleans Pelicans": "NO",
    "New York Knicks": "NY", "Oklahoma City Thunder": "OKC", "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX", "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC", "San Antonio Spurs": "SA", "Toronto Raptors": "TOR",
    "Utah Jazz": "UTAH", "Washington Wizards": "WSH",
}


def _team_abbr(name: str) -> str:
    if not name:
        return ""
    return _NBA_NAME_TO_ABBR.get(name.strip(), name.strip())


# ── Parsing helpers ───────────────────────────────────────

def _int_odds(val: Any) -> int | None:
    """Coerce DK's American odds value (str or int) to int."""
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


def _match_key(away_abbr: str, home_abbr: str) -> str:
    return f"{away_abbr}@{home_abbr}"


def _event_teams(event: dict) -> tuple[str, str]:
    """Extract (away_abbr, home_abbr) from a DK event dict."""
    # DK events commonly have teamName1 / teamName2 (1 = away, 2 = home in their convention,
    # but NOT guaranteed — always check via eventMetadata participant role if present).
    away_name = event.get("teamName1") or event.get("awayTeamName") or ""
    home_name = event.get("teamName2") or event.get("homeTeamName") or ""

    # Participants array is more reliable when present
    participants = event.get("eventMetadata", {}).get("participantMetadata") or []
    for p in participants:
        role = (p.get("role") or "").lower()
        name = p.get("name") or ""
        if role == "home":
            home_name = name
        elif role == "away":
            away_name = name

    return _team_abbr(away_name), _team_abbr(home_name)


# ── Subcategory classification ────────────────────────────

# Matches any Q1/first-quarter-related label
_Q1_HINTS = ("1st quarter", "first quarter", "1q ", "q1 ", "- 1q", "- q1")


def _is_q1(label: str) -> bool:
    s = (label or "").lower()
    return any(h in s for h in _Q1_HINTS)


def _classify_market(label: str) -> str | None:
    """Return one of: 'ml', 'spread', 'total' or None."""
    s = (label or "").lower()
    if "moneyline" in s or "money line" in s:
        return "ml"
    if "spread" in s or "line" in s:  # "Point Spread", "Run Line"
        return "spread"
    if "total" in s or "over" in s:
        return "total"
    return None


# ── Parsers for a single offer ────────────────────────────

def _apply_ml(entry: dict, outcomes: list, q1: bool,
              event_map: dict[int, tuple[str, str]]) -> None:
    """Outcomes are two sides: home/away moneyline."""
    event_id = entry.get("eventId")
    if event_id not in event_map:
        return
    away_abbr, home_abbr = event_map[event_id]
    key = _match_key(away_abbr, home_abbr)

    for o in outcomes:
        part_name = o.get("participant") or o.get("label") or ""
        abbr = _team_abbr(part_name)
        price = _int_odds(o.get("oddsAmerican") or o.get("odds"))
        if price is None:
            continue

        slot_home = "q1_home_ml" if q1 else "home_ml"
        slot_away = "q1_away_ml" if q1 else "away_ml"

        bucket = _bucket(key)
        if abbr == home_abbr:
            bucket[slot_home] = price
        elif abbr == away_abbr:
            bucket[slot_away] = price


def _apply_spread(entry: dict, outcomes: list, q1: bool,
                  event_map: dict[int, tuple[str, str]]) -> None:
    event_id = entry.get("eventId")
    if event_id not in event_map:
        return
    away_abbr, home_abbr = event_map[event_id]
    key = _match_key(away_abbr, home_abbr)

    for o in outcomes:
        part_name = o.get("participant") or o.get("label") or ""
        abbr = _team_abbr(part_name)
        price = _int_odds(o.get("oddsAmerican") or o.get("odds"))
        line = _float_line(o.get("line") or o.get("handicap"))
        if price is None or line is None:
            continue

        bucket = _bucket(key)
        if q1:
            if abbr == home_abbr:
                bucket["q1_spread"] = line
                bucket["q1_spread_home_odds"] = price
            elif abbr == away_abbr:
                bucket["q1_spread_away_odds"] = price
        else:
            if abbr == home_abbr:
                bucket["home_spread_point"] = line
                bucket["home_spread_odds"] = price
            elif abbr == away_abbr:
                bucket["away_spread_point"] = line
                bucket["away_spread_odds"] = price


def _apply_total(entry: dict, outcomes: list, q1: bool,
                 event_map: dict[int, tuple[str, str]]) -> None:
    event_id = entry.get("eventId")
    if event_id not in event_map:
        return
    away_abbr, home_abbr = event_map[event_id]
    key = _match_key(away_abbr, home_abbr)
    bucket = _bucket(key)

    for o in outcomes:
        label = (o.get("label") or "").lower()
        price = _int_odds(o.get("oddsAmerican") or o.get("odds"))
        line = _float_line(o.get("line") or o.get("handicap"))
        if price is None:
            continue

        if line is not None:
            bucket["q1_total" if q1 else "over_under"] = line

        if "over" in label:
            bucket["q1_over_odds" if q1 else "over_odds"] = price
        elif "under" in label:
            bucket["q1_under_odds" if q1 else "under_odds"] = price


# Holds the current result dict while parsing
_current_result: dict[str, dict] | None = None


def _bucket(key: str) -> dict:
    global _current_result
    assert _current_result is not None
    if key not in _current_result:
        _current_result[key] = {"provider": "DraftKings"}
    return _current_result[key]


# ── Main entry point ──────────────────────────────────────

def fetch_nba_dk_odds() -> dict:
    """Fetch NBA odds (full-game + Q1 markets) straight from DraftKings.

    Returns dict keyed by "AWAY@HOME" with the same schema as
    scrapers/nba_odds.py::fetch_nba_odds. Empty dict on failure.
    """
    global _cache, _cache_ts, _current_result

    if _cache and time.time() - _cache_ts < CACHE_TTL:
        return _cache

    data = None
    for url in DK_URLS:
        data = _fetch_dk(url)
        if data:
            logger.info("DraftKings NBA: connected via %s",
                        url.split("//")[1].split("/")[0])
            break

    if not data:
        logger.warning("DraftKings NBA: all endpoints returned empty/403")
        return {}

    _current_result = {}

    try:
        event_group = data.get("eventGroup", {}) or {}

        # Build event_id -> (away_abbr, home_abbr) map
        event_map: dict[int, tuple[str, str]] = {}
        for event in event_group.get("events", []) or []:
            eid = event.get("eventId") or event.get("providerEventId")
            if not eid:
                continue
            try:
                eid = int(eid)
            except (ValueError, TypeError):
                continue
            away, home = _event_teams(event)
            if away and home:
                event_map[eid] = (away, home)

        if not event_map:
            logger.warning("DraftKings NBA: no events found in response")
            return {}

        # Walk all categories/subcategories and parse matching market types
        offer_cats = event_group.get("offerCategories", []) or []
        subcat_names_logged: set[str] = set()

        for cat in offer_cats:
            cat_name = (cat.get("name") or "").strip()
            for subcat in cat.get("offerSubcategoryDescriptors", []) or []:
                sub_name = (subcat.get("name") or "").strip()
                if sub_name not in subcat_names_logged:
                    subcat_names_logged.add(sub_name)
                is_q1 = _is_q1(sub_name) or _is_q1(cat_name)
                market_kind = _classify_market(sub_name)
                if market_kind is None:
                    continue

                offers = (subcat.get("offerSubcategory", {}) or {}).get("offers", []) or []
                for offer_group in offers:
                    if not isinstance(offer_group, list):
                        # Some responses nest differently
                        offer_group = [offer_group]
                    for offer in offer_group:
                        if not isinstance(offer, dict):
                            continue
                        outcomes = offer.get("outcomes") or []
                        if len(outcomes) < 2:
                            continue

                        # Offers may also carry their own label — prefer it
                        # over the subcat name for classification accuracy
                        offer_label = (offer.get("label") or "").strip()
                        effective_q1 = is_q1 or _is_q1(offer_label)
                        effective_kind = _classify_market(offer_label) or market_kind

                        if effective_kind == "ml":
                            _apply_ml(offer, outcomes, effective_q1, event_map)
                        elif effective_kind == "spread":
                            _apply_spread(offer, outcomes, effective_q1, event_map)
                        elif effective_kind == "total":
                            _apply_total(offer, outcomes, effective_q1, event_map)

        result = {k: v for k, v in _current_result.items()
                  if v.get("home_ml") or v.get("q1_home_ml")
                  or v.get("q1_spread") is not None
                  or v.get("q1_total") is not None}

        q1_count = sum(1 for v in result.values()
                       if v.get("q1_spread") is not None
                       or v.get("q1_total") is not None
                       or v.get("q1_home_ml") is not None)
        logger.info("DraftKings NBA: parsed %d games (%d with Q1 markets) "
                    "from subcategories: %s",
                    len(result), q1_count, sorted(subcat_names_logged))

    except Exception as e:
        logger.error("Error parsing DraftKings NBA odds: %s", e, exc_info=True)
        result = {}
    finally:
        _current_result = None

    _cache = result
    _cache_ts = time.time()
    return result


# ── CLI ───────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    odds = fetch_nba_dk_odds()
    print(f"\n{'=' * 60}")
    print(f"  DraftKings NBA Odds ({len(odds)} games)")
    print(f"{'=' * 60}")
    for key, v in sorted(odds.items()):
        print(f"\n  {key}")
        for k in ("home_ml", "away_ml", "over_under",
                  "q1_home_ml", "q1_away_ml",
                  "q1_spread", "q1_spread_home_odds", "q1_spread_away_odds",
                  "q1_total", "q1_over_odds", "q1_under_odds"):
            if k in v:
                print(f"    {k:25s} {v[k]}")
