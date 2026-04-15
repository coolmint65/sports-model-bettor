"""
The Odds API integration for MLB odds.

Paid tier ($30/mo = 20K credits) supports per-event markets.
Returns full-game ML/O/U/RL plus NRFI and F5 markets from DraftKings
(first inning total, F5 ML, F5 O/U, F5 RL).

Sign up at: https://the-odds-api.com/
Set your API key in data/odds_api_key.txt or as environment variable ODDS_API_KEY.
"""

import json
import logging
import os
import urllib.request
from pathlib import Path

import time

logger = logging.getLogger(__name__)

API_BASE = "https://api.the-odds-api.com/v4"
KEY_FILE = Path(__file__).resolve().parent.parent / "data" / "odds_api_key.txt"

MLB_SPORT = "baseball_mlb"
PREFERRED_BOOK = "draftkings"

# Full-game pricing now uses a trimmed-mean consensus across the same
# four books we fall back through for per-event markets. DK still
# anchors the point lines (over/under and run line) since that's where
# the user actually bets; only the price (juice) is consensus-averaged.
# This dampens single-book stale-line skew without making the displayed
# point spreads inconsistent with the user's sportsbook.
CONSENSUS_BOOKS = ["draftkings", "fanduel", "betmgm", "bovada"]

# Per-event inning-specific markets (paid tier only; 1 credit per market per event)
PER_EVENT_MARKETS = [
    "totals_1st_1_innings",   # NRFI / YRFI
    "h2h_1st_5_innings",      # F5 ML
    "totals_1st_5_innings",   # F5 O/U
    "spreads_1st_5_innings",  # F5 RL
]

# Bookmaker preference for per-event markets. DK posts these lines very late
# (only hours before first pitch), while FanDuel / BetMGM / Bovada post them
# early. Fall through the list so we always get a price when one is available.
PER_EVENT_BOOK_PREFERENCE = ["draftkings", "fanduel", "betmgm", "bovada"]

# Cache odds for 10 minutes to avoid burning API credits
_odds_cache: dict | None = None
_odds_cache_time: float = 0
ODDS_CACHE_TTL = 600  # 10 minutes

# Last-known credit balance from the x-requests-remaining response header.
# Populated on every successful bulk fetch; surfaced via /health so we can
# alarm before we hit the monthly cap.
_last_credits_remaining: int | None = None
_last_credits_at: float = 0


def get_credits_status() -> dict:
    """Return the most recent Odds API credit balance + age in seconds."""
    return {
        "remaining": _last_credits_remaining,
        "age_seconds": int(time.time() - _last_credits_at) if _last_credits_at else None,
    }


def _get_api_key() -> str | None:
    """Load API key from file or environment."""
    # Try environment variable first
    key = os.environ.get("ODDS_API_KEY")
    if key:
        return key.strip()

    # Try key file
    if KEY_FILE.exists():
        return KEY_FILE.read_text().strip()

    return None


def fetch_odds(include_per_event: bool = True) -> dict:
    """
    Fetch MLB odds from The Odds API.

    Args:
        include_per_event: If True (default), also fetch NRFI / F5 markets
            via per-event endpoint. Paid tier required.

    Returns dict keyed by normalized matchup:
    {
        "MIN@KC": {
            "home_ml": -168, "away_ml": 139,
            "over_under": 10.5, "over_odds": -103, "under_odds": -117,
            "home_spread_odds": -130, "away_spread_odds": 110,
            "spread": -1.5,
            # Per-event markets (when include_per_event=True):
            "nrfi_line": 0.5, "nrfi_over_odds": 115, "nrfi_under_odds": -140,
            "f5_home_ml": -135, "f5_away_ml": 115,
            "f5_total": 5.0, "f5_over_odds": -110, "f5_under_odds": -110,
            "f5_spread": -0.5, "f5_home_spread_odds": -115,
            "f5_away_spread_odds": -105,
            "provider": "DraftKings"
        }
    }
    """
    global _odds_cache, _odds_cache_time

    # Return cached odds if fresh
    if _odds_cache and (time.time() - _odds_cache_time) < ODDS_CACHE_TTL:
        logger.debug("Odds API: returning cached odds (%d games)", len(_odds_cache))
        return _odds_cache

    api_key = _get_api_key()
    if not api_key:
        logger.info("No Odds API key found. Set ODDS_API_KEY env var or create data/odds_api_key.txt")
        return {}

    # Fetch full-game markets across all CONSENSUS_BOOKS so we can
    # trim-mean the prices. Costs 1 credit per market regardless of how
    # many books we request.
    url = (f"{API_BASE}/sports/{MLB_SPORT}/odds/"
           f"?apiKey={api_key}"
           f"&regions=us"
           f"&markets=h2h,spreads,totals"
           f"&oddsFormat=american"
           f"&bookmakers={','.join(CONSENSUS_BOOKS)}")

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "MLBPredictionEngine/1.0",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            # Log + remember remaining requests so /health can show them
            remaining = resp.headers.get("x-requests-remaining", "?")
            logger.info("Odds API: %s requests remaining this month", remaining)
            global _last_credits_remaining, _last_credits_at
            try:
                _last_credits_remaining = int(remaining)
                _last_credits_at = time.time()
            except (TypeError, ValueError):
                pass
    except Exception as e:
        logger.warning("Odds API failed: %s", e)
        return {}

    if not data or not isinstance(data, list):
        return {}

    odds_map = {}
    # matchup_key -> odds-api event_id (used by per-event fetch)
    event_ids: dict[str, str] = {}

    for game in data:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        event_id = game.get("id", "")

        h_abbr = _team_abbr(home)
        a_abbr = _team_abbr(away)
        key = f"{a_abbr}@{h_abbr}"

        bookmakers = game.get("bookmakers", [])
        if not bookmakers:
            continue

        # Consensus aggregation: trim-mean across CONSENSUS_BOOKS.
        # Point lines anchor to DK (where the user bets); only prices
        # get consensus-averaged.
        result = _consensus_aggregate(home, away, bookmakers)
        if result.get("home_ml"):
            odds_map[key] = result
            if event_id:
                event_ids[key] = event_id

    # Per-event enrichment (NRFI + F5 markets)
    if include_per_event and event_ids:
        per_event = _fetch_per_event_markets(event_ids, api_key)
        for key, markets in per_event.items():
            if key in odds_map:
                odds_map[key].update(markets)

    logger.info("Odds API: fetched odds for %d games", len(odds_map))

    # Cache the merged results
    _odds_cache = odds_map
    _odds_cache_time = time.time()

    return odds_map


def _trim_mean(prices: list[int | float]) -> int | None:
    """Symmetric trimmed mean: drop highest and lowest, average the rest.

    Falls back gracefully on small samples:
      0 prices -> None
      1 price  -> that price
      2 prices -> mean of both (no trim possible)
      3+ prices -> drop high + low, mean the rest
    Returns an int (American-odds prices are integers).
    """
    if not prices:
        return None
    if len(prices) == 1:
        return int(prices[0])
    if len(prices) == 2:
        return int(round((prices[0] + prices[1]) / 2))
    s = sorted(prices)
    trimmed = s[1:-1]
    return int(round(sum(trimmed) / len(trimmed)))


def _consensus_aggregate(home: str, away: str, bookmakers: list) -> dict:
    """Build a single odds dict from multiple books via trimmed-mean prices.

    Anchors point lines (over_under, run-line spread) to DK so the
    displayed line matches the book the user is presumably betting at.
    Falls back to the first book that has a line if DK isn't present.
    Trim-means the price at the anchored line across books that offer
    the same line (so we never average prices on different totals).
    """
    by_key = {b.get("key"): b for b in bookmakers if b.get("key")}
    dk = by_key.get(PREFERRED_BOOK)

    # Per-book extracted markets, keyed by bookmaker key.
    extracted: dict[str, dict] = {}
    for bk_key, bk in by_key.items():
        extracted[bk_key] = _extract_book_markets(bk, home, away)

    # ── ML (no point line, straight trim-mean) ──
    home_mls = [v["home_ml"] for v in extracted.values() if v.get("home_ml")]
    away_mls = [v["away_ml"] for v in extracted.values() if v.get("away_ml")]

    # ── Totals: anchor line to DK (or first book), then trim-mean prices
    # at that line. Skip prices for books on a different line so we
    # never average -110 at 8.5 with -108 at 9. ──
    anchor_total = None
    if dk and extracted.get(PREFERRED_BOOK, {}).get("over_under") is not None:
        anchor_total = extracted[PREFERRED_BOOK]["over_under"]
    else:
        for v in extracted.values():
            if v.get("over_under") is not None:
                anchor_total = v["over_under"]
                break

    over_prices, under_prices = [], []
    if anchor_total is not None:
        for v in extracted.values():
            if v.get("over_under") == anchor_total:
                if v.get("over_odds") is not None:
                    over_prices.append(v["over_odds"])
                if v.get("under_odds") is not None:
                    under_prices.append(v["under_odds"])

    # ── Spreads (run line). MLB run line is essentially always +/-1.5;
    # anchor to DK and trim-mean prices at that magnitude. ──
    anchor_home_spread = None
    if dk and extracted.get(PREFERRED_BOOK, {}).get("home_spread_point") is not None:
        anchor_home_spread = extracted[PREFERRED_BOOK]["home_spread_point"]
    else:
        for v in extracted.values():
            if v.get("home_spread_point") is not None:
                anchor_home_spread = v["home_spread_point"]
                break

    home_sp_prices, away_sp_prices = [], []
    if anchor_home_spread is not None:
        for v in extracted.values():
            if v.get("home_spread_point") == anchor_home_spread:
                if v.get("home_spread_odds") is not None:
                    home_sp_prices.append(v["home_spread_odds"])
                if v.get("away_spread_odds") is not None:
                    away_sp_prices.append(v["away_spread_odds"])

    # Provider label tracks what actually contributed (so the UI / health
    # endpoint shows the real consensus, not a hardcoded "DraftKings").
    contributing = sorted({k for k, v in extracted.items() if v.get("home_ml")})
    provider = "Consensus (" + "/".join(
        _SHORT_BOOK_NAMES.get(k, k) for k in contributing
    ) + ")" if len(contributing) > 1 else (
        _BOOK_TITLES.get(contributing[0], contributing[0]) if contributing else "Unknown"
    )

    result = {
        "provider": provider,
        "consensus_books": contributing,
        "home_ml": _trim_mean(home_mls),
        "away_ml": _trim_mean(away_mls),
        "over_under": anchor_total,
        "over_odds": _trim_mean(over_prices),
        "under_odds": _trim_mean(under_prices),
        "spread": 1.5,  # back-compat
        "home_spread_point": anchor_home_spread,
        "away_spread_point": (-anchor_home_spread) if anchor_home_spread is not None else None,
        "home_spread_odds": _trim_mean(home_sp_prices),
        "away_spread_odds": _trim_mean(away_sp_prices),
    }
    return result


def _extract_book_markets(book: dict, home: str, away: str) -> dict:
    """Pull the standard h2h / spreads / totals fields out of one book's payload."""
    out: dict = {}
    for market in book.get("markets", []):
        mkey = market.get("key", "")
        outcomes = market.get("outcomes", [])
        if mkey == "h2h":
            for o in outcomes:
                name = o.get("name", "")
                price = o.get("price")
                if name == home and price is not None:
                    out["home_ml"] = price
                elif name == away and price is not None:
                    out["away_ml"] = price
        elif mkey == "spreads":
            for o in outcomes:
                name = o.get("name", "")
                price = o.get("price")
                point = o.get("point")
                if name == home:
                    if price is not None:
                        out["home_spread_odds"] = price
                    if point is not None:
                        out["home_spread_point"] = point
                elif name == away:
                    if price is not None:
                        out["away_spread_odds"] = price
                    if point is not None:
                        out["away_spread_point"] = point
        elif mkey == "totals":
            for o in outcomes:
                name = o.get("name", "").lower()
                price = o.get("price")
                point = o.get("point")
                if "over" in name:
                    if price is not None:
                        out["over_odds"] = price
                    if point is not None:
                        out["over_under"] = point
                elif "under" in name:
                    if price is not None:
                        out["under_odds"] = price
    return out


_BOOK_TITLES = {
    "draftkings": "DraftKings",
    "fanduel":    "FanDuel",
    "betmgm":     "BetMGM",
    "bovada":     "Bovada",
}
_SHORT_BOOK_NAMES = {
    "draftkings": "DK",
    "fanduel":    "FD",
    "betmgm":     "MGM",
    "bovada":     "Bov",
}


def _fetch_per_event_markets(event_ids: dict[str, str], api_key: str) -> dict:
    """Fetch NRFI + F5 markets for each event. Paid tier required.

    Args:
        event_ids: matchup_key -> odds-api event_id
        api_key: Odds API key

    Returns:
        matchup_key -> dict of per-event market fields (nrfi_*, f5_*)
    """
    markets_param = ",".join(PER_EVENT_MARKETS)
    books_param = ",".join(PER_EVENT_BOOK_PREFERENCE)
    out: dict = {}

    for key, event_id in event_ids.items():
        # Pull all preferred books in one call and let the parser pick the
        # first that has data. DK posts F5/NRFI lines only in the hours
        # before first pitch; FD/MGM/Bovada post them much earlier.
        url = (f"{API_BASE}/sports/{MLB_SPORT}/events/{event_id}/odds"
               f"?apiKey={api_key}"
               f"&regions=us"
               f"&markets={markets_param}"
               f"&oddsFormat=american"
               f"&bookmakers={books_param}")
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "MLBPredictionEngine/1.0",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                event = json.loads(resp.read().decode())
        except Exception as e:
            logger.warning("Odds API per-event failed for %s (%s): %s", key, event_id, e)
            continue

        parsed = _parse_per_event(event)
        if parsed:
            out[key] = parsed

    if out:
        logger.info("Odds API: fetched per-event markets for %d games", len(out))
    return out


def _parse_per_event(event: dict) -> dict:
    """Parse per-event response into flat field dict for NRFI + F5 markets.

    Iterates bookmakers in the configured preference order and uses the
    first one that returns any of the requested markets. When DK hasn't
    posted F5/NRFI lines yet, falls through to FanDuel / BetMGM / Bovada.
    """
    home = event.get("home_team", "")
    away = event.get("away_team", "")
    bookmakers = event.get("bookmakers", [])
    if not bookmakers:
        return {}

    by_key = {b.get("key"): b for b in bookmakers}
    book = None
    for pref in PER_EVENT_BOOK_PREFERENCE:
        if pref in by_key and by_key[pref].get("markets"):
            book = by_key[pref]
            break
    if book is None:
        # No preferred book -- fall back to the first bookmaker with markets
        for b in bookmakers:
            if b.get("markets"):
                book = b
                break
    if book is None:
        return {}

    result: dict = {"per_event_provider": book.get("title") or book.get("key")}

    for market in book.get("markets", []):
        mkey = market.get("key", "")
        outcomes = market.get("outcomes", [])

        if mkey == "totals_1st_1_innings":  # NRFI/YRFI
            for o in outcomes:
                name = o.get("name", "").lower()
                price = o.get("price")
                point = o.get("point")
                if "over" in name:
                    result["nrfi_over_odds"] = price
                    result["nrfi_line"] = point
                elif "under" in name:
                    result["nrfi_under_odds"] = price

        elif mkey == "h2h_1st_5_innings":  # F5 ML
            for o in outcomes:
                name = o.get("name", "")
                price = o.get("price")
                if name == home:
                    result["f5_home_ml"] = price
                elif name == away:
                    result["f5_away_ml"] = price

        elif mkey == "totals_1st_5_innings":  # F5 O/U
            for o in outcomes:
                name = o.get("name", "").lower()
                price = o.get("price")
                point = o.get("point")
                if "over" in name:
                    result["f5_over_odds"] = price
                    result["f5_total"] = point
                elif "under" in name:
                    result["f5_under_odds"] = price

        elif mkey == "spreads_1st_5_innings":  # F5 RL
            for o in outcomes:
                name = o.get("name", "")
                price = o.get("price")
                point = o.get("point")
                if name == home:
                    result["f5_home_spread_odds"] = price
                    result["f5_home_spread_point"] = point
                elif name == away:
                    result["f5_away_spread_odds"] = price
                    result["f5_away_spread_point"] = point
            # Convenience: canonical F5 spread is the home-side magnitude
            if "f5_home_spread_point" in result:
                result["f5_spread"] = result["f5_home_spread_point"]

    return result


# Team name to abbreviation mapping
_TEAM_MAP = {
    "Arizona Diamondbacks": "AZ", "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL", "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL", "Detroit Tigers": "DET",
    "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA", "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN", "New York Mets": "NYM",
    "New York Yankees": "NYY", "Oakland Athletics": "ATH",
    "Philadelphia Phillies": "PHI", "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD", "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB", "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR", "Washington Nationals": "WSH",
}


def _team_abbr(name: str) -> str:
    return _TEAM_MAP.get(name, name)
