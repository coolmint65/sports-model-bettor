"""
Odds scraper for NHL games — primary/fallback architecture.

PRIMARY: The Odds API (Hard Rock Bet via us2 region + generic US
bookmakers: FanDuel, BetMGM, Caesars, DraftKings, PointsBet, etc.)
provides clean, validated pricing. Requires ODDS_API_KEY env var.

FALLBACK: Direct sportsbook scrapers (DraftKings, FanDuel, Kambi,
Bovada) are only activated when The Odds API returns no data — e.g.
API key expired, rate-limited, or service outage.

This avoids unnecessary scraper load and API credit waste during
normal operation while keeping resilience for edge cases.
"""

import asyncio
import logging
import time as _time_mod
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.constants import SCRAPER_HEADERS
from app.models.game import Game
from app.models.team import Team
from app.scrapers.http_helpers import make_request as _make_request
from app.scrapers.team_map import NHL_TEAM_MAP, NHL_ABBREVIATIONS, resolve_team
from app.services.odds import american_to_implied as _svc_american_to_implied

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# O/U line plausibility range.
# Pregame NHL totals are 4.5-8.5, but live totals climb as goals are scored.
# Upper bound of 15.0 covers any realistic in-progress scenario.
# ---------------------------------------------------------------------------
_OU_LINE_MIN = 4.0
_OU_LINE_MAX = 15.0

# ---------------------------------------------------------------------------
# Team-name normalisation — delegates to the canonical team_map module.
# Re-exports for backward compatibility with code that imports from here.
# ---------------------------------------------------------------------------

_COMMON_TEAM_MAP = NHL_TEAM_MAP
_ABBREV_SET = NHL_ABBREVIATIONS


def _map_team(name: str) -> str:
    """Resolve a team name to its 3-letter abbreviation."""
    return resolve_team(name)


# ---------------------------------------------------------------------------
# Odds conversion helpers
# ---------------------------------------------------------------------------

def decimal_to_american(decimal_odds: float) -> float:
    """Convert decimal odds (e.g. 1.91) to American (e.g. -110)."""
    if decimal_odds <= 1.0:
        return 0.0
    if decimal_odds >= 2.0:
        return round((decimal_odds - 1) * 100, 0)
    else:
        return round(-100 / (decimal_odds - 1), 0)



def _normalize_spread_line(val: float) -> float:
    """Normalize a spread value to the nearest .5 increment.

    NHL sportsbooks always use .5 lines (±1.5, ±2.5, etc.) but some
    scraped sources return whole numbers (±1, ±2).  Snap to .5.
    Also enforces a minimum of ±1.5 since puck lines below that
    (e.g. ±0.5) don't exist in standard NHL betting.
    """
    if val == 0:
        return val
    if val % 1 == 0.5 or val % 1 == -0.5:
        sign = -1 if val < 0 else 1
        # Enforce minimum ±1.5
        if abs(val) < 1.5:
            return sign * 1.5
        return val
    sign = -1 if val < 0 else 1
    result = sign * (round(abs(val) * 2) / 2)
    if result % 1 == 0:
        result += sign * 0.5
    # Enforce minimum ±1.5
    if abs(result) < 1.5:
        result = sign * 1.5
    return result


# ---------------------------------------------------------------------------
# Standardised odds event structure
# ---------------------------------------------------------------------------

class OddsEvent:
    """Normalised odds for a single game from a single source."""

    __slots__ = (
        "source", "home_team", "away_team", "home_abbr", "away_abbr",
        "commence_time", "home_ml", "away_ml",
        "home_spread", "away_spread", "home_spread_price", "away_spread_price",
        "total_line", "over_price", "under_price",
        "alt_totals", "alt_spreads",
        # 1st period odds
        "p1_home_ml", "p1_away_ml", "p1_draw_price",
        "p1_spread_line", "p1_home_spread_price", "p1_away_spread_price",
        "p1_total_line", "p1_over_price", "p1_under_price",
        # Regulation winner (3-way moneyline)
        "reg_home_price", "reg_away_price", "reg_draw_price",
        # Both Teams to Score (BTTS)
        "btts_yes_price", "btts_no_price",
    )

    def __init__(
        self,
        source: str,
        home_team: str,
        away_team: str,
        commence_time: Optional[str] = None,
        home_ml: float = 0.0,
        away_ml: float = 0.0,
        home_spread: float = 0.0,
        away_spread: float = 0.0,
        home_spread_price: float = -110.0,
        away_spread_price: float = -110.0,
        total_line: float = 0.0,
        over_price: float = -110.0,
        under_price: float = -110.0,
        alt_totals: Optional[List] = None,
        alt_spreads: Optional[List] = None,
    ):
        self.source = source
        self.home_team = home_team
        self.away_team = away_team
        self.home_abbr = _map_team(home_team)
        self.away_abbr = _map_team(away_team)
        self.commence_time = commence_time
        self.home_ml = home_ml
        self.away_ml = away_ml
        self.home_spread = home_spread
        self.away_spread = away_spread
        self.home_spread_price = home_spread_price
        self.away_spread_price = away_spread_price
        self.total_line = total_line
        self.over_price = over_price
        self.under_price = under_price
        # alt_totals: list of {"line": float, "over_price": float, "under_price": float}
        self.alt_totals = alt_totals or []
        # alt_spreads: list of {"line": float, "home_price": float, "away_price": float}
        self.alt_spreads = alt_spreads or []
        # 1st period odds (set after construction if available)
        self.p1_home_ml: float = 0.0
        self.p1_away_ml: float = 0.0
        self.p1_draw_price: float = 0.0
        self.p1_spread_line: float = 0.0
        self.p1_home_spread_price: float = 0.0
        self.p1_away_spread_price: float = 0.0
        self.p1_total_line: float = 0.0
        self.p1_over_price: float = 0.0
        self.p1_under_price: float = 0.0
        # Regulation winner (3-way moneyline)
        self.reg_home_price: float = 0.0
        self.reg_away_price: float = 0.0
        self.reg_draw_price: float = 0.0
        # Both Teams to Score (BTTS)
        self.btts_yes_price: float = 0.0
        self.btts_no_price: float = 0.0

    def has_moneyline(self) -> bool:
        return self.home_ml != 0 and self.away_ml != 0

    def has_spread(self) -> bool:
        return self.home_spread != 0 or self.away_spread != 0

    def has_total(self) -> bool:
        return self.total_line > 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "home_abbr": self.home_abbr,
            "away_abbr": self.away_abbr,
            "commence_time": self.commence_time,
            "home_ml": self.home_ml,
            "away_ml": self.away_ml,
            "home_spread": self.home_spread,
            "away_spread": self.away_spread,
            "home_spread_price": self.home_spread_price,
            "away_spread_price": self.away_spread_price,
            "total_line": self.total_line,
            "over_price": self.over_price,
            "under_price": self.under_price,
            "alt_totals": self.alt_totals,
            "alt_spreads": self.alt_spreads,
            "reg_home_price": self.reg_home_price,
            "reg_away_price": self.reg_away_price,
            "reg_draw_price": self.reg_draw_price,
            "btts_yes_price": self.btts_yes_price,
            "btts_no_price": self.btts_no_price,
        }


def _validate_event(event: OddsEvent) -> OddsEvent:
    """Apply source-level validation to an OddsEvent before merge.

    Validates moneyline, total, and spread data using the
    ``odds_validation`` module.  Invalid entries are zeroed out
    or removed from alt lines so that only clean data enters the
    merge pipeline.
    """
    from app.scrapers.odds_validation import (
        is_valid_american_odds,
        validate_odds_event_totals,
        validate_odds_event_spreads,
        validate_odds_event_primary_spread,
    )

    matchup = f"{event.away_abbr}@{event.home_abbr}"

    # Validate moneyline — just check individual odds are valid American
    if event.has_moneyline():
        if not is_valid_american_odds(event.home_ml) or not is_valid_american_odds(event.away_ml):
            logger.warning(
                "[%s] %s: invalid ML odds H=%s A=%s — zeroing out",
                event.source, matchup, event.home_ml, event.away_ml,
            )
            event.home_ml = 0
            event.away_ml = 0

    # Validate totals (primary + alt)
    cleaned_alts, pline, pover, punder = validate_odds_event_totals(
        event.alt_totals,
        event.total_line, event.over_price, event.under_price,
        event.source, matchup,
    )
    event.alt_totals = cleaned_alts
    event.total_line = pline
    event.over_price = pover
    event.under_price = punder

    # Validate primary spread prices (catches moneyline contamination)
    hs, as_, hp, ap = validate_odds_event_primary_spread(
        event.home_spread, event.away_spread,
        event.home_spread_price, event.away_spread_price,
        event.source, matchup,
    )
    event.home_spread = hs
    event.away_spread = as_
    event.home_spread_price = hp
    event.away_spread_price = ap

    # Validate alt spreads
    event.alt_spreads = validate_odds_event_spreads(
        event.alt_spreads, event.source, matchup,
    )

    # Validate 1st period spread — only ±0.5 is a valid hockey 1st
    # period puck line.  Lines of ±1.0, ±1.5, ±2.0, etc. are either
    # alternate lines or data errors and should not be used.
    return event


# ---------------------------------------------------------------------------
# Source-specific fetchers
# ---------------------------------------------------------------------------

# _make_request is imported from app.scrapers.http_helpers above.


# ---- DraftKings ----

async def _fetch_draftkings(client: httpx.AsyncClient) -> List[OddsEvent]:
    """
    Fetch NHL odds from the DraftKings sportsbook public API.

    DraftKings exposes a JSON API that their website uses.  The NHL
    event group ID is 42133.  We request the main offer categories
    which include moneyline, spread, and totals.
    """
    events: List[OddsEvent] = []
    headers = {
        **SCRAPER_HEADERS,
        "Referer": "https://sportsbook.draftkings.com/",
        "Origin": "https://sportsbook.draftkings.com",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Ch-Ua": '"Chromium";v="134", "Google Chrome";v="134", "Not:A-Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
    }

    # DraftKings NHL event group ID: 42133
    # Try multiple endpoint variants (DK changes these periodically)
    # Include Illinois and Nashville subdomains which have been
    # more reliable for non-US IPs in some reports.
    urls = [
        "https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/42133?format=json",
        "https://sportsbook-us-il.draftkings.com/sites/US-IL-SB/api/v5/eventgroups/42133?format=json",
        "https://sportsbook-nash.draftkings.com/sites/US-SB/api/v5/eventgroups/42133?format=json",
        "https://sportsbook-us-nj.draftkings.com/sites/US-NJ-SB/api/v5/eventgroups/42133?format=json",
    ]
    data = None
    for url in urls:
        # Use matching Referer/Origin for state-specific subdomains
        domain = url.split("/sites/")[0] if "/sites/" in url else "https://sportsbook.draftkings.com"
        headers["Referer"] = domain + "/"
        headers["Origin"] = domain
        data = await _make_request(client, url, headers=headers)
        if data:
            logger.info("DraftKings: connected via %s", url.split("/sites/")[1].split("/")[0] if "/sites/" in url else url)
            break

    if not data:
        logger.warning("DraftKings: all %d endpoints failed — no data", len(urls))
        return events

    try:
        # Navigate the DK response structure
        event_groups = data if isinstance(data, dict) else {}
        offers = event_groups.get("eventGroup", {}).get("offerCategories", [])
        if not offers:
            # Try alternate structure
            offers = event_groups.get("offerCategories", [])

        dk_events = event_groups.get("eventGroup", {}).get("events", [])
        if not dk_events:
            dk_events = event_groups.get("events", [])

        # Build event map for team names
        event_map: Dict[int, Dict[str, str]] = {}
        if isinstance(dk_events, list):
            for ev in dk_events:
                eid = ev.get("eventId")
                if eid:
                    home = ""
                    away = ""
                    team_short1 = ev.get("teamShortName1", "")
                    team_short2 = ev.get("teamShortName2", "")
                    name = ev.get("name", "")

                    # Try to extract from event name "Team1 @ Team2" or "Team1 vs Team2"
                    if " @ " in name:
                        parts = name.split(" @ ")
                        away = parts[0].strip()
                        home = parts[1].strip()
                    elif " vs " in name:
                        parts = name.split(" vs ")
                        home = parts[0].strip()
                        away = parts[1].strip()
                    elif " at " in name.lower():
                        parts = name.lower().split(" at ")
                        # Reconstruct with proper case from original
                        idx = name.lower().index(" at ")
                        away = name[:idx].strip()
                        home = name[idx + 4:].strip()

                    # Use teamShortName fields as fallback when name
                    # parsing fails or mapping doesn't resolve.
                    if not _map_team(home) and team_short1:
                        home = team_short1
                    if not _map_team(away) and team_short2:
                        away = team_short2

                    if not home or not away:
                        logger.debug(
                            "DraftKings: could not extract teams from event %s "
                            "(name=%r, short1=%r, short2=%r)",
                            eid, name, team_short1, team_short2,
                        )
                        continue

                    # Skip non-NHL events early — if neither team maps to
                    # an NHL abbreviation, this is likely NCAAB or another
                    # sport that leaked into the response.
                    if not _map_team(home) or not _map_team(away):
                        continue

                    start_time = ev.get("startDate", "")
                    event_map[eid] = {
                        "home": home, "away": away, "start": start_time
                    }

        # Parse offer categories for odds
        game_odds: Dict[int, Dict[str, Any]] = {}

        for cat in offers if isinstance(offers, list) else []:
            cat_name = (cat.get("name") or "").lower()
            sub_cats = cat.get("offerSubcategoryDescriptors", [])
            if not isinstance(sub_cats, list):
                continue

            for sub in sub_cats:
                sub_name = (sub.get("name") or "").lower()
                offer_list = sub.get("offerSubcategory", {}).get("offers", [])
                if not isinstance(offer_list, list):
                    continue

                # Determine if this is a period-specific subcategory
                _PERIOD_TOKENS = ("1st", "2nd", "3rd", "period", "half", "quarter", "inning")
                is_period_sub = any(tok in sub_name for tok in _PERIOD_TOKENS)
                is_1st_period = "1st" in sub_name

                for offer_row in offer_list:
                    if not isinstance(offer_row, list):
                        offer_row = [offer_row]
                    for offer in offer_row:
                        eid = offer.get("eventId")
                        if not eid:
                            continue
                        if eid not in game_odds:
                            game_odds[eid] = {}

                        outcomes = offer.get("outcomes", [])
                        if not isinstance(outcomes, list):
                            continue

                        label = (offer.get("label") or "").lower()
                        is_period_label = any(tok in label for tok in _PERIOD_TOKENS)

                        # Skip remaining period markets (2nd, 3rd, etc.)
                        if is_period_sub or is_period_label:
                            continue

                        # Moneyline
                        if "moneyline" in label or "money line" in label or "game" in sub_name and "line" not in label and "total" not in label:
                            if len(outcomes) >= 2:
                                for oc in outcomes:
                                    odds_am = oc.get("oddsAmerican", "")
                                    try:
                                        odds_val = float(str(odds_am).replace("+", ""))
                                    except (ValueError, TypeError):
                                        continue
                                    oc_label = (oc.get("label") or "").strip()
                                    # Determine home/away
                                    mapped = _map_team(oc_label)
                                    ev_info = event_map.get(eid, {})
                                    home_mapped = _map_team(ev_info.get("home", ""))
                                    if mapped and mapped == home_mapped:
                                        game_odds[eid]["home_ml"] = odds_val
                                    elif mapped:
                                        game_odds[eid]["away_ml"] = odds_val
                                    elif oc.get("type") == "home" or oc_label == ev_info.get("home"):
                                        game_odds[eid]["home_ml"] = odds_val
                                    else:
                                        game_odds[eid]["away_ml"] = odds_val

                        # Spread / Puck Line — collect ALL available lines.
                        # Only use the standard 1.5 puck line for the
                        # primary spread fields; alternate lines go into
                        # dk_alt_spreads only.
                        elif "spread" in label or "puck" in label or "handicap" in label:
                            spread_lines: Dict[float, Dict[str, float]] = {}
                            for oc in outcomes:
                                line = oc.get("line", 0)
                                odds_am = oc.get("oddsAmerican", "")
                                try:
                                    odds_val = float(str(odds_am).replace("+", ""))
                                    line_val = float(line)
                                except (ValueError, TypeError):
                                    continue
                                oc_label = (oc.get("label") or "").strip()
                                mapped = _map_team(oc_label)
                                ev_info = event_map.get(eid, {})
                                home_mapped = _map_team(ev_info.get("home", ""))
                                abs_line = abs(line_val)
                                if abs_line not in spread_lines:
                                    spread_lines[abs_line] = {}
                                if mapped and mapped == home_mapped:
                                    # Only set primary spread from the
                                    # standard 1.5 puck line (or the first
                                    # line if no 1.5 seen yet) to prevent
                                    # alternate lines from overwriting.
                                    if abs_line == 1.5 or "home_spread" not in game_odds[eid]:
                                        game_odds[eid]["home_spread"] = line_val
                                        game_odds[eid]["home_spread_price"] = odds_val
                                    spread_lines[abs_line]["home_spread"] = line_val
                                    spread_lines[abs_line]["home_price"] = odds_val
                                elif mapped:
                                    if abs_line == 1.5 or "away_spread" not in game_odds[eid]:
                                        game_odds[eid]["away_spread"] = line_val
                                        game_odds[eid]["away_spread_price"] = odds_val
                                    spread_lines[abs_line]["away_spread"] = line_val
                                    spread_lines[abs_line]["away_price"] = odds_val

                            for abs_lv, prices in spread_lines.items():
                                if "home_price" in prices and "away_price" in prices:
                                    if "dk_alt_spreads" not in game_odds[eid]:
                                        game_odds[eid]["dk_alt_spreads"] = []
                                    game_odds[eid]["dk_alt_spreads"].append({
                                        "line": abs_lv,
                                        "home_spread": prices.get("home_spread", -abs_lv),
                                        "away_spread": prices.get("away_spread", abs_lv),
                                        "home_price": prices["home_price"],
                                        "away_price": prices["away_price"],
                                    })

                        # Totals — collect ALL available lines
                        elif "total" in label or "over" in label:
                            # Collect over/under prices per line value
                            offer_lines: Dict[float, Dict[str, float]] = {}
                            for oc in outcomes:
                                line = oc.get("line", 0)
                                odds_am = oc.get("oddsAmerican", "")
                                try:
                                    odds_val = float(str(odds_am).replace("+", ""))
                                    line_val = float(line)
                                except (ValueError, TypeError):
                                    continue
                                if line_val not in offer_lines:
                                    offer_lines[line_val] = {}
                                oc_label = (oc.get("label") or "").lower()
                                if "over" in oc_label:
                                    offer_lines[line_val]["over_price"] = odds_val
                                elif "under" in oc_label:
                                    offer_lines[line_val]["under_price"] = odds_val

                            # Store all lines as alt_totals
                            for lv, prices in offer_lines.items():
                                if "over_price" in prices and "under_price" in prices:
                                    if "dk_alt_totals" not in game_odds[eid]:
                                        game_odds[eid]["dk_alt_totals"] = []
                                    game_odds[eid]["dk_alt_totals"].append({
                                        "line": lv,
                                        "over_price": prices["over_price"],
                                        "under_price": prices["under_price"],
                                    })

                            # Keep backward compat: primary total_line
                            # is the one closest to the "main" market line
                            if offer_lines:
                                # Pick the line with the tightest juice
                                # (closest to -110/-110) as the primary
                                best_line = None
                                best_juice = float("inf")
                                for lv, prices in offer_lines.items():
                                    op = prices.get("over_price", -110)
                                    up = prices.get("under_price", -110)
                                    # Tightest juice = both sides closest
                                    # to -110
                                    juice = abs(op - (-110)) + abs(up - (-110))
                                    if juice < best_juice:
                                        best_juice = juice
                                        best_line = lv
                                if best_line is not None and best_line in offer_lines:
                                    game_odds[eid]["total_line"] = best_line
                                    game_odds[eid]["over_price"] = offer_lines[best_line].get("over_price", -110)
                                    game_odds[eid]["under_price"] = offer_lines[best_line].get("under_price", -110)

        # Build OddsEvent objects
        for eid, odds in game_odds.items():
            ev_info = event_map.get(eid, {})
            home = ev_info.get("home", "")
            away = ev_info.get("away", "")
            if not home or not away:
                logger.warning(
                    "DraftKings: dropping event %s — no team names (home=%r, away=%r)",
                    eid, home, away,
                )
                continue

            event = OddsEvent(
                source="draftkings",
                home_team=home,
                away_team=away,
                commence_time=ev_info.get("start", ""),
                home_ml=odds.get("home_ml", 0),
                away_ml=odds.get("away_ml", 0),
                home_spread=odds.get("home_spread", 0),
                away_spread=odds.get("away_spread", 0),
                home_spread_price=odds.get("home_spread_price", -110),
                away_spread_price=odds.get("away_spread_price", -110),
                total_line=odds.get("total_line", 0),
                over_price=odds.get("over_price", -110),
                under_price=odds.get("under_price", -110),
                alt_totals=odds.get("dk_alt_totals", []),
                alt_spreads=odds.get("dk_alt_spreads", []),
            )

            if event.home_abbr and event.away_abbr:
                events.append(_validate_event(event))
            else:
                logger.warning(
                    "DraftKings: dropping event -- unmapped teams "
                    "(home=%r->%r, away=%r->%r)",
                    home, event.home_abbr, away, event.away_abbr,
                )

    except Exception as exc:
        logger.warning("DraftKings parse error: %s", exc)

    logger.info(
        "DraftKings: fetched odds for %d events (from %d raw events)",
        len(events), len(event_map),
    )
    return events


# ---- FanDuel helpers ----


def _parse_fd_odds(runner: dict) -> Optional[float]:
    """Extract American odds from a FanDuel runner dict, or *None*."""
    win_running = runner.get("winRunnerOdds", {})
    american_odds_str = (
        win_running.get("americanDisplayOdds", {}).get("americanOdds", "")
        or win_running.get("americanOdds", "")
    )
    decimal_odds = win_running.get("trueOdds", {}).get("decimalOdds", {}).get("decimalOdds", 0)
    try:
        if american_odds_str:
            return float(str(american_odds_str).replace("+", ""))
        elif decimal_odds:
            return decimal_to_american(float(decimal_odds))
        return None
    except (ValueError, TypeError):
        return None


def _parse_fd_yes_no_runners(
    runners: Any,
    eid: str,
    game_odds: dict,
    yes_key: str,
    no_key: str,
) -> None:
    """Parse yes/no FanDuel runners (BTTS, OT) into *game_odds*."""
    for runner in runners if isinstance(runners, list) else []:
        runner_name = (runner.get("runnerName", "") or "").lower()
        odds_val = _parse_fd_odds(runner)
        if odds_val is None:
            continue
        if "yes" in runner_name:
            game_odds[eid][yes_key] = odds_val
        elif "no" in runner_name:
            game_odds[eid][no_key] = odds_val


# ---- FanDuel ----

async def _fetch_fanduel(client: httpx.AsyncClient) -> List[OddsEvent]:
    """
    Fetch NHL odds from the FanDuel sportsbook public API.

    FanDuel uses a content-managed page API. NHL event type ID = 7524.
    """
    events: List[OddsEvent] = []
    headers = {
        **SCRAPER_HEADERS,
        "Sec-Ch-Ua": '"Chromium";v="134", "Google Chrome";v="134", "Not:A-Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
    }

    # FanDuel NHL page - try multiple states and page types
    params_variants = [
        {
            "page": "CUSTOM",
            "customPageId": "nhl",
            "_ak": "FhMFpcPWXMeyZxOx",
            "betexRegion": "GBR",
            "capiJurisdiction": "intl",
            "currencyCode": "USD",
            "exchangeLocale": "en_US",
            "language": "en",
            "regionCode": "NAMERICA",
        },
        {
            "page": "SPORT",
            "eventTypeId": "7524",
            "_ak": "FhMFpcPWXMeyZxOx",
            "timezone": "America/New_York",
        },
    ]

    data = None
    for state in ["il", "nj", "mi", "pa", "co", "ny"]:
        for params in params_variants:
            url = f"https://sbapi.{state}.sportsbook.fanduel.com/api/content-managed-page"
            data = await _make_request(client, url, headers=headers, params=params)
            if data and data.get("attachments"):
                break
        if data and data.get("attachments"):
            break

    if not data:
        logger.info("FanDuel: no data returned")
        return events

    try:
        attachments = data.get("attachments", {})
        fd_events = attachments.get("events", {})
        markets = attachments.get("markets", {})

        # Build event-to-teams map
        event_info: Dict[str, Dict[str, Any]] = {}
        for eid, ev in fd_events.items() if isinstance(fd_events, dict) else []:
            runners_info = ev.get("runners", [])
            name = ev.get("name", "")
            open_date = ev.get("openDate", "")
            home = ""
            away = ""

            # FanDuel names: "Team1 @ Team2" or "Team1 v Team2"
            if " @ " in name:
                parts = name.split(" @ ")
                away = parts[0].strip()
                home = parts[1].strip()
            elif " v " in name:
                parts = name.split(" v ")
                home = parts[0].strip()
                away = parts[1].strip()

            # Skip non-NHL events early (e.g. NCAAB leaking through)
            if not home or not away or not _map_team(home) or not _map_team(away):
                continue

            event_info[eid] = {"home": home, "away": away, "start": open_date}

        # Parse markets
        game_odds: Dict[str, Dict[str, Any]] = {}

        for mid, market in markets.items() if isinstance(markets, dict) else []:
            eid = str(market.get("eventId", ""))
            if not eid or eid not in event_info:
                continue

            if eid not in game_odds:
                game_odds[eid] = {}

            market_type = (market.get("marketType", "") or "").upper()
            market_name = (market.get("marketName", "") or market.get("name", "") or "").lower()
            runners = market.get("runners", [])

            # Detect period-specific markets.  Allow 1st period through
            # (we generate predictions for P1 ML, total, spread, BTTS) but
            # skip 2nd/3rd periods and non-hockey scopes.
            _FD_SKIP_TOKENS = ("2nd", "3rd", "half", "quarter", "inning")
            if any(tok in market_name for tok in _FD_SKIP_TOKENS):
                continue
            if any(tok in market_type for tok in ("HALF", "QUARTER", "INNING")):
                continue

            is_period1 = (
                "1st" in market_name
                or "FIRST_PERIOD" in market_type
                or "1ST_PERIOD" in market_type
            )


            # Moneyline
            if market_type in ("MATCH_BETTING", "MONEY_LINE", "HEAD_TO_HEAD", "MATCH_ODDS"):
                ev_info = event_info[eid]
                for runner in runners if isinstance(runners, list) else []:
                    odds_val = _parse_fd_odds(runner)
                    if odds_val is None:
                        continue
                    runner_name = runner.get("runnerName", "")
                    mapped = _map_team(runner_name)
                    home_mapped = _map_team(ev_info["home"])
                    if mapped and mapped == home_mapped:
                        game_odds[eid]["home_ml"] = odds_val
                    elif mapped:
                        game_odds[eid]["away_ml"] = odds_val

            # Spread / Puck Line — collect ALL available lines.
            # Only use the standard 1.5 puck line for the primary
            # spread fields; alternate lines go into fd_alt_spreads.
            elif market_type in ("MATCH_HANDICAP", "SPREAD", "PUCK_LINE", "HANDICAP", "ASIAN_HANDICAP"):
                ev_info = event_info[eid]
                fd_spread_lines: Dict[float, Dict[str, float]] = {}
                for runner in runners if isinstance(runners, list) else []:
                    odds_val = _parse_fd_odds(runner)
                    if odds_val is None:
                        continue
                    try:
                        line_val = float(runner.get("handicap", 0))
                    except (ValueError, TypeError):
                        continue
                    runner_name = runner.get("runnerName", "")
                    mapped = _map_team(runner_name)
                    home_mapped = _map_team(ev_info["home"])
                    abs_line = abs(line_val)
                    if abs_line not in fd_spread_lines:
                        fd_spread_lines[abs_line] = {}
                    if mapped and mapped == home_mapped:
                        if abs_line == 1.5 or "home_spread" not in game_odds[eid]:
                            game_odds[eid]["home_spread"] = line_val
                            game_odds[eid]["home_spread_price"] = odds_val
                        fd_spread_lines[abs_line]["home_spread"] = line_val
                        fd_spread_lines[abs_line]["home_price"] = odds_val
                    elif mapped:
                        if abs_line == 1.5 or "away_spread" not in game_odds[eid]:
                            game_odds[eid]["away_spread"] = line_val
                            game_odds[eid]["away_spread_price"] = odds_val
                        fd_spread_lines[abs_line]["away_spread"] = line_val
                        fd_spread_lines[abs_line]["away_price"] = odds_val

                for abs_lv, prices in fd_spread_lines.items():
                    if "home_price" in prices and "away_price" in prices:
                        if "fd_alt_spreads" not in game_odds[eid]:
                            game_odds[eid]["fd_alt_spreads"] = []
                        game_odds[eid]["fd_alt_spreads"].append({
                            "line": abs_lv,
                            "home_spread": prices.get("home_spread", -abs_lv),
                            "away_spread": prices.get("away_spread", abs_lv),
                            "home_price": prices["home_price"],
                            "away_price": prices["away_price"],
                        })

            # Totals — collect ALL available lines
            elif market_type in ("TOTAL_GOALS", "MATCH_TOTAL", "TOTAL_POINTS", "OVER_UNDER", "TOTAL"):
                fd_lines: Dict[float, Dict[str, float]] = {}
                for runner in runners if isinstance(runners, list) else []:
                    odds_val = _parse_fd_odds(runner)
                    if odds_val is None:
                        continue
                    try:
                        line_val = float(runner.get("handicap", 0))
                    except (ValueError, TypeError):
                        continue
                    runner_name = (runner.get("runnerName", "") or "").lower()
                    if line_val not in fd_lines:
                        fd_lines[line_val] = {}
                    if "over" in runner_name:
                        fd_lines[line_val]["over_price"] = odds_val
                    elif "under" in runner_name:
                        fd_lines[line_val]["under_price"] = odds_val

                # Store all lines as alt_totals
                for lv, prices in fd_lines.items():
                    if "over_price" in prices and "under_price" in prices:
                        if "fd_alt_totals" not in game_odds[eid]:
                            game_odds[eid]["fd_alt_totals"] = []
                        game_odds[eid]["fd_alt_totals"].append({
                            "line": lv,
                            "over_price": prices["over_price"],
                            "under_price": prices["under_price"],
                        })

                # Keep backward compat: primary line = tightest juice
                if fd_lines:
                    best_line = None
                    best_juice = float("inf")
                    for lv, prices in fd_lines.items():
                        op = prices.get("over_price", -110)
                        up = prices.get("under_price", -110)
                        juice = abs(op - (-110)) + abs(up - (-110))
                        if juice < best_juice:
                            best_juice = juice
                            best_line = lv
                    if best_line is not None and best_line in fd_lines:
                        game_odds[eid]["total_line"] = best_line
                        game_odds[eid]["over_price"] = fd_lines[best_line].get("over_price", -110)
                        game_odds[eid]["under_price"] = fd_lines[best_line].get("under_price", -110)

            # Log unmatched market types at DEBUG for diagnostics
            else:
                logger.debug(
                    "FanDuel: unmatched market type=%r name=%r for event %s",
                    market_type, market_name, eid,
                )

        # Build OddsEvent objects
        for eid, odds in game_odds.items():
            ev_info = event_info.get(eid, {})
            home = ev_info.get("home", "")
            away = ev_info.get("away", "")
            if not home or not away:
                logger.warning(
                    "FanDuel: dropping event %s — no team names", eid,
                )
                continue

            event = OddsEvent(
                source="fanduel",
                home_team=home,
                away_team=away,
                commence_time=ev_info.get("start", ""),
                home_ml=odds.get("home_ml", 0),
                away_ml=odds.get("away_ml", 0),
                home_spread=odds.get("home_spread", 0),
                away_spread=odds.get("away_spread", 0),
                home_spread_price=odds.get("home_spread_price", -110),
                away_spread_price=odds.get("away_spread_price", -110),
                total_line=odds.get("total_line", 0),
                over_price=odds.get("over_price", -110),
                under_price=odds.get("under_price", -110),
                alt_totals=odds.get("fd_alt_totals", []),
                alt_spreads=odds.get("fd_alt_spreads", []),
            )
            if event.home_abbr and event.away_abbr:
                events.append(_validate_event(event))
            else:
                logger.warning(
                    "FanDuel: dropping event -- unmapped teams "
                    "(home=%r->%r, away=%r->%r)",
                    home, event.home_abbr, away, event.away_abbr,
                )

    except Exception as exc:
        logger.warning("FanDuel parse error: %s", exc)

    logger.info(
        "FanDuel: fetched odds for %d events (from %d raw events)",
        len(events), len(event_info),
    )
    return events


# ---- Kambi CDN (powers BetRivers, Unibet, 888sport) ----

async def _fetch_kambi(client: httpx.AsyncClient) -> List[OddsEvent]:
    """
    Fetch NHL odds from the Kambi offering API (CDN).

    Kambi powers BetRivers, Unibet, 888sport, and other sportsbooks.
    The offering API is publicly accessible and returns odds in decimal format.
    """
    events: List[OddsEvent] = []
    headers = SCRAPER_HEADERS

    # Kambi offering API — try one operator+path combo at a time with
    # a delay between attempts to avoid 429 rate-limiting from the CDN.
    # The triple-nested loop previously hammered 12 URLs in rapid
    # succession; now we flatten and throttle.
    _kambi_combos = [
        # Most reliable first
        ("https://eu-offering-api.kambicdn.com",
         "/offering/v2018/rsiuspa/listView/ice_hockey/nhl/all/all/matches.json",
         "BetRivers PA"),
        ("https://eu-offering-api.kambicdn.com",
         "/offering/v2018/rsiuspa/listView/ice_hockey/nhl.json",
         "BetRivers PA (alt)"),
        ("https://eu-offering-api.kambicdn.com",
         "/offering/v2018/ub/listView/ice_hockey/nhl/all/all/matches.json",
         "Unibet"),
        ("https://eu-offering.kambicdn.org",
         "/offering/v2018/rsiuspa/listView/ice_hockey/nhl/all/all/matches.json",
         "BetRivers PA (mirror)"),
    ]
    params = {
        "lang": "en_US",
        "market": "US",
        "client_id": "2",
        "channel_id": "1",
        "useCombined": "true",
        "includeParticipants": "true",
    }

    data = None
    for i, (host, path, label) in enumerate(_kambi_combos):
        if i > 0:
            await asyncio.sleep(1.0)  # rate-limit between attempts
        url = host + path
        data = await _make_request(client, url, headers=headers, params=params)
        if data and data.get("events"):
            logger.info("Kambi: got data via %s", label)
            break

    if not data:
        logger.info("Kambi: no data returned from any operator")
        return events

    try:
        kambi_events = data.get("events", [])

        for ev in kambi_events if isinstance(kambi_events, list) else []:
            ev_data = ev.get("event", {})
            home_name = ev_data.get("homeName", "")
            away_name = ev_data.get("awayName", "")
            start = ev_data.get("start", "")

            if not home_name or not away_name:
                continue

            bet_offers = ev.get("betOffers", [])
            odds: Dict[str, Any] = {}

            for offer in bet_offers if isinstance(bet_offers, list) else []:
                criterion = offer.get("criterion", {})
                criterion_label = (criterion.get("label") or "").lower()
                offer_type = offer.get("betOfferType", {}).get("name", "").lower()
                outcomes = offer.get("outcomes", [])

                # Detect period-specific markets early so they don't
                # get caught by the generic ML/spread/BTTS checks.
                _is_p1 = "1st" in criterion_label or "period 1" in criterion_label
                _is_period = _is_p1 or any(
                    tok in criterion_label for tok in ("2nd", "3rd", "period 2", "period 3")
                )

                # --- Full-game markets ---

                # Moneyline (Match Winner / 1X2 without draw for hockey)
                if "winner" in criterion_label or "match" in criterion_label or offer_type == "match":
                    for oc in outcomes if isinstance(outcomes, list) else []:
                        oc_label = oc.get("label", "")
                        oc_type = (oc.get("type", "") or "").upper()
                        decimal_odds_val = oc.get("odds", 0)
                        if not decimal_odds_val:
                            continue
                        # Kambi odds are in milliBet format (e.g., 1910 = 1.91)
                        dec = decimal_odds_val / 1000.0 if decimal_odds_val > 100 else decimal_odds_val
                        american = decimal_to_american(dec)

                        if oc_type == "OT_ONE" or oc_label == home_name:
                            odds["home_ml"] = american
                        elif oc_type == "OT_TWO" or oc_label == away_name:
                            odds["away_ml"] = american

                # Spread / Handicap — collect ALL available lines
                elif "handicap" in criterion_label or "spread" in criterion_label or "puck" in criterion_label:
                    kambi_spread_lines: Dict[float, Dict[str, float]] = {}
                    for oc in outcomes if isinstance(outcomes, list) else []:
                        oc_label = oc.get("label", "")
                        line = oc.get("line", 0)
                        decimal_odds_val = oc.get("odds", 0)
                        if not decimal_odds_val:
                            continue
                        dec = decimal_odds_val / 1000.0 if decimal_odds_val > 100 else decimal_odds_val
                        american = decimal_to_american(dec)
                        try:
                            line_val = float(line) / 1000.0 if abs(line) > 100 else float(line)
                        except (ValueError, TypeError):
                            continue

                        abs_line = abs(line_val)
                        if abs_line not in kambi_spread_lines:
                            kambi_spread_lines[abs_line] = {}
                        if oc_label == home_name:
                            odds["home_spread"] = line_val
                            odds["home_spread_price"] = american
                            kambi_spread_lines[abs_line]["home_spread"] = line_val
                            kambi_spread_lines[abs_line]["home_price"] = american
                        elif oc_label == away_name:
                            odds["away_spread"] = line_val
                            odds["away_spread_price"] = american
                            kambi_spread_lines[abs_line]["away_spread"] = line_val
                            kambi_spread_lines[abs_line]["away_price"] = american

                    for abs_lv, prices in kambi_spread_lines.items():
                        if "home_price" in prices and "away_price" in prices:
                            if "kambi_alt_spreads" not in odds:
                                odds["kambi_alt_spreads"] = []
                            odds["kambi_alt_spreads"].append({
                                "line": abs_lv,
                                "home_spread": prices.get("home_spread", -abs_lv),
                                "away_spread": prices.get("away_spread", abs_lv),
                                "home_price": prices["home_price"],
                                "away_price": prices["away_price"],
                            })

                # Totals (Over/Under) — collect ALL available lines
                # Skip period-specific totals (already handled above)
                elif ("total" in criterion_label or "over" in criterion_label) and not any(
                    tok in criterion_label for tok in ("1st", "2nd", "3rd", "period", "half", "quarter", "inning")
                ):
                    kambi_lines: Dict[float, Dict[str, float]] = {}
                    for oc in outcomes if isinstance(outcomes, list) else []:
                        oc_label = (oc.get("label", "") or "").lower()
                        oc_type = (oc.get("type", "") or "").upper()
                        line = oc.get("line", 0)
                        decimal_odds_val = oc.get("odds", 0)
                        if not decimal_odds_val:
                            continue
                        dec = decimal_odds_val / 1000.0 if decimal_odds_val > 100 else decimal_odds_val
                        american = decimal_to_american(dec)
                        try:
                            line_val = float(line) / 1000.0 if abs(line) > 100 else float(line)
                        except (ValueError, TypeError):
                            continue

                        if line_val not in kambi_lines:
                            kambi_lines[line_val] = {}
                        if "over" in oc_label or oc_type == "OT_OVER":
                            kambi_lines[line_val]["over_price"] = american
                        elif "under" in oc_label or oc_type == "OT_UNDER":
                            kambi_lines[line_val]["under_price"] = american

                    for lv, prices in kambi_lines.items():
                        if "over_price" in prices and "under_price" in prices:
                            if "kambi_alt_totals" not in odds:
                                odds["kambi_alt_totals"] = []
                            odds["kambi_alt_totals"].append({
                                "line": lv,
                                "over_price": prices["over_price"],
                                "under_price": prices["under_price"],
                            })

                    if kambi_lines:
                        best_line = None
                        best_juice = float("inf")
                        for lv, prices in kambi_lines.items():
                            op = prices.get("over_price", -110)
                            up = prices.get("under_price", -110)
                            juice = abs(op - (-110)) + abs(up - (-110))
                            if juice < best_juice:
                                best_juice = juice
                                best_line = lv
                        if best_line is not None and best_line in kambi_lines:
                            odds["total_line"] = best_line
                            odds["over_price"] = kambi_lines[best_line].get("over_price", -110)
                            odds["under_price"] = kambi_lines[best_line].get("under_price", -110)

            event = OddsEvent(
                source="kambi",
                home_team=home_name,
                away_team=away_name,
                commence_time=start,
                home_ml=odds.get("home_ml", 0),
                away_ml=odds.get("away_ml", 0),
                home_spread=odds.get("home_spread", 0),
                away_spread=odds.get("away_spread", 0),
                home_spread_price=odds.get("home_spread_price", -110),
                away_spread_price=odds.get("away_spread_price", -110),
                total_line=odds.get("total_line", 0),
                over_price=odds.get("over_price", -110),
                under_price=odds.get("under_price", -110),
                alt_totals=odds.get("kambi_alt_totals", []),
                alt_spreads=odds.get("kambi_alt_spreads", []),
            )
            if event.home_abbr and event.away_abbr:
                events.append(_validate_event(event))
            else:
                logger.warning(
                    "Kambi: dropping event -- unmapped teams "
                    "(home=%r->%r, away=%r->%r)",
                    home_name, event.home_abbr, away_name, event.away_abbr,
                )

    except Exception as exc:
        logger.warning("Kambi parse error: %s", exc)

    logger.info("Kambi: fetched odds for %d events", len(events))
    return events


# ---- Bovada ----

async def _fetch_bovada(client: httpx.AsyncClient) -> List[OddsEvent]:
    """
    Fetch NHL odds from Bovada's public coupon API.

    Bovada returns all odds formats (American, decimal, fractional, etc.)
    in a clean, well-structured response.  No API key required.
    """
    events: List[OddsEvent] = []
    headers = SCRAPER_HEADERS

    url = "https://www.bovada.lv/services/sports/event/coupon/events/A/description/hockey/nhl"
    data = await _make_request(client, url, headers=headers)

    if not data or not isinstance(data, list):
        logger.info("Bovada: no data returned")
        return events

    try:
        for section in data:
            section_events = section.get("events", [])
            if not isinstance(section_events, list):
                continue

            for ev in section_events:
                # Get team info from competitors list
                competitors = ev.get("competitors", [])
                home_name = ""
                away_name = ""
                for comp in competitors if isinstance(competitors, list) else []:
                    if comp.get("home"):
                        home_name = comp.get("name", "")
                    else:
                        away_name = comp.get("name", "")

                if not home_name or not away_name:
                    # Try parsing from description: "Away @ Home"
                    desc = ev.get("description", "")
                    if " @ " in desc:
                        parts = desc.split(" @ ")
                        away_name = parts[0].strip()
                        home_name = parts[1].strip()

                if not home_name or not away_name:
                    continue

                # Parse start time (epoch milliseconds)
                start_time = ev.get("startTime", 0)
                commence = ""
                if start_time:
                    try:
                        dt = datetime.fromtimestamp(start_time / 1000, tz=timezone.utc)
                        commence = dt.isoformat()
                    except (ValueError, TypeError, OverflowError):
                        pass

                odds: Dict[str, Any] = {}

                # Parse display groups -> markets
                for dg in ev.get("displayGroups", []):
                    if not isinstance(dg, dict):
                        continue
                    for market in dg.get("markets", []):
                        if not isinstance(market, dict):
                            continue

                        market_key = (market.get("key", "") or "").upper()
                        market_desc = (market.get("description", "") or "").lower()
                        outcomes = market.get("outcomes", [])
                        if not isinstance(outcomes, list):
                            continue

                        # Moneyline
                        if market_key == "2W-ML" or "moneyline" in market_desc:
                            for oc in outcomes:
                                price = oc.get("price", {})
                                if not isinstance(price, dict):
                                    continue
                                american_str = price.get("american", "")
                                try:
                                    odds_val = float(str(american_str).replace("+", ""))
                                except (ValueError, TypeError):
                                    continue
                                oc_type = (oc.get("type", "") or "").upper()
                                if oc_type == "H":
                                    odds["home_ml"] = odds_val
                                elif oc_type == "A":
                                    odds["away_ml"] = odds_val

                        # Spread / Puck Line — collect ALL available lines
                        elif market_key == "2W-SPRD" or "spread" in market_desc or "puck" in market_desc:
                            bov_spread_lines: Dict[float, Dict[str, float]] = {}
                            for oc in outcomes:
                                price = oc.get("price", {})
                                if not isinstance(price, dict):
                                    continue
                                american_str = price.get("american", "")
                                handicap_str = price.get("handicap", "0")
                                try:
                                    odds_val = float(str(american_str).replace("+", ""))
                                    line_val = float(handicap_str)
                                except (ValueError, TypeError):
                                    continue
                                abs_line = abs(line_val)
                                if abs_line not in bov_spread_lines:
                                    bov_spread_lines[abs_line] = {}
                                oc_type = (oc.get("type", "") or "").upper()
                                if oc_type == "H":
                                    odds["home_spread"] = line_val
                                    odds["home_spread_price"] = odds_val
                                    bov_spread_lines[abs_line]["home_spread"] = line_val
                                    bov_spread_lines[abs_line]["home_price"] = odds_val
                                elif oc_type == "A":
                                    odds["away_spread"] = line_val
                                    odds["away_spread_price"] = odds_val
                                    bov_spread_lines[abs_line]["away_spread"] = line_val
                                    bov_spread_lines[abs_line]["away_price"] = odds_val

                            for abs_lv, prices in bov_spread_lines.items():
                                if "home_price" in prices and "away_price" in prices:
                                    if "bov_alt_spreads" not in odds:
                                        odds["bov_alt_spreads"] = []
                                    odds["bov_alt_spreads"].append({
                                        "line": abs_lv,
                                        "home_spread": prices.get("home_spread", -abs_lv),
                                        "away_spread": prices.get("away_spread", abs_lv),
                                        "home_price": prices["home_price"],
                                        "away_price": prices["away_price"],
                                    })

                        # Totals — collect ALL available lines
                        elif market_key == "2W-OU" or "total" in market_desc:
                            bov_lines: Dict[float, Dict[str, float]] = {}
                            for oc in outcomes:
                                price = oc.get("price", {})
                                if not isinstance(price, dict):
                                    continue
                                american_str = price.get("american", "")
                                handicap_str = price.get("handicap", "0")
                                try:
                                    odds_val = float(str(american_str).replace("+", ""))
                                    line_val = float(handicap_str)
                                except (ValueError, TypeError):
                                    continue
                                if line_val not in bov_lines:
                                    bov_lines[line_val] = {}
                                oc_type = (oc.get("type", "") or "").upper()
                                if oc_type == "O":
                                    bov_lines[line_val]["over_price"] = odds_val
                                elif oc_type == "U":
                                    bov_lines[line_val]["under_price"] = odds_val

                            for lv, prices in bov_lines.items():
                                if "over_price" in prices and "under_price" in prices:
                                    if "bov_alt_totals" not in odds:
                                        odds["bov_alt_totals"] = []
                                    odds["bov_alt_totals"].append({
                                        "line": lv,
                                        "over_price": prices["over_price"],
                                        "under_price": prices["under_price"],
                                    })

                            if bov_lines:
                                best_line = None
                                best_juice = float("inf")
                                for lv, prices in bov_lines.items():
                                    op = prices.get("over_price", -110)
                                    up = prices.get("under_price", -110)
                                    juice = abs(op - (-110)) + abs(up - (-110))
                                    if juice < best_juice:
                                        best_juice = juice
                                        best_line = lv
                                if best_line is not None and best_line in bov_lines:
                                    odds["total_line"] = best_line
                                    odds["over_price"] = bov_lines[best_line].get("over_price", -110)
                                    odds["under_price"] = bov_lines[best_line].get("under_price", -110)

                # ---- Fix Bovada spread home/away inversion ----
                # Bovada's "H"/"A" type on spread outcomes sometimes
                # assigns the handicap to the wrong side.  The moneyline
                # type mapping IS reliable, so use ML to detect and
                # correct the inversion before the data reaches merging.
                _hml = odds.get("home_ml", 0)
                _aml = odds.get("away_ml", 0)
                _hs = odds.get("home_spread", 0)
                if _hml and _aml and _hs and _hml != _aml:
                    _home_fav = _hml < _aml
                    # If home is favorite, home_spread should be negative
                    # (giving points).  If positive, the sides are swapped.
                    _signs_wrong = (
                        (_home_fav and _hs > 0)
                        or (not _home_fav and _hs < 0)
                    )
                    if _signs_wrong:
                        logger.debug(
                            "Bovada %s @ %s: fixing spread H/A swap "
                            "(home %+.1f @ %+.0f ↔ away %+.1f @ %+.0f)",
                            away_name, home_name,
                            odds.get("home_spread", 0),
                            odds.get("home_spread_price", 0),
                            odds.get("away_spread", 0),
                            odds.get("away_spread_price", 0),
                        )
                        odds["home_spread"], odds["away_spread"] = (
                            odds.get("away_spread", 0),
                            odds.get("home_spread", 0),
                        )
                        odds["home_spread_price"], odds["away_spread_price"] = (
                            odds.get("away_spread_price", -110),
                            odds.get("home_spread_price", -110),
                        )
                        for alt in odds.get("bov_alt_spreads", []):
                            alt["home_spread"], alt["away_spread"] = (
                                alt["away_spread"], alt["home_spread"],
                            )
                            alt["home_price"], alt["away_price"] = (
                                alt["away_price"], alt["home_price"],
                            )

                event = OddsEvent(
                    source="bovada",
                    home_team=home_name,
                    away_team=away_name,
                    commence_time=commence,
                    home_ml=odds.get("home_ml", 0),
                    away_ml=odds.get("away_ml", 0),
                    home_spread=odds.get("home_spread", 0),
                    away_spread=odds.get("away_spread", 0),
                    home_spread_price=odds.get("home_spread_price", -110),
                    away_spread_price=odds.get("away_spread_price", -110),
                    total_line=odds.get("total_line", 0),
                    over_price=odds.get("over_price", -110),
                    under_price=odds.get("under_price", -110),
                    alt_totals=odds.get("bov_alt_totals", []),
                    alt_spreads=odds.get("bov_alt_spreads", []),
                )
                if event.home_abbr and event.away_abbr:
                    events.append(_validate_event(event))
                else:
                    logger.warning(
                        "Bovada: dropping event -- unmapped teams "
                        "(home=%r->%r, away=%r->%r)",
                        home_name, event.home_abbr, away_name, event.away_abbr,
                    )

    except Exception as exc:
        logger.warning("Bovada parse error: %s", exc)

    logger.info("Bovada: fetched odds for %d events", len(events))
    return events


# ---- The Odds API (existing, requires API key) ----

# Shared cache for The Odds API response so we don't make duplicate calls.
# We use the ``us`` region which includes all major US sportsbooks
# (FanDuel, BetMGM, DraftKings, Caesars, etc.) for broad coverage.
# Hard Rock may also appear via the ``us2`` region — we try both if
# the primary request returns no usable bookmaker data.
_odds_api_cache: Dict[str, Any] = {"data": None, "timestamp": 0.0}
_ODDS_API_CACHE_TTL = 10.0  # seconds

# ---------------------------------------------------------------------------
# Alternate-line cache — fetched once pregame, reused across fast sync cycles.
# Keyed by Odds API event_id → (alt_totals, alt_spreads).
# Refreshed on a slow cadence (default 30 min) to avoid per-event API calls
# on every sync cycle.
# ---------------------------------------------------------------------------
_alt_line_cache: Dict[str, Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]] = {}
_alt_line_cache_ts: float = 0.0  # monotonic timestamp of last full refresh
_ALT_LINE_CACHE_TTL: float = 3600.0  # 60 minutes (conserve Odds API credits)


async def _fetch_odds_api_raw(
    client: httpx.AsyncClient,
) -> Optional[List[Dict[str, Any]]]:
    """Fetch raw data from The Odds API with caching.

    Requests the combined ``us,us2`` regions in a single API call to get
    ALL US sportsbooks (FanDuel, DraftKings, BetMGM, Caesars, Hard Rock,
    etc.) in one response.  Falls back to individual regions if the
    combined request fails.  The result is cached for
    ``_ODDS_API_CACHE_TTL`` seconds.
    """
    import time as _time

    now = _time.monotonic()
    if (
        _odds_api_cache["data"] is not None
        and now - _odds_api_cache["timestamp"] < _ODDS_API_CACHE_TTL
    ):
        return _odds_api_cache["data"]

    api_key = settings.odds_api_key
    if not api_key:
        logger.warning(
            "ODDS_API_KEY is not set — The Odds API will not be called. "
            "Create backend/.env with your key (see .env.example)."
        )
        return None

    url = "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds"

    # Try market sets in order: full (with game props + period markets)
    # then core-only.  Each market × region = 1 credit.
    # Full set: h2h, spreads, totals, btts, h2h_h1, totals_h1, spreads_h1
    #   = 7 credits per sync (covers ALL events in a single call).
    # Fallback: core 3 markets only if the extended set fails.
    _MARKET_SETS = [
        "h2h,spreads,totals,btts,h2h_h1,totals_h1,spreads_h1",
        "h2h,spreads,totals",
    ]

    # Use a single region to halve credit usage.  "us" covers
    # FanDuel, DraftKings, BetMGM, Caesars which is sufficient.
    for markets in _MARKET_SETS:
        for region in ("us",):
            params = {
                "apiKey": api_key,
                "regions": region,
                "markets": markets,
                "oddsFormat": "american",
            }

            data = await _make_request(client, url, params=params)
            if data is None:
                logger.warning(
                    "Odds API: markets=%r region='%s' failed "
                    "(network error or HTTP error).",
                    markets, region,
                )
                continue
            if isinstance(data, dict) and data.get("message"):
                # API returns {"message": "..."} for auth errors
                logger.warning("Odds API error: %s", data.get("message"))
                return None
            if isinstance(data, list) and len(data) > 0:
                # Check that at least one event has bookmaker data
                has_bookmakers = any(
                    len(ev.get("bookmakers", [])) > 0 for ev in data
                )
                if has_bookmakers:
                    total_books = set()
                    for ev in data:
                        for bm in ev.get("bookmakers", []):
                            total_books.add(bm.get("key", "unknown"))
                    logger.info(
                        "Odds API: got %d events from '%s' region(s), "
                        "markets=%r, %d bookmakers: %s",
                        len(data), region, markets, len(total_books),
                        ", ".join(sorted(total_books)),
                    )
                    _odds_api_cache["data"] = data
                    _odds_api_cache["timestamp"] = now
                    return data
                else:
                    logger.info(
                        "Odds API: '%s' region returned %d events but no bookmakers, trying next",
                        region, len(data),
                    )

    logger.warning("Odds API: all region attempts failed — no data returned")
    return None


async def _fetch_odds_api(client: httpx.AsyncClient) -> List[OddsEvent]:
    """
    Fetch NHL odds from The Odds API (v4).

    Requires ODDS_API_KEY environment variable. Returns empty list if
    no key is configured. This is a paid/rate-limited source — we use
    it as a fallback/supplementary source.

    Uses a combined us+us2 region request shared with ``_fetch_hardrock``
    to avoid doubling API quota usage.
    """
    events: List[OddsEvent] = []
    api_key = settings.odds_api_key
    if not api_key:
        logger.info("The Odds API: no API key configured, skipping")
        return events

    data = await _fetch_odds_api_raw(client)
    if not data:
        logger.info("The Odds API: no data returned")
        return events

    for ev in data:
        home_team = ev.get("home_team", "")
        away_team = ev.get("away_team", "")
        commence = ev.get("commence_time", "")

        # Aggregate across all bookmakers for best odds.
        # Exclude Hard Rock bookmakers — they are handled by
        # _fetch_hardrock() as a dedicated source to avoid
        # double-counting in the merge.
        all_home_ml: List[float] = []
        all_away_ml: List[float] = []
        all_home_spread: List[Tuple[float, float]] = []  # (line, price)
        all_away_spread: List[Tuple[float, float]] = []
        all_total: List[Tuple[float, float, float]] = []  # (line, over_price, under_price)

        # Regulation winner (3-way moneyline: h2h_3)
        all_reg_home: List[float] = []
        all_reg_away: List[float] = []
        all_reg_draw: List[float] = []

        # 1st period markets (h2h_h1, spreads_h1, totals_h1)
        all_p1_home_ml: List[float] = []
        all_p1_away_ml: List[float] = []
        all_p1_draw: List[float] = []
        all_p1_home_spread: List[Tuple[float, float]] = []
        all_p1_away_spread: List[Tuple[float, float]] = []
        all_p1_total: List[Tuple[float, float, float]] = []

        # Both Teams to Score (BTTS)
        all_btts_yes: List[float] = []
        all_btts_no: List[float] = []

        for bm in ev.get("bookmakers", []):
            bm_key = bm.get("key", "").lower().replace("-", "_")
            bm_title = bm.get("title", "").lower()
            if bm_key in _HARDROCK_KEYS or "hard rock" in bm_title:
                continue  # handled separately by _fetch_hardrock
            for market in bm.get("markets", []):
                mkey = market.get("key", "")
                outcomes = market.get("outcomes", [])

                if mkey == "h2h":
                    for oc in outcomes:
                        name = oc.get("name", "")
                        price = oc.get("price", 0)
                        if name == home_team:
                            all_home_ml.append(float(price))
                        elif name == away_team:
                            all_away_ml.append(float(price))

                elif mkey == "h2h_3":
                    for oc in outcomes:
                        name = oc.get("name", "")
                        price = float(oc.get("price", 0))
                        if name == home_team:
                            all_reg_home.append(price)
                        elif name == away_team:
                            all_reg_away.append(price)
                        elif name.lower() in ("draw", "tie"):
                            all_reg_draw.append(price)

                elif mkey == "spreads":
                    for oc in outcomes:
                        name = oc.get("name", "")
                        point = float(oc.get("point", 0))
                        price = float(oc.get("price", 0))
                        if name == home_team:
                            all_home_spread.append((point, price))
                        elif name == away_team:
                            all_away_spread.append((point, price))

                elif mkey == "totals":
                    over_p = under_p = 0.0
                    line = 0.0
                    for oc in outcomes:
                        point = float(oc.get("point", 0))
                        price = float(oc.get("price", 0))
                        if oc.get("name", "").lower() == "over":
                            over_p = price
                            line = point
                        elif oc.get("name", "").lower() == "under":
                            under_p = price
                    if line > 0:
                        all_total.append((line, over_p, under_p))

                # --- 1st period markets ---
                elif mkey == "h2h_h1":
                    for oc in outcomes:
                        name = oc.get("name", "")
                        price = float(oc.get("price", 0))
                        if name == home_team:
                            all_p1_home_ml.append(price)
                        elif name == away_team:
                            all_p1_away_ml.append(price)
                        elif name.lower() in ("draw", "tie"):
                            all_p1_draw.append(price)

                elif mkey == "spreads_h1":
                    for oc in outcomes:
                        name = oc.get("name", "")
                        point = float(oc.get("point", 0))
                        price = float(oc.get("price", 0))
                        if name == home_team:
                            all_p1_home_spread.append((point, price))
                        elif name == away_team:
                            all_p1_away_spread.append((point, price))

                elif mkey == "totals_h1":
                    p1_over = p1_under = 0.0
                    p1_line = 0.0
                    for oc in outcomes:
                        point = float(oc.get("point", 0))
                        price = float(oc.get("price", 0))
                        if oc.get("name", "").lower() == "over":
                            p1_over = price
                            p1_line = point
                        elif oc.get("name", "").lower() == "under":
                            p1_under = price
                    if p1_line > 0:
                        all_p1_total.append((p1_line, p1_over, p1_under))

                # --- BTTS (Both Teams to Score) ---
                elif mkey == "btts":
                    for oc in outcomes:
                        name = oc.get("name", "").lower()
                        price = float(oc.get("price", 0))
                        if name == "yes":
                            all_btts_yes.append(price)
                        elif name == "no":
                            all_btts_no.append(price)


        # Best odds across books
        home_ml = max(all_home_ml) if all_home_ml else 0
        away_ml = max(all_away_ml) if all_away_ml else 0

        # Consensus spread
        home_spread = home_spread_price = 0.0
        away_spread = away_spread_price = 0.0
        if all_home_spread:
            # Most common absolute spread
            spread_counts = Counter(abs(s[0]) for s in all_home_spread)
            consensus = spread_counts.most_common(1)[0][0]
            consensus_books = [s for s in all_home_spread if abs(s[0]) == consensus]
            home_spread = sum(s[0] for s in consensus_books) / len(consensus_books)
            home_spread_price = sum(s[1] for s in consensus_books) / len(consensus_books)
        if all_away_spread:
            spread_counts = Counter(abs(s[0]) for s in all_away_spread)
            consensus = spread_counts.most_common(1)[0][0]
            consensus_books = [s for s in all_away_spread if abs(s[0]) == consensus]
            away_spread = sum(s[0] for s in consensus_books) / len(consensus_books)
            away_spread_price = sum(s[1] for s in consensus_books) / len(consensus_books)

        # Aggregate all totals by line value — keep best price per line
        total_line = over_price = under_price = 0.0
        oa_alt_totals: List[Dict[str, float]] = []
        if all_total:
            by_line: Dict[float, Dict[str, List[float]]] = {}
            for t_line, t_over, t_under in all_total:
                if t_line not in by_line:
                    by_line[t_line] = {"over": [], "under": []}
                if t_over:
                    by_line[t_line]["over"].append(t_over)
                if t_under:
                    by_line[t_line]["under"].append(t_under)

            for lv in sorted(by_line):
                overs = by_line[lv]["over"]
                unders = by_line[lv]["under"]
                if overs and unders:
                    # Best (highest) price across books for each side
                    oa_alt_totals.append({
                        "line": lv,
                        "over_price": round(max(overs)),
                        "under_price": round(max(unders)),
                    })

            # Consensus for primary line
            line_counts = Counter(t[0] for t in all_total)
            consensus_line = line_counts.most_common(1)[0][0]
            consensus_totals = [t for t in all_total if t[0] == consensus_line]
            total_line = consensus_line
            over_price = sum(t[1] for t in consensus_totals) / len(consensus_totals)
            under_price = sum(t[2] for t in consensus_totals) / len(consensus_totals)

        event = OddsEvent(
            source="the_odds_api",
            home_team=home_team,
            away_team=away_team,
            commence_time=commence,
            home_ml=home_ml,
            away_ml=away_ml,
            home_spread=round(home_spread, 1),
            away_spread=round(away_spread, 1),
            home_spread_price=round(home_spread_price),
            away_spread_price=round(away_spread_price),
            total_line=total_line,
            over_price=round(over_price),
            under_price=round(under_price),
            alt_totals=oa_alt_totals,
        )

        # Attach 1st period odds
        if all_p1_home_ml:
            event.p1_home_ml = round(max(all_p1_home_ml))
        if all_p1_away_ml:
            event.p1_away_ml = round(max(all_p1_away_ml))
        if all_p1_draw:
            event.p1_draw_price = round(max(all_p1_draw))
        if all_p1_home_spread:
            # Use the most common absolute line, best price
            p1_sp_counts = Counter(abs(s[0]) for s in all_p1_home_spread)
            p1_sp_consensus = p1_sp_counts.most_common(1)[0][0]
            p1_sp_books = [s for s in all_p1_home_spread if abs(s[0]) == p1_sp_consensus]
            event.p1_spread_line = round(p1_sp_books[0][0], 1)
            event.p1_home_spread_price = round(max(s[1] for s in p1_sp_books))
        if all_p1_away_spread:
            p1_sp_counts = Counter(abs(s[0]) for s in all_p1_away_spread)
            p1_sp_consensus = p1_sp_counts.most_common(1)[0][0]
            p1_sp_books = [s for s in all_p1_away_spread if abs(s[0]) == p1_sp_consensus]
            event.p1_away_spread_price = round(max(s[1] for s in p1_sp_books))
        if all_p1_total:
            # Consensus line, best prices
            p1_tl_counts = Counter(t[0] for t in all_p1_total)
            p1_tl_consensus = p1_tl_counts.most_common(1)[0][0]
            p1_tl_books = [t for t in all_p1_total if t[0] == p1_tl_consensus]
            event.p1_total_line = p1_tl_consensus
            event.p1_over_price = round(max(t[1] for t in p1_tl_books))
            event.p1_under_price = round(max(t[2] for t in p1_tl_books))

        # Attach regulation winner (3-way) odds — best price across books
        if all_reg_home:
            event.reg_home_price = round(max(all_reg_home))
        if all_reg_away:
            event.reg_away_price = round(max(all_reg_away))
        if all_reg_draw:
            event.reg_draw_price = round(max(all_reg_draw))

        # Attach BTTS odds — best price across books
        if all_btts_yes:
            event.btts_yes_price = round(max(all_btts_yes))
        if all_btts_no:
            event.btts_no_price = round(max(all_btts_no))

        if event.home_abbr and event.away_abbr:
            events.append(_validate_event(event))
        else:
            logger.warning(
                "The Odds API: dropping event -- unmapped teams "
                "(home=%r->%r, away=%r->%r)",
                home_team, event.home_abbr, away_team, event.away_abbr,
            )

    logger.info("The Odds API: fetched odds for %d events", len(events))
    return events


# ---- Hard Rock Bet (via The Odds API, us2 region) ----

# Known candidate bookmaker keys for Hard Rock Bet on The Odds API.
# The exact key isn't publicly documented — we try common variants.
_HARDROCK_KEYS = {"hard_rock_bet", "hardrockbet", "hardrock", "hard_rock"}


async def _fetch_hardrock_alt_lines(
    client: httpx.AsyncClient,
    event_id: str,
    home_team: str,
    away_team: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Fetch alternate spreads and totals for a single event from Hard Rock.

    Uses the per-event endpoint ``/events/{eventId}/odds`` which supports
    additional markets like ``alternate_spreads`` and ``alternate_totals``.

    Returns (alt_totals, alt_spreads) lists ready for ``OddsEvent``.
    """
    alt_totals: List[Dict[str, Any]] = []
    alt_spreads: List[Dict[str, Any]] = []

    api_key = settings.odds_api_key
    if not api_key:
        return alt_totals, alt_spreads

    url = f"https://api.the-odds-api.com/v4/sports/icehockey_nhl/events/{event_id}/odds"
    params = {
        "apiKey": api_key,
        "regions": "us2",
        "markets": "alternate_spreads,alternate_totals",
        "oddsFormat": "american",
    }

    data = await _make_request(client, url, params=params)
    if not data or not isinstance(data, dict):
        return alt_totals, alt_spreads

    for bm in data.get("bookmakers", []):
        bm_key = bm.get("key", "").lower().replace("-", "_")
        bm_title = bm.get("title", "").lower()
        if bm_key not in _HARDROCK_KEYS and "hard rock" not in bm_title:
            continue

        for market in bm.get("markets", []):
            mkey = market.get("key", "")
            outcomes = market.get("outcomes", [])

            if mkey == "alternate_totals":
                # Group outcomes by point value into over/under pairs
                by_line: Dict[float, Dict[str, float]] = {}
                for oc in outcomes:
                    point = float(oc.get("point", 0))
                    price = float(oc.get("price", 0))
                    side = oc.get("name", "").lower()
                    if point <= 0:
                        continue
                    if point not in by_line:
                        by_line[point] = {}
                    if "over" in side:
                        by_line[point]["over_price"] = round(price)
                    elif "under" in side:
                        by_line[point]["under_price"] = round(price)
                for lv in sorted(by_line):
                    prices = by_line[lv]
                    if "over_price" in prices and "under_price" in prices:
                        alt_totals.append({
                            "line": lv,
                            "over_price": prices["over_price"],
                            "under_price": prices["under_price"],
                        })

            elif mkey == "alternate_spreads":
                # Group outcomes by absolute point value into home/away pairs
                by_abs: Dict[float, Dict[str, float]] = {}
                for oc in outcomes:
                    name = oc.get("name", "")
                    point = float(oc.get("point", 0))
                    price = float(oc.get("price", 0))
                    abs_line = abs(point)
                    if abs_line not in by_abs:
                        by_abs[abs_line] = {}
                    if name == home_team:
                        by_abs[abs_line]["home_spread"] = point
                        by_abs[abs_line]["home_price"] = round(price)
                    elif name == away_team:
                        by_abs[abs_line]["away_spread"] = point
                        by_abs[abs_line]["away_price"] = round(price)
                for abs_lv in sorted(by_abs):
                    prices = by_abs[abs_lv]
                    if "home_price" in prices and "away_price" in prices:
                        alt_spreads.append({
                            "line": abs_lv,
                            "home_spread": prices.get("home_spread", -abs_lv),
                            "away_spread": prices.get("away_spread", abs_lv),
                            "home_price": prices["home_price"],
                            "away_price": prices["away_price"],
                        })
        break  # only need the Hard Rock bookmaker

    return alt_totals, alt_spreads


async def _fetch_hardrock(
    client: httpx.AsyncClient,
    skip_alternates: bool = False,
) -> List[OddsEvent]:
    """
    Fetch NHL odds specifically from Hard Rock Bet via The Odds API (us2 region).

    Hard Rock Bet uses Kambi Odds Feed+ on a proprietary platform with no
    public API.  The Odds API aggregator includes Hard Rock in its ``us2``
    region.  We extract only the Hard Rock bookmaker data so that its odds
    enter the merge as a first-class source with accurate, round-number pricing.

    Primary lines come from the bulk ``/odds`` endpoint (shared via
    ``_fetch_odds_api_raw``).  Alternate spreads and totals are fetched
    per-event from ``/events/{eventId}/odds`` with the
    ``alternate_spreads,alternate_totals`` markets.

    When ``skip_alternates`` is True, per-event alternate line API calls are
    skipped and cached data is used instead.  This dramatically reduces API
    credit usage during fast sync cycles.

    Requires ODDS_API_KEY.  Returns empty list if no key is configured or
    Hard Rock data is not present in the response.
    """
    events: List[OddsEvent] = []
    api_key = settings.odds_api_key
    if not api_key:
        logger.info("Hard Rock: no Odds API key configured, skipping")
        return events

    data = await _fetch_odds_api_raw(client)
    if not data:
        logger.info("Hard Rock: no data returned from Odds API")
        return events

    # First pass: extract primary lines and collect event IDs for alt-line fetch
    primary_data: List[Dict[str, Any]] = []
    for ev in data:
        home_team = ev.get("home_team", "")
        away_team = ev.get("away_team", "")
        commence = ev.get("commence_time", "")
        event_id = ev.get("id", "")

        # Find the Hard Rock bookmaker entry
        hr_bm = None
        for bm in ev.get("bookmakers", []):
            bm_key = bm.get("key", "").lower().replace("-", "_")
            bm_title = bm.get("title", "").lower()
            if bm_key in _HARDROCK_KEYS or "hard rock" in bm_title:
                hr_bm = bm
                break

        if hr_bm is None:
            continue

        # Parse markets from the Hard Rock bookmaker
        home_ml = away_ml = 0.0
        home_spread = away_spread = 0.0
        home_spread_price = away_spread_price = 0.0
        total_line = over_price = under_price = 0.0

        for market in hr_bm.get("markets", []):
            mkey = market.get("key", "")
            outcomes = market.get("outcomes", [])

            if mkey == "h2h":
                for oc in outcomes:
                    name = oc.get("name", "")
                    price = float(oc.get("price", 0))
                    if name == home_team:
                        home_ml = price
                    elif name == away_team:
                        away_ml = price

            elif mkey == "spreads":
                for oc in outcomes:
                    name = oc.get("name", "")
                    point = float(oc.get("point", 0))
                    price = float(oc.get("price", 0))
                    if name == home_team:
                        home_spread = point
                        home_spread_price = price
                    elif name == away_team:
                        away_spread = point
                        away_spread_price = price

            elif mkey == "totals":
                for oc in outcomes:
                    point = float(oc.get("point", 0))
                    price = float(oc.get("price", 0))
                    if oc.get("name", "").lower() == "over":
                        over_price = price
                        total_line = point
                    elif oc.get("name", "").lower() == "under":
                        under_price = price

        # Parse 1st period markets from Hard Rock
        hr_p1_home_ml = hr_p1_away_ml = hr_p1_draw = 0.0
        hr_p1_spread_line = hr_p1_hsp = hr_p1_asp = 0.0
        hr_p1_total_line = hr_p1_over = hr_p1_under = 0.0

        for market in hr_bm.get("markets", []):
            mkey = market.get("key", "")
            outcomes = market.get("outcomes", [])

            if mkey == "h2h_h1":
                for oc in outcomes:
                    name = oc.get("name", "")
                    price = float(oc.get("price", 0))
                    if name == home_team:
                        hr_p1_home_ml = price
                    elif name == away_team:
                        hr_p1_away_ml = price
                    elif name.lower() in ("draw", "tie"):
                        hr_p1_draw = price

            elif mkey == "spreads_h1":
                for oc in outcomes:
                    name = oc.get("name", "")
                    point = float(oc.get("point", 0))
                    price = float(oc.get("price", 0))
                    if name == home_team:
                        hr_p1_spread_line = point
                        hr_p1_hsp = price
                    elif name == away_team:
                        hr_p1_asp = price

            elif mkey == "totals_h1":
                for oc in outcomes:
                    point = float(oc.get("point", 0))
                    price = float(oc.get("price", 0))
                    if oc.get("name", "").lower() == "over":
                        hr_p1_over = price
                        hr_p1_total_line = point
                    elif oc.get("name", "").lower() == "under":
                        hr_p1_under = price

        # Only add if we got at least moneyline data
        if home_ml == 0 and away_ml == 0:
            continue

        primary_data.append({
            "event_id": event_id,
            "home_team": home_team,
            "away_team": away_team,
            "commence": commence,
            "home_ml": round(home_ml),
            "away_ml": round(away_ml),
            "home_spread": round(home_spread, 1),
            "away_spread": round(away_spread, 1),
            "home_spread_price": round(home_spread_price),
            "away_spread_price": round(away_spread_price),
            "total_line": total_line,
            "over_price": round(over_price),
            "under_price": round(under_price),
            "p1_home_ml": round(hr_p1_home_ml) if hr_p1_home_ml else 0,
            "p1_away_ml": round(hr_p1_away_ml) if hr_p1_away_ml else 0,
            "p1_draw": round(hr_p1_draw) if hr_p1_draw else 0,
            "p1_spread_line": round(hr_p1_spread_line, 1) if hr_p1_spread_line else 0,
            "p1_hsp": round(hr_p1_hsp) if hr_p1_hsp else 0,
            "p1_asp": round(hr_p1_asp) if hr_p1_asp else 0,
            "p1_total_line": hr_p1_total_line,
            "p1_over": round(hr_p1_over) if hr_p1_over else 0,
            "p1_under": round(hr_p1_under) if hr_p1_under else 0,
        })

    # Second pass: fetch alt lines — either fresh (full sync) or from cache
    # (fast sync).  The alt-line cache avoids per-event API calls that burn
    # through Odds API credits on every 60-second sync cycle.
    global _alt_line_cache, _alt_line_cache_ts

    now_mono = _time_mod.monotonic()
    cache_fresh = (
        _alt_line_cache
        and (now_mono - _alt_line_cache_ts) < _ALT_LINE_CACHE_TTL
    )

    if skip_alternates and cache_fresh:
        # Use cached alt lines — zero API calls
        alt_map = _alt_line_cache
        logger.debug(
            "Hard Rock: using cached alt lines for %d events (age %.0fs)",
            len(alt_map), now_mono - _alt_line_cache_ts,
        )
    else:
        # Full fetch: one API call per event
        alt_results = await asyncio.gather(
            *(
                _fetch_hardrock_alt_lines(
                    client, pd["event_id"], pd["home_team"], pd["away_team"]
                )
                for pd in primary_data
                if pd["event_id"]
            ),
            return_exceptions=True,
        )

        alt_map: Dict[str, Tuple[List, List]] = {}
        alt_idx = 0
        for pd in primary_data:
            if pd["event_id"]:
                result = alt_results[alt_idx]
                alt_idx += 1
                if isinstance(result, tuple):
                    alt_map[pd["event_id"]] = result
                else:
                    logger.warning("Hard Rock alt-line fetch failed for %s: %s", pd["event_id"], result)

        # Update the cache
        _alt_line_cache = dict(alt_map)
        _alt_line_cache_ts = now_mono
        logger.info(
            "Hard Rock: refreshed alt-line cache for %d events",
            len(alt_map),
        )

    # Build OddsEvent objects with alt lines
    for pd in primary_data:
        alt_totals, alt_spreads = alt_map.get(pd["event_id"], ([], []))

        event = OddsEvent(
            source="hardrock",
            home_team=pd["home_team"],
            away_team=pd["away_team"],
            commence_time=pd["commence"],
            home_ml=pd["home_ml"],
            away_ml=pd["away_ml"],
            home_spread=pd["home_spread"],
            away_spread=pd["away_spread"],
            home_spread_price=pd["home_spread_price"],
            away_spread_price=pd["away_spread_price"],
            total_line=pd["total_line"],
            over_price=pd["over_price"],
            under_price=pd["under_price"],
            alt_totals=alt_totals,
            alt_spreads=alt_spreads,
        )

        # Attach 1st period odds from Hard Rock
        if pd.get("p1_home_ml"):
            event.p1_home_ml = pd["p1_home_ml"]
        if pd.get("p1_away_ml"):
            event.p1_away_ml = pd["p1_away_ml"]
        if pd.get("p1_draw"):
            event.p1_draw_price = pd["p1_draw"]
        if pd.get("p1_spread_line"):
            event.p1_spread_line = pd["p1_spread_line"]
        if pd.get("p1_hsp"):
            event.p1_home_spread_price = pd["p1_hsp"]
        if pd.get("p1_asp"):
            event.p1_away_spread_price = pd["p1_asp"]
        if pd.get("p1_total_line"):
            event.p1_total_line = pd["p1_total_line"]
        if pd.get("p1_over"):
            event.p1_over_price = pd["p1_over"]
        if pd.get("p1_under"):
            event.p1_under_price = pd["p1_under"]

        if event.home_abbr and event.away_abbr:
            events.append(_validate_event(event))
        else:
            logger.warning(
                "Hard Rock: dropping event -- unmapped teams "
                "(home=%r->%r, away=%r->%r)",
                pd["home_team"], event.home_abbr,
                pd["away_team"], event.away_abbr,
            )

    total_alts = sum(
        len(e.alt_totals) + len(e.alt_spreads) for e in events
    )
    logger.info(
        "Hard Rock: fetched odds for %d events (%d alt lines)",
        len(events), total_alts,
    )

    # Log the discovered bookmaker key on first successful fetch for debugging
    if events and data:
        for bm in data[0].get("bookmakers", []):
            bm_key = bm.get("key", "").lower().replace("-", "_")
            bm_title = bm.get("title", "").lower()
            if bm_key in _HARDROCK_KEYS or "hard rock" in bm_title:
                logger.info(
                    "Hard Rock: discovered bookmaker key=%r title=%r",
                    bm.get("key"), bm.get("title"),
                )
                break

    return events


# ---------------------------------------------------------------------------
# Multi-source aggregation
# ---------------------------------------------------------------------------

def _american_to_implied(odds: float) -> float:
    """Convert American odds to implied probability (0-1).

    Thin wrapper around the canonical implementation in services.odds,
    adapted for the non-Optional signature used throughout this module.
    """
    result = _svc_american_to_implied(odds)
    return result if result is not None else 0.0


def _implied_to_american(prob: float) -> float:
    """Convert implied probability (0-1) back to American odds."""
    from app.services.odds import implied_to_american
    result = implied_to_american(prob)
    if result is not None:
        return result
    if prob >= 1.0:
        return -10000.0
    if prob <= 0.0:
        return 10000.0
    return 0.0


def _mean_odds(odds_list: List[float]) -> float:
    """Average a list of American odds via implied probability.

    Converts each American odds value to implied probability, averages
    the probabilities, then converts back.  This is mathematically
    correct because implied probability is linear while American odds
    are not.
    """
    probs = [_american_to_implied(o) for o in odds_list if o != 0]
    if not probs:
        return 0.0
    avg_prob = sum(probs) / len(probs)
    return round(_implied_to_american(avg_prob))


def _normalize_moneyline_pair(
    home_ml: float, away_ml: float
) -> Tuple[float, float]:
    """Ensure a moneyline pair has one favorite (negative) and one underdog (positive).

    When averaging implied probabilities independently per side, both can land
    below 0.5 (both positive odds) if sources disagree on the favorite.  Fix
    this by re-normalising the pair so probabilities sum to a realistic
    overround (~1.03-1.05) while preserving the ratio between the two sides.
    The higher-probability side is then guaranteed to exceed 0.5 and receive
    negative American odds.
    """
    home_prob = _american_to_implied(home_ml)
    away_prob = _american_to_implied(away_ml)
    raw_sum = home_prob + away_prob

    # If the pair already has one negative side, no adjustment needed.
    if home_ml < 0 or away_ml < 0:
        return home_ml, away_ml

    # Both positive — re-normalise to a ~4% overround (typical sportsbook vig).
    target_sum = 1.04
    scale = target_sum / raw_sum
    home_prob *= scale
    away_prob *= scale

    return round(_implied_to_american(home_prob)), round(_implied_to_american(away_prob))


def _merge_odds_events(
    all_events: List[List[OddsEvent]],
) -> List[Dict[str, Any]]:
    """
    Merge odds from multiple sources into a single best-odds-per-game dict.

    Groups events by (home_abbr, away_abbr) matchup, then selects the
    best available line for each market across all sources.

    Returns a list of dicts, each representing one game with merged odds.
    """
    # Group by matchup key
    matchup_odds: Dict[str, List[OddsEvent]] = {}
    for source_events in all_events:
        for ev in source_events:
            key = f"{ev.home_abbr}_{ev.away_abbr}"
            if key not in matchup_odds:
                matchup_odds[key] = []
            matchup_odds[key].append(ev)

    logger.info(
        "Odds merge: %d unique matchups from sources — %s",
        len(matchup_odds),
        ", ".join(sorted(matchup_odds.keys())),
    )

    merged: List[Dict[str, Any]] = []

    for key, ev_list in matchup_odds.items():
        # Consensus moneyline: average implied probabilities across all
        # sources, then convert back to American odds.  Averaging American
        # odds directly is mathematically wrong (non-linear scale) and can
        # produce two positive sides; implied probability is the correct
        # domain for averaging.
        home_mls = [e.home_ml for e in ev_list if e.has_moneyline()]
        away_mls = [e.away_ml for e in ev_list if e.has_moneyline()]

        best_home_ml = _mean_odds(home_mls) if home_mls else None
        best_away_ml = _mean_odds(away_mls) if away_mls else None

        # Guard against both sides showing positive odds.  This happens
        # when sources disagree on the favorite and independent averaging
        # pushes both implied probs below 0.5.  Re-normalise so the
        # stronger side always gets negative odds.
        if best_home_ml and best_away_ml:
            best_home_ml, best_away_ml = _normalize_moneyline_pair(
                best_home_ml, best_away_ml
            )

        # Consensus spread: most common absolute value across sources.
        # Use moneyline data to determine the correct sign so that
        # conflicting source signs never flip the puck line.
        home_spreads = [(e.home_spread, e.home_spread_price, e.source) for e in ev_list if e.has_spread()]
        away_spreads = [(e.away_spread, e.away_spread_price, e.source) for e in ev_list if e.has_spread()]

        # Log per-source spread data to identify bad sources
        if home_spreads:
            for hs, hp, src in home_spreads:
                for _as, ap, _ in [(a, b, c) for a, b, c in away_spreads if c == src]:
                    logger.info(
                        "  spread %s@%s [%s]: home %+.1f @ %+.0f  away %+.1f @ %+.0f",
                        ev_list[0].away_abbr, ev_list[0].home_abbr, src,
                        hs, hp, _as, ap,
                    )

        # Determine which team is the favorite from moneyline data.
        # Lower moneyline = favorite.  This is the source of truth
        # for which team gets the negative spread.
        home_is_fav = None
        if best_home_ml and best_away_ml:
            home_is_fav = best_home_ml < best_away_ml
        elif home_spreads:
            # Fall back to majority vote of signed home_spread values
            neg_count = sum(1 for s in home_spreads if s[0] < 0)
            pos_count = sum(1 for s in home_spreads if s[0] > 0)
            if neg_count > pos_count:
                home_is_fav = True
            elif pos_count > neg_count:
                home_is_fav = False

        best_home_spread = best_home_spread_price = None
        best_away_spread = best_away_spread_price = None
        if home_spreads:
            spread_vals = Counter(abs(s[0]) for s in home_spreads if s[0] != 0)
            if spread_vals:
                consensus = spread_vals.most_common(1)[0][0]
            else:
                consensus = 1.5  # NHL standard puck line
            # Enforce minimum ±1.5 — puck lines below that don't exist
            if consensus < 1.5:
                consensus = 1.5

            # Enforce correct sign based on moneyline-derived favorite.
            # This prevents conflicting source signs from flipping the
            # puck line direction.
            if home_is_fav is True:
                best_home_spread = -consensus
                best_away_spread = consensus
            elif home_is_fav is False:
                best_home_spread = consensus
                best_away_spread = -consensus
            else:
                # No moneyline data; use first non-zero signed value
                signed = [s[0] for s in home_spreads if s[0] != 0]
                if signed:
                    best_home_spread = -consensus if signed[0] < 0 else consensus
                    best_away_spread = -best_home_spread
                else:
                    best_home_spread = -consensus
                    best_away_spread = consensus

            # Best price: only use entries whose spread sign matches
            # the moneyline-derived direction.  Sources that report the
            # home spread with the wrong sign have swapped home/away,
            # so their prices belong to the opposite side and would
            # contaminate the average.
            if home_is_fav is not None:
                if home_is_fav:
                    # Home is favorite → home spread should be negative
                    consensus_home_books = [
                        s for s in home_spreads
                        if abs(s[0]) == consensus and s[0] < 0
                    ]
                    consensus_away_books = [
                        s for s in away_spreads
                        if abs(s[0]) == consensus and s[0] > 0
                    ]
                else:
                    # Home is underdog → home spread should be positive
                    consensus_home_books = [
                        s for s in home_spreads
                        if abs(s[0]) == consensus and s[0] > 0
                    ]
                    consensus_away_books = [
                        s for s in away_spreads
                        if abs(s[0]) == consensus and s[0] < 0
                    ]
                # Log rejected entries
                all_home = [s for s in home_spreads if abs(s[0]) == consensus]
                all_away = [s for s in away_spreads if abs(s[0]) == consensus]
                for r in all_home:
                    if r not in consensus_home_books:
                        logger.warning(
                            "REJECTED %s@%s [%s]: home spread %+.1f @ %+.0f — "
                            "wrong sign direction (home_is_fav=%s)",
                            ev_list[0].away_abbr, ev_list[0].home_abbr,
                            r[2], r[0], r[1], home_is_fav,
                        )
                for r in all_away:
                    if r not in consensus_away_books:
                        logger.warning(
                            "REJECTED %s@%s [%s]: away spread %+.1f @ %+.0f — "
                            "wrong sign direction (home_is_fav=%s)",
                            ev_list[0].away_abbr, ev_list[0].home_abbr,
                            r[2], r[0], r[1], home_is_fav,
                        )
                # Fall back to all entries if filtering removed everything
                if not consensus_home_books:
                    consensus_home_books = all_home
                if not consensus_away_books:
                    consensus_away_books = all_away
            else:
                consensus_home_books = [s for s in home_spreads if abs(s[0]) == consensus]
                consensus_away_books = [s for s in away_spreads if abs(s[0]) == consensus]

            # Defense-in-depth: filter out any remaining extreme spread
            # prices that slipped through source validation (e.g. moneyline
            # values in spread fields).
            from app.scrapers.odds_validation import is_reasonable_spread_price, MAX_SPREAD_PRICE_ABS
            reasonable_home = [s for s in consensus_home_books if is_reasonable_spread_price(s[1])]
            reasonable_away = [s for s in consensus_away_books if is_reasonable_spread_price(s[1])]
            for s in consensus_home_books:
                if not is_reasonable_spread_price(s[1]):
                    logger.warning(
                        "REJECTED %s@%s [%s]: home spread price %+.0f exceeds max ±%d",
                        ev_list[0].away_abbr, ev_list[0].home_abbr, s[2], s[1],
                        MAX_SPREAD_PRICE_ABS,
                    )
            for s in consensus_away_books:
                if not is_reasonable_spread_price(s[1]):
                    logger.warning(
                        "REJECTED %s@%s [%s]: away spread price %+.0f exceeds max ±%d",
                        ev_list[0].away_abbr, ev_list[0].home_abbr, s[2], s[1],
                        MAX_SPREAD_PRICE_ABS,
                    )
            if reasonable_home:
                consensus_home_books = reasonable_home
            if reasonable_away:
                consensus_away_books = reasonable_away

            if consensus_home_books:
                best_home_spread_price = _mean_odds([s[1] for s in consensus_home_books])
            if consensus_away_books:
                best_away_spread_price = _mean_odds([s[1] for s in consensus_away_books])

        # Consensus total — filter out implausible lines first.
        # Filter implausible O/U lines (pregame ~4.5-8.5, live can go higher).
        total_data = [
            (e.total_line, e.over_price, e.under_price)
            for e in ev_list
            if e.has_total() and _OU_LINE_MIN <= e.total_line <= _OU_LINE_MAX
        ]
        best_total = best_over = best_under = None
        if total_data:
            line_counts = Counter(t[0] for t in total_data)
            consensus_line = line_counts.most_common(1)[0][0]
            consensus_totals = [t for t in total_data if t[0] == consensus_line]
            best_total = consensus_line
            best_over = _mean_odds([t[1] for t in consensus_totals])
            best_under = _mean_odds([t[2] for t in consensus_totals])

        # Aggregate ALL available total lines across all sources.
        # Pair-based selection: over/under prices for each line must come
        # from the same source and pass vig validation.  Among valid pairs
        # per line, pick the one with lowest total vig (best for bettor).
        from app.scrapers.odds_validation import (
            validate_total_line_pair,
            validate_alt_totals_monotonicity,
            validate_alt_spreads_monotonicity,
            validate_moneyline,
            validate_spread_pair,
        )

        matchup_label = f"{ev_list[0].away_abbr}@{ev_list[0].home_abbr}"

        # Collect all (over, under) pairs per line, keyed by source
        all_line_pairs: Dict[float, List[Tuple[float, float, str]]] = {}
        for e in ev_list:
            # Primary line from this source
            if e.has_total() and _OU_LINE_MIN <= e.total_line <= _OU_LINE_MAX:
                lv = e.total_line
                if lv not in all_line_pairs:
                    all_line_pairs[lv] = []
                all_line_pairs[lv].append((e.over_price, e.under_price, e.source))
            # Alt lines from this source
            for alt in e.alt_totals:
                lv = alt["line"]
                if lv < _OU_LINE_MIN or lv > _OU_LINE_MAX:
                    continue
                op = alt.get("over_price", 0)
                up = alt.get("under_price", 0)
                if op == 0 or up == 0:
                    continue
                if lv not in all_line_pairs:
                    all_line_pairs[lv] = []
                all_line_pairs[lv].append((op, up, e.source))

        # For each line, pick the best valid pair
        all_lines_map: Dict[float, Dict[str, float]] = {}
        for lv, pairs in all_line_pairs.items():
            best_pair = None
            best_vig = float("inf")
            for op, up, src in pairs:
                if not validate_total_line_pair(lv, op, up):
                    logger.debug(
                        "Merge %s: total %.1f from %s rejected (bad vig: O=%s U=%s)",
                        matchup_label, lv, src, op, up,
                    )
                    continue
                # Vig = sum of implied probs - 1.0; lower is better for bettor
                imp_o = abs(op) / (abs(op) + 100) if op < 0 else 100 / (op + 100)
                imp_u = abs(up) / (abs(up) + 100) if up < 0 else 100 / (up + 100)
                vig = imp_o + imp_u - 1.0
                if vig < best_vig:
                    best_vig = vig
                    best_pair = (op, up, src)
            if best_pair:
                all_lines_map[lv] = {
                    "over_price": best_pair[0],
                    "under_price": best_pair[1],
                }

        # When no primary total was found, derive from best alt lines
        if best_total is None and all_lines_map:
            # Pick the line with the most source contributions
            line_counts = Counter(
                lv for lv, pairs in all_line_pairs.items()
                if lv in all_lines_map
            )
            if line_counts:
                consensus_line = line_counts.most_common(1)[0][0]
                prices = all_lines_map[consensus_line]
                best_total = consensus_line
                best_over = prices["over_price"]
                best_under = prices["under_price"]

        # Build sorted list and enforce monotonicity
        all_total_lines_raw = sorted([
            {
                "line": lv,
                "over_price": round(prices["over_price"]),
                "under_price": round(prices["under_price"]),
            }
            for lv, prices in all_lines_map.items()
        ], key=lambda x: x["line"])

        all_total_lines = validate_alt_totals_monotonicity(
            all_total_lines_raw, label=matchup_label,
        )

        # Aggregate ALL available spread lines across all sources.
        # Use moneyline-derived ``home_is_fav`` to enforce correct
        # spread direction so prices always pair with the right side.
        all_spreads_map: Dict[float, Dict[str, float]] = {}

        def _init_spread_entry(abs_line: float) -> Dict[str, float]:
            """Create a new spread entry with correct signs from ML data."""
            if home_is_fav is True:
                return {
                    "home_price": -999, "away_price": -999,
                    "home_spread": -abs_line, "away_spread": abs_line,
                }
            elif home_is_fav is False:
                return {
                    "home_price": -999, "away_price": -999,
                    "home_spread": abs_line, "away_spread": -abs_line,
                }
            else:
                return {
                    "home_price": -999, "away_price": -999,
                    "home_spread": 0, "away_spread": 0,
                }

        def _valid_spread_price(
            price: float, abs_line: float, is_home: bool,
        ) -> bool:
            """Check if a spread price has a plausible sign.

            For lines ≥ 2.0 we can validate against moneyline data:
            the team giving up goals (negative spread / favorite)
            should have positive odds, and the team getting goals
            (positive spread / underdog) should have negative odds.
            A violation means the source likely swapped home/away.
            """
            if abs_line < 2.0 or home_is_fav is None:
                return True  # can't validate small lines or without ML
            if is_home:
                if home_is_fav:
                    return price > 0   # home -2.5+ → positive price
                else:
                    return price < 0   # home +2.5+ → negative price
            else:
                if home_is_fav:
                    return price < 0   # away +2.5+ → negative price
                else:
                    return price > 0   # away -2.5+ → positive price

        # Pair-based spread selection: collect all (home_price,
        # away_price, source) tuples per line, then pick the best valid
        # pair per line.  This prevents mixing prices from different
        # sources (which can produce invalid combinations like +480/-165
        # from different sportsbooks).
        all_spread_pairs: Dict[float, List[Tuple[float, float, str, float, float]]] = {}
        # Each tuple: (home_price, away_price, source, home_spread, away_spread)

        for e in ev_list:
            # Include primary spread
            if e.has_spread():
                abs_line = abs(e.home_spread) if e.home_spread else abs(e.away_spread)

                # Determine if this source's spread direction is inverted
                # relative to the ML-derived favorite.  If so, swap prices.
                e_hp = e.home_spread_price
                e_ap = e.away_spread_price
                e_hs = e.home_spread
                e_as = e.away_spread
                if home_is_fav is not None and e.home_spread:
                    source_inverted = (
                        (home_is_fav and e.home_spread > 0)
                        or (not home_is_fav and e.home_spread < 0)
                    )
                    if source_inverted:
                        e_hp, e_ap = e_ap, e_hp
                        e_hs, e_as = e_as, e_hs

                if abs_line not in all_spread_pairs:
                    all_spread_pairs[abs_line] = []
                all_spread_pairs[abs_line].append((e_hp, e_ap, e.source, e_hs, e_as))

            # Include alt spreads
            for alt in e.alt_spreads:
                abs_line = alt["line"]
                hp = alt.get("home_price", -110)
                ap = alt.get("away_price", -110)
                alt_hs = alt.get("home_spread", 0)
                alt_as = alt.get("away_spread", 0)
                # Determine expected home_spread sign from ML data
                expected_hs_sign = None
                if home_is_fav is True:
                    expected_hs_sign = -1  # home is fav → negative spread
                elif home_is_fav is False:
                    expected_hs_sign = 1   # home is dog → positive spread
                # If direction disagrees, swap prices
                if (expected_hs_sign is not None and alt_hs
                        and (alt_hs > 0) != (expected_hs_sign > 0)):
                    hp, ap = ap, hp
                    alt_hs, alt_as = alt_as, alt_hs

                if abs_line not in all_spread_pairs:
                    all_spread_pairs[abs_line] = []
                all_spread_pairs[abs_line].append((hp, ap, e.source, alt_hs, alt_as))

        # For each line, pick the best valid pair (lowest vig)
        for abs_line, pairs in all_spread_pairs.items():
            best_pair = None
            best_vig = float("inf")
            for hp, ap, src, hs, a_s in pairs:
                if not _valid_spread_price(hp, abs_line, True):
                    logger.debug(
                        "Merge %s: spread %.1f from %s home price %s rejected (wrong sign)",
                        matchup_label, abs_line, src, hp,
                    )
                    continue
                if not _valid_spread_price(ap, abs_line, False):
                    logger.debug(
                        "Merge %s: spread %.1f from %s away price %s rejected (wrong sign)",
                        matchup_label, abs_line, src, ap,
                    )
                    continue
                if not validate_spread_pair(hp, ap):
                    logger.debug(
                        "Merge %s: spread %.1f from %s rejected (bad vig: H=%s A=%s)",
                        matchup_label, abs_line, src, hp, ap,
                    )
                    continue
                # Compute vig
                imp_h = abs(hp) / (abs(hp) + 100) if hp < 0 else 100 / (hp + 100)
                imp_a = abs(ap) / (abs(ap) + 100) if ap < 0 else 100 / (ap + 100)
                vig = imp_h + imp_a - 1.0
                if vig < best_vig:
                    best_vig = vig
                    best_pair = (hp, ap, src, hs, a_s)

            if best_pair:
                hp, ap, src, hs, a_s = best_pair
                entry = _init_spread_entry(abs_line)
                entry["home_price"] = hp
                entry["away_price"] = ap
                if hs:
                    entry["home_spread"] = hs
                if a_s:
                    entry["away_spread"] = a_s
                all_spreads_map[abs_line] = entry

        all_spread_lines_raw = sorted([
            {
                "line": abs_lv,
                "home_spread": round(prices["home_spread"], 1),
                "away_spread": round(prices["away_spread"], 1),
                "home_price": round(prices["home_price"]),
                "away_price": round(prices["away_price"]),
            }
            for abs_lv, prices in all_spreads_map.items()
            if prices["home_price"] > -999 and prices["away_price"] > -999
            and abs_lv >= 1.5  # NHL puck lines below ±1.5 don't exist
        ], key=lambda x: x["line"])

        all_spread_lines = validate_alt_spreads_monotonicity(
            all_spread_lines_raw, label=matchup_label,
        )

        # Use first event for metadata
        first = ev_list[0]
        sources_used = list(set(e.source for e in ev_list))

        logger.info(
            "Merge %s@%s: sources=%s, ML=%s/%s, O/U=%s (%s/%s), spread=%s (%s/%s)",
            first.away_abbr, first.home_abbr,
            "+".join(sorted(sources_used)),
            round(best_home_ml) if best_home_ml is not None else "?",
            round(best_away_ml) if best_away_ml is not None else "?",
            f"{best_total:.1f}" if best_total is not None else "?",
            round(best_over) if best_over is not None else "?",
            round(best_under) if best_under is not None else "?",
            f"{best_home_spread:+.1f}" if best_home_spread is not None else "?",
            round(best_home_spread_price) if best_home_spread_price is not None else "?",
            round(best_away_spread_price) if best_away_spread_price is not None else "?",
        )

        # ----- Prop odds: best price across all sources -----
        def _best_price(values, negative_preferred=True):
            """Pick the best (least negative / most positive) American price."""
            valid = [v for v in values if v and v != 0.0]
            if not valid:
                return None
            # For negative odds, least negative is best (e.g. -150 > -180)
            # For positive odds, most positive is best (e.g. +150 > +120)
            return round(max(valid))

        merged.append({
            "home_team": first.home_team,
            "away_team": first.away_team,
            "home_abbrev": first.home_abbr,
            "away_abbrev": first.away_abbr,
            "commence_time": first.commence_time,
            "sources": sources_used,
            "best_odds": {
                "home_moneyline": round(best_home_ml) if best_home_ml is not None else None,
                "away_moneyline": round(best_away_ml) if best_away_ml is not None else None,
                "home_spread": _normalize_spread_line(round(best_home_spread, 1)) if best_home_spread is not None else None,
                "away_spread": _normalize_spread_line(round(best_away_spread, 1)) if best_away_spread is not None else None,
                "home_spread_price": round(best_home_spread_price) if best_home_spread_price is not None else None,
                "away_spread_price": round(best_away_spread_price) if best_away_spread_price is not None else None,
                "over_under": best_total,
                "over_price": round(best_over) if best_over is not None else None,
                "under_price": round(best_under) if best_under is not None else None,
            },
            "all_total_lines": all_total_lines,
            "all_spread_lines": all_spread_lines,
            # 1st period odds: best price across all sources
            "p1_odds": {
                "home_ml": _best_price([getattr(e, "p1_home_ml", None) for e in ev_list]),
                "away_ml": _best_price([getattr(e, "p1_away_ml", None) for e in ev_list]),
                "draw_price": _best_price([getattr(e, "p1_draw_price", None) for e in ev_list]),
                "spread_line": getattr(first, "p1_spread_line", None) if hasattr(first, "p1_spread_line") else None,
                "home_spread_price": _best_price([getattr(e, "p1_home_spread_price", None) for e in ev_list]),
                "away_spread_price": _best_price([getattr(e, "p1_away_spread_price", None) for e in ev_list]),
                "total_line": getattr(first, "p1_total_line", None) if hasattr(first, "p1_total_line") else None,
                "over_price": _best_price([getattr(e, "p1_over_price", None) for e in ev_list]),
                "under_price": _best_price([getattr(e, "p1_under_price", None) for e in ev_list]),
            },
            # Regulation winner (3-way) odds: best price across all sources
            "reg_odds": {
                "home_price": _best_price([getattr(e, "reg_home_price", None) for e in ev_list]),
                "away_price": _best_price([getattr(e, "reg_away_price", None) for e in ev_list]),
                "draw_price": _best_price([getattr(e, "reg_draw_price", None) for e in ev_list]),
            },
            # Both Teams to Score (BTTS) odds
            "btts_odds": {
                "yes_price": _best_price([getattr(e, "btts_yes_price", None) for e in ev_list]),
                "no_price": _best_price([getattr(e, "btts_no_price", None) for e in ev_list]),
            },
            "all_sources": [e.to_dict() for e in ev_list],
        })

    return merged


# ---------------------------------------------------------------------------
# Public API: MultiSourceOddsScraper
# ---------------------------------------------------------------------------

class MultiSourceOddsScraper:
    """
    Orchestrates odds fetching from multiple sportsbook sources and
    merges them to find the best available lines for each NHL game.

    Usage:
        scraper = MultiSourceOddsScraper()
        odds = await scraper.fetch_best_odds()
        await scraper.sync_odds(db_session)
        await scraper.close()
    """

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(20.0),
                follow_redirects=True,
                limits=httpx.Limits(
                    max_keepalive_connections=10,
                    max_connections=20,
                ),
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    async def fetch_best_odds(
        self,
        skip_alternates: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Fetch odds using a primary/fallback strategy.

        PRIMARY: The Odds API (Hard Rock + generic bookmakers). This is the
        authoritative source with clean, validated pricing across all major
        US sportsbooks.

        FALLBACK: Direct scrapers (DraftKings, FanDuel, Kambi, Bovada) are
        only invoked if the Odds API returns no data — e.g. API key expired,
        rate-limited, or service outage.

        When ``skip_alternates`` is True, per-event alternate line API calls
        are skipped in favour of cached data, dramatically reducing API
        credit usage for fast sync cycles.
        """
        client = self._get_client()

        # ── Phase 1: Odds API (primary) ─────────────────────────────
        primary_results = await asyncio.gather(
            _fetch_hardrock(client, skip_alternates=skip_alternates),
            _fetch_odds_api(client),
            return_exceptions=True,
        )

        all_events: List[List[OddsEvent]] = []
        primary_names = ["Hard Rock", "Odds API"]

        for i, result in enumerate(primary_results):
            if isinstance(result, Exception):
                logger.warning("%s fetch failed: %s", primary_names[i], result)
                all_events.append([])
            else:
                all_events.append(result)

        primary_event_count = sum(len(evts) for evts in all_events)

        if primary_event_count > 0:
            logger.info(
                "Odds API primary: %d events from %d/%d sources",
                primary_event_count,
                sum(1 for evts in all_events if evts),
                len(primary_names),
            )
            self._log_events(all_events, primary_names)
            return _merge_odds_events(all_events)

        # ── Phase 2: Direct scrapers (fallback) ─────────────────────
        logger.warning(
            "Odds API returned no data — falling back to direct scrapers"
        )

        fallback_results = await asyncio.gather(
            _fetch_draftkings(client),
            _fetch_fanduel(client),
            _fetch_kambi(client),
            _fetch_bovada(client),
            return_exceptions=True,
        )

        all_events = []
        fallback_names = ["DraftKings", "FanDuel", "Kambi", "Bovada"]

        for i, result in enumerate(fallback_results):
            if isinstance(result, Exception):
                logger.warning("%s fetch failed: %s", fallback_names[i], result)
                all_events.append([])
            else:
                all_events.append(result)

        fallback_event_count = sum(len(evts) for evts in all_events)
        fallback_sources = sum(1 for evts in all_events if evts)

        if fallback_event_count == 0:
            logger.warning("No odds data from any source (primary or fallback)!")
            return []

        logger.info(
            "Scraper fallback: %d events from %d/%d sources",
            fallback_event_count, fallback_sources, len(fallback_names),
        )
        self._log_events(all_events, fallback_names)
        return _merge_odds_events(all_events)

    def _log_events(
        self,
        all_events: List[List["OddsEvent"]],
        source_names: List[str],
    ) -> None:
        """Log per-event details at DEBUG level."""
        for i, evts in enumerate(all_events):
            if not evts:
                continue
            for ev in evts:
                ml_str = (
                    f"ML {round(ev.home_ml)}/{round(ev.away_ml)}"
                    if ev.has_moneyline() else "ML --"
                )
                ou_str = (
                    f"O/U {ev.total_line:.1f} ({round(ev.over_price)}/{round(ev.under_price)})"
                    if ev.has_total() else "O/U --"
                )
                n_alts = len(ev.alt_totals)
                n_aspr = len(ev.alt_spreads)
                logger.debug(
                    "[%s] %s@%s: %s, %s, %d alt totals, %d alt spreads",
                    source_names[i], ev.away_abbr, ev.home_abbr,
                    ml_str, ou_str, n_alts, n_aspr,
                )

    async def sync_odds(
        self,
        db: AsyncSession,
        skip_alternates: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Fetch odds from all sources and sync them to Game records in the DB.

        Matches odds to existing Game records by team abbreviations and date.
        Updates Game model odds fields with the best available lines.

        When ``skip_alternates`` is True, per-event alternate line API calls
        are skipped (cached data is used instead).

        Returns the list of matched odds dicts.
        """
        odds_list = await self.fetch_best_odds(skip_alternates=skip_alternates)

        if not odds_list:
            logger.info("No odds to sync")
            return []

        logger.info(
            "Odds sync starting: %d games from sportsbooks: %s",
            len(odds_list),
            ", ".join(
                f"{o.get('away_abbrev','?')}@{o.get('home_abbrev','?')}"
                for o in odds_list
            ),
        )

        matched: List[Dict[str, Any]] = []

        for odds in odds_list:
            home_abbrev = odds.get("home_abbrev", "")
            away_abbrev = odds.get("away_abbrev", "")

            if not home_abbrev or not away_abbrev:
                logger.warning(
                    "Odds sync: skipping event with missing abbreviation "
                    "(home=%r, away=%r, raw_teams=%s vs %s)",
                    home_abbrev, away_abbrev,
                    odds.get("away_team", ""), odds.get("home_team", ""),
                )
                continue

            # Parse commence_time to the LOCAL game date.
            # The NHL API stores Game.date as the local (ET) calendar date,
            # but commence_time from odds sources is UTC.  A 10:30 PM ET
            # game is 3:30 AM UTC the next day — so we must convert to ET
            # before extracting .date() to avoid a one-day mismatch for
            # late-night games.
            commence = odds.get("commence_time", "")
            game_date = None
            if commence:
                try:
                    if isinstance(commence, str):
                        ct = commence.replace("Z", "+00:00")
                        dt = datetime.fromisoformat(ct)
                    else:
                        dt = commence
                    # Convert to US/Eastern so the date matches
                    # the local game date stored in the DB.
                    dt_et = dt.astimezone(ZoneInfo("America/New_York"))
                    game_date = dt_et.date()
                except (ValueError, TypeError, AttributeError):
                    # Try parsing as timestamp
                    try:
                        dt = datetime.fromtimestamp(float(commence) / 1000, tz=timezone.utc)
                        dt_et = dt.astimezone(ZoneInfo("America/New_York"))
                        game_date = dt_et.date()
                    except (ValueError, TypeError, OverflowError):
                        continue
            else:
                logger.warning(
                    "Odds sync: no commence_time for %s@%s — skipping",
                    away_abbrev, home_abbrev,
                )
                continue

            logger.info(
                "Odds sync: %s@%s commence=%s -> game_date=%s",
                away_abbrev, home_abbrev,
                odds.get("commence_time", ""), game_date,
            )

            # Find matching teams
            home_result = await db.execute(
                select(Team).where(Team.abbreviation == home_abbrev)
            )
            home_team = home_result.scalar_one_or_none()

            away_result = await db.execute(
                select(Team).where(Team.abbreviation == away_abbrev)
            )
            away_team = away_result.scalar_one_or_none()

            if not home_team or not away_team:
                logger.warning(
                    "Odds sync: team lookup failed for %s vs %s "
                    "(home=%s, away=%s)",
                    home_abbrev, away_abbrev,
                    "found" if home_team else "NOT FOUND",
                    "found" if away_team else "NOT FOUND",
                )
                continue

            # Find the matching game.  Try exact date first, then
            # adjacent day as a safety net for DST edge cases.
            game = None
            for candidate_date in (game_date, game_date - timedelta(days=1), game_date + timedelta(days=1)):
                game_result = await db.execute(
                    select(Game).where(
                        Game.home_team_id == home_team.id,
                        Game.away_team_id == away_team.id,
                        Game.date == candidate_date,
                    )
                )
                game = game_result.scalar_one_or_none()
                if game is not None:
                    break

            if game is None:
                logger.warning(
                    "Odds sync: no DB game for %s vs %s on %s (±1 day) "
                    "[commence=%s]",
                    home_abbrev, away_abbrev, game_date,
                    odds.get("commence_time", ""),
                )
                continue

            # Snapshot pregame odds before overwriting with live values.
            # If the game is in-progress and we haven't snapshotted yet,
            # copy current odds to the pregame_* fields.
            if (
                game.status and game.status.lower() in ("in_progress", "live")
                and game.pregame_home_moneyline is None
                and game.home_moneyline is not None
            ):
                game.pregame_home_moneyline = game.home_moneyline
                game.pregame_away_moneyline = game.away_moneyline
                game.pregame_over_under_line = game.over_under_line
                game.pregame_home_spread_line = game.home_spread_line
                game.pregame_away_spread_line = game.away_spread_line
                game.pregame_home_spread_price = game.home_spread_price
                game.pregame_away_spread_price = game.away_spread_price
                game.pregame_over_price = game.over_price
                game.pregame_under_price = game.under_price
                logger.info(
                    "Pregame odds snapshot saved for %s@%s (game %s)",
                    away_abbrev, home_abbrev, game.id,
                )

            # Persist best odds to the Game record.
            # Validate each market pair before writing — skip invalid
            # fields and preserve existing DB data.
            from app.scrapers.odds_validation import (
                is_valid_american_odds,
                validate_moneyline,
                validate_total_line_pair,
                validate_spread_pair,
            )

            best = odds.get("best_odds", {})
            sync_label = f"{away_abbrev}@{home_abbrev}"

            # Moneyline: validate both sides are valid American odds
            h_ml = best.get("home_moneyline")
            a_ml = best.get("away_moneyline")
            if h_ml is not None and a_ml is not None:
                if is_valid_american_odds(h_ml) and is_valid_american_odds(a_ml):
                    game.home_moneyline = h_ml
                    game.away_moneyline = a_ml
                else:
                    logger.warning(
                        "DB sync %s: skipping invalid ML H=%s A=%s",
                        sync_label, h_ml, a_ml,
                    )
            elif h_ml is not None and is_valid_american_odds(h_ml):
                game.home_moneyline = h_ml
            elif a_ml is not None and is_valid_american_odds(a_ml):
                game.away_moneyline = a_ml

            # Over/Under: validate line + prices as a valid market
            ou_line = best.get("over_under")
            op = best.get("over_price")
            up = best.get("under_price")
            if ou_line is not None:
                ou_raw = float(ou_line)
                # Normalize to nearest .5 line.
                if ou_raw % 1 != 0.5:
                    ou_raw = round(ou_raw * 2) / 2
                    if ou_raw % 1 == 0:
                        ou_raw += 0.5
                # Sanity check: must be within plausible range (includes live totals)
                if _OU_LINE_MIN <= ou_raw <= _OU_LINE_MAX:
                    if op is not None and up is not None:
                        if validate_total_line_pair(ou_raw, op, up):
                            game.over_under_line = ou_raw
                            game.over_price = op
                            game.under_price = up
                        else:
                            logger.warning(
                                "DB sync %s: O/U %.1f failed vig check (O=%s U=%s) — keeping existing",
                                sync_label, ou_raw, op, up,
                            )
                    else:
                        # Line present but missing prices — write line only
                        game.over_under_line = ou_raw
                        if op is not None:
                            game.over_price = op
                        if up is not None:
                            game.under_price = up
                else:
                    logger.warning(
                        "DB sync %s: discarding out-of-range O/U line %.1f",
                        sync_label, ou_raw,
                    )

            # Spread: validate price pair
            hs = best.get("home_spread")
            aws = best.get("away_spread")
            hsp = best.get("home_spread_price")
            asp = best.get("away_spread_price")
            if hs is not None:
                game.home_spread_line = _normalize_spread_line(float(hs))
            if aws is not None:
                game.away_spread_line = _normalize_spread_line(float(aws))
            if hsp is not None and asp is not None:
                if validate_spread_pair(hsp, asp):
                    game.home_spread_price = hsp
                    game.away_spread_price = asp
                else:
                    logger.warning(
                        "DB sync %s: spread prices failed vig check (H=%s A=%s) — keeping existing",
                        sync_label, hsp, asp,
                    )
            else:
                if hsp is not None:
                    game.home_spread_price = hsp
                if asp is not None:
                    game.away_spread_price = asp

            # Persist all available total/spread lines
            atl = odds.get("all_total_lines")
            if atl:
                game.all_total_lines = atl
            asl = odds.get("all_spread_lines")
            if asl:
                game.all_spread_lines = asl

            # Persist 1st period odds
            p1 = odds.get("p1_odds", {})
            if p1.get("home_ml") is not None:
                game.period1_home_ml = p1["home_ml"]
            if p1.get("away_ml") is not None:
                game.period1_away_ml = p1["away_ml"]
            if p1.get("draw_price") is not None:
                game.period1_draw_price = p1["draw_price"]
            if p1.get("spread_line") is not None:
                game.period1_spread_line = p1["spread_line"]
            if p1.get("home_spread_price") is not None:
                game.period1_home_spread_price = p1["home_spread_price"]
            if p1.get("away_spread_price") is not None:
                game.period1_away_spread_price = p1["away_spread_price"]
            if p1.get("total_line") is not None:
                game.period1_total_line = p1["total_line"]
            if p1.get("over_price") is not None:
                game.period1_over_price = p1["over_price"]
            if p1.get("under_price") is not None:
                game.period1_under_price = p1["under_price"]

            # Persist regulation winner (3-way) odds
            reg = odds.get("reg_odds", {})
            if reg.get("home_price") is not None:
                game.reg_home_price = reg["home_price"]
            if reg.get("away_price") is not None:
                game.reg_away_price = reg["away_price"]
            if reg.get("draw_price") is not None:
                game.reg_draw_price = reg["draw_price"]

            # Persist BTTS odds
            btts = odds.get("btts_odds", {})
            if btts.get("yes_price") is not None:
                game.btts_yes_price = btts["yes_price"]
            if btts.get("no_price") is not None:
                game.btts_no_price = btts["no_price"]

            game.odds_updated_at = datetime.now(timezone.utc)

            matched.append({
                "game_id": game.id,
                "game_external_id": game.external_id,
                "home_abbrev": home_abbrev,
                "away_abbrev": away_abbrev,
                "game_date": str(game_date),
                "sources": odds.get("sources", []),
                "best_odds": best,
            })

        if len(matched) < len(odds_list):
            matched_keys = {f"{m['home_abbrev']}v{m['away_abbrev']}" for m in matched}
            all_keys = {f"{o.get('home_abbrev','')}v{o.get('away_abbrev','')}" for o in odds_list}
            unmatched = all_keys - matched_keys
            logger.warning(
                "Odds sync: %d/%d games matched. Unmatched: %s",
                len(matched), len(odds_list), ", ".join(sorted(unmatched)),
            )
        else:
            logger.info(
                "Odds sync: all %d games matched successfully",
                len(matched),
            )
        return matched
