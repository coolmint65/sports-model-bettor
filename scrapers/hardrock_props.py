"""
Hard Rock Bet player props scraper (MLB; NBA/NHL stubbed for 2h/2i).

Phase 2f-ii: a sibling module to ``scrapers.hardrock_odds`` that
harvests per-player prop markets from Hard Rock's GraphQL ``betSync``
endpoint. Same host, same auth flow — different query body that asks
for the ``markets()`` field on each event without a ``marketTypes``
filter, so the response includes the full prop menu (the same call
the Hard Rock UI fires when a user opens the Player Props tab).

Why a separate module: ``hardrock_odds.py`` is already 1.6k lines
covering game lines + Phase 1 derivatives. Mixing prop parsing into
that file would push it well past the 500-line ceiling in
CLAUDE.md and tangle two distinct concerns. Shared transport
(headers, decoding, team extraction) is reused by import.

Returns a dict keyed by ``AWAY@HOME`` matchup, with each value
holding a list of normalized prop rows:

    {
        "BAL@BOS": [
            {
                "player_name": "Kyle Bradish",
                "bet_type": "Pitcher Ks O/U",
                "line": 3.5,
                "side": None,
                "over_odds": -120,
                "under_odds": -110,
                "is_alt": False,
                "hr_market_id": "5331275392722534738",
                "hr_market_type": "BASEBALL:FT:PROPSO",
            },
            ...
        ],
        ...
    }

For alt-line yes/no markets (``PROPALTSTRIKEOUTS5``, ``PROPALTHR2``,
etc.), the threshold is converted to standard O/U semantics: a
"5+ Strikeouts" market becomes ``line=4.5`` with the YES price as
``over_odds``. That way the settler doesn't need a separate
">=" branch for alts — Over 4.5 wins iff actual > 4.5 iff actual
≥ 5. Per the user's directive "always store all the alts" we keep
them so the model can pick whichever line offers the best edge.

Empty dict on any failure — best-bets and the props tracker
should treat this as "no props available" and skip prop picks
for that tick rather than blow up.
"""

from __future__ import annotations

import json
import logging
import re
import time
import urllib.error
import urllib.request
import gzip
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Reuse the existing module's transport and helpers — single source
# of truth for headers (session cookies live there) and team-name
# normalization. If hardrock_odds is broken, props are also broken,
# which is the right blast radius (same upstream).
from scrapers.hardrock_odds import (  # noqa: E402
    HARDROCK_HOST,
    EVENT_TREE_URL,
    _load_headers,
    _parse_odds_string,
    _extract_teams,
    _team_abbr,
)


# ── Cache ─────────────────────────────────────────────────────
_cache: dict[str, tuple[float, dict]] = {}
CACHE_TTL = 60          # match game-line scraper TTL
EMPTY_CACHE_TTL = 120   # back off twice as hard on empty


# ── GraphQL query ─────────────────────────────────────────────
# Hybrid of the new schema (compId filter, sort) plus the legacy
# ``selection { ... }`` field that carries the actual odds. HR's
# UI uses two queries — a metadata probe + a market fetch — but
# this combined shape returns everything in one round trip and
# matches the introspected schema (verified via live response,
# 2026-04-26).
_QUERY = """
query betSync(
  $filters: [Filter]
  $segment: String
  $region: String
  $language: String
  $channel: String
  $marketTypes: [String]
  $sort: Sort
) {
  betSync(cmsSegment: $segment, region: $region, language: $language, channel: $channel) {
    events(filters: $filters, sort: $sort) {
      data {
        id name eventTime sport inplay outright
        path playerPropsAvailable
        markets(keyMarkets: false, marketTypes: $marketTypes) {
          id name type subtype line period state
          selection { id name type odds rootIdx }
        }
      }
      count
    }
  }
}
""".strip()

# League competition IDs (compId) — discovered from live HR feed on
# 2026-04-26. NBA has two compIds (full season + playoff event groups);
# we use the larger event-count one.
_COMP_ID = {
    "mlb": "2899138833758814469",
    "nba": "691033199537586178",
    "nhl": "691036012789334019",
}


def _build_request_body(comp_id: str) -> dict:
    return {
        "operationName": "betSync",
        "query": _QUERY,
        "variables": {
            "channel": "FLORIDA_ONLINE",
            "segment": "fl",
            "region": "us",
            "language": "enus",
            "filters": [
                {"field": "compId", "values": comp_id},
                {"field": "outright", "value": "false"},
            ],
            "sort": {"field": "compEventWeightingV2", "descending": True},
        },
    }


# ── Type-code → bet-type label ────────────────────────────────
# Maps Hard Rock market type codes to the canonical bet-type
# labels we use in ``backend/server._MLB_PROP_TYPES`` and the
# ``player_props_picks`` table. New labels added here must also
# be added to the bet-type set or the settler trampoline will
# silently drop them.

# Per-sport main O/U markets (selection has Over and Under sides).
_MLB_MAIN: dict[str, str] = {
    "BASEBALL:FT:PROPSO":  "Pitcher Ks O/U",
    "BASEBALL:FT:PROPBB":  "Pitcher Walks O/U",
    "BASEBALL:FT:PROPOUT": "Pitcher Outs Recorded",
    "BASEBALL:FT:PROPER":  "Pitcher Earned Runs",
    "BASEBALL:FT:PROPHA":  "Pitcher Hits Allowed",
    "BASEBALL:FT:PROPHR":  "Batter HR",
    "BASEBALL:FT:PROPH":   "Batter Hits O/U",
    "BASEBALL:FT:PROPTB":  "Batter TB",
    "BASEBALL:FT:PROPRBI": "Batter RBI",
    "BASEBALL:FT:PROPR":   "Batter Runs Scored",
    "BASEBALL:FT:PROPSB":  "Batter Stolen Bases",
    "BASEBALL:FT:PROPBBB": "Batter Walks",
}

_NBA_MAIN: dict[str, str] = {
    "BASKETBALL:FT:PROPPTS":   "Player Points",
    "BASKETBALL:FT:PROPREB":   "Player Rebounds",
    "BASKETBALL:FT:PROPAST":   "Player Assists",
    "BASKETBALL:FT:PROPPPAPR": "Player PRA",
    "BASKETBALL:FT:PROP3PM":   "Player 3PM",
    "BASKETBALL:FT:PROPSTL":   "Player Steals",
    "BASKETBALL:FT:PROPBLK":   "Player Blocks",
}

_NHL_MAIN: dict[str, str] = {
    "ICE_HOCKEY:FT:PROPG":   "Skater Goals",
    "ICE_HOCKEY:FT:PROPA":   "Skater Assists",
    "ICE_HOCKEY:FT:PROPPTS": "Skater Points",
    "ICE_HOCKEY:FT:PROPS":   "Skater SOG",
    "ICE_HOCKEY:FT:PROPSV":  "Goalie Saves",
    "ICE_HOCKEY:FT:PROPGA":  "Goalie Goals Against",
}

# Alt yes-only markets per sport. Suffix N is the line threshold (5+,
# 10+, etc.) — converted to standard Over X.5 by the parser so the
# settler doesn't need a separate ">=" branch.
_MLB_ALT: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^BASEBALL:FT:PROPALTSTRIKEOUTS(\d+)$"), "Pitcher Ks O/U"),
    (re.compile(r"^BASEBALL:FT:PROPALTHR(\d+)$"),         "Batter HR"),
    (re.compile(r"^BASEBALL:FT:PROPALTHITS(\d+)$"),       "Batter Hits O/U"),
    (re.compile(r"^BASEBALL:FT:PROPALTRBI(\d+)$"),        "Batter RBI"),
    (re.compile(r"^BASEBALL:FT:PROPALTRUNS(\d+)$"),       "Batter Runs Scored"),
    (re.compile(r"^BASEBALL:FT:PROPALTBASES(\d+)$"),      "Batter TB"),
]

_NBA_ALT: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^BASKETBALL:FT:PROPALTPTS(\d+)$"),    "Player Points"),
    (re.compile(r"^BASKETBALL:FT:PROPALTREB(\d+)$"),    "Player Rebounds"),
    (re.compile(r"^BASKETBALL:FT:PROPALTAST(\d+)$"),    "Player Assists"),
    (re.compile(r"^BASKETBALL:FT:PROPALT3MADE(\d+)$"),  "Player 3PM"),
]

_NHL_ALT: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^ICE_HOCKEY:FT:PROPALTGOALS(\d+)$"), "Skater Goals"),
    (re.compile(r"^ICE_HOCKEY:FT:PROPALTSHOTS(\d+)$"), "Skater SOG"),
]


_MAIN_BY_SPORT = {"mlb": _MLB_MAIN, "nba": _NBA_MAIN, "nhl": _NHL_MAIN}
_ALT_BY_SPORT  = {"mlb": _MLB_ALT,  "nba": _NBA_ALT,  "nhl": _NHL_ALT}


def _classify_market(market_type: str, sport: str = "mlb") -> tuple[str, bool, float | None]:
    """Returns ``(bet_type, is_alt, alt_threshold)``. ``bet_type`` is
    empty when the market is unrecognized — caller should drop it."""
    main = _MAIN_BY_SPORT.get(sport, {})
    alts = _ALT_BY_SPORT.get(sport, [])
    if market_type in main:
        return main[market_type], False, None
    for pattern, label in alts:
        m = pattern.match(market_type)
        if m:
            return label, True, float(m.group(1))
    return "", False, None


# ── Field extraction ──────────────────────────────────────────

# "Kyle Bradish - Strikeouts" / "Marcelo Mayer - 2+ Home Runs" /
# "Wilyer Abreu - To Record 1+ Hits" — player name is everything
# left of the first " - " (or unicode em-dash variant). HR uses an
# ASCII hyphen wrapped in single spaces; we accept en/em dashes too
# in case localization changes the separator.
_PLAYER_SPLIT = re.compile(r"\s[-\u2013\u2014]\s")


def _extract_player_name(market_name: str) -> str:
    if not market_name:
        return ""
    parts = _PLAYER_SPLIT.split(market_name, maxsplit=1)
    return parts[0].strip() if parts else ""


# Subtype encodes the line for main markets: "M:A:1928#3.5".
# For alts the subtype is just the side prefix without #line.
_LINE_FROM_SUBTYPE = re.compile(r"#(-?\d+(?:\.\d+)?)")


def _extract_line(market: dict) -> float | None:
    """Pull the numeric O/U line. Subtype has the ``#X.Y`` suffix on
    main markets; falls back to the ``line`` field, then to parsing
    the first selection's name (``"Over 3.5"``)."""
    sub = market.get("subtype") or ""
    m = _LINE_FROM_SUBTYPE.search(sub)
    if m:
        return float(m.group(1))
    raw_line = market.get("line")
    if raw_line not in (None, ""):
        try:
            return float(raw_line)
        except (TypeError, ValueError):
            pass
    for sel in (market.get("selection") or []):
        name = sel.get("name", "") or ""
        m = re.search(r"(-?\d+(?:\.\d+)?)", name)
        if m:
            return float(m.group(1))
    return None


def _extract_side(market: dict) -> str | None:
    """Returns 'A' (away-team player), 'B' (home-team player), or
    None when the subtype/period don't carry side info (pitchers
    sometimes appear without an A/B prefix)."""
    sub = market.get("subtype") or market.get("period") or ""
    if ":A:" in sub:
        return "A"
    if ":B:" in sub:
        return "B"
    return None


def _selections_to_odds(selections: list[dict]) -> tuple[int | None, int | None]:
    """Returns ``(over_odds, under_odds)`` in American format, or
    ``(yes_odds, None)`` for alt yes/no markets (HR ships them with
    a single ``Yes`` / ``Over`` selection)."""
    over: int | None = None
    under: int | None = None
    yes:  int | None = None
    for sel in selections or []:
        sel_type = (sel.get("type") or "").lower()
        odds = _parse_odds_string(sel.get("odds"))
        if odds is None:
            continue
        if sel_type == "over":
            over = odds
        elif sel_type == "under":
            under = odds
        elif sel_type in ("yes", ""):
            # Some alt markets use an empty type or "Yes" — treat as
            # the affirmative side.
            yes = odds
    if over is not None or under is not None:
        return over, under
    return yes, None


# ── Public API ────────────────────────────────────────────────

def _matchup_key(event_name: str, away_abbr: str, home_abbr: str) -> str:
    """Prefer the abbr-based key (``BAL@BOS``) when team-name → abbr
    resolution succeeded. Keeps prop matchup keys aligned with the
    game-line scraper's keys so downstream code (picker, tracker)
    only needs one shape. Falls back to the event name when abbr
    lookup fails so the caller can still join on something
    deterministic."""
    if away_abbr and home_abbr:
        return f"{away_abbr}@{home_abbr}"
    return event_name or ""


def _fetch_raw(comp_id: str) -> dict:
    """POST the betSync query and return the parsed JSON, or {} on
    any error. Decompresses gzip responses."""
    body = _build_request_body(comp_id)
    headers = _load_headers()
    req = urllib.request.Request(
        EVENT_TREE_URL,
        data=json.dumps(body).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding", "").lower() == "gzip":
                raw = gzip.decompress(raw)
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        logger.warning("HR props HTTP %s %s", e.code, e.reason)
    except (urllib.error.URLError, TimeoutError) as e:
        logger.warning("HR props network error: %s", e)
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("HR props bad JSON: %s", e)
    return {}


def _parse_event(event: dict, sport: str = "mlb") -> tuple[str, list[dict]]:
    """Convert one HR event blob into ``(matchup_key, [prop_rows])``."""
    away_name, home_name = _extract_teams(event)
    # Resolve names → abbreviations so matchup keys match the game-line
    # scraper's shape ("BAL@BOS"). Without this the picker can't join
    # props back to today's game by abbr-pair.
    away_abbr = _team_abbr(sport, away_name)
    home_abbr = _team_abbr(sport, home_name)
    key = _matchup_key(event.get("name", ""), away_abbr, home_abbr)
    rows: list[dict] = []
    for market in (event.get("markets") or []):
        mtype = market.get("type") or ""
        bet_type, is_alt, alt_threshold = _classify_market(mtype, sport)
        if not bet_type:
            continue
        if market.get("state") != "OPEN":
            # Suspended / settled markets aren't actionable. Skip
            # without logging — every event has a few of these.
            continue
        if is_alt:
            # Encode "5+ Strikeouts" as Over 4.5 so the settler can use
            # uniform Over/Under semantics (actual > line wins). Without
            # this the settler needs a separate ">=" branch for alts.
            line = alt_threshold - 0.5
        else:
            line = _extract_line(market)
        if line is None:
            continue
        over_odds, under_odds = _selections_to_odds(market.get("selection") or [])
        if over_odds is None and under_odds is None:
            continue
        rows.append({
            "player_name": _extract_player_name(market.get("name", "")),
            "bet_type": bet_type,
            "line": line,
            "team_side": _extract_side(market),  # 'A' (away player) or 'B' (home player)
            "over_odds": over_odds,
            "under_odds": under_odds,
            "is_alt": is_alt,
            "hr_market_id": market.get("id"),
            "hr_market_type": mtype,
        })
    return key, rows


def _fetch_props(sport: str) -> dict[str, list[dict]]:
    """Fetch + parse player props for a sport. Cached for
    ``CACHE_TTL`` seconds to spare upstream."""
    now = time.time()
    if sport in _cache:
        ts, cached = _cache[sport]
        ttl = EMPTY_CACHE_TTL if not cached else CACHE_TTL
        if now - ts < ttl:
            return cached

    comp_id = _COMP_ID.get(sport)
    if not comp_id:
        return {}
    payload = _fetch_raw(comp_id)
    events = (
        payload.get("data", {}).get("betSync", {})
        .get("events", {}).get("data", []) or []
    )

    out: dict[str, list[dict]] = {}
    for event in events:
        key, rows = _parse_event(event, sport)
        if not key or not rows:
            continue
        out.setdefault(key, []).extend(rows)

    _cache[sport] = (now, out)
    logger.info("HR props %s: %d matchups, %d total rows",
                sport.upper(), len(out), sum(len(v) for v in out.values()))
    return out


def fetch_mlb_props() -> dict[str, list[dict]]: return _fetch_props("mlb")
def fetch_nba_props() -> dict[str, list[dict]]: return _fetch_props("nba")
def fetch_nhl_props() -> dict[str, list[dict]]: return _fetch_props("nhl")


__all__ = ["fetch_mlb_props", "fetch_nba_props", "fetch_nhl_props"]
