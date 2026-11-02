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
    _graphql_post,
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
    # NBA — refreshed 2026-05-29. Old comp 691033199537586178 returned
    # just game-line markets (51); HR's live NBA player-prop board is
    # under the main NBA comp 7594438161586061565 (~900 markets per
    # event including PTS / REB / AST / 3PM lines + alts). The two-
    # comp split was a 2026-04-26 transition that's since collapsed
    # back to one comp.
    "nba": "7594438161586061565",
    "nhl": "691036012789334019",
    # WNBA — discovered from basketball framework's LEAGUE_REGISTRY,
    # 2026-05-13. HR ships full player-prop boards (PTS / REB / AST /
    # 3PM / STL / BLK lines + alts) for in-season games.
    "wnba": "691031701106622466",
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


_NBA_ML = re.compile(r"^BASKETBALL:FT:PROP(PTS|REB|AST|3PM|PPAP|STL)MULTILINE$")
_NBA_ML_BT = {"PTS": "Player Points", "REB": "Player Rebounds", "AST": "Player Assists",
              "3PM": "Player 3PM", "PPAP": "Player PRA", "STL": "Player Steals"}
_ML_SEL = re.compile(r"^(over|under)\s+(-?\d+(?:\.\d+)?)$", re.I)


_MAIN_BY_SPORT = {"mlb": _MLB_MAIN, "nba": _NBA_MAIN, "nhl": _NHL_MAIN,
                  "wnba": _NBA_MAIN}
_ALT_BY_SPORT  = {"mlb": _MLB_ALT,  "nba": _NBA_ALT,  "nhl": _NHL_ALT,
                  "wnba": _NBA_ALT}


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
    """Fetch events (with markets) for a comp via the migrated REST route.

    HR moved markets off the GraphQL ``betSync`` POST (/java-graphql/graphql)
    to the REST ``competitions/{comp}/events?includeMarkets=true`` GET on
    2026-10-13; the old POST now 403s (props flatlined across MLB/NHL/WNBA/NFL
    while game lines kept working -- game lines migrated to the REST route,
    props hadn't). Reuse ``_fetch_events_for_comp`` from hardrock_odds: it uses
    the new route AND normalizes the new shape (``selections``->``selection``,
    ``rootIdx``->``odds``) so the parse below reads the legacy shape unchanged.
    Wrap the events in the old ``data.betSync.events.data`` envelope so every
    caller (_fetch_props + the NFL harvesters) finds the list unchanged.
    MintyBot 2026-10-15 (see hr_odds_coverage_gap_2026-10-13)."""
    from scrapers.hardrock_odds import _fetch_events_for_comp
    events, err = _fetch_events_for_comp(comp_id)
    if err:
        logger.warning("HR props fetch (comp %s) err=%s", comp_id, err)
    return {"data": {"betSync": {"events": {"data": events or []}}}}


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
    ml_rows: dict[tuple, dict] = {}
    for market in (event.get("markets") or []):
        mtype = market.get("type") or ""
        mlm = _NBA_ML.match(mtype) if sport in ("nba", "wnba") else None
        if mlm:
            # MULTILINE ladder (MintyBot 2026-11-01): one market, selections "Over X" / "Under X" across many X.
            # HR posts these hours before the per-line PROPPTS mains (11-01 board had only these + one-sided
            # ALTs, so NBA props priced nothing); merged per (player, stat, line) into two-sided rows.
            if market.get("state") != "OPEN":
                continue
            player = _extract_player_name(market.get("name", ""))
            if not player:
                continue
            bt = _NBA_ML_BT[mlm.group(1)]
            for sel in (market.get("selection") or []):
                od = _parse_odds_string(sel.get("odds"))
                mt = _ML_SEL.match((sel.get("type") or sel.get("name") or "").strip())
                if od is None or not mt:
                    continue
                ln = float(mt.group(2))
                row = ml_rows.get((player, bt, ln))
                if row is None:
                    row = ml_rows[(player, bt, ln)] = {
                        "player_name": player, "bet_type": bt, "line": ln,
                        "team_side": _extract_side(market), "over_odds": None, "under_odds": None,
                        "is_alt": False, "hr_market_id": market.get("id"), "hr_market_type": mtype}
                row["over_odds" if mt.group(1).lower() == "over" else "under_odds"] = od
            continue
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
    rows.extend(ml_rows.values())
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


# ── NFL player props: passing/rushing/receiving yards + receptions ──
# HR prices these as ALTERNATE-LINE thresholds ("125+ Pass Yards" YES/NO),
# market type PROPALT{PSYDS,RSYDS,RECYDS,RECEPTIONS}<threshold>. We surface
# each (player, market, threshold, YES-odds); the model prices P(stat >= line).
import re as _re_nflpp
_NFL_PP_ALT = _re_nflpp.compile(r"PROPALT(PSYDS|RSYDS|RECYDS|RECEPTIONS)(\d+)$")
_NFL_PP_MARKET = {"PSYDS": "Passing Yards", "RSYDS": "Rushing Yards",
                  "RECYDS": "Receiving Yards", "RECEPTIONS": "Receptions"}


# HR "NFL" regular-season competition id (engine.football LEAGUE_REGISTRY),
# shared by the NFL player-props fetcher below.
_NFL_COMP_ID = "2295257447167426824"


def fetch_nfl_player_props() -> list[dict]:
    """HR NFL player-prop thresholds (alt-line: passing/rushing/receiving yards
    + receptions). Returns rows {player_name, bet_type, line(threshold), yes_odds,
    event_name, hr_market_id}. YES = the 'Over'/clears-the-threshold selection.
    Empty list on any failure (model then runs probability-only)."""
    now = time.time()
    key = "nfl_playerprops"
    if key in _cache:
        ts, cached = _cache[key]
        ttl = EMPTY_CACHE_TTL if not cached else CACHE_TTL
        if now - ts < ttl:
            return cached
    payload = _fetch_raw(_NFL_COMP_ID)
    events = (payload.get("data", {}).get("betSync", {})
              .get("events", {}).get("data", []) or [])
    out: list[dict] = []
    seen: set = set()
    for ev in events:
        ev_name = ev.get("name") or ""
        for m in (ev.get("markets") or []):
            mm = _NFL_PP_ALT.search((m.get("type") or "").upper())
            if not mm or m.get("state") != "OPEN":
                continue
            bet_type = _NFL_PP_MARKET.get(mm.group(1))
            if not bet_type:
                continue
            thr = int(mm.group(2))
            nm = m.get("name") or ""
            player = (nm.split(" - ")[0].strip() if " - " in nm
                      else _extract_player_name(nm))
            if not player:
                continue
            yes = None
            for sel in (m.get("selection") or []):
                st = (sel.get("type") or "").lower()
                od = _parse_odds_string(sel.get("odds"))
                if od is None:
                    continue
                if st.startswith("over") or st == "yes":
                    yes = od
            if yes is None:
                continue
            dedup = (player, bet_type, thr)
            if dedup in seen:
                continue
            seen.add(dedup)
            out.append({"player_name": player, "bet_type": bet_type,
                        "line": float(thr), "yes_odds": yes,
                        "event_name": ev_name, "hr_market_id": m.get("id")})
    _cache[key] = (now, out)
    logger.info("HR NFL player props: %d thresholds across %d events",
                len(out), len(events))
    return out


# ── NFL player props v2: two-sided ladders (MintyBot 2026-10-26) ──
# Austin: "follow in the footsteps of NHL props" -> main line only, both sides,
# de-vigged + market-anchored. fetch_nfl_player_props above only kept the YES
# price of the ALT thresholds, and its regex wanted PROPALTRSYDS -- HR's rushing
# market is PROPALTRUYDS, so rushing yards was never priced. HR ships every NFL
# player stat twice: ALT threshold markets (PROPALT<STAT><N>, one market per N,
# Over/Under selections at N-0.5) and one MULTILINE market (selections typed
# "Over X" / "Under X" across many X). Both are merged per (player, stat, line)
# into the shared {matchup: [rows]} shape the NHL/NBA pickers and
# engine.player_props_clv already read (American over_odds / under_odds).
_NFL_PP2_ALT = _re_nflpp.compile(
    r"^AMERICAN_FOOTBALL:FT:PROPALT(PSYDS|RUYDS|RECYDS|RECEPTIONS|RSATT)(\d+)$")
_NFL_PP2_ML = _re_nflpp.compile(
    r"^AMERICAN_FOOTBALL:FT:PROP(PSYDS|RSYDS|RCYDS|REC|PSATT|PSCMP)MULTILINE$")
# per-line main O/U (subtype "...#14.5", selections typed Over / Under) -- added 2026-11-01
_NFL_PP2_MAIN = _re_nflpp.compile(r"^AMERICAN_FOOTBALL:FT:PROP(RSATT|RSPRCYDS)$")
_NFL_PP2_BT = {"PSYDS": "Passing Yards", "RUYDS": "Rushing Yards",
               "RSYDS": "Rushing Yards", "RECYDS": "Receiving Yards",
               "RCYDS": "Receiving Yards", "RECEPTIONS": "Receptions",
               "REC": "Receptions", "RSATT": "Rushing Attempts",
               "PSATT": "Pass Attempts", "PSCMP": "Completions",
               "RSPRCYDS": "Rush+Rec Yards"}
_NFL_PP2_SEL = _re_nflpp.compile(r"^(over|under)\s+(-?\d+(?:\.\d+)?)$", _re_nflpp.I)


def fetch_nfl_props() -> dict[str, list[dict]]:
    """HR NFL player props as two-sided lines, ``{"AWAY@HOME": [row, ...]}``.

    Row: ``player_name, bet_type, line, over_odds, under_odds, team_side``
    ('home'/'away' -- HR's A = home, B = away), ``hr_player_id, hr_market_id,
    hr_market_type, event_name, event_time`` (ms). Pre-match events only; a
    line may carry one side only (deep ladder rungs). Cached CACHE_TTL."""
    now = time.time()
    key = "nfl_props_v2"
    if key in _cache:
        ts, cached = _cache[key]
        if now - ts < (EMPTY_CACHE_TTL if not cached else CACHE_TTL):
            return cached
    payload = _fetch_raw(_NFL_COMP_ID)
    events = (payload.get("data", {}).get("betSync", {})
              .get("events", {}).get("data", []) or [])
    try:
        from engine.football._odds import _build_name_lookup, _resolve_team
        name_to_abbr = _build_name_lookup("nfl")
    except Exception as e:
        logger.warning("HR NFL props v2: team lookup unavailable: %s", e)
        return {}
    out: dict[str, list[dict]] = {}
    n_lines = 0
    for ev in events:
        if ev.get("inplay"):
            continue
        away_name, home_name = _extract_teams(ev)
        a = _resolve_team(away_name, name_to_abbr, "nfl")
        h = _resolve_team(home_name, name_to_abbr, "nfl")
        if not (a and h):
            continue
        lines: dict[tuple, dict] = {}
        for m in (ev.get("markets") or []):
            if m.get("state") != "OPEN":
                continue
            t = (m.get("type") or "").upper()
            ma = _NFL_PP2_ALT.match(t)
            mm = None if ma else _NFL_PP2_ML.match(t)
            mn = None if (ma or mm) else _NFL_PP2_MAIN.match(t)
            if not (ma or mm or mn):
                continue
            bet_type = _NFL_PP2_BT[(ma or mm or mn).group(1)]
            main_line = None
            if mn:
                _ml = re.search(r"#(-?\d+(?:\.\d+)?)", m.get("subtype") or "")
                if not _ml:
                    continue
                main_line = float(_ml.group(1))
            nm = m.get("name") or ""
            player = (nm.split(" - ")[0].strip() if " - " in nm
                      else _extract_player_name(nm))
            if not player:
                continue
            sub = m.get("subtype") or m.get("period") or ""
            parts = sub.split("#")[0].split(":")
            team_side = {"A": "home", "B": "away"}.get(parts[1] if len(parts) > 1 else "")
            hr_pid = parts[2] if len(parts) > 2 else None
            for sel in (m.get("selection") or []):
                od = _parse_odds_string(sel.get("odds"))
                if od is None:
                    continue
                if ma or mn:
                    st = (sel.get("type") or "").strip().lower()
                    if st not in ("over", "under"):
                        continue
                    side, line = st, (float(ma.group(2)) - 0.5 if ma else main_line)
                else:
                    mt = (_NFL_PP2_SEL.match((sel.get("type") or "").strip())
                          or _NFL_PP2_SEL.match((sel.get("name") or "").strip()))
                    if not mt:
                        continue
                    side, line = mt.group(1).lower(), float(mt.group(2))
                row = lines.get((player, bet_type, line))
                if row is None:
                    row = lines[(player, bet_type, line)] = {
                        "player_name": player, "bet_type": bet_type, "line": line,
                        "over_odds": None, "under_odds": None, "is_alt": False,
                        "team_side": team_side, "hr_player_id": hr_pid,
                        "hr_market_id": m.get("id"), "hr_market_type": t,
                        "event_name": ev.get("name") or "",
                        "event_time": ev.get("eventTime")}
                row["over_odds" if side == "over" else "under_odds"] = od
        if lines:
            out.setdefault(f"{a}@{h}", []).extend(lines.values())
            n_lines += len(lines)
    _cache[key] = (now, out)
    logger.info("HR NFL props v2: %d lines across %d games", n_lines, len(out))
    return out


__all__ = ["fetch_mlb_props", "fetch_nba_props", "fetch_nhl_props",
           "fetch_nfl_player_props", "fetch_nfl_props"]
