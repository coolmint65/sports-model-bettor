"""
Hard Rock live-markets fetch.

The HR BetSync GraphQL endpoint at scrapers.hardrock_odds returns
BOTH prematch and in-progress events in the same payload. The prematch
scraper filters out started events via `_event_already_started`. For
live betting we invert that filter — keep only events that have
already tipped off / dropped the puck.

Why we don't query a different endpoint: HR doesn't expose a separate
"live-only" GraphQL path. Both surfaces consume the same `betSync`
tree; the categorization is per-event by `startTime`. Reusing the
prematch transport (cookies, headers, _graphql_post retry logic)
means one auth flow covers both engines.

Per-event output mirrors the prematch parsing — same odds dict shape
(home_ml, away_ml, home_spread_point, over_under, alt_spreads,
alt_totals, q1_*, etc.). The conditional predictor in 3b reads the
same shape regardless of whether the source was prematch or live.

Public API:
    fetch_live_odds(sport)            — returns dict keyed by f"{AWAY}@{HOME}"
                                         containing only in-progress games
    fetch_live_odds_for_event(sport, event_id)
                                       — single-game lookup

Both are cheap reads against the existing HR session. The worker
calls fetch_live_odds() once per tick per sport, then cross-references
with the live state from _state.py so each in-progress game gets
both halves (state + live odds) in the store.
"""

from __future__ import annotations
import logging
from datetime import datetime, timezone

from scrapers.hardrock_odds import (
    _fetch_with_markets, _walk_events_flat, _extract_teams, _team_abbr,
    _parse_response,  # full parse shared with prematch
)

logger = logging.getLogger(__name__)

# Sport names HR uses internally vs our internal codes.
_HR_SPORT_NAMES = {
    "nba": ("Basketball", "NBA"),
    "nhl": ("Hockey", "NHL"),
    "mlb": ("Baseball", "MLB"),  # exposed but not used by Phase 3
}


def _event_started(event: dict) -> bool:
    """True iff this HR event has tipped off / dropped the puck.

    Mirrors `scrapers.hardrock_odds._event_already_started`. Priority:
      1. ``inplay`` boolean (new schema) — authoritative.
      2. ``trackerStatus`` string ("LIVE" vs "NOT_STARTED") — secondary.
      3. ``eventTime`` / ``startTime`` — numeric epoch-ms or ISO string;
         past timestamp == game has nominally started.

    The prematch path drops these; the live path *only* keeps these.
    """
    inplay = event.get("inplay")
    if isinstance(inplay, bool):
        return inplay

    tracker = event.get("trackerStatus")
    if isinstance(tracker, str) and tracker:
        t = tracker.strip().upper()
        if t in ("LIVE", "IN_PLAY", "INPLAY", "STARTED"):
            return True
        if t in ("NOT_STARTED", "NOT STARTED", "PRE", "PREMATCH"):
            return False

    now = datetime.now(timezone.utc)
    for field in ("startTime", "eventTime"):
        raw = event.get(field)
        if isinstance(raw, str) and raw:
            try:
                s = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
                ts = datetime.fromisoformat(s)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                return ts < now
            except (ValueError, TypeError):
                continue
        if isinstance(raw, (int, float)) and raw > 0:
            try:
                ts = datetime.fromtimestamp(float(raw) / 1000.0, tz=timezone.utc)
                return ts < now
            except (ValueError, TypeError, OSError):
                continue
    return False


def _fetch_raw_payload(sport: str) -> dict | None:
    """Two-phase fetch (sports tree → per-comp events). Returns the
    synthetic legacy-shaped tree built by ``_fetch_with_markets``, or
    None on transport failure. Sport is required to scope the comp
    lookup — passing a non-NBA/NHL sport here is an error elsewhere
    in the live stack."""
    data, err = _fetch_with_markets(sport)
    if err:
        logger.warning("HR live fetch (%s): %s", sport, err)
        return None
    return data


def _iter_events_from_synthetic(tree: dict):
    """Yield each event dict from the synthetic-tree shape that
    ``_fetch_with_markets`` builds — ``data.betSync.sports[].
    competitions[].events.data[]``."""
    if not isinstance(tree, dict):
        return
    sports = tree.get("data", {}).get("betSync", {}).get("sports") or []
    for sp in sports:
        if not isinstance(sp, dict):
            continue
        for comp in sp.get("competitions") or []:
            if not isinstance(comp, dict):
                continue
            data = (comp.get("events") or {}).get("data") or []
            for ev in data:
                if isinstance(ev, dict):
                    yield ev


def fetch_live_odds(sport: str) -> dict[str, dict]:
    """Live odds dict for in-progress games of `sport`.

    Returns ``{"AWAY@HOME": {odds_home, odds_away, home_ml, away_ml,
    home_spread_point, over_under, alt_spreads, alt_totals, q1_*,
    period markers, ...}, ...}``. Empty dict when HR session is dead
    or no live games match.

    Filter applied:
      1. Sport name matches (NBA / NHL — others rejected by Phase 3 spec).
      2. event is in-play (HR ``inplay`` flag, with eventTime fallback).
      3. event has at least one market with active selections.

    The returned odds dict is identical in shape to what prematch
    produces, so downstream consumers (engine.live._predict) don't
    care about provenance.
    """
    if sport not in ("nba", "nhl", "wnba", "ncaam", "afl"):
        raise ValueError(
            f"live odds only cover NBA/NHL/WNBA/NCAAM/AFL; got {sport!r}")

    raw = _fetch_raw_payload(sport)
    if not raw:
        return {}

    # Re-use prematch's full-tree parser, then post-filter.
    # _parse_response keys results by AWAY@HOME (and HHMM-suffix
    # variants for doubleheaders, but NBA/NHL don't have those today).
    # Note: _parse_response drops events where _event_already_started
    # returns True — exactly the events we want here. Run a dedicated
    # walk over the synthetic tree to capture the inplay set first.
    started_keys: set[str] = set()
    inplay_events: list[dict] = []
    for event in _iter_events_from_synthetic(raw):
        if not _event_started(event):
            continue
        away_name, home_name = _extract_teams(event)
        if not (away_name and home_name):
            continue
        a = _team_abbr(sport, away_name)
        h = _team_abbr(sport, home_name)
        if not (a and h):
            continue
        started_keys.add(f"{a}@{h}")
        inplay_events.append(event)

    if not inplay_events:
        return {}

    # Synthesize a tree containing ONLY the inplay events so the parser
    # (which drops started events for prematch) instead processes them.
    # Easiest way: bypass _event_already_started by feeding events that
    # report inplay=False to the parser — but we want the prematch
    # filter intact. Cleaner: parse a copy of the tree where inplay is
    # rewritten to False on the games we want kept. But mutating the
    # cached HR response is risky.
    #
    # Simplest correct approach: build per-event odds dicts inline using
    # the same parser, but feed each as a one-event tree with inplay
    # cleared.
    out: dict[str, dict] = {}
    sport_code = {
        "nba": "BASKETBALL",
        "nhl": "ICE_HOCKEY",
        "wnba": "BASKETBALL",
        "ncaam": "BASKETBALL",
        "afl": "AUSSIE_RULES",
    }[sport]
    for ev in inplay_events:
        ev_copy = dict(ev)
        ev_copy["inplay"] = False
        ev_copy.pop("trackerStatus", None)
        synthetic = {
            "data": {
                "betSync": {
                    "sports": [{
                        "id": "live",
                        "code": sport_code,
                        "competitions": [{
                            "id": ev.get("compId") or "live",
                            "name": ev.get("compName") or "",
                            "events": {"data": [ev_copy], "count": 1},
                        }],
                    }]
                }
            }
        }
        parsed = _parse_response(sport, synthetic)
        for k, v in parsed.items():
            if k.count("@") != 1:
                continue
            if k in started_keys:
                out[k] = v
    return out


def fetch_live_odds_for_event(sport: str, event_id: str) -> dict | None:
    """Single-game live odds lookup. Returns the same per-event dict
    shape that fetch_live_odds yields, or None if the event isn't
    currently in HR's live tree.

    Used when the worker wants to refresh ONE game (e.g. after a state
    transition like "Q1 just ended") without paying for a full slate
    fetch. For now this still pulls the full tree because HR doesn't
    expose a per-event GraphQL filter; we just isolate the match
    afterwards. If HR ever ships an event-scoped query we'll route it
    through here.
    """
    if not event_id:
        return None
    raw = _fetch_raw_payload(sport)
    if not raw:
        return None
    target = str(event_id)
    for event in _iter_events_from_synthetic(raw):
        eid = str(event.get("id") or event.get("eventId") or "")
        if eid != target:
            continue
        if not _event_started(event):
            return None
        # Funnel through _parse_response by isolating this single event
        # within the same tree shape. Cheaper to reuse fetch_live_odds
        # than rebuild the per-event parser.
        full = fetch_live_odds(sport)
        away_name, home_name = _extract_teams(event)
        if not (away_name and home_name):
            return None
        a = _team_abbr(sport, away_name)
        h = _team_abbr(sport, home_name)
        return full.get(f"{a}@{h}")
    return None
