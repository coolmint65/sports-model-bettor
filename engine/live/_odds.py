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
    EVENT_TREE_URL, _load_query, _load_headers, _graphql_post,
    _walk_events_flat, _extract_teams, _team_abbr,
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
    """True iff this HR event's startTime is in the past — i.e. the
    game has already begun. Inverts engine.scrapers.hardrock_odds.
    _event_already_started's intent: prematch wants pre-tip events,
    live wants post-tip ones.

    A missing or unparseable startTime returns False so we don't
    accidentally treat a malformed prematch entry as live.
    """
    raw = event.get("startTime")
    if not isinstance(raw, str) or not raw:
        return False
    try:
        s = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
        ts = datetime.fromisoformat(s)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return False
    return ts < datetime.now(timezone.utc)


def _fetch_raw_payload() -> dict | None:
    """Single round-trip to HR with current session. Returns the raw
    GraphQL response body or None on transport failure."""
    body = _load_query()
    headers = _load_headers()
    status, raw_bytes, err = _graphql_post(EVENT_TREE_URL, body, headers,
                                           timeout=30)
    if status != 200 or not raw_bytes:
        logger.warning("HR live fetch: status=%s err=%s", status, err)
        return None
    try:
        import json
        return json.loads(raw_bytes.decode())
    except (UnicodeDecodeError, ValueError) as e:
        logger.warning("HR live fetch JSON decode failed: %s", e)
        return None


def fetch_live_odds(sport: str) -> dict[str, dict]:
    """Live odds dict for in-progress games of `sport`.

    Returns ``{"AWAY@HOME": {odds_home, odds_away, home_ml, away_ml,
    home_spread_point, over_under, alt_spreads, alt_totals, q1_*,
    period markers, ...}, ...}``. Empty dict when HR session is dead
    or no live games match.

    Filter applied:
      1. Sport name matches (NBA / NHL — others rejected by Phase 3 spec).
      2. event has already started (`startTime` < now UTC).
      3. event has at least one market with active selections.

    The returned odds dict is identical in shape to what prematch
    produces, so downstream consumers (engine.live._predict) don't
    care about provenance.
    """
    if sport not in ("nba", "nhl"):
        raise ValueError(f"live odds only cover NBA + NHL; got {sport!r}")

    raw = _fetch_raw_payload()
    if not raw:
        return {}

    # Re-use prematch's full-tree parser, then post-filter.
    # _parse_response keys results by AWAY@HOME (and HHMM-suffix
    # variants for doubleheaders, but NBA/NHL don't have those today).
    full = _parse_response(sport, raw)

    # Walk the raw event list once more so we can match started-events
    # against the parsed dict (the parser dropped the startTime field).
    started_keys: set[str] = set()
    for event in _walk_events_flat(raw):
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

    if not started_keys:
        return {}

    out: dict[str, dict] = {}
    for k, v in full.items():
        # Drop HHMM suffix variants — NBA/NHL don't run doubleheaders.
        # Keep only the plain "AWAY@HOME" form for live consumers.
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
    raw = _fetch_raw_payload()
    if not raw:
        return None
    target = str(event_id)
    for event in _walk_events_flat(raw):
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
