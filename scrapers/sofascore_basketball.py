"""SofaScore basketball results fallback.

RealGM publishes finals 12-36h after games end for small leagues, which
strands pending picks (e.g. NZ NBL 2026-05-13: Manawatu Jets beat
Franklin Bulls 112-103 hours after pick id=16 fired on FRA -4.5, but
RealGM still showed status='pre'). SofaScore's unauthed JSON API serves
same-day finals and works via curl_cffi/Firefox impersonation, so we
wire it in as a secondary results source.

Scope is deliberately narrow:
    fetch_results_for_date(tournament_id, date) -> list of dicts

Output shape mirrors ``scrapers.realgm_basketball.fetch_schedule_for_date``
so the basketball framework's ingest can treat both feeds the same way.

Anti-bot: SofaScore sits behind Cloudflare. urllib gets 403'd on JA3
fingerprint mismatch; curl_cffi with firefox133 impersonation clears it
(same fix used for Hard Rock — see scrapers.hardrock_odds._graphql_post).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

try:
    from curl_cffi import requests as _cc_requests  # type: ignore
    _HAS_CURL_CFFI = True
except ImportError:
    _HAS_CURL_CFFI = False
    _cc_requests = None

logger = logging.getLogger(__name__)

_BASE = "https://api.sofascore.com/api/v1"
_TIMEOUT_S = 15.0


def _get_json(path: str) -> dict | None:
    """Fetch ``path`` (full URL or path under _BASE) and return parsed
    JSON. Returns None on any error. Quiet on success."""
    if not _HAS_CURL_CFFI:
        logger.warning("sofascore: curl_cffi not installed — fallback disabled")
        return None
    url = path if path.startswith("http") else f"{_BASE}{path}"
    try:
        r = _cc_requests.get(url, impersonate="firefox133",
                              timeout=_TIMEOUT_S)
    except Exception as e:
        logger.debug("sofascore GET %s crashed: %s", url, e)
        return None
    if r.status_code != 200:
        logger.debug("sofascore GET %s -> HTTP %s", url, r.status_code)
        return None
    try:
        return r.json()
    except Exception as e:
        logger.debug("sofascore parse %s: %s", url, e)
        return None


# Map SofaScore status types to our internal taxonomy.
_STATUS_MAP = {
    "finished": "final",
    "notstarted": "pre",
    "inprogress": "in_progress",
    "postponed": "postponed",
    "canceled": "cancelled",
    "willcontinue": "in_progress",
}


def _normalize_event(event: dict) -> dict | None:
    """Translate one SofaScore event blob into our results-row shape."""
    try:
        home = (event.get("homeTeam") or {}).get("name")
        away = (event.get("awayTeam") or {}).get("name")
        if not home or not away:
            return None
        ts = event.get("startTimestamp")
        # SofaScore timestamps are UTC seconds. Coerce to YYYY-MM-DD by
        # the start-of-game UTC date — callers refetch a window so a
        # tz-offset to ET would shift games into the wrong day.
        date_iso = (datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                    if ts else None)
        status_raw = ((event.get("status") or {}).get("type") or "").lower()
        status = _STATUS_MAP.get(status_raw, status_raw or "pre")
        hs_obj = event.get("homeScore") or {}
        as_obj = event.get("awayScore") or {}
        hs = hs_obj.get("current")
        as_ = as_obj.get("current")

        def _pq(side: dict, n: int):
            v = side.get(f"period{n}")
            try:
                return int(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        return {
            "date": date_iso,
            "away": away,
            "home": home,
            "away_score": int(as_) if as_ is not None else None,
            "home_score": int(hs) if hs is not None else None,
            "home_q1": _pq(hs_obj, 1),
            "home_q2": _pq(hs_obj, 2),
            "home_q3": _pq(hs_obj, 3),
            "home_q4": _pq(hs_obj, 4),
            "away_q1": _pq(as_obj, 1),
            "away_q2": _pq(as_obj, 2),
            "away_q3": _pq(as_obj, 3),
            "away_q4": _pq(as_obj, 4),
            "status": status,
            "venue": None,
            "source": "sofascore",
            "event_id": event.get("id"),
        }
    except Exception as e:
        logger.debug("sofascore normalize crashed: %s", e)
        return None


def _current_season_id(tournament_id: int) -> int | None:
    """The most recent season ID for ``tournament_id`` per SofaScore's
    /seasons endpoint. Result is unstable across years (SofaScore mints
    a fresh ID per season), so we look it up each ingest pass — cheap
    JSON fetch, no observed rate limit at this cadence.

    SofaScore returns seasons in DESCENDING recency order; the first
    entry is the live/current season. Returns None if the endpoint is
    empty (off-season or unknown tournament)."""
    data = _get_json(f"/unique-tournament/{int(tournament_id)}/seasons")
    if not data:
        return None
    seasons = data.get("seasons") or []
    if not seasons:
        return None
    try:
        return int(seasons[0].get("id"))
    except (TypeError, ValueError):
        return None


def _events_for_tournament(tournament_id: int,
                            pages: int = 2) -> list[dict]:
    """Fetch the most recent ``pages`` pages of finished events for
    ``tournament_id``. SofaScore's per-tournament events endpoint
    requires a season id — we resolve it via ``/seasons``.

    Returns the raw event blobs; callers normalize with ``_normalize_event``."""
    season_id = _current_season_id(tournament_id)
    if not season_id:
        return []
    out: list[dict] = []
    for page in range(int(pages)):
        data = _get_json(
            f"/unique-tournament/{int(tournament_id)}/season/{season_id}"
            f"/events/last/{page}"
        )
        if not data:
            break
        page_events = data.get("events") or []
        if not page_events:
            break
        out.extend(page_events)
        # Stop walking pages once we've gathered enough recency; the
        # ingest window is 3 days and SofaScore returns ~30 events per
        # page, so one page is almost always sufficient.
        if len(page_events) < 25:
            break
    return out


def fetch_results_for_date(tournament_id: int,
                            date_str: str) -> list[dict]:
    """All events for ``tournament_id`` whose start-day matches ``date_str``.

    Returns rows in the same shape as ``realgm_basketball`` so the
    framework ingest can merge them directly into the games table."""
    if not tournament_id:
        return []
    events = _events_for_tournament(tournament_id)
    out: list[dict] = []
    seen = set()
    for ev in events:
        norm = _normalize_event(ev)
        if not norm or norm["date"] != date_str:
            continue
        # Dedup on (date, away, home) since pagination can overlap when
        # the league plays daily.
        k = (norm["date"], norm["away"], norm["home"])
        if k in seen:
            continue
        seen.add(k)
        out.append(norm)
    return out


def fetch_teams(tournament_id: int) -> dict[int, str]:
    """Every team in ``tournament_id``'s current season, as
    ``{sofa_team_id: team_name}``. Two-pass:

      1. ``/standings/total`` — covers every team currently registered
         in the season (typically 12-20 teams for basketball leagues).
      2. Fallback: walk the most-recent events when standings is empty
         (off-season window, knockout-only tournaments that don't
         publish a table).

    Used by the logo-backfill path — SofaScore exposes a stable logo
    URL at ``/api/v1/team/{id}/image`` for every team id, so once we
    have the id we can persist the URL into our teams table without
    per-team scraping. Returns an empty dict on full failure."""
    if not tournament_id:
        return {}
    season_id = _current_season_id(tournament_id)
    teams: dict[int, str] = {}
    if season_id:
        standings = _get_json(
            f"/unique-tournament/{int(tournament_id)}/season/{season_id}"
            f"/standings/total"
        )
        if standings:
            for grp in (standings.get("standings") or []):
                for row in (grp.get("rows") or []):
                    t = row.get("team") or {}
                    tid = t.get("id")
                    tname = t.get("name")
                    if tid and tname:
                        teams[int(tid)] = str(tname)
    # Fallback / supplement: events walk picks up teams that have played
    # recent games but aren't on the standings (newly-promoted or playoff
    # qualifiers that bypass the regular-season table).
    events = _events_for_tournament(tournament_id, pages=2)
    for ev in events:
        for side in ("homeTeam", "awayTeam"):
            t = ev.get(side) or {}
            tid = t.get("id")
            tname = t.get("name")
            if tid and tname and tid not in teams:
                teams[int(tid)] = str(tname)
    return teams


def team_logo_url(sofa_team_id: int) -> str:
    """Stable SofaScore CDN URL for a team's logo PNG. Returns the path
    even when the team id is bogus — caller's responsibility to verify
    via HEAD/GET if they care about 404 distinction."""
    return f"{_BASE}/team/{int(sofa_team_id)}/image"


def search_team(name: str, *, sport: str = "Basketball",
                 country: str | None = None) -> dict | None:
    """Lookup a team by name via SofaScore's search endpoint. Used as
    a fallback when the league's standings + recent events don't carry
    the team (relegated / inactive / lower-division clubs that we still
    track in our DB).

    Filters results to ``sport`` exactly (so a search for "Zarate"
    doesn't return a tennis player's profile). When ``country`` is
    provided, also filters to category.name == country.

    Returns the first matching team dict or None."""
    if not name:
        return None
    import urllib.parse
    q = urllib.parse.quote(name)
    data = _get_json(f"/search/teams/{q}")
    if not data:
        return None
    hits = data.get("teams") or []
    for t in hits:
        sp = (t.get("sport") or {}).get("name", "")
        if sp != sport:
            continue
        if country:
            cat = (t.get("category") or {}).get("name", "")
            if cat and cat.lower() != country.lower():
                continue
        return t
    return None


def fetch_history_for_season(tournament_id: int, season_id: int,
                              *, max_pages: int = 25,
                              page_throttle: float = 0.4) -> list[dict]:
    """Walk every finished event in one (tournament, season). Paginates
    /events/last/{N} until SofaScore returns an empty page or we exceed
    ``max_pages``. Each page is ~30 events; the cap of 25 covers a
    400-game regular season + playoffs comfortably.

    ``page_throttle`` sleeps between page fetches — SofaScore silently
    drops bursts of >5 same-IP requests/sec, so a small delay keeps the
    walker reliable.
    """
    import time as _time
    out: list[dict] = []
    seen = set()
    for page in range(int(max_pages)):
        if page > 0 and page_throttle > 0:
            _time.sleep(page_throttle)
        data = _get_json(
            f"/unique-tournament/{int(tournament_id)}/season/{int(season_id)}"
            f"/events/last/{page}"
        )
        if not data:
            break
        page_events = data.get("events") or []
        if not page_events:
            break
        for ev in page_events:
            norm = _normalize_event(ev)
            if not norm or not norm["date"]:
                continue
            k = (norm["date"], norm["away"], norm["home"])
            if k in seen:
                continue
            seen.add(k)
            out.append(norm)
        if len(page_events) < 25:
            break
    return out


def list_seasons(tournament_id: int) -> list[dict]:
    """All seasons SofaScore tracks for ``tournament_id``, most-recent
    first. Returns rows with id + name + year."""
    data = _get_json(f"/unique-tournament/{int(tournament_id)}/seasons")
    if not data:
        return []
    out = []
    for s in (data.get("seasons") or []):
        try:
            out.append({"id": int(s.get("id")),
                        "year": s.get("year"),
                        "name": s.get("name")})
        except (TypeError, ValueError):
            continue
    return out


def fetch_results_window(tournament_id: int, days_back: int = 3) -> list[dict]:
    """All finished events for ``tournament_id`` over the trailing
    ``days_back`` days. Single fetch (vs N per-date calls) when the
    caller wants a sweep instead of a single-day pull.

    Used by ingest_today's fallback path: cheaper than calling
    fetch_results_for_date once per pending date when most leagues only
    have 1-2 days of stuck results."""
    if not tournament_id:
        return []
    events = _events_for_tournament(tournament_id)
    if not events:
        return []
    today = datetime.utcnow()
    earliest = (today - timedelta(days=int(days_back))).strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")
    out: list[dict] = []
    seen = set()
    for ev in events:
        norm = _normalize_event(ev)
        if not norm or not norm["date"]:
            continue
        if norm["date"] < earliest or norm["date"] > today_str:
            continue
        k = (norm["date"], norm["away"], norm["home"])
        if k in seen:
            continue
        seen.add(k)
        out.append(norm)
    return out
