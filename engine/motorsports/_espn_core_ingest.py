"""ESPN core.api ingest for NASCAR (Cup) + IndyCar.

Complements ``_ergast_ingest`` which handles F1 via the Ergast/Jolpi
API. ESPN's site.api doesn't carry NASCAR / IndyCar summary payloads,
but the deeper ``sports.core.api.espn.com/v2/sports/racing/leagues/
{slug}/events`` endpoint has full event lists per season with embedded
competitor arrays (order + winner flag + athlete ref).

Two-step ingest per season:
  1. Fetch event list → for each event, fetch event detail → walk
     competitors array (embedded, no per-competitor requests).
  2. Athletes are fetched on demand — one HTTP call per unique
     driver, cached in-memory across events + across series in the
     same process.

Both series share the schema in ``_db.py`` (races, drivers,
race_results). Same downstream predictor + odds pipeline.

Public:
    ingest_season(series, season) -> {races, results, drivers}
    backfill(series, start_season, end_season) -> {seasons, ...}
    ingest_today(series) -> results for current-year races
"""
from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

from ._config import get_series_config
from ._db import get_conn

logger = logging.getLogger(__name__)


_CORE_BASE = "https://sports.core.api.espn.com/v2/sports/racing/leagues"
_UA = {"User-Agent": "Mozilla/5.0"}
_ATHLETE_CACHE: dict[str, dict] = {}


def _fetch(url: str, retries: int = 3, timeout: float = 15.0) -> dict | None:
    """GET a JSON payload with retry/backoff. Returns None on final
    failure so callers can skip cleanly rather than crash the season."""
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, headers=_UA),
                timeout=timeout,
            ) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                # Bad endpoint / no data for the query — retrying won't
                # change the outcome. Bubble up as None.
                logger.debug("ESPN core %s: %s", url, e)
                return None
            wait = 2 ** attempt
            logger.debug("ESPN core %s HTTP %d, retry in %ds", url, e.code, wait)
            time.sleep(wait)
        except Exception as e:
            wait = 2 ** attempt
            logger.debug("ESPN core %s error %s, retry in %ds", url, e, wait)
            time.sleep(wait)
    return None


def _athlete_id_from_ref(ref: str) -> str | None:
    """Pull the athlete id out of a $ref URL like
    ``.../racing/athletes/4574?lang=en&region=us``. ESPN uses stable
    integer ids so we can dedupe across every race in the series."""
    if not ref:
        return None
    tail = ref.split("athletes/")[-1]
    aid = tail.split("?")[0].split("/")[0]
    return aid or None


def _fetch_athlete(aid: str) -> dict | None:
    """One HTTP call per driver, then cached. Process-lifetime cache
    keeps a full-season backfill under 40-50 unique athlete requests
    for NASCAR / 25-30 for IndyCar."""
    if not aid:
        return None
    cached = _ATHLETE_CACHE.get(aid)
    if cached is not None:
        return cached
    url = f"https://sports.core.api.espn.com/v2/sports/racing/athletes/{aid}"
    data = _fetch(url)
    if data:
        _ATHLETE_CACHE[aid] = data
    return data


def _upsert_team(conn, name: str) -> int | None:
    """Upsert a racing team (owner-team for NASCAR / IndyCar). Uses the
    team display name as the ergast_id key so re-runs are idempotent
    even before we ever get an ESPN team id."""
    if not name:
        return None
    key = name.strip()
    if not key:
        return None
    row = conn.execute(
        "SELECT id FROM teams WHERE ergast_id = ? OR name = ?",
        (key, key),
    ).fetchone()
    if row:
        return int(row["id"])
    # Short abbreviation from the team name — first letters of each
    # word, up to 4 chars ("Joe Gibbs Racing" → "JGR").
    tokens = [t for t in key.split() if t and t[0].isalpha()]
    abbr = "".join(t[0].upper() for t in tokens)[:4] or key[:3].upper()
    cur = conn.execute(
        "INSERT INTO teams (name, abbreviation, ergast_id) "
        "VALUES (?, ?, ?)",
        (key, abbr, key),
    )
    return int(cur.lastrowid)


def _upsert_driver(conn, athlete: dict,
                    team_id: int | None = None) -> int | None:
    """Insert / merge a driver row. Uses ESPN athlete id as the stable
    ``ergast_id`` since the framework's driver identifier is source-
    agnostic — Ergast id for F1, ESPN athlete id for NASCAR / IndyCar.
    Enriches nationality from ``athlete.flag.alt`` (ESPN's country tag)
    and links to the current team when supplied."""
    if not athlete:
        return None
    aid = str(athlete.get("id") or "").strip()
    if not aid:
        return None
    name = (athlete.get("fullName") or athlete.get("displayName")
             or f"{athlete.get('firstName','')} {athlete.get('lastName','')}"
             ).strip()
    if not name:
        return None
    # 3-letter code — ESPN doesn't ship one, so derive from lastName
    # (e.g. "ELLIOTT" → "ELL"). Falls back to first three letters of
    # displayName. Sport-specific naming (like NASCAR's Jr./III) is
    # normalized by picking the leading capitalized tokens.
    last = (athlete.get("lastName") or "").strip().upper()
    if not last and " " in name:
        last = name.rsplit(" ", 1)[1].strip().upper()
    abbr = (last[:3] if last else name[:3].upper()).strip()
    # Nationality: ESPN ships a ``flag.alt`` field on athletes with the
    # 3-letter ISO country tag (e.g. "USA", "BRA"). Fall back to
    # birthPlace.country when flag is absent (some series omit the flag
    # block on older athletes).
    flag = athlete.get("flag") or {}
    nationality = (flag.get("alt") or "").strip()
    if not nationality:
        bp = athlete.get("birthPlace") or {}
        nationality = (bp.get("country") or "").strip()
    row = conn.execute(
        "SELECT id, name, abbreviation, team_id, nationality "
        "FROM drivers WHERE ergast_id = ?",
        (aid,),
    ).fetchone()
    if row:
        # Refresh mutable fields (name, abbreviation, team, nationality)
        # if ESPN updated them since the last ingest. Team changes
        # mid-season are common in NASCAR / IndyCar.
        conn.execute(
            "UPDATE drivers SET name = ?, abbreviation = ?, "
            "team_id = COALESCE(?, team_id), "
            "nationality = COALESCE(NULLIF(?, ''), nationality) "
            "WHERE ergast_id = ?",
            (name, abbr, team_id, nationality, aid),
        )
        return int(row["id"])
    cur = conn.execute(
        "INSERT INTO drivers (name, abbreviation, team_id, nationality, "
        "                     ergast_id) VALUES (?, ?, ?, ?, ?)",
        (name, abbr, team_id, nationality or None, aid),
    )
    return int(cur.lastrowid)


def _upsert_race(conn, series: str, season: int, ev: dict,
                  round_num: int) -> str | None:
    """Persist one race row keyed by ``{season}-{round}``. Copies
    ergast_id / hr_event_id columns even though NASCAR uses ESPN event
    ids — keeps the join surface uniform with F1."""
    ev_id = str(ev.get("id") or "")
    if not ev_id:
        return None
    race_id = f"{season}-{round_num:02d}"
    date = (ev.get("date") or "")[:10]
    name = ev.get("name") or ev.get("shortName") or f"Race {round_num}"

    # Venue: try the first venue on the event; core.api often ships an
    # empty venue on the event root but a populated one on the
    # competition. Bail cleanly when neither has anything.
    venues = ev.get("venues") or []
    venue_name = ""
    country = ""
    if venues:
        v = venues[0]
        # v may be a $ref or an inline dict
        if isinstance(v, dict) and "$ref" in v and "fullName" not in v:
            v = _fetch(v["$ref"]) or {}
        venue_name = (v or {}).get("fullName") or ""
        addr = (v or {}).get("address") or {}
        country = addr.get("country") or addr.get("countryCode") or ""

    # ESPN core.api ships event.status as None on completed races and
    # keeps the flag under competition.status.$ref (one extra HTTP
    # hop). Rather than paying that per-race, infer completion from
    # the finisher data: any race with a competitor marked ``winner``
    # is done. Falls back to date-in-the-past for edge cases where the
    # winner tag hasn't populated yet.
    comps_for_status = ((ev.get("competitions") or [{}])[0]
                         .get("competitors") or [])
    has_winner = any(
        (c.get("winner") if isinstance(c, dict) else False)
        for c in comps_for_status
    )
    past = bool(date) and date < datetime.now().strftime("%Y-%m-%d")
    status = "complete" if (has_winner or past) else "scheduled"

    conn.execute(
        "INSERT INTO races (race_id, season, round, name, circuit, "
        "                    country, race_date, race_time, status, "
        "                    ergast_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(race_id) DO UPDATE SET "
        "  name = excluded.name, circuit = excluded.circuit, "
        "  country = excluded.country, race_date = excluded.race_date, "
        "  race_time = excluded.race_time, status = excluded.status",
        (race_id, season, round_num, name, venue_name, country, date,
         ev.get("date") or "", status, ev_id),
    )
    return race_id


def _persist_results(conn, race_id: str, comps: list[dict]) -> int:
    """Walk the embedded competitors array + upsert race_results rows.
    Returns the count of results written."""
    n = 0
    for c in comps:
        # c is either an inline dict or a $ref that we need to expand.
        # The competitors LIST at competition.competitors already has
        # order/winner inline (verified 2026-07-03) so per-competitor
        # requests aren't needed.
        if isinstance(c, dict) and "order" not in c and "$ref" in c:
            c = _fetch(c["$ref"]) or {}
        order = c.get("order")
        winner = 1 if c.get("winner") else 0
        athlete_ref = c.get("athlete")
        if isinstance(athlete_ref, dict):
            aid = _athlete_id_from_ref(athlete_ref.get("$ref") or "")
        else:
            aid = _athlete_id_from_ref(athlete_ref) if athlete_ref else None
        if not aid:
            continue
        athlete = _fetch_athlete(aid)
        # Vehicle block carries the racing team + manufacturer + car
        # number for NASCAR / IndyCar. F1 events don't ship this — the
        # Ergast ingest still owns constructor data for that series.
        vehicle = c.get("vehicle") or {}
        team_name = (vehicle.get("team") or "").strip()
        team_id = _upsert_team(conn, team_name) if team_name else None
        driver_id = _upsert_driver(conn, athlete or {"id": aid},
                                     team_id=team_id)
        if driver_id is None:
            continue
        finish_pos = int(order) if order is not None else None
        conn.execute(
            "INSERT INTO race_results (race_id, driver_id, team_id, "
            "                          qualifying_pos, finish_pos, status) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(race_id, driver_id) DO UPDATE SET "
            "  team_id = COALESCE(excluded.team_id, race_results.team_id), "
            "  finish_pos = excluded.finish_pos, "
            "  qualifying_pos = COALESCE(excluded.qualifying_pos, "
            "                             race_results.qualifying_pos)",
            (
                race_id, driver_id, team_id,
                int(c.get("startOrder")) if c.get("startOrder") else None,
                finish_pos,
                "Winner" if winner else ("Finished" if finish_pos else None),
            ),
        )
        n += 1
    return n


def _list_events(series: str, season: int) -> list[dict]:
    cfg = get_series_config(series)
    slug = cfg.get("espn_league_slug")
    if not slug:
        return []
    url = f"{_CORE_BASE}/{slug}/events?dates={season}&limit=100"
    data = _fetch(url)
    return (data or {}).get("items") or []


def ingest_season(series: str, season: int, *,
                   throttle_s: float = 0.15) -> dict:
    """Backfill one season's races + results for ``series``.

    Args:
        series: 'nascar' or 'indycar'.
        season: 4-digit year.
        throttle_s: sleep between event fetches — keeps ESPN happy on
                    a full-history backfill (~40 events × 3 seasons).

    Returns totals: ``{races, results, drivers, skipped}``.
    """
    cfg = get_series_config(series)
    slug = cfg.get("espn_league_slug")
    if not slug:
        return {"error": f"{series!r} has no espn_league_slug"}

    conn = get_conn(series)
    items = _list_events(series, season)
    if not items:
        return {"races": 0, "results": 0, "drivers": 0,
                "skipped": 0, "reason": "no events for season"}

    totals = {"races": 0, "results": 0, "drivers": 0, "skipped": 0}
    # Sort by date so `round` numbering is chronological (ESPN's
    # unordered event list would otherwise mint round=1 for whichever
    # race sits atop the JSON blob).
    events_with_dates: list[tuple[dict, str]] = []
    for item in items:
        ev = _fetch(item["$ref"])
        if ev and ev.get("date"):
            events_with_dates.append((ev, ev["date"]))
    events_with_dates.sort(key=lambda p: p[1])

    for round_num, (ev, _date) in enumerate(events_with_dates, start=1):
        race_id = _upsert_race(conn, series, season, ev, round_num)
        if not race_id:
            totals["skipped"] += 1
            continue
        totals["races"] += 1
        # Competition + competitors — the results source of truth.
        comps_list = (ev.get("competitions") or [{}])[0].get("competitors") or []
        n_res = _persist_results(conn, race_id, comps_list)
        totals["results"] += n_res
        time.sleep(throttle_s)
    conn.commit()
    totals["drivers"] = conn.execute(
        "SELECT COUNT(*) FROM drivers"
    ).fetchone()[0]
    logger.info("[motorsports:%s] season %d ingested: %s",
                 series, season, totals)
    return totals


def backfill(series: str, start_season: int, end_season: int,
              *, throttle_s: float = 0.15) -> dict:
    """Chronological backfill across seasons. Idempotent — safe to
    re-run against a partially populated DB."""
    all_totals = {"races": 0, "results": 0, "skipped": 0,
                   "drivers": 0, "seasons": 0}
    for season in range(start_season, end_season + 1):
        sub = ingest_season(series, season, throttle_s=throttle_s)
        for k in ("races", "results", "skipped"):
            all_totals[k] += sub.get(k, 0)
        if sub.get("races"):
            all_totals["seasons"] += 1
    all_totals["drivers"] = get_conn(series).execute(
        "SELECT COUNT(*) FROM drivers"
    ).fetchone()[0]
    return all_totals


def ingest_today(series: str) -> dict:
    """Refresh the current season only — meant for the worker's hourly
    tick. Cheaper than a full backfill; picks up newly-final races +
    any next-race schedule adjustments."""
    return ingest_season(series, datetime.now().year, throttle_s=0.1)
