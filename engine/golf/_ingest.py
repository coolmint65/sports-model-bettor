"""ESPN ingest for the golf framework.

Pulls scoreboards (today + upcoming tournaments) and per-event
summaries (full field + per-round scores) from ESPN's public golf API.
No auth needed; same shape as the team-sport ESPN ingest used
elsewhere in the codebase.

Public entry points:
    ingest_today(tour)             — refresh schedule + active leaderboards
    ingest_summary(tour, event_id) — pull one event's full field
    backfill(tour, year)           — historical sweep for skill rating
"""
from __future__ import annotations

import http.client
import json
import logging
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

from ._config import get_tour_config
from ._db import get_conn

logger = logging.getLogger(__name__)

_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"
_REQUEST_INTERVAL_S = 0.25


def _fetch_json(url: str, timeout: int = 20) -> dict | None:
    """One retry on transient errors. Returns None on permanent failure
    so callers can fall through without blowing up the slate."""
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "sports-model-bettor/golf",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if 500 <= e.code < 600 and attempt == 1:
                time.sleep(1.0)
                continue
            logger.warning("ESPN golf HTTP %s for %s", e.code, url)
            return None
        except (urllib.error.URLError, TimeoutError,
                http.client.HTTPException, OSError) as e:
            if attempt == 1:
                time.sleep(1.0)
                continue
            logger.warning("ESPN golf network error: %s", e)
            return None
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("ESPN golf bad JSON: %s", e)
            return None
    return None


# ── Status mapping ────────────────────────────────────────────

_STATUS_MAP = {
    "STATUS_SCHEDULED": "scheduled",
    "STATUS_IN_PROGRESS": "in",
    "STATUS_FINAL": "final",
    "STATUS_CANCELED": "cancelled",
    "STATUS_POSTPONED": "postponed",
    "STATUS_SUSPENDED": "suspended",
}


def _normalize_status(raw: str | None) -> str:
    if not raw:
        return "scheduled"
    return _STATUS_MAP.get(raw, raw.lower().replace("status_", ""))


# ── Score parsing ─────────────────────────────────────────────

def _parse_score_to_par(s: str | None) -> int | None:
    """ESPN ships score-to-par as "E" / "-8" / "+3" / "" (pre-round)."""
    if not s:
        return None
    s = s.strip()
    if not s or s in ("--", "CUT", "WD", "DQ"):
        return None
    if s.upper() == "E":
        return 0
    try:
        return int(s.lstrip("+"))
    except (TypeError, ValueError):
        return None


def _parse_position(pos: str | int | None) -> int | None:
    """Position is shipped as "T5" / "1" / "CUT" / "WD" / None."""
    if pos is None:
        return None
    if isinstance(pos, int):
        return pos
    s = str(pos).strip()
    if not s or s in ("CUT", "WD", "DQ"):
        return None
    if s.startswith("T"):
        s = s[1:]
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


# ── Event-level upserts ──────────────────────────────────────

def _upsert_tournament(conn, event: dict) -> str | None:
    eid = str(event.get("id") or "")
    if not eid:
        return None
    name = event.get("name") or ""
    short_name = event.get("shortName") or name
    start_iso = event.get("date") or ""
    end_iso = event.get("endDate") or start_iso
    status_raw = ((event.get("status") or {}).get("type") or {}).get("name")
    status = _normalize_status(status_raw)
    comp = (event.get("competitions") or [{}])[0]
    course = (comp.get("course") or {}).get("name") or ""
    # Season = calendar year of the tournament start
    try:
        season = int(start_iso[:4]) if start_iso else None
    except (TypeError, ValueError):
        season = None
    is_major = 1 if any(m in name.lower() for m in
                         ("masters", "pga championship", "u.s. open",
                          "us open", "open championship", "the open"))\
                  else 0
    conn.execute(
        "INSERT INTO tournaments (id, name, short_name, course, "
        "start_date, end_date, status, is_major, season, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,datetime('now')) "
        "ON CONFLICT(id) DO UPDATE SET "
        "  name = excluded.name, short_name = excluded.short_name, "
        "  course = excluded.course, end_date = excluded.end_date, "
        "  status = excluded.status, season = excluded.season, "
        "  is_major = excluded.is_major, "
        "  updated_at = datetime('now')",
        (eid, name, short_name, course, start_iso, end_iso, status,
         is_major, season),
    )
    return eid


def _upsert_player(conn, athlete: dict, fallback_id=None) -> int | None:
    # ESPN's scoreboard nests athlete under competitor, where the
    # *competitor*'s id is the athlete id (athlete dict itself drops
    # the id field). Caller passes fallback_id from the competitor row.
    pid_raw = athlete.get("id") or fallback_id
    try:
        pid = int(pid_raw) if pid_raw else None
    except (TypeError, ValueError):
        return None
    if not pid:
        return None
    name = athlete.get("displayName") or athlete.get("fullName") or ""
    short = athlete.get("shortName") or ""
    flag = athlete.get("flag") or {}
    country = flag.get("alt") or flag.get("name") or ""
    flag_url = flag.get("href") or ""
    headshot = (athlete.get("headshot") or {}).get("href") or ""
    conn.execute(
        "INSERT INTO players (id, name, short_name, country, "
        "flag_url, headshot_url, updated_at) "
        "VALUES (?,?,?,?,?,?,datetime('now')) "
        "ON CONFLICT(id) DO UPDATE SET "
        "  name = excluded.name, short_name = excluded.short_name, "
        "  country = excluded.country, flag_url = excluded.flag_url, "
        "  headshot_url = excluded.headshot_url, "
        "  updated_at = datetime('now')",
        (pid, name, short, country, flag_url, headshot),
    )
    return pid


def _upsert_field_entry(conn, tournament_id: str, competitor: dict) -> bool:
    athlete = competitor.get("athlete") or {}
    player_id = _upsert_player(conn, athlete,
                                fallback_id=competitor.get("id"))
    if not player_id:
        return False
    score_to_par = _parse_score_to_par(competitor.get("score"))
    position = _parse_position(competitor.get("order")
                                or (competitor.get("status") or {}).get("position", {}).get("id"))
    status_raw = ((competitor.get("status") or {}).get("type") or {}).get("name", "")
    withdrew = 1 if "WITHDRAW" in status_raw.upper() else 0
    disqualified = 1 if "DISQUALIFIED" in status_raw.upper() else 0
    # made_cut: ESPN doesn't ship an explicit flag pre-cut; infer from
    # status text + linescore length. Players who made the cut have
    # ≥3 linescores by R3; missed-cut competitors have exactly 2.
    linescores = competitor.get("linescores") or []
    made_cut = None
    if linescores and len(linescores) >= 3:
        made_cut = 1
    elif "CUT" in status_raw.upper():
        made_cut = 0
    rounds_json = json.dumps(linescores) if linescores else None
    conn.execute(
        "INSERT INTO field_entries "
        "(tournament_id, player_id, final_position, score_to_par, "
        " made_cut, rounds_json, withdrew, disqualified) "
        "VALUES (?,?,?,?,?,?,?,?) "
        "ON CONFLICT(tournament_id, player_id) DO UPDATE SET "
        "  final_position = excluded.final_position, "
        "  score_to_par   = excluded.score_to_par, "
        "  made_cut       = COALESCE(excluded.made_cut, made_cut), "
        "  rounds_json    = excluded.rounds_json, "
        "  withdrew       = excluded.withdrew, "
        "  disqualified   = excluded.disqualified",
        (tournament_id, player_id, position, score_to_par,
         made_cut, rounds_json, withdrew, disqualified),
    )
    return True


# ── Public ingest ───────────────────────────────────────────

def ingest_today(tour: str) -> dict:
    """Refresh the scoreboard for ``tour``. ESPN's golf scoreboard
    returns a slim event list (no field) by default — to get the full
    156-player competitor blob we re-hit the same endpoint with
    ``?event=<id>`` per active event. Cheap (one extra fetch per
    tournament-week, ~25 events per year for PGA)."""
    cfg = get_tour_config(tour)
    url = f"{_ESPN_BASE}/{cfg['espn_scoreboard_path']}"
    data = _fetch_json(url)
    if not data:
        return {"events": 0, "field_rows": 0, "error": "fetch_failed"}
    conn = get_conn(tour)
    events = data.get("events") or []
    n_field = 0
    seen_events = 0
    for ev in events:
        eid = _upsert_tournament(conn, ev)
        if not eid:
            continue
        seen_events += 1
        # Re-fetch with ?event= to get the full field. The plain
        # scoreboard's competitions[0].competitors is empty for
        # multi-day tournaments; the param'd call returns the same
        # event shape with the full list populated.
        ev_full = _fetch_json(f"{url}?event={eid}")
        comp_full = None
        if ev_full:
            full_events = ev_full.get("events") or []
            if full_events:
                comp_full = (full_events[0].get("competitions") or [{}])[0]
        if not comp_full:
            comp_full = (ev.get("competitions") or [{}])[0]
        for c in comp_full.get("competitors") or []:
            if _upsert_field_entry(conn, eid, c):
                n_field += 1
        time.sleep(_REQUEST_INTERVAL_S)
    conn.commit()
    logger.info("[golf:%s] ingest_today events=%d field_rows=%d",
                tour, seen_events, n_field)
    return {"events": seen_events, "field_rows": n_field}


def ingest_field_from_hr(tour: str, tournament_id: str) -> dict:
    """Populate ``field_entries`` for an upcoming tournament from HR's
    WINNER market when ESPN hasn't posted the field yet.

    ESPN only exposes a tournament's field 24-48h before the event;
    the route + predictor can't run until that window. HR posts the
    outright odds days earlier (every entered player gets a WINNER
    price). When we see an upcoming-scheduled tournament with no
    field_entries but HR has WINNER odds, the player IDs in the odds
    map ARE the field. Insert each with NULL final_position so the
    settler later overwrites with real results via ingest_today.

    Returns ``{rows: N}``.
    """
    from ._odds import fetch_tournament_odds
    odds = fetch_tournament_odds(tour, str(tournament_id))
    winner = odds.get("WINNER") if isinstance(odds, dict) else None
    if not isinstance(winner, dict) or not winner:
        return {"rows": 0, "reason": "no_winner_market"}
    conn = get_conn(tour)
    n = 0
    for pid in winner.keys():
        try:
            conn.execute(
                "INSERT OR IGNORE INTO field_entries "
                "(tournament_id, player_id, final_position, score_to_par, "
                " made_cut, rounds_json, withdrew, disqualified) "
                "VALUES (?, ?, NULL, NULL, NULL, NULL, 0, 0)",
                (str(tournament_id), int(pid)),
            )
            if conn.total_changes > 0:
                n += 1
        except Exception as e:
            logger.debug("[golf:%s] hr-field insert %s failed: %s",
                          tour, pid, e)
    conn.commit()
    logger.info("[golf:%s] HR field hydrate tournament=%s rows=%d",
                tour, tournament_id, n)
    return {"rows": n, "source": "hr_winner_market"}


def ingest_summary(tour: str, event_id: str) -> dict:
    """Pull one event's full field via the scoreboard ``?event=``
    endpoint. ESPN's /summary path returns 404 for golf (verified
    2026-05-13); only the param'd scoreboard exposes the leaderboard.

    Used by the backfill loop and by callers who want to re-pull a
    specific event's field outside the slate-refresh path."""
    cfg = get_tour_config(tour)
    url = f"{_ESPN_BASE}/{cfg['espn_scoreboard_path']}?event={event_id}"
    data = _fetch_json(url)
    if not data:
        return {"field_rows": 0, "error": "fetch_failed"}
    events = data.get("events") or []
    if not events:
        return {"field_rows": 0, "error": "no_event"}
    ev = events[0]
    conn = get_conn(tour)
    eid = _upsert_tournament(conn, ev)
    if not eid:
        return {"field_rows": 0, "error": "no_event_id"}
    comp = (ev.get("competitions") or [{}])[0]
    competitors = comp.get("competitors") or []
    n = 0
    for c in competitors:
        if _upsert_field_entry(conn, eid, c):
            n += 1
    conn.commit()
    logger.info("[golf:%s] summary %s -> %d field rows", tour, event_id, n)
    return {"field_rows": n}


def backfill_courses(tour: str) -> dict:
    """Pull pgatour.com's current schedule and fill `course` on any
    matching DB tournaments where it's empty. ESPN's year-level
    scoreboard strips course attribution — this scraper recovers it
    for the current season at least.

    Match strategy: name (case + punctuation insensitive) + year.
    Returns ``{matched, updated, scraped}``."""
    if tour != "pga":
        return {"error": f"course backfill only supports 'pga' (got {tour!r})"}
    try:
        from scrapers.pgatour_schedule import fetch_schedule
    except Exception as e:
        return {"error": f"scraper import failed: {e}"}
    scraped = fetch_schedule()
    if not scraped:
        return {"scraped": 0, "matched": 0, "updated": 0}
    conn = get_conn(tour)
    # Build a name → courseName lookup, normalized.
    def _norm(s: str) -> str:
        return "".join(c.lower() for c in (s or "") if c.isalnum())
    course_by_name: dict[str, str] = {}
    for row in scraped:
        if row.get("courseName"):
            course_by_name[_norm(row["name"])] = row["courseName"]
    if not course_by_name:
        return {"scraped": len(scraped), "matched": 0, "updated": 0}
    # Walk our tournaments missing course attribution.
    tournaments = conn.execute(
        "SELECT id, name, course FROM tournaments "
        "WHERE course IS NULL OR course = ''"
    ).fetchall()
    matched = updated = 0
    for t in tournaments:
        key = _norm(t["name"])
        course = course_by_name.get(key)
        if not course:
            continue
        matched += 1
        conn.execute(
            "UPDATE tournaments SET course = ?, updated_at = datetime('now') "
            "WHERE id = ?",
            (course, t["id"]),
        )
        updated += 1
    conn.commit()
    logger.info("[golf:%s] course backfill: scraped=%d matched=%d updated=%d",
                tour, len(scraped), matched, updated)
    return {"scraped": len(scraped), "matched": matched, "updated": updated}


def backfill(tour: str, year: int) -> dict:
    """Walk a season's tournaments via the scoreboard ``?dates=YYYY``
    listing. ESPN ships every event's full leaderboard inline in that
    response — no per-event re-fetch needed.

    Notably, ESPN's ``?event=X`` param is **ignored** for golf (it
    silently returns the current week's scoreboard regardless of the
    requested id). The year-level scoreboard is the working path for
    historical pulls."""
    cfg = get_tour_config(tour)
    schedule_url = (
        f"{_ESPN_BASE}/{cfg['espn_scoreboard_path']}?dates={year}"
    )
    data = _fetch_json(schedule_url)
    if not data:
        return {"events": 0, "field_rows": 0, "error": "fetch_failed"}
    events = data.get("events") or []
    conn = get_conn(tour)
    n_events = n_field = 0
    for ev in events:
        eid = _upsert_tournament(conn, ev)
        if not eid:
            continue
        comp = (ev.get("competitions") or [{}])[0]
        for c in comp.get("competitors") or []:
            if _upsert_field_entry(conn, eid, c):
                n_field += 1
        n_events += 1
    conn.commit()
    logger.info("[golf:%s] backfill %d: events=%d field_rows=%d",
                tour, year, n_events, n_field)
    return {"events": n_events, "field_rows": n_field}
