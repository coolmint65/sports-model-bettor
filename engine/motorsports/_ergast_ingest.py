"""Ergast (jolpi.ca mirror) F1 ingest.

Ergast endpoints used:
  - /api/f1/{season}.json                    season race calendar
  - /api/f1/{season}/drivers.json            season driver entries
  - /api/f1/{season}/constructors.json       season constructor entries
  - /api/f1/{season}/results.json            full race results (paginated)
  - /api/f1/{season}/qualifying.json         qualifying results (paginated)

Why jolpi.ca: the original ergast.com retired its public API in
late 2024; jolpi.ca runs a drop-in mirror with the same paths and
schema. Rate limit is generous (4 req/sec sustained) — we throttle
at 0.3s between calls to stay polite.

Pagination: Ergast caps responses at 100 rows by default and 1000
hard-max. ``_paged_get`` walks ``offset`` until the API stops
returning new rows.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from typing import Any

from ._config import get_series_config
from ._db import get_conn

logger = logging.getLogger(__name__)


_USER_AGENT = "Mozilla/5.0 sports-model-bettor"
_THROTTLE_S = 0.3
_PAGE_SIZE = 100  # jolpi.ca caps at 100 regardless of what we ask


def _http_get(url: str, timeout: float = 15.0) -> dict | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError,
            json.JSONDecodeError, TimeoutError) as e:
        logger.warning("Ergast GET %s failed: %s", url, e)
        return None


def _paged_get(base_url: str, table_key: str, row_key: str,
               nested_row_key: str | None = None) -> list[dict]:
    """Walk Ergast pagination until exhausted. ``MRData.total`` counts
    leaf rows: for /drivers, leaf=driver; for /results, leaf=result entry
    (one per driver per race). When ``nested_row_key`` is set (e.g.
    'Results' or 'QualifyingResults'), advancing offset uses the nested
    count so we don't loop forever or bail early — earlier we incremented
    by race count and missed ~75% of season results."""
    out: list[dict] = []
    offset = 0
    while True:
        sep = "&" if "?" in base_url else "?"
        url = f"{base_url}{sep}limit={_PAGE_SIZE}&offset={offset}"
        body = _http_get(url)
        if not body:
            break
        md = body.get("MRData", {})
        table = md.get(f"{table_key}Table", {})
        rows = table.get(row_key) or []
        if not rows:
            break
        out.extend(rows)
        try:
            total = int(md.get("total") or 0)
        except (TypeError, ValueError):
            total = 0
        if nested_row_key:
            # Count leaf rows actually returned this page.
            page_leaves = sum(len(r.get(nested_row_key) or []) for r in rows)
        else:
            page_leaves = len(rows)
        offset += page_leaves
        if offset >= total or page_leaves == 0:
            break
        time.sleep(_THROTTLE_S)
    return out


# ── Drivers / Constructors ────────────────────────────────────

def ingest_drivers(series: str, season: int) -> int:
    """Upsert ``season``'s driver entries. Returns rows touched."""
    cfg = get_series_config(series)
    base = cfg["ergast_base"].rstrip("/")
    rows = _paged_get(f"{base}/{season}/drivers/?format=json",
                       "Driver", "Drivers")
    conn = get_conn(series)
    n = 0
    for d in rows:
        ergast_id = d.get("driverId")
        if not ergast_id:
            continue
        name = f"{d.get('givenName','').strip()} {d.get('familyName','').strip()}".strip()
        code = d.get("code")
        nat = d.get("nationality")
        # Insert or update by ergast_id
        conn.execute("""
            INSERT INTO drivers (name, abbreviation, ergast_id, nationality)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ergast_id) DO UPDATE SET
                name = excluded.name,
                abbreviation = COALESCE(excluded.abbreviation, drivers.abbreviation),
                nationality = COALESCE(excluded.nationality, drivers.nationality)
        """, (name, code, ergast_id, nat))
        n += 1
    conn.commit()
    logger.info("[%s] drivers ingest %d (season=%d)", series, n, season)
    return n


def ingest_constructors(series: str, season: int) -> int:
    """Upsert ``season``'s constructor entries."""
    cfg = get_series_config(series)
    base = cfg["ergast_base"].rstrip("/")
    rows = _paged_get(f"{base}/{season}/constructors/?format=json",
                       "Constructor", "Constructors")
    conn = get_conn(series)
    n = 0
    for c in rows:
        ergast_id = c.get("constructorId")
        if not ergast_id:
            continue
        name = (c.get("name") or "").strip()
        nat = c.get("nationality")
        # Auto-derive a 3-letter abbreviation from the team name
        # (Ergast doesn't ship one). E.g., "Red Bull Racing" -> "RBR".
        abbrev = _team_abbrev(name)
        conn.execute("""
            INSERT INTO teams (name, abbreviation, ergast_id, nationality)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ergast_id) DO UPDATE SET
                name = excluded.name,
                abbreviation = COALESCE(teams.abbreviation, excluded.abbreviation),
                nationality = COALESCE(excluded.nationality, teams.nationality)
        """, (name, abbrev, ergast_id, nat))
        n += 1
    conn.commit()
    logger.info("[%s] constructors ingest %d (season=%d)", series, n, season)
    return n


def _team_abbrev(name: str) -> str:
    """Derive a 3-letter team abbreviation. First letter of each word,
    capped at 3 chars (Ferrari → FER, Red Bull Racing → RBR)."""
    if not name:
        return ""
    words = [w for w in name.split() if w and w[0].isalpha()]
    if len(words) == 1:
        return words[0][:3].upper()
    return "".join(w[0] for w in words[:3]).upper()


# ── Calendar ──────────────────────────────────────────────────

def ingest_calendar(series: str, season: int) -> int:
    """Upsert ``season``'s race calendar (no results yet)."""
    cfg = get_series_config(series)
    base = cfg["ergast_base"].rstrip("/")
    rows = _paged_get(f"{base}/{season}/?format=json", "Race", "Races")
    conn = get_conn(series)
    n = 0
    for r in rows:
        round_n = int(r.get("round") or 0)
        if not round_n:
            continue
        race_id = f"{season}-{round_n:02d}"
        date = r.get("date")
        time_s = r.get("time") or ""
        # ISO datetime UTC = date + 'T' + time. Time is "HH:MM:SSZ".
        race_time = None
        if date and time_s:
            race_time = f"{date}T{time_s}" if not time_s.endswith("Z") else \
                         f"{date}T{time_s}"
        circuit = (r.get("Circuit") or {})
        country = ((circuit.get("Location") or {}).get("country"))
        wiki_url = circuit.get("url")
        conn.execute("""
            INSERT INTO races (race_id, season, round, name, circuit,
                               country, race_date, race_time, ergast_id,
                               circuit_wiki_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(race_id) DO UPDATE SET
                name = excluded.name,
                circuit = excluded.circuit,
                country = excluded.country,
                race_date = excluded.race_date,
                race_time = COALESCE(excluded.race_time, races.race_time),
                circuit_wiki_url = COALESCE(excluded.circuit_wiki_url,
                                             races.circuit_wiki_url)
        """, (race_id, season, round_n, r.get("raceName"),
              circuit.get("circuitName"), country, date, race_time,
              circuit.get("circuitId"), wiki_url))
        n += 1
    conn.commit()
    logger.info("[%s] calendar ingest %d (season=%d)", series, n, season)
    return n


# ── Results + Qualifying ─────────────────────────────────────

def ingest_results(series: str, season: int) -> int:
    """Pull race results for an entire season (paginated). Each row
    upserts into race_results keyed by (race_id, driver_id)."""
    cfg = get_series_config(series)
    base = cfg["ergast_base"].rstrip("/")
    rows = _paged_get(f"{base}/{season}/results/?format=json",
                       "Race", "Races", nested_row_key="Results")
    # The same race may appear in multiple pages because pagination is
    # by result-row. Merge results across page boundaries so each race
    # ends up with a single, complete Results list.
    rows = _merge_paged_races(rows, "Results")
    conn = get_conn(series)
    n = 0
    for race in rows:
        race_id = f"{season}-{int(race.get('round') or 0):02d}"
        # Race must already exist in the races table — calendar ingest
        # is a precondition for results ingest.
        for res in (race.get("Results") or []):
            d = res.get("Driver") or {}
            c = res.get("Constructor") or {}
            driver_row = conn.execute(
                "SELECT id FROM drivers WHERE ergast_id = ?",
                (d.get("driverId"),),
            ).fetchone()
            team_row = conn.execute(
                "SELECT id FROM teams WHERE ergast_id = ?",
                (c.get("constructorId"),),
            ).fetchone()
            if not driver_row:
                # Driver not in our table — likely a one-off entry in an
                # older season that isn't on the current driver list.
                # Insert a stub row so the result links somewhere.
                conn.execute(
                    "INSERT OR IGNORE INTO drivers (name, abbreviation, "
                    "ergast_id, nationality) VALUES (?, ?, ?, ?)",
                    (f"{d.get('givenName','')} {d.get('familyName','')}".strip(),
                     d.get("code"), d.get("driverId"), d.get("nationality"))
                )
                driver_row = conn.execute(
                    "SELECT id FROM drivers WHERE ergast_id = ?",
                    (d.get("driverId"),),
                ).fetchone()
            if not team_row:
                conn.execute(
                    "INSERT OR IGNORE INTO teams (name, abbreviation, "
                    "ergast_id, nationality) VALUES (?, ?, ?, ?)",
                    ((c.get("name") or "").strip(),
                     _team_abbrev(c.get("name") or ""),
                     c.get("constructorId"), c.get("nationality"))
                )
                team_row = conn.execute(
                    "SELECT id FROM teams WHERE ergast_id = ?",
                    (c.get("constructorId"),),
                ).fetchone()
            grid = _safe_int(res.get("grid"))
            pos = _safe_int(res.get("position"))
            laps = _safe_int(res.get("laps"))
            status = (res.get("status") or "").strip()
            points = _safe_float(res.get("points"))
            fl = (res.get("FastestLap") or {})
            fl_rank = _safe_int(fl.get("rank")) if fl else None
            conn.execute("""
                INSERT INTO race_results (race_id, driver_id, team_id,
                    qualifying_pos, finish_pos, status, laps,
                    fastest_lap_rank, points)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(race_id, driver_id) DO UPDATE SET
                    team_id = excluded.team_id,
                    qualifying_pos = COALESCE(excluded.qualifying_pos, race_results.qualifying_pos),
                    finish_pos = excluded.finish_pos,
                    status = excluded.status,
                    laps = excluded.laps,
                    fastest_lap_rank = excluded.fastest_lap_rank,
                    points = excluded.points
            """, (race_id, driver_row["id"], team_row["id"] if team_row else None,
                  grid, pos, status, laps, fl_rank, points))
            n += 1
        # Mark the race complete since it has results.
        conn.execute(
            "UPDATE races SET status = 'complete' WHERE race_id = ?",
            (race_id,),
        )
    conn.commit()
    logger.info("[%s] results ingest %d rows (season=%d)", series, n, season)
    return n


def _merge_paged_races(rows: list[dict], child_key: str) -> list[dict]:
    """Result rows can split across pages — round 5 might have 10
    selections on page 0 and 10 on page 1. Merge by (season, round) so
    downstream code sees one race per round with the full Results list."""
    by_key: dict[tuple, dict] = {}
    for r in rows:
        k = (r.get("season"), r.get("round"))
        if k not in by_key:
            by_key[k] = {**r, child_key: list(r.get(child_key) or [])}
        else:
            by_key[k][child_key].extend(r.get(child_key) or [])
    return list(by_key.values())


def _safe_int(v: Any) -> int | None:
    try:
        return int(v) if v not in (None, "", "\\N") else None
    except (TypeError, ValueError):
        return None


def _safe_float(v: Any) -> float | None:
    try:
        return float(v) if v not in (None, "", "\\N") else None
    except (TypeError, ValueError):
        return None


# ── One-shot driver for the route ────────────────────────────

def ingest_current(series: str, lookback_seasons: int = 3) -> dict:
    """Pull drivers + constructors + calendar for the current ET season,
    plus the last ``lookback_seasons`` worth of results so the predictor
    has training data on day one. Returns counts per stage."""
    from datetime import datetime
    season = datetime.utcnow().year
    out: dict[str, int] = {}
    out["drivers"] = ingest_drivers(series, season)
    out["constructors"] = ingest_constructors(series, season)
    # Drivers/constructors must exist before results ingest can resolve
    # foreign keys. Backfill last N seasons of results so Elo has context.
    out["calendar"] = ingest_calendar(series, season)
    results_total = 0
    for s in range(season - lookback_seasons + 1, season + 1):
        # Each season's drivers/constructors must be in the table for
        # the result-row foreign keys. Backfill them too.
        if s != season:
            ingest_drivers(series, s)
            ingest_constructors(series, s)
            ingest_calendar(series, s)
        results_total += ingest_results(series, s)
    out["results"] = results_total
    return out
