"""theScore API ingest helper — shared by AHL, PWHL, and any future
league theScore covers (KHL, SHL, Liiga, etc).

theScore exposes free, unauthenticated JSON at ``api.thescore.com/<league>/``
covering teams + events (games) with consistent schema across leagues.
For hockey: AHL + PWHL both use this layer and the event schema is
field-for-field identical, so one ingest module handles both.

Public API:
    fetch_teams(league_slug)  -> list[dict]
    fetch_events(league_slug, *, status=None, since_id=None) -> list[dict]
    backfill(league_slug, conn, *, start_date=None, status='final') -> dict
    upsert_teams(conn, teams, table_name)
    upsert_event(conn, ev, table_name) -> bool

The DB schema is per-league (each gets its own teams + games table) so
the ingest module is just a transport — the league-specific module owns
its connection + schema. Mirrors the pattern in
``engine.basketball._espn_ingest``.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import ssl
import time
import urllib.request
from datetime import datetime
from typing import Any, Iterable

logger = logging.getLogger(__name__)

_API_BASE = "https://api.thescore.com"
# theScore's CDN is fine with a default TLS chain in production but the
# Windows Python install ships an incomplete CA bundle on this host; the
# unverified context is acceptable for a public, read-only API.
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

_PAGE_SIZE = 100  # theScore caps at ~100 per page; rpp param tuned safe


def _fetch(path: str, retries: int = 3) -> Any:
    """GET ``path`` (relative to api.thescore.com) and return parsed JSON.
    Returns None on failure rather than raising, so a single bad page
    doesn't abort a multi-page backfill."""
    url = f"{_API_BASE}{path}"
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=15, context=_SSL_CTX) as r:
                return json.loads(r.read())
        except Exception as e:
            logger.debug("thescore fetch %s failed (attempt %d): %s",
                          url, attempt + 1, e)
            time.sleep(0.5 * (attempt + 1))
    return None


def fetch_teams(league_slug: str) -> list[dict]:
    """Pull every team for ``league_slug`` (e.g. 'ahl', 'pwhl')."""
    data = _fetch(f"/{league_slug}/teams")
    if not isinstance(data, list):
        return []
    return data


def fetch_events_page(league_slug: str, *, status: str | None = None,
                      since_id: int | None = None,
                      rpp: int = _PAGE_SIZE) -> list[dict]:
    """Pull one page of events. Used by ``backfill`` to walk the full
    history; callers can also use it directly for incremental syncs.

    ``status`` filters to 'final' / 'in_progress' / 'pre_game' etc.
    ``since_id`` returns events with id > since_id (theScore IDs are
    monotonically assigned in event-creation order, not chronological).
    """
    qs = [f"rpp={rpp}"]
    if status:
        qs.append(f"status={status}")
    if since_id is not None:
        qs.append(f"id.gt={since_id}")
    path = f"/{league_slug}/events?" + "&".join(qs)
    data = _fetch(path)
    if not isinstance(data, list):
        return []
    return data


def fetch_events_all(league_slug: str, *, status: str | None = None,
                      max_pages: int = 200) -> list[dict]:
    """Walk every page of events for the league. Pages by ``id.gt`` —
    theScore returns ids ascending, so each request fetches the next
    chunk. Caps at ``max_pages`` (= 20K events) as a safety bound."""
    out: list[dict] = []
    since: int | None = None
    for _ in range(max_pages):
        page = fetch_events_page(league_slug, status=status, since_id=since)
        if not page:
            break
        out.extend(page)
        # IDs may be non-monotonic across leagues so just advance from
        # the highest seen id in this page.
        page_max = max((int(e.get("id") or 0) for e in page), default=since or 0)
        if since is not None and page_max <= since:
            break
        since = page_max
        time.sleep(0.1)  # polite — theScore tolerates much more
    return out


# ── DB shape ─────────────────────────────────────────────────

# Conservative shared schema. League-specific helpers create the table
# names + add per-league columns (e.g. shootouts) on top of this.
_TEAMS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id            INTEGER PRIMARY KEY,         -- theScore team id
    full_name     TEXT NOT NULL,
    short_name    TEXT,
    abbreviation  TEXT,
    city          TEXT,
    division      TEXT,
    conference    TEXT,
    color_primary TEXT,
    color_secondary TEXT,
    logo_url      TEXT,
    updated_at    TEXT DEFAULT (datetime('now'))
);
"""

_GAMES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id            INTEGER PRIMARY KEY,         -- theScore event id
    date          TEXT NOT NULL,               -- YYYY-MM-DD (UTC)
    start_time    TEXT,                        -- ISO-8601 UTC (real puck-drop)
    home_team_id  INTEGER NOT NULL,
    away_team_id  INTEGER NOT NULL,
    home_score    INTEGER,
    away_score    INTEGER,
    status        TEXT,                        -- 'final' / 'in_progress' / 'pre_game'
    game_type     TEXT,                        -- 'Regular Season' / 'Playoffs' / 'Preseason'
    season        INTEGER,                     -- starting calendar year
    has_shootout  INTEGER DEFAULT 0,
    has_overtime  INTEGER DEFAULT 0,
    venue         TEXT,
    -- Per-period scoring. theScore ships these via the /box_scores
    -- sub-endpoint as line_scores[home][...].score by segment 1-3.
    -- Backfilled separately from the main ingest because each game
    -- needs its own /box_scores hit (avoid hammering during refresh).
    home_p1 INTEGER, away_p1 INTEGER,
    home_p2 INTEGER, away_p2 INTEGER,
    home_p3 INTEGER, away_p3 INTEGER,
    updated_at    TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_{table}_date ON {table}(date);
CREATE INDEX IF NOT EXISTS idx_{table}_status ON {table}(status);
CREATE INDEX IF NOT EXISTS idx_{table}_season ON {table}(season);
"""


_PICKS_DDL = """
CREATE TABLE IF NOT EXISTS picks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id      INTEGER NOT NULL,
    date         TEXT NOT NULL,
    matchup      TEXT,
    bet_type     TEXT NOT NULL,           -- 'ML' / 'PL' / 'OU'
    pick         TEXT NOT NULL,           -- 'Home' / 'Away -1.5' / 'Over 5.5'
    model_prob   REAL,
    edge         REAL,
    odds         INTEGER,
    confidence   TEXT,                    -- strong/moderate/lean/skip
    result       TEXT,                    -- W/L/P/V (NULL = pending)
    profit       REAL,
    closing_odds INTEGER,                 -- HR closing snapshot for CLV
    settled_at   TEXT,
    created_at   TEXT DEFAULT (datetime('now'))
);
-- FULL UNIQUE. Previous partial-on-pending let dupes accumulate
-- after settle (2026-06-10 hardening pass swept all sports). Date
-- stays off the key because a game still has one canonical bet
-- regardless of which slate-tick day it was recorded on.
CREATE UNIQUE INDEX IF NOT EXISTS uq_picks_family
    ON picks(game_id, bet_type, pick);
CREATE INDEX IF NOT EXISTS idx_picks_date ON picks(date);
CREATE INDEX IF NOT EXISTS idx_picks_pending ON picks(result) WHERE result IS NULL;
"""


def init_schema(conn: sqlite3.Connection, *, teams_table: str,
                games_table: str) -> None:
    conn.executescript(_TEAMS_DDL.format(table=teams_table)
                        + _GAMES_DDL.format(table=games_table)
                        + _PICKS_DDL)
    # Defensive ALTERs for DBs created before these columns existed.
    cols = {r[1] for r in conn.execute(
        f"PRAGMA table_info({teams_table})").fetchall()}
    if "logo_url" not in cols:
        try:
            conn.execute(f"ALTER TABLE {teams_table} ADD COLUMN logo_url TEXT")
        except sqlite3.OperationalError:
            pass
    g_cols = {r[1] for r in conn.execute(
        f"PRAGMA table_info({games_table})").fetchall()}
    if "start_time" not in g_cols:
        try:
            conn.execute(f"ALTER TABLE {games_table} ADD COLUMN start_time TEXT")
        except sqlite3.OperationalError:
            pass
    # Per-period columns — added 2026-05-15 to unlock state-MC for
    # AHL/PWHL. Backfilled by engine.sports._thescore_periods.
    for col in ("home_p1", "away_p1", "home_p2", "away_p2",
                "home_p3", "away_p3"):
        if col not in g_cols:
            try:
                conn.execute(f"ALTER TABLE {games_table} ADD COLUMN {col} INTEGER")
            except sqlite3.OperationalError:
                pass
    # closing_odds column on picks — added 2026-05-16 to unlock CLV
    # capture for the hockey framework (AHL/PWHL/AIHL/NZIHL).
    p_cols = {r[1] for r in conn.execute(
        "PRAGMA table_info(picks)").fetchall()}
    if "closing_odds" not in p_cols:
        try:
            conn.execute("ALTER TABLE picks ADD COLUMN closing_odds INTEGER")
        except sqlite3.OperationalError:
            pass
    conn.commit()


def upsert_teams(conn: sqlite3.Connection, teams: Iterable[dict],
                 table: str) -> int:
    n = 0
    for t in teams:
        if not t.get("id"):
            continue
        # Skip placeholder teams. theScore ships a "TBD" entry for
        # unscheduled future-round playoff slots; including it shows up
        # in standings as a 0-0 ghost team and looks broken.
        full = (t.get("full_name") or t.get("name") or "").strip().upper()
        short = (t.get("short_name") or "").strip().upper()
        abbr = (t.get("abbreviation") or "").strip().upper()
        if full == "TBD" or short == "TBD" or abbr == "TBD":
            continue
        # theScore packs every logo size at team.logos.{small,large,...};
        # `small` is the right size for sidebar/card rendering.
        logos = t.get("logos") or {}
        logo_url = logos.get("small") or logos.get("w72xh72") or logos.get("large")
        conn.execute(
            f"INSERT OR REPLACE INTO {table} (id, full_name, short_name, "
            f"abbreviation, city, division, conference, color_primary, "
            f"color_secondary, logo_url, updated_at) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            (
                int(t["id"]),
                t.get("full_name") or t.get("name") or "",
                t.get("short_name"),
                t.get("abbreviation"),
                t.get("location"),
                t.get("division"),
                t.get("conference"),
                t.get("colour_1"),
                t.get("colour_2"),
                logo_url,
            ),
        )
        n += 1
    conn.commit()
    return n


try:
    from zoneinfo import ZoneInfo
    _US_EASTERN = ZoneInfo("America/New_York")
except ImportError:
    _US_EASTERN = None


def _parse_event_date(ev: dict) -> str | None:
    """theScore ships ``game_date`` typically as RFC-822 UTC. Returns the
    game date in US Eastern.

    Why ET, not UTC: AHL/PWHL are North American leagues; a 10 PM ET
    tipoff stamps as 02:00 UTC the *next* day. A UTC-derived date column
    would file that game under tomorrow, surfacing it on the wrong day
    in the slate (user report 2026-05-13: COLEA@COVF showed as 5/14 even
    though the 10 PM ET tipoff was tonight). Using ET as the canonical
    game-day matches the user's mental model and the betting market's
    convention.

    Falls back to raw UTC date when zoneinfo isn't available (Windows
    Python <3.9 without tzdata)."""
    raw = ev.get("game_date")
    if not raw:
        return None
    raw = raw.strip()
    # Parse to a UTC-aware datetime, then convert to ET before
    # extracting the date.
    dt = None
    if raw and raw[0].isdigit():
        # ISO-8601 — e.g. "2026-05-14T02:00:00Z"
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        # RFC-822 — e.g. "Wed, 14 May 2026 02:00:00 +0000"
        try:
            from datetime import timezone
            dt = datetime.strptime(raw[:25], "%a, %d %b %Y %H:%M:%S")
            dt = dt.replace(tzinfo=timezone.utc)
        except ValueError:
            # Truncated-date fallback only — no time data to convert.
            try:
                return datetime.fromisoformat(raw[:10]).strftime("%Y-%m-%d")
            except ValueError:
                return None
    if _US_EASTERN is not None:
        dt = dt.astimezone(_US_EASTERN)
    return dt.strftime("%Y-%m-%d")


def _parse_event_start(ev: dict) -> str | None:
    """Full ISO-8601 UTC start time so the frontend can render local
    (ET for US users) instead of a midnight placeholder. theScore's
    ``game_date`` is the puck-drop wall clock."""
    raw = ev.get("game_date")
    if not raw:
        return None
    try:
        dt = datetime.strptime(raw[:25], "%a, %d %b %Y %H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def _season_from_date(date_iso: str) -> int:
    """Hockey seasons span calendar years (Oct -> May). Use the
    starting calendar year as the season label, matching how the rest of
    the codebase tags hockey seasons."""
    y, m, _ = date_iso.split("-")
    y = int(y)
    return y if int(m) >= 9 else y - 1


def upsert_event(conn: sqlite3.Connection, ev: dict, table: str) -> bool:
    """Upsert one theScore event. Returns True if a row was written."""
    if not ev.get("id"):
        return False
    date = _parse_event_date(ev)
    if not date:
        return False
    home = ev.get("home_team") or {}
    away = ev.get("away_team") or {}
    if not (home.get("id") and away.get("id")):
        return False
    box = ev.get("box_score") or {}
    score = box.get("score") or {}
    home_pts = (score.get("home") or {}).get("score") if isinstance(score.get("home"), dict) else None
    away_pts = (score.get("away") or {}).get("score") if isinstance(score.get("away"), dict) else None
    progress = box.get("progress") or {}
    has_so = bool(progress.get("shootout"))
    has_ot = bool(progress.get("overtime"))
    season = _season_from_date(date)
    start_time = _parse_event_start(ev)
    conn.execute(
        f"INSERT OR REPLACE INTO {table} (id, date, start_time, "
        f"home_team_id, away_team_id, home_score, away_score, status, "
        f"game_type, season, has_shootout, has_overtime, venue, updated_at) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
        (
            int(ev["id"]),
            date,
            start_time,
            int(home["id"]),
            int(away["id"]),
            home_pts,
            away_pts,
            ev.get("event_status") or ev.get("status"),
            ev.get("game_type"),
            season,
            1 if has_so else 0,
            1 if has_ot else 0,
            ev.get("location") or "",
        ),
    )
    return True


def backfill(league_slug: str, conn: sqlite3.Connection, *,
             teams_table: str, games_table: str,
             status: str | None = None) -> dict:
    """Pull all teams + every event from theScore and upsert into the
    league's DB. Returns aggregate counts.

    Also flips any local game still stuck in 'pre_game' status with
    a date more than 2 days in the past to 'cancelled' — theScore
    drops postponed/relocated games from the feed silently, leaving
    our DB with phantom pre_game rows that pollute the slate and
    leave picks dangling. The 2-day buffer absorbs timezone drift
    on overnight games before we assume the event was dropped.
    """
    from datetime import datetime, timedelta
    init_schema(conn, teams_table=teams_table, games_table=games_table)
    teams = fetch_teams(league_slug)
    n_teams = upsert_teams(conn, teams, teams_table)
    events = fetch_events_all(league_slug, status=status)
    n_events = 0
    for ev in events:
        try:
            if upsert_event(conn, ev, games_table):
                n_events += 1
        except Exception as e:
            logger.debug("upsert event %s failed: %s", ev.get("id"), e)
    # Stale-game sweep — keeps the slate clean week-to-week.
    cutoff = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    cur = conn.execute(
        f"UPDATE {games_table} SET status = 'cancelled' "
        f"WHERE status = 'pre_game' AND date < ? "
        f"  AND (home_score IS NULL OR home_score = 0) "
        f"  AND (away_score IS NULL OR away_score = 0)",
        (cutoff,),
    )
    n_cancelled = cur.rowcount
    conn.commit()
    logger.info("[%s] backfill complete: %d teams, %d events, "
                "%d stale pre_game -> cancelled",
                league_slug, n_teams, n_events, n_cancelled)
    return {"teams": n_teams, "events": n_events,
            "stale_cancelled": n_cancelled}
