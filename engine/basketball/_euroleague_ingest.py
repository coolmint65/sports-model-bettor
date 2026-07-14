"""Euroleague XML ingest.

Source: ``api-live.euroleague.net/v1/{schedules,results}`` — public,
no auth, browser User-Agent required. Returns XML; parsed via
``xml.etree.ElementTree`` (stdlib, no extra deps).

Three entry points (mirroring the ESPN ingester contract):

    ingest_teams()                      — pull team list from schedule scan
    ingest_today()                      — refresh today's games + scores
    backfill(season_code, throttle_s)   — bulk historical pull

Persists into ``data/basketball/euroleague.db`` via the framework's
generic per-league DB layer.

Season codes: Euroleague uses ``E<year>`` for the season starting in
that year (E2024 = 2024-25 season). The CLI accepts these directly.
"""
from __future__ import annotations

import logging
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Iterable

from ._db import get_conn, teams_table, games_table
from .._tz import et_today_str

logger = logging.getLogger(__name__)


_BASE = "https://api-live.euroleague.net/v1"

try:
    from zoneinfo import ZoneInfo
    # Euroleague's localtimezone offset varies by venue; the schedule
    # XML uses a single canonical timezone across the league. Belgrade /
    # Berlin / Athens all sit in CET/CEST. Use Berlin as the reference;
    # the actual game-local time may differ by an hour for Greek games
    # but the slate-sort + frontend ET conversion stays correct because
    # we always emit ISO with explicit offset.
    _CET_TZ = ZoneInfo("Europe/Berlin")
except Exception:
    _CET_TZ = None


def _euroleague_iso_start(date: str | None, startime: str | None) -> str | None:
    """Compose an ISO-8601 string with the right CET/CEST offset for
    ``date``. Returns None on bad inputs.

    Treats ``"00:00"`` as missing-time placeholder rather than midnight.
    Euroleague's feed ships "00:00" for not-yet-scheduled games (notably
    Final Four bracket placeholders before TV slots publish); preserving
    those as literal midnight gave the frontend a wrong "0:00 ET" tipoff
    instead of showing TBD. User directive: no synthesized times."""
    if not (date and startime and ":" in startime):
        return None
    if startime.strip() in ("00:00", "0:00"):
        return None
    if _CET_TZ is None:
        # Fallback: month-keyed offset. CEST (UTC+2) approx
        # late-March through late-October; CET (UTC+1) otherwise.
        try:
            m = int(date.split("-")[1])
        except (ValueError, IndexError, AttributeError):
            m = 1
        offset = "+02:00" if 4 <= m <= 10 else "+01:00"
        return f"{date}T{startime}:00{offset}"
    try:
        from datetime import datetime
        naive = datetime.strptime(f"{date} {startime}", "%Y-%m-%d %H:%M")
        # Localize into Berlin time so DST is applied automatically.
        local = naive.replace(tzinfo=_CET_TZ)
        return local.isoformat()
    except ValueError:
        return None
_HEADERS = {
    # Browser UA required — bare scripts get 403.
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 SportsBettor/1.0"
    ),
    "Accept": "text/xml,application/xml",
}


def _fetch_xml(url: str, retries: int = 3) -> ET.Element | None:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=15) as r:
                raw = r.read()
            return ET.fromstring(raw)
        except Exception as e:
            logger.warning("Euroleague fetch %d/%d failed (%s): %s",
                           attempt + 1, retries, url, e)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


# ── Date parse ─────────────────────────────────────────────
# Euroleague schedule dates look like "Oct 03, 2024".
_MONTHS = {m: i for i, m in enumerate(
    ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), 1)}


def _parse_date(s: str | None) -> str | None:
    """'Oct 03, 2024' → '2024-10-03'. Returns None on parse fail."""
    if not s:
        return None
    parts = s.replace(",", "").split()
    if len(parts) != 3:
        return None
    try:
        m = _MONTHS[parts[0][:3]]
        d = int(parts[1])
        y = int(parts[2])
        return f"{y:04d}-{m:02d}-{d:02d}"
    except (KeyError, ValueError):
        return None


# ── Teams ──────────────────────────────────────────────────
# Euroleague doesn't ship a separate teams endpoint; we derive the team
# list from the unique (code, name) pairs in the schedule. Idempotent
# on team code so re-running just refreshes names.

def ingest_teams(season_code: str = "E2024") -> int:
    """Walk the season schedule and upsert distinct teams. Team id is
    a stable hash of the team code so it survives schema rebuilds."""
    schedule_root = _fetch_xml(f"{_BASE}/schedules?seasonCode={season_code}")
    if schedule_root is None:
        return 0
    teams: dict[str, str] = {}
    for item in schedule_root.findall("item"):
        for code_tag, name_tag in (("homecode", "hometeam"),
                                    ("awaycode", "awayteam")):
            code = (item.findtext(code_tag) or "").strip()
            name = (item.findtext(name_tag) or "").strip()
            if code and name and code not in teams:
                teams[code] = name
    if not teams:
        return 0
    conn = get_conn("euroleague")
    tbl = teams_table("euroleague")
    n = 0
    for code, name in teams.items():
        team_id = _team_id_from_code(code)
        conn.execute(
            f"INSERT OR REPLACE INTO {tbl} "
            f"(id, name, abbreviation, city, venue, external_id) "
            f"VALUES (?, ?, ?, ?, ?, ?)",
            (team_id, name, code, "", "", code),
        )
        n += 1
    conn.commit()
    logger.info("[euroleague] ingested %d teams from %s", n, season_code)
    return n


def _team_id_from_code(code: str) -> int:
    """Stable int id for a team code. Used because the framework's
    schema expects integer team ids and Euroleague only ships codes."""
    # Pack the 3-letter code into a deterministic int. Codes are all
    # uppercase A-Z so this fits comfortably in 32 bits and never
    # collides across the ~25 Euroleague clubs.
    code = (code or "").upper().ljust(3)[:3]
    return ord(code[0]) * 65536 + ord(code[1]) * 256 + ord(code[2])


# ── Games ──────────────────────────────────────────────────

def _upsert_games_for_gameday(season_code: str, gameday: int) -> dict:
    """Pull schedule + results for one gameday and upsert into games."""
    sched = _fetch_xml(
        f"{_BASE}/schedules?seasonCode={season_code}&gameNumber={gameday}"
    )
    res = _fetch_xml(
        f"{_BASE}/results?seasonCode={season_code}&gameNumber={gameday}"
    )
    if sched is None:
        return {"ingested": 0, "skipped": 0}

    # Index results by gamecode for score lookup.
    scores: dict[str, dict] = {}
    if res is not None:
        for g in res.findall("game"):
            code = g.findtext("gamecode")
            if not code:
                continue
            scores[code] = {
                "home": _safe_int(g.findtext("homescore")),
                "away": _safe_int(g.findtext("awayscore")),
                "played": (g.findtext("played") or "").lower() == "true",
            }

    conn = get_conn("euroleague")
    g_tbl = games_table("euroleague")
    out = {"ingested": 0, "skipped": 0}
    for item in sched.findall("item"):
        gamecode = (item.findtext("gamecode") or "").strip()
        if not gamecode:
            out["skipped"] += 1
            continue
        date = _parse_date(item.findtext("date"))
        startime = (item.findtext("startime") or "").strip()
        # Euroleague returns startime as "HH:MM" in Europe/Belgrade-ish
        # local time. Resolve through zoneinfo so DST is handled correctly
        # (CEST = UTC+2 in May, CET = UTC+1 in winter). Without this the
        # ET conversion drifted by 1 hour each side of the DST boundary.
        iso_start = _euroleague_iso_start(date, startime)
        home_code = (item.findtext("homecode") or "").strip()
        away_code = (item.findtext("awaycode") or "").strip()
        if not (date and home_code and away_code):
            out["skipped"] += 1
            continue
        home_id = _team_id_from_code(home_code)
        away_id = _team_id_from_code(away_code)
        season = _season_from_code(season_code)
        score_row = scores.get(gamecode)
        played = (item.findtext("played") or "").lower() == "true"
        if score_row and score_row["played"]:
            status = "final"
            hs, asc = score_row["home"], score_row["away"]
        elif played:
            status = "final"
            hs = asc = None
        else:
            status = "scheduled"
            hs = asc = None

        conn.execute(
            f"INSERT OR REPLACE INTO {g_tbl} "
            f"(game_id, date, start_time, home_team_id, away_team_id, "
            f" home_score, away_score, status, season, external_id) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (gamecode, date, iso_start, home_id, away_id, hs, asc,
             status, season, gamecode),
        )
        out["ingested"] += 1
    conn.commit()
    return out


def _season_from_code(code: str) -> int:
    """'E2024' → 2024."""
    digits = ''.join(c for c in code if c.isdigit())
    return int(digits) if digits else 0


def _safe_int(v, default=None):
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def backfill(season_code: str = "E2024", max_gameday: int = 50,
              throttle_s: float = 0.5) -> dict:
    """Walk every gameday in a season and ingest. Stops early when 3
    consecutive gamedays return zero items (off-season tail).

    ``max_gameday`` defaults to 50 so playoffs (gamedays 39-43+ and
    Final Four) come through. Regular season alone is gamedays 1-38."""
    out = {"season": season_code, "gamedays": 0, "ingested": 0, "skipped": 0}
    empty_streak = 0
    for gd in range(1, max_gameday + 1):
        res = _upsert_games_for_gameday(season_code, gd)
        out["gamedays"] += 1
        out["ingested"] += res["ingested"]
        out["skipped"] += res["skipped"]
        if res["ingested"] == 0:
            empty_streak += 1
            if empty_streak >= 3:
                break
        else:
            empty_streak = 0
        time.sleep(throttle_s)
    logger.info("[euroleague] backfill %s: gamedays=%d ingested=%d",
                season_code, out["gamedays"], out["ingested"])
    return out


def ingest_today(date: str | None = None) -> dict:
    """Refresh today's-or-later schedule. Euroleague's API is gameday-
    keyed not date-keyed, so 'today' is implemented by re-walking the
    current season's schedule and filtering. Cheap enough — ~38
    gamedays per season; we only re-fetch the current + next 2."""
    season = _current_season_code()
    target_date = date or et_today_str()
    out = {"date": target_date, "ingested": 0, "skipped": 0,
           "cancelled": 0}
    # Pull whole schedule once, find which gameday(s) include today.
    schedule_root = _fetch_xml(f"{_BASE}/schedules?seasonCode={season}")
    if schedule_root is None:
        return out
    target_gamedays: set[int] = set()
    # Set of game numbers currently in the upstream schedule — used
    # to detect playoff games that got cancelled when a series ended
    # in a sweep (E2025_396 OLY@MCO disappeared after OLY went 3-0).
    # Euroleague exposes the per-game ID under <game> (and the trailing
    # part of <gamecode>); the <gamenumber> tag the earlier code read
    # is always empty in the live schedule feed (confirmed 2026-05-11),
    # so the prior check populated an empty set and the cancellation
    # branch never fired — phantom games E2025_400-402 sat scheduled.
    upstream_game_numbers: set[int] = set()
    for item in schedule_root.findall("item"):
        # Prefer the <game> tag, fall back to splitting <gamecode>.
        gcode = (item.findtext("gamecode") or "").strip()
        gnum_raw = (item.findtext("game") or "").strip()
        if not gnum_raw and "_" in gcode:
            gnum_raw = gcode.rsplit("_", 1)[-1]
        try:
            gnum = int(gnum_raw)
            if gnum:
                upstream_game_numbers.add(gnum)
        except (ValueError, TypeError):
            pass
        d = _parse_date(item.findtext("date"))
        if d == target_date:
            try:
                gd = int(item.findtext("gameday") or 0)
                if gd:
                    target_gamedays.add(gd)
            except ValueError:
                continue
    for gd in sorted(target_gamedays):
        res = _upsert_games_for_gameday(season, gd)
        out["ingested"] += res["ingested"]
        out["skipped"] += res["skipped"]
    # Mark any locally-scheduled games for the next 30 days that are
    # no longer in the upstream schedule as cancelled. Earlier the
    # window was 14 days, but a deep playoff series can span 20+ days
    # (every-other-day scheduling), and the older window started
    # silently cancelling Game 5+ once the series went past day 14.
    # Skips when the upstream list is empty (probable fetch error —
    # don't punish everything).
    if upstream_game_numbers:
        from datetime import timedelta
        from ._db import get_conn, games_table
        conn = get_conn("euroleague")
        gtbl = games_table("euroleague")
        cutoff = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        local = conn.execute(
            f"SELECT game_id FROM {gtbl} "
            f"WHERE status = 'scheduled' AND date >= ? AND date <= ? "
            f"  AND season = ?",
            (target_date, cutoff, _season_from_code(season)),
        ).fetchall()
        for row in local:
            gid = row["game_id"]
            try:
                num = int(str(gid).split("_")[-1])
            except (ValueError, IndexError):
                continue
            if num in upstream_game_numbers:
                continue
            conn.execute(
                f"UPDATE {gtbl} SET status = 'cancelled' WHERE game_id = ?",
                (gid,),
            )
            out["cancelled"] += 1
        if out["cancelled"]:
            conn.commit()
            logger.info("[euroleague] marked %d cancelled (no longer in "
                        "upstream schedule)", out["cancelled"])
    return out


def _current_season_code() -> str:
    """Euroleague seasons start in October. E2024 = 2024-25 season.
    Reads the pivot from the registry like other ingests so a future
    Euroleague schedule shift (e.g. start moves to September) only
    requires a registry edit, not a code edit."""
    from ._config import LEAGUE_REGISTRY
    months = (LEAGUE_REGISTRY.get("euroleague") or {}).get("season_months") or (10,)
    primary = months[0]
    now = datetime.now()
    start_year = now.year if now.month >= primary else now.year - 1
    return f"E{start_year}"


# ── CLI ────────────────────────────────────────────────────

def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.basketball._euroleague_ingest")
    ap.add_argument("--season", default=None,
                    help="Euroleague season code (E2024 = 2024-25). "
                         "Defaults to current.")
    ap.add_argument("--teams", action="store_true")
    ap.add_argument("--today", action="store_true")
    ap.add_argument("--date", default=None)
    ap.add_argument("--backfill", action="store_true",
                    help="Backfill the full season's schedule + results.")
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    season = args.season or _current_season_code()
    did = False
    if args.teams:
        n = ingest_teams(season)
        print(f"[euroleague] teams ingested: {n}")
        did = True
    if args.backfill:
        res = backfill(season)
        print(f"[euroleague] backfill: {res}")
        did = True
    if args.today or args.date:
        res = ingest_today(args.date)
        print(f"[euroleague] {args.date or 'today'}: {res}")
        did = True
    if not did:
        ap.error("specify --teams, --today, --date, or --backfill")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
