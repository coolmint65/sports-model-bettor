"""ESPN scoreboard ingest for baseball framework leagues.

Mirror of engine.football._espn_ingest. Auto-creates team stubs for
historical-only teams that ESPN's /teams endpoint no longer returns
(college baseball has heavy roster churn — graduating classes,
program shutdowns, etc.). Stale-game auto-sweep runs on every
ingest_today / backfill so postponed events don't leave phantom
'scheduled' rows.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

from . import get_league_config
from ._db import get_conn

logger = logging.getLogger(__name__)


_USER_AGENT = "sports-model-bettor/baseball"
_HEADERS = {"User-Agent": _USER_AGENT}


try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:
    _ET = None


def _et_date_for_iso(iso: str) -> str:
    if not iso:
        return ""
    try:
        s = iso.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
    except Exception:
        return iso[:10]
    if _ET is not None:
        return dt.astimezone(_ET).strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d")


def _today_et() -> str:
    if _ET is not None:
        return datetime.now(_ET).strftime("%Y-%m-%d")
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _fetch(url: str, *, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            logger.debug("[baseball] HTTPError %s: %s", e.code, url)
        except Exception as e:
            logger.debug("[baseball] fetch failed (%d/%d): %s",
                          attempt + 1, retries, e)
        time.sleep(0.4 * (attempt + 1))
    return None


def ingest_teams(league: str) -> int:
    cfg = get_league_config(league)
    espn_path = cfg.get("espn_league_path") or f"baseball/{league}"
    url = (f"https://site.api.espn.com/apis/site/v2/sports/"
            f"{espn_path}/teams?limit=400")
    data = _fetch(url)
    if not data:
        return 0
    teams = (data.get("sports", [{}])[0]
              .get("leagues", [{}])[0]
              .get("teams") or [])
    conn = get_conn(league)
    n = 0
    for entry in teams:
        team = entry.get("team", {})
        tid = team.get("id")
        if not tid:
            continue
        logos = team.get("logos") or []
        logo = logos[0].get("href", "") if logos else ""
        conn.execute(
            "INSERT OR REPLACE INTO teams "
            "(id, name, abbreviation, short_name, location, logo_url, "
            " updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, datetime('now'))",
            (
                int(tid),
                team.get("displayName") or "",
                team.get("abbreviation") or "",
                team.get("shortDisplayName") or "",
                team.get("location") or "",
                logo,
            ),
        )
        n += 1
    conn.commit()
    logger.info("[baseball:%s] ingest_teams: %d", league, n)
    return n


def _parse_status(event: dict) -> str:
    state = (event.get("status") or {}).get("type", {})
    name = (state.get("name") or "").upper()
    if "FINAL" in name:
        return "final"
    if "PROGRESS" in name or name == "STATUS_IN":
        return "live"
    if "POSTPONED" in name:
        return "postponed"
    if "CANCELED" in name or "CANCELLED" in name or "SUSPEND" in name:
        return "cancelled"
    return "scheduled"


def _safe_int(v) -> int | None:
    if v in (None, ""):
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def ingest_today(league: str, *, date: str | None = None) -> dict:
    cfg = get_league_config(league)
    espn_path = cfg.get("espn_league_path") or f"baseball/{league}"
    target = date or _today_et()
    # Walk today + 2 prior ET days. Without this backsweep, games caught
    # in 'live' status during yesterday's ingest stay 'live' forever
    # because ingest_today only fetched today's scoreboard. Same fix
    # the soccer ingest got 2026-05-21 — see _settler_audit_2026_05_21.md
    target_dt = datetime.strptime(target, "%Y-%m-%d")
    dates_to_walk = [
        (target_dt - timedelta(days=d)).strftime("%Y-%m-%d") for d in (0, 1, 2)
    ]
    all_data = []
    for d_iter in dates_to_walk:
        url = (f"https://site.api.espn.com/apis/site/v2/sports/"
                f"{espn_path}/scoreboard?dates={d_iter.replace('-', '')}&limit=400")
        d_data = _fetch(url)
        if d_data:
            all_data.append(d_data)
    if not all_data:
        return {"ingested": 0, "skipped": 0}
    data = all_data[0]  # primary (for season info)
    # Merge events from all walked days.
    events_combined = []
    for d_data in all_data:
        events_combined.extend(d_data.get("events") or [])
    data = {**data, "events": events_combined}

    conn = get_conn(league)
    if not conn.execute("SELECT 1 FROM teams LIMIT 1").fetchone():
        ingest_teams(league)

    events = data.get("events") or []
    ingested = 0
    skipped = 0
    season = (data.get("season") or {}).get("year")
    for ev in events:
        eid = str(ev.get("id") or "").strip()
        if not eid:
            skipped += 1
            continue
        comp = (ev.get("competitions") or [{}])[0]
        competitors = comp.get("competitors") or []
        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if not (home and away):
            skipped += 1
            continue
        h_id = int(home.get("team", {}).get("id") or 0)
        a_id = int(away.get("team", {}).get("id") or 0)
        if not (h_id and a_id):
            skipped += 1
            continue
        # Auto-stub teams missing from /teams. College baseball rosters
        # churn far more than NFL/UFL — old programs drop, new ones
        # join, and 6 of every 100 historical games reference a team
        # the live /teams endpoint no longer ships.
        for side in (home, away):
            t = side.get("team", {}) or {}
            tid = int(t.get("id") or 0)
            if not tid:
                continue
            if conn.execute(
                "SELECT 1 FROM teams WHERE id = ? LIMIT 1", (tid,)
            ).fetchone():
                continue
            logos = t.get("logos") or []
            logo = logos[0].get("href", "") if logos else (t.get("logo") or "")
            conn.execute(
                "INSERT OR IGNORE INTO teams "
                "(id, name, abbreviation, short_name, location, logo_url) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (tid,
                  t.get("displayName") or t.get("name") or f"team-{tid}",
                  t.get("abbreviation") or "",
                  t.get("shortDisplayName") or "",
                  t.get("location") or "",
                  logo),
            )
        h_score = _safe_int(home.get("score"))
        a_score = _safe_int(away.get("score"))
        status = _parse_status(ev)
        iso = comp.get("date") or ev.get("date") or ""
        et_date = _et_date_for_iso(iso) or target
        venue = (comp.get("venue") or {}).get("fullName") or ""
        # is_postseason marker — ESPN doesn't ship a clean flag for
        # college baseball, but the "season type" field on the event
        # is 3 for postseason. Used by the picker for conservative
        # juice handling on tournament games.
        season_type = (ev.get("season") or {}).get("type")
        is_post = 1 if season_type in (3, "3", "postseason") else 0
        conn.execute(
            "INSERT OR REPLACE INTO games "
            "(game_id, date, start_time, home_team_id, away_team_id, "
            " home_score, away_score, status, season, venue, "
            " is_postseason, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            (eid, et_date, iso, h_id, a_id, h_score, a_score, status,
              season, venue, is_post),
        )
        ingested += 1
    # Stale-game sweep — same pattern as soccer + football. Postponed/
    # relocated events drop out of the ESPN feed silently and would
    # otherwise hang as phantom 'scheduled' rows in our DB.
    n_cancelled = 0
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=2)
                   ).strftime("%Y-%m-%d")
        cur = conn.execute(
            "UPDATE games SET status = 'cancelled' "
            "WHERE date < ? AND status IN ('scheduled', 'live') "
            "  AND home_score IS NULL",
            (cutoff,),
        )
        n_cancelled = cur.rowcount
    except Exception as e:
        logger.debug("[baseball:%s] stale-sweep failed: %s", league, e)
    conn.commit()
    logger.info("[baseball:%s] ingest_today date=%s ingested=%d skipped=%d "
                "stale_cancelled=%d",
                league, target, ingested, skipped, n_cancelled)
    return {"ingested": ingested, "skipped": skipped, "date": target,
             "stale_cancelled": n_cancelled}


def backfill(league: str, start_date: str, end_date: str,
              *, throttle: float = 0.15) -> dict:
    try:
        cur = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        return {"error": f"bad date: {e}"}
    if cur > end:
        return {"error": "start > end"}
    totals = {"days": 0, "ingested": 0, "skipped": 0}
    while cur <= end:
        d = cur.strftime("%Y-%m-%d")
        res = ingest_today(league, date=d)
        totals["days"] += 1
        totals["ingested"] += res.get("ingested", 0)
        totals["skipped"] += res.get("skipped", 0)
        cur += timedelta(days=1)
        time.sleep(throttle)
    logger.info("[baseball:%s] backfill %s -> %s: %s",
                league, start_date, end_date, totals)
    return totals


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.baseball._espn_ingest")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_teams = sub.add_parser("teams"); p_teams.add_argument("league")
    p_today = sub.add_parser("today")
    p_today.add_argument("league")
    p_today.add_argument("--date", default=None)
    p_back = sub.add_parser("backfill")
    p_back.add_argument("league")
    p_back.add_argument("start")
    p_back.add_argument("end")
    p_back.add_argument("--throttle", type=float, default=0.15)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    if args.cmd == "teams":
        print({"teams": ingest_teams(args.league)})
    elif args.cmd == "today":
        print(ingest_today(args.league, date=args.date))
    elif args.cmd == "backfill":
        print(backfill(args.league, args.start, args.end,
                        throttle=args.throttle))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
