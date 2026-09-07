"""ESPN scoreboard ingest for football leagues.

Pulls teams + games for one ``league`` (e.g., ``ufl``). Same pattern
as ``engine.soccer._espn_ingest`` and ``engine.hockey`` ingest:

  - ``ingest_teams(league)`` walks the /teams endpoint once per day
    to refresh roster metadata (logo, abbr, name).
  - ``ingest_today(league)`` pulls the /scoreboard endpoint for the
    current ET day, writing scheduled + final games into the DB.
  - ``backfill(league, start, end)`` walks day-by-day across a date
    range. ESPN doesn't ship a multi-day scoreboard endpoint for
    football leagues so we paginate at 200ms throttle.
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


_USER_AGENT = "curl/8.4.0"
_HEADERS = {"User-Agent": _USER_AGENT}


try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:
    _ET = None


def _et_date_for_iso(iso: str) -> str:
    """Convert an ESPN ISO timestamp to its ET calendar date. ESPN
    ships UTC; a 7 PM ET kickoff (23:00 UTC) is "today" not "tomorrow"
    in the user's mind. Falls back to the UTC date when zoneinfo is
    unavailable."""
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
            logger.debug("[football] HTTPError %s: %s", e.code, url)
        except Exception as e:
            logger.debug("[football] fetch failed (%d/%d): %s",
                          attempt + 1, retries, e)
        time.sleep(0.4 * (attempt + 1))
    return None


def ingest_teams(league: str) -> int:
    """Refresh team metadata. Idempotent — INSERT OR REPLACE keyed on
    the ESPN team id."""
    cfg = get_league_config(league)
    espn_path = cfg.get("espn_league_path") or f"football/{league}"
    url = (f"https://site.api.espn.com/apis/site/v2/sports/"
            f"{espn_path}/teams" + cfg.get("espn_teams_suffix", ""))
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
        logo = ""
        logos = team.get("logos") or []
        if logos:
            logo = logos[0].get("href", "")
        # 2026-09-07: UPSERT (was INSERT OR REPLACE) so ESPN's blank /teams
        # logos never clobber a good logo the scoreboard path already healed.
        conn.execute(
            "INSERT INTO teams "
            "(id, name, abbreviation, short_name, location, logo_url, "
            " updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, datetime('now')) "
            "ON CONFLICT(id) DO UPDATE SET "
            "  name=excluded.name, "
            "  abbreviation=excluded.abbreviation, "
            "  short_name=excluded.short_name, "
            "  location=excluded.location, "
            "  logo_url=CASE WHEN excluded.logo_url != '' "
            "                THEN excluded.logo_url ELSE teams.logo_url END, "
            "  updated_at=datetime('now')",
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
    logger.info("[football:%s] ingest_teams: %d", league, n)
    return n


def _parse_status(event: dict) -> str:
    state = (event.get("status") or {}).get("type", {})
    name = (state.get("name") or "").upper()
    if "FINAL" in name:
        return "final"
    if "IN_PROGRESS" in name or "PROGRESS" in name or name == "STATUS_IN":
        return "live"
    if "POSTPONED" in name:
        return "postponed"
    if "CANCELED" in name or "CANCELLED" in name:
        return "cancelled"
    return "scheduled"


def ingest_today(league: str, *, date: str | None = None,
                 extra_query: str = "") -> dict:
    """Pull one day's scoreboard. ``date`` is YYYY-MM-DD in ET; when
    omitted, today's ET date."""
    cfg = get_league_config(league)
    espn_path = cfg.get("espn_league_path") or f"football/{league}"
    target = date or _today_et()
    yyyymmdd = target.replace("-", "")
    # 2026-09-03: ESPN's CFB scoreboard defaults to the featured/Top-25 slate
    # (~10-20 games/day). The registry's ``espn_scoreboard_suffix`` opts a
    # league into the full board (CFB: ``&groups=80&limit=400`` = all FBS,
    # incl. FBS-vs-FCS) so backfill/Elo see every game HR prices.
    # 2026-09-03: a league may span several ESPN "groups" boards (CFB: 80=FBS, 81=FCS — HR
    # prices FCS-vs-FCS games that the default FBS board omits; Austin noticed the gap on
    # 9/3). Fetch each board and merge the events. An explicit extra_query replaces the
    # registry's boards (ESPN honours the first `groups=`).
    base = (f"https://site.api.espn.com/apis/site/v2/sports/"
            f"{espn_path}/scoreboard?dates={yyyymmdd}")
    if extra_query:
        suffixes = [extra_query]
    else:
        groups = cfg.get("espn_scoreboard_groups")
        suffixes = ([f"&groups={g}&limit=400" for g in groups] if groups
                    else [cfg.get("espn_scoreboard_suffix") or ""])
    data = None
    merged: list = []
    seen: set = set()
    for sfx in suffixes:
        d = _fetch(base + sfx)
        if not d:
            continue
        if data is None:
            data = d
        for ev in (d.get("events") or []):
            eid = str(ev.get("id") or "")
            if eid and eid not in seen:
                seen.add(eid); merged.append(ev)
    if not data:
        return {"ingested": 0, "skipped": 0}
    data["events"] = merged

    conn = get_conn(league)
    # Make sure teams are present + logos fresh. The old gate ran only when
    # the table was EMPTY, so NFL (seeded logo-less on first ingest) never
    # got its logos and the tracker showed initials. Refresh on a ~daily TTL
    # (also covers the empty case: "" < stale). Day-bounded so a team
    # permanently absent from the /teams roster cannot cause a refetch loop.
    _last_team_sync = conn.execute(
        "SELECT MAX(updated_at) FROM teams"
    ).fetchone()[0] or ""
    _team_stale = (datetime.utcnow() - timedelta(hours=20)).strftime(
        "%Y-%m-%d %H:%M:%S")
    if _last_team_sync < _team_stale:
        ingest_teams(league)

    events = data.get("events") or []
    ingested = 0
    skipped = 0
    season = (data.get("season") or {}).get("year")
    skip_pre = bool(cfg.get("skip_preseason"))
    for ev in events:
        eid = str(ev.get("id") or "").strip()
        if not eid:
            skipped += 1
            continue
        # 2026-09-03: NFL preseason results are noise for Elo (starters sit) — skip them
        # for leagues flagged skip_preseason. ESPN: event.season.type 1=pre, 2=reg, 3=post.
        if skip_pre and ((ev.get("season") or {}).get("type") == 1):
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
        # UFL has had team renames/relocations across seasons
        # (Arlington Renegades → Dallas Renegades, etc.). Historical
        # backfill rows can reference a team id no longer in the
        # /teams roster. Auto-create a stub so FK doesn't fail.
        for side in (home, away):
            t = side.get("team", {}) or {}
            tid = int(t.get("id") or 0)
            if not tid:
                continue
            logos = t.get("logos") or []
            logo = logos[0].get("href", "") if logos else (t.get("logo") or "")
            existing = conn.execute(
                "SELECT logo_url FROM teams WHERE id = ? LIMIT 1", (tid,)
            ).fetchone()
            if existing:
                # 2026-09-07: heal a previously logo-less stub (FCS team or
                # relocation first seen via the scoreboard, which enters
                # blank) when this event carries a logo. Never overwrite a
                # good logo with a blank.
                if logo and not (existing[0] or ""):
                    conn.execute(
                        "UPDATE teams SET logo_url = ?, "
                        "  updated_at = datetime('now') WHERE id = ?",
                        (logo, tid),
                    )
                continue
            conn.execute(
                "INSERT OR IGNORE INTO teams "
                "(id, name, abbreviation, short_name, location, logo_url, "
                " updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, datetime('now'))",
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
        # ESPN's `date` is UTC. Store ET-derived date so the slate
        # filter doesn't drop late-evening games.
        iso = comp.get("date") or ev.get("date") or ""
        et_date = _et_date_for_iso(iso) or target
        venue = (comp.get("venue") or {}).get("fullName") or ""
        week = ((ev.get("week") or {}).get("number") or None)
        conn.execute(
            "INSERT OR REPLACE INTO games "
            "(game_id, date, start_time, home_team_id, away_team_id, "
            " home_score, away_score, status, season, week, venue, "
            " updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            (eid, et_date, iso, h_id, a_id, h_score, a_score, status,
              season, week, venue),
        )
        ingested += 1
    conn.commit()
    logger.info("[football:%s] ingest_today date=%s ingested=%d skipped=%d",
                league, target, ingested, skipped)
    return {"ingested": ingested, "skipped": skipped, "date": target}


def backfill(league: str, start_date: str, end_date: str,
              *, throttle: float = 0.2, extra_query: str = "") -> dict:
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
        res = ingest_today(league, date=d, extra_query=extra_query)
        totals["days"] += 1
        totals["ingested"] += res.get("ingested", 0)
        totals["skipped"] += res.get("skipped", 0)
        cur += timedelta(days=1)
        time.sleep(throttle)
    logger.info("[football:%s] backfill %s → %s: %s",
                league, start_date, end_date, totals)
    return totals


def _safe_int(v) -> int | None:
    if v in (None, ""):
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.football._espn_ingest")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_teams = sub.add_parser("teams"); p_teams.add_argument("league")
    p_today = sub.add_parser("today")
    p_today.add_argument("league")
    p_today.add_argument("--date", default=None)
    p_back = sub.add_parser("backfill")
    p_back.add_argument("league")
    p_back.add_argument("start")
    p_back.add_argument("end")
    p_back.add_argument("--throttle", type=float, default=0.2)
    # e.g. "&groups=81&limit=400" = ESPN's FCS board (default = FBS) — 2026-09-03
    p_back.add_argument("--extra", default="")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    if args.cmd == "teams":
        print({"teams": ingest_teams(args.league)})
    elif args.cmd == "today":
        print(ingest_today(args.league, date=args.date))
    elif args.cmd == "backfill":
        print(backfill(args.league, args.start, args.end,
                        throttle=args.throttle, extra_query=args.extra))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
