"""ESPN data ingest for basketball leagues whose ESPN path is set in
LEAGUE_REGISTRY. Currently covers NBA / WNBA / NCAAM / NCAAW — every
ESPN-tracked basketball league shares the same response shape so one
ingester handles them all, parameterized by ``league``.

Three entry points:

    ingest_teams(league)           — populate teams table
    ingest_today(league, date=None) — upsert today's scoreboard
    backfill(league, start, end)   — historical season pull

Data lands in the league's per-league DB at ``data/basketball/<league>.db``
(uniform schema: ``teams`` + ``games``). NBA continues to use its
existing legacy DB + scraper — this module is for the framework-managed
leagues only.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Iterable

from ._config import get_league_config
from ._db import get_conn, teams_table, games_table

logger = logging.getLogger(__name__)


# ── HTTP ─────────────────────────────────────────────────────

_USER_AGENT = "SportsBettor/1.0 (basketball-framework)"


def _fetch(url: str, retries: int = 3) -> dict | list | None:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": _USER_AGENT,
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            logger.warning("ESPN fetch %d/%d failed (%s): %s",
                           attempt + 1, retries, url, e)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def _espn_base(league: str) -> str:
    cfg = get_league_config(league)
    path = cfg.get("espn_league_path")
    if not path:
        raise ValueError(f"League {league!r} has no espn_league_path")
    return f"https://site.api.espn.com/apis/site/v2/sports/{path}"


# ── Date helpers ────────────────────────────────────────────

try:
    from zoneinfo import ZoneInfo
    _US_EASTERN = ZoneInfo("America/New_York")
except Exception:
    _US_EASTERN = None


def _us_eastern_date(iso_utc: str) -> str:
    """Convert ESPN's UTC ISO to US-Eastern calendar date. Mirrors the
    NBA scraper — every US league schedules by ET game-night so a 10pm
    ET tipoff that crosses midnight UTC must keep its night-of date."""
    if not iso_utc:
        return ""
    s = iso_utc.replace("Z", "+00:00") if iso_utc.endswith("Z") else iso_utc
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return iso_utc[:10]
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if _US_EASTERN is not None:
        return dt.astimezone(_US_EASTERN).strftime("%Y-%m-%d")
    # Fallback: month-keyed offset. EDT (UTC-4) approx mid-March
    # through early November; EST (UTC-5) otherwise.
    offset_hours = 4 if 3 <= dt.month <= 10 else 5
    return (dt - timedelta(hours=offset_hours)).strftime("%Y-%m-%d")


# ── Teams ───────────────────────────────────────────────────

def ingest_teams(league: str) -> int:
    """Pull the teams roster for ``league`` from ESPN and upsert into
    the per-league teams table. Returns the row count.

    For NCAAM (~360 D1 teams + lower divisions ESPN tracks for ~1100
    total) the site API caps at ~50 (top ranked). The core API exposes
    the full roster with pagination, so we route NCAAM through it.
    """
    if league in ("ncaam", "ncaaw"):
        return _ingest_teams_core_paginated(league)
    base = _espn_base(league)
    data = _fetch(f"{base}/teams")
    if not data:
        return 0
    sports = data.get("sports") or []
    if not sports:
        return 0
    leagues = sports[0].get("leagues") or []
    if not leagues:
        return 0
    teams = (leagues[0].get("teams") or [])
    conn = get_conn(league)
    tbl = teams_table(league)
    n = 0
    for entry in teams:
        team = entry.get("team") or {}
        if not team.get("id"):
            continue
        # ESPN's site API ships logos under team.logos[].href; the
        # primary club logo is index 0 (default variant). Falls back
        # to team.logo (single-string field on some leagues).
        logo_url = team.get("logo")
        if not logo_url:
            logos = team.get("logos") or []
            if isinstance(logos, list) and logos:
                logo_url = (logos[0] or {}).get("href")
        conn.execute(
            f"INSERT OR REPLACE INTO {tbl} "
            f"(id, name, abbreviation, city, venue, external_id, logo_url) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                int(team["id"]),
                team.get("displayName") or team.get("name") or "",
                team.get("abbreviation") or "",
                team.get("location") or "",
                ((team.get("venue") or {}).get("fullName")) or "",
                str(team["id"]),
                logo_url,
            ),
        )
        n += 1
    conn.commit()
    logger.info("[%s] ingested %d teams", league, n)
    return n


def _ingest_teams_core_paginated(league: str) -> int:
    """Pull the full team roster via the ESPN core API.

    The core API returns shallow refs that point at per-team detail
    URLs; we follow each ref to get name/abbreviation/etc. For NCAAM
    that's ~1100 detail fetches per refresh — runs in ~5 min at the
    default 0.05s throttle. Refresh is rare (preseason + roster
    moves) so the cost is acceptable.
    """
    season = _current_season_year(league)
    sport_path = "basketball/leagues/" + (
        "mens-college-basketball" if league == "ncaam" else "womens-college-basketball"
    )
    conn = get_conn(league)
    tbl = teams_table(league)
    n_total = 0
    page = 1
    while True:
        list_url = (
            f"https://sports.core.api.espn.com/v2/sports/{sport_path}/"
            f"seasons/{season}/teams?limit=500&page={page}"
        )
        data = _fetch(list_url)
        if not data:
            break
        items = data.get("items") or []
        if not items:
            break
        for ref in items:
            ref_url = ref.get("$ref")
            if not ref_url:
                continue
            team = _fetch(ref_url)
            if not team or not team.get("id"):
                continue
            logo_url = team.get("logo")
            if not logo_url:
                logos = team.get("logos") or []
                if isinstance(logos, list) and logos:
                    logo_url = (logos[0] or {}).get("href")
            conn.execute(
                f"INSERT OR REPLACE INTO {tbl} "
                f"(id, name, abbreviation, city, venue, external_id, logo_url) "
                f"VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    int(team["id"]),
                    team.get("displayName") or team.get("name") or "",
                    team.get("abbreviation") or "",
                    team.get("location") or "",
                    ((team.get("venue") or {}).get("fullName")) or "",
                    str(team["id"]),
                    logo_url,
                ),
            )
            n_total += 1
            time.sleep(0.05)
        page_count = data.get("pageCount") or 1
        if page >= page_count:
            break
        page += 1
    conn.commit()
    logger.info("[%s] ingested %d teams via core API (paginated)",
                league, n_total)
    return n_total


# ── Games / scoreboard ──────────────────────────────────────

def _resolve_team_id(league: str, team: dict) -> tuple[int, bool]:
    """Map an ESPN scoreboard team payload to our internal team_id.

    ESPN uses different team-id namespaces per sub-league:
      - Vegas Summer League returns parent NBA franchise IDs (1..30)
      - California / Utah Summer League return their OWN team IDs
        (110xxx, 132xxx) that DON'T map to the parent franchise table
      - Bracket-TBD rows ship id = -1 / -2 with abbr='TBD'

    Strategy:
      1. TBD rows -> (0, False) so caller skips the game entirely.
      2. If ESPN's team id already exists in our teams table, use it
         (the fast/common path).
      3. Fall back to abbreviation lookup — California SL Warriors
         (id=132761, abbr='GS') resolves to our GS row (id=9).
      4. Last resort: upsert a stub row with ESPN's id + whatever
         metadata we have so downstream joins at least render *something*
         instead of "None@None".
    """
    tbl = teams_table(league)
    conn = get_conn(league)
    tid_raw = team.get("id")
    try:
        tid = int(tid_raw) if tid_raw is not None else 0
    except (ValueError, TypeError):
        tid = 0
    abbr = (team.get("abbreviation") or "").strip().upper()
    # 1. Placeholder rows (TBD bracket).
    if tid < 0 or abbr == "TBD":
        return (0, False)
    if not tid:
        return (0, False)
    # 2. Fast path: id already known.
    hit = conn.execute(
        f"SELECT id FROM {tbl} WHERE id = ? LIMIT 1", (tid,),
    ).fetchone()
    if hit:
        return (tid, True)
    # 3. Abbreviation → existing team_id (handles the California /
    # Utah SL case where the sub-league ships franchise-alias teams
    # with different ids but the same abbreviation).
    if abbr:
        alias = conn.execute(
            f"SELECT id FROM {tbl} WHERE UPPER(abbreviation) = ? "
            f"ORDER BY id LIMIT 1",
            (abbr,),
        ).fetchone()
        if alias:
            return (int(alias["id"]), True)
    # 4. Upsert a stub row so we at least persist enough for the
    # frontend to render abbrev + name. Better than dropping the game.
    name = team.get("displayName") or team.get("name") or abbr or f"Team {tid}"
    logo = team.get("logo")
    if not logo:
        logos = team.get("logos") or []
        if isinstance(logos, list) and logos:
            logo = (logos[0] or {}).get("href")
    try:
        conn.execute(
            f"INSERT OR IGNORE INTO {tbl} "
            f"(id, name, abbreviation, city, venue, external_id, logo_url) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?)",
            (tid, name, abbr, team.get("location") or "",
             ((team.get("venue") or {}).get("fullName")) or "",
             str(tid), logo),
        )
    except Exception as e:
        logger.debug("[%s] team stub upsert failed for id=%s: %s",
                     league, tid, e)
        return (0, False)
    return (tid, True)


def _upsert_game(league: str, ev: dict) -> bool:
    """Upsert one ESPN event into the games table. Returns True if a row
    was written."""
    conn = get_conn(league)
    g_tbl = games_table(league)
    comp = (ev.get("competitions") or [{}])[0]
    competitors = comp.get("competitors") or []
    if len(competitors) < 2:
        return False
    home = next((c for c in competitors if c.get("homeAway") == "home"), None)
    away = next((c for c in competitors if c.get("homeAway") == "away"), None)
    if not home or not away:
        return False
    home_id, home_ok = _resolve_team_id(league, home.get("team") or {})
    away_id, away_ok = _resolve_team_id(league, away.get("team") or {})
    if not home_ok or not away_ok:
        # TBD-bracket rows (id=-1/-2, abbr=TBD) and any unresolvable
        # ESPN team ship as "ghost games" that render as None@None on
        # the frontend + can't be settled. Skip the insert entirely
        # rather than persisting a row with unresolvable team_ids.
        logger.debug(
            "[%s] skipping game %s: unresolvable team (home=%s/%s away=%s/%s)",
            league, ev.get("id"),
            (home.get("team") or {}).get("id"),
            (home.get("team") or {}).get("abbreviation"),
            (away.get("team") or {}).get("id"),
            (away.get("team") or {}).get("abbreviation"),
        )
        return False
    state = ((ev.get("status") or {}).get("type") or {}).get("state") or "pre"
    status = "final" if state == "post" else ("in" if state == "in" else "scheduled")
    home_score = _safe_int(home.get("score"))
    away_score = _safe_int(away.get("score"))
    # Period splits — ESPN ships each period on competitor.linescores,
    # including the in-progress period's running total. Persisting that
    # partial value into home_q{N}/away_q{N} makes the live-pick scope
    # resolver believe period N has closed (any non-null value reads as
    # "final period score"), which settles a Q2 pick mid-Q2 against the
    # partial score. Clip the linescore tail to *closed* periods only —
    # the live overlay will fill in the open period when it actually
    # ends.
    cur_period = _safe_int((ev.get("status") or {}).get("period"), default=0)
    # STATUS_END_PERIOD / STATUS_HALFTIME mean the current period just
    # closed (clock 0:00 in the between-period gap) — persist its score
    # even though state is still "in".
    status_name = ((ev.get("status") or {}).get("type") or {}).get("name", "")
    intermission = status_name in ("STATUS_END_PERIOD", "STATUS_HALFTIME")
    home_quarters = _periods(home.get("linescores") or [],
                              status=status, cur_period=cur_period,
                              intermission=intermission)
    away_quarters = _periods(away.get("linescores") or [],
                              status=status, cur_period=cur_period,
                              intermission=intermission)
    season = (ev.get("season") or {}).get("year") or _current_season_year(league)
    game_id = str(ev.get("id"))
    iso_date = ev.get("date") or comp.get("date") or ""
    date = _us_eastern_date(iso_date)

    conn.execute(
        f"INSERT OR REPLACE INTO {g_tbl} "
        f"(game_id, date, start_time, home_team_id, away_team_id, "
        f" home_score, away_score, "
        f" home_q1, away_q1, home_q2, away_q2, "
        f" home_q3, away_q3, home_q4, away_q4, "
        f" status, season, external_id) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            game_id, date, iso_date, home_id, away_id,
            # Persist scores for in-progress games too — the live card
            # shows 0-0 instead of the actual scoreline otherwise. Only
            # 'pre' (scheduled) gets the null treatment because no
            # score exists yet at that point.
            home_score if status in ("final", "in") else None,
            away_score if status in ("final", "in") else None,
            home_quarters[0], away_quarters[0],
            home_quarters[1], away_quarters[1],
            home_quarters[2], away_quarters[2],
            home_quarters[3], away_quarters[3],
            status, int(season), game_id,
        ),
    )
    return True


def _periods(linescores: list[dict], *, status: str = "final",
              cur_period: int = 0, intermission: bool = False) -> list[int | None]:
    """Return up to 4 period scores padded with None. For final games
    every linescore is final and gets persisted. For in-progress games
    only periods strictly before ``cur_period`` are closed — the current
    period's value is a running snapshot, not a final score, and writing
    it would let the live settle path resolve period markets against
    partial scores. ``intermission`` (clock 0:00, end-of-period state)
    promotes the current period to closed too."""
    out: list[int | None] = [None, None, None, None]
    if status == "final":
        closed_through = 4
    elif intermission:
        closed_through = cur_period
    else:
        closed_through = max(cur_period - 1, 0)
    for i, ls in enumerate(linescores[:4]):
        if i >= closed_through:
            break
        out[i] = _safe_int(ls.get("value"), default=None)
    return out


def _safe_int(v, default=0):
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _current_season_year(league: str) -> int:
    cfg = get_league_config(league)
    months = cfg.get("season_months") or (10,)
    primary = months[0]
    now = datetime.now()
    return now.year if now.month >= primary else now.year - 1


def ingest_today(league: str, date: str | None = None) -> dict:
    """Pull a single date's scoreboard. Returns ``{ingested, skipped}``.

    Also re-pulls the prior 2 days so games caught mid-action ("in"
    status) get their final score + 'post' status caught. Without this
    backsweep, WNBA CON @ POR 5/18 sat at status='in' with score 64-64
    forever because ingest_today only fetched today.

    When ``date`` is None and the league is far-east (Asia/Oceania), this
    also pulls tomorrow ET. Their local tipoffs (7 PM AEDT/JST/KST) land
    at 3-7 AM ET the next day, so games their fans consider "tonight"
    only appear on tomorrow's ESPN scoreboard. AFL is the current ESPN-
    backed far-east league; the rule applies to any future onboardings.
    """
    if date is None:
        out = _ingest_one_date(league, None)
        # Backsweep 2 prior days for stale-status refresh.
        today_dt = datetime.now()
        for back in (1, 2):
            d = (today_dt - timedelta(days=back)).strftime("%Y-%m-%d")
            sub = _ingest_one_date(league, d)
            out["ingested"] += sub.get("ingested", 0)
            out["skipped"] += sub.get("skipped", 0)
        from ._config import is_far_east
        if is_far_east(league):
            tmrw = (today_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            sub = _ingest_one_date(league, tmrw)
            out["ingested"] += sub.get("ingested", 0)
            out["skipped"] += sub.get("skipped", 0)
        return out
    return _ingest_one_date(league, date)


def _ingest_one_date(league: str, date: str | None) -> dict:
    # NCAAM (and NCAAW) span ~360 D1 teams — without the ``groups=50``
    # filter ESPN's site API only returns top-25-ranked games (5-15/day
    # vs the actual ~50-100). 50 = NCAA Division I; 51 would be D2 etc.
    # Limit=500 covers the busiest tournament days.
    extra = "&groups=50&limit=500" if league in ("ncaam", "ncaaw") else ""
    # Multi-slug leagues (NBA Summer League bundles Las Vegas +
    # California Classic + Salt Lake City under one HR comp). The
    # primary path stays in ``espn_league_path``; siblings live in
    # ``espn_extra_paths``. Each ESPN slug maps to a distinct
    # scoreboard endpoint; iterate them and union the events.
    cfg = get_league_config(league)
    paths = [cfg.get("espn_league_path")]
    for p in (cfg.get("espn_extra_paths") or []):
        if p and p not in paths:
            paths.append(p)
    out = {"ingested": 0, "skipped": 0}
    for path in paths:
        if not path:
            continue
        base = f"https://site.api.espn.com/apis/site/v2/sports/{path}"
        if date:
            url = f"{base}/scoreboard?dates={date.replace('-', '')}{extra}"
        else:
            url = f"{base}/scoreboard?{extra.lstrip('&')}" if extra else f"{base}/scoreboard"
        data = _fetch(url)
        if not data:
            continue
        for ev in data.get("events") or []:
            try:
                wrote = _upsert_game(league, ev)
            except Exception as e:
                logger.warning("[%s] event %s upsert failed: %s",
                               league, ev.get("id"), e)
                wrote = False
            if wrote:
                out["ingested"] += 1
            else:
                out["skipped"] += 1
    get_conn(league).commit()
    logger.info("[%s] ingest_today date=%s ingested=%d skipped=%d",
                league, date or "today", out["ingested"], out["skipped"])
    return out


def backfill(league: str, start_date: str, end_date: str,
              throttle_s: float = 0.25) -> dict:
    """Walk ``start_date`` .. ``end_date`` (inclusive) and ingest each day.

    Throttles ESPN at 4 req/sec by default — generous; ESPN tolerates
    much more but we play nice. Returns aggregate ``{days, ingested,
    skipped}``."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    if end < start:
        return {"days": 0, "ingested": 0, "skipped": 0}
    out = {"days": 0, "ingested": 0, "skipped": 0}
    cur = start
    while cur <= end:
        d = cur.strftime("%Y-%m-%d")
        res = ingest_today(league, date=d)
        out["days"] += 1
        out["ingested"] += res.get("ingested", 0)
        out["skipped"] += res.get("skipped", 0)
        cur += timedelta(days=1)
        time.sleep(throttle_s)
    return out


# ── CLI ─────────────────────────────────────────────────────

def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.basketball._espn_ingest")
    ap.add_argument("league", help="Registry key (wnba / ncaam / ncaaw)")
    ap.add_argument("--teams", action="store_true",
                    help="Ingest the teams roster only.")
    ap.add_argument("--today", action="store_true",
                    help="Ingest today's scoreboard.")
    ap.add_argument("--date", default=None,
                    help="Ingest a single specific date (YYYY-MM-DD).")
    ap.add_argument("--backfill", nargs=2, metavar=("START", "END"),
                    help="Date range to backfill (inclusive).")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    did = False
    if args.teams:
        n = ingest_teams(args.league)
        print(f"[{args.league}] teams ingested: {n}")
        did = True
    if args.today or args.date:
        res = ingest_today(args.league, date=args.date)
        print(f"[{args.league}] {args.date or 'today'}: {res}")
        did = True
    if args.backfill:
        res = backfill(args.league, args.backfill[0], args.backfill[1])
        print(f"[{args.league}] backfill: {res}")
        did = True
    if not did:
        ap.error("specify --teams, --today, --date, or --backfill")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
