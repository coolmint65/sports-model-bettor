"""ESPN data ingest for every soccer league whose ``espn_league_path``
is set in LEAGUE_REGISTRY.

ESPN's site API has consistent shape across all soccer competitions —
clubs, internationals, men's, women's. One ingester handles them all,
parameterized by ``league``.

Three entry points:

    ingest_teams(league)
    ingest_today(league, date=None)
    backfill(league, start, end)

Data lands in ``data/soccer/<league>.db`` via the framework's uniform
schema. Match scores are stored as a (home, away) pair (not margin) so
every soccer market — 1X2, OU, BTTS — derives from one row.

Date handling: matches are dated by their UTC kickoff calendar day.
Soccer fixtures span every continent, so US-Eastern day-of (the
basketball convention) would mis-bucket Asian/European/Australian
kickoffs. UTC keeps the framework time-zone-neutral; the frontend can
re-localize at render time.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.request
from datetime import datetime, timedelta, timezone

from ._config import get_league_config
from ._db import get_conn

logger = logging.getLogger(__name__)


# ── HTTP ─────────────────────────────────────────────────────

_USER_AGENT = "SportsBettor/1.0 (soccer-framework)"


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
    return f"https://site.api.espn.com/apis/site/v2/sports/soccer/{path}"


def _espn_paths(league: str) -> list[str]:
    """All ESPN paths for ``league`` — the primary plus any ``espn_extra_paths``.
    fifa_internationals uses this to pool every confederation's qualifiers
    + friendlies into one DB so a single Elo ladder spans the global game."""
    cfg = get_league_config(league)
    out = [cfg["espn_league_path"]]
    out.extend(cfg.get("espn_extra_paths") or [])
    return out


# ── Helpers ─────────────────────────────────────────────────

try:
    from zoneinfo import ZoneInfo
    _US_EASTERN = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover — zoneinfo always available on 3.9+
    _US_EASTERN = None


def _utc_date(iso_utc: str) -> str:
    """ISO UTC → US-Eastern calendar date 'YYYY-MM-DD'. Matches the
    basketball / hockey convention so a 10pm ET MLS kickoff that
    crosses 00:00 UTC stays on its night-of date instead of jumping
    to the next day. International matches (European leagues, AFC)
    that play in the afternoon UTC are unaffected because their UTC
    date == ET date once the wraparound is corrected.

    Function name kept stable for back-compat with existing callers."""
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


def _safe_int(v, default=None):
    if v is None or v == "":
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _current_season_year(league: str) -> int:
    """Best-guess current season-year. ESPN's "season.year" usually wins
    when it's on the event — this only fires when the field is missing."""
    cfg = get_league_config(league)
    months = cfg.get("season_months") or (1,)
    primary = months[0]
    now = datetime.now(timezone.utc)
    return now.year if now.month >= primary else now.year - 1


# ── Teams ───────────────────────────────────────────────────

def ingest_teams(league: str) -> int:
    """Pull club roster for ``league`` from ESPN and upsert."""
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
    teams = leagues[0].get("teams") or []
    conn = get_conn(league)
    n = 0
    for entry in teams:
        team = entry.get("team") or {}
        if not team.get("id"):
            continue
        logo_url = team.get("logo")
        if not logo_url:
            logos = team.get("logos") or []
            if isinstance(logos, list) and logos:
                logo_url = (logos[0] or {}).get("href")
        # ESPN ships nationality on national-team rosters via team.location.
        # For club teams location is the city — same column, ambiguous use,
        # but consistent within a league.
        conn.execute(
            "INSERT OR REPLACE INTO teams "
            "(id, name, short_name, abbreviation, country, logo_url, "
            " primary_color, external_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                int(team["id"]),
                team.get("displayName") or team.get("name") or "",
                team.get("shortDisplayName") or team.get("name") or "",
                team.get("abbreviation") or "",
                team.get("location") or "",
                logo_url,
                team.get("color"),
                str(team["id"]),
            ),
        )
        n += 1
    conn.commit()
    logger.info("[%s] ingested %d teams", league, n)
    return n


# ── Matches / scoreboard ────────────────────────────────────

def _upsert_match(league: str, ev: dict) -> bool:
    """Upsert one ESPN event into matches. Returns True when written."""
    conn = get_conn(league)
    comp = (ev.get("competitions") or [{}])[0]
    competitors = comp.get("competitors") or []
    if len(competitors) < 2:
        return False
    home = next((c for c in competitors if c.get("homeAway") == "home"), None)
    away = next((c for c in competitors if c.get("homeAway") == "away"), None)
    if not home or not away:
        return False
    home_team = home.get("team") or {}
    away_team = away.get("team") or {}
    home_id = _safe_int(home_team.get("id"))
    away_id = _safe_int(away_team.get("id"))
    if not home_id or not away_id:
        return False

    # Side-effect: when the team isn't in our teams table yet (cup games
    # often bring in foreign opposition before ingest_teams sees them),
    # synthesize a stub row so the FK from matches.home_team_id resolves.
    _ensure_team_stub(conn, home_team)
    _ensure_team_stub(conn, away_team)

    status_block = (ev.get("status") or {}).get("type") or {}
    state = status_block.get("state") or "pre"
    if state == "post":
        status = "final"
    elif state == "in":
        status = "live"
    elif status_block.get("name") == "STATUS_POSTPONED":
        status = "postponed"
    elif status_block.get("name") == "STATUS_CANCELLED":
        status = "cancelled"
    else:
        status = "scheduled"
    status_detail = status_block.get("shortDetail") or ""

    # Score handling — books settle OU / BTTS / ML on REGULATION only
    # (90 minutes + stoppage), NOT after extra time. ESPN's
    # ``competitor.score`` is the aggregate final incl. ET, so for AET
    # matches we have to derive regulation from the linescores array.
    # linescores[0] = 1st half, [1] = 2nd half, [2] = ET1, [3] = ET2.
    # Regulation = linescores[0] + linescores[1]. Non-AET matches have
    # only H1/H2 populated so summing the whole array matches score
    # anyway — safe to always prefer the derived value when a
    # linescores array is present.
    def _reg_score_from_linescores(comp: dict) -> int | None:
        ls = comp.get("linescores") or []
        if not ls:
            return None
        # ESPN sometimes only ships H1 and no H2 mid-match; leave
        # regulation as None then so we don't stamp a partial value.
        if len(ls) < 2:
            return None
        total = 0
        for i in range(min(2, len(ls))):
            try:
                total += int((ls[i] or {}).get("displayValue") or 0)
            except (TypeError, ValueError):
                return None
        return total

    detail = (status_block.get("detail") or "").upper()
    is_aet_or_pen = detail in ("AET", "PEN") or "AET" in detail or "PEN" in detail
    if is_aet_or_pen:
        home_reg = _reg_score_from_linescores(home)
        away_reg = _reg_score_from_linescores(away)
        home_score = (home_reg if home_reg is not None
                      else _safe_int(home.get("score")))
        away_score = (away_reg if away_reg is not None
                      else _safe_int(away.get("score")))
    else:
        home_score = _safe_int(home.get("score"))
        away_score = _safe_int(away.get("score"))
    season = _safe_int((ev.get("season") or {}).get("year"),
                        default=_current_season_year(league))
    iso_kickoff = ev.get("date") or comp.get("date") or ""
    date = _utc_date(iso_kickoff)
    match_id = _safe_int(ev.get("id"))
    if not match_id:
        return False

    # Round/matchweek metadata. ESPN ships round info in two places
    # depending on competition shape:
    #   * League competitions use ``event.week.text`` ("Matchweek 12")
    #   * Cup/tournament competitions use ``event.season.slug``
    #     ("group-stage", "round-of-32", "quarterfinal", "final"). The
    #     ``week`` block is null on these, so reading only ``week.text``
    #     left every World Cup row with round_name="" — knockout-stage
    #     awareness gone. Prefer the explicit week.text when present,
    #     fall through to season.slug for cup formats.
    week_block = ev.get("week") or {}
    matchweek = _safe_int(week_block.get("number"))
    round_name = (week_block.get("text")
                  or (ev.get("season") or {}).get("slug")
                  or "")

    # Neutral-site detection. ESPN ships ``competition.neutralSite``
    # for some leagues but leaves it null on the World Cup — every WC
    # row was landing as non-neutral even when Czechia played in
    # Estadio Banorte (Mexico City), inverting which side got the
    # home_advantage tilt in the predictor. Derive from venue country:
    # when the venue isn't in either team's home country it's neutral;
    # when it matches the away team's country the away side is the
    # true home (handled by the predictor when neutral_site=False but
    # the model treats whichever side ESPN labels "home" as home —
    # we flip the neutral flag on so it nets to zero rather than
    # giving the wrong side a tilt). Falls back to the explicit
    # neutralSite flag when team-country isn't populated.
    venue_block = comp.get("venue") or {}
    venue = venue_block.get("fullName") or ""
    venue_country = ((venue_block.get("address") or {}).get("country")
                     or "").strip()
    home_country = (home_team.get("location")
                    or home_team.get("displayName") or "").strip()
    away_country = (away_team.get("location")
                    or away_team.get("displayName") or "").strip()
    # ESPN ships team.location as the long form ("United States") but
    # venue.address.country as the short form ("USA"). A naive case-
    # insensitive compare misses this — USA-Bosnia at Levi's Stadium
    # was flagging neutral when it's a true home game. Normalize the
    # common WC-bracket variants.
    _ALIAS = {
        "usa": "united states",
        "us": "united states",
        "u.s.": "united states",
        "u.s.a.": "united states",
        "uk": "united kingdom",
    }
    def _norm(s: str) -> str:
        k = (s or "").strip().lower()
        return _ALIAS.get(k, k)

    if venue_country and (home_country or away_country):
        vc = _norm(venue_country)
        hc = _norm(home_country)
        ac = _norm(away_country)
        if vc == hc:
            home_side = "home"
        elif vc == ac:
            home_side = "away"  # away team is the true home side
        else:
            home_side = "neutral"
    elif comp.get("neutralSite"):
        home_side = "neutral"
    else:
        home_side = "home"  # default for league play

    neutral = 0 if home_side == "home" else 1

    # INSERT OR REPLACE wipes columns not listed back to NULL — keep HT
    # scores + ET/PEN data via a manual UPSERT so re-running the
    # scoreboard ingest doesn't destroy half-time backfill (discovered
    # 2026-06-30 during WC neutral-site fix; ht_backfill had to re-run
    # every time). The two-step pattern preserves columns this ingest
    # doesn't own.
    existing = conn.execute(
        "SELECT home_score_ht, away_score_ht, home_score_et, away_score_et, "
        "       home_pens, away_pens, has_extra_time, has_shootout "
        "FROM matches WHERE id = ?", (match_id,)
    ).fetchone()
    conn.execute(
        "INSERT OR REPLACE INTO matches "
        "(id, external_id, date, start_time, home_team_id, away_team_id, "
        " home_score, away_score, home_score_ht, away_score_ht, "
        " home_score_et, away_score_et, home_pens, away_pens, "
        " has_extra_time, has_shootout, "
        " status, status_detail, season, "
        " matchweek, round_name, venue, neutral_site, "
        " home_side, venue_country) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
        "        ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            match_id, str(match_id), date, iso_kickoff, home_id, away_id,
            # Persist scores for live + final; null while pre-game so a
            # 0-0 in-progress doesn't get mis-stored on scheduled rows.
            home_score if status in ("final", "live") else None,
            away_score if status in ("final", "live") else None,
            existing["home_score_ht"] if existing else None,
            existing["away_score_ht"] if existing else None,
            existing["home_score_et"] if existing else None,
            existing["away_score_et"] if existing else None,
            existing["home_pens"] if existing else None,
            existing["away_pens"] if existing else None,
            existing["has_extra_time"] if existing else 0,
            existing["has_shootout"] if existing else 0,
            status, status_detail, season, matchweek, round_name,
            venue, neutral,
            home_side, venue_country or None,
        ),
    )
    return True


def _ensure_team_stub(conn, team: dict) -> None:
    """Insert a minimal teams row if one doesn't exist. Cup matches can
    expose teams whose own league hasn't ingested them yet (a Brazilian
    Série A side appearing in Libertadores group stage). Without a stub
    the FK in matches.home_team_id would fail; with the stub the
    league's full teams ingest can later upgrade the row in-place via
    INSERT OR REPLACE."""
    tid = _safe_int(team.get("id"))
    if not tid:
        return
    exists = conn.execute(
        "SELECT 1 FROM teams WHERE id = ?", (tid,)
    ).fetchone()
    if exists:
        return
    logo = team.get("logo")
    if not logo:
        logos = team.get("logos") or []
        if isinstance(logos, list) and logos:
            logo = (logos[0] or {}).get("href")
    conn.execute(
        "INSERT INTO teams "
        "(id, name, short_name, abbreviation, country, logo_url, external_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            tid,
            team.get("displayName") or team.get("name") or "",
            team.get("shortDisplayName") or "",
            team.get("abbreviation") or "",
            team.get("location") or "",
            logo,
            str(tid),
        ),
    )


def ingest_today(league: str, date: str | None = None) -> dict:
    """Pull one calendar-day's scoreboard. ESPN's ``dates=YYYYMMDD``
    parameter returns the match list for that UTC day. When ``date`` is
    None we pull today's UTC scoreboard. Multi-path leagues (like
    fifa_internationals) walk every configured path into the same DB.

    Also re-ingests the prior 2 UTC days. Without that backsweep, soccer
    matches caught mid-game stay at ``status='live'`` forever because
    ``ingest_today`` only fetches today's scoreboard — yesterday's
    Libertadores 2-1 game never got its FT promotion. The 2-day window
    catches overnight UTC↔ET wrap + the rare matches whose final
    status posts >24h later.
    """
    from datetime import timedelta as _td
    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    paths = _espn_paths(league)
    # Walk today + 2 prior UTC days. Most matches finalize within hours
    # of full-time but the buffer keeps stale 'live' rows from piling up.
    base = datetime.strptime(date, "%Y-%m-%d")
    dates_to_walk = [
        (base - _td(days=d)).strftime("%Y-%m-%d") for d in (0, 1, 2)
    ]
    ingested = skipped = 0
    for d_iter in dates_to_walk:
        for path in paths:
            url = (
                f"https://site.api.espn.com/apis/site/v2/sports/soccer/{path}"
                f"/scoreboard?dates={d_iter.replace('-', '')}"
            )
            data = _fetch(url)
            if not data:
                continue
            events = data.get("events") or []
            conn = get_conn(league)
            for ev in events:
                try:
                    if _upsert_match(league, ev):
                        ingested += 1
                    else:
                        skipped += 1
                except Exception as e:
                    logger.warning("[%s/%s] upsert %s failed: %s",
                                    league, path, ev.get("id"), e)
                    skipped += 1
            conn.commit()
    # Stale-match sweep — ESPN sometimes drops postponed/relocated
    # games from the feed silently, leaving our DB with phantom
    # 'scheduled' rows that pollute the slate and leave picks dangling.
    # Flip anything > 2 days past with no score to 'cancelled'. The
    # 2-day buffer absorbs UTC↔ET overnight games.
    n_cancelled = 0
    try:
        from datetime import timedelta as _td
        cutoff = (datetime.now(timezone.utc) - _td(days=2)
                   ).strftime("%Y-%m-%d")
        conn = get_conn(league)
        cur = conn.execute(
            "UPDATE matches SET status = 'cancelled' "
            "WHERE date < ? AND status IN ('scheduled', 'live') "
            "  AND home_score IS NULL",
            (cutoff,),
        )
        n_cancelled = cur.rowcount
        conn.commit()
    except Exception as e:
        logger.debug("[%s] stale-sweep failed: %s", league, e)
    logger.info("[%s] ingest_today date=%s ingested=%d skipped=%d (paths=%d, "
                "stale_cancelled=%d)",
                league, date, ingested, skipped, len(paths), n_cancelled)
    return {"ingested": ingested, "skipped": skipped, "date": date,
             "paths_walked": len(paths), "stale_cancelled": n_cancelled}


def backfill(league: str, start_date: str, end_date: str,
              *, throttle: float = 0.15) -> dict:
    """Sweep one day at a time from ``start_date`` to ``end_date``. ESPN
    has no multi-day endpoint, so we paginate by day. The default 150ms
    throttle keeps full-season backfills polite (~38s per club-season at
    ~250 matchdays)."""
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
    logger.info("[%s] backfill %s → %s: %s", league, start_date, end_date, totals)
    return totals


# ── CLI ─────────────────────────────────────────────────────

def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.soccer._espn_ingest")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_teams = sub.add_parser("teams")
    p_teams.add_argument("league")
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
                        format="%(asctime)s [%(levelname)s] %(message)s")
    if args.cmd == "teams":
        n = ingest_teams(args.league)
        print({"teams": n})
    elif args.cmd == "today":
        print(ingest_today(args.league, date=args.date))
    elif args.cmd == "backfill":
        print(backfill(args.league, args.start, args.end,
                       throttle=args.throttle))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
