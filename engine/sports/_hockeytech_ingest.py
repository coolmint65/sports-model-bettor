"""HockeyTech CMS ingest — multi-season backfill for AHL + PWHL.

theScore caps AHL/PWHL at one current season; HockeyTech (the CMS that
powers ahl.com / thepwhl.com) hosts public per-game report HTML
going back to 2012+. The schedule/standings JSON endpoints require a
widget key we can't extract, but the per-game `game_reports/official-
game-report.php` URL works WITHOUT a key — it just takes
`client_code` + `game_id`.

Strategy: walk a sequential `game_id` range, parse each HTML page for
date / home / away / scores / OT/SO, upsert to the per-league DB.

Pattern (verified 2026-05-04):
    game_id 1010000 ≈ Nov 2012 game (Charlotte vs Chicago)
    game_id 1020000 ≈ Dec 2019 (Hartford vs Binghamton)
    game_id 1028911 ≈ Apr 2026 (Utica vs Providence)

So ~5000 ids covers ~5 seasons of recent AHL play.

Public API:
    backfill_range(league, conn, start_id, end_id, throttle=0.5)
        Walks ids inclusive, parses, upserts. Returns aggregate counts.

CLI:
    python -m engine.sports._hockeytech_ingest ahl --range 1024000 1029000
    python -m engine.sports._hockeytech_ingest pwhl --range <s> <e>
"""
from __future__ import annotations

import logging
import re
import sqlite3
import ssl
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Iterable

logger = logging.getLogger(__name__)

_BASE = "https://lscluster.hockeytech.com/game_reports/official-game-report.php"
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

# HockeyTech's standard month abbreviations in the gamesheet header.
_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _fetch(client_code: str, game_id: int, retries: int = 2) -> str | None:
    """GET one game-report HTML. Returns None on 404 or unrecoverable error."""
    url = (f"{_BASE}?lang_id=1&client_code={client_code}&game_id={game_id}")
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0"},
            )
            with urllib.request.urlopen(req, timeout=15, context=_SSL_CTX) as r:
                return r.read().decode("utf-8", "ignore")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            time.sleep(0.5 * (attempt + 1))
        except Exception:
            time.sleep(0.5 * (attempt + 1))
    return None


# ── HTML parsing ──────────────────────────────────────────────

_RE_DATE = re.compile(
    r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+(\d{1,2}),?\s+(\d{4})\b",
    re.IGNORECASE,
)

# Time-of-puck-drop. HockeyTech gamesheets typically render this as
# "7:00 PM ET" or "Time: 7:30 PM" in the header. Tolerate trailing TZ
# token (ET / EST / EDT / CT / MT / PT / etc.) but don't bind on it
# since the wall-clock hour is what we want.
_RE_TIME = re.compile(
    r"\b(\d{1,2}):(\d{2})\s*(AM|PM)\b",
    re.IGNORECASE,
)

# HockeyTech's standard "Team N goals" or "Team\nN" markup. The header
# shows "Visiting" vs "Home" with team name + goals.
_RE_TEAMS_GOALS = re.compile(
    r'<td[^>]*class="[^"]*game[^"]*"[^>]*>\s*(.+?)\s*<',
    re.IGNORECASE | re.DOTALL,
)

# Cleaner heuristic — find pattern like "Hartford 3" / "Wolf Pack 4"
# adjacent to "Final" / "Final OT" / "Final SO". Fall back to a
# table-row scan when present.
_RE_FINAL_LINE = re.compile(
    r"(?P<away>[A-Za-z][A-Za-z\s\.\-/]{0,40})\s+(?P<as>\d+)\s+at\s+"
    r"(?P<home>[A-Za-z][A-Za-z\s\.\-/]{0,40})\s+(?P<hs>\d+)",
    re.IGNORECASE,
)


_ALL_STAR_NAMES = {
    "atlantic division", "central division", "north division",
    "pacific division", "eastern conference", "western conference",
    "team toews", "team taveras", "team mcdavid", "team matthews",
}


def _is_all_star(name: str) -> bool:
    """Detect All-Star / exhibition game labels that aren't real teams.
    Filters out e.g. "Atlantic Division" so the resolver doesn't waste
    cycles trying to map them, and the empty-game rows don't pollute
    standings/calibration."""
    return (name or "").strip().lower() in _ALL_STAR_NAMES


def parse_game_report(html: str) -> dict | None:
    """Extract date, away/home, scores, OT/SO from one game-report HTML.

    Returns None when the page didn't render game data (404-equivalent).
    Resilient to HockeyTech CMS quirks across seasons — uses regex over
    the printed "Visiting Team @ Home Team — Final / OT / SO" line that
    appears in the gamesheet header on every league theScore + AHL +
    PWHL deploys.
    """
    if not html or "American Hockey League Game" not in html and "Game #" not in html and "PWHL" not in html.upper():
        # Some PWHL reports lack the "American Hockey League" marker;
        # widen the heuristic.
        if "<title>" not in html or "Final" not in html:
            return None

    # Date — first matching month-day-year token in the gamesheet header.
    date_iso = None
    m = _RE_DATE.search(html)
    if m:
        mon = _MONTHS.get(m.group(1).lower()[:3], 0)
        try:
            day = int(m.group(2)); year = int(m.group(3))
            if mon and 1 <= day <= 31 and 1900 < year < 2100:
                date_iso = f"{year:04d}-{mon:02d}-{day:02d}"
        except ValueError:
            pass
    if not date_iso:
        return None

    # OT / Shootout indicator from the "Final" line.
    has_ot = bool(re.search(r"\bFinal\s*(?:OT|/?\s*OT)\b", html, re.I))
    has_so = bool(re.search(r"\bFinal\s*(?:SO|/?\s*SO|/?\s*Shootout)\b", html, re.I))

    # Best-effort time-of-puck-drop. HockeyTech reports usually carry
    # one or two timestamps in the header (start time + report-generated).
    # Take the FIRST match — historically the start time appears before
    # any report-generation stamp. Format as ISO-naive in the gamesheet
    # locale (assumed ET; HockeyTech doesn't expose TZ markers reliably).
    start_time = None
    tm = _RE_TIME.search(html)
    if tm:
        try:
            hour = int(tm.group(1))
            minute = int(tm.group(2))
            ampm = tm.group(3).upper()
            if ampm == "PM" and hour != 12:
                hour += 12
            elif ampm == "AM" and hour == 12:
                hour = 0
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                start_time = f"{date_iso}T{hour:02d}:{minute:02d}:00"
        except (TypeError, ValueError):
            pass

    # Teams + scores — pull from the canonical scoreboard table. This
    # is the most reliable structure: a <td> with the team city and
    # an adjacent <td> with the goal total. We extract via a
    # tolerant pattern that scans for each team's row.
    away, away_score, home, home_score = _extract_teams_scores(html)
    if not (away and home and away_score is not None and home_score is not None):
        return None
    # Skip All-Star / exhibition games — these dilute standings + calibration
    # with non-competitive matchups whose teams aren't in our teams table.
    if _is_all_star(away) or _is_all_star(home):
        return None

    return {
        "date": date_iso,
        "start_time": start_time,
        "away_name": away.strip(),
        "home_name": home.strip(),
        "away_score": away_score,
        "home_score": home_score,
        "has_overtime": has_ot,
        "has_shootout": has_so,
    }


def _extract_teams_scores(html: str) -> tuple[str | None, int | None,
                                                 str | None, int | None]:
    """Pull (away_name, away_score, home_name, home_score) from the
    HockeyTech gamesheet HTML. Their layout is consistent: a top-of-page
    table with two rows — Visiting on top, Home on bottom — each row
    has the team's city + nickname + final goal count."""
    # The gamesheet ships a small <table> at the top with two rows
    # for visiting/home. The team name is in a class-tagged <span> or
    # plain text and the goals appear as a 1-3 digit value just after.
    # Strip tags first then scan for "Visiting" / "Home" labels.
    text = re.sub(r"<script[^>]*>.*?</script>", "", html,
                   flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text,
                   flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)

    # Pattern 1: explicit "Visiting" / "Home" markers on first appearance.
    # Anchored search around "Visiting" then capture team + score.
    away = away_score = home = home_score = None
    m_vis = re.search(
        r"Visiting\s*[:\-]?\s*([A-Za-z][\w\s\.\-/']{1,40}?)\s+(\d{1,3})\b",
        text,
    )
    m_home = re.search(
        r"Home\s*[:\-]?\s*([A-Za-z][\w\s\.\-/']{1,40}?)\s+(\d{1,3})\b",
        text,
    )
    if m_vis and m_home:
        try:
            away_score = int(m_vis.group(2)); home_score = int(m_home.group(2))
            away = m_vis.group(1).strip()
            home = m_home.group(1).strip()
            return away, away_score, home, home_score
        except ValueError:
            pass

    # Pattern 2: "<away> N at <home> N" / "<away> N @ <home> N"
    for at_pat in (r"(\d{1,3})\s+at\s+", r"(\d{1,3})\s+@\s+"):
        m = re.search(
            r"([A-Z][A-Za-z\s\.\-/']{2,40}?)\s+" + at_pat
            + r"([A-Z][A-Za-z\s\.\-/']{2,40}?)\s+(\d{1,3})",
            text,
        )
        if m:
            try:
                return (m.group(1).strip(), int(m.group(2)),
                         m.group(3).strip(), int(m.group(4)))
            except ValueError:
                continue
    return None, None, None, None


# ── DB upsert ─────────────────────────────────────────────────

def _strip_accents(s: str) -> str:
    """Fold é → e etc so 'Montreal' matches 'Montréal Victoire'."""
    import unicodedata
    return "".join(
        c for c in unicodedata.normalize("NFKD", s or "")
        if not unicodedata.combining(c)
    )


def _resolve_team(conn: sqlite3.Connection, name: str) -> dict | None:
    """Match a HockeyTech team name (city or full name) to our teams
    row. HockeyTech often ships just the city ("Hartford" instead of
    "Hartford Wolf Pack"), so we try multiple patterns including
    accent-folded first-word match against full_name."""
    if not name:
        return None
    n = name.strip().lower()
    n_ascii = _strip_accents(n)
    parts = n_ascii.split()
    first = parts[0] if parts else n_ascii
    last = parts[-1] if parts else n_ascii
    n_up = name.strip().upper()
    n_ascii_up = n_ascii.upper()
    queries = [
        # Exact full name (e.g. "Hartford Wolf Pack")
        ("LOWER(full_name) = ?", (n,)),
        ("LOWER(full_name) = ?", (n_ascii,)),
        # Exact short / abbr (e.g. "HARW")
        ("LOWER(short_name) = ?", (n,)),
        ("UPPER(short_name) = ?", (n_up,)),
        ("UPPER(abbreviation) = ?", (n_up,)),
        ("UPPER(abbreviation) = ?", (n_ascii_up,)),
        # City prefix (HockeyTech: "Hartford" → "Hartford Wolf Pack").
        # Accent-folded so "Montreal" matches "Montréal Victoire".
        ("LOWER(full_name) LIKE ?", (f"{first} %",)),
        ("LOWER(city) = ?", (first,)) if True else None,
        # Nickname (last word of full_name)
        ("LOWER(full_name) LIKE ?", (f"% {last}",)),
        # Two-word match for cities like "Grand Rapids" / "Wilkes-Barre"
        ("LOWER(full_name) LIKE ?", (f"{n_ascii} %",)) if " " in n_ascii else None,
    ]
    queries = [q for q in queries if q is not None]
    cols = {r[1] for r in conn.execute("PRAGMA table_info(teams)").fetchall()}
    has_city = "city" in cols
    for where, params in queries:
        if "city" in where and not has_city:
            continue
        # Accent-fold the candidate side too — `LIKE` is case-insensitive
        # in SQLite by default for ASCII but NOT for accented chars, so
        # we wrap full_name in a Python comparison when the pattern
        # lacks accents but the row has them.
        if "full_name" in where and "%" in (params[0] if params else ""):
            # Run the LIKE first; on miss, do a Python-side accent-fold scan.
            row = conn.execute(
                f"SELECT id, full_name, short_name FROM teams WHERE {where} LIMIT 1",
                params,
            ).fetchone()
            if row:
                return dict(row)
            # Fallback: fetch all teams and scan with accent fold.
            all_rows = conn.execute(
                "SELECT id, full_name, short_name FROM teams"
            ).fetchall()
            pat = (params[0] or "").strip("% ").lower()
            for r in all_rows:
                fold = _strip_accents((r["full_name"] or "").lower())
                if pat and (fold.startswith(pat + " ") or fold.endswith(" " + pat)
                             or fold == pat):
                    return dict(r)
            continue
        row = conn.execute(
            f"SELECT id, full_name, short_name FROM teams WHERE {where} LIMIT 1",
            params,
        ).fetchone()
        if row:
            return dict(row)
    return None


def _upsert_game(conn: sqlite3.Connection, league: str, game_id: int,
                  parsed: dict) -> bool:
    home = _resolve_team(conn, parsed["home_name"])
    away = _resolve_team(conn, parsed["away_name"])
    if not (home and away):
        # Don't fail outright — log + skip. Defunct AHL teams from
        # older seasons may not be in our current teams table; the
        # walk-forward calibration just won't include them.
        logger.debug("[%s] team resolution miss: %s @ %s",
                      league, parsed["away_name"], parsed["home_name"])
        return False
    season = _hockey_season(parsed["date"])
    # Skip if we already have a row for this game_id (idempotent).
    existing = conn.execute(
        "SELECT 1 FROM games WHERE id = ? LIMIT 1", (game_id,),
    ).fetchone()
    if existing:
        return False
    conn.execute(
        "INSERT INTO games (id, date, start_time, home_team_id, away_team_id, "
        "home_score, away_score, status, game_type, season, "
        "has_shootout, has_overtime, venue) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'final', 'Regular Season', "
        "?, ?, ?, '')",
        (
            game_id, parsed["date"], parsed.get("start_time"),
            home["id"], away["id"],
            parsed["home_score"], parsed["away_score"],
            season,
            1 if parsed["has_shootout"] else 0,
            1 if parsed["has_overtime"] else 0,
        ),
    )
    return True


def _hockey_season(date_iso: str, league: str | None = None) -> int:
    """Calendar-year-of-season-start. NHL/AHL/PWHL all run Sept-Apr,
    so the September pivot works for every hockey league we currently
    support. When a league with a different season window is added,
    register its season_months in HOCKEY_LEAGUE_REGISTRY and pass
    league= so this picks up the right pivot."""
    y, m, _ = date_iso.split("-")
    y = int(y)
    primary = 9
    if league:
        try:
            from engine.hockey import HOCKEY_LEAGUE_REGISTRY
            months = (HOCKEY_LEAGUE_REGISTRY.get(league) or {}).get("season_months")
            if months:
                primary = months[0]
        except Exception:
            pass
    return y if int(m) >= primary else y - 1


# ── Bulk backfill ────────────────────────────────────────────

def backfill_range(league: str, conn: sqlite3.Connection,
                    start_id: int, end_id: int, *,
                    throttle: float = 0.5,
                    progress_every: int = 200) -> dict:
    """Walk inclusive ``[start_id, end_id]`` range, parse each game-report,
    upsert to the league's games table. Idempotent on game_id.

    Returns aggregate counts: ``{scanned, parsed, ingested, skipped,
    not_found, team_misses}``."""
    out = {"scanned": 0, "parsed": 0, "ingested": 0, "skipped": 0,
            "not_found": 0, "team_misses": 0}
    for gid in range(start_id, end_id + 1):
        out["scanned"] += 1
        html = _fetch(league, gid)
        if not html:
            out["not_found"] += 1
        else:
            parsed = parse_game_report(html)
            if not parsed:
                out["not_found"] += 1
            else:
                out["parsed"] += 1
                try:
                    if _upsert_game(conn, league, gid, parsed):
                        out["ingested"] += 1
                    else:
                        out["skipped"] += 1
                except Exception as e:
                    logger.debug("[%s] upsert game %s failed: %s",
                                  league, gid, e)
                    out["team_misses"] += 1
        if out["scanned"] % progress_every == 0:
            logger.info("[%s] %d scanned, %d ingested, %d skipped, %d 404s",
                          league, out["scanned"], out["ingested"],
                          out["skipped"], out["not_found"])
        time.sleep(throttle)
    conn.commit()
    return out


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.sports._hockeytech_ingest")
    ap.add_argument("league", choices=("ahl", "pwhl"))
    ap.add_argument("--range", nargs=2, type=int, required=True,
                    metavar=("START", "END"),
                    help="Game-id range to walk (inclusive).")
    ap.add_argument("--throttle", type=float, default=0.5,
                    help="Sleep between fetches in seconds.")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    mod = __import__(f"engine.sports.{args.league}.db",
                      fromlist=["get_conn"])
    conn = mod.get_conn()
    res = backfill_range(args.league, conn, args.range[0], args.range[1],
                          throttle=args.throttle)
    print(res)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
