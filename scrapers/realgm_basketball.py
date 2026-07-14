"""RealGM basketball scraper.

Source-of-truth schedule + standings for the China CBA (and any other
basketball.realgm.com international league we need to onboard later —
the URL pattern is shared, league_id is the only difference).

RealGM blocks plain ``urllib`` requests with 403; the browser-like
header set below (full ``Sec-Ch-Ua-*`` + ``Referer``) gets through. No
CAPTCHA, no JS rendering required — the data is plain server-rendered
HTML inside ``<table>`` elements.

Public API
----------
- ``fetch_standings(league_id, slug)`` — current season W/L + PPG/OPPG
  per team. Used to seed the teams table on first ingest.
- ``fetch_schedule_for_date(league_id, slug, date)`` — game list for a
  specific date. ``date`` accepts YYYY-MM-DD; the page returns Preview
  rows for upcoming games and ``"AWAY-HOME"`` score strings for finals.
- ``fetch_schedule_today(league_id, slug)`` — convenience wrapper that
  hits the bare ``/schedules`` page (RealGM defaults to today).

Rate-limit etiquette
--------------------
RealGM hosts a non-trivial site; we throttle to 1 request per 1.2s in
this module and never retry on 4xx. The caller is responsible for
batching across multiple URLs (date backfill walks days serially).
"""

from __future__ import annotations

import gzip
import logging
import re
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Iterable

logger = logging.getLogger(__name__)

_BASE = "https://basketball.realgm.com"
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
       "AppleWebKit/537.36 (KHTML, like Gecko) "
       "Chrome/120.0.0.0 Safari/537.36")
_HEADERS = {
    "User-Agent": _UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://basketball.realgm.com/",
    "Sec-Ch-Ua": '"Chromium";v="120", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Upgrade-Insecure-Requests": "1",
}
_RATE_LIMIT_GAP_S = 1.2
_LAST_FETCH_TS = 0.0


def _fetch(url: str, timeout: int = 20) -> str | None:
    """Single GET with rate-limit guard. Returns body text or None on
    transport / HTTP error. Never raises."""
    global _LAST_FETCH_TS
    now = time.monotonic()
    delta = now - _LAST_FETCH_TS
    if delta < _RATE_LIMIT_GAP_S:
        time.sleep(_RATE_LIMIT_GAP_S - delta)
    req = urllib.request.Request(url, headers=_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            if body[:2] == b"\x1f\x8b":
                body = gzip.decompress(body)
            return body.decode("utf-8", errors="ignore")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
        logger.warning("realgm fetch failed (%s): %s", url, e)
        return None
    finally:
        _LAST_FETCH_TS = time.monotonic()


# ── Date parsing ────────────────────────────────────────────────

# RealGM stamps schedule rows like "May 6, 20265:00 AM ET" — no space
# between year and time because the table cell shoves them together.
_DATE_RE = re.compile(
    r"(?P<m>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
    r"\w*\s+(?P<d>\d{1,2}),\s*(?P<y>\d{4})"
    r"(?P<h>\d{1,2}):(?P<min>\d{2})\s*(?P<ampm>AM|PM)\s*ET"
)
_MONTHS = {m: i + 1 for i, m in enumerate(
    ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))}


def _parse_datetime_et(text: str) -> tuple[str, str | None]:
    """Return (YYYY-MM-DD, HH:MM ET) or (date, None) when only date is
    parseable. Used for the schedule table's first column."""
    m = _DATE_RE.search(text or "")
    if not m:
        # Fallback: just the date portion
        md = re.search(
            r"(?P<m>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+"
            r"(?P<d>\d{1,2}),\s*(?P<y>\d{4})", text or "")
        if not md:
            return ("", None)
        date = f"{md['y']}-{_MONTHS[md['m']]:02d}-{int(md['d']):02d}"
        return (date, None)
    h = int(m["h"])
    if m["ampm"] == "PM" and h != 12:
        h += 12
    elif m["ampm"] == "AM" and h == 12:
        h = 0
    date = f"{m['y']}-{_MONTHS[m['m']]:02d}-{int(m['d']):02d}"
    return (date, f"{h:02d}:{int(m['min']):02d} ET")


_SCORE_RE = re.compile(r"^(\d{1,3})\s*-\s*(\d{1,3})$")


# ── Public fetchers ─────────────────────────────────────────────

def fetch_standings(league_id: int, slug: str) -> list[dict]:
    """Fetch current standings table. Each row carries
    ``{rank, team, wins, losses, pct, gb, l10, streak, ppg, oppg}``.
    Slug accepts any case — RealGM is case-insensitive on slugs."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.error("bs4 not installed; cannot parse RealGM")
        return []
    url = f"{_BASE}/international/league/{league_id}/{slug}/standings"
    html = _fetch(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []
    rows = table.find_all("tr")
    out: list[dict] = []
    if not rows:
        return out
    header = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
    for tr in rows[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
        if not cells or len(cells) < 4:
            continue
        d = dict(zip(header, cells))
        # Try to coerce common columns; tolerate missing fields.
        try:
            wins = int(d.get("W") or 0)
            losses = int(d.get("L") or 0)
        except (ValueError, TypeError):
            continue
        out.append({
            "rank": int(d.get("#", 0) or 0),
            "team": d.get("Team") or "",
            "wins": wins, "losses": losses,
            "pct": d.get("PCT") or "",
            "gb": d.get("GB") or "",
            "l10": d.get("L10") or "",
            "streak": d.get("STRK") or "",
            "ppg": float(d.get("PPG") or 0) or None,
            "oppg": float(d.get("OPPG") or 0) or None,
        })
    return out


def fetch_schedule_for_date(league_id: int, slug: str,
                             date: str) -> list[dict]:
    """Fetch the schedule for a specific YYYY-MM-DD. Returns rows in
    the shape {date, time_et, away, home, status, score, venue}.
    Status is ``"final"`` when the result column carries a score,
    ``"pre"`` when it's "Preview"."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []
    url = (f"{_BASE}/international/league/{league_id}/{slug}/"
           f"schedules/{date}")
    html = _fetch(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []
    rows = table.find_all("tr")
    out: list[dict] = []
    for tr in rows[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
        if len(cells) < 4:
            continue
        date_txt = cells[0]
        away = cells[1]
        home = cells[2]
        result = cells[3]
        venue = cells[4] if len(cells) > 4 else ""
        d, t = _parse_datetime_et(date_txt)
        away_score = home_score = None
        status = "pre"
        m = _SCORE_RE.match(result)
        if m:
            away_score = int(m.group(1))
            home_score = int(m.group(2))
            status = "final"
        out.append({
            "date": d or date,
            "time_et": t,
            "away": away,
            "home": home,
            "status": status,
            "away_score": away_score,
            "home_score": home_score,
            "venue": venue,
        })
    return out


def fetch_schedule_today(league_id: int, slug: str) -> list[dict]:
    """Bare /schedules URL — RealGM defaults this to "today" (in the
    site's timezone). Same row shape as ``fetch_schedule_for_date``."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []
    url = f"{_BASE}/international/league/{league_id}/{slug}/schedules"
    html = _fetch(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []
    rows = table.find_all("tr")
    out: list[dict] = []
    for tr in rows[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
        if len(cells) < 4:
            continue
        d, t = _parse_datetime_et(cells[0])
        result = cells[3]
        m = _SCORE_RE.match(result)
        away_score = home_score = None
        status = "pre"
        if m:
            away_score = int(m.group(1))
            home_score = int(m.group(2))
            status = "final"
        out.append({
            "date": d or datetime.now().strftime("%Y-%m-%d"),
            "time_et": t,
            "away": cells[1],
            "home": cells[2],
            "status": status,
            "away_score": away_score,
            "home_score": home_score,
            "venue": cells[4] if len(cells) > 4 else "",
        })
    return out


def fetch_schedule_window(league_id: int, slug: str,
                           start_date: str, days: int) -> list[dict]:
    """Walk ``days`` consecutive dates starting at ``start_date`` and
    accumulate a flat list of schedule rows. Used for backfill — each
    day is one HTTP fetch (rate-limited)."""
    from datetime import datetime as _dt, timedelta as _td
    base = _dt.strptime(start_date, "%Y-%m-%d")
    out: list[dict] = []
    for i in range(int(days)):
        d = (base + _td(days=i)).strftime("%Y-%m-%d")
        out.extend(fetch_schedule_for_date(league_id, slug, d))
    return out
