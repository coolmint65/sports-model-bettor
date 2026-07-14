"""Historical NBA closing-odds backfill from scoresandodds.com.

Pulls per-date HTML, parses JSON-LD for game metadata + the structured
event-card rows for closing-line spread/total/moneyline. Writes into
``data/nba/historical_odds.db`` (separate DB so it doesn't bloat the
main nba.db and can be rebuilt without touching live state).

Coverage window: 2024-07-15 onward (earlier dates return 404). The
2023-24 NBA season is not available here — that gap is filed under
the V3.1 watchlist for later backfill via a paid odds API.

Usage::

    python -m scrapers.scoresandodds_nba --start 2024-07-15 --end 2026-05-17
    python -m scrapers.scoresandodds_nba --date 2025-01-15
    python -m scrapers.scoresandodds_nba --gap-fill           # only days we don't already have

Powers V3.1 (market-as-feature) by giving the NBA GBM access to
``market_implied_prob`` + ``line_movement`` features computed from this
table. Match-back to ``nba_games`` is via (date, home_abbr, away_abbr).
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import time
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DB_PATH = (Path(__file__).resolve().parent.parent
            / "data" / "nba" / "historical_odds.db")
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0 Safari/537.36"
)


# ── DB ──

_SCHEMA = """
CREATE TABLE IF NOT EXISTS nba_historical_odds (
    event_id          INTEGER PRIMARY KEY,
    date              TEXT NOT NULL,            -- ET date
    start_time_utc    TEXT,
    home_abbr         TEXT,
    away_abbr         TEXT,
    home_score        INTEGER,
    away_score        INTEGER,
    -- closing (most recent live) odds
    spread_home_line  REAL,
    spread_home_odds  INTEGER,
    spread_away_line  REAL,
    spread_away_odds  INTEGER,
    total_line        REAL,
    over_odds         INTEGER,
    under_odds        INTEGER,
    home_ml           INTEGER,
    away_ml           INTEGER,
    -- opening odds (first snapshot in line-movements column)
    open_spread_line  REAL,
    open_spread_odds  INTEGER,
    open_total_line   REAL,
    open_total_odds   INTEGER,
    -- bookkeeping
    fetched_at        TEXT NOT NULL,
    source            TEXT NOT NULL DEFAULT 'scoresandodds'
);
CREATE INDEX IF NOT EXISTS idx_nbahist_date  ON nba_historical_odds(date);
CREATE INDEX IF NOT EXISTS idx_nbahist_teams ON nba_historical_odds(home_abbr, away_abbr);
CREATE TABLE IF NOT EXISTS nba_historical_odds_dates (
    -- One row per date we've successfully scraped; lets gap-fill skip
    -- already-pulled dates cheaply without scanning the per-event table.
    date         TEXT PRIMARY KEY,
    n_events     INTEGER,
    scraped_at   TEXT NOT NULL
);
"""


def _conn() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), isolation_level=None, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    return conn


# ── HTTP ──

def _fetch(date_iso: str, timeout: float = 12.0) -> str | None:
    """Pull the day's NBA results page. None on 404 (out-of-window date)
    or any HTTP/network failure."""
    url = f"https://www.scoresandodds.com/nba?date={date_iso}"
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        logger.warning("scoresandodds %s HTTP %s", date_iso, e.code)
        return None
    except Exception as e:
        logger.warning("scoresandodds %s fetch failed: %s", date_iso, e)
        return None


# ── Parsers ──

# JSON-LD blocks live in <script type="application/ld+json"> wrappers.
# Each SportsEvent gives us identifier, startDate, away/home team names.
_JSONLD_RE = re.compile(
    r'<script[^>]*type=[\'"]application/ld\+json[\'"][^>]*>(.*?)</script>',
    re.DOTALL,
)


def _parse_jsonld_events(body: str) -> dict[int, dict]:
    """Return a dict keyed by event identifier → meta from JSON-LD."""
    out: dict[int, dict] = {}
    for raw in _JSONLD_RE.findall(body):
        try:
            j = json.loads(raw.strip())
        except json.JSONDecodeError:
            continue
        if not isinstance(j, dict) or j.get("@type") != "SportsEvent":
            continue
        ident = j.get("identifier")
        if ident is None:
            continue
        out[int(ident)] = {
            "name": j.get("name") or "",
            "url": j.get("url"),
            "start_date": j.get("startDate"),
            "away_name": (j.get("awayTeam") or {}).get("name") or "",
            "home_name": (j.get("homeTeam") or {}).get("name") or "",
        }
    return out


# Each game's odds live inside <div id="nba.{event_id}" class="event-card" ...>
# with a two-row table for away (data-side="away") and home (data-side="home").
_EVENT_CARD_RE = re.compile(
    r'<div[^>]+id="nba\.(\d+)"[^>]+class="[^"]*event-card[^"]*"[^>]*>'
    r'(.*?)(?=<div[^>]+id="nba\.\d+"|<footer|\Z)',
    re.DOTALL,
)


def _parse_event_card(card_html: str) -> dict:
    """Extract odds + scores from one event-card HTML region.

    Returns a dict with closing spread/total/ML for each side + opening
    snapshots from the line-movements column (3 snapshots, we use the
    first one as 'open' and the last as the close cross-check)."""
    out: dict[str, Any] = {}
    # Split into away/home row regions. Anchor on the ``<tr ... data-side=>``
    # boundary — there are multiple ``data-side="away"`` attributes inside
    # individual ``<td>`` cells (live-spread, live-total, live-moneyline)
    # so plain ``find('data-side="away"')`` lands inside a <td>, not the
    # row. The earlier slicing bug crossed rows and let the opening-total
    # parser pick up a spread number from the wrong cell.
    rows = list(re.finditer(
        r'<tr[^>]+class="event-card-row[^"]*"[^>]+data-side="(away|home)"',
        card_html,
    ))
    if len(rows) < 2:
        return out
    away_match = next((m for m in rows if m.group(1) == "away"), None)
    home_match = next((m for m in rows if m.group(1) == "home"), None)
    if not away_match or not home_match:
        return out
    away_region = card_html[away_match.start():home_match.start()]
    home_end = card_html.find('</tbody>', home_match.start())
    home_region = card_html[home_match.start():
                              home_end if home_end > 0 else len(card_html)]

    def _team_abbr(region: str) -> str | None:
        # Team abbreviation lives in <a ... data-abbr="Knicks"> — use the
        # team-name <a>, not the rotation-number <span data-abbr="501">.
        m = re.search(r'class="team-name"[^>]*>\s*<a[^>]+data-abbr="([^"]+)"',
                       region, re.DOTALL)
        return m.group(1) if m else None

    def _score(region: str) -> int | None:
        m = re.search(r'class="event-card-score[^"]*">\s*(\d+)\s*<', region)
        return int(m.group(1)) if m else None

    def _live_field(region: str, field: str) -> tuple[str | None, int | None]:
        # <td data-field="live-spread" data-side="..."> <span class="data-value">...</span> <small class="data-odds">...</small>
        m = re.search(
            rf'data-field="{field}"[^>]*>\s*'
            rf'<span class="data-value">\s*([^<]+?)\s*</span>'
            rf'(?:\s*<small class="data-odds">\s*([+-]?\d+)\s*</small>)?',
            region, re.DOTALL,
        )
        if not m:
            return None, None
        val = m.group(1).strip()
        odds = int(m.group(2)) if m.group(2) else None
        return val, odds

    def _open_snapshots(region: str) -> list[tuple[str, int]]:
        # Pull every snapshot inside the row's event-card-movements td.
        # Snapshots for different markets can live in either row depending
        # on the game; the caller classifies by value pattern (``o.../u...``
        # → total, signed number → spread).
        out: list[tuple[str, int]] = []
        cell = re.search(
            r'<td[^>]*class="event-card-movements[^"]*"[^>]*>(.*?)</td>',
            region, re.DOTALL,
        )
        if not cell:
            return out
        for snap in re.finditer(
            r'<div[^>]*data-role="openable"[^>]*>\s*'
            r'<span class="data-value">\s*([^<]+?)\s*</span>'
            r'\s*<small class="data-odds">\s*([+-]?\d+)\s*</small>',
            cell.group(1), re.DOTALL,
        ):
            out.append((snap.group(1).strip(), int(snap.group(2))))
        return out

    def _classify(val: str) -> str:
        v = val.strip()
        # Totals shipped as "o219.5" / "u219" / "O219.5"
        if v and v[0] in ("o", "O", "u", "U"):
            return "total"
        # Anything else (signed numeric like "-5.5", "+3") is a spread
        return "spread"

    away_abbr = _team_abbr(away_region)
    home_abbr = _team_abbr(home_region)
    if not (away_abbr and home_abbr):
        return out

    out["away_abbr"] = away_abbr
    out["home_abbr"] = home_abbr
    out["away_score"] = _score(away_region)
    out["home_score"] = _score(home_region)

    # Closing (live) odds — paired across rows
    a_spread_val, a_spread_odds = _live_field(away_region, "live-spread")
    h_spread_val, h_spread_odds = _live_field(home_region, "live-spread")
    a_total_val,  a_total_odds  = _live_field(away_region, "live-total")
    h_total_val,  h_total_odds  = _live_field(home_region, "live-total")
    a_ml_val, _ = _live_field(away_region, "live-moneyline")
    h_ml_val, _ = _live_field(home_region, "live-moneyline")

    out["spread_away_line"] = _to_float(a_spread_val)
    out["spread_away_odds"] = a_spread_odds
    out["spread_home_line"] = _to_float(h_spread_val)
    out["spread_home_odds"] = h_spread_odds
    # Total line: away row carries "o219.5", home carries "u219.5" —
    # they're the same line, just over vs under labels.
    out["total_line"] = _to_float(_strip_ou(a_total_val) or _strip_ou(h_total_val))
    out["over_odds"] = a_total_odds
    out["under_odds"] = h_total_odds
    out["away_ml"] = _to_int(a_ml_val)
    out["home_ml"] = _to_int(h_ml_val)

    # Opening snapshots — each row's line-movements column can carry
    # either spread or total snapshots; the assignment is game-specific
    # (some games show home row = total, others show home row = spread).
    # Classify by value pattern: "o219.5"/"u219" = total; "-7"/"+3.5" =
    # spread.
    #
    # Spread perspective: away row carries the away spread, home row
    # carries the home spread. We normalize to HOME perspective so
    # ``open_spread_line`` aligns with ``spread_home_line`` — a positive
    # value means home is the underdog regardless of which row supplied
    # the snapshot. Without this flip, the line-movement feature gets a
    # false "11-point move" on a non-moving line because closing was
    # parsed home-side (+5.5) but opening was parsed away-side (-5.5).
    away_snaps = [(v, o, "away") for v, o in _open_snapshots(away_region)]
    home_snaps = [(v, o, "home") for v, o in _open_snapshots(home_region)]
    all_snaps = away_snaps + home_snaps
    first_spread = next(
        (s for s in all_snaps if _classify(s[0]) == "spread"), None,
    )
    first_total = next(
        (s for s in all_snaps if _classify(s[0]) == "total"), None,
    )
    if first_spread:
        val = _to_float(first_spread[0])
        # Negate when the snapshot came from the away row so the stored
        # value is from home perspective.
        if val is not None and first_spread[2] == "away":
            val = -val
        out["open_spread_line"] = val
        out["open_spread_odds"] = first_spread[1]
    if first_total:
        out["open_total_line"]  = _to_float(_strip_ou(first_total[0]))
        out["open_total_odds"]  = first_total[1]

    return out


_OU_PREFIX_RE = re.compile(r'^[ou]\s*', re.I)


def _strip_ou(s: str | None) -> str | None:
    if not s:
        return None
    return _OU_PREFIX_RE.sub("", s).strip()


def _to_float(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_int(s: str | None) -> int | None:
    if s is None or s == "":
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


# ── Date sweep ──

def scrape_date(date_iso: str) -> dict:
    """Pull one date, parse, persist. Returns counts dict."""
    body = _fetch(date_iso)
    if body is None:
        # 404 — out of coverage window. Still record the attempt so
        # gap-fill doesn't re-hit it on every run.
        conn = _conn()
        conn.execute(
            "INSERT OR REPLACE INTO nba_historical_odds_dates "
            "(date, n_events, scraped_at) VALUES (?, 0, ?)",
            (date_iso, datetime.utcnow().isoformat()),
        )
        return {"date": date_iso, "events": 0, "ok": False, "reason": "404"}

    meta = _parse_jsonld_events(body)
    cards = dict(_EVENT_CARD_RE.findall(body))
    conn = _conn()
    n_written = 0
    fetched_at = datetime.utcnow().isoformat()
    for event_id_str, card_html in cards.items():
        event_id = int(event_id_str)
        m = meta.get(event_id) or {}
        parsed = _parse_event_card(card_html)
        if not parsed.get("home_abbr") or not parsed.get("away_abbr"):
            continue
        conn.execute("""
            INSERT OR REPLACE INTO nba_historical_odds (
                event_id, date, start_time_utc,
                home_abbr, away_abbr,
                home_score, away_score,
                spread_home_line, spread_home_odds,
                spread_away_line, spread_away_odds,
                total_line, over_odds, under_odds,
                home_ml, away_ml,
                open_spread_line, open_spread_odds,
                open_total_line,  open_total_odds,
                fetched_at, source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            event_id, date_iso, m.get("start_date"),
            parsed.get("home_abbr"), parsed.get("away_abbr"),
            parsed.get("home_score"), parsed.get("away_score"),
            parsed.get("spread_home_line"), parsed.get("spread_home_odds"),
            parsed.get("spread_away_line"), parsed.get("spread_away_odds"),
            parsed.get("total_line"), parsed.get("over_odds"), parsed.get("under_odds"),
            parsed.get("home_ml"), parsed.get("away_ml"),
            parsed.get("open_spread_line"), parsed.get("open_spread_odds"),
            parsed.get("open_total_line"), parsed.get("open_total_odds"),
            fetched_at, "scoresandodds",
        ))
        n_written += 1
    conn.execute(
        "INSERT OR REPLACE INTO nba_historical_odds_dates "
        "(date, n_events, scraped_at) VALUES (?, ?, ?)",
        (date_iso, n_written, fetched_at),
    )
    return {"date": date_iso, "events": n_written, "ok": True}


def scrape_range(start_iso: str, end_iso: str,
                  *, throttle_s: float = 0.6,
                  gap_fill: bool = False) -> dict:
    """Sweep [start..end] inclusive. ``gap_fill`` skips dates already
    in the dates table."""
    start = datetime.strptime(start_iso, "%Y-%m-%d").date()
    end = datetime.strptime(end_iso, "%Y-%m-%d").date()
    if start > end:
        return {"error": "start > end"}
    conn = _conn()
    already: set[str] = set()
    if gap_fill:
        rows = conn.execute(
            "SELECT date FROM nba_historical_odds_dates"
        ).fetchall()
        already = {r["date"] for r in rows}
    cur = start
    totals = {"days": 0, "events": 0, "skipped": 0,
              "ok": 0, "out_of_window": 0}
    while cur <= end:
        iso = cur.strftime("%Y-%m-%d")
        if iso in already:
            totals["skipped"] += 1
        else:
            res = scrape_date(iso)
            totals["days"] += 1
            totals["events"] += res.get("events", 0)
            if res.get("ok"):
                totals["ok"] += 1
            elif res.get("reason") == "404":
                totals["out_of_window"] += 1
            if totals["days"] % 30 == 0:
                logger.info("scoresandodds backfill: %s — %d days, "
                            "%d events", iso, totals["days"], totals["events"])
            time.sleep(throttle_s)
        cur += timedelta(days=1)
    return totals


# ── CLI ──

def _cli() -> int:
    ap = argparse.ArgumentParser(prog="scrapers.scoresandodds_nba")
    ap.add_argument("--start", help="YYYY-MM-DD")
    ap.add_argument("--end",   help="YYYY-MM-DD")
    ap.add_argument("--date",  help="single date YYYY-MM-DD")
    ap.add_argument("--gap-fill", action="store_true",
                     help="skip dates already in the dates table")
    ap.add_argument("--throttle", type=float, default=0.6)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    if args.date:
        print(json.dumps(scrape_date(args.date), indent=2))
        return 0
    if not args.start or not args.end:
        ap.error("specify --date OR --start + --end")
        return 1
    totals = scrape_range(args.start, args.end,
                          throttle_s=args.throttle,
                          gap_fill=args.gap_fill)
    print(json.dumps(totals, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
