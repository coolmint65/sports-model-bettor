"""
Tennis Explorer results scraper.

Why this exists
---------------
Settlement of HR-supplement picks (Challenger / ITF / sub-tour events)
was completely broken: ESPN's tennis scoreboard doesn't carry those
tours, Sackmann's CSVs ship annually, and HR drops the market the
moment a match starts. That left ~95 pending picks per sync with no
result source — the calibration table never learned from them, and
the tracker accumulated zombies.

Tennis Explorer (https://www.tennisexplorer.com/) publishes daily
result tables for every tour we care about — ATP main, ATP
Challenger, WTA, WTA125, ITF Futures (men + women). The result page
URL pattern is::

    /results/?type=<tour>&year=YYYY&month=MM&day=DD

where ``type`` is one of:
    atp-single        ATP main + Challenger (consolidated)
    wta-single        WTA main + WTA125
    atp-itf-single    ITF Futures men
    wta-itf-single    ITF Futures women

Each match block on a page renders as TWO ``<tr>`` rows (rowspan=2).
The first row carries the time, match-detail link, and player A; the
second row carries player B. Per-set scores live in ``<td class="score">``
cells inside each row. Sets-won is in ``<td class="result">``.

Output
------
List of dicts:

    {
        "source_match_id": "3190776",
        "tour": "atp",
        "tournament": "Madrid",
        "p1_name": "Sinner J.",
        "p1_te_id": "sinner-8b8e8",
        "p2_name": "Fils A.",
        "p2_te_id": "fils",
        "winner": "p1" | "p2",
        "score": "6-2 6-4",         # winner-first
        "date": "2026-05-01",
    }

Photo URLs are extracted into a parallel mapping the photo backfiller
can consume — see ``fetch_photo_for``.

Network etiquette
-----------------
1.5s gap between page fetches, single User-Agent, no retries on 4xx.
Tennis Explorer is a small site; we don't want to be a problem.
"""

from __future__ import annotations

import logging
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Iterable

logger = logging.getLogger(__name__)


_BASE = "https://www.tennisexplorer.com"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
_RATE_LIMIT_GAP_S = 1.5
_LAST_FETCH_TS = 0.0


# Tour-type pairs. ATP main + Challenger ship on the same page; same
# for WTA main + WTA125. ITF Futures has its own page per gender.
_TYPES = {
    "atp": "atp-single",        # ATP main + Challenger
    "wta": "wta-single",        # WTA main + WTA125
    "atp_itf": "atp-itf-single",
    "wta_itf": "wta-itf-single",
}


def _fetch(url: str, timeout: int = 15) -> str | None:
    """Single GET with a fixed-rate sleep guard. Returns body text or
    None on transport / HTTP error. We never raise so the caller can
    keep going on a partial-tour failure."""
    global _LAST_FETCH_TS
    now = time.monotonic()
    delta = now - _LAST_FETCH_TS
    if delta < _RATE_LIMIT_GAP_S:
        time.sleep(_RATE_LIMIT_GAP_S - delta)
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
        logger.warning("tennis_results fetch failed (%s): %s", url, e)
        return None
    finally:
        _LAST_FETCH_TS = time.monotonic()
    return body


def _date_url(type_key: str, date: str) -> str:
    """Build a results URL for a given (type, YYYY-MM-DD)."""
    type_token = _TYPES[type_key]
    y, m, d = date.split("-")
    return (f"{_BASE}/results/?type={type_token}"
            f"&year={int(y):04d}&month={int(m):02d}&day={int(d):02d}")


def _schedule_url(type_key: str, date: str) -> str:
    """Build the upcoming-matches URL for ``date``. Same per-tour
    page structure as ``/results/`` but with empty result/score cells
    and a time-of-day in the first column instead of completed-time."""
    type_token = _TYPES[type_key]
    y, m, d = date.split("-")
    return (f"{_BASE}/matches/?type={type_token}"
            f"&year={int(y):04d}&month={int(m):02d}&day={int(d):02d}")


# ── Parser ───────────────────────────────────────────────────────

def _parse_results_page(html: str, *,
                         default_tour: str,
                         date: str) -> list[dict]:
    """Walk the result table rows, group rowspan=2 pairs into matches.

    The page emits ``<tr class="head flags">`` per tournament header
    followed by alternating match-pair rows. We keep a running
    ``current_tournament`` cursor so each match inherits its bracket.

    Tournament name distinguishes Challenger from ATP main events —
    e.g. "Madrid" vs "Aix en Provence challenger". We tag tour by the
    page's default (atp/wta) and let downstream classifiers (the
    schedule's level-code logic) refine the tier.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.error("bs4 not installed; cannot parse tennis_results")
        return []

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="result")
    if not table:
        return []

    out: list[dict] = []
    current_tournament: str | None = None
    pending: dict | None = None  # accumulating across rowspan pair

    for tr in table.find_all("tr"):
        classes = tr.get("class") or []
        # Tournament header
        if "head" in classes:
            t_cell = tr.find("td", class_="t-name")
            if t_cell:
                a = t_cell.find("a")
                if a:
                    # Drop the embedded flag/type spans, keep visible text
                    current_tournament = a.get_text(strip=True)
            pending = None
            continue

        # Match rows. First row of a pair has 'fRow' OR 'bott' (varies)
        # — we identify the boundary by whether the row carries the
        # time/match-detail cell. Row B is the row WITHOUT the
        # rowspan="2" first-cell. Rather than relying on fragile class
        # combinations, we drive state from the row content itself.
        first_cell = tr.find("td", class_="first")
        is_match_first = first_cell is not None
        if is_match_first:
            # Start a new pair. Carry over tournament cursor.
            pending = {
                "tournament": current_tournament,
                "tour": default_tour,
                "date": date,
                "source_match_id": None,
                "p1_name": None,
                "p1_te_id": None,
                "p2_name": None,
                "p2_te_id": None,
                "p1_sets": None,
                "p2_sets": None,
                "p1_set_scores": [],
                "p2_set_scores": [],
            }
            # Match-detail link can appear in either the first/time cell
            # (when a highlight reel exists) or the trailing "info" cell
            # at the end of the row (always present). Walk all cells in
            # row A so we don't depend on which side renders the link.
            link = tr.find("a", href=re.compile(r"match-detail"))
            if link:
                m = re.search(r"id=(\d+)", link.get("href", ""))
                if m:
                    pending["source_match_id"] = m.group(1)
            # Player A
            _fill_player(tr, pending, slot="p1")
            continue

        # Row B of a pair (no first/time cell) — fill player B and emit
        if pending is None:
            continue
        _fill_player(tr, pending, slot="p2")

        match = _finalize_pair(pending)
        pending = None
        if match:
            out.append(match)

    return out


def _fill_player(tr, pending: dict, *, slot: str) -> None:
    """Extract player name, te-id, set-result, per-set scores from a row."""
    tcells = tr.find_all("td", class_="t-name")
    if not tcells:
        return
    pcell = tcells[-1]   # last t-name cell — first one (head) is tournament
    a = pcell.find("a", href=re.compile(r"^/player/"))
    if a:
        href = a.get("href", "")
        m = re.match(r"^/player/([^/]+)/", href)
        if m:
            pending[f"{slot}_te_id"] = m.group(1)
        pending[f"{slot}_name"] = a.get_text(strip=True)

    # Sets-won (number of sets the player took)
    rcell = tr.find("td", class_="result")
    if rcell:
        text = rcell.get_text(strip=True)
        if text.isdigit():
            pending[f"{slot}_sets"] = int(text)

    # Per-set scores
    score_cells = tr.find_all("td", class_="score")
    set_scores: list[int] = []
    for sc in score_cells:
        text = sc.get_text(strip=True)
        if text.isdigit():
            set_scores.append(int(text))
        # Tiebreak digits ('7^6' etc.) and non-digit cells (header 'S')
        # are skipped — we only need the headline set scores. The
        # number of digit cells per row gives us the set count.
    pending[f"{slot}_set_scores"] = set_scores


def _parse_schedule_page(html: str, *,
                          default_tour: str,
                          date: str) -> list[dict]:
    """Walk the upcoming-matches table the same way ``_parse_results_page``
    walks the results table — same row-pair structure, same head-row
    rhythm. The only difference is the finalize: scheduled rows have
    no winner/score, but DO carry the time-of-day from the first cell.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.error("bs4 not installed; cannot parse tennis schedule")
        return []

    soup = BeautifulSoup(html, "html.parser")
    # The /matches/ page splits the schedule across multiple tables —
    # one for the first dump of UTR / sub-tour matches, one giant one
    # holding the bulk of named tournaments (Rome, Madrid, Challengers,
    # ITF), plus one or two trailing "interesting" featured tables.
    # Walk every ``class*=result`` table EXCEPT ``interesting`` ones
    # (the featured tables duplicate matches already in the main table
    # and would double-count Rome/etc).
    tables = [t for t in soup.find_all("table",
                                          class_=lambda c: c and "result" in c)
              if "interesting" not in (t.get("class") or [])]
    if not tables:
        return []

    out: list[dict] = []
    seen_ids: set[str] = set()

    for table in tables:
        current_tournament: str | None = None
        # Override the per-row tour when the head row's href encodes a
        # gender ("/atp-men/" vs "/wta-women/"). atp_itf-single returns
        # the GLOBAL schedule (men + women) regardless of the URL gender
        # token, so we can't trust the page-level default — every match
        # has to read its tour from its tournament's head row.
        current_tour: str = default_tour
        # When True, the current head's tournament is doubles — drop
        # every match in the block. Singles-only flag in the href is
        # ?type=double; absence means singles.
        skip_block = False
        pending: dict | None = None

        for tr in table.find_all("tr"):
            classes = tr.get("class") or []
            if "head" in classes:
                t_cell = tr.find("td", class_="t-name")
                tournament_name: str | None = None
                href = ""
                # Check gender span BEFORE we strip spans for the name
                # fallback below — decompose() would otherwise eat them.
                has_women_span = bool(
                    t_cell and t_cell.find("span", class_="type-women2"))
                has_men_span = bool(
                    t_cell and t_cell.find("span", class_="type-men2"))
                if t_cell:
                    a = t_cell.find("a")
                    if a:
                        tournament_name = a.get_text(strip=True)
                        href = a.get("href") or ""
                    else:
                        # ITF Futures and a few other tiers ship the
                        # head row with no <a> — strip flag/gender
                        # spans and keep the plain text label.
                        for sp in t_cell.find_all("span"):
                            sp.decompose()
                        tournament_name = t_cell.get_text(strip=True) or None
                current_tournament = tournament_name
                skip_block = "type=double" in href
                if "wta-women" in href:
                    current_tour = "wta"
                elif "atp-men" in href:
                    current_tour = "atp"
                elif has_women_span:
                    current_tour = "wta"
                elif has_men_span:
                    current_tour = "atp"
                else:
                    current_tour = default_tour
                pending = None
                continue
            if skip_block:
                continue

            first_cell = tr.find("td", class_="first")
            is_match_first = first_cell is not None
            if is_match_first:
                # The time cell has overlay icons (Live/1xBet/TV) merged
                # into its text content — extract just the leading HH:MM.
                raw_time = first_cell.get_text(strip=True) or ""
                m = re.match(r"(\d{1,2}:\d{2})", raw_time)
                time_text = m.group(1) if m else None
                pending = {
                    "tournament": current_tournament,
                    "tour": current_tour,
                    "date": date,
                    "start_time": time_text,
                    "source_match_id": None,
                    "p1_name": None,
                    "p1_te_id": None,
                    "p2_name": None,
                    "p2_te_id": None,
                    "p1_sets": None,
                    "p2_sets": None,
                    "p1_set_scores": [],
                    "p2_set_scores": [],
                }
                link = tr.find("a", href=re.compile(r"match-detail"))
                if link:
                    m = re.search(r"id=(\d+)", link.get("href", ""))
                    if m:
                        pending["source_match_id"] = m.group(1)
                _fill_player(tr, pending, slot="p1")
                continue

            if pending is None:
                continue
            _fill_player(tr, pending, slot="p2")
            match = _finalize_schedule_pair(pending)
            pending = None
            if match and match["source_match_id"] not in seen_ids:
                seen_ids.add(match["source_match_id"])
                out.append(match)

    return out


def _finalize_schedule_pair(p: dict) -> dict | None:
    """Convert an accumulated schedule-page pair into a row, or None
    if either player slot didn't fill (head misalignment, parser
    drift). Schedule rows never have a winner — that's the point.
    """
    if not (p.get("p1_name") and p.get("p2_name")):
        return None
    if not p.get("source_match_id"):
        return None
    return {
        "source": "tennisexplorer",
        "source_match_id": p["source_match_id"],
        "date": p["date"],
        "start_time": p.get("start_time"),
        "tour": p["tour"],
        "tournament": p.get("tournament"),
        "p1_name": p["p1_name"],
        "p1_te_id": p.get("p1_te_id"),
        "p2_name": p["p2_name"],
        "p2_te_id": p.get("p2_te_id"),
    }


def _finalize_pair(p: dict) -> dict | None:
    """Convert an accumulated pair into a result row, or None if the
    pair is a future / abandoned / data-incomplete match.

    Winner is decided by sets-won; pushes / unfinished matches without
    a sets-won value emit None so the caller can skip rather than
    misattribute.
    """
    if not (p.get("p1_name") and p.get("p2_name")):
        return None
    if not p.get("source_match_id"):
        return None
    s1, s2 = p.get("p1_sets"), p.get("p2_sets")
    if s1 is None or s2 is None:
        return None
    if s1 == s2:
        # Walkover / retire / unfinished — no clear winner from sets
        return None
    winner = "p1" if s1 > s2 else "p2"
    # Build winner-first score string ("6-2 6-4")
    p1_sc = p.get("p1_set_scores") or []
    p2_sc = p.get("p2_set_scores") or []
    n_sets = min(len(p1_sc), len(p2_sc))
    if winner == "p1":
        sets_text = " ".join(f"{p1_sc[i]}-{p2_sc[i]}" for i in range(n_sets))
    else:
        sets_text = " ".join(f"{p2_sc[i]}-{p1_sc[i]}" for i in range(n_sets))
    return {
        "source": "tennisexplorer",
        "source_match_id": p["source_match_id"],
        "date": p["date"],
        "tour": p["tour"],
        "tournament": p.get("tournament"),
        "p1_name": p["p1_name"],
        "p1_te_id": p.get("p1_te_id"),
        "p2_name": p["p2_name"],
        "p2_te_id": p.get("p2_te_id"),
        "winner": winner,
        "score": sets_text,
    }


# ── Public API ───────────────────────────────────────────────────

def fetch_schedule_for_date(date: str,
                             type_keys: Iterable[str] = ("atp_itf", "wta_itf"),
                             ) -> list[dict]:
    """Pull all upcoming-match pages for ``date`` and return a flat list
    of scheduled rows. ESPN's tennis scoreboard misses Challenger / ITF
    Futures / WTA125 entirely; HR trades them but only ships names. TE
    is the missing canonical source — same /matches/ URL family as the
    results pages, just empty result cells until the match plays.

    Default ``type_keys`` is the two ITF-pages because they're SUPERSETS:
    atp-itf-single returns every ATP-side match (main + Challenger +
    ITF Futures), wta-itf-single returns every WTA-side match (main +
    WTA125 + ITF Futures). The plain atp / wta pages duplicate matches
    already on the ITF pages — fetching all four hits TE 4x for the
    same data and the cross-page dedupe below collapses them anyway.
    Errors per page are logged but never raised."""
    out: list[dict] = []
    seen_ids: set[str] = set()
    for tk in type_keys:
        url = _schedule_url(tk, date)
        body = _fetch(url)
        if not body:
            continue
        default_tour = "atp" if tk.startswith("atp") else "wta"
        rows = _parse_schedule_page(body, default_tour=default_tour, date=date)
        added = 0
        for r in rows:
            mid = r.get("source_match_id")
            if not mid or mid in seen_ids:
                continue
            seen_ids.add(mid)
            out.append(r)
            added += 1
        logger.info("tennis_schedule: %s/%s → %d matches (%d new)",
                    date, tk, len(rows), added)
    return out


def fetch_results_for_date(date: str,
                            type_keys: Iterable[str] = ("atp_itf", "wta_itf"),
                            ) -> list[dict]:
    """Pull all tour pages for ``date`` and return a flat list of
    result rows. Errors per page are logged but never raised — a
    partial-tour failure shouldn't abort the whole settle pass.

    Default ``type_keys`` is the two ITF-pages because each is a
    SUPERSET of its non-ITF sibling (verified 2026-05-07: atp_itf
    returns every ATP-side match including main + Challenger + ITF
    Futures; same for wta_itf). Pulling all four hits TE 4x per date
    for the same data — same optimization as the schedule fetcher."""
    out: list[dict] = []
    seen_ids: set[str] = set()
    for tk in type_keys:
        url = _date_url(tk, date)
        body = _fetch(url)
        if not body:
            continue
        default_tour = "atp" if tk.startswith("atp") else "wta"
        rows = _parse_results_page(body, default_tour=default_tour, date=date)
        added = 0
        for r in rows:
            mid = r.get("source_match_id")
            if not mid or mid in seen_ids:
                continue
            seen_ids.add(mid)
            out.append(r)
            added += 1
        logger.info("tennis_results: %s/%s -> %d matches (%d new)",
                    date, tk, len(rows), added)
    return out


# ── Persistence ─────────────────────────────────────────────────

def store_results(rows: list[dict]) -> dict:
    """Insert into tennis_match_results, resolving Sackmann ids when
    possible. Idempotent on (source, source_match_id).

    Returns {"inserted": int, "updated": int, "resolved": int}.
    """
    from engine.tennis_db import (get_conn, ensure_tables, resolve_player_id)
    ensure_tables()
    conn = get_conn()
    inserted = 0
    updated = 0
    resolved = 0

    # Pre-compute the set of currently-stored match ids so we know
    # which rows are inserts vs updates without a SELECT per row.
    existing = {
        r[0] for r in conn.execute(
            "SELECT source_match_id FROM tennis_match_results "
            "WHERE source = 'tennisexplorer'"
        ).fetchall()
    }

    now_iso = datetime.now(timezone.utc).isoformat()
    # Wrap all 393+ inserts in a single explicit transaction. The
    # tennis_db connection runs in autocommit mode (isolation_level=
    # None), so without BEGIN IMMEDIATE each INSERT becomes its own
    # txn → its own fsync → minute-scale runtime when the backend is
    # holding any concurrent writer. Settler froze 2026-05-28 because
    # the per-row commit model lost the race against the live picks
    # cache writer over and over.
    conn.execute("BEGIN IMMEDIATE")
    try:
        for r in rows:
            # Resolve TE name → Sackmann id via our active-roster fuzzy
            # resolver. ``recent_only`` keeps stale namesakes out.
            p1_id = _resolve_te_name(r["p1_name"], r["tour"], resolve_player_id)
            p2_id = _resolve_te_name(r["p2_name"], r["tour"], resolve_player_id)
            if p1_id and p2_id:
                resolved += 1
            try:
                conn.execute(
                    """
                    INSERT INTO tennis_match_results (
                        source, source_match_id, date, tour, tournament,
                        p1_name, p1_id, p1_te_id,
                        p2_name, p2_id, p2_te_id,
                        winner, score, fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source, source_match_id) DO UPDATE SET
                        winner = excluded.winner,
                        score = excluded.score,
                        p1_id = COALESCE(excluded.p1_id, p1_id),
                        p2_id = COALESCE(excluded.p2_id, p2_id),
                        p1_te_id = COALESCE(excluded.p1_te_id, p1_te_id),
                        p2_te_id = COALESCE(excluded.p2_te_id, p2_te_id),
                        fetched_at = excluded.fetched_at
                    """,
                    (
                        r["source"], r["source_match_id"], r["date"],
                        r["tour"], r.get("tournament"),
                        r["p1_name"], p1_id, r.get("p1_te_id"),
                        r["p2_name"], p2_id, r.get("p2_te_id"),
                        r["winner"], r.get("score"), now_iso,
                    ),
                )
                if r["source_match_id"] in existing:
                    updated += 1
                else:
                    inserted += 1
            except Exception as e:
                logger.warning("store_results insert failed for %s: %s",
                               r.get("source_match_id"), e)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {"inserted": inserted, "updated": updated, "resolved": resolved}


# Cache TE name → player_id resolutions across a single store_results
# run. _resolve_te_name was costing ~80ms per call (3930 calls per
# 5-day fetch = 5 min of blocking). The same TE name shows up dozens
# of times across days/tournaments, so memoizing on (tour, te_name)
# collapses to a few hundred unique resolutions.
_resolution_cache: dict[tuple[str, str], int | None] = {}


def _clear_resolution_cache() -> None:
    _resolution_cache.clear()


def _resolve_te_name(te_name: str, tour: str, resolver) -> int | None:
    """Tennis Explorer renders names as "Sinner J." (last + space +
    initial + period). Existing ``resolve_player_id`` works on full
    "First Last" strings. We synthesize a probable full-name prefix
    by token-rotating: "Sinner J." → "J Sinner" → resolver. The
    resolver's last-name + active-only fallback closes the gap when
    the synthesized name doesn't exact-match.

    Process-scoped memoization keyed on (tour, raw te_name) — store_results
    invokes this ~4k times per 5-day fetch and the same names repeat
    heavily across days, so the cache turns 5-min runs into seconds.
    """
    if not te_name:
        return None
    cache_key = (tour, te_name)
    if cache_key in _resolution_cache:
        return _resolution_cache[cache_key]
    # Strip seed parens "(2)" if present
    name = re.sub(r"\s*\(\d+\)\s*$", "", te_name).strip()
    # Token-rotate "Last F." → "F Last"
    parts = name.split()
    if len(parts) >= 2 and parts[-1].rstrip(".").isalpha() and len(parts[-1].rstrip(".")) <= 2:
        last = " ".join(parts[:-1])
        initial = parts[-1].rstrip(".")
        synth = f"{initial} {last}"
    else:
        synth = name
    result = resolver(tour, synth, recent_only=True)
    _resolution_cache[cache_key] = result
    return result


# ── Photo backfill helper ───────────────────────────────────────

def fetch_photo_for(te_id: str) -> str | None:
    """Hit a Tennis Explorer player profile, extract the headshot URL.
    Returns absolute URL or None. Used by tennis_player_photos as a
    fallback when ESPN's search API misses (Challenger / ITF players
    rarely show up on ESPN)."""
    if not te_id:
        return None
    body = _fetch(f"{_BASE}/player/{te_id}/")
    if not body:
        return None
    # The headshot lives in a <div id="plDetail"> ... <img src="/res/img/player/...">
    m = re.search(r'<img[^>]+src="(/res/img/player/[^"]+)"', body)
    if not m:
        return None
    return _BASE + m.group(1)


# ── CLI ─────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    import argparse
    import json
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(prog="scrapers.tennis_results")
    ap.add_argument("--date", default=None,
                    help="YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--lookback", type=int, default=1,
                    help="Days to fetch back from --date (default 1)")
    ap.add_argument("--no-store", action="store_true",
                    help="Print rows without writing to DB")
    args = ap.parse_args(argv)

    from datetime import date as _date, timedelta
    base = (_date.fromisoformat(args.date) if args.date
            else (_date.today() - timedelta(days=1)))
    summaries = []
    for i in range(args.lookback):
        d = (base - timedelta(days=i)).isoformat()
        rows = fetch_results_for_date(d)
        if args.no_store:
            print(f"\n=== {d}  ({len(rows)} matches) ===")
            for r in rows[:10]:
                print(f"  {r['tournament']}: {r['p1_name']} vs "
                      f"{r['p2_name']} winner={r['winner']} score={r['score']!r}")
        else:
            s = store_results(rows)
            s["date"] = d
            s["fetched"] = len(rows)
            summaries.append(s)
    if not args.no_store:
        print(json.dumps(summaries, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())


__all__ = ["fetch_results_for_date", "store_results", "fetch_photo_for"]
