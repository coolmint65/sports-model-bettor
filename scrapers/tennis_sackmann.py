"""
Tennis ingest from Jeff Sackmann's GitHub repos.

ATP: https://github.com/JeffSackmann/tennis_atp
WTA: https://github.com/JeffSackmann/tennis_wta

Both repos publish one ``{tour}_matches_{year}.csv`` file per season
plus a single ``{tour}_players.csv`` master roster. CSV format is
stable across years (Sackmann is meticulous).

Why these CSVs and not the live-results endpoints:
    The Sackmann files are the canonical training corpus for tennis
    Elo. They go back decades, are pre-cleaned, and update within a
    week or two of completed events. For live (today's draws) we'll
    layer ESPN tennis on top in a later phase.

Usage::

    python -m scripts.run scrapers.tennis_sackmann --full
    python -m scripts.run scrapers.tennis_sackmann --tour atp --years 2023 2024 2025
    python -m scripts.run scrapers.tennis_sackmann --tour wta --since-year 2020

Idempotent: matches dedupe on (tour, match_id), players on
(tour, player_id), so re-running just brings the DB up to date.
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Iterable

logger = logging.getLogger(__name__)


# Repo URLs
_BASES = {
    "atp": "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master",
    "wta": "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master",
}

# Earliest year the ATP/WTA repos consistently carry full match data.
# Earlier years exist (ATP back to 1968) but stat columns become
# sparse — for Elo we want at least 2000 so the rating decay has
# clean data to work with.
_DEFAULT_SINCE = {"atp": 2000, "wta": 2000}

# Current year cap. Auto-bumped on each call so a Jan 2 run picks up
# the new year's file the moment Sackmann publishes it.
def _current_year() -> int:
    return datetime.now().year


# ── Networking ─────────────────────────────────────────────────

def _fetch(url: str, retries: int = 3, sleep: float = 0.5) -> str | None:
    """Single-shot CSV fetch. Returns body text or None on failure."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "tennis-ingest/1.0",
                         "Accept": "text/csv,*/*"},
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.debug("404 for %s (file may not exist yet)", url)
                return None
            logger.warning("HTTP %s on %s, retry %d/%d", e.code, url,
                           attempt + 1, retries)
        except (urllib.error.URLError, OSError) as e:
            logger.warning("network error on %s: %s (retry %d/%d)",
                           url, e, attempt + 1, retries)
        time.sleep(sleep * (attempt + 1))
    return None


# ── Field coercion ─────────────────────────────────────────────

def _to_int(v) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _to_float(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _normalize_date(yyyymmdd: str | None) -> str | None:
    """Sackmann ships tourney_date as 8-digit YYYYMMDD. Convert to
    YYYY-MM-DD for SQL date handling."""
    if not yyyymmdd or len(yyyymmdd) < 8:
        return None
    s = str(yyyymmdd).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    # Already ISO?
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    return None


def _match_key(tourney_date: str | None, tourney_id: str | None,
                match_num: str | None) -> str:
    """Synthesize a stable per-match key. Sackmann's natural composite
    is (tourney_date, tourney_id, match_num) — unique even across
    years. Underscore-joined so SQLite sorts cleanly."""
    return f"{tourney_date or ''}_{tourney_id or ''}_{match_num or ''}"


# ── Players ───────────────────────────────────────────────────

def _fetch_players(tour: str) -> list[dict]:
    """Pull the master player roster CSV. Header (Sackmann)::

        player_id, name_first, name_last, hand, dob, ioc, height
    """
    url = f"{_BASES[tour]}/{tour}_players.csv"
    body = _fetch(url)
    if not body:
        return []
    reader = csv.DictReader(io.StringIO(body))
    out: list[dict] = []
    for row in reader:
        pid = _to_int(row.get("player_id"))
        if pid is None:
            continue
        first = (row.get("name_first") or "").strip()
        last = (row.get("name_last") or "").strip()
        full = f"{first} {last}".strip()
        out.append({
            "tour": tour,
            "player_id": pid,
            "name": full,
            "name_first": first or None,
            "name_last": last or None,
            "hand": (row.get("hand") or "").strip() or None,
            "dob": _normalize_date(row.get("dob")),
            "country": (row.get("ioc") or "").strip() or None,
            "height_cm": _to_int(row.get("height")),
        })
    return out


def _upsert_players(rows: list[dict]) -> int:
    if not rows:
        return 0
    from engine.tennis_db import get_conn, ensure_tables
    ensure_tables()
    conn = get_conn()
    inserted = 0
    for r in rows:
        try:
            cur = conn.execute(
                "INSERT OR REPLACE INTO tennis_players "
                "(tour, player_id, name, name_first, name_last, "
                " hand, dob, country, height_cm) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (r["tour"], r["player_id"], r["name"], r["name_first"],
                 r["name_last"], r["hand"], r["dob"], r["country"],
                 r["height_cm"]),
            )
            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            logger.warning("player insert failed for %s/%s: %s",
                           r["tour"], r["player_id"], e)
    return inserted


# ── Matches ────────────────────────────────────────────────────

_MATCH_COLUMNS = (
    "tour", "match_id",
    "tourney_id", "tourney_name", "tourney_level", "tourney_date",
    "surface", "draw_size", "best_of", "round", "minutes",
    "winner_id", "winner_name", "winner_seed", "winner_entry",
    "winner_hand", "winner_age", "winner_rank", "winner_rank_pts",
    "loser_id", "loser_name", "loser_seed", "loser_entry",
    "loser_hand", "loser_age", "loser_rank", "loser_rank_pts",
    "score",
    "w_ace", "w_df", "w_svpt", "w_1stIn", "w_1stWon", "w_2ndWon",
    "w_SvGms", "w_bpSaved", "w_bpFaced",
    "l_ace", "l_df", "l_svpt", "l_1stIn", "l_1stWon", "l_2ndWon",
    "l_SvGms", "l_bpSaved", "l_bpFaced",
)


def _row_to_match(tour: str, row: dict) -> dict | None:
    tourney_date = _normalize_date(row.get("tourney_date"))
    if not tourney_date:
        return None
    match_id = _match_key(tourney_date, row.get("tourney_id"),
                           row.get("match_num"))
    return {
        "tour":            tour,
        "match_id":        match_id,
        "tourney_id":      row.get("tourney_id") or None,
        "tourney_name":    row.get("tourney_name") or None,
        "tourney_level":   row.get("tourney_level") or None,
        "tourney_date":    tourney_date,
        "surface":         row.get("surface") or None,
        "draw_size":       _to_int(row.get("draw_size")),
        "best_of":         _to_int(row.get("best_of")),
        "round":           row.get("round") or None,
        "minutes":         _to_int(row.get("minutes")),
        "winner_id":       _to_int(row.get("winner_id")),
        "winner_name":     row.get("winner_name") or None,
        "winner_seed":     _to_int(row.get("winner_seed")),
        "winner_entry":    row.get("winner_entry") or None,
        "winner_hand":     row.get("winner_hand") or None,
        "winner_age":      _to_float(row.get("winner_age")),
        "winner_rank":     _to_int(row.get("winner_rank")),
        "winner_rank_pts": _to_int(row.get("winner_rank_pts")),
        "loser_id":        _to_int(row.get("loser_id")),
        "loser_name":      row.get("loser_name") or None,
        "loser_seed":      _to_int(row.get("loser_seed")),
        "loser_entry":     row.get("loser_entry") or None,
        "loser_hand":      row.get("loser_hand") or None,
        "loser_age":       _to_float(row.get("loser_age")),
        "loser_rank":      _to_int(row.get("loser_rank")),
        "loser_rank_pts":  _to_int(row.get("loser_rank_pts")),
        "score":           row.get("score") or None,
        "w_ace":           _to_int(row.get("w_ace")),
        "w_df":            _to_int(row.get("w_df")),
        "w_svpt":          _to_int(row.get("w_svpt")),
        "w_1stIn":         _to_int(row.get("w_1stIn")),
        "w_1stWon":        _to_int(row.get("w_1stWon")),
        "w_2ndWon":        _to_int(row.get("w_2ndWon")),
        "w_SvGms":         _to_int(row.get("w_SvGms")),
        "w_bpSaved":       _to_int(row.get("w_bpSaved")),
        "w_bpFaced":       _to_int(row.get("w_bpFaced")),
        "l_ace":           _to_int(row.get("l_ace")),
        "l_df":            _to_int(row.get("l_df")),
        "l_svpt":          _to_int(row.get("l_svpt")),
        "l_1stIn":         _to_int(row.get("l_1stIn")),
        "l_1stWon":        _to_int(row.get("l_1stWon")),
        "l_2ndWon":        _to_int(row.get("l_2ndWon")),
        "l_SvGms":         _to_int(row.get("l_SvGms")),
        "l_bpSaved":       _to_int(row.get("l_bpSaved")),
        "l_bpFaced":       _to_int(row.get("l_bpFaced")),
    }


def _upsert_matches(rows: list[dict]) -> int:
    if not rows:
        return 0
    from engine.tennis_db import get_conn, ensure_tables
    ensure_tables()
    conn = get_conn()
    cols = _MATCH_COLUMNS
    placeholders = ",".join("?" * len(cols))
    sql = (f"INSERT OR REPLACE INTO tennis_matches "
            f"({','.join(cols)}) VALUES ({placeholders})")
    inserted = 0
    for r in rows:
        try:
            values = tuple(r.get(c) for c in cols)
            cur = conn.execute(sql, values)
            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            logger.warning("match insert failed for %s/%s: %s",
                           r.get("tour"), r.get("match_id"), e)
    return inserted


def _fetch_year(tour: str, year: int) -> list[dict]:
    """Pull every match tier Sackmann publishes for the year.

    Main tour CSV alone covers ~3000 matches/year per tour but
    excludes ALL Challenger / Futures / ITF results — which is
    exactly the cohort our predictor was defaulting to 1500 Elo on
    (audit 2026-05-01: Matsuda-Daniel, Sawangkaew-Kinoshita, etc.
    were unrated because every prior match they played was at the
    sub-tour level Sackmann ships separately).

    File patterns:
      ATP main:        atp_matches_{year}.csv
      ATP Challenger:  atp_matches_qual_chall_{year}.csv
      ATP Futures:     atp_matches_futures_{year}.csv
      WTA main:        wta_matches_{year}.csv
      WTA Qual+ITF:    wta_matches_qual_itf_{year}.csv

    Some files won't exist for a given year (Challenger started
    1978, Futures 1991, etc.) — missing files just no-op.
    """
    if tour == "atp":
        urls = [
            f"{_BASES[tour]}/atp_matches_{year}.csv",
            f"{_BASES[tour]}/atp_matches_qual_chall_{year}.csv",
            f"{_BASES[tour]}/atp_matches_futures_{year}.csv",
        ]
    elif tour == "wta":
        urls = [
            f"{_BASES[tour]}/wta_matches_{year}.csv",
            f"{_BASES[tour]}/wta_matches_qual_itf_{year}.csv",
        ]
    else:
        urls = [f"{_BASES[tour]}/{tour}_matches_{year}.csv"]

    out: list[dict] = []
    for url in urls:
        body = _fetch(url)
        if not body:
            continue
        reader = csv.DictReader(io.StringIO(body))
        for row in reader:
            m = _row_to_match(tour, row)
            if m is not None:
                out.append(m)
    return out


# ── Top-level ──────────────────────────────────────────────────

def ingest_tour(tour: str, *, years: Iterable[int] | None = None,
                since_year: int | None = None,
                full: bool = False) -> dict:
    """Ingest matches for one tour. Returns ``{tour, years, players,
    matches}``.

    Year selection precedence:
      1. ``years`` explicit list
      2. ``since_year`` lower bound (defaults to current year when
         ``full=False``, _DEFAULT_SINCE[tour] when ``full=True``)
    """
    if tour not in _BASES:
        raise ValueError(f"unknown tour: {tour!r}")

    if years is None:
        cur = _current_year()
        if since_year is None:
            since_year = _DEFAULT_SINCE[tour] if full else cur
        years = list(range(int(since_year), cur + 1))

    summary = {"tour": tour, "years": list(years),
               "players": 0, "matches": 0, "files_found": 0}

    # Players are global to the tour — fetch once per call.
    players = _fetch_players(tour)
    summary["players"] = _upsert_players(players)

    for year in years:
        rows = _fetch_year(tour, year)
        if rows:
            summary["files_found"] += 1
            inserted = _upsert_matches(rows)
            summary["matches"] += inserted
            logger.info("[%s %s] %d matches", tour, year, inserted)
        else:
            logger.info("[%s %s] (no file or empty)", tour, year)
    return summary


def ingest_all(*, since_year: int | None = None,
               full: bool = True) -> dict:
    """Ingest both tours."""
    return {t: ingest_tour(t, since_year=since_year, full=full)
            for t in _BASES}


# ── CLI ────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(prog="scrapers.tennis_sackmann")
    ap.add_argument("--tour", choices=("atp", "wta", "all"),
                    default="all")
    ap.add_argument("--full", action="store_true",
                    help="Ingest full historical range from "
                         "_DEFAULT_SINCE through current year.")
    ap.add_argument("--since-year", type=int, default=None,
                    help="Lower bound (inclusive) on years to fetch. "
                         "Overrides --full default.")
    ap.add_argument("--years", type=int, nargs="*",
                    help="Explicit year list (overrides --full / "
                         "--since-year).")
    args = ap.parse_args(argv)

    if args.tour == "all":
        result = {}
        for t in _BASES:
            result[t] = ingest_tour(
                t, years=args.years,
                since_year=args.since_year, full=args.full,
            )
    else:
        result = {args.tour: ingest_tour(
            args.tour, years=args.years,
            since_year=args.since_year, full=args.full,
        )}
    print()
    for tour, summary in result.items():
        print(f"  {tour}: {summary['matches']} matches across "
              f"{summary['files_found']} year files, "
              f"{summary['players']} players upserted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
