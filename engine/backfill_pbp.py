"""
Historical play-by-play backfill.

Pulls per-game plays from ESPN's summary endpoint for completed games
in the local DB, persists them to a new ``historical_pbp`` table that
mirrors the live_pbp shape but isn't subject to the live worker's
30-min purge cycle.

Why a separate table from live_pbp:
  - live_pbp is ephemeral (gets purged 30 min after game end so the
    cache stays small)
  - historical_pbp is a permanent training corpus — once populated,
    it doesn't change unless ESPN edits a play

Source for downstream phases:
  - 5e (live GBM) trains on (prematch features + game state at time T)
    → final outcome pairs, sourced from this table
  - 5f (game-state MC) fits possession + scoring rate distributions
    from millions of historical possessions

Sport coverage:
  - NBA: ✓ via engine.live._pbp.fetch_plays (ESPN summary endpoint)
  - NHL: ✓ via engine.live._pbp_nhl.fetch_plays (NHL Stats API
    api-web.nhle.com/v1/gamecenter/{gamePk}/play-by-play)
  - MLB: ⨯ (different play-level granularity — pitch-by-pitch — and
    a separate downstream training pipeline; deferred)

Resumable
---------
The natural key (sport, game_id, play_id) is INSERT OR IGNORE'd, so
re-running picks up where it left off. There's no progress file —
the table itself is the source of truth for "what's already been
backfilled." Use ``--limit`` to cap a single run for testing.

CLI::

    python -m engine.backfill_pbp --sport nba [--season YEAR] [--limit N]
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "live.db"
_LOCK = threading.Lock()


def _conn() -> sqlite3.Connection:
    """Per-thread connection to the live db. WAL so we don't block
    the live worker if it's running concurrently."""
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def ensure_table() -> None:
    """Idempotent DDL. Mirrors live_pbp shape — same column set so
    downstream consumers can union both tables when needed."""
    with _LOCK:
        conn = _conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS historical_pbp (
                sport             TEXT NOT NULL,
                game_id           TEXT NOT NULL,
                play_id           TEXT NOT NULL,
                sequence          INTEGER,
                period            INTEGER NOT NULL,
                clock_secs        INTEGER,
                clock_display     TEXT,
                home_score        INTEGER,
                away_score        INTEGER,
                team_id           TEXT,
                type_id           TEXT,
                type_text         TEXT,
                text              TEXT,
                scoring_play      INTEGER DEFAULT 0,
                score_value       INTEGER DEFAULT 0,
                shooting_play     INTEGER DEFAULT 0,
                wallclock         TEXT,
                participants_json TEXT,
                raw_json          TEXT,
                fetched_at        TEXT NOT NULL,
                PRIMARY KEY (sport, game_id, play_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_hpbp_game_period "
            "ON historical_pbp(sport, game_id, period, sequence)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_hpbp_sport_date "
            "ON historical_pbp(sport, fetched_at)"
        )


def _pluck_completed_game_ids(sport: str, season: int | None,
                               limit: int | None) -> list[str]:
    """Return the list of completed-game ids for ``sport``.

    For NBA these are ESPN event ids (matches what _pbp.fetch_plays
    expects). For NHL these are NHL Stats API gamePks (matches what
    _pbp_nhl.fetch_plays expects). The downstream fetcher dispatch in
    ``backfill_sport`` knows which one to call.

    Skips games already represented in historical_pbp (any row at all
    for that game id counts as "ingested"). The skip is conservative —
    a partial ingestion is treated as complete.
    """
    if sport == "nba":
        from .nba_db import get_conn as _gc
        conn = _gc()
        table = "nba_games"
    elif sport == "nhl":
        from .nhl_db import get_conn as _gc
        conn = _gc()
        table = "nhl_games"
    elif sport == "wnba":
        # Basketball framework — per-league DB. WNBA carries 1982+
        # finalized games (full 2024-25 backfill).
        from .basketball._db import get_conn as _gc
        conn = _gc("wnba")
        table = "games"
    elif sport == "ncaam":
        # NCAAM is a much larger corpus (~25k finals). Use --limit to
        # cap for first-pass GBM training; full backfill takes hours.
        from .basketball._db import get_conn as _gc
        conn = _gc("ncaam")
        table = "games"
    elif sport == "afl":
        # AFL via the basketball framework (same ESPN ingest pattern,
        # different game shape). 1371 finals — small enough to backfill
        # in one pass without --limit.
        from .basketball._db import get_conn as _gc
        conn = _gc("afl")
        table = "games"
    else:
        raise ValueError(f"backfill not yet supported for sport={sport!r}")

    sql = [f"SELECT game_id FROM {table}",
           "WHERE status = 'final' AND game_id IS NOT NULL"]
    params: list[Any] = []
    if season is not None:
        sql.append("AND season = ?")
        params.append(season)
    sql.append("ORDER BY date DESC")
    if limit is not None:
        sql.append("LIMIT ?")
        params.append(int(limit))
    rows = conn.execute(" ".join(sql), params).fetchall()
    candidates = [str(r["game_id"]) for r in rows]
    if not candidates:
        return []

    # Skip games already populated. Single roundtrip with the IN clause
    # — SQLite's parameter limit is 32766, well above any season.
    placeholders = ",".join("?" * len(candidates))
    hconn = _conn()
    ensure_table()
    seen = {
        r["game_id"] for r in hconn.execute(
            f"SELECT DISTINCT game_id FROM historical_pbp "
            f"WHERE sport = ? AND game_id IN ({placeholders})",
            (sport, *candidates),
        ).fetchall()
    }
    return [g for g in candidates if g not in seen]


def _persist(sport: str, game_id: str, plays: list[dict]) -> int:
    """Write normalized plays. Returns rows inserted (zero when every
    play was already present)."""
    if not plays:
        return 0
    import json
    ts = datetime.now(timezone.utc).isoformat()
    inserted = 0
    with _LOCK:
        conn = _conn()
        for p in plays:
            try:
                participants = json.dumps(p.get("participants") or [])
                raw = json.dumps(p.get("raw") or {}, default=str)
                cur = conn.execute(
                    "INSERT OR IGNORE INTO historical_pbp ("
                    " sport, game_id, play_id, sequence, period, "
                    " clock_secs, clock_display, home_score, away_score, "
                    " team_id, type_id, type_text, text, "
                    " scoring_play, score_value, shooting_play, "
                    " wallclock, participants_json, raw_json, fetched_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (sport, str(game_id), str(p["play_id"]),
                     p.get("sequence") or 0,
                     p.get("period") or 0,
                     p.get("clock_secs"),
                     p.get("clock_display"),
                     p.get("home_score"),
                     p.get("away_score"),
                     p.get("team_id"),
                     p.get("type_id"),
                     p.get("type_text"),
                     p.get("text"),
                     1 if p.get("scoring_play") else 0,
                     p.get("score_value") or 0,
                     1 if p.get("shooting_play") else 0,
                     p.get("wallclock"),
                     participants,
                     raw,
                     ts),
                )
                if cur.rowcount > 0:
                    inserted += 1
            except Exception as e:
                logger.warning("historical_pbp insert failed for %s/%s/%s: %s",
                               sport, game_id, p.get("play_id"), e)
    return inserted


def _fetcher_for(sport: str):
    """Return the per-sport fetch_plays callable. NBA's signature is
    ``(sport, game_id)``; NHL's is ``(gamePk)`` — we wrap the NHL one
    so the orchestrator can call both the same way."""
    if sport == "nba":
        from .live._pbp import fetch_plays as _espn_fetch
        return lambda gid: _espn_fetch("nba", gid)
    if sport == "wnba":
        from .live._pbp import fetch_plays as _espn_fetch
        return lambda gid: _espn_fetch("wnba", gid)
    if sport == "ncaam":
        from .live._pbp import fetch_plays as _espn_fetch
        return lambda gid: _espn_fetch("ncaam", gid)
    if sport == "afl":
        from .live._pbp import fetch_plays as _espn_fetch
        return lambda gid: _espn_fetch("afl", gid)
    if sport == "nhl":
        from .live._pbp_nhl import fetch_plays as _nhl_fetch
        return _nhl_fetch
    raise ValueError(f"no fetcher for sport={sport!r}")


def backfill_sport(sport: str, *, season: int | None = None,
                    limit: int | None = None,
                    sleep_s: float = 0.3,
                    progress_every: int = 50) -> dict:
    """Backfill ``sport`` historical PBP. Returns a summary dict.

    ``sleep_s`` between fetches keeps us under both source providers'
    politeness thresholds (~1 req/sec). Default 0.3s = ~3 req/sec
    measured fine in live testing on both ESPN and NHL Stats API;
    bump to 1.0 if you see 429s.
    """
    fetch_plays = _fetcher_for(sport)

    summary = {
        "sport": sport, "season": season,
        "candidates": 0, "fetched": 0,
        "plays_inserted": 0, "errors": 0,
    }
    candidates = _pluck_completed_game_ids(sport, season, limit)
    summary["candidates"] = len(candidates)
    if not candidates:
        logger.info("backfill[%s]: nothing to do (all caught up)", sport)
        return summary

    logger.info("backfill[%s]: %d game(s) to fetch", sport, len(candidates))
    started = time.monotonic()
    for idx, game_id in enumerate(candidates, 1):
        try:
            plays = fetch_plays(game_id)
        except Exception as e:
            logger.warning("backfill[%s]: fetch failed for %s: %s",
                           sport, game_id, e)
            summary["errors"] += 1
            time.sleep(sleep_s)
            continue
        if not plays:
            logger.debug("backfill[%s]: no plays for %s "
                         "(ESPN returned empty)", sport, game_id)
            summary["errors"] += 1
            time.sleep(sleep_s)
            continue
        try:
            inserted = _persist(sport, game_id, plays)
        except Exception as e:
            logger.warning("backfill[%s]: persist failed for %s: %s",
                           sport, game_id, e)
            summary["errors"] += 1
            time.sleep(sleep_s)
            continue
        summary["fetched"] += 1
        summary["plays_inserted"] += inserted
        if idx % progress_every == 0:
            elapsed = time.monotonic() - started
            rate = idx / max(0.1, elapsed)
            eta = (len(candidates) - idx) / max(0.1, rate)
            logger.info("backfill[%s]: %d/%d (%.1f games/s, ~%.0fs left, "
                        "%d plays so far)",
                        sport, idx, len(candidates), rate, eta,
                        summary["plays_inserted"])
        time.sleep(sleep_s)

    elapsed = time.monotonic() - started
    logger.info("backfill[%s]: done — %d games, %d plays, %d errors in %.0fs",
                sport, summary["fetched"], summary["plays_inserted"],
                summary["errors"], elapsed)
    return summary


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    ap = argparse.ArgumentParser(prog="engine.backfill_pbp")
    ap.add_argument("--sport",
                    choices=("nba", "nhl", "wnba", "ncaam", "afl"),
                    required=True,
                    help="Sport to backfill. NBA/WNBA/NCAAM/AFL = "
                         "ESPN PBP; NHL = NHL Stats API.")
    ap.add_argument("--season", type=int, default=None,
                    help="Limit to one season (year). Default: all "
                         "completed games in the DB.")
    ap.add_argument("--limit", type=int, default=None,
                    help="Cap fetches in this run (testing).")
    ap.add_argument("--sleep", type=float, default=0.3,
                    help="Seconds between ESPN fetches (default 0.3).")
    args = ap.parse_args(argv)

    res = backfill_sport(
        args.sport, season=args.season,
        limit=args.limit, sleep_s=args.sleep,
    )
    print(f"summary: {res}")
    return 0 if res["errors"] < res["candidates"] else 1


if __name__ == "__main__":
    sys.exit(main())
