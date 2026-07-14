"""Unified picks store across all sports.

Additive store (#160): writes go to a single ``unified_picks`` table that
mirrors the columns common to mlb/nba/nhl/tennis pick tables, plus a
``sport`` discriminator and a JSON ``context`` blob for sport-specific
fields (surface, tour, is_paper, etc.).

This module is **strictly additive** in this iteration:

  - Each per-sport tracker keeps writing to its own table (mlb.db.picks,
    nba.db.nba_picks, nhl.db.nhl_picks, tennis.db.tennis_picks).
  - Optionally, each tracker calls ``unified_tracker.write(...)`` as a
    write-through hook, OR the operator runs ``backfill()`` to repopulate
    the unified table from the per-sport sources.
  - No reads happen against unified_picks yet. The tracker dashboards
    still query their per-sport DB.

This lets us validate the unified shape against weeks of dual-write data
before cutting over reads (next iteration).

Schema:
    sport        TEXT NOT NULL          -- 'mlb', 'nba', 'nhl', 'tennis'
    external_id  TEXT                    -- per-sport game_id/match_id
    date         TEXT NOT NULL
    matchup      TEXT
    bet_type     TEXT NOT NULL
    pick         TEXT NOT NULL
    model_prob   REAL
    edge         REAL
    odds         INTEGER NOT NULL
    closing_odds INTEGER
    result       TEXT                    -- 'W', 'L', 'P', or NULL
    profit       REAL
    created_at   TEXT NOT NULL
    settled_at   TEXT
    context      TEXT                    -- JSON blob for sport extras

The unique pending index lets the per-sport write-through be idempotent
across re-runs without piling up duplicates while a pick is open.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)


_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "unified_picks.db"

_DDL = """
CREATE TABLE IF NOT EXISTS unified_picks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    sport        TEXT NOT NULL,
    external_id  TEXT,
    date         TEXT NOT NULL,
    matchup      TEXT,
    bet_type     TEXT NOT NULL,
    pick         TEXT NOT NULL,
    model_prob   REAL,
    edge         REAL,
    odds         INTEGER NOT NULL,
    closing_odds INTEGER,
    result       TEXT,
    profit       REAL,
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    settled_at   TEXT,
    context      TEXT
);
CREATE INDEX IF NOT EXISTS idx_unified_picks_date ON unified_picks(date);
CREATE INDEX IF NOT EXISTS idx_unified_picks_sport_date ON unified_picks(sport, date);
CREATE INDEX IF NOT EXISTS idx_unified_picks_pending
    ON unified_picks(result) WHERE result IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_unified_picks_pending_unique
    ON unified_picks(sport, date, external_id, bet_type, pick) WHERE result IS NULL;
-- Settled rows: deduped in write() by checking for an existing settled
-- row with the same key BEFORE inserting. A DB-level unique would block
-- the case where a pending row gets settled to a result that happens to
-- match an existing settled row (rare race) — caller handles via early-
-- return instead of relying on the index.
"""


def _conn() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_DDL)
    return conn


def write(
    sport: str,
    *,
    date: str,
    bet_type: str,
    pick: str,
    odds: int,
    external_id: str | None = None,
    matchup: str | None = None,
    model_prob: float | None = None,
    edge: float | None = None,
    closing_odds: int | None = None,
    result: str | None = None,
    profit: float | None = None,
    settled_at: str | None = None,
    created_at: str | None = None,
    context: dict[str, Any] | None = None,
) -> int | None:
    """Idempotent insert. Returns row id, or None on dedup.

    Pending rows dedupe on (sport, date, external_id, bet_type, pick) — the
    same tuple the per-sport trackers use for their unique pending indices.
    Settled rows are append-only (the partial unique index excludes them).
    """
    sport = sport.lower()
    if sport not in {"mlb", "nba", "nhl", "tennis"}:
        raise ValueError(f"unknown sport: {sport}")
    ctx_json = json.dumps(context, sort_keys=True) if context else None
    conn = _conn()
    try:
        # Pre-check: for SETTLED rows (result set), look up whether an
        # identical settled row already exists. The pending unique index
        # only covers result IS NULL, so without this check, re-running
        # backfill duplicates every settled row each pass.
        if result not in (None, ""):
            existing = conn.execute(
                """
                SELECT id FROM unified_picks
                 WHERE sport = ? AND date = ?
                   AND COALESCE(external_id,'') = COALESCE(?,'')
                   AND bet_type = ? AND pick = ?
                   AND result = ?
                """,
                (sport, date, external_id, bet_type, pick, result),
            ).fetchone()
            if existing:
                return None
        cur = conn.execute(
            """
            INSERT INTO unified_picks (
                sport, external_id, date, matchup, bet_type, pick,
                model_prob, edge, odds, closing_odds, result, profit,
                created_at, settled_at, context
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      COALESCE(?, datetime('now')), ?, ?)
            """,
            (
                sport, external_id, date, matchup, bet_type, pick,
                model_prob, edge, odds, closing_odds, result, profit,
                created_at, settled_at, ctx_json,
            ),
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        # Hit the partial-unique pending index — pick already recorded.
        return None
    finally:
        conn.close()


def settle(
    sport: str,
    *,
    date: str,
    bet_type: str,
    pick: str,
    external_id: str | None,
    result: str,
    profit: float,
    closing_odds: int | None = None,
    settled_at: str | None = None,
) -> int:
    """Stamp a pending unified row with its outcome. Returns rows updated.

    Matches the per-sport settle path: same (sport, date, external_id,
    bet_type, pick) tuple. Updates only rows still pending (result IS NULL)
    so a re-settle doesn't double-pay.
    """
    conn = _conn()
    try:
        cur = conn.execute(
            """
            UPDATE unified_picks
               SET result       = ?,
                   profit       = ?,
                   closing_odds = COALESCE(?, closing_odds),
                   settled_at   = COALESCE(?, datetime('now'))
             WHERE sport       = ?
               AND date        = ?
               AND bet_type    = ?
               AND pick        = ?
               AND COALESCE(external_id, '') = COALESCE(?, '')
               AND result IS NULL
            """,
            (result, profit, closing_odds, settled_at,
             sport.lower(), date, bet_type, pick, external_id),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


# ── Backfill ──

def _iter_mlb() -> Iterable[dict]:
    from .db import get_conn
    rows = get_conn().execute(
        "SELECT game_id, date, matchup, bet_type, pick, model_prob, edge, "
        "       odds, result, profit, created_at, settled_at "
        "FROM picks"
    ).fetchall()
    for r in rows:
        yield {
            "sport": "mlb",
            "external_id": str(r["game_id"]) if r["game_id"] is not None else None,
            "date": r["date"],
            "matchup": r["matchup"],
            "bet_type": r["bet_type"],
            "pick": r["pick"],
            "model_prob": r["model_prob"],
            "edge": r["edge"],
            "odds": r["odds"],
            "closing_odds": None,
            "result": r["result"],
            "profit": r["profit"],
            "created_at": r["created_at"],
            "settled_at": r["settled_at"],
            "context": None,
        }


def _iter_nba() -> Iterable[dict]:
    from .nba_db import get_conn
    rows = get_conn().execute(
        "SELECT game_id, date, matchup, bet_type, pick, model_prob, edge, "
        "       odds, closing_odds, result, profit, created_at, settled_at "
        "FROM nba_picks"
    ).fetchall()
    for r in rows:
        yield {
            "sport": "nba",
            "external_id": str(r["game_id"]) if r["game_id"] is not None else None,
            "date": r["date"],
            "matchup": r["matchup"],
            "bet_type": r["bet_type"],
            "pick": r["pick"],
            "model_prob": r["model_prob"],
            "edge": r["edge"],
            "odds": r["odds"],
            "closing_odds": r["closing_odds"],
            "result": r["result"],
            "profit": r["profit"],
            "created_at": r["created_at"],
            "settled_at": r["settled_at"],
            "context": None,
        }


def _iter_nhl() -> Iterable[dict]:
    from .nhl_tracker._helpers import _get_nhl_db
    rows = _get_nhl_db().execute(
        "SELECT game_id, date, matchup, bet_type, pick, model_prob, edge, "
        "       odds, closing_odds, result, profit, created_at, settled_at "
        "FROM nhl_picks"
    ).fetchall()
    for r in rows:
        yield {
            "sport": "nhl",
            "external_id": str(r["game_id"]) if r["game_id"] is not None else None,
            "date": r["date"],
            "matchup": r["matchup"],
            "bet_type": r["bet_type"],
            "pick": r["pick"],
            "model_prob": r["model_prob"],
            "edge": r["edge"],
            "odds": r["odds"],
            "closing_odds": r["closing_odds"],
            "result": r["result"],
            "profit": r["profit"],
            "created_at": r["created_at"],
            "settled_at": r["settled_at"],
            "context": None,
        }


def _iter_tennis() -> Iterable[dict]:
    from .tennis_db import get_conn
    rows = get_conn().execute(
        "SELECT tour, match_id, date, matchup, surface, best_of, tourney_level, "
        "       p1_id, p2_id, bet_type, pick, model_prob, edge, odds, "
        "       closing_odds, result, profit, created_at, settled_at, "
        "       conviction_score, is_paper "
        "FROM tennis_picks"
    ).fetchall()
    for r in rows:
        # Sport-specific extras live in context so the unified row stays
        # rectangular and tracker dashboards can recover them.
        ctx = {
            "tour": r["tour"],
            "surface": r["surface"],
            "best_of": r["best_of"],
            "tourney_level": r["tourney_level"],
            "p1_id": r["p1_id"],
            "p2_id": r["p2_id"],
            "conviction_score": r["conviction_score"],
            "is_paper": r["is_paper"],
        }
        yield {
            "sport": "tennis",
            "external_id": r["match_id"],
            "date": r["date"],
            "matchup": r["matchup"],
            "bet_type": r["bet_type"],
            "pick": r["pick"],
            "model_prob": r["model_prob"],
            "edge": r["edge"],
            "odds": r["odds"],
            "closing_odds": r["closing_odds"],
            "result": r["result"],
            "profit": r["profit"],
            "created_at": r["created_at"],
            "settled_at": r["settled_at"],
            "context": ctx,
        }


_SOURCES = {
    "mlb": _iter_mlb,
    "nba": _iter_nba,
    "nhl": _iter_nhl,
    "tennis": _iter_tennis,
}


def sync_for_date(sport: str, date: str) -> int:
    """Mirror per-sport rows for a single date into unified_picks.

    Idempotent — pending dedupe + settled append-only via the partial
    unique index. Designed to be called at the end of each per-sport
    ``record_picks(date=...)`` run as a write-through hook. Quiet
    failures (logs but doesn't raise) so a unified-store hiccup never
    blocks the per-sport pick from being recorded.
    """
    sport = sport.lower()
    iter_fn = _SOURCES.get(sport)
    if iter_fn is None:
        return 0
    n = 0
    try:
        for row in iter_fn():
            if row.get("date") != date:
                continue
            rid = write(**row)
            if rid is not None:
                n += 1
    except Exception as exc:
        logger.warning("unified sync_for_date %s %s skipped: %s", sport, date, exc)
    return n


def backfill(sports: Iterable[str] | None = None, *, truncate: bool = False) -> dict[str, int]:
    """Repopulate ``unified_picks`` from the per-sport tables.

    Args:
        sports: subset of {"mlb","nba","nhl","tennis"}; default all.
        truncate: if True, wipe the unified table before backfill (useful
            for re-deriving from scratch when a per-sport schema changes).

    Returns: per-sport rowcounts ingested.
    """
    targets = list(sports) if sports else list(_SOURCES.keys())

    if truncate:
        conn = _conn()
        try:
            conn.execute("DELETE FROM unified_picks")
            conn.commit()
        finally:
            conn.close()

    counts: dict[str, int] = {}
    for sport in targets:
        if sport not in _SOURCES:
            continue
        n_inserted = 0
        n_settled = 0
        try:
            for row in _SOURCES[sport]():
                # New (or pending-only) row → insert with full state.
                rid = write(**row)
                if rid is not None:
                    n_inserted += 1
                    continue
                # write() dedup'd on the pending unique index. If the
                # per-sport source has since settled this pick, sync the
                # outcome over. Without this, unified drifts: per-sport
                # tables grade nightly but unified stays stuck on the
                # original pending row forever.
                if row.get("result") not in (None, ""):
                    upd = settle(
                        row["sport"],
                        date=row["date"],
                        bet_type=row["bet_type"],
                        pick=row["pick"],
                        external_id=row.get("external_id"),
                        result=row["result"],
                        profit=float(row.get("profit") or 0.0),
                        closing_odds=row.get("closing_odds"),
                        settled_at=row.get("settled_at"),
                    )
                    if upd:
                        n_settled += 1
        except Exception as exc:
            logger.warning("unified backfill %s skipped: %s", sport, exc)
        counts[sport] = n_inserted + n_settled
        logger.info("unified backfill %s: %d inserted, %d settled",
                     sport, n_inserted, n_settled)
    return counts


def stats() -> dict[str, Any]:
    """Quick health snapshot — row counts per sport, pending counts."""
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT sport, COUNT(*) AS total, "
            "       SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) AS pending "
            "FROM unified_picks GROUP BY sport"
        ).fetchall()
    finally:
        conn.close()
    return {
        r["sport"]: {"total": r["total"], "pending": r["pending"]}
        for r in rows
    }


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Unified picks store admin")
    p.add_argument("--backfill", action="store_true",
                   help="Backfill from all per-sport tables")
    p.add_argument("--truncate", action="store_true",
                   help="Wipe unified_picks before backfill")
    p.add_argument("--sports", nargs="+",
                   choices=["mlb", "nba", "nhl", "tennis"],
                   help="Restrict to specific sports")
    p.add_argument("--stats", action="store_true",
                   help="Print row counts only")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    if args.stats:
        for sport, s in stats().items():
            print(f"{sport:>8}: {s['total']:>6} total, {s['pending']:>4} pending")
    elif args.backfill:
        counts = backfill(sports=args.sports, truncate=args.truncate)
        print("Backfill complete:")
        for sport, n in counts.items():
            print(f"  {sport:>8}: {n} rows")
    else:
        p.print_help()
