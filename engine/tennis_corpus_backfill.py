"""
ESPN → tennis_matches corpus backfill.

Why this exists
---------------
Sackmann's GitHub-hosted match files are our training corpus, but
he only updates them annually — as of 2026-05-01 his latest file is
the 2024 season. Everything since (Slams 2025, Masters 2025,
Challenger / ITF cycles) is a black hole for our Elo trainer + the
recent-form / walkover / retirement / density signals.

This module fills the gap by replaying COMPLETED matches from
``tennis_scheduled_matches`` (ESPN-derived) into ``tennis_matches``.
The two schemas don't perfectly align, but the fields the predictor
+ Elo trainer need are mostly there:

    tennis_scheduled_matches → tennis_matches
    --------------------------------------------------------------
    tour                     → tour
    match_id (str)           → match_id (str)
    date (YYYY-MM-DD)        → tourney_date
    tournament               → tourney_name
    surface                  → surface
    best_of                  → best_of
    p1_id (winner if winner='p1')  → winner_id  / loser_id
    p1_name                  → winner_name      / loser_name
    score                    → score
    minutes                  → NULL (ESPN doesn't ship)

Idempotent on (tour, match_id) so re-running across syncs doesn't
double-insert. Skips any row that's already in tennis_matches.

CLI::

    python -m engine.tennis_corpus_backfill                 # all settled
    python -m engine.tennis_corpus_backfill --since 2025-01-01
    python -m engine.tennis_corpus_backfill --dry-run
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def _level_code_for_corpus(tournament: str | None,
                            hr_comp: str | None = None) -> str:
    """Sackmann-style level code. Reuses the schedule classifier so
    the predictor's K-factor lookup works on backfilled rows the
    same way it does on Sackmann-native rows."""
    try:
        from .tennis_schedule import _level_code_for
        return _level_code_for(tournament, hr_comp=hr_comp)
    except Exception:
        return "A"


def backfill(since: str | None = None,
              dry_run: bool = False) -> dict:
    """Convert completed tennis_scheduled_matches + tennis_match_results
    rows into tennis_matches. Returns a summary dict.

    ``since`` (YYYY-MM-DD) bounds the lookback so re-running on the
    nightly cron only processes recently-settled rows. Default: all.

    Two pipelines feed the same destination table:
      1. ESPN scoreboard rows (tennis_scheduled_matches with status='post')
         — covers ATP/WTA main tour
      2. Tennis Explorer rows (tennis_match_results) — covers everything
         ESPN misses: Challenger, ITF Futures, WTA125. This is the main
         gap-filler for 2025-26 sub-tour data, since Sackmann's CSVs
         only ship through the prior calendar year.

    Both share the same target schema and the same idempotent
    INSERT OR IGNORE on (tour, match_id), so ordering doesn't matter
    and re-runs are safe.
    """
    from .tennis_db import get_conn, ensure_tables
    ensure_tables()
    conn = get_conn()

    summary = {"scanned": 0, "inserted": 0,
               "already_present": 0, "skipped_no_data": 0,
               "scanned_espn": 0, "scanned_te": 0,
               "inserted_espn": 0, "inserted_te": 0}

    # ── Pipeline 1: ESPN scoreboard ──
    where = ["status = 'post'", "score IS NOT NULL", "score != ''",
             "winner IS NOT NULL",
             "p1_id IS NOT NULL", "p2_id IS NOT NULL"]
    params: list = []
    if since:
        where.append("date >= ?")
        params.append(since)
    sql = (f"SELECT * FROM tennis_scheduled_matches "
           f"WHERE {' AND '.join(where)}")
    espn_rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    summary["scanned_espn"] = len(espn_rows)

    # Pre-compute existing set so insert-vs-update accounting is right.
    existing = {(r["tour"], r["match_id"]) for r in conn.execute(
        "SELECT tour, match_id FROM tennis_matches"
    ).fetchall()}

    for r in espn_rows:
        key = (r["tour"], str(r["match_id"]))
        if key in existing:
            summary["already_present"] += 1
            continue
        if r["winner"] not in ("p1", "p2"):
            summary["skipped_no_data"] += 1
            continue

        if r["winner"] == "p1":
            winner_id, winner_name = r["p1_id"], r["p1_name"]
            loser_id,  loser_name  = r["p2_id"], r["p2_name"]
        else:
            winner_id, winner_name = r["p2_id"], r["p2_name"]
            loser_id,  loser_name  = r["p1_id"], r["p1_name"]

        # Score normalization. Both sources use 'X-Y X-Y' but ESPN
        # writes p1-p2 always (regardless of who won) while Sackmann
        # writes winner-first. Convert ESPN to winner-first if winner
        # was actually p2.
        score = (r.get("score") or "").strip()
        if r["winner"] == "p2" and score:
            import re as _re
            def _flip(m):
                a, b = m.group(1), m.group(2)
                tail = m.group(3) or ""
                return f"{b}-{a}{tail}"
            score = _re.sub(r"(\d+)-(\d+)(\([^)]*\))?", _flip, score)

        level = _level_code_for_corpus(r.get("tournament"))

        if dry_run:
            summary["inserted_espn"] += 1
            continue
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO tennis_matches (
                    tour, match_id, tourney_date, tourney_name, surface,
                    tourney_level, best_of, round,
                    winner_id, winner_name, loser_id, loser_name,
                    score
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r["tour"], str(r["match_id"]),
                    r["date"], r.get("tournament"),
                    r.get("surface"), level,
                    int(r.get("best_of") or 3),
                    r.get("round"),
                    int(winner_id), winner_name,
                    int(loser_id), loser_name,
                    score,
                ),
            )
            if conn.total_changes:
                summary["inserted_espn"] += 1
                existing.add(key)
        except Exception as e:
            logger.warning("ESPN backfill insert failed for %s/%s: %s",
                           r["tour"], r["match_id"], e)

    # ── Pipeline 2: Tennis Explorer results ──
    # TE covers Challenger / ITF / WTA125 — the post-Sackmann sub-tour
    # gap that ESPN doesn't carry. Score is already winner-first from
    # the scraper, p1_id/p2_id are resolved Sackmann ids when possible.
    # Skip rows where we couldn't resolve both ids (predictor needs them
    # for Elo lookups).
    te_where = ["winner IS NOT NULL",
                "p1_id IS NOT NULL", "p2_id IS NOT NULL",
                "score IS NOT NULL", "score != ''"]
    te_params: list = []
    if since:
        te_where.append("date >= ?")
        te_params.append(since)
    te_sql = (f"SELECT * FROM tennis_match_results "
              f"WHERE {' AND '.join(te_where)}")
    te_rows = [dict(r) for r in conn.execute(te_sql, te_params).fetchall()]
    summary["scanned_te"] = len(te_rows)

    for r in te_rows:
        # Synthesize a stable match_id from the source so the unique key
        # stays disjoint from ESPN-derived ids ("hr-..." prefix on HR
        # supplement, numeric for ESPN, "te-..." for these).
        match_id = f"te-{r['source_match_id']}"
        key = (r["tour"], match_id)
        if key in existing:
            summary["already_present"] += 1
            continue

        winner = r["winner"]
        if winner not in ("p1", "p2"):
            summary["skipped_no_data"] += 1
            continue
        if winner == "p1":
            winner_id, winner_name = r["p1_id"], r["p1_name"]
            loser_id,  loser_name  = r["p2_id"], r["p2_name"]
        else:
            winner_id, winner_name = r["p2_id"], r["p2_name"]
            loser_id,  loser_name  = r["p1_id"], r["p1_name"]

        # Tier classification mirrors the schedule's logic — Madrid /
        # Aix-en-Provence / etc. all flow through the same level-code
        # function so K-factor lookup works the same way for these rows.
        level = _level_code_for_corpus(r.get("tournament"))

        if dry_run:
            summary["inserted_te"] += 1
            continue
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO tennis_matches (
                    tour, match_id, tourney_date, tourney_name, surface,
                    tourney_level, best_of, round,
                    winner_id, winner_name, loser_id, loser_name,
                    score
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r["tour"], match_id,
                    r["date"], r.get("tournament"),
                    None,  # surface — TE doesn't ship per-row, derived elsewhere
                    level,
                    3,    # best_of — most sub-tour events are BO3; main-tour
                          # men's slams override via _coerce_best_of in the
                          # schedule module if/when that pipeline rebuilds
                    None,  # round — TE doesn't expose structured round id
                    int(winner_id), winner_name,
                    int(loser_id), loser_name,
                    r.get("score"),
                ),
            )
            if conn.total_changes:
                summary["inserted_te"] += 1
                existing.add(key)
        except Exception as e:
            logger.warning("TE backfill insert failed for %s/%s: %s",
                           r["tour"], match_id, e)

    summary["scanned"] = summary["scanned_espn"] + summary["scanned_te"]
    summary["inserted"] = summary["inserted_espn"] + summary["inserted_te"]
    if not dry_run:
        conn.commit()
    return summary


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(prog="engine.tennis_corpus_backfill")
    ap.add_argument("--since", default=None,
                    help="YYYY-MM-DD lower bound on match date")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    summary = backfill(since=args.since, dry_run=args.dry_run)
    import json as _json
    print(_json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())


__all__ = ["backfill"]
