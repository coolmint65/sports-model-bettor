"""Backfill the events table from existing per-sport picks tables.

A1's first deliverable needs the event log populated with history,
otherwise every materialized view starts empty and calibration tables
re-derived from events would lose every pre-existing data point.

Strategy: read each sport's picks table, emit one ``decision`` event
per row + (when settled) one ``settle`` event. Idempotent — picks
already represented in the events table (by sport + pick_id) are
skipped on re-run, so this can run repeatedly safely.

Sources covered:
  - mlb        : engine.db.picks (main MLB tracker)
  - mlb-deriv  : engine.db.derivative_picks (MLB derivatives)
  - nhl        : engine.nhl_db.nhl_picks
  - nba        : engine.nba_db.nba_picks
  - tennis     : engine.tennis_db.tennis_picks
  - basketball framework: each league's picks table

CLI::

    python -m engine.events_backfill                    # all sports
    python -m engine.events_backfill --sport mlb        # one sport
    python -m engine.events_backfill --dry-run          # report only
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

from . import events

logger = logging.getLogger(__name__)


def _existing_pick_ids(sport: str) -> set[int]:
    """Pick ids already represented by a decision event for this sport."""
    conn = events._get_conn()
    rows = conn.execute(
        "SELECT DISTINCT pick_id FROM events "
        "WHERE event_type = 'decision' AND sport = ? AND pick_id IS NOT NULL",
        (sport,),
    ).fetchall()
    return {int(r["pick_id"]) for r in rows if r["pick_id"] is not None}


def _existing_settle_ids(sport: str) -> set[int]:
    """Pick ids already represented by a settle event for this sport.
    Separate from decision idempotency: a pick may have been backfilled
    pre-settlement, so the decision exists but the settle doesn't yet."""
    conn = events._get_conn()
    rows = conn.execute(
        "SELECT DISTINCT pick_id FROM events "
        "WHERE event_type = 'settle' AND sport = ? AND pick_id IS NOT NULL",
        (sport,),
    ).fetchall()
    return {int(r["pick_id"]) for r in rows if r["pick_id"] is not None}


def _backfill_one(
    sport: str,
    rows: Iterable[dict],
    *,
    dry_run: bool = False,
    league: str | None = None,
) -> dict:
    """Emit decision + (when settled) settle events from a row iterable.
    Each row dict must carry: id, date, game_id, matchup, bet_type,
    pick, model_prob, edge, odds, result, profit, settled_at, created_at.
    """
    seen_decisions = _existing_pick_ids(sport)
    seen_settles = _existing_settle_ids(sport)
    out = {"sport": sport, "scanned": 0, "decision_emitted": 0,
           "settle_emitted": 0, "skipped_already_logged": 0}
    for row in rows:
        out["scanned"] += 1
        pick_id = row.get("id")
        if pick_id is None:
            continue
        pid = int(pick_id)
        decision_needed = pid not in seen_decisions
        settle_needed = (
            row.get("result") in ("W", "L", "P", "V")
            and pid not in seen_settles
        )
        if not decision_needed and not settle_needed:
            out["skipped_already_logged"] += 1
            continue
        if dry_run:
            if decision_needed:
                out["decision_emitted"] += 1
            if settle_needed:
                out["settle_emitted"] += 1
            continue
        # Emit decision event when missing — backfill timestamp from
        # created_at when available, else fall back to date midnight UTC.
        if decision_needed:
            ts = row.get("created_at")
            if not ts and row.get("date"):
                ts = f"{row['date']}T00:00:00+00:00"
            events.write_decision(
                sport=sport, league=league,
                pick_id=pid,
                game_id=str(row.get("game_id") or "") or None,
                bet_type=row.get("bet_type") or "?",
                pick_text=row.get("pick") or "?",
                raw_prob=row.get("model_prob"),
                calibrated_prob=row.get("model_prob"),
                odds=row.get("odds"),
                edge_pct=row.get("edge"),
                accepted=True,
                calibration_source="historical_backfill",
                extra={"matchup": row.get("matchup"),
                        "from_backfill": True},
                ts=ts,
            )
            out["decision_emitted"] += 1
        if settle_needed:
            settled_ts = row.get("settled_at") or row.get("date")
            if settled_ts and "T" not in settled_ts:
                settled_ts = f"{settled_ts}T23:59:59+00:00"
            events.write_settle(
                sport=sport, league=league,
                pick_id=pid,
                result=row["result"],
                profit=float(row.get("profit") or 0),
                game_id=str(row.get("game_id") or "") or None,
                bet_type=row.get("bet_type"),
                pick_text=row.get("pick"),
                ts=settled_ts,
            )
            out["settle_emitted"] += 1
    return out


# ── Per-sport pullers ────────────────────────────────────────

def _pull_mlb_main() -> Iterable[dict]:
    from .db import get_conn
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, date, game_id, matchup, bet_type, pick, "
        "       model_prob, edge, odds, result, profit, "
        "       settled_at, created_at "
        "FROM picks ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def _pull_mlb_deriv() -> Iterable[dict]:
    from .db import get_conn
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, date, game_id, matchup, bet_type, pick, "
            "       model_prob, edge, odds, result, profit, "
            "       settled_at, created_at "
            "FROM derivative_picks ORDER BY id"
        ).fetchall()
    except Exception as e:
        logger.debug("MLB deriv pull skipped: %s", e)
        return []
    return [dict(r) for r in rows]


def _pull_nhl() -> Iterable[dict]:
    from .nhl_db import get_conn
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, date, game_id, matchup, bet_type, pick, "
        "       model_prob, edge, odds, result, profit, "
        "       settled_at, created_at "
        "FROM nhl_picks ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def _pull_nba() -> Iterable[dict]:
    from .nba_db import get_conn
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, date, game_id, matchup, bet_type, pick, "
        "       model_prob, edge, odds, result, profit, "
        "       settled_at, created_at "
        "FROM nba_picks ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def _pull_tennis() -> Iterable[dict]:
    from .tennis_db import get_conn
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, date, match_id AS game_id, matchup, bet_type, pick, "
        "       model_prob, edge, odds, result, profit, "
        "       settled_at, created_at, tour "
        "FROM tennis_picks ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def _pull_basketball_framework(league: str) -> Iterable[dict]:
    from .basketball._db import get_conn
    conn = get_conn(league)
    try:
        rows = conn.execute(
            "SELECT id, date, game_id, matchup, bet_type, pick, "
            "       model_prob, edge, odds, result, profit, "
            "       settled_at, created_at "
            "FROM picks ORDER BY id"
        ).fetchall()
    except Exception as e:
        logger.debug("basketball %s pull skipped: %s", league, e)
        return []
    return [dict(r) for r in rows]


# ── Orchestrator ─────────────────────────────────────────────

def backfill_all(*, dry_run: bool = False) -> dict:
    """Backfill every sport. Returns per-sport summaries."""
    out = {}
    out["mlb"] = _backfill_one("mlb", _pull_mlb_main(), dry_run=dry_run)
    out["mlb_deriv"] = _backfill_one("mlb", _pull_mlb_deriv(),
                                       dry_run=dry_run)
    out["nhl"] = _backfill_one("nhl", _pull_nhl(), dry_run=dry_run)
    out["nba"] = _backfill_one("nba", _pull_nba(), dry_run=dry_run)
    out["tennis"] = _backfill_one("tennis", _pull_tennis(), dry_run=dry_run)
    # Basketball framework leagues — only ones that have actually
    # been onboarded with a populated DB.
    for league in ("wnba", "euroleague"):
        out[f"basketball_{league}"] = _backfill_one(
            league, _pull_basketball_framework(league),
            dry_run=dry_run, league=league,
        )
    return out


def backfill_sport(sport: str, *, dry_run: bool = False) -> dict:
    pullers = {
        "mlb":          lambda: _backfill_one("mlb", _pull_mlb_main(), dry_run=dry_run),
        "mlb_deriv":    lambda: _backfill_one("mlb", _pull_mlb_deriv(), dry_run=dry_run),
        "nhl":          lambda: _backfill_one("nhl", _pull_nhl(), dry_run=dry_run),
        "nba":          lambda: _backfill_one("nba", _pull_nba(), dry_run=dry_run),
        "tennis":       lambda: _backfill_one("tennis", _pull_tennis(), dry_run=dry_run),
        "wnba":         lambda: _backfill_one("wnba", _pull_basketball_framework("wnba"),
                                                dry_run=dry_run, league="wnba"),
        "euroleague":   lambda: _backfill_one("euroleague", _pull_basketball_framework("euroleague"),
                                                dry_run=dry_run, league="euroleague"),
    }
    if sport not in pullers:
        raise KeyError(f"Unknown sport {sport!r}; known: {sorted(pullers)}")
    return pullers[sport]()


# ── CLI ─────────────────────────────────────────────────────

def _cli() -> int:
    import argparse, json, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.events_backfill")
    ap.add_argument("--sport", default=None,
                    help="Single sport to backfill (default: all)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report counts without writing")
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    if args.sport:
        out = backfill_sport(args.sport, dry_run=args.dry_run)
    else:
        out = backfill_all(dry_run=args.dry_run)
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
