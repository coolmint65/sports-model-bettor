"""Per-component prediction log for the stacking ensemble.

Each call to ``ensemble_{mlb,nhl,nba}`` writes the component
predictions (factor / mc / gbm) for each (game, market). Joined to
realized outcomes by ``ensemble_stacker.fit_sport`` to train the
meta-learner.

Schema is intentionally narrow (game_id + market + per-component
prob/value) so it's cheap to write per call. One row per (sport,
date, game_id, market) — re-runs UPSERT, so the latest snapshot
before lock is the training row.

Storage: data/ensemble_log.db — separate from the per-sport DBs so
heavy logging never contends with picks/tracker writes.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)


_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "ensemble_log.db"

_DDL = """
CREATE TABLE IF NOT EXISTS ensemble_log (
    sport      TEXT NOT NULL,
    date       TEXT NOT NULL,
    game_id    TEXT NOT NULL,
    market     TEXT NOT NULL,
    factor_val REAL,
    mc_val     REAL,
    gbm_val    REAL,
    blended    REAL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (sport, date, game_id, market)
);
CREATE INDEX IF NOT EXISTS idx_ensemble_log_sport_market
    ON ensemble_log(sport, market);
CREATE INDEX IF NOT EXISTS idx_ensemble_log_date
    ON ensemble_log(date);
"""


def _conn() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_DDL)
    return conn


def record(sport: str, date: str, game_id: str | int, market: str,
           factor_val: float | None, mc_val: float | None,
           gbm_val: float | None, blended: float | None = None) -> None:
    """UPSERT one (sport, date, game_id, market) prediction snapshot.
    Quiet failures — never block the predict path."""
    try:
        conn = _conn()
        conn.execute(
            "INSERT INTO ensemble_log (sport, date, game_id, market, "
            "  factor_val, mc_val, gbm_val, blended, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now')) "
            "ON CONFLICT(sport, date, game_id, market) DO UPDATE SET "
            "  factor_val = excluded.factor_val, "
            "  mc_val = excluded.mc_val, "
            "  gbm_val = excluded.gbm_val, "
            "  blended = excluded.blended, "
            "  updated_at = excluded.updated_at",
            (sport.lower(), date, str(game_id), market,
             factor_val, mc_val, gbm_val, blended),
        )
        conn.commit()
    except Exception as exc:
        logger.debug("ensemble_log.record(%s) skipped: %s", market, exc)


def load_predictions(sport: str) -> dict[str, list[tuple]]:
    """Return logged predictions grouped by market::

        {market: [(game_id, factor_val, mc_val, gbm_val), ...]}

    Used by ensemble_stacker.fit_sport. game_id is TEXT for cross-sport
    portability."""
    out: dict[str, list[tuple]] = {}
    try:
        rows = _conn().execute(
            "SELECT market, game_id, factor_val, mc_val, gbm_val "
            "FROM ensemble_log WHERE sport = ?",
            (sport.lower(),),
        ).fetchall()
    except Exception as exc:
        logger.warning("ensemble_log: load failed (%s)", exc)
        return out
    for r in rows:
        out.setdefault(r["market"], []).append(
            (r["game_id"], r["factor_val"], r["mc_val"], r["gbm_val"])
        )
    return out


def stats() -> dict:
    """Quick row-count snapshot for ops."""
    try:
        rows = _conn().execute(
            "SELECT sport, market, COUNT(*) AS n FROM ensemble_log "
            "GROUP BY sport, market"
        ).fetchall()
    except Exception:
        return {}
    out: dict[str, dict[str, int]] = {}
    for r in rows:
        out.setdefault(r["sport"], {})[r["market"]] = r["n"]
    return out


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Ensemble log admin")
    p.add_argument("--stats", action="store_true",
                   help="Print row counts per (sport, market)")
    args = p.parse_args()
    if args.stats:
        s = stats()
        if not s:
            print("(no logged predictions yet)")
        for sport in sorted(s):
            print(f"== {sport.upper()} ==")
            for market in sorted(s[sport]):
                print(f"  {market:18s} {s[sport][market]:>5d}")
