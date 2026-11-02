"""Backfill for tennis — the outright sport.

Same pattern as the per-sport backfills but native_id varies:
- tennis: tennis_picks.match_id (string)
"""
from __future__ import annotations

import logging
import sqlite3

from ._game_key import GameKey
from ._recorder import record_pick
from ._schema import get_conn as _unified_conn
from ._types import Pick, Result, Scope, Variant


logger = logging.getLogger(__name__)


def backfill_tennis() -> dict:
    from ..tennis_db import get_conn
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM tennis_picks ORDER BY id").fetchall()
    out = {"read": len(rows), "inserted": 0, "skipped": 0, "errors": 0}
    unified = _unified_conn()
    for r in rows:
        try:
            mid = r["match_id"]
            tour = (r["tour"] or "atp").lower()
            if not mid:
                out["skipped"] += 1
                continue
            res = (Result(r["result"])
                   if r["result"] in ("W", "L", "P", "V") else None)
            pick = Pick(
                sport="tennis", league=tour,
                game_key=str(GameKey.for_tennis(tour, mid)),
                pick_date=r["date"] or "",
                matchup=r["matchup"] or "",
                scope=Scope.FULL,
                bet_type=r["bet_type"] or "",
                variant=Variant.MAIN,
                pick_text=r["pick"] or "",
                odds=int(r["odds"] or 0),
                closing_odds=r["closing_odds"],
                prob=float(r["model_prob"] or 0.0),
                edge_pct=float(r["edge"] or 0.0),
                stake_units=float(r["stake_units"] or 0.0),
                created_at=r["created_at"],
                settled_at=r["settled_at"],
                result=res, profit=r["profit"],
            )
            existing = unified.execute(
                "SELECT 1 FROM picks WHERE game_key=? AND scope=? "
                "  AND bet_type=? AND pick_text=?",
                (pick.game_key, Scope.FULL.value, pick.bet_type, pick.pick_text),
            ).fetchone()
            if existing:
                out["skipped"] += 1
                continue
            record_pick(pick)
            out["inserted"] += 1
        except Exception as e:
            logger.warning("tennis backfill id=%s: %s", r["id"], e)
            out["errors"] += 1
    return out
