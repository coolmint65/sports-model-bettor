"""Basketball framework → unified backfill.

Walks every registered basketball-framework league (skipping NBA which
has its own backfill) and migrates ``picks`` to unified rows under
``sport='basketball', league={league}``.
"""
from __future__ import annotations

import logging
import sqlite3

from ._game_key import GameKey
from ._recorder import record_pick
from ._schema import get_conn as _unified_conn
from ._types import Pick, Result, Scope, Variant


logger = logging.getLogger(__name__)


def _classify(bet_type: str) -> tuple[Scope, str, Variant]:
    bt = (bet_type or "").upper().strip()
    if bt.startswith("Q1_") or bt.endswith("Q1"):
        return Scope.Q1, bet_type, Variant.MAIN
    if bt.startswith("Q2_") or bt.endswith("Q2"):
        return Scope.Q2, bet_type, Variant.MAIN
    if bt.startswith("Q3_") or bt.endswith("Q3"):
        return Scope.Q3, bet_type, Variant.MAIN
    if bt.startswith("Q4_") or bt.endswith("Q4"):
        return Scope.Q4, bet_type, Variant.MAIN
    if bt.startswith("H1"):
        return Scope.H1, bet_type, Variant.MAIN
    if bt.startswith("ALT "):
        return Scope.FULL, bet_type, Variant.ALT
    return Scope.FULL, bet_type, Variant.MAIN


def backfill_basketball(*, leagues: list[str] | None = None) -> dict:
    from ..basketball import LEAGUE_REGISTRY
    from ..basketball._db import get_conn as _bb_conn, picks_table
    if leagues is None:
        leagues = [k for k in LEAGUE_REGISTRY if k != "nba"]
    per_league = {}
    unified = _unified_conn()
    for league in leagues:
        out = {"read": 0, "inserted": 0, "skipped": 0, "errors": 0}
        try:
            conn = _bb_conn(league)
            conn.row_factory = sqlite3.Row
            p_tbl = picks_table(league)
            rows = conn.execute(
                f"SELECT * FROM {p_tbl} ORDER BY id ASC"
            ).fetchall()
        except Exception as e:
            logger.warning("basketball backfill %s: %s", league, e)
            per_league[league] = {"error": str(e)}
            continue
        out["read"] = len(rows)
        for r in rows:
            try:
                gid = r["game_id"]
                if not gid:
                    out["skipped"] += 1
                    continue
                scope, bt, variant = _classify(r["bet_type"])
                res = (Result(r["result"])
                       if r["result"] in ("W", "L", "P", "V") else None)
                pick = Pick(
                    sport="basketball", league=league,
                    game_key=str(GameKey.for_basketball(league, gid)),
                    pick_date=r["date"] or "",
                    matchup=r["matchup"] or "",
                    scope=scope, bet_type=bt, variant=variant,
                    pick_text=r["pick"] or "",
                    odds=int(r["odds"] or 0),
                    closing_odds=r["closing_odds"]
                                  if "closing_odds" in r.keys() else None,
                    prob=float(r["model_prob"] or 0.0),
                    edge_pct=float(r["edge"] or 0.0),
                    stake_units=float(r["stake_units"] or 0.0)
                                 if "stake_units" in r.keys() else 0.0,
                    created_at=r["created_at"],
                    settled_at=r["settled_at"]
                                if "settled_at" in r.keys() else None,
                    result=res, profit=r["profit"],
                )
                existing = unified.execute(
                    "SELECT 1 FROM picks WHERE game_key=? AND scope=? "
                    "  AND bet_type=? AND pick_text=?",
                    (pick.game_key, scope.value, bt, pick.pick_text),
                ).fetchone()
                if existing:
                    out["skipped"] += 1
                    continue
                record_pick(pick)
                out["inserted"] += 1
            except Exception as e:
                logger.warning("backfill %s id=%s: %s", league, r["id"], e)
                out["errors"] += 1
        per_league[league] = out
    return per_league
