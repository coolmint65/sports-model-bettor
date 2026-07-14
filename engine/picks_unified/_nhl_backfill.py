"""NHL → unified backfill."""
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
    if bt.startswith("P1 "):
        return Scope.P1, bet_type, Variant.DERIVATIVE
    if bt.startswith("P2 "):
        return Scope.P2, bet_type, Variant.DERIVATIVE
    if bt.startswith("P3 "):
        return Scope.P3, bet_type, Variant.DERIVATIVE
    if bt in ("ML", "O/U", "PL"):
        return Scope.FULL, bt, Variant.MAIN
    if bt in ("ALT O/U", "ALT PL"):
        return Scope.FULL, bt, Variant.ALT
    return Scope.FULL, bet_type, Variant.MAIN


def backfill_nhl(*, limit: int | None = None) -> dict:
    from ..nhl_db import get_conn as _nhl_conn
    src = _nhl_conn()
    src.row_factory = sqlite3.Row
    sql = "SELECT * FROM nhl_picks ORDER BY id ASC"
    if limit:
        sql += f" LIMIT {int(limit)}"
    rows = src.execute(sql).fetchall()
    out = {"read": len(rows), "inserted": 0, "skipped": 0, "errors": 0}
    unified = _unified_conn()
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
                sport="nhl", league="nhl",
                game_key=str(GameKey.for_nhl(gid)),
                pick_date=r["date"] or "",
                matchup=r["matchup"] or "",
                scope=scope, bet_type=bt, variant=variant,
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
                (pick.game_key, scope.value, bt, pick.pick_text),
            ).fetchone()
            if existing:
                out["skipped"] += 1
                continue
            record_pick(pick)
            out["inserted"] += 1
        except Exception as e:
            logger.warning("nhl backfill id=%s crash: %s", r["id"], e)
            out["errors"] += 1
    return out


def parity_audit_nhl() -> dict:
    from ..nhl_db import get_conn as _nhl_conn
    src = _nhl_conn()
    src.row_factory = sqlite3.Row
    unified = _unified_conn()
    old = src.execute(
        "SELECT COUNT(*) AS n, "
        "       SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) AS w, "
        "       SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) AS l, "
        "       COALESCE(SUM(profit * COALESCE(stake_units, 1.0)), 0) AS profit "
        "FROM nhl_picks"
    ).fetchone()
    new = unified.execute(
        "SELECT COUNT(*) AS n, "
        "       SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) AS w, "
        "       SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) AS l, "
        "       COALESCE(SUM(profit * stake_units), 0) AS profit "
        "FROM picks WHERE sport='nhl'"
    ).fetchone()
    return {"old": dict(old), "new": dict(new)}
