"""Backfill MLB historical picks into the unified table.

Reads ``mlb.picks`` (the existing per-sport table) and writes the
canonical shape. Idempotent — the recorder's INSERT OR IGNORE on the
unique key means re-running is safe.

After backfill we run a parity audit:
  - row counts per (bet_type, result) bucket
  - stake-weighted profit and ROI

If parity matches, callers can flip the writes to the new table.
"""
from __future__ import annotations

import logging
import sqlite3

from ._game_key import GameKey
from ._recorder import record_pick
from ._schema import get_conn as _unified_conn
from ._types import Pick, Scope, Variant, Result


logger = logging.getLogger(__name__)


# Map old bet_type strings → unified (scope, bet_type, variant). The
# old MLB picks table mixes case and uses "1st INN" for NRFI/YRFI.
def _classify(bet_type: str) -> tuple[Scope, str, Variant]:
    bt = (bet_type or "").upper().strip()
    if bt in ("ML", "RL", "O/U"):
        return Scope.FULL, bt, Variant.MAIN
    if bt in ("ALT RL", "ALT O/U"):
        return Scope.FULL, bt, Variant.ALT
    if bt == "1ST INN":
        return Scope.FULL, "1st INN", Variant.DERIVATIVE
    if bt.startswith("F5"):
        # F5 ML / F5 O/U / F5 RL → scope F5 isn't an enum member yet,
        # but H1 maps to "first half" semantically (first 5 innings = MLB's H1).
        # We use FULL+derivative for the cleanest backfill — downstream
        # readers filter on bet_type starting with "F5" to scope.
        return Scope.FULL, bt, Variant.DERIVATIVE
    if "TEAM TOTAL" in bt or "PITCHER" in bt or "BATTER" in bt:
        return Scope.FULL, bt, Variant.PROP
    return Scope.FULL, bt, Variant.MAIN


def backfill_mlb(*, limit: int | None = None,
                  dry_run: bool = False) -> dict:
    """Walk mlb.picks, write to picks_unified. Returns counts."""
    from ..db import get_conn as _mlb_conn
    src = _mlb_conn()
    src.row_factory = sqlite3.Row
    sql = ("SELECT * FROM picks ORDER BY id ASC"
           + (f" LIMIT {int(limit)}" if limit else ""))
    rows = src.execute(sql).fetchall()
    out = {"read": len(rows), "inserted": 0, "skipped_no_gid": 0,
            "skipped_existing": 0, "errors": 0}

    unified = _unified_conn()

    for r in rows:
        try:
            game_id = r["game_id"]  # MLB game_pk
            if not game_id:
                out["skipped_no_gid"] += 1
                continue
            scope, bet_type_clean, variant = _classify(r["bet_type"])
            result = (Result(r["result"])
                      if r["result"] in ("W", "L", "P", "V") else None)
            pick = Pick(
                sport="mlb",
                league="mlb",
                game_key=str(GameKey.for_mlb(game_id)),
                pick_date=r["date"] or "",
                matchup=r["matchup"] or "",
                scope=scope,
                bet_type=bet_type_clean,
                variant=variant,
                pick_text=r["pick"] or "",
                side=None,
                line=None,
                odds=int(r["odds"] or 0),
                closing_odds=r["closing_odds"],
                prob=float(r["model_prob"] or 0.0),
                prob_raw=None,
                edge_pct=float(r["edge"] or 0.0),
                stake_units=float(r["stake_units"] or 0.0),
                created_at=r["created_at"],
                settled_at=r["settled_at"],
                result=result,
                profit=r["profit"],
            )
            if dry_run:
                out["inserted"] += 1
                continue
            before_id = unified.execute(
                "SELECT id FROM picks WHERE game_key=? AND scope=? "
                "  AND bet_type=? AND pick_text=?",
                (pick.game_key, scope.value, bet_type_clean, pick.pick_text),
            ).fetchone()
            if before_id:
                out["skipped_existing"] += 1
                continue
            record_pick(pick)
            out["inserted"] += 1
        except Exception as e:
            logger.warning("backfill_mlb: id=%s crash: %s", r["id"], e)
            out["errors"] += 1
    return out


def parity_audit_mlb() -> dict:
    """Compare row counts + stake-weighted profit between old + new
    tables. Should agree (modulo skips for no-gid rows) once backfill
    completes."""
    from ..db import get_conn as _mlb_conn
    src = _mlb_conn()
    src.row_factory = sqlite3.Row
    unified = _unified_conn()

    old = src.execute(
        "SELECT COUNT(*) as n, "
        "       SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) AS w, "
        "       SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) AS l, "
        "       COALESCE(SUM(profit * COALESCE(stake_units, 1.0)), 0) AS profit "
        "FROM picks"
    ).fetchone()
    new = unified.execute(
        "SELECT COUNT(*) as n, "
        "       SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) AS w, "
        "       SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) AS l, "
        "       COALESCE(SUM(profit * stake_units), 0) AS profit "
        "FROM picks WHERE sport='mlb'"
    ).fetchone()
    return {
        "old": dict(old),
        "new": dict(new),
        "matches": (
            old["n"] == new["n"]
            and (old["w"] or 0) == (new["w"] or 0)
            and (old["l"] or 0) == (new["l"] or 0)
            and abs((old["profit"] or 0) - (new["profit"] or 0)) < 0.05
        ),
    }
