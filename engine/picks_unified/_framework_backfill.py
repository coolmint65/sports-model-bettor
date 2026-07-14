"""Framework backfills — UFL/college baseball/hockey leagues. Same shape
as basketball_backfill (per-league walk, INSERT OR IGNORE on unique).
"""
from __future__ import annotations

import logging
import sqlite3

from ._game_key import GameKey
from ._recorder import record_pick
from ._schema import get_conn as _unified_conn
from ._types import Pick, Result, Scope, Variant


logger = logging.getLogger(__name__)


def _classify_default(bet_type: str) -> tuple[Scope, str, Variant]:
    bt = (bet_type or "").upper().strip()
    if bt.startswith("ALT "):
        return Scope.FULL, bet_type, Variant.ALT
    return Scope.FULL, bet_type, Variant.MAIN


def _migrate_picks_table(sport: str, league: str, conn: sqlite3.Connection,
                          *, key_factory) -> dict:
    out = {"read": 0, "inserted": 0, "skipped": 0, "errors": 0}
    try:
        rows = conn.execute(
            "SELECT * FROM picks ORDER BY id ASC"
        ).fetchall()
    except Exception as e:
        return {"error": str(e)}
    out["read"] = len(rows)
    unified = _unified_conn()
    for r in rows:
        try:
            gid = r["game_id"]
            if not gid:
                out["skipped"] += 1
                continue
            scope, bt, variant = _classify_default(r["bet_type"])
            res = (Result(r["result"])
                   if r["result"] in ("W", "L", "P", "V") else None)
            pick = Pick(
                sport=sport, league=league,
                game_key=str(key_factory(league, gid)),
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
            logger.warning("backfill %s/%s id=%s: %s",
                            sport, league, r["id"], e)
            out["errors"] += 1
    return out


def backfill_football() -> dict:
    from ..football import LEAGUE_REGISTRY
    from ..football._db import get_conn
    per = {}
    for league in LEAGUE_REGISTRY:
        try:
            conn = get_conn(league)
            conn.row_factory = sqlite3.Row
            per[league] = _migrate_picks_table(
                "football", league, conn, key_factory=GameKey.for_football,
            )
        except Exception as e:
            per[league] = {"error": str(e)}
    return per


def backfill_baseball() -> dict:
    from ..baseball import LEAGUE_REGISTRY
    from ..baseball._db import get_conn
    per = {}
    for league in LEAGUE_REGISTRY:
        try:
            conn = get_conn(league)
            conn.row_factory = sqlite3.Row
            per[league] = _migrate_picks_table(
                "baseball", league, conn, key_factory=GameKey.for_baseball,
            )
        except Exception as e:
            per[league] = {"error": str(e)}
    return per


def backfill_hockey() -> dict:
    from ..hockey import HOCKEY_LEAGUE_REGISTRY
    per = {}
    for league in HOCKEY_LEAGUE_REGISTRY:
        if league == "nhl":
            continue
        try:
            mod = __import__(f"engine.sports.{league}.db",
                              fromlist=["get_conn"])
            conn = mod.get_conn()
            conn.row_factory = sqlite3.Row
            per[league] = _migrate_picks_table(
                "hockey", league, conn, key_factory=GameKey.for_hockey,
            )
        except Exception as e:
            per[league] = {"error": str(e)}
    return per
