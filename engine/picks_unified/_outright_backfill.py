"""Backfill for tennis / golf / motorsports — the outright sports.

Same pattern as the per-sport backfills but native_id varies:
- tennis: tennis_picks.match_id (string)
- golf: picks.tournament_id (int → string)
- motorsports: picks.race_id (string)
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


def backfill_golf() -> dict:
    from ..golf import TOUR_REGISTRY
    from ..golf._db import get_conn
    per_tour = {}
    unified = _unified_conn()
    for tour in TOUR_REGISTRY:
        out = {"read": 0, "inserted": 0, "skipped": 0, "errors": 0}
        try:
            conn = get_conn(tour)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM picks ORDER BY id"
            ).fetchall()
        except Exception as e:
            per_tour[tour] = {"error": str(e)}
            continue
        out["read"] = len(rows)
        for r in rows:
            try:
                tid = r["tournament_id"]
                if not tid:
                    out["skipped"] += 1
                    continue
                res = (Result(r["result"])
                       if r["result"] in ("W", "L", "P", "V") else None)
                pick = Pick(
                    sport="golf", league=tour,
                    game_key=str(GameKey.for_golf(tour, tid)),
                    pick_date=r["date"] or "",
                    matchup="",
                    scope=Scope.FULL,
                    bet_type=r["bet_type"] or "",
                    variant=Variant.PROP if "TOP_" in (r["bet_type"] or "")
                            else Variant.MAIN,
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
                    (pick.game_key, Scope.FULL.value, pick.bet_type, pick.pick_text),
                ).fetchone()
                if existing:
                    out["skipped"] += 1
                    continue
                record_pick(pick)
                out["inserted"] += 1
            except Exception as e:
                logger.warning("golf %s backfill id=%s: %s", tour, r["id"], e)
                out["errors"] += 1
        per_tour[tour] = out
    return per_tour


def backfill_motorsports() -> dict:
    from ..motorsports import SERIES_REGISTRY
    from ..motorsports._db import get_conn
    per_series = {}
    unified = _unified_conn()
    for series in SERIES_REGISTRY:
        out = {"read": 0, "inserted": 0, "skipped": 0, "errors": 0}
        try:
            conn = get_conn(series)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM picks ORDER BY id").fetchall()
        except Exception as e:
            per_series[series] = {"error": str(e)}
            continue
        out["read"] = len(rows)
        for r in rows:
            try:
                rid = r["race_id"]
                if not rid:
                    out["skipped"] += 1
                    continue
                res = (Result(r["result"])
                       if r["result"] in ("W", "L", "P", "V") else None)
                pick = Pick(
                    sport="motorsports", league=series,
                    game_key=str(GameKey.for_motorsports(series, rid)),
                    pick_date=r["date"] or "",
                    matchup="",
                    scope=Scope.FULL,
                    bet_type=r["bet_type"] or "",
                    variant=Variant.PROP if "TOP_" in (r["bet_type"] or "")
                            else Variant.MAIN,
                    pick_text=r["pick"] or "",
                    odds=int(r["odds"] or 0),
                    closing_odds=r["closing_odds"]
                                  if "closing_odds" in r.keys() else None,
                    prob=float(r["model_prob"] or 0.0),
                    edge_pct=float(r["edge"] or 0.0),
                    stake_units=0.0,  # motorsports legacy lacks stake column
                    created_at=r["created_at"],
                    settled_at=r["settled_at"]
                                if "settled_at" in r.keys() else None,
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
                logger.warning("motorsports %s backfill id=%s: %s",
                                series, r["id"], e)
                out["errors"] += 1
        per_series[series] = out
    return per_series
