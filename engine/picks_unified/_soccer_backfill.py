"""Soccer framework → unified backfill."""
from __future__ import annotations

import logging
import sqlite3

from ._game_key import GameKey
from ._recorder import record_pick
from ._schema import get_conn as _unified_conn
from ._types import Pick, Result, Scope, Variant


logger = logging.getLogger(__name__)


def _classify(bet_type: str) -> tuple[Scope, str, Variant]:
    bt = (bet_type or "").strip()
    if bt.startswith("H1_") or bt.startswith("H1 "):
        return Scope.H1, bt, Variant.MAIN
    return Scope.FULL, bt, Variant.MAIN


def backfill_soccer(*, leagues: list[str] | None = None) -> dict:
    from ..soccer import LEAGUE_REGISTRY
    from ..soccer._db import get_conn
    if leagues is None:
        leagues = list(LEAGUE_REGISTRY)
    per_league = {}
    unified = _unified_conn()
    for league in leagues:
        out = {"read": 0, "inserted": 0, "skipped": 0, "errors": 0}
        try:
            conn = get_conn(league)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT p.*, m.date AS match_date "
                "FROM picks p JOIN matches m ON m.id = p.match_id "
                "ORDER BY p.id ASC"
            ).fetchall()
        except Exception as e:
            logger.warning("soccer backfill %s: %s", league, e)
            per_league[league] = {"error": str(e)}
            continue
        out["read"] = len(rows)
        for r in rows:
            try:
                mid = r["match_id"]
                if not mid:
                    out["skipped"] += 1
                    continue
                scope, bt, variant = _classify(r["bet_type"])
                res = (Result(r["result"])
                       if r["result"] in ("W", "L", "P", "V") else None)
                pick = Pick(
                    sport="soccer", league=league,
                    game_key=str(GameKey.for_soccer(league, mid)),
                    pick_date=r["match_date"] or r["date"] or "",
                    matchup=r["matchup"] or "",
                    scope=scope, bet_type=bt, variant=variant,
                    pick_text=r["pick"] or "",
                    side=r["side"] if "side" in r.keys() else None,
                    line=r["line"] if "line" in r.keys() else None,
                    odds=int(r["odds"] or 0),
                    closing_odds=r["closing_odds"],
                    prob=float(r["model_prob"] or 0.0),
                    edge_pct=float(r["edge"] or 0.0),
                    stake_units=float(r["stake_units"] or 0.0)
                                 if "stake_units" in r.keys() else 0.0,
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
                logger.warning("soccer backfill %s id=%s: %s",
                                league, r["id"], e)
                out["errors"] += 1
        per_league[league] = out
    return per_league
