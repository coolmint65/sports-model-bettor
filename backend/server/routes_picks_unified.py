"""Unified picks read API.

Single endpoint family for the entire portfolio. Replaces the 13+ per-
sport tracker endpoints with one consistent shape. Frontend trackers,
dashboards, and the cross-sport ROI view all consume this.

  GET /api/picks                — list filtered picks
  GET /api/picks/summary        — stake-weighted overall + by_type + by_sport
  GET /api/picks/sports         — sports + per-sport counts
  GET /api/picks/{id}           — single pick by id
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query


router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/api/picks")
def api_picks_list(
    sport: Optional[str] = None,
    league: Optional[str] = None,
    result: Optional[str] = Query(None, pattern=r"^(pending|W|L|P|V)$"),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    include_shadows: bool = True,
    limit: int = Query(200, ge=1, le=2000),
) -> dict:
    """Filtered list of picks, newest-first by creation time."""
    from engine.picks_unified import list_picks
    from engine.picks_unified._legacy_bridge import ensure_adapters_registered
    ensure_adapters_registered()
    rows = list_picks(
        sport=sport, league=league, result=result,
        date_from=date_from, date_to=date_to,
        include_shadows=include_shadows,
        limit=limit,
    )
    # Pick → dict for JSON. Project stake-weighted profit per row so
    # display matches the summary endpoint's totals.
    out = []
    for p in rows:
        d = {
            "id": p.id,
            "sport": p.sport,
            "league": p.league,
            "game_key": p.game_key,
            "pick_date": p.pick_date,
            "matchup": p.matchup,
            "scope": p.scope.value if p.scope else None,
            "bet_type": p.bet_type,
            "variant": p.variant.value if p.variant else None,
            "pick_text": p.pick_text,
            "side": p.side,
            "line": p.line,
            "odds": p.odds,
            "closing_odds": p.closing_odds,
            "prob": p.prob,
            "edge_pct": p.edge_pct,
            "stake_units": p.stake_units,
            "confidence": p.confidence,
            "is_shadow": p.is_shadow,
            "result": p.result.value if p.result else None,
            "profit": (round(p.profit * p.stake_units, 2)
                        if p.profit is not None else None),
            "created_at": p.created_at,
            "settled_at": p.settled_at,
        }
        out.append(d)
    return {"count": len(out), "picks": out}


@router.get("/api/picks/summary")
def api_picks_summary(
    sport: Optional[str] = None,
    league: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> dict:
    """Stake-weighted overall + by_type + recent. Add a by_sport
    breakdown so the cross-sport dashboard can render without 13
    separate requests."""
    from engine.picks_unified import get_summary
    from engine.picks_unified._legacy_bridge import ensure_adapters_registered
    from engine.picks_unified._schema import get_conn
    ensure_adapters_registered()
    base = get_summary(sport=sport, league=league,
                       date_from=date_from, date_to=date_to)

    # by_sport breakdown — cross-sport leaderboard.
    conn = get_conn()
    clauses = []
    params = []
    if league:
        clauses.append("league=?")
        params.append(league)
    if date_from:
        clauses.append("pick_date >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("pick_date <= ?")
        params.append(date_to)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sport_rows = conn.execute(
        f"""SELECT sport,
              COUNT(*) AS total,
              SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) AS wins,
              SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) AS losses,
              SUM(CASE WHEN result='P' THEN 1 ELSE 0 END) AS pushes,
              SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) AS pending,
              COALESCE(SUM(profit * stake_units), 0) AS profit,
              COALESCE(SUM(CASE WHEN result IN ('W','L')
                                  THEN stake_units ELSE 0 END), 0) AS stake_settled
            FROM picks {where}
            GROUP BY sport ORDER BY total DESC""",
        params,
    ).fetchall()
    by_sport = {}
    for r in sport_rows:
        w = r["wins"] or 0; l = r["losses"] or 0
        st = r["stake_settled"] or 0
        by_sport[r["sport"]] = {
            "total": r["total"] or 0,
            "wins": w, "losses": l,
            "pushes": r["pushes"] or 0,
            "pending": r["pending"] or 0,
            "profit": round(r["profit"] or 0, 2),
            "win_pct": round(w / (w + l) * 100, 1) if (w + l) else 0.0,
            "roi": round((r["profit"] or 0) / st, 1) if st else 0.0,
        }

    base["by_sport"] = by_sport
    return base


@router.get("/api/picks/sports")
def api_picks_sports() -> dict:
    """Sports + leagues present in the unified picks table, with
    counts. Frontend nav uses this to decide which sport tabs to show."""
    from engine.picks_unified._schema import get_conn
    from engine.picks_unified._legacy_bridge import ensure_adapters_registered
    ensure_adapters_registered()
    conn = get_conn()
    rows = conn.execute(
        "SELECT sport, league, COUNT(*) AS n, "
        "       SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) AS pending "
        "FROM picks GROUP BY sport, league ORDER BY sport, league"
    ).fetchall()
    out: dict[str, list] = {}
    for r in rows:
        out.setdefault(r["sport"], []).append({
            "league": r["league"], "count": r["n"], "pending": r["pending"],
        })
    return {"sports": out}


@router.get("/api/picks/{pick_id}")
def api_picks_get(pick_id: int) -> dict:
    from engine.picks_unified import get_pick_by_id
    from engine.picks_unified._legacy_bridge import ensure_adapters_registered
    ensure_adapters_registered()
    p = get_pick_by_id(pick_id)
    if not p:
        raise HTTPException(status_code=404, detail="pick not found")
    return {
        "id": p.id, "sport": p.sport, "league": p.league,
        "game_key": p.game_key, "pick_date": p.pick_date,
        "matchup": p.matchup,
        "scope": p.scope.value if p.scope else None,
        "bet_type": p.bet_type,
        "variant": p.variant.value if p.variant else None,
        "pick_text": p.pick_text, "side": p.side, "line": p.line,
        "odds": p.odds, "closing_odds": p.closing_odds,
        "prob": p.prob, "prob_raw": p.prob_raw,
        "edge_pct": p.edge_pct, "stake_units": p.stake_units,
        "confidence": p.confidence, "is_shadow": p.is_shadow,
        "result": p.result.value if p.result else None,
        "profit_per_unit": p.profit,
        "profit_realized": (round(p.profit * p.stake_units, 2)
                              if p.profit is not None else None),
        "created_at": p.created_at, "settled_at": p.settled_at,
    }


@router.post("/api/picks/settle")
def api_picks_settle(sport: Optional[str] = None) -> dict:
    """Trigger the unified settler. Optional sport scoping. Same shape
    as the legacy per-sport /tracker/settle endpoints."""
    from engine.picks_unified import settle_pending
    from engine.picks_unified._legacy_bridge import ensure_adapters_registered
    ensure_adapters_registered()
    return settle_pending(sport=sport)
