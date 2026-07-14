"""Golf backend routes.

v1: tour-scoped read endpoints that surface the next upcoming
tournament + per-player picks. Mirrors the motorsports route layout
so the frontend can reuse the outright-style panel pattern.
"""
from __future__ import annotations

import logging
import time

from fastapi import APIRouter

router = APIRouter()
logger = logging.getLogger(__name__)


# Debounce slate ingest to one fetch per N seconds — frontend will
# poll every 30-60s and we don't want to hammer ESPN.
_INGEST_TTL_S = 60
_INGEST_TS: dict[str, float] = {}


@router.get("/api/golf/{tour}/today")
def api_golf_today(tour: str) -> dict:
    """Next-tournament slate for ``tour``.

    Returns the upcoming or in-progress tournament with predictor
    probs + HR market odds + picks per player.

    Response shape::

        {
          "tour": "pga",
          "display_name": "PGA Tour",
          "tournament": {
            "id": "401811947",
            "name": "PGA Championship",
            "course": "Quail Hollow Club",
            "start_date": "2026-05-14T...",
            "end_date":   "2026-05-17T...",
            "status": "scheduled",
            "is_major": 1,
          },
          "players": [
            {"id", "name", "country", "headshot_url",
             "p_winner", "p_top_5", "p_top_10", "p_top_20",
             "p_made_cut", "odds": {"WINNER": ..., "TOP_5": ..., ...},
             "edges":     {"WINNER": ..., ...}},
            ...
          ],
          "picks_summary": {"winner": N, "top_5": N, ...},
        }
    """
    from engine.golf import TOUR_REGISTRY, get_tour_config
    from engine.golf._db import get_conn
    from engine.golf._predict import predict_field
    from engine.golf._odds import fetch_tournament_odds
    from engine.golf._picks import generate_picks
    from engine.golf._ingest import ingest_today
    from engine.golf._tracker import record_picks, settle_picks

    if tour not in TOUR_REGISTRY:
        return {"error": f"unknown tour {tour!r}"}
    cfg = get_tour_config(tour)

    # Debounced ingest — keep schedule + leaderboard fresh.
    now = time.monotonic()
    if (now - _INGEST_TS.get(tour, 0.0)) >= _INGEST_TTL_S:
        try:
            ingest_today(tour)
            _INGEST_TS[tour] = now
        except Exception as e:
            logger.warning("[golf:%s] ingest failed: %s", tour, e)

    conn = get_conn(tour)
    # Pick the next upcoming or live tournament. Order: in-progress
    # first, then scheduled within the next 14 days, finally falls
    # back to the most recent final for tracker context.
    upcoming = conn.execute(
        "SELECT * FROM tournaments "
        "WHERE status IN ('in', 'scheduled') "
        "  AND start_date <= date('now', '+14 days') "
        "ORDER BY status DESC, start_date ASC LIMIT 1"
    ).fetchone()
    if not upcoming:
        upcoming = conn.execute(
            "SELECT * FROM tournaments WHERE status = 'final' "
            "ORDER BY end_date DESC LIMIT 1"
        ).fetchone()
    if not upcoming:
        return {
            "tour": tour,
            "display_name": cfg.get("display_name") or tour,
            "tournament": None,
            "players": [],
            "picks_summary": {},
        }

    tournament_id = upcoming["id"]

    # Hydrate field_entries from HR's WINNER market for upcoming
    # tournaments. ESPN doesn't publish the field until ~24-48h before
    # tournament start; HR posts outright odds days earlier. Without
    # this fallback, an upcoming-scheduled tournament with HR odds
    # already live renders an empty field + no picks.
    field_count = conn.execute(
        "SELECT COUNT(*) FROM field_entries WHERE tournament_id = ?",
        (tournament_id,),
    ).fetchone()[0]
    if field_count == 0 and (upcoming["status"] or "").lower() == "scheduled":
        try:
            from engine.golf._ingest import ingest_field_from_hr
            ingest_field_from_hr(tour, tournament_id)
        except Exception as e:
            logger.debug("[golf:%s] HR field hydrate failed: %s", tour, e)

    pred = predict_field(tour, tournament_id)
    odds = fetch_tournament_odds(tour, tournament_id)
    picks = generate_picks(tour, tournament_id)
    picks_by_player_market: dict[tuple[int, str], dict] = {}
    for p in picks:
        picks_by_player_market[(p["player_id"], p["bet_type"])] = p

    # Build per-player rows. Sort by p_winner desc so the headline
    # leaderboard mirrors a betting-market view.
    field_rows = conn.execute(
        "SELECT p.id, p.name, p.country, p.flag_url, p.headshot_url, "
        "       f.final_position, f.score_to_par "
        "FROM field_entries f JOIN players p ON p.id = f.player_id "
        "WHERE f.tournament_id = ?",
        (tournament_id,),
    ).fetchall()

    players: list[dict] = []
    for r in field_rows:
        pid = int(r["id"])
        d = pred.get(pid) or {}
        row_odds: dict[str, int] = {}
        edges: dict[str, float] = {}
        for bet_type in ("WINNER", "TOP_5", "TOP_10", "TOP_20", "MAKE_CUT"):
            market = odds.get(bet_type) or {}
            if pid in market:
                row_odds[bet_type] = market[pid]
                if (pid, bet_type) in picks_by_player_market:
                    edges[bet_type] = picks_by_player_market[(pid, bet_type)]["edge"]
        players.append({
            "id": pid,
            "name": r["name"],
            "country": r["country"],
            "flag_url": r["flag_url"],
            "headshot_url": r["headshot_url"],
            "final_position": r["final_position"],
            "score_to_par": r["score_to_par"],
            "p_winner": d.get("p_winner"),
            "p_top_5": d.get("p_top_5"),
            "p_top_10": d.get("p_top_10"),
            "p_top_20": d.get("p_top_20"),
            "p_made_cut": d.get("p_made_cut"),
            "odds": row_odds,
            "edges": edges,
            "skill_n": d.get("skill_n"),
        })
    players.sort(key=lambda p: -(p.get("p_winner") or 0))

    # Piggyback record + settle on every slate hit — same pattern team
    # sports use so the tracker keeps up without a separate cron.
    try:
        record_picks(tour, tournament_id, picks)
        settle_picks(tour)
    except Exception as e:
        logger.warning("[golf:%s] record/settle piggyback failed: %s",
                       tour, e)

    picks_summary: dict[str, int] = {}
    for p in picks:
        picks_summary[p["bet_type"].lower()] = picks_summary.get(
            p["bet_type"].lower(), 0) + 1

    return {
        "tour": tour,
        "display_name": cfg.get("display_name") or tour,
        "tournament": {
            "id": upcoming["id"],
            "name": upcoming["name"],
            "short_name": upcoming["short_name"],
            "course": upcoming["course"],
            "start_date": upcoming["start_date"],
            "end_date": upcoming["end_date"],
            "status": upcoming["status"],
            "is_major": bool(upcoming["is_major"]),
            "hr_event_id": odds.get("_event_id"),
            "hr_event_name": odds.get("_event_name"),
        },
        "players": players,
        "picks": picks,
        "picks_summary": picks_summary,
    }


@router.get("/api/golf/tours")
def api_golf_tours() -> dict:
    """Sidebar discovery endpoint — every configured tour."""
    from engine.golf import TOUR_REGISTRY
    return {
        "tours": [
            {"key": k, "display_name": v.get("display_name") or k,
             "status": v.get("status"),
             "default_rounds": v.get("default_rounds")}
            for k, v in TOUR_REGISTRY.items()
        ],
    }


@router.get("/api/golf/counts")
def api_golf_counts() -> dict:
    """Sidebar-friendly count of live golf action.

    Returns the total open picks across every tour. Matches the other
    sports' "where's the action right now" model — sidebar badge shows
    a number when at least one tour has open recommendations, hidden
    when none.
    """
    from engine.golf import TOUR_REGISTRY
    from engine.golf._db import get_conn
    total_open = 0
    per_tour: dict[str, int] = {}
    for tour in TOUR_REGISTRY:
        try:
            conn = get_conn(tour)
            n = conn.execute(
                "SELECT COUNT(*) FROM picks WHERE result IS NULL"
            ).fetchone()[0]
            per_tour[tour] = int(n)
            total_open += int(n)
        except Exception as e:
            logger.debug("[golf:%s] counts probe failed: %s", tour, e)
            per_tour[tour] = 0
    return {"open_picks": total_open, "by_tour": per_tour}


@router.get("/api/golf/{tour}/tracker/history")
def api_golf_tracker_history(tour: str, limit: int = 200) -> dict:
    """Tracker view — pending + settled picks. Auto-settles any picks
    whose tournament finalized between requests so the user sees
    fresh results without clicking Settle."""
    from engine.golf import TOUR_REGISTRY
    from engine.golf._tracker import list_history, settle_picks
    if tour not in TOUR_REGISTRY:
        return {"error": f"unknown tour {tour!r}"}
    try:
        settle_picks(tour)
    except Exception as e:
        logger.debug("[golf:%s] settle on history hit failed: %s", tour, e)
    rows = list_history(tour, limit=limit)
    rows_out = []
    for r in rows:
        d = dict(r) if not isinstance(r, dict) else dict(r)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        rows_out.append(d)
    # Summary scans the FULL picks table (not the limit-200 slice).
    # Stake-weighted: each row contributes profit * stake_units; ROI
    # denom is sum(stake_units) for W/L. NULL → 1.0u legacy.
    from engine.golf._db import get_conn as _g_conn
    conn = _g_conn(tour)
    all_picks = conn.execute(
        "SELECT bet_type, result, profit, stake_units FROM picks"
    ).fetchall()
    wins = losses = pending = 0
    profit = 0.0
    stake_settled = 0.0
    by_type: dict[str, dict] = {}
    for r in all_picks:
        bt = r["bet_type"] or "?"
        b = by_type.setdefault(bt, {"total": 0, "wins": 0, "losses": 0,
                                     "pending": 0, "profit": 0.0,
                                     "stake_settled": 0.0})
        b["total"] += 1
        res = r["result"]
        stake_u = float(r["stake_units"] if r["stake_units"] is not None else 1.0)
        pr = float(r["profit"] or 0) * stake_u
        if res == "W":
            b["wins"] += 1; wins += 1
            b["profit"] += pr; profit += pr
            b["stake_settled"] += stake_u; stake_settled += stake_u
        elif res == "L":
            b["losses"] += 1; losses += 1
            b["profit"] += pr; profit += pr
            b["stake_settled"] += stake_u; stake_settled += stake_u
        else:
            b["pending"] += 1; pending += 1
    for bt, b in by_type.items():
        d = b["wins"] + b["losses"]
        st = b.pop("stake_settled", 0)
        b["win_pct"] = round(b["wins"] / d * 100, 1) if d else 0.0
        b["roi"] = round(b["profit"] / st, 1) if st else 0.0
        b["profit"] = round(b["profit"], 2)
    decided = wins + losses
    summary = {
        "overall": {
            "total": wins + losses,
            "wins": wins,
            "losses": losses,
            "pending": pending,
            "profit": round(profit, 2),
            "win_pct": round(wins / decided * 100, 1) if decided else 0.0,
            "roi": round(profit / stake_settled, 1) if stake_settled else 0.0,
        },
        "by_type": by_type,
    }
    return {"tour": tour, "rows": rows_out, "summary": summary}


@router.post("/api/golf/{tour}/tracker/settle")
def api_golf_tracker_settle(tour: str) -> dict:
    from engine.golf import TOUR_REGISTRY
    from engine.golf._tracker import settle_picks
    if tour not in TOUR_REGISTRY:
        return {"error": f"unknown tour {tour!r}"}
    return settle_picks(tour)


@router.post("/api/golf/{tour}/tracker/record")
def api_golf_tracker_record(tour: str) -> dict:
    """Force-regenerate picks for the active tournament and persist
    them. Mirrors the NBA/NHL ``/tracker/record`` shape so the shared
    PickHistory primitive's "Record today" button works without a fork."""
    from engine.golf import TOUR_REGISTRY
    from engine.golf._picks import generate_picks
    from engine.golf._tracker import record_picks
    from engine.golf._db import get_conn
    if tour not in TOUR_REGISTRY:
        return {"error": f"unknown tour {tour!r}"}
    conn = get_conn(tour)
    upcoming = conn.execute(
        "SELECT id FROM tournaments "
        "WHERE status IN ('scheduled', 'in') "
        "ORDER BY start_date ASC LIMIT 1"
    ).fetchone()
    if not upcoming:
        return {"recorded": 0, "duplicate": 0, "errors": 0,
                "note": "no active tournament"}
    picks = generate_picks(tour, upcoming["id"])
    return record_picks(tour, upcoming["id"], picks)
