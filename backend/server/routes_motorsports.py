"""Motorsports API routes.

Endpoints (mounted under /api/motorsports):

  GET /api/motorsports/series              List supported series + status
  GET /api/motorsports/{series}/today      Slate for the next race —
                                           drivers, model probs, market
                                           odds, picks. Falls forward
                                           to the next scheduled race
                                           (F1 races every 1-2 weeks).
  GET /api/motorsports/{series}/tracker    Picks history + W/L summary

Falls back to {"error": ...} on unknown series so the frontend can
detect mis-routed requests early.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime

from fastapi import APIRouter

from ._tz import et_today_str

logger = logging.getLogger(__name__)
router = APIRouter()


# Per-(series, ts) ingest debounce — first hit pays the Ergast latency,
# subsequent hits inside the window serve fresh-enough state. 30min
# is generous because F1 calendar updates rarely (only on schedule
# changes which are a once-a-week kind of event).
_INGEST_TTL_S = 1800
_INGEST_TS: dict[str, float] = {}


@router.get("/api/motorsports/series")
def api_motorsports_series() -> dict:
    """Surface the supported series so the sidebar can build its menu
    without hardcoding series keys client-side. Mirrors
    /api/soccer/leagues + /api/basketball/leagues in shape so the
    Sidebar's MotorsportsGroup can render the same chrome as the
    other expandable-group entries."""
    from datetime import datetime
    from engine.motorsports import SERIES_REGISTRY
    today = datetime.utcnow()
    out = []
    for k, v in SERIES_REGISTRY.items():
        months = v.get("season_months") or ()
        in_season = today.month in months if months else True
        out.append({
            "key": k,
            "display_name": v.get("display_name") or k,
            "short_name": v.get("short_name") or k.upper(),
            "status": v.get("status"),
            "in_season": in_season,
            "data_source": v.get("data_source"),
        })
    return {"series": out}


@router.get("/api/motorsports/{series}/today")
def api_motorsports_today(series: str) -> dict:
    """Next-race slate. Returns the upcoming scheduled race (within 21
    days), with predictor probs + HR market odds + picks per driver.

    Response shape matches the team-sport panels' contract enough that
    the standard layout primitives (date pill, fell-forward banner,
    pick-history badge) can render without per-sport adapters:

        {
          "series": "f1",
          "display_name": "Formula 1",
          "date": "2026-05-24",        # next race date
          "real_today": "2026-05-10",  # for the fall-forward banner
          "fell_forward": True,
          "race": {
              "race_id": "2026-05",
              "name": "Canadian Grand Prix",
              "circuit": "Circuit Gilles Villeneuve",
              "country": "Canada",
              "race_time": "2026-05-24T20:00:00Z",
          },
          "drivers": [
              {"abbrev": "ANT", "name": "...", "team": "Mercedes",
               "grid_pos": 1, "p_win": 0.36, "p_podium": 0.80,
               "winner_odds": 200, "podium_odds": -450,
               "winner_edge": 0.34, "podium_edge": 0.18,
               "is_winner_pick": True, "is_podium_pick": True},
              ...
          ],
          "picks_summary": {"winner": 1, "podium": 3},
        }
    """
    from engine.motorsports import SERIES_REGISTRY, get_series_config
    from engine.motorsports._db import get_conn
    from engine.motorsports._predict import predict_race
    from engine.motorsports._odds import fetch_series_odds
    from engine.motorsports._picks import generate_picks, _devig_field

    if series not in SERIES_REGISTRY:
        return {"error": f"unknown series {series!r}"}
    cfg = get_series_config(series)

    # Short-circuit for series whose ingest pipeline isn't wired yet
    # (IndyCar / NASCAR). The frontend renders a "data pipeline
    # pending" placeholder; the API returns a structured stub so any
    # cross-cutting consumers (POTD, tracker scans) treat it as
    # "no race" rather than erroring on a missing predictor.
    if (cfg.get("status") == "pending_data"
            or cfg.get("data_source") == "pending"):
        real_today = et_today_str()
        return {
            "series": series,
            "display_name": cfg.get("display_name") or series,
            "status": cfg.get("status"),
            "date": real_today,
            "real_today": real_today,
            "race": None,
            "drivers": [],
            "picks_summary": {"winner": 0, "podium": 0},
            "pending_data": True,
        }

    # Ingest refresh (debounced) — keeps the calendar + recent results
    # current so the predictor sees the latest race outcomes. Route by
    # data_source: F1 uses Ergast, NASCAR+IndyCar use ESPN's core.api
    # (see engine.motorsports._espn_core_ingest). Series without a
    # wired ingest short-circuit above at the pending_data check.
    now_ts = time.monotonic()
    if (now_ts - _INGEST_TS.get(series, 0.0)) >= _INGEST_TTL_S:
        try:
            ds = cfg.get("data_source")
            season = datetime.utcnow().year
            if ds == "ergast":
                from engine.motorsports._ergast_ingest import (
                    ingest_calendar, ingest_results,
                )
                from engine.motorsports._wiki_circuits import ingest_track_images
                ingest_calendar(series, season)
                ingest_results(series, season)
                ingest_track_images(series)
            elif ds == "espn_core":
                from engine.motorsports._espn_core_ingest import (
                    ingest_today as _mc_today,
                )
                _mc_today(series)
            _INGEST_TS[series] = now_ts
        except Exception as e:
            logger.warning("[%s] ingest refresh failed: %s", series, e)

    real_today = et_today_str()
    conn = get_conn(series)
    # Find the next race within 21 days. F1 races every 1-2 weeks so
    # 21d covers any plausible "next slate" question.
    row = conn.execute("""
        SELECT race_id, season, round, name, circuit, country,
               race_date, race_time, status, ergast_id,
               circuit_wiki_url, circuit_image_url
        FROM races
        WHERE race_date >= ? AND status = 'scheduled'
              AND race_date <= date(?, '+21 days')
        ORDER BY race_date LIMIT 1
    """, (real_today, real_today)).fetchone()

    if not row:
        return {
            "series": series,
            "display_name": cfg.get("display_name") or series,
            "date": real_today,
            "real_today": real_today,
            "fell_forward": False,
            "race": None,
            "drivers": [],
            "picks_summary": {"winner": 0, "podium": 0},
        }

    race = dict(row)
    race_id = race["race_id"]
    fell_forward = race["race_date"] != real_today

    # Curated circuit metadata (length / laps / lap record). Empty
    # dict if we don't have the circuit on the calendar yet — frontend
    # handles missing fields gracefully.
    from engine.motorsports._circuits import circuit_info as _circuit_info
    race["info"] = _circuit_info(race.get("ergast_id"))

    # Last-3 winners at this circuit — useful track-history strip in
    # the race header. Two-step match:
    #   1. Exact circuit or name match (works for F1's Silverstone-year
    #      after-year naming + most IndyCar / NASCAR)
    #   2. Location-substring fallback: extract "Chicago" from
    #      "NASCAR Cup Series at Chicago", search prior races whose
    #      circuit OR name contains that token. Catches NASCAR's
    #      "at Chicago" vs "at Chicago (Street)" year-to-year drift
    #      + F1's occasional name reflow.
    circuit = race.get("circuit") or ""
    name = race.get("name") or ""
    last_winners_rows = conn.execute("""
        SELECT r.season, r.race_date, d.abbreviation AS abbrev,
               d.name, t.name AS team
        FROM races r
        JOIN race_results rr ON rr.race_id = r.race_id
                               AND rr.finish_pos = 1
        JOIN drivers d ON d.id = rr.driver_id
        LEFT JOIN teams t ON t.id = rr.team_id
        WHERE r.status = 'complete' AND r.race_date < ?
              AND ((? != '' AND r.circuit = ?)
                   OR (? != '' AND r.name = ?))
        ORDER BY r.race_date DESC LIMIT 3
    """, (real_today, circuit, circuit, name, name)).fetchall()
    if not last_winners_rows and (circuit or name):
        # Fallback: pull the last significant token from the race name.
        # NASCAR ships "NASCAR Cup Series at Chicago" → "Chicago";
        # IndyCar "Grand Prix of Mid-Ohio" → "Mid-Ohio"; F1 doesn't hit
        # this path (exact match wins). Skip 1-4 char tokens (series
        # prefixes, prepositions) so we don't LIKE-match on "at" or
        # "Cup".
        source = name or circuit
        candidates = [
            t.strip("()")
            for t in source.split()
            if len(t.strip("()")) > 4
        ]
        loc = candidates[-1] if candidates else ""
        if loc:
            like = f"%{loc}%"
            last_winners_rows = conn.execute("""
                SELECT r.season, r.race_date, d.abbreviation AS abbrev,
                       d.name, t.name AS team
                FROM races r
                JOIN race_results rr ON rr.race_id = r.race_id
                                       AND rr.finish_pos = 1
                JOIN drivers d ON d.id = rr.driver_id
                LEFT JOIN teams t ON t.id = rr.team_id
                WHERE r.status = 'complete' AND r.race_date < ?
                      AND (r.circuit LIKE ? OR r.name LIKE ?)
                ORDER BY r.race_date DESC LIMIT 3
            """, (real_today, like, like)).fetchall()
    race["last_winners"] = [dict(w) for w in last_winners_rows]

    # Predictor + odds. Odds may legitimately be empty if HR hasn't
    # priced this race yet (typically 2-3 weeks before race day) — the
    # panel still shows the predictor grid in that case.
    pred = predict_race(series, race_id)
    try:
        odds_all = fetch_series_odds(series)
    except Exception as e:
        logger.warning("[%s] HR odds fetch failed: %s", series, e)
        odds_all = {}
    race_odds = odds_all.get(race_id) or {}
    win_odds = race_odds.get("winner") or {}
    pod_odds = race_odds.get("podium") or {}

    # De-vigged implied probabilities for the per-driver edge column
    win_implied = _devig_field(win_odds, market_norm=1.0)
    pod_implied = _devig_field(pod_odds, market_norm=3.0)

    # Picks (idempotent — re-running for same race upserts existing rows)
    try:
        picks = generate_picks(series, race_id, pred, race_odds,
                                write_to_db=True)
    except Exception as e:
        logger.warning("[%s] picks generation failed: %s", series, e)
        picks = []
    pick_set = {(p["bet_type"], p["driver_id"]) for p in picks}

    drivers_out = []
    for d in pred.get("drivers", []):
        did = d["driver_id"]
        wo = (win_odds.get(did) or {}).get("odds")
        po = (pod_odds.get(did) or {}).get("odds")
        wi = win_implied.get(did, 0.0)
        pi = pod_implied.get(did, 0.0)
        we = (d["p_win"] - wi) / wi if wi > 0 else None
        pe = (d["p_podium"] - pi) / pi if pi > 0 else None
        drivers_out.append({
            "driver_id": did,
            "abbrev": d["abbrev"],
            "name": d["name"],
            "team": d["team"],
            "nationality": d.get("nationality") or "",
            "grid_pos": d["grid_pos"],
            "p_win": d["p_win"],
            "p_podium": d["p_podium"],
            "winner_odds": wo,
            "podium_odds": po,
            "winner_edge": we,
            "podium_edge": pe,
            "is_winner_pick": ("WINNER", did) in pick_set,
            "is_podium_pick": ("PODIUM", did) in pick_set,
        })

    summary = {
        "winner": sum(1 for p in picks if p["bet_type"] == "WINNER"),
        "podium": sum(1 for p in picks if p["bet_type"] == "PODIUM"),
    }

    # In-season check uses config-defined month range; matches the
    # basketball/hockey panels' off-season badge so the F1 header
    # follows the same pattern.
    from datetime import datetime as _dt
    month_now = _dt.utcnow().month
    in_season = month_now in (cfg.get("season_months") or ())

    # Days-until-race for the sidebar countdown badge — analog of the
    # team-sports "N games today" count. F1 races weekly so an absolute
    # day count is more useful than a 0/1 binary.
    try:
        race_dt = _dt.strptime(race["race_date"], "%Y-%m-%d")
        today_dt = _dt.strptime(real_today, "%Y-%m-%d")
        days_until = max(0, (race_dt - today_dt).days)
    except (ValueError, TypeError):
        days_until = None

    # Odds availability signal — surfaces the fact that HR routinely
    # posts NASCAR/IndyCar race markets 24-48h before green flag while
    # F1 keeps them up all week. Without this the panel's "0 picks"
    # state was indistinguishable from a broken pipeline: the user's
    # 2026-07-03 report "Indy and Nascar missing odds" was HR having
    # no race odds up yet, not a data-flow bug.
    n_priced = sum(1 for d in drivers_out
                    if d.get("winner_odds") is not None)
    odds_available = n_priced > 0
    return {
        "series": series,
        "display_name": cfg.get("display_name") or series,
        "region": _series_region(cfg),
        "status": cfg.get("status"),
        "in_season": in_season,
        "days_until_race": days_until,
        "date": race["race_date"],
        "real_today": real_today,
        "fell_forward": fell_forward,
        "race": race,
        "drivers": drivers_out,
        "picks_summary": summary,
        "odds_available": odds_available,
        "n_drivers_priced": n_priced,
    }


def _series_region(cfg: dict) -> str:
    """Derive a region label from the series config so the panel header
    can show 'International' / 'USA' next to the title — mirrors the
    basketball panel's region pill. Hard-coded for v1 since we only have
    F1; expand once IndyCar / NASCAR / Formula E are added."""
    return "International"


@router.get("/api/motorsports/{series}/calibration")
def api_motorsports_calibration(series: str) -> dict:
    """Calibration diagnostics: Brier scores, per-bucket empirical hit
    rates, and the fitted map currently being applied to live picks.
    Drives the Calibration tab — no picks history needed for this view
    since we backtest the predictor directly against historical results.
    """
    from engine.motorsports import SERIES_REGISTRY
    from engine.motorsports import _calibration

    if series not in SERIES_REGISTRY:
        return {"error": f"unknown series {series!r}"}

    diag = _calibration.diagnostics(series)
    return {
        "series": series,
        "n": diag.get("n", 0),
        "brier_win": diag.get("brier_win"),
        "brier_podium": diag.get("brier_podium"),
        "calibration": diag.get("calibration") or {},
    }


@router.get("/api/motorsports/{series}/standings")
def api_motorsports_standings(series: str, season: int | None = None) -> dict:
    """Drivers championship + constructors championship for the given
    season (defaults to current). Computed from race_results.points
    rather than hitting Ergast's standings endpoint — same numbers, one
    less network hop, and stays fresh after each results ingest.

    Response::

        {
          "series": "f1",
          "season": 2026,
          "drivers": [
              {"rank": 1, "abbrev": "ANT", "name": "...",
               "team": "Mercedes", "nationality": "Italian",
               "points": 93, "wins": 3, "podiums": 4, "races": 4},
              ...
          ],
          "constructors": [
              {"rank": 1, "name": "Mercedes",
               "nationality": "German", "points": 160,
               "wins": 3, "podiums": 5},
              ...
          ],
        }
    """
    from engine.motorsports import SERIES_REGISTRY
    from engine.motorsports._db import get_conn

    if series not in SERIES_REGISTRY:
        return {"error": f"unknown series {series!r}"}

    if season is None:
        season = datetime.utcnow().year

    conn = get_conn(series)

    drivers_rows = conn.execute("""
        SELECT d.id AS driver_id, d.abbreviation, d.name, d.nationality,
               t.name AS team,
               SUM(rr.points) AS points,
               SUM(CASE WHEN rr.finish_pos = 1 THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN rr.finish_pos <= 3 AND rr.finish_pos IS NOT NULL
                        THEN 1 ELSE 0 END) AS podiums,
               COUNT(rr.race_id) AS races
        FROM race_results rr
        JOIN drivers d ON d.id = rr.driver_id
        LEFT JOIN teams t ON t.id = rr.team_id
        JOIN races r ON r.race_id = rr.race_id
        WHERE r.season = ? AND r.status = 'complete'
        GROUP BY d.id
        ORDER BY points DESC, wins DESC, podiums DESC
    """, (season,)).fetchall()

    drivers = []
    for i, r in enumerate(drivers_rows, start=1):
        d = dict(r)
        drivers.append({
            "rank": i,
            "driver_id": d["driver_id"],
            "abbrev": d["abbreviation"],
            "name": d["name"],
            "team": d["team"] or "",
            "nationality": d["nationality"] or "",
            "points": float(d["points"] or 0),
            "wins": int(d["wins"] or 0),
            "podiums": int(d["podiums"] or 0),
            "races": int(d["races"] or 0),
        })

    constructors_rows = conn.execute("""
        SELECT t.id AS team_id, t.name, t.nationality,
               SUM(rr.points) AS points,
               SUM(CASE WHEN rr.finish_pos = 1 THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN rr.finish_pos <= 3 AND rr.finish_pos IS NOT NULL
                        THEN 1 ELSE 0 END) AS podiums
        FROM race_results rr
        JOIN teams t ON t.id = rr.team_id
        JOIN races r ON r.race_id = rr.race_id
        WHERE r.season = ? AND r.status = 'complete'
        GROUP BY t.id
        ORDER BY points DESC, wins DESC
    """, (season,)).fetchall()

    constructors = []
    for i, r in enumerate(constructors_rows, start=1):
        c = dict(r)
        constructors.append({
            "rank": i,
            "team_id": c["team_id"],
            "name": c["name"],
            "nationality": c["nationality"] or "",
            "points": float(c["points"] or 0),
            "wins": int(c["wins"] or 0),
            "podiums": int(c["podiums"] or 0),
        })

    return {
        "series": series,
        "season": season,
        "drivers": drivers,
        "constructors": constructors,
    }


@router.get("/api/motorsports/{series}/tracker")
def api_motorsports_tracker(series: str, limit: int = 200) -> dict:
    """Picks history + W/L summary for the given series. Settles any
    finished races whose picks are still pending before returning."""
    from engine.motorsports import SERIES_REGISTRY
    from engine.motorsports._db import get_conn
    from engine.motorsports._picks import settle_race

    if series not in SERIES_REGISTRY:
        return {"error": f"unknown series {series!r}"}

    conn = get_conn(series)
    # Settle any complete-races-with-pending-picks before the query so
    # the response reflects the latest results.
    pending_races = conn.execute("""
        SELECT DISTINCT p.race_id FROM picks p
        JOIN races r ON r.race_id = p.race_id
        WHERE p.result IS NULL AND r.status = 'complete'
    """).fetchall()
    for pr in pending_races:
        try:
            settle_race(series, pr["race_id"])
        except Exception as e:
            logger.warning("settle %s/%s: %s", series, pr["race_id"], e)

    rows = conn.execute("""
        SELECT p.id, p.date, p.race_id, p.driver_id, d.abbreviation AS pick,
               d.name AS driver_name, p.bet_type, p.model_prob, p.edge,
               p.odds, p.result, p.profit, p.stake_units, p.closing_odds,
               p.settled_at, r.name AS race_name
        FROM picks p
        LEFT JOIN drivers d ON d.id = p.driver_id
        LEFT JOIN races r ON r.race_id = p.race_id
        ORDER BY p.date DESC, p.id DESC LIMIT ?
    """, (limit,)).fetchall()

    # Reshape rows into the PicksTable contract used by every other
    # tracker (fields: matchup / pick / bet_type / odds / model_prob /
    # edge as percent points / result / profit). Earlier the rows came
    # back in a custom shape and the frontend had to render its own
    # table; now PickHistory's PicksTable can drop in unchanged.
    rows_out = []
    for r in rows:
        d = dict(r)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        row_profit = d["profit"]
        if row_profit is not None:
            row_profit = round(row_profit * stake_u, 2)
        rows_out.append({
            "id": d["id"],
            "date": d["date"],
            "matchup": d["race_name"],
            "bet_type": d["bet_type"],
            "pick": (d["pick"] or "?") + (f" ({d['driver_name']})" if d['driver_name'] else ""),
            "model_prob": d["model_prob"],
            # PicksTable expects edge in percent POINTS (10.5 = 10.5%),
            # not the 0..1 fraction we store internally. Convert here so
            # the table cell stays sensible (10.5% rather than 0.105%).
            "edge": (d["edge"] * 100.0) if d["edge"] is not None else None,
            "odds": d["odds"],
            "closing_odds": d["closing_odds"],
            "result": d["result"],
            "profit": row_profit,
            "stake_units": d.get("stake_units"),
            "settled_at": d["settled_at"],
        })

    # Summary scans every pick on file (not the limit-200 slice).
    # Stake-weighted: each row contributes profit*stake_units; ROI
    # denom is sum(stake_units) for W/L. NULL → 1.0u legacy.
    all_picks = conn.execute(
        "SELECT bet_type, result, profit, stake_units FROM picks"
    ).fetchall()
    all_settled = [r for r in all_picks if r["result"] in ("W", "L", "P")]
    wins = sum(1 for r in all_settled if r["result"] == "W")
    pending = sum(1 for r in all_picks if r["result"] is None)
    profit = 0.0
    stake_settled = 0.0
    for r in all_settled:
        stake_u = float(r["stake_units"] if r["stake_units"] is not None else 1.0)
        profit += float(r["profit"] or 0) * stake_u
        if r["result"] in ("W", "L"):
            stake_settled += stake_u

    by_type: dict[str, dict] = {}
    for r in all_settled:
        bt = r["bet_type"] or "?"
        b = by_type.setdefault(bt, {"n": 0, "w": 0, "profit": 0.0,
                                     "stake_settled": 0.0})
        b["n"] += 1
        stake_u = float(r["stake_units"] if r["stake_units"] is not None else 1.0)
        b["profit"] += (r["profit"] or 0) * stake_u
        if r["result"] == "W":
            b["w"] += 1
        if r["result"] in ("W", "L"):
            b["stake_settled"] += stake_u
    for bt, b in by_type.items():
        st = b.pop("stake_settled", 0)
        b["win_rate"] = b["w"] / b["n"] if b["n"] else None
        b["roi"] = (b["profit"] / st) if st else None

    summary = {
        "total": len(all_picks),
        "settled": len(all_settled),
        "pending": pending,
        "wins": wins,
        "losses": len(all_settled) - wins,
        "win_rate": (wins / len(all_settled)) if all_settled else None,
        "profit": profit,
        "roi": (profit / stake_settled) if stake_settled else None,
        "overall": {
            "total": len(all_picks),
            "settled": len(all_settled),
            "win_rate": (wins / len(all_settled)) if all_settled else None,
            "profit": profit,
            "roi": (profit / stake_settled) if stake_settled else None,
        },
        "by_type": by_type,
    }

    return {
        "series": series,
        "rows": rows_out,
        "summary": summary,
    }
