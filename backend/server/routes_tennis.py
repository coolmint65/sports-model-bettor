"""Tennis API routes.

Extracted 2026-05-02 from backend/server/__init__.py as the first sport-router
split. Pattern to follow for MLB / NHL / NBA / props / POTD extractions:
each gets its own routes_{sport}.py with its own APIRouter, mounted via
``app.include_router(...)`` in ``__init__.py``.

Module-level helpers stay in __init__.py for now; routes import engine
helpers per-function (already the existing convention).
"""

from __future__ import annotations

import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Body

from ._tz import et_today_str, et_now

logger = logging.getLogger(__name__)

router = APIRouter()


# Process-local response cache for /api/tennis/scheduled. HR's GraphQL
# returns 429 if hit per request; this caches the assembled tour
# response so rapid tour-toggle clicks don't re-run predict + picks
# for every match. Lives here (not __init__.py) so the extracted
# router is self-contained.
_TENNIS_RESPONSE_CACHE: dict = {}
# 5-min TTL — the per-request build takes 20-30s (predict 100 matches +
# 800 player joins + picks). 60s was way too tight; users toggling
# ATP↔WTA inside 90s hit the rebuild. Predict state is deterministic
# (no real-time odds in the assembly itself) so a few-minute stale is
# safe. Performance audit win: ~20-30s saved per tour-toggle click.
_TENNIS_RESPONSE_TTL_S = 300.0


@router.get("/api/tennis/scheduled")
def api_tennis_scheduled(date: str | None = Query(None),
                          days: int = Query(2, ge=1, le=7),
                          tour: str | None = Query(None),
                          refresh: bool = Query(False),
                          include_unbookable: bool = Query(False)):
    """Tennis slate — ESPN-fetched matches with predictions, HR odds,
    and per-match picks pre-computed. Mirrors the response shape of
    `/api/{sport}/best-bets` so the frontend can render the bet card
    identically to team sports.

    Defaults to a 2-day window (today + tomorrow). Each match dict
    carries:
        prediction:  {p1_win_prob, p2_win_prob, p1_rating, p2_rating, ...}
        odds:        flat dict of HR markets (when found by name match)
        picks:       full pick list (every market with edge clearing
                     the floor), sorted by conviction desc
        best_pick:   highest-conviction pick from picks[] — used as
                     the EdgeBadge headline on the card
        confidence:  best_pick.confidence ('strong' / 'moderate' / 'lean')
                     or 'skip' when picks is empty
    """
    import time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import timedelta as _td
    from engine.tennis_db import get_conn, ensure_tables
    from engine.tennis_predict import predict_match
    from engine.tennis_picks import generate_tennis_picks
    from engine.tennis_odds import (build_lookup, find_match,
                                      align_to_caller)
    ensure_tables()
    base = (datetime.strptime(date, "%Y-%m-%d")
            if date else et_now().replace(tzinfo=None))
    start_date = base.strftime("%Y-%m-%d")
    end_date = (base + _td(days=int(days) - 1)).strftime("%Y-%m-%d")

    # Response-level cache. Tour switches inside the TTL window
    # reuse the prior response — was 26s end-to-end before and made
    # ATP↔WTA toggling unusable. Refresh requests bypass the cache.
    cache_key = (start_date, end_date, tour or "all",
                  bool(include_unbookable))
    if not refresh:
        cached_resp = _TENNIS_RESPONSE_CACHE.get(cache_key)
        if cached_resp and (_time.time() - cached_resp["ts"]
                              < _TENNIS_RESPONSE_TTL_S):
            return cached_resp["payload"]

    if refresh:
        try:
            from engine.tennis_schedule import ingest_schedule_window
            ingest_schedule_window(start_date, days=int(days))
        except Exception as e:
            logger.warning("tennis schedule refresh failed: %s", e)

    conn = get_conn()
    sql = ("SELECT * FROM tennis_scheduled_matches "
            "WHERE date >= ? AND date <= ? "
            "  AND p1_id IS NOT NULL AND p2_id IS NOT NULL")
    params: list = [start_date, end_date]
    if tour in ("atp", "wta"):
        sql += " AND tour = ?"
        params.append(tour)
    sql += " ORDER BY date, tournament, start_time"
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]

    # Stale-live-match auto-refresh. If any 'in'-status row's fetched_at
    # is older than 5 min, ESPN may have flipped it to 'post' since.
    # Without this, completed matches stayed showing as live for hours
    # (Sinner/Zverev was stuck at status='in' score='2-0' winner=None
    # 3+ hours after the match ended). One refresh per stale-live
    # window — bounded cost.
    if not refresh:
        try:
            from datetime import datetime as _dt
            now_utc = _dt.utcnow()
            for r in rows:
                if r.get("status") != "in":
                    continue
                fetched = r.get("fetched_at") or ""
                try:
                    ft = _dt.fromisoformat(fetched.replace("Z", ""))
                except (ValueError, TypeError):
                    continue
                age_s = (now_utc - ft).total_seconds()
                if age_s > 300:
                    from engine.tennis_schedule import ingest_schedule_window
                    ingest_schedule_window(start_date, days=int(days))
                    # Re-read after refresh.
                    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
                    break
        except Exception as e:
            logger.debug("tennis stale-live refresh skipped: %s", e)

    # Backfill missing player images. Two layers, both read-only at
    # serve time:
    #   1) cross-row: copy image/flag from any other row in the
    #      schedule that has them for the same Sackmann player_id
    #      (handles ESPN-derived players who appear in multiple rows)
    #   2) name cache: engine.tennis_player_photos resolves players
    #      via ESPN search and persists to disk; lookup_image() is
    #      a cache-only read here — backfill runs out of band via
    #      the CLI / nightly sync, never blocks the request
    img_map: dict[int, tuple[str, str]] = {}
    for cache_row in conn.execute(
            "SELECT p1_id, p1_image, p1_flag FROM tennis_scheduled_matches "
            "WHERE p1_image IS NOT NULL").fetchall():
        pid = cache_row["p1_id"]
        if pid is not None and pid not in img_map:
            img_map[int(pid)] = (cache_row["p1_image"], cache_row["p1_flag"])
    for cache_row in conn.execute(
            "SELECT p2_id, p2_image, p2_flag FROM tennis_scheduled_matches "
            "WHERE p2_image IS NOT NULL").fetchall():
        pid = cache_row["p2_id"]
        if pid is not None and pid not in img_map:
            img_map[int(pid)] = (cache_row["p2_image"], cache_row["p2_flag"])
    try:
        from engine.tennis_player_photos import lookup_image as _photo_cache
    except Exception:
        _photo_cache = None
    for r in rows:
        if not r.get("p1_image") and r.get("p1_id") in img_map:
            r["p1_image"], r["p1_flag"] = img_map[int(r["p1_id"])]
        if not r.get("p2_image") and r.get("p2_id") in img_map:
            r["p2_image"], r["p2_flag"] = img_map[int(r["p2_id"])]
        if _photo_cache:
            if not r.get("p1_image") and r.get("p1_name"):
                hit = _photo_cache(r["p1_name"])
                if hit:
                    r["p1_image"] = hit
            if not r.get("p2_image") and r.get("p2_name"):
                hit = _photo_cache(r["p2_name"])
                if hit:
                    r["p2_image"] = hit

    # HR cache + circuit breaker live in engine.tennis_odds now —
    # every caller (API, picker, schedule supplement) shares one
    # 15-min in-process cache + disk fallback + 30-min cooldown
    # on 429. Prevents repeated rate-limit hits.
    from engine.tennis_odds import fetch_all as _fetch_hr_odds
    hr_events = _fetch_hr_odds(force=refresh)
    hr_lookup = build_lookup(hr_events) if hr_events else {}

    # Tournament classification + priority shared with the picker so
    # both edge floors and slate ordering use one source of truth.
    from engine.tennis_schedule import (
        _level_code_for as _level_code,
        TOURNAMENT_PRIORITY,
    )

    def _build_one(r: dict) -> dict:
        """Per-match work — predict + odds-match + picks + h2h/form.
        Pulled into a helper so the slate processes in parallel; was
        serial-26s previously."""
        from engine.tennis_db import head_to_head, recent_form
        match = dict(r)
        try:
            pred = predict_match(
                r["tour"], int(r["p1_id"]), int(r["p2_id"]),
                surface=r.get("surface") or "Hard",
                best_of=int(r.get("best_of") or 3),
            )
            match["prediction"] = pred
        except Exception as e:
            logger.debug("tennis predict failed for %s: %s",
                         r.get("match_id"), e)
            match["prediction"] = None
            pred = None

        odds = {}
        if hr_lookup and r.get("p1_name") and r.get("p2_name"):
            hr_match = find_match(hr_lookup, r["p1_name"], r["p2_name"])
            if hr_match:
                odds = align_to_caller(hr_match, r["p1_name"], r["p2_name"])
        match["odds"] = odds

        # HR carries the bookmaker's live start time — that's what the
        # user sees on their bet slip, so it's the right thing to show
        # on the card too. ESPN + TE both ship placeholder times for
        # matches where the exact court time isn't published at ingest
        # (Wimbledon 2026-07-04: ESPN 04:00Z, TE 15:30 local UK). HR
        # has the actual "play begins at ~10 AM ET" time. Override the
        # DB row when HR ships a start_time.
        hr_start = (odds or {}).get("start_time") if odds else None
        if hr_start:
            match["start_time"] = hr_start

        level = _level_code(r.get("tournament"),
                             hr_comp=(odds or {}).get("comp"))
        picks: list[dict] = []
        if pred:
            pred_for_picks = dict(pred)
            pred_for_picks["p1_name"] = r["p1_name"]
            pred_for_picks["p2_name"] = r["p2_name"]
            try:
                picks = generate_tennis_picks(
                    pred_for_picks, odds,
                    min_edge_ml=4.0,
                    min_edge_other=6.0,
                    tournament_level=level,
                    include_lower_tiers=True,
                )
            except Exception as e:
                logger.debug("tennis picks gen failed for %s: %s",
                             r.get("match_id"), e)
        picks.sort(key=lambda p: -(p.get("conviction_score") or 0))
        match["picks"] = picks
        best = picks[0] if picks else None
        match["best_pick"] = best
        match["confidence"] = (best.get("confidence") if best else "skip")
        match["tournament_level"] = level
        match["tournament_priority"] = TOURNAMENT_PRIORITY.get(level, 30)

        try:
            sfc = r.get("surface")
            match["h2h"] = head_to_head(
                r["tour"], int(r["p1_id"]), int(r["p2_id"]), limit=10)
            match["p1_form"] = recent_form(
                r["tour"], int(r["p1_id"]), surface=sfc, limit=10)
            match["p2_form"] = recent_form(
                r["tour"], int(r["p2_id"]), surface=sfc, limit=10)
        except Exception as e:
            logger.debug("tennis H2H/form failed for %s: %s",
                         r.get("match_id"), e)
            match["h2h"] = None
            match["p1_form"] = None
            match["p2_form"] = None

        # Line movement vs morning capture. Returns None when no
        # opening row exists yet (sync hasn't run, or this match
        # appeared after the morning snapshot).
        try:
            from engine.tennis_line_movement import compute_movement
            match["line_movement"] = compute_movement(
                r["match_id"], odds, date=r.get("date"))
        except Exception as e:
            logger.debug("tennis line_movement failed for %s: %s",
                         r.get("match_id"), e)
            match["line_movement"] = None
        return match

    # Pre-filter matches that HR isn't booking BEFORE the heavy
    # _build_one fan-out — predict + h2h + form + line_movement on a
    # 380-match slate when only ~140 are bookable burned 30s+ of
    # work for cards we'd discard at the end. With this gate, the
    # slate route drops from a 60-120s wall to ~5-10s.
    if not include_unbookable and hr_lookup:
        from engine.tennis_odds import _normalize_name_key as _nk
        bookable = []
        for r in rows:
            n1 = _nk(r.get("p1_name") or "")
            n2 = _nk(r.get("p2_name") or "")
            if n1 and n2 and frozenset({n1, n2}) in hr_lookup:
                bookable.append(r)
        logger.info("tennis slate: pre-filtered %d/%d bookable",
                    len(bookable), len(rows))
        rows = bookable

    # Parallel fan-out across the slate. Per-match work is mostly
    # I/O-bound (SQLite reads for Elo / form / H2H) so threads
    # parallelize cleanly without blocking on the GIL. 8 workers
    # covers a 100+ match slate in ~3-5s (was 26s serial).
    out: list[dict] = []
    with ThreadPoolExecutor(max_workers=8,
                              thread_name_prefix="tennis-bb") as pool:
        futures = [pool.submit(_build_one, r) for r in rows]
        for fut in as_completed(futures):
            try:
                out.append(fut.result())
            except Exception as e:
                logger.warning("tennis _build_one failed: %s", e)

    # Drop matches HR doesn't book. ESPN ships full-draw schedules for
    # tournaments HR doesn't trade (Jiangxi / Istanbul Open) plus
    # qualifier rounds in main events — those rows have no actionable
    # markets, only a prediction. Showing them on a betting card is
    # noise. ``include_unbookable=true`` opts back in for diagnostic /
    # rankings views that want the full slate.
    total_before = len(out)
    if not include_unbookable:
        out = [m for m in out if (m.get("odds") or {}).get("markets")]
        if total_before and len(out) < total_before:
            logger.info("tennis slate: filtered %d/%d matches without HR odds",
                        total_before - len(out), total_before)

    # Stable sort: tournament priority desc, then best_pick edge desc.
    # Keeps the high-importance Slams/Masters at the top regardless
    # of edge, while still ranking matches inside each tournament by
    # the model's headline edge.
    out.sort(key=lambda m: (
        -m.get("tournament_priority", 30),
        -((m.get("best_pick") or {}).get("edge") or 0),
    ))

    # Pick events — track best_pick transitions per match so the
    # PickEventsBadge popover shows the breadcrumb of swaps. Mirrors
    # NBA/NHL/MLB. Tennis has only one headline (no Q1/Full split)
    # so we use the unscoped path with sport='tennis'.
    try:
        from engine.pick_events import detect_transitions
        ev = detect_transitions("tennis", [
            {"game_id": m.get("match_id"),
             "matchup": (m.get("p1_name") or "") + " vs " + (m.get("p2_name") or ""),
             "best_pick": m.get("best_pick")}
            for m in out if m.get("match_id")
        ], date=et_today_str())
        if any(ev.values()):
            logger.info("Pick events (tennis): %s", ev)
    except Exception as e:
        logger.debug("tennis pick-events failed: %s", e)

    # Record ONE pick per match into the tracker — the highest-edge
    # one (i.e. m.best_pick). Mirrors the team-sport convention where
    # only the headline pick lands in the tracker; alt markets stay
    # browseable on the slate but don't multiply tracker rows.
    # record_tennis_pick is idempotent on (tour, match_id, bet_type,
    # pick) while pending, so re-recording the same opportunity returns
    # the existing id.
    try:
        from engine.tennis_tracker import record_tennis_pick
        from engine.tennis_schedule import _level_code_for as _lc
        recorded = 0
        for m in out:
            if not m.get("match_id"):
                continue
            best = m.get("best_pick")
            if not best:
                continue
            try:
                record_tennis_pick({
                    "tour":             m.get("tour"),
                    "match_id":         m.get("match_id"),
                    "date":             m.get("date"),
                    "matchup":          f'{m.get("p1_name","")} vs {m.get("p2_name","")}',
                    "surface":          m.get("surface"),
                    "best_of":          int(m.get("best_of") or 3),
                    "tourney_level":    _lc(
                        m.get("tournament"),
                        hr_comp=(m.get("odds") or {}).get("comp"),
                    ),
                    "p1_id":            m.get("p1_id"),
                    "p2_id":            m.get("p2_id"),
                    "bet_type":         best.get("type"),
                    "pick":             best.get("pick"),
                    "odds":             int(best.get("odds") or 0),
                    "model_prob":       float(best.get("model_prob") or 0),
                    "edge":             float(best.get("edge") or 0),
                    "conviction_score": float(best.get("conviction_score") or 0),
                    # Kelly-derived stake from picks_core.score_pick;
                    # was silently being dropped on the way to the
                    # tracker (2026-07-03 report: every recorded row
                    # showed flat $100 P/L because stake_units landed
                    # as NULL and the read path defaulted to 1.0u).
                    "stake_units":      best.get("stake_units"),
                })
                recorded += 1
            except Exception as e:
                logger.debug("tennis pick record failed for %s/%s: %s",
                             m.get("match_id"), best.get("type"), e)
        if recorded:
            logger.info("Tennis picks recorded into tracker: %d (idempotent)",
                         recorded)
    except Exception as e:
        logger.debug("tennis tracker record path failed: %s", e)
    payload = {"date": start_date, "end_date": end_date,
                "days": int(days), "count": len(out), "matches": out}
    _TENNIS_RESPONSE_CACHE[cache_key] = {"payload": payload, "ts": _time.time()}
    return payload


@router.post("/api/tennis/scheduled/refresh")
def api_tennis_scheduled_refresh(date: str | None = Body(None,
                                                           embed=True)):
    """Force a fresh ESPN pull. Returns the ingest summary."""
    from engine.tennis_schedule import ingest_schedule
    return ingest_schedule(date)


@router.get("/api/tennis/tournaments")
def api_tennis_tournaments(
    tour: str | None = Query(None),
    days: int = Query(7, ge=1, le=21),
):
    """List active tournaments with counts so the panel can render a
    tournament index instead of one flat slate of every match.

    Mirrors the basketball / hockey framework's `/leagues` registry
    shape — each row is "openable" from the sidebar/panel and the
    detail view pulls the per-tournament slate via the existing
    /api/tennis/scheduled with a tournament filter.

    Window: today through ``days`` ahead (default 7) — short enough
    to keep ITF/Challenger churn manageable, long enough to surface
    next-week Slams while a current Slam is still on.

    Returns::

        {
          "tournaments": [
              {
                "tour": "atp",                # 'atp' | 'wta'
                "tournament": "Rome ATP",     # display name
                "tournament_id": "...",       # ESPN league id (stable)
                "surface": "Clay",
                "level": "M",                 # tier code: G/M/500/250/C/ITF
                "priority": 90,               # 0..100, higher = more important
                "first_date": "2026-05-10",
                "last_date":  "2026-05-15",
                "matches_total": 18,
                "matches_today": 6,
                "matches_pre": 12,
                "matches_live": 0,
                "edges": 3,                   # picks with strong/moderate/lean
              },
              ...
          ],
          "date": "2026-05-12"
        }
    """
    from datetime import datetime, timedelta
    from engine.tennis_db import get_conn, ensure_tables
    from engine.tennis_schedule import (
        _level_code_for as _level_code,
        TOURNAMENT_PRIORITY,
    )

    # FastAPI Query default "None" sometimes lands as the string "None"
    # via auto-conversion when callers omit the param via OpenAPI clients.
    # Treat empty-string + literal "None" the same as None.
    if tour in (None, "", "None", "null"):
        tour = None
    if tour and tour not in ("atp", "wta"):
        raise HTTPException(status_code=400,
                             detail="tour must be atp or wta")

    ensure_tables()
    conn = get_conn()
    today = et_today_str()
    end_date = (datetime.strptime(today, "%Y-%m-%d") + timedelta(days=days)
                ).strftime("%Y-%m-%d")

    where = ["date BETWEEN ? AND ?",
             "p1_id IS NOT NULL", "p2_id IS NOT NULL"]
    params: list = [today, end_date]
    if tour:
        where.append("tour = ?")
        params.append(tour)

    rows = conn.execute(f"""
        SELECT tour, tournament, tournament_id,
               surface,
               COUNT(*) AS matches_total,
               SUM(CASE WHEN date = ? THEN 1 ELSE 0 END) AS matches_today,
               SUM(CASE WHEN status IS NULL OR status = 'pre'
                        THEN 1 ELSE 0 END) AS matches_pre,
               SUM(CASE WHEN status = 'in' THEN 1 ELSE 0 END) AS matches_live,
               MIN(date) AS first_date,
               MAX(date) AS last_date
        FROM tennis_scheduled_matches
        WHERE {' AND '.join(where)}
        GROUP BY tour, tournament, tournament_id
    """, [today, *params]).fetchall()

    # Edge counts per tournament — pick volume from settled+pending picks
    # in the window. tennis_picks doesn't carry `tournament` directly, so
    # we join to tennis_scheduled_matches on match_id to attach the
    # tournament name. Cheap aggregate; runs once per index hit.
    edge_rows = conn.execute("""
        SELECT m.tour, m.tournament, COUNT(*) AS edges
        FROM tennis_picks p
        LEFT JOIN tennis_scheduled_matches m ON m.match_id = p.match_id
        WHERE p.date BETWEEN ? AND ?
        GROUP BY m.tour, m.tournament
    """, [today, end_date]).fetchall()
    edges_by_key: dict[tuple, int] = {
        (e["tour"], e["tournament"]): int(e["edges"])
        for e in edge_rows if e["tournament"]
    }

    out = []
    for r in rows:
        d = dict(r)
        level = _level_code(d["tournament"])
        priority = TOURNAMENT_PRIORITY.get(level, 30)
        out.append({
            "tour": d["tour"],
            "tournament": d["tournament"],
            "tournament_id": d["tournament_id"],
            "surface": d["surface"],
            "level": level,
            "priority": priority,
            "first_date": d["first_date"],
            "last_date": d["last_date"],
            "matches_total": int(d["matches_total"] or 0),
            "matches_today": int(d["matches_today"] or 0),
            "matches_pre": int(d["matches_pre"] or 0),
            "matches_live": int(d["matches_live"] or 0),
            "edges": edges_by_key.get((d["tour"], d["tournament"]), 0),
        })

    # Sort: live first, then by priority desc, then alpha for stable
    # rendering inside the same priority tier.
    out.sort(key=lambda t: (
        -t["matches_live"],
        -t["priority"],
        (t["tournament"] or "").lower(),
    ))

    return {"tournaments": out, "date": today, "days": days}


@router.get("/api/tennis/calibration")
def api_tennis_calibration():
    """Calibration diagnostics for the tennis stack.

    Computes Brier (raw vs calibrated) across every settled W/L pick,
    sliced by (tour, bet_type). Also returns the dynamic_reliability
    multiplier per bet_type so the user can see how much picks_core
    is up/down-weighting each market. Mirrors the F1 calibration view.

    The empirical calibration table itself is bucket-keyed
    ``(bet_type, prob_bucket)`` plus the fav/dog and direction splits
    when sample is high enough — too unwieldy to return verbatim.
    Instead we report per-(tour, bet_type) Brier delta as the
    headline "is calibration helping" signal.
    """
    import sqlite3
    from engine import empirical_calibration as ec
    from engine.dynamic_reliability import get_reliability, _settled_stats
    from engine.tennis_db import get_conn, ensure_tables

    ec.refresh_calibration("tennis")
    ensure_tables()
    conn = get_conn()
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT tour, bet_type, model_prob, result, odds, edge, pick
        FROM tennis_picks
        WHERE result IN ('W', 'L') AND model_prob IS NOT NULL
    """).fetchall()

    from collections import defaultdict
    groups: dict[tuple, list] = defaultdict(list)
    overall = []
    for r in rows:
        d = dict(r)
        groups[(d["tour"], d["bet_type"])].append(d)
        overall.append(d)

    def _brier(rows: list[dict]) -> tuple[int, float, float]:
        if not rows:
            return 0, 0.0, 0.0
        raw_sum = 0.0
        cal_sum = 0.0
        for d in rows:
            y = 1 if d["result"] == "W" else 0
            raw = float(d["model_prob"])
            cal = float(ec.calibrate(d["bet_type"], raw, sport="tennis",
                                      edge=d.get("edge"), odds=d.get("odds"),
                                      pick_text=d.get("pick")))
            raw_sum += (raw - y) ** 2
            cal_sum += (cal - y) ** 2
        n = len(rows)
        return n, raw_sum / n, cal_sum / n

    n_all, b_raw_all, b_cal_all = _brier(overall)
    per_market: list[dict] = []
    for key, rs in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        n, b_raw, b_cal = _brier(rs)
        # Dynamic reliability multiplier (auto-tuned from realized ROI).
        # Returned alongside Brier so the user sees both calibration
        # quality (Brier) and trust signal (reliability) for each market.
        rel_stats_n, rel_stats_roi = _settled_stats("tennis", key[1])
        per_market.append({
            "tour": key[0],
            "bet_type": key[1],
            "n": n,
            "wins": sum(1 for r in rs if r["result"] == "W"),
            "brier_raw": round(b_raw, 4),
            "brier_calibrated": round(b_cal, 4),
            "brier_delta": round(b_cal - b_raw, 4),
            "improvement_pct": (round((b_raw - b_cal) / b_raw * 100, 1)
                                 if b_raw > 0 else 0.0),
            "reliability": round(get_reliability("tennis", key[1]), 3),
            "roi_per_pick": round(rel_stats_roi, 2),
        })

    return {
        "n": n_all,
        "brier_raw": round(b_raw_all, 4),
        "brier_calibrated": round(b_cal_all, 4),
        "brier_delta": round(b_cal_all - b_raw_all, 4),
        "improvement_pct": (round((b_raw_all - b_cal_all) / b_raw_all * 100, 1)
                             if b_raw_all > 0 else 0.0),
        "per_market": per_market,
    }


@router.get("/api/tennis/scheduled/count")
def api_tennis_scheduled_count():
    """Lightweight count of today's resolvable, not-yet-played matches
    on ATP/WTA main + Challenger tiers. Used by the sidebar badge —
    avoids hitting the heavy /scheduled endpoint just to learn how
    many matches are on the slate. ITF Futures are excluded (they
    inflate the count to 300+ and aren't the user's primary target).
    """
    from datetime import datetime
    from engine.tennis_db import get_conn, ensure_tables
    ensure_tables()
    conn = get_conn()
    today = et_today_str()
    n = conn.execute(
        "SELECT COUNT(*) FROM tennis_scheduled_matches "
        "WHERE date = ? AND p1_id IS NOT NULL AND p2_id IS NOT NULL "
        "  AND (status IS NULL OR status NOT IN ('post','final')) "
        "  AND NOT (LOWER(COALESCE(tournament,'')) LIKE '%itf%' "
        "        OR LOWER(COALESCE(tournament,'')) LIKE '%futures%' "
        "        OR LOWER(COALESCE(tournament,'')) LIKE '%utr%')",
        (today,),
    ).fetchone()[0]
    return {"count": int(n), "date": today}


@router.get("/api/tennis/top-players")
def api_tennis_top_players(tour: str = Query("atp"),
                            surface: str = Query("Hard"),
                            limit: int = Query(25, ge=1, le=100)):
    """Top-N rated players per (tour, surface). Powers the rankings
    panel on the Tennis tab."""
    if tour not in _TENNIS_TOURS:
        raise HTTPException(status_code=400, detail="tour must be atp/wta")
    from engine.tennis_elo import top_players
    rows = top_players(tour, surface=surface, limit=limit, min_matches=30)
    return {"tour": tour, "surface": surface, "players": rows}


@router.get("/api/tennis/players/search")
def api_tennis_player_search(tour: str = Query("atp"),
                              q: str = Query("")):
    """Lookup players by name fragment. Used by the matchup picker
    autocomplete on the Tennis tab."""
    if tour not in _TENNIS_TOURS:
        raise HTTPException(status_code=400, detail="tour must be atp/wta")
    from engine.tennis_db import get_conn, ensure_tables
    ensure_tables()
    if not q or len(q) < 2:
        return {"tour": tour, "results": []}
    conn = get_conn()
    rows = conn.execute(
        "SELECT player_id, name, country, hand FROM tennis_players "
        "WHERE tour = ? AND name LIKE ? ORDER BY name LIMIT 25",
        (tour, f"%{q}%"),
    ).fetchall()
    return {"tour": tour, "results": [dict(r) for r in rows]}


@router.get("/api/tennis/predict")
def api_tennis_predict(tour: str = Query("atp"),
                        p1_id: int = Query(...),
                        p2_id: int = Query(...),
                        surface: str = Query("Hard"),
                        best_of: int = Query(3),
                        date: str | None = Query(None)):
    """Predict outcome for a single matchup."""
    if tour not in _TENNIS_TOURS:
        raise HTTPException(status_code=400, detail="tour must be atp/wta")
    if best_of not in (3, 5):
        raise HTTPException(status_code=400, detail="best_of must be 3 or 5")
    from engine.tennis_predict import predict_match
    from engine.tennis_db import get_player_by_id
    pred = predict_match(tour, p1_id, p2_id,
                          surface=surface, best_of=best_of, date=date)
    p1 = get_player_by_id(tour, p1_id)
    p2 = get_player_by_id(tour, p2_id)
    if p1:
        pred["p1_name"] = p1["name"]
    if p2:
        pred["p2_name"] = p2["name"]
    return pred


@router.get("/api/tennis/picks/preview")
def api_tennis_picks_preview(tour: str = Query("atp"),
                              p1_id: int = Query(...),
                              p2_id: int = Query(...),
                              surface: str = Query("Hard"),
                              best_of: int = Query(3),
                              p1_ml: int | None = Query(None),
                              p2_ml: int | None = Query(None),
                              tourney_level: str | None = Query(None)):
    """Generate candidate picks for a matchup at given odds. Caller
    POSTs to /tracker/record to lock one."""
    if tour not in _TENNIS_TOURS:
        raise HTTPException(status_code=400, detail="tour must be atp/wta")
    from engine.tennis_predict import predict_match
    from engine.tennis_picks import generate_tennis_picks
    from engine.tennis_db import get_player_by_id
    pred = predict_match(tour, p1_id, p2_id,
                          surface=surface, best_of=best_of)
    p1 = get_player_by_id(tour, p1_id)
    p2 = get_player_by_id(tour, p2_id)
    if p1:
        pred["p1_name"] = p1["name"]
    if p2:
        pred["p2_name"] = p2["name"]
    odds = {"p1_ml": p1_ml, "p2_ml": p2_ml}
    picks = generate_tennis_picks(pred, odds,
                                   tournament_level=tourney_level,
                                   include_lower_tiers=True)
    return {"tour": tour, "prediction": pred, "picks": picks}


_TENNIS_SETTLE_TS: float = 0.0
_TENNIS_SETTLE_LOCK = __import__("threading").Lock()
# Settle pulls 5 days of TennisExplorer (~30s-2min). Running it inside
# the request blocks the tracker tab — user reported the tab "takes
# forever to load." We now fire it in a daemon thread the first time
# the tab is hit (or after a 5-min cooldown) and return cached rows
# immediately. Next hit reflects the freshly-settled results.
_TENNIS_SETTLE_TTL_S = 300


def _kick_tennis_settle() -> None:
    import time, threading
    global _TENNIS_SETTLE_TS
    with _TENNIS_SETTLE_LOCK:
        now = time.monotonic()
        if (now - _TENNIS_SETTLE_TS) < _TENNIS_SETTLE_TTL_S:
            return
        _TENNIS_SETTLE_TS = now  # claim the slot before kicking the thread
    def _worker():
        try:
            from engine.tennis_tracker import settle_tennis_picks
            settle_tennis_picks()
        except Exception as e:
            logger.debug("tennis settle (background) failed: %s", e)
    threading.Thread(target=_worker, daemon=True).start()


@router.get("/api/tennis/tracker/history")
def api_tennis_tracker_history(tour: str | None = Query(None),
                                limit: int = Query(200)):
    """Tracker view — pending + settled tennis picks.

    Settle runs in a background thread on a 5-min cooldown so the
    route returns immediately; freshly-settled rows appear on the
    next refresh."""
    from engine.tennis_tracker import list_history
    _kick_tennis_settle()
    rows = list_history(tour, limit=limit)
    return {"tour": tour or "all", "rows": rows}


@router.get("/api/tennis/tracker/summary")
def api_tennis_tracker_summary(tour: str | None = Query(None)):
    """W-L-P / profit / ROI summary + probation status. Frontend
    surfaces probation as a banner so the user knows tennis is on
    paper-bet only."""
    from engine.tennis_tracker import (
        get_pick_summary, is_on_probation,
        PROBATION_MIN_DECIDED, PROBATION_BRIER_CEILING,
    )
    summary = get_pick_summary(tour) or {}
    summary["on_probation"] = is_on_probation()
    summary["probation_min_decided"] = PROBATION_MIN_DECIDED
    summary["probation_brier_ceiling"] = PROBATION_BRIER_CEILING
    return summary


@router.post("/api/tennis/tracker/record")
def api_tennis_tracker_record(payload: dict = Body(...)):
    """Lock a tennis pick into the tracker."""
    from engine.tennis_tracker import record_tennis_pick
    try:
        new_id = record_tennis_pick(payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"id": new_id}


@router.post("/api/tennis/tracker/settle")
def api_tennis_tracker_settle():
    """Manual trigger for the tennis settler (matches table walk)."""
    from engine.tennis_tracker import settle_tennis_picks
    return settle_tennis_picks()
