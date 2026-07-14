"""Baseball backend routes — same shape as routes_football."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from fastapi import APIRouter

router = APIRouter()
logger = logging.getLogger(__name__)


try:
    from zoneinfo import ZoneInfo
    _US_EASTERN = ZoneInfo("America/New_York")
except Exception:
    _US_EASTERN = None


def _today_et() -> str:
    if _US_EASTERN is not None:
        return datetime.now(_US_EASTERN).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc)
    offset = 4 if 3 <= now.month <= 10 else 5
    return (datetime.fromtimestamp(now.timestamp() - offset * 3600)
             .strftime("%Y-%m-%d"))


_INGEST_TTL_S = 90
_INGEST_TS: dict[str, float] = {}


_NOT_LIVE_STATUSES = frozenset({
    "scheduled", "pre_game", "pregame",
    "final", "complete", "completed",
    "cancelled", "postponed", "voided",
})


def _lock_baseball_live_pick(conn, d: dict) -> None:
    status = (d.get("status") or "scheduled").lower()
    if status in _NOT_LIVE_STATUSES:
        return
    gid = d.get("game_id")
    if not gid:
        return
    try:
        rows = conn.execute(
            "SELECT bet_type, pick, model_prob, edge, odds, stake_units "
            "FROM picks WHERE game_id = ? AND result IS NULL "
            "ORDER BY edge DESC",
            (str(gid),),
        ).fetchall()
    except Exception:
        return
    if not rows:
        return
    locked = {
        "type":       rows[0]["bet_type"],
        "pick":       rows[0]["pick"],
        "model_prob": rows[0]["model_prob"],
        "edge":       rows[0]["edge"],
        "odds":       rows[0]["odds"],
        "stake_units": rows[0]["stake_units"] if "stake_units" in rows[0].keys() else None,
        "locked":     True,
    }
    d["best_pick"] = locked
    d["picks"] = [locked]
    d["pick_locked"] = True


@router.get("/api/baseball/leagues")
def api_baseball_leagues() -> dict:
    from engine.baseball import LEAGUE_REGISTRY, active_leagues
    from engine.baseball._db import get_conn
    in_season = set(active_leagues())
    today = _today_et()
    out = []
    for k, v in LEAGUE_REGISTRY.items():
        n = 0
        if k in in_season:
            try:
                conn = get_conn(k)
                # Same TBD-opponent exclusion the /today route applies —
                # otherwise the sidebar badge advertises games the slate
                # itself would hide (postseason regionals).
                count_sql = (
                    "SELECT COUNT(*) FROM games g "
                    "JOIN teams ht ON ht.id = g.home_team_id "
                    "JOIN teams at ON at.id = g.away_team_id "
                    "WHERE g.date = ? AND g.status IN "
                    "  ('scheduled', 'live', 'final') "
                    "  AND COALESCE(ht.abbreviation, '') <> '' "
                    "  AND COALESCE(at.abbreviation, '') <> '' "
                    "  AND ht.name NOT LIKE 'TBD%' "
                    "  AND at.name NOT LIKE 'TBD%'"
                )
                row = conn.execute(count_sql, (today,)).fetchone()
                n = int(row[0] or 0)
                if n == 0:
                    nxt = conn.execute(
                        count_sql.replace("g.date = ?",
                                          "g.date > ?")
                                  .replace("g.status IN "
                                            "  ('scheduled', 'live', 'final')",
                                            "g.status = 'scheduled'"),
                        (today,),
                    ).fetchone()
                    n = int(nxt[0] or 0)
            except Exception:
                n = 0
        out.append({
            "key":              k,
            "display_name":     v.get("display_name") or k,
            "country":          v.get("country"),
            "region":           v.get("region"),
            "status":           v.get("status"),
            "in_season":        k in in_season,
            "game_count_today": n,
        })
    return {"leagues": out}


@router.get("/api/baseball/{league}/today")
def api_baseball_today(league: str) -> dict:
    from engine.baseball import LEAGUE_REGISTRY, get_league_config
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    cfg = get_league_config(league)

    from engine.baseball._espn_ingest import ingest_today
    now = time.monotonic()
    if (now - _INGEST_TS.get(league, 0.0)) >= _INGEST_TTL_S:
        try:
            ingest_today(league)
            # 2-day backsweep — late-finalized games (regional bracket
            # results that ESPN finalizes hours after the route last
            # ran) get re-ingested so picks can settle on the next tick.
            from datetime import datetime as _dt0, timedelta as _td0
            today_dt = _dt0.strptime(_today_et(), "%Y-%m-%d")
            for back in (1, 2):
                d = (today_dt - _td0(days=back)).strftime("%Y-%m-%d")
                try:
                    ingest_today(league, date=d)
                except Exception as e:
                    logger.debug("[baseball:%s] backsweep %s failed: %s",
                                  league, d, e)
            _INGEST_TS[league] = now
        except Exception as e:
            logger.warning("[baseball:%s] ingest failed: %s", league, e)

    from engine.baseball._db import get_conn
    from engine.baseball._predict import predict_match, log_signals
    from engine.baseball._odds import fetch_league_odds
    from engine.baseball._picks import generate_picks
    from engine.baseball._tracker import record_picks, settle_picks
    from engine.baseball._elo import replay
    conn = get_conn(league)

    today = _today_et()
    target = today
    rows = []
    fell_forward = False
    from datetime import datetime as _dt, timedelta as _td
    for delta in range(0, 8):
        cand = (_dt.strptime(today, "%Y-%m-%d") + _td(days=delta)
                 ).strftime("%Y-%m-%d")
        # Filter out games where either team is TBD — common during
        # postseason regional brackets where the opponent depends on an
        # earlier-round winner. ESPN materializes both records with a
        # TBD team stub (empty abbr); the slate would otherwise show
        # rows like " @ UGA" with no odds and no pick possible. Once
        # the earlier round resolves and ESPN updates, the next ingest
        # tick re-includes them automatically.
        rows = conn.execute(
            "SELECT g.*, ht.abbreviation AS home_abbr, "
            "       ht.name AS home_name, ht.logo_url AS home_logo, "
            "       at.abbreviation AS away_abbr, "
            "       at.name AS away_name, at.logo_url AS away_logo "
            "FROM games g "
            "JOIN teams ht ON ht.id = g.home_team_id "
            "JOIN teams at ON at.id = g.away_team_id "
            "WHERE g.date = ? "
            "  AND g.status IN ('scheduled', 'live', 'final') "
            "  AND COALESCE(ht.abbreviation, '') <> '' "
            "  AND COALESCE(at.abbreviation, '') <> '' "
            "  AND ht.name NOT LIKE 'TBD%' "
            "  AND at.name NOT LIKE 'TBD%' "
            "ORDER BY g.start_time ASC",
            (cand,),
        ).fetchall()
        if rows:
            target = cand
            fell_forward = (delta > 0)
            break

    odds_by_key = fetch_league_odds(league)
    ratings = replay(league)
    games = []
    for r in rows:
        d = dict(r)
        pred = predict_match(league, int(d["home_team_id"]),
                              int(d["away_team_id"]), ratings=ratings)
        pred["game_id"] = d["game_id"]
        pred["home_team_id"] = int(d["home_team_id"])
        pred["away_team_id"] = int(d["away_team_id"])
        key = f"{d['away_abbr']}@{d['home_abbr']}"
        odds = odds_by_key.get(key)
        d["odds"] = odds
        d["prediction"] = pred
        picks = []
        if d["status"] == "scheduled" and odds:
            try:
                picks = generate_picks(league, pred, odds,
                                        game_id=d["game_id"])
            except Exception as e:
                logger.warning("[baseball:%s] picks failed for %s: %s",
                                league, d["game_id"], e)
        best = picks[0] if picks else None
        d["picks"] = [best] if best else []
        d["best_pick"] = best
        _lock_baseball_live_pick(conn, d)
        log_signals(league, pred)
        games.append(d)

    # Record picks for the nearest day only — bounds the tracker so
    # the user sees the same one pick on the card and in the tracker.
    today_dt = _dt.strptime(today, "%Y-%m-%d")
    max_record_date = (today_dt + _td(days=3)).strftime("%Y-%m-%d")
    try:
        for g in games:
            if g.get("pick_locked"):
                continue
            if (g.get("status") == "scheduled"
                    and g.get("picks")
                    and (g.get("date") or "9999") <= max_record_date):
                record_picks(league, g["game_id"], g["picks"])
        settle_picks(league)
    except Exception as e:
        logger.warning("[baseball:%s] record/settle piggyback failed: %s",
                        league, e)

    # POTD — pick the highest-conviction non-shadow pick on today's
    # slate (same selector basketball framework + AFL use). Stored in
    # the league's own pick_of_day table. Added 2026-05-29 in response
    # to user ask "Does college baseball have a POTD?" — answer is now
    # yes.
    potd = None
    try:
        from engine.pick_of_day._select import get_or_create_potd
        from engine.pick_of_day import get_today_potd as _potd_read
        from engine.pick_of_day._storage import _ensure_potd_table
        _ensure_potd_table(league)
        existing = _potd_read(league)
        if existing:
            potd = existing
        else:
            bets = []
            for g in games:
                if g.get("status") != "scheduled":
                    continue
                bp = g.get("best_pick")
                if not bp:
                    continue
                bp_norm = dict(bp)
                if "prob" not in bp_norm and "model_prob" in bp_norm:
                    bp_norm["prob"] = bp_norm["model_prob"]
                bets.append({
                    "game_id": g.get("game_id"),
                    "matchup": (f"{g.get('away_abbr') or '?'} @ "
                                f"{g.get('home_abbr') or '?'}"),
                    "best_pick": bp_norm,
                    "home": {"name": g.get("home_name"),
                              "abbreviation": g.get("home_abbr")},
                    "away": {"name": g.get("away_name"),
                              "abbreviation": g.get("away_abbr")},
                    "time": g.get("start_time") or "",
                    "venue": "",
                    "status": {"state": "pre"},
                })
            potd = get_or_create_potd(league, bets)
    except Exception as e:
        logger.warning("[baseball:%s] potd compute failed: %s", league, e)

    return {
        "league":       league,
        "display_name": cfg.get("display_name") or league,
        "date":         target,
        "fell_forward": fell_forward,
        "games":        games,
        "potd":         potd,
    }


@router.post("/api/baseball/{league}/tracker/settle")
def api_baseball_tracker_settle(league: str) -> dict:
    from engine.baseball import LEAGUE_REGISTRY
    from engine.baseball._tracker import settle_picks
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    return settle_picks(league)


@router.post("/api/baseball/{league}/tracker/record")
def api_baseball_tracker_record(league: str) -> dict:
    """Force-record today's slate picks. Mirrors the
    /tracker/record contract every other framework exposes — the
    Record button in PickHistory hits this."""
    from engine.baseball import LEAGUE_REGISTRY
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    # Re-running the slate endpoint records picks as a side-effect.
    res = api_baseball_today(league)
    n = sum(1 for g in (res.get("games") or []) if g.get("picks"))
    return {"recorded": n, "date": res.get("date")}


@router.get("/api/baseball/{league}/potd")
def api_baseball_potd(league: str) -> dict:
    """Pick of the Day for a baseball framework league. Locks at first
    creation per (league, date). Mirrors the basketball-framework POTD
    contract so PickOfDayHero can hit `/baseball/{league}/potd` the
    same way it hits `/basketball/{league}/potd`."""
    from engine.baseball import LEAGUE_REGISTRY
    from engine.pick_of_day import get_or_create_potd, get_today_potd
    from engine.pick_of_day._storage import _ensure_potd_table
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    _ensure_potd_table(league)
    existing = get_today_potd(league)
    if existing:
        return existing
    slate = api_baseball_today(league)
    games = slate.get("games") or []
    bets = []
    for g in games:
        if g.get("status") != "scheduled":
            continue
        bp = g.get("best_pick")
        if not bp:
            continue
        bp_norm = dict(bp)
        if "prob" not in bp_norm and "model_prob" in bp_norm:
            bp_norm["prob"] = bp_norm["model_prob"]
        bets.append({
            "game_id": g.get("game_id"),
            "matchup": (f"{g.get('away_abbr') or '?'} @ "
                        f"{g.get('home_abbr') or '?'}"),
            "best_pick": bp_norm,
            "home": {"name": g.get("home_name"),
                      "abbreviation": g.get("home_abbr")},
            "away": {"name": g.get("away_name"),
                      "abbreviation": g.get("away_abbr")},
            "time": g.get("start_time") or "",
            "venue": "",
            "status": {"state": "pre"},
        })
    if not bets:
        return {"message": "No qualifying picks today", "sport": league}
    potd = get_or_create_potd(league, bets)
    return potd or {"message": "No qualifying picks today", "sport": league}


@router.get("/api/baseball/{league}/potd/summary")
def api_baseball_potd_summary(league: str) -> dict:
    """POTD running totals for the league."""
    from engine.baseball import LEAGUE_REGISTRY
    from engine.pick_of_day import get_potd_summary, settle_potd
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    try:
        settle_potd(league)
    except Exception as e:
        logger.debug("POTD auto-settle failed for %s: %s", league, e)
    return get_potd_summary(league)


@router.get("/api/baseball/{league}/pick-events")
def api_baseball_pick_events(league: str, game_id: str = "",
                              hours: int = 24) -> dict:
    """Pick-events feed for the 📜 popover. Baseball framework
    doesn't yet emit events (only NBA/MLB/NHL do via the unified
    pick_events table) — returning an empty list keeps the badge
    behaving rather than 404-ing.
    """
    from engine.baseball import LEAGUE_REGISTRY
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    return {"league": league, "game_id": game_id, "events": []}


@router.get("/api/baseball/{league}/standings")
def api_baseball_standings(league: str) -> list[dict]:
    """Standings — computed from finalized games. W-L record + run
    differential + L10 + current streak. NCAA baseball doesn't have
    OT/SO so the hockey-style points column doesn't apply.
    """
    from engine.baseball import LEAGUE_REGISTRY
    if league not in LEAGUE_REGISTRY:
        return []
    from engine.baseball._db import get_conn
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT g.home_team_id, g.away_team_id, g.home_score, g.away_score, "
        "       g.date "
        "FROM games g "
        "WHERE g.status = 'final' "
        "  AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL "
        "ORDER BY g.date ASC, g.game_id ASC"
    ).fetchall()
    teams_meta = {
        r["id"]: dict(r) for r in conn.execute(
            "SELECT id, name, abbreviation, logo_url FROM teams"
        ).fetchall()
    }
    # Per-team rolling record. Streak = trailing sequence of same
    # result; L10 = last 10 W/L count. Same shape every other
    # framework's standings endpoint returns.
    state: dict[int, dict] = {}
    for r in rows:
        h = int(r["home_team_id"]); a = int(r["away_team_id"])
        hs = int(r["home_score"]); as_ = int(r["away_score"])
        for tid, scored, allowed, won in (
            (h, hs, as_, hs > as_),
            (a, as_, hs, as_ > hs),
        ):
            s = state.setdefault(tid, {
                "team_id": tid, "wins": 0, "losses": 0,
                "runs_for": 0, "runs_against": 0,
                "recent": [], "streak_type": None, "streak_len": 0,
            })
            s["runs_for"] += scored
            s["runs_against"] += allowed
            result = "W" if won else "L"
            if won:
                s["wins"] += 1
            else:
                s["losses"] += 1
            s["recent"].append(result)
            if s["streak_type"] == result:
                s["streak_len"] += 1
            else:
                s["streak_type"] = result
                s["streak_len"] = 1
    out = []
    for tid, s in state.items():
        team = teams_meta.get(tid, {})
        wins = s["wins"]; losses = s["losses"]
        played = wins + losses
        last10 = s["recent"][-10:]
        l10_w = sum(1 for r in last10 if r == "W")
        l10_l = sum(1 for r in last10 if r == "L")
        out.append({
            "team_id":      tid,
            "name":         team.get("name"),
            "abbreviation": team.get("abbreviation"),
            "logo_url":     team.get("logo_url"),
            "wins":         wins,
            "losses":       losses,
            "pct":          round(wins / played, 3) if played else 0.0,
            "runs_for":     s["runs_for"],
            "runs_against": s["runs_against"],
            "run_diff":     s["runs_for"] - s["runs_against"],
            "l10":          f"{l10_w}-{l10_l}",
            "streak":       (f"{s['streak_type']}{s['streak_len']}"
                              if s["streak_type"] else "-"),
        })
    out.sort(key=lambda r: (-r["pct"], -r["wins"], r["losses"]))
    # Top-100 only — college baseball has 300+ programs and shipping
    # the whole list to the panel is a UX disaster.
    return out[:100]


@router.get("/api/baseball/{league}/calibration")
def api_baseball_calibration(league: str) -> dict:
    from engine.baseball import LEAGUE_REGISTRY, get_league_config
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    cfg = get_league_config(league)
    from engine.baseball._db import get_conn
    conn = get_conn(league)
    settled_n = conn.execute(
        "SELECT COUNT(*) FROM picks WHERE result IN ('W','L','P')"
    ).fetchone()[0]
    from engine.baseball._ensemble import _load_weights
    from engine.baseball._gbm import _is_trained
    gbm_trained = _is_trained(league)
    weights = _load_weights(league, gbm_trained)
    constants = {
        "home_advantage":   cfg.get("home_advantage"),
        "league_avg_total": cfg.get("league_avg_total"),
        "margin_sigma":     cfg.get("margin_sigma"),
        "total_sigma":      cfg.get("total_sigma"),
        "fitted_n":         cfg.get("fitted_n"),
        "status":           cfg.get("status"),
    }
    return {
        "league":    league,
        "constants": constants,
        "ensemble":  {**weights, "gbm_trained": gbm_trained},
        "n_settled": settled_n,
        "buckets":   [],
    }


@router.get("/api/baseball/{league}/tracker/history")
def api_baseball_tracker_history(league: str, limit: int = 200) -> dict:
    from engine.baseball import LEAGUE_REGISTRY
    from engine.baseball._tracker import list_history, settle_picks
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    try:
        settle_picks(league)
    except Exception as e:
        logger.debug("[baseball:%s] settle on history failed: %s",
                      league, e)
    rows = list_history(league, limit=limit)
    rows_out = []
    for r in rows:
        d = dict(r) if not isinstance(r, dict) else dict(r)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        rows_out.append(d)
    from engine.baseball._db import get_conn
    conn = get_conn(league)
    all_picks = conn.execute(
        "SELECT bet_type, result, profit, stake_units FROM picks"
    ).fetchall()
    wins = losses = pushes = pending = 0
    profit = 0.0
    stake_settled = 0.0
    by_type: dict[str, dict] = {}
    for r in all_picks:
        bt = r["bet_type"] or "?"
        b = by_type.setdefault(bt, {"total": 0, "wins": 0, "losses": 0,
                                      "pushes": 0, "pending": 0,
                                      "profit": 0.0, "stake_settled": 0.0})
        b["total"] += 1
        res = r["result"]
        stake_u = float(r["stake_units"] if r["stake_units"] is not None else 1.0)
        pr = float(r["profit"] or 0) * stake_u
        if res == "W":
            b["wins"] += 1; wins += 1; b["profit"] += pr; profit += pr
            b["stake_settled"] += stake_u; stake_settled += stake_u
        elif res == "L":
            b["losses"] += 1; losses += 1; b["profit"] += pr; profit += pr
            b["stake_settled"] += stake_u; stake_settled += stake_u
        elif res == "P":
            b["pushes"] += 1; pushes += 1
        else:
            b["pending"] += 1; pending += 1
    for b in by_type.values():
        d = b["wins"] + b["losses"]
        st = b.pop("stake_settled", 0)
        b["win_pct"] = round(b["wins"] / d * 100, 1) if d else 0.0
        b["roi"] = round(b["profit"] / st, 1) if st else 0.0
        b["profit"] = round(b["profit"], 2)
    decided = wins + losses
    summary = {
        "overall": {
            "total": wins + losses + pushes, "wins": wins,
            "losses": losses, "pushes": pushes, "pending": pending,
            "profit": round(profit, 2),
            "win_pct": round(wins / decided * 100, 1) if decided else 0.0,
            "roi": round(profit / stake_settled, 1) if stake_settled else 0.0,
        },
        "by_type": by_type,
    }
    return {"league": league, "rows": rows_out, "summary": summary}
