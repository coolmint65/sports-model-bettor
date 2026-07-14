"""Football backend routes.

Per-league read endpoints. Pattern mirrors routes_hockey + routes_golf:
one ingest + predict + odds + picks call per slate hit, with debounced
ESPN ingest at the layer below.
"""
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


def _lock_football_live_pick(conn, d: dict) -> None:
    """Lock-on-go-live: once a football game flips to live, the card
    surfaces the recorded pre-kickoff pick instead of regenerating
    from current odds. Same pattern as soccer/basketball/hockey
    framework routes."""
    status = (d.get("status") or "scheduled").lower()
    if status in _NOT_LIVE_STATUSES:
        return
    gid = d.get("game_id")
    if not gid:
        return
    try:
        rows = conn.execute(
            "SELECT bet_type, pick, model_prob, edge, odds, stake_units "
            "FROM picks "
            "WHERE game_id = ? AND result IS NULL "
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


@router.get("/api/football/leagues")
def api_football_leagues() -> dict:
    """Sidebar discovery — every football league with status/in-season
    + today's game count."""
    from engine.football import LEAGUE_REGISTRY, active_leagues
    from engine.football._db import get_conn
    in_season = set(active_leagues())
    today = _today_et()
    out = []
    for k, v in LEAGUE_REGISTRY.items():
        n = 0
        if k in in_season:
            try:
                conn = get_conn(k)
                row = conn.execute(
                    "SELECT COUNT(*) FROM games "
                    "WHERE date = ? AND status IN "
                    "  ('scheduled', 'live', 'final')",
                    (today,),
                ).fetchone()
                n = int(row[0] or 0)
                if n == 0:
                    nxt = conn.execute(
                        "SELECT COUNT(*) FROM games "
                        "WHERE date > ? AND status = 'scheduled' "
                        "  ORDER BY date ASC LIMIT 7",
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


@router.get("/api/football/{league}/today")
def api_football_today(league: str) -> dict:
    """Slate for ``league``. Fall-forward to the nearest upcoming day
    when today has no scheduled or finalized games."""
    from engine.football import LEAGUE_REGISTRY, get_league_config
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    cfg = get_league_config(league)

    from engine.football._espn_ingest import ingest_today
    now = time.monotonic()
    if (now - _INGEST_TS.get(league, 0.0)) >= _INGEST_TTL_S:
        try:
            ingest_today(league)
            # Re-ingest the prior 2 days too. ESPN sometimes finalizes
            # a late-night game hours after kickoff; without this sweep,
            # games like DAL@STL 2026-05-29 stay stuck at status='
            # scheduled' and their picks sit pending forever. Cheap —
            # the scoreboard endpoint is one HTTP call per day.
            from datetime import datetime as _dt, timedelta as _td
            today_dt = _dt.strptime(_today_et(), "%Y-%m-%d")
            for back in (1, 2):
                d = (today_dt - _td(days=back)).strftime("%Y-%m-%d")
                try:
                    ingest_today(league, date=d)
                except Exception as e:
                    logger.debug("[football:%s] backsweep %s failed: %s",
                                  league, d, e)
            _INGEST_TS[league] = now
        except Exception as e:
            logger.warning("[football:%s] ingest failed: %s", league, e)

    from engine.football._db import get_conn
    from engine.football._predict import predict_match
    from engine.football._odds import fetch_league_odds
    from engine.football._picks import generate_picks
    from engine.football._tracker import record_picks, settle_picks
    from engine.football._elo import replay
    conn = get_conn(league)

    today = _today_et()
    # Try today first; fall forward up to 7 days if empty.
    target = today
    rows = []
    for delta in range(0, 8):
        from datetime import datetime as _dt, timedelta as _td
        cand = (_dt.strptime(today, "%Y-%m-%d") + _td(days=delta)
                 ).strftime("%Y-%m-%d")
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
            "ORDER BY g.start_time ASC",
            (cand,),
        ).fetchall()
        if rows:
            target = cand
            break

    odds_by_key = fetch_league_odds(league)
    ratings = replay(league)
    from engine.football._predict import log_signals
    games = []
    for r in rows:
        d = dict(r)
        pred = predict_match(league, int(d["home_team_id"]),
                              int(d["away_team_id"]), ratings=ratings)
        # Stamp IDs onto the prediction so log_signals can attribute
        # the row. The bare predict_match doesn't know which game it
        # came from.
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
                logger.warning("[football:%s] picks failed for %s: %s",
                                league, d["game_id"], e)
        best = picks[0] if picks else None
        d["picks"] = [best] if best else []
        d["best_pick"] = best
        # Lock-on-go-live mirror of the other frameworks.
        _lock_football_live_pick(conn, d)
        # V3.2 signal log — fire-and-forget; opens a short-lived
        # writer conn so concurrent slate hits don't race the cached
        # one.
        log_signals(league, pred)
        games.append(d)

    # Piggyback record + settle each slate hit. Live games skip
    # re-record (lock semantics). Games more than ~3 days away also
    # skip — football is weekly and a pick recorded against a game
    # 5+ days out muddles the tracker with picks the user hasn't
    # actually seen on the slate yet. Sticks to the "ONE pick on the
    # upcoming card, tracker shows the same one pick" mental model.
    from datetime import datetime as _dt, timedelta as _td
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
        logger.warning("[football:%s] record/settle piggyback failed: %s",
                        league, e)

    return {
        "league":       league,
        "display_name": cfg.get("display_name") or league,
        "date":         target,
        "games":        games,
    }


@router.post("/api/football/{league}/tracker/settle")
def api_football_tracker_settle(league: str) -> dict:
    from engine.football import LEAGUE_REGISTRY
    from engine.football._tracker import settle_picks
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    return settle_picks(league)


@router.get("/api/football/{league}/standings")
def api_football_standings(league: str) -> list[dict]:
    """Standings — W-L record + points-for/against + L10 + streak.
    Mirrors the baseball / basketball framework shape so the shared
    StandingsView UI primitive renders football identically."""
    from engine.football import LEAGUE_REGISTRY
    if league not in LEAGUE_REGISTRY:
        return []
    from engine.football._db import get_conn
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
    state: dict[int, dict] = {}
    for r in rows:
        h = int(r["home_team_id"]); a = int(r["away_team_id"])
        hs = int(r["home_score"]); as_ = int(r["away_score"])
        for tid, scored, allowed, won, tied in (
            (h, hs, as_, hs > as_, hs == as_),
            (a, as_, hs, as_ > hs, hs == as_),
        ):
            s = state.setdefault(tid, {
                "team_id": tid, "wins": 0, "losses": 0, "ties": 0,
                "points_for": 0, "points_against": 0,
                "recent": [], "streak_type": None, "streak_len": 0,
            })
            s["points_for"] += scored
            s["points_against"] += allowed
            if tied:
                s["ties"] += 1
                result = "T"
            elif won:
                s["wins"] += 1
                result = "W"
            else:
                s["losses"] += 1
                result = "L"
            s["recent"].append(result)
            if s["streak_type"] == result:
                s["streak_len"] += 1
            else:
                s["streak_type"] = result
                s["streak_len"] = 1
    out = []
    for tid, s in state.items():
        team = teams_meta.get(tid, {})
        wins = s["wins"]; losses = s["losses"]; ties = s["ties"]
        played = wins + losses + ties
        last10 = s["recent"][-10:]
        l10_w = sum(1 for r in last10 if r == "W")
        l10_l = sum(1 for r in last10 if r == "L")
        out.append({
            "team_id":        tid,
            "name":           team.get("name"),
            "abbreviation":   team.get("abbreviation"),
            "logo_url":       team.get("logo_url"),
            "wins":           wins,
            "losses":         losses,
            "ties":           ties,
            "pct":            round((wins + 0.5 * ties) / played, 3) if played else 0.0,
            "points_for":     s["points_for"],
            "points_against": s["points_against"],
            "point_diff":     s["points_for"] - s["points_against"],
            "l10":            f"{l10_w}-{l10_l}",
            "streak":         (f"{s['streak_type']}{s['streak_len']}"
                                  if s["streak_type"] else "-"),
        })
    out.sort(key=lambda x: (-x["pct"], -x["point_diff"]))
    return out


@router.get("/api/football/{league}/calibration")
def api_football_calibration(league: str) -> dict:
    """Calibration tab payload: fitted Elo/Normal constants, ensemble
    weights, and per-bucket Brier rolls once enough picks settle.
    Mirrors /api/hockey/{league}/calibration shape so the React panel
    can reuse the layout."""
    from engine.football import LEAGUE_REGISTRY, get_league_config
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    cfg = get_league_config(league)
    from engine.football._db import get_conn
    conn = get_conn(league)
    settled_n = conn.execute(
        "SELECT COUNT(*) FROM picks WHERE result IN ('W','L','P')"
    ).fetchone()[0]
    # Ensemble weights + GBM-trained flag — pulled directly from the
    # ensemble module so the panel reports what predictions actually
    # use, not a hard-coded constant.
    from engine.football._ensemble import _load_weights
    from engine.football._gbm import _is_trained
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


@router.get("/api/football/{league}/tracker/history")
def api_football_tracker_history(league: str, limit: int = 200) -> dict:
    from engine.football import LEAGUE_REGISTRY
    from engine.football._tracker import list_history, settle_picks
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    try:
        settle_picks(league)
    except Exception as e:
        logger.debug("[football:%s] settle on history failed: %s",
                      league, e)
    rows = list_history(league, limit=limit)
    # Project stake-weighted P/L onto each returned row so the table
    # column matches the summed totals below.
    rows_out = []
    for r in rows:
        d = dict(r) if not isinstance(r, dict) else dict(r)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        rows_out.append(d)

    from engine.football._db import get_conn
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
            "total": wins + losses + pushes,
            "wins": wins, "losses": losses, "pushes": pushes,
            "pending": pending,
            "profit": round(profit, 2),
            "win_pct": round(wins / decided * 100, 1) if decided else 0.0,
            "roi": round(profit / stake_settled, 1) if stake_settled else 0.0,
        },
        "by_type": by_type,
    }
    return {"league": league, "rows": rows_out, "summary": summary}
