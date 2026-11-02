"""NBA player-props PAPER picker v2 -- the NHL / NFL playbook (MintyBot 2026-10-26).

Austin 2026-10-26: "Lets get to work on NBA for PA and then work on it's player props".

v1 (this module until today) was the MLB picker with basketball stat keys: every alt-line
rung of HR's ladder, the best edge per player-stat, last-60-days samples through a fitted
distribution and a pinned post-hoc curve, no market anchor. Its box scores were only
ingested when a pending pick needed settling, so the 2026-27 season opened with zero picks
(no picks -> no box scores -> no model data -> no picks). Last season's 113 settled went
48-65.

v2 mirrors engine.sports.nfl.prop_picks / engine.sports.nhl.prop_picks:
  * MAIN LINE ONLY: per (player, stat) the two-sided rung of HR's ladder closest to even.
  * Over AND Under from engine.sports.nba.prop_model (minutes x per-minute rate x opponent,
    NegBin), ANCHORED to the de-vigged line with engine.sports.nba.prop_eval's per-stat
    weight -- the DraftKings backtest's (points .05, rebounds .25, assists .20 ...) until
    the weekly refit on graded HR lines moves it.
  * 40% floor on the anchored prob, juice wall / max odds / 4% edge as every picker.
  * Missing-information guard: a raw model prob outside 20-80% against a near-even main
    line means the market knows something the box scores don't (role, injury, minutes
    limit) -- logged, never picked. Same for players ESPN lists Out and players with < 2
    regular-season games this season.
  * Real ESPN game_id + game date, matchup "AWAY @ HOME", headshot + team logo.
  * Austin's volume rules: at most ONE pick per game, the top THREE per date.
  * CARD LOCK: picks are recorded the first time HR's board prices the game (Austin 2026-10-29,
    "should work like NHL"; was game day from 09:00 ET) until each tip, and never voided or
    swapped, so the CLV read is honest. The anchor weights are fit against the DK OPEN, so the
    opener is what the model is tuned to beat.
  * Every main line priced, every run, goes to prop_eval_log with DraftKings' line for the
    same player-stat, so "does the model -- or HR's softness vs DK -- carry an edge?" is
    graded on hundreds of lines a night, not only the picks.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone

from . import prop_model as _pm
from . import prop_eval as _ev
from ...player_props_db import insert_pick, _conn_for
from ..mlb.prop_picks import (PROP_MIN_EDGE_PCT, PROP_MAX_ODDS, PROP_JUICE_WALL,
                              TOP_N_PER_GAME, TOP_N_PER_SPORT, _confidence_for,
                              _normalize_name)
from ..._tz import et_now, et_today_str, _US_EASTERN

logger = logging.getLogger(__name__)

_PROP_MIN_PROB = 0.40        # never headline a side more likely to lose than win
_RAW_BAND = (0.20, 0.80)     # raw model prob vs a near-even main line; outside = skip
_MIN_SEASON_GAMES = 2        # regular-season games this season before a player is priced
_ESPN_HEAD = "https://a.espncdn.com/i/headshots/nba/players/full/{}.png"
_ESPN_LOGO = "https://a.espncdn.com/i/teamlogos/nba/500/{}.png"

_last_gen = {"ts": 0.0, "date": None, "result": None}
_GEN_TTL = 120
_GEN_LOCK = threading.Lock()


def _imp(o) -> float | None:
    if o is None:
        return None
    o = float(o)
    if o == 0:
        return None
    return 100.0 / (o + 100.0) if o > 0 else -o / (-o + 100.0)


def _et(ts_utc: datetime) -> datetime:
    if _US_EASTERN is not None:
        return ts_utc.astimezone(_US_EASTERN)
    return ts_utc.astimezone(timezone(timedelta(hours=-4 if 3 <= ts_utc.month <= 10 else -5)))


def _games() -> dict:
    """{(hr_away, hr_home) for every abbreviation alias: game} for NBA games from
    yesterday to +2 days, tip as an aware UTC datetime and the ET game date."""
    from .db import get_conn
    try:
        from ...abbr import aliases_for
    except Exception:
        aliases_for = None
    rows = get_conn().execute(
        "SELECT g.game_id, g.date, g.start_time, g.status, g.home_team_id, g.away_team_id, "
        "ht.abbreviation AS home, at.abbreviation AS away FROM nba_games g "
        "JOIN nba_teams ht ON ht.id = g.home_team_id JOIN nba_teams at ON at.id = g.away_team_id "
        "WHERE g.date BETWEEN date('now', '-1 day') AND date('now', '+2 day')").fetchall()
    out = {}
    for r in rows:
        g = dict(r)
        try:
            tip = datetime.fromisoformat(str(g["start_time"]).replace("Z", "+00:00"))
            if tip.tzinfo is None:
                tip = tip.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        g["tip"] = tip
        g["et_date"] = _et(tip).strftime("%Y-%m-%d")
        aw = aliases_for(g["away"], sport="nba") if aliases_for else [g["away"]]
        hm = aliases_for(g["home"], sport="nba") if aliases_for else [g["home"]]
        for a in aw or [g["away"]]:
            for h in hm or [g["home"]]:
                prev = out.get((a, h))
                if prev is None or g["tip"] < prev["tip"]:
                    out[(a, h)] = g
    return out


def _initial_keys(norm: str) -> set:
    parts = norm.split()
    return {f"{parts[0][0]} {' '.join(parts[1:])}"} if len(parts) >= 2 else set()


def _player_index(asof: str) -> dict:
    """normalized name (+ 'f lastname') -> [candidates] from the box scores (latest team)."""
    rows = _conn_for("nba").execute(
        "SELECT player_id, player_name, team_id, MAX(date) AS last_date "
        "FROM player_game_logs WHERE date >= ? GROUP BY player_id",
        ((datetime.strptime(asof, "%Y-%m-%d") - timedelta(days=500)).strftime("%Y-%m-%d"),)
    ).fetchall()
    idx: dict = {}
    for r in rows:
        cand = {"player_id": int(r["player_id"]), "team_id": int(r["team_id"] or 0),
                "last_date": r["last_date"] or ""}
        nm = _normalize_name(r["player_name"])
        for key in {nm} | _initial_keys(nm):
            if key:
                idx.setdefault(key, []).append(cand)
    return idx


def _resolve(idx: dict, name: str, team_id: int | None, teams: tuple) -> dict | None:
    nm = _normalize_name(name)
    cands = idx.get(nm) or []
    if not cands:
        for key in _initial_keys(nm):
            cands = idx.get(key) or []
    want = (team_id,) if team_id else teams
    cands = [c for c in cands if c["team_id"] in want]
    if not cands:
        return None
    cands.sort(key=lambda c: c["last_date"], reverse=True)
    return cands[0]


def _out_names() -> set:
    """Normalized names ESPN currently lists Out (nba_injuries snapshot)."""
    try:
        from .db import get_conn
        return {_normalize_name(r["name"]) for r in get_conn().execute(
            "SELECT name FROM nba_injuries WHERE LOWER(status) = 'out'").fetchall()}
    except Exception:
        return set()


def _main_lines(rows: list) -> list:
    """Per (player, stat) keep the two-sided rung whose prices are closest to even."""
    best: dict = {}
    for r in rows:
        if r.get("bet_type") not in _pm.MARKETS or r.get("line") is None:
            continue
        io, iu = _imp(r.get("over_odds")), _imp(r.get("under_odds"))
        if io is None or iu is None:
            continue
        k = (r["player_name"], r["bet_type"])
        bal = abs(io - iu)
        if k not in best or bal < best[k][0]:
            best[k] = (bal, r)
    return [v[1] for v in best.values()]


def _score(prop: dict, p_over: float, p_under: float) -> dict | None:
    from ...market_blend import anchor_to_market as _anchor
    oo, uo = prop["over_odds"], prop["under_odds"]
    w = _ev.weight_for(prop["bet_type"])
    po = _anchor(p_over, oo, uo, w=w)
    pu = _anchor(p_under, uo, oo, w=w)
    cands = []
    for side, odds, p in (("Over", oo, po), ("Under", uo, pu)):
        if odds is None or odds > PROP_MAX_ODDS or odds < PROP_JUICE_WALL or p < _PROP_MIN_PROB:
            continue
        implied = _imp(odds)
        cands.append({"side": side, "line": float(prop["line"]), "odds": int(odds),
                      "model_prob": round(p, 4), "implied_prob": round(implied, 4),
                      "edge": round((p - implied) * 100.0, 2), "anchor_w": w})
    if not cands:
        return None
    best = max(cands, key=lambda c: c["edge"])
    return best if best["edge"] >= PROP_MIN_EDGE_PCT else None


def _rank(entry: dict) -> float:
    try:
        from ...dynamic_reliability import get_reliability
        rel = float(get_reliability("nba", entry["bet_type"]))
    except Exception:
        rel = 1.0
    return float(entry["scored"]["edge"]) * rel


def project_slate(target_date: str | None = None) -> dict:
    """Price every main line on HR's NBA board for games that haven't tipped. Returns
    {date, have_odds, count, picks (every priced line, display), evaluated, priced,
    skipped, _cands (qualifying candidates by game)}."""
    today = target_date or et_today_str()
    board = {}
    try:
        from scrapers.hardrock_props import fetch_nba_props
        board = fetch_nba_props() or {}
    except Exception as e:
        logger.warning("nba prop_picks: HR fetch failed: %s", e)
    games = _games()
    idx = _player_index(today)
    outs = _out_names()
    out_ids = {c["player_id"] for nm in outs for c in (idx.get(nm) or [])}   # teammate-out boost
    now = datetime.now(timezone.utc)
    skipped = {"no_game": 0, "started": 0, "no_player": 0, "few_games": 0, "out": 0,
               "no_projection": 0, "raw_band": 0, "no_edge": 0}
    display, eval_rows, cands = [], [], {}
    evaluated = 0
    for mk, rows in board.items():
        if "@" not in mk:
            continue
        away, home = mk.split("@", 1)
        g = games.get((away, home))
        if not g:
            skipped["no_game"] += len(rows)
            continue
        if g["tip"] <= now or (g["status"] or "").lower() in ("final", "live", "in", "postponed"):
            skipped["started"] += len(rows)
            continue
        matchup = f"{g['away']} @ {g['home']}"
        dk = _pm.dk_lines(g["game_id"])
        teams = (g["away_team_id"], g["home_team_id"])
        for prop in _main_lines(rows):
            evaluated += 1
            side_team = (g["home_team_id"] if prop.get("team_side") == "home"
                         else g["away_team_id"] if prop.get("team_side") == "away" else None)
            cand = _resolve(idx, prop["player_name"], side_team, teams)
            if not cand:
                skipped["no_player"] += 1
                continue
            stat = _pm.MARKETS[prop["bet_type"]]
            opp = g["away_team_id"] if cand["team_id"] == g["home_team_id"] else g["home_team_id"]
            pj = _pm.project(cand["player_id"], opp, g["et_date"], out_ids=out_ids)
            if not pj:
                skipped["no_projection"] += 1
                continue
            mu = pj["mu"].get(stat)
            p_over = _pm.p_over(stat, mu, float(prop["line"]))
            if p_over is None:
                skipped["no_projection"] += 1
                continue
            p_under = 1.0 - p_over
            is_home = cand["team_id"] == g["home_team_id"]
            entry = {
                "game_id": str(g["game_id"]), "date": g["et_date"], "matchup": matchup,
                "player_id": cand["player_id"], "player_name": prop["player_name"],
                "team": g["home"] if is_home else g["away"], "bet_type": prop["bet_type"],
                "market": prop["bet_type"], "stat_key": stat, "line": float(prop["line"]),
                "over_odds": prop["over_odds"], "under_odds": prop["under_odds"],
                "projection": round(mu, 2), "minutes": round(pj["min"], 1),
                "games_season": pj["n_season"], "p_over_raw": round(p_over, 4),
                "teammates_out": pj.get("tmo"),
                "p_under_raw": round(p_under, 4),
                "dk": dk.get((cand["player_id"], stat)),
                "headshot": _ESPN_HEAD.format(cand["player_id"]),
                "team_logo": _ESPN_LOGO.format((g["home"] if is_home else g["away"]).lower()),
                "tip": g["tip"].isoformat(),
            }
            if pj["n_season"] < _MIN_SEASON_GAMES:
                skipped["few_games"] += 1      # rookies / no games yet: no model, not logged
                continue
            eval_rows.append(entry)
            if _normalize_name(prop["player_name"]) in outs:
                skipped["out"] += 1
                continue
            if not (_RAW_BAND[0] <= p_over <= _RAW_BAND[1]):
                skipped["raw_band"] += 1
                continue
            scored = _score(prop, p_over, p_under)
            if not scored:
                skipped["no_edge"] += 1
                display.append(dict(entry, side=None, model_prob=None, odds=None, edge=None,
                                    plus_ev=False))
                continue
            entry["scored"] = scored
            cands.setdefault(entry["game_id"], []).append(entry)
            display.append(dict(entry, side=scored["side"], model_prob=scored["model_prob"],
                                odds=scored["odds"], implied_prob=scored["implied_prob"],
                                edge=scored["edge"], plus_ev=True))
    try:   # the card can lock any day HR prices (NHL standard), so every date's first sight is its pick snapshot
        by_day: dict = {}
        for e in eval_rows:
            by_day.setdefault(e["date"], []).append(e)
        for day, rows_d in by_day.items():
            _ev.log_lines([{"game_id": e["game_id"], "game_date": e["date"],
                            "player_id": e["player_id"], "player_name": e["player_name"],
                            "bet_type": e["bet_type"], "line": e["line"],
                            "over_odds": e["over_odds"], "under_odds": e["under_odds"],
                            "p_over": e["p_over_raw"], "proj": e["projection"], "dk": e["dk"]}
                           for e in rows_d], pick_day=day)
    except Exception as e:
        logger.warning("nba prop_picks: eval log failed: %s", e)
    display.sort(key=lambda x: (x["edge"] is not None, x["edge"] or -999), reverse=True)
    for d in display:
        d.pop("scored", None)
    return {"date": today, "have_odds": bool(board), "count": len(display), "picks": display,
            "evaluated": evaluated, "priced": len(eval_rows), "skipped": skipped, "_cands": cands}


def _record_game_day(cands: dict, now_et: datetime) -> int:
    """Card lock (NHL standard, 2026-10-29): for every game date from today on, fill up to
    TOP_N_PER_SPORT picks, at most TOP_N_PER_GAME per game, best edge x reliability first, the
    first time HR's board prices the game (no game-day hour gate). Existing picks for a date
    count against its caps and are never voided or swapped."""
    today = now_et.strftime("%Y-%m-%d")
    by_date: dict = {}
    for gid, cl in cands.items():
        if cl and cl[0]["date"] >= today:
            by_date.setdefault(cl[0]["date"], {})[gid] = cl
    conn = _conn_for("nba")
    rec = 0
    for day in sorted(by_date):
        rec += _record_date(conn, day, by_date[day])
    return rec


def _record_date(conn, day: str, games: dict) -> int:
    have = conn.execute(
        "SELECT game_id, COUNT(*) AS n FROM player_props_picks WHERE date = ? GROUP BY game_id",
        (day,)).fetchall()
    per_game = {str(r["game_id"]): int(r["n"]) for r in have}
    slots = TOP_N_PER_SPORT - sum(per_game.values())
    if slots <= 0:
        return 0
    winners = []
    for gid, cl in games.items():
        if per_game.get(gid, 0) >= TOP_N_PER_GAME:
            continue
        winners.append(max(cl, key=_rank))
    winners.sort(key=_rank, reverse=True)
    rec = 0
    for e in winners[:slots]:
        s = e["scored"]
        pid = insert_pick(
            "nba", game_id=e["game_id"], date=e["date"], matchup=e["matchup"],
            player_id=int(e["player_id"]), player_name=e["player_name"], bet_type=e["bet_type"],
            pick="%s %g" % (s["side"], s["line"]), line=s["line"], side=s["side"],
            model_prob=s["model_prob"], edge=s["edge"], odds=s["odds"],
            confidence=_confidence_for(s["edge"]), headshot=e["headshot"], team_logo=e["team_logo"])
        if pid:
            rec += 1
            logger.info("nba prop_picks: LOCKED %s %s %s %s @%s (model %.3f raw %.3f w %.2f "
                        "proj %.1f in %.0f min, edge %+.1f)",
                        e["matchup"], e["player_name"], e["bet_type"],
                        "%s %g" % (s["side"], s["line"]), s["odds"], s["model_prob"],
                        e["p_over_raw"] if s["side"] == "Over" else e["p_under_raw"],
                        s["anchor_w"], e["projection"], e["minutes"], s["edge"])
    return rec


def generate_picks(date: str | None = None, props=None, *, target_date: str | None = None,
                   force: bool = False, **_ignored) -> dict:
    """Price the board, log every main line, and (on game day) lock the card. Returns
    {date, have_odds, count, picks, evaluated, priced, recorded, picked, skipped}."""
    today = target_date or date or et_today_str()
    now = time.time()
    if (not force and _last_gen["result"] is not None
            and _last_gen["date"] == today and (now - _last_gen["ts"]) < _GEN_TTL):
        return _last_gen["result"]
    with _GEN_LOCK:
        now = time.time()
        if (not force and _last_gen["result"] is not None
                and _last_gen["date"] == today and (now - _last_gen["ts"]) < _GEN_TTL):
            return _last_gen["result"]
        out = project_slate(today)
        cands = out.pop("_cands", {})
        try:
            out["recorded"] = _record_game_day(cands, et_now())
        except Exception as e:
            logger.warning("nba prop_picks: record failed: %s", e)
            out["recorded"] = 0
        out["picked"] = out["recorded"]
        logger.info("nba prop_picks v2: %d main lines, %d priced, %d candidates, %d recorded, "
                    "skipped %s", out.get("evaluated", 0), out.get("priced", 0),
                    sum(len(v) for v in cands.values()), out["recorded"], out.get("skipped"))
        _last_gen.update({"ts": now, "date": today, "result": out})
        return out


__all__ = ["project_slate", "generate_picks"]
