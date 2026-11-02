"""NFL player-props PAPER picker -- v3 projections since 2026-11-02 (Austin "Switch NFL"), v2 machinery.

v3 (engine.sports.nfl.prop_v3, MintyBot 2026-11-02): the projection + distribution come from the walk-forward
LightGBM research model (as-of player / team / opponent / QB / vacated-volume / game-line features; P(over) from
the neighbour pool of out-of-sample projections), anchored at a flat w=.35 (its research setting -- the weekly v2
refit stays off v3). Its research edge was an OPEN-price edge (+21.9% +/-7 on every candidate at the open, +11% at
the close, ~0 on the capped card), so: the card gate replays v3's FIRST-SEEN price per line, counts only lines first
seen after v3 went live (_V3_SINCE), and the card lock records the first time HR prices a game (no game-day hour).
_MODEL = "v2" reverts to engine.sports.nfl.prop_model in one line.

v2 notes (2026-10-26) follow.

Austin 2026-10-25: "Lets work on NFL player props now. We revitalised NHL player props
and they've been doing great ... We need to follow in the footsteps of that."

v1 (this module until today) priced only the YES side of HR's alt thresholds and kept,
per player-stat, whichever rung showed the BIGGEST edge -- a longshot hunt (pending
picks claimed +15pp on average, up to +88pp; settled ones: model 58-61% vs 47-55% hit,
closing-line value ~0). Its rows used the slate DATE as game_id, so the same bet was
re-recorded every day (619 pending rows = 268 bets) and settled off "the player's next
game after that date". Rushing yards never priced (HR's market is PROPALTRUYDS).

v2 mirrors engine.sports.nhl.prop_picks:
  * MAIN LINE ONLY: per (player, stat) the two-sided line closest to a coin flip
    (scrapers.hardrock_props.fetch_nfl_props merges HR's ALT + MULTILINE ladders).
  * Over AND Under from engine.sports.nfl.prop_model (projection + calibrated pool
    distribution), ANCHORED to the de-vigged line -- the model keeps 35% (NHL's value)
    until engine.sports.nfl.prop_refit's weekly fit on graded lines moves it per stat.
  * 40% floor on the anchored prob, juice wall / max odds / 4% edge as every picker.
  * Missing-information guard: a raw model prob outside 20-80% against a near-50/50
    main line almost always means the market knows something the logs don't (role
    change, injury, trade), so it is logged but never picked. Same for players listed
    likely-out by the injury layer and players with < 2 games this season.
  * Real ESPN game_id + game date, matchup "AWAY @ HOME", headshot + team logo.
  * Austin's volume rules (2026-04-29 / 05-20): at most ONE pick per game, the top
    THREE per slate date (edge x dynamic reliability).
  * CARD LOCK: picks are recorded on game day (from 09:00 ET, until each kickoff) and
    never voided or swapped, so the CLV read is honest -- re-ranking toward kickoff
    would silently drop every pick whose line moved against it.
  * Every main line evaluated, every run, is logged to prop_eval_log (first + last
    price, raw model prob, projection) so the model's disagreement with the market can
    be graded on hundreds of lines a week, not only the headline picks.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone

from . import prop_model as _pm
from . import prop_refit as _refit
from . import prop_v3 as _v3
from .player_props import _conn, _ensure, _norm, _initial_keys, _ESPN_HEAD
from ...player_props_db import insert_pick, _conn_for
from ..mlb.prop_picks import (PROP_MIN_EDGE_PCT, PROP_MAX_ODDS, PROP_JUICE_WALL,
                              TOP_N_PER_GAME, TOP_N_PER_SPORT, _confidence_for)
from ..._tz import et_now, et_today_str, _US_EASTERN

logger = logging.getLogger(__name__)

_PROP_ANCHOR_W = _refit.DEFAULT_W   # model's say vs the de-vigged market; weekly refit per stat
_PROP_MIN_PROB = 0.40        # never headline a side more likely to lose than win
_RAW_BAND = (0.20, 0.80)     # raw model prob vs a near-coin-flip main line; outside = skip
_MIN_SEASON_GAMES = 2        # games this season before a player is priced
_LIKELY_OUT = 0.50           # injury-layer miss probability that removes a player
_PICK_FROM_HOUR_ET = 9       # eval-log pick snapshot: game day from 09:00 ET (v2's gate price; v3 gates on first seen)
_SEASON = 2026
_MODEL = "v3"                # projections: "v3" = engine.sports.nfl.prop_v3, "v2" = engine.sports.nfl.prop_model
_V3_W = 0.35                 # v3 anchor weight (research V0); the v2 refit's per-stat weights were fit on v2 probs
_V3_SINCE = "2026-11-02T13:25:00Z"   # v3 live: the gate counts lines FIRST SEEN from here (v2-priced lines don't vouch)
_LEGACY_ID = "____-__-__"    # v1 rows: game_id = slate date (LIKE pattern)

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


def _upcoming_games() -> dict:
    """{(away_abbr, home_abbr): game} for NFL games from yesterday to +9 days (nfl.db,
    read-only), with the kickoff as an aware UTC datetime and the ET game date."""
    from engine.football._db import get_conn
    rows = get_conn("nfl").execute(
        "SELECT g.game_id, g.date, g.start_time, g.status, ht.id AS home_id, at.id AS away_id, "
        "ht.abbreviation AS home, at.abbreviation AS away, ht.logo_url AS home_logo, "
        "at.logo_url AS away_logo FROM games g JOIN teams ht ON ht.id = g.home_team_id "
        "JOIN teams at ON at.id = g.away_team_id "
        "WHERE g.date BETWEEN date('now', '-1 day') AND date('now', '+9 day')").fetchall()
    out = {}
    for r in rows:
        g = dict(r)
        try:
            ko = datetime.fromisoformat(str(g["start_time"]).replace("Z", "+00:00"))
            if ko.tzinfo is None:
                ko = ko.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        g["kickoff"] = ko
        g["et_date"] = _et(ko).strftime("%Y-%m-%d")
        out[(g["away"], g["home"])] = g
    return out


def _player_index() -> dict:
    """normalized name (+ 'f lastname') -> [candidates], latest team in ESPN codes."""
    c = _conn()
    _ensure(c)
    try:
        rows = c.execute(
            "SELECT espn_id, player_name, team, position, MAX(date) AS d, "
            "SUM(CASE WHEN season = ? THEN 1 ELSE 0 END) AS ns "
            "FROM player_prop_logs GROUP BY espn_id", (_SEASON,)).fetchall()
    finally:
        c.close()
    idx: dict[str, list] = {}
    for r in rows:
        cand = {"espn_id": int(r["espn_id"]), "name": r["player_name"],
                "team": _pm.NFLV_TO_ESPN.get(r["team"], r["team"]),
                "position": (r["position"] or "").upper(), "last_date": r["d"] or "",
                "games_season": int(r["ns"] or 0)}
        nm = _norm(r["player_name"])
        for key in {nm} | _initial_keys(nm):
            if key:
                idx.setdefault(key, []).append(cand)
    return idx


# Hard Rock nickname -> nflverse name (HR lists "Hollywood Brown"; nflverse "Marquise Brown").
_NAME_ALIASES = {"hollywood brown": "marquise brown"}


def _resolve(idx: dict, name: str, team: str | None, teams: tuple) -> dict | None:
    nm = _norm(name)
    nm = _NAME_ALIASES.get(nm, nm)
    cands = idx.get(nm) or []
    if not cands:
        for key in _initial_keys(nm):
            cands = idx.get(key) or []
    want = (team,) if team else teams
    cands = [c for c in cands if c["team"] in want]
    if not cands:
        return None
    cands.sort(key=lambda c: c["last_date"], reverse=True)
    return cands[0]


def _likely_out(game: dict) -> set:
    """Normalized names the injury layer has at >= _LIKELY_OUT to miss this game."""
    out = set()
    try:
        from engine.football import _injuries as _inj
        g = _inj.game_for("nfl", game["home"], game["away"])
        for tid in (game["home_id"], game["away_id"]):
            for row in _inj.team_injury_detail("nfl", tid, g) or []:
                if row and len(row) > 2 and (row[2] or 0) >= _LIKELY_OUT:
                    out.add(_norm(row[0]))
    except Exception as e:
        logger.debug("nfl prop_picks: injury lookup failed: %s", e)
    return out


def _main_lines(rows: list) -> list:
    """Per (player, stat) keep the two-sided line whose prices are closest to even."""
    best: dict = {}
    for r in rows:
        io, iu = _imp(r.get("over_odds")), _imp(r.get("under_odds"))
        if io is None or iu is None:
            continue
        k = (r["player_name"], r["bet_type"])
        bal = abs(io - iu)
        if k not in best or bal < best[k][0]:
            best[k] = (bal, r)
    return [v[1] for v in best.values()]


def _score(prop: dict, p_over: float, p_under: float, w: float | None = None) -> dict | None:
    from ...market_blend import anchor_to_market as _anchor
    oo, uo = prop["over_odds"], prop["under_odds"]
    if w is None:
        w = _V3_W if _MODEL == "v3" else _refit.weight_for(prop["bet_type"])
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


def _ensure_eval_log(c) -> None:
    c.execute("""CREATE TABLE IF NOT EXISTS prop_eval_log(
        game_id TEXT NOT NULL, game_date TEXT, espn_id INTEGER NOT NULL, player_name TEXT,
        bet_type TEXT NOT NULL, pg TEXT,
        first_seen TEXT, first_line REAL, first_over INTEGER, first_under INTEGER,
        first_p_over REAL, first_proj REAL,
        last_seen TEXT, last_line REAL, last_over INTEGER, last_under INTEGER,
        last_p_over REAL, last_proj REAL, n_seen INTEGER DEFAULT 0,
        actual REAL, graded_at TEXT,
        PRIMARY KEY (game_id, espn_id, bet_type))""")
    # Pick-time snapshot (2026-10-26): the price on game day once the card can lock
    # (>= 09:00 ET) -- what the weekly blend refit (prop_refit) fits against.
    have = {r[1] for r in c.execute("PRAGMA table_info(prop_eval_log)")}
    for col, typ in (("pick_seen", "TEXT"), ("pick_line", "REAL"), ("pick_over", "INTEGER"),
                     ("pick_under", "INTEGER"), ("pick_p_over", "REAL"),
                     ("dk_open", "REAL"), ("dk_first", "REAL"), ("dk_last", "REAL")):
        if col not in have:
            c.execute("ALTER TABLE prop_eval_log ADD COLUMN %s %s" % (col, typ))


# DraftKings reference line (MintyBot 2026-11-01, Austin "NFL player props is something we gotta work on"): ESPN's
# core API carries DK's NFL player-prop LINES (open + current target, no prices). Logged next to every HR line so
# "does HR's line lag DK's?" / "does the HR-vs-DK line gap predict the result?" can be measured.
_DK_TYPES = {"Total Receiving Yards (incl. overtime)": "rec_yds", "Total Receptions (incl. overtime)": "receptions",
             "Total Rushing Yards (incl. overtime)": "rush_yds", "Total Passing Yards (incl. overtime)": "pass_yds",
             "Total Carries (incl. overtime)": "carries", "Total Pass Completions (incl. overtime)": "completions",
             "Total Passing Attempts (incl. overtime)": "pass_att",
             "Total Rushing Plus Receiving Yards (incl. overtime)": "rr_yds"}
_DK_CACHE: dict = {}
_DK_TTL_S = 600


def _dk_lines(game_id) -> dict:
    """{(espn_athlete_id, stat): (open_line, current_line)} from DK via ESPN core; {} on any failure. Cached 10 min."""
    import json as _json, urllib.request as _ur
    gid = str(game_id)
    hit = _DK_CACHE.get(gid)
    if hit and time.time() - hit[0] < _DK_TTL_S:
        return hit[1]
    out: dict = {}
    try:
        page = 1
        while True:
            url = (f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{gid}/competitions/{gid}"
                   f"/odds/100/propBets?lang=en&region=us&limit=1000&page={page}")
            with _ur.urlopen(_ur.Request(url, headers={"User-Agent": "curl/8.4.0"}), timeout=15) as r:
                d = _json.loads(r.read()) or {}
            for it in d.get("items") or []:
                st = _DK_TYPES.get((it.get("type") or {}).get("name"))
                ath = ((it.get("athlete") or {}).get("$ref") or "").split("/athletes/")[-1].split("?")[0]
                cur = ((it.get("current") or {}).get("target") or {}).get("value")
                op = ((it.get("open") or {}).get("target") or {}).get("value")
                if st and ath.isdigit() and cur is not None and (int(ath), st) not in out:
                    out[(int(ath), st)] = (float(op) if op is not None else None, float(cur))
            if page >= int(d.get("pageCount") or 1):
                break
            page += 1
    except Exception as e:
        logger.debug("nfl dk lines %s failed: %s", gid, e)
    _DK_CACHE[gid] = (time.time(), out)
    return out


def _log_eval(rows: list) -> None:
    if not rows:
        return
    now_et = et_now()
    pick_day = now_et.strftime("%Y-%m-%d") if now_et.hour >= _PICK_FROM_HOUR_ET else None
    c = _conn()
    try:
        _ensure_eval_log(c)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        c.execute("BEGIN")
        for e in rows:
            v = (e["game_id"], e["date"], e["espn_id"], e["player_name"], e["bet_type"], e["pg"],
                 now, e["line"], e["over_odds"], e["under_odds"], e["p_over_raw"], e["projection"])
            c.execute(
                "INSERT INTO prop_eval_log(game_id, game_date, espn_id, player_name, bet_type, pg, "
                "first_seen, first_line, first_over, first_under, first_p_over, first_proj, "
                "last_seen, last_line, last_over, last_under, last_p_over, last_proj, n_seen) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1) "
                "ON CONFLICT(game_id, espn_id, bet_type) DO UPDATE SET "
                "last_seen = excluded.last_seen, last_line = excluded.last_line, "
                "last_over = excluded.last_over, last_under = excluded.last_under, "
                "last_p_over = excluded.last_p_over, last_proj = excluded.last_proj, "
                "n_seen = prop_eval_log.n_seen + 1",
                v + v[6:])
            dk = _dk_lines(e["game_id"]).get((int(e["espn_id"]), e.get("stat_key")))
            if dk:
                c.execute("UPDATE prop_eval_log SET dk_open = COALESCE(dk_open, ?), dk_first = COALESCE(dk_first, ?), "
                          "dk_last = ? WHERE game_id = ? AND espn_id = ? AND bet_type = ?",
                          (dk[0], dk[1], dk[1], e["game_id"], e["espn_id"], e["bet_type"]))
            if e["date"] == pick_day:
                c.execute(
                    "UPDATE prop_eval_log SET pick_seen = ?, pick_line = ?, pick_over = ?, "
                    "pick_under = ?, pick_p_over = ? WHERE game_id = ? AND espn_id = ? "
                    "AND bet_type = ? AND pick_seen IS NULL",
                    v[6:11] + (v[0], v[2], v[4]))
        c.execute("COMMIT")
    except Exception as e:
        logger.warning("nfl prop_picks: eval log write failed: %s", e)
    finally:
        c.close()


def grade_eval_log(limit: int = 3000) -> int:
    """Fill ``actual`` on prop_eval_log rows whose game is in the logs (same settlement
    read as the picks). Returns rows graded."""
    c = _conn()
    try:
        _ensure_eval_log(c)
        rows = c.execute(
            "SELECT game_id, espn_id, bet_type FROM prop_eval_log WHERE actual IS NULL "
            "AND graded_at IS NULL AND game_date < ? LIMIT ?", (et_today_str(), int(limit))).fetchall()
    finally:
        c.close()
    done = []
    for r in rows:
        stat = _pm.MARKETS.get(r["bet_type"], (None,))[0]
        if not stat:
            continue
        status, val = _pm.settle_lookup(int(r["espn_id"]), r["game_id"], stat)
        if status == "ok":
            done.append((val, r["game_id"], r["espn_id"], r["bet_type"]))
        elif status == "dnp":
            done.append((None, r["game_id"], r["espn_id"], r["bet_type"]))
    if done:
        c = _conn()
        try:
            stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            c.executemany("UPDATE prop_eval_log SET actual = ?, graded_at = '" + stamp + "' "
                          "WHERE game_id = ? AND espn_id = ? AND bet_type = ?", done)
        finally:
            c.close()
    return len(done)


# CARD GATE (MintyBot 2026-11-01, Austin: calibration "NFL too"; he bets the card for real on Hard Rock).
# Walk-forward on 9,749 historical main lines (ESPN BET 2025 wk1-13 open+close prices, DK 2026 wk1-8 lines at
# -115) the current rule's card went 39-36, ROI +1.2% +/- 11.4 at the close, claimed 58% and won 52% (winner's
# curse ~6pp); every qualifying candidate (n=521) +0.4% +/- 4.2; Overs -14%; blanket Overs at real prices -13.5%
# (n=5,353). No variant (Platt per stat, refit w, stricter floors, more shrink, proven cells at the POTD bar,
# Unders/receptions only) was positive in both halves at the close. So a (bet_type, side) cell reaches the card
# only once the rule's own shadow record on graded HR lines (prop_eval_log, pick-time price) clears
# n >= _CELL_MIN_N and win% - break-even > _CELL_MIN_SE x SE. That bar never opened in the 17-week history,
# while the POTD's looser one (n>=20, 0.5 SE) opened on noise and lost 22%. Until a cell proves out, NFL props
# record NO card picks; every line is still priced and logged, so the shadow record keeps growing.
_CELL_MIN_N = 100
_CELL_MIN_SE = 1.5
_CELL_TTL_S = 1800
_CELL_CACHE: dict = {"ts": 0.0, "cells": None}


def shadow_cells(force: bool = False) -> dict:
    """{"<bet_type> <side>": {n, wins, win_pct, be_pct, margin_pp, se_pp, roi, proven}}: every graded main line in
    prop_eval_log the live rule would have picked (raw band + _score at the pick-time snapshot, else first seen),
    flat 1u at that price. Pushes skipped. Cached 30 min."""
    if not force and _CELL_CACHE["cells"] is not None and time.time() - _CELL_CACHE["ts"] < _CELL_TTL_S:
        return _CELL_CACHE["cells"]
    acc: dict = {}
    c = _conn()
    try:
        _ensure_eval_log(c)
        if _MODEL == "v3":    # v3's edge is at the open: its first-seen price, v3-priced lines only
            rows = c.execute(
                "SELECT bet_type, actual, first_line AS l, first_over AS oo, first_under AS uo, first_p_over AS q "
                "FROM prop_eval_log WHERE actual IS NOT NULL AND first_seen >= ?", (_V3_SINCE,)).fetchall()
        else:
            rows = c.execute(
                "SELECT bet_type, actual, "
                "CASE WHEN pick_seen IS NOT NULL THEN pick_line ELSE first_line END AS l, "
                "CASE WHEN pick_seen IS NOT NULL THEN pick_over ELSE first_over END AS oo, "
                "CASE WHEN pick_seen IS NOT NULL THEN pick_under ELSE first_under END AS uo, "
                "CASE WHEN pick_seen IS NOT NULL THEN pick_p_over ELSE first_p_over END AS q "
                "FROM prop_eval_log WHERE actual IS NOT NULL").fetchall()
    finally:
        c.close()
    for r in rows:
        if None in (r["l"], r["oo"], r["uo"], r["q"]) or r["actual"] == r["l"]:
            continue
        q = float(r["q"])
        if not (_RAW_BAND[0] <= q <= _RAW_BAND[1]):
            continue
        sc = _score({"over_odds": r["oo"], "under_odds": r["uo"], "line": r["l"], "bet_type": r["bet_type"]},
                    q, 1.0 - q)
        if not sc:
            continue
        won = (r["actual"] > r["l"]) if sc["side"] == "Over" else (r["actual"] < r["l"])
        o = float(sc["odds"])
        a = acc.setdefault(f"{r['bet_type']} {sc['side']}", [0, 0, 0.0, 0.0])
        a[0] += 1
        a[1] += 1 if won else 0
        a[2] += _imp(o)
        a[3] += ((o / 100.0) if o > 0 else (100.0 / -o)) if won else -1.0
    cells = {}
    for k, (n, w, be_sum, pl) in acc.items():
        be = be_sum / n
        se = (be * (1 - be) / n) ** 0.5
        margin = w / n - be
        cells[k] = {"n": n, "wins": w, "win_pct": round(100 * w / n, 1), "be_pct": round(100 * be, 1),
                    "margin_pp": round(100 * margin, 1), "se_pp": round(100 * se, 1), "roi": round(100 * pl / n, 1),
                    "proven": n >= _CELL_MIN_N and margin > _CELL_MIN_SE * se}
    _CELL_CACHE.update(ts=time.time(), cells=cells)
    return cells


def _cell_proven(bet_type: str, side: str) -> bool:
    try:
        return bool((shadow_cells().get(f"{bet_type} {side}") or {}).get("proven"))
    except Exception as e:
        logger.warning("nfl prop_picks: shadow cell read failed (gate stays closed): %s", e)
        return False


def card_gate() -> dict:
    cells = shadow_cells()
    v3 = _MODEL == "v3"
    return {"rule": f"card pick only from a cell whose shadow record (graded HR main lines the rule would pick"
                    f"{', v3 at its first-seen price' if v3 else ''}) has n>={_CELL_MIN_N} and win% - break-even > "
                    f"{_CELL_MIN_SE} SE",
            "model": _MODEL, "since": _V3_SINCE[:10] if v3 else None, "min_n": _CELL_MIN_N, "min_se": _CELL_MIN_SE,
            "proven": sorted(k for k, v in cells.items() if v["proven"]), "cells": cells}


def _rank(entry: dict) -> float:
    try:
        from ...dynamic_reliability import get_reliability
        rel = float(get_reliability("nfl", entry["bet_type"]))
    except Exception:
        rel = 1.0
    return float(entry["scored"]["edge"]) * rel


def project_slate(target_date: str | None = None) -> dict:
    """Price every main line on HR's NFL board for upcoming games. Returns the board
    ({date, have_odds, count, picks, evaluated, skipped}); ``picks`` holds every priced
    main line (display), ``_cands`` the qualifying candidates keyed by game."""
    today = target_date or et_today_str()
    from .player_props import _ensure_refresh
    _ensure_refresh()
    board = {}
    try:
        from scrapers.hardrock_props import fetch_nfl_props
        board = fetch_nfl_props() or {}
    except Exception as e:
        logger.warning("nfl prop_picks: HR fetch failed: %s", e)
    games = _upcoming_games()
    idx = _player_index()
    now = datetime.now(timezone.utc)
    skipped = {"no_game": 0, "started": 0, "no_player": 0, "few_games": 0, "likely_out": 0,
               "no_projection": 0, "raw_band": 0, "no_edge": 0}
    display, eval_rows, cands = [], [], {}
    evaluated = 0
    for mk, rows in board.items():
        away, home = mk.split("@", 1)
        g = games.get((away, home))
        if not g:
            skipped["no_game"] += len(rows)
            continue
        if g["kickoff"] <= now or (g["status"] or "").lower() in ("final", "live", "in", "postponed"):
            skipped["started"] += len(rows)
            continue
        out_names = _likely_out(g)
        matchup = f"{away} @ {home}"
        for prop in _main_lines(rows):
            evaluated += 1
            team = home if prop.get("team_side") == "home" else (away if prop.get("team_side") == "away" else None)
            cand = _resolve(idx, prop["player_name"], team, (away, home))
            if not cand:
                skipped["no_player"] += 1
                continue
            stat = _pm.MARKETS[prop["bet_type"]][0]
            opp = away if cand["team"] == home else home
            if _MODEL == "v3":
                v = _v3.prediction(g["game_id"], cand["espn_id"], stat)
                pj = v and {"pg": v["pgp"], "proj": round(float(v["pred"]), 2), "matchup": None,
                            "games": v["games"], "games_season": v["games_season"]}
                pr = v and _v3.probs(stat, v["pgp"], float(v["pred"]), float(prop["line"]), v["date"], v["season"])
            else:
                pj = _pm.project(cand["espn_id"], stat, opp, g["et_date"], _SEASON)
                pr = pj and _pm.probs(stat, pj["pg"], pj["proj"], float(prop["line"]), _SEASON)
            if not pj or not pr:
                skipped["no_projection"] += 1
                continue
            p_over, p_under, pool_n = pr
            entry = {
                "game_id": str(g["game_id"]), "date": g["et_date"], "matchup": matchup,
                "espn_id": cand["espn_id"], "player_id": cand["espn_id"],
                "player_name": prop["player_name"], "position": cand["position"],
                "team": cand["team"], "bet_type": prop["bet_type"], "market": prop["bet_type"],
                "stat_key": stat, "pg": pj["pg"], "line": float(prop["line"]),
                "over_odds": prop["over_odds"], "under_odds": prop["under_odds"],
                "projection": pj["proj"], "matchup_mult": pj["matchup"], "games": pj["games"],
                "games_season": pj["games_season"], "pool_n": pool_n,
                "p_over_raw": round(p_over, 4), "p_under_raw": round(p_under, 4),
                "headshot": _ESPN_HEAD.format(cand["espn_id"]),
                "team_logo": g["home_logo"] if cand["team"] == home else g["away_logo"],
                "kickoff": g["kickoff"].isoformat(),
            }
            eval_rows.append(entry)
            if pj["games_season"] < _MIN_SEASON_GAMES:
                skipped["few_games"] += 1
                continue
            if _norm(prop["player_name"]) in out_names:
                skipped["likely_out"] += 1
                continue
            if not (_RAW_BAND[0] <= p_over <= _RAW_BAND[1]):
                skipped["raw_band"] += 1
                continue
            scored = _score(prop, p_over, p_under)
            if not scored:
                skipped["no_edge"] += 1
                display.append(dict(entry, side=None, model_prob=None, odds=None, edge=None, plus_ev=False))
                continue
            entry["scored"] = scored
            cands.setdefault(entry["game_id"], []).append(entry)
            display.append(dict(entry, side=scored["side"], model_prob=scored["model_prob"],
                                odds=scored["odds"], implied_prob=scored["implied_prob"],
                                edge=scored["edge"], plus_ev=True,
                                card_eligible=_cell_proven(prop["bet_type"], scored["side"])))
    _log_eval(eval_rows)
    display.sort(key=lambda x: (x["edge"] is not None, x["edge"] or -999), reverse=True)
    for d in display:
        d.pop("scored", None)
    try:
        gate = card_gate()
    except Exception as e:
        gate = {"error": str(e)}
    return {"date": today, "have_odds": bool(board), "count": len(display), "picks": display,
            "evaluated": evaluated, "priced": len(eval_rows), "skipped": skipped, "card_gate": gate, "_cands": cands}


def _record_game_day(cands: dict, now_et: datetime) -> int:
    """Card lock: on each game date (from 09:00 ET) fill up to TOP_N_PER_SPORT picks, at
    most TOP_N_PER_GAME per game, best edge x reliability first. Existing picks for the
    date count against the caps and are never voided or swapped."""
    today = now_et.strftime("%Y-%m-%d")
    if _MODEL == "v3":      # first time HR prices the game, any upcoming date (v3's edge is the early-week price)
        dates = sorted({cl[0]["date"] for cl in cands.values() if cl and cl[0]["date"] >= today})
        return sum(_record_date(cands, d) for d in dates)
    if now_et.hour < _PICK_FROM_HOUR_ET:
        return 0
    return _record_date(cands, today)


def _record_date(cands: dict, day: str) -> int:
    todays = {gid: cl for gid, cl in cands.items() if cl and cl[0]["date"] == day}
    if not todays:
        return 0
    today = day
    conn = _conn_for("nfl")
    have = conn.execute(
        "SELECT game_id, COUNT(*) AS n FROM player_props_picks WHERE date = ? "
        "AND game_id NOT LIKE ? GROUP BY game_id", (today, _LEGACY_ID)).fetchall()
    per_game = {str(r["game_id"]): int(r["n"]) for r in have}
    slots = TOP_N_PER_SPORT - sum(per_game.values())
    if slots <= 0:
        return 0
    winners, gated = [], 0
    for gid, cl in todays.items():
        if per_game.get(gid, 0) >= TOP_N_PER_GAME:
            continue
        ok = [e for e in cl if _cell_proven(e["bet_type"], e["scored"]["side"])]   # card gate (see shadow_cells)
        gated += len(cl) - len(ok)
        if ok:
            winners.append(max(ok, key=_rank))
    if gated:
        logger.info("nfl prop_picks: card gate held back %d candidate(s) on %s (proven cells: %s)",
                    gated, today, card_gate()["proven"] or "none")
    winners.sort(key=_rank, reverse=True)
    rec = 0
    for e in winners[:slots]:
        s = e["scored"]
        pid = insert_pick(
            "nfl", game_id=e["game_id"], date=e["date"], matchup=e["matchup"],
            player_id=int(e["espn_id"]), player_name=e["player_name"], bet_type=e["bet_type"],
            pick="%s %g" % (s["side"], s["line"]), line=s["line"], side=s["side"],
            model_prob=s["model_prob"], edge=s["edge"], odds=s["odds"],
            confidence=_confidence_for(s["edge"]), headshot=e["headshot"], team_logo=e["team_logo"])
        if pid:
            rec += 1
            logger.info("nfl prop_picks: LOCKED %s %s %s %s @%s (model %.3f raw %.3f w %.2f proj %.1f, edge %+.1f)",
                        e["matchup"], e["player_name"], e["bet_type"], "%s %g" % (s["side"], s["line"]),
                        s["odds"], s["model_prob"],
                        e["p_over_raw"] if s["side"] == "Over" else e["p_under_raw"],
                        s["anchor_w"], e["projection"], s["edge"])
    return rec


def generate_picks(target_date: str | None = None, force: bool = False) -> dict:
    """Price the board, log every main line, and (on game day) lock the card. Same
    return shape as v1: ``{date, have_odds, count, picks, recorded, ...}``."""
    today = target_date or et_today_str()
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
            logger.warning("nfl prop_picks: record failed: %s", e)
            out["recorded"] = 0
        logger.info("nfl prop_picks (%s): %d main lines, %d priced, %d candidates, %d recorded, skipped %s",
                    _MODEL, out.get("evaluated", 0), out.get("priced", 0),
                    sum(len(v) for v in cands.values()), out["recorded"], out.get("skipped"))
        _last_gen.update({"ts": now, "date": today, "result": out})
        return out


__all__ = ["project_slate", "generate_picks", "grade_eval_log", "shadow_cells", "card_gate"]
