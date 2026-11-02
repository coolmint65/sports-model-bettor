"""NFL player-prop model v2: projection + calibrated distribution (MintyBot 2026-10-26).

Austin 2026-10-25: "Lets work on NFL player props now ... follow in the footsteps of
[NHL]". The v1 model fit each player's own last 3-8 games of THIS season (Normal for
yards, Poisson for receptions). Walk-forward on nflverse 2024-26 (test = 2025-26, the
rows v1 could price), mean absolute error of the projection:

    stat          v1 last-8 mean   v2
    pass_yds          65.2         62.5
    rush_yds          23.3         23.0
    rec_yds           23.7         22.9
    receptions        1.70         1.65

and v1's per-player Normal/Poisson put P(over a line at the projection) 5-10pp too
high for RB/WR/TE (rec_yds/WR Brier .259 vs .244) -- where its fat Over edges came from.

v2:
  * Opportunity x efficiency. Opportunities (pass attempts / carries / targets) are an
    exponentially weighted mean of the player's last 20 games across seasons
    (half-life 6 games, prior-season games x0.7); efficiency (yards per opportunity,
    catch rate) is shrunk toward the position norm by K_EFF opportunities.
  * Matchup (the NHL layer, kept only where it validated): the opponent's allowance to
    the player's position group vs the league, shrunk g/(g+C), clamped [0.80, 1.25].
    Passing uses yards allowed per game (C=6; MAE -2.2%). Rushing/receiving use yards
    (or catches) allowed per opportunity with heavy shrinkage (C=20, last season at
    0.5): a near-zero but non-negative effect. Per-game volume allowed made rushing and
    receiving WORSE out of sample, so it is not used for them.
  * Distribution: P(stat > line) is read off how players with the SAME projection
    actually finished -- the (stat, position group) pool of walk-forward projections,
    nearest >= POOL_MIN, recency weighted. Calibrated by construction (zero-catch games,
    skew and blowups included) instead of a per-player fit on a handful of games. 2024-25
    pool -> 2026 test: predicted P(over the projection) within 1-4pp of the hit rate.

Logs come from ``player_prop_logs`` (engine.sports.nfl.player_props.refresh: nflverse
PBP 2024-26 + snap counts); team codes there are nflverse (LA, WAS), ESPN uses LAR, WSH.
"""
from __future__ import annotations

import bisect
import math
import logging
import threading
import time
from collections import defaultdict
from itertools import accumulate

logger = logging.getLogger(__name__)

# HR bet_type -> (stat column, opportunity column)
MARKETS = {
    "Passing Yards":   ("pass_yds", "pass_att"),
    "Rushing Yards":   ("rush_yds", "carries"),
    "Receiving Yards": ("rec_yds", "targets"),
    "Receptions":      ("receptions", "targets"),
    # added 2026-11-01 (HR posts these two-sided; volume-only, no efficiency to estimate)
    "Rushing Attempts": ("carries", "carries"),
    "Pass Attempts":    ("pass_att", "pass_att"),
    "Completions":      ("completions", "pass_att"),
    "Rush+Rec Yards":   ("rr_yds", "carries"),     # combo: rush_yds + rec_yds projections (RBs), see COMBOS
}
_OPP_COL = {s: o for s, o in MARKETS.values()}
# Minimum projected opportunities per game for a (stat, position group) to be priced.
ELIGIBLE = {
    "pass_yds":   {"QB": 20.0},
    "rush_yds":   {"RB": 6.0, "QB": 3.0},
    "rec_yds":    {"WR": 3.0, "TE": 3.0, "RB": 2.5},
    "receptions": {"WR": 3.0, "TE": 3.0, "RB": 2.5},
    "carries":    {"RB": 6.0, "QB": 3.0},
    "pass_att":   {"QB": 20.0},
    "completions": {"QB": 20.0},
}
LEAGUE_EFF = {
    "pass_yds":   {"QB": 6.9},
    "rush_yds":   {"RB": 4.3, "QB": 5.0},
    "rec_yds":    {"WR": 8.0, "TE": 7.0, "RB": 6.0},
    "receptions": {"WR": 0.63, "TE": 0.70, "RB": 0.76},
    "carries":    {"RB": 1.0, "QB": 1.0},
    "pass_att":   {"QB": 1.0},
    "completions": {"QB": 0.64},
}
K_EFF = {"pass_yds": 150.0, "rush_yds": 60.0, "rec_yds": 40.0, "receptions": 40.0,
         "carries": 1.0, "pass_att": 1.0, "completions": 150.0}
HALF_LIFE = 6.0
PRIOR_SEASON_W = 0.7
MAX_GAMES = 20
MATCHUP = {"pass_yds": ("vol", 6.0, 0.0), "rush_yds": ("eff", 20.0, 0.5),
           "rec_yds": ("eff", 20.0, 0.5), "receptions": ("eff", 20.0, 0.5),
           "carries": ("eff", 20.0, 0.5), "pass_att": ("eff", 20.0, 0.5), "completions": ("eff", 20.0, 0.5)}
MM_CLAMP = (0.80, 1.25)
POOL_MIN = 250
POOL_SEASON_W = (1.0, 1.0, 0.5)     # this season, last season, two back
_POS_GROUP = {"QB": "QB", "RB": "RB", "FB": "RB", "WR": "WR", "TE": "TE"}
_STAT_COLS = ("pass_yds", "pass_att", "completions", "rush_yds", "carries", "rec_yds", "receptions", "targets")
ESPN_TO_NFLV = {"LAR": "LA", "WSH": "WAS"}
NFLV_TO_ESPN = {v: k for k, v in ESPN_TO_NFLV.items()}
_TTL = 6 * 3600

_STATE: dict = {"key": None, "ts": 0.0, "data": None}
_LOCK = threading.Lock()


class _Cum:
    """Per-game sums for one (defense, position group, season) or (position group,
    season) series, as prefix sums over date so 'everything before D' is a bisect."""
    __slots__ = ("dates", "cum")

    def __init__(self, items):
        items.sort(key=lambda x: x[0])
        self.dates = [d for d, _ in items]
        cols = _STAT_COLS + ("n",)
        self.cum = {c: list(accumulate((s.get(c, 0.0) for _, s in items), initial=0.0)) for c in cols}

    def before(self, date):
        i = bisect.bisect_left(self.dates, date)
        return {c: a[i] for c, a in self.cum.items()}

    def total(self):
        return {c: a[-1] for c, a in self.cum.items()}


def _matchup(data, stat, pg, opp, season, date) -> float:
    mode, C, carry = MATCHUP[stat]
    den_col = _OPP_COL[stat] if mode == "eff" else "n"

    def acc(cur, prev):
        a = cur.before(date) if cur else None
        b = prev.total() if (prev is not None and carry > 0) else None
        num = (a[stat] if a else 0.0) + (carry * b[stat] if b else 0.0)
        den = (a[den_col] if a else 0.0) + (carry * b[den_col] if b else 0.0)
        g = (a["n"] if a else 0.0) + (carry * b["n"] if b else 0.0)
        return num, den, g

    num, den, g = acc(data["def"].get((opp, pg, season)), data["def"].get((opp, pg, season - 1)))
    lnum, lden, lg = acc(data["lg"].get((pg, season)), data["lg"].get((pg, season - 1)))
    if g <= 0 or den <= 0 or lden <= 0 or lnum <= 0 or lg < 20:
        return 1.0
    raw = (num / den) / (lnum / lden)
    k = g / (g + C)
    return float(min(MM_CLAMP[1], max(MM_CLAMP[0], 1.0 + (raw - 1.0) * k)))


def _base(prior, stat, pg, season):
    """(projected opportunities, shrunk efficiency) from up to MAX_GAMES prior games."""
    ocol = _OPP_COL[stat]
    n = len(prior)
    num_o = num_s = sw = 0.0
    for i, g in enumerate(prior):
        w = 0.5 ** ((n - 1 - i) / HALF_LIFE) * (PRIOR_SEASON_W if g["season"] < season else 1.0)
        sw += w
        num_o += w * (g[ocol] or 0.0)
        num_s += w * (g[stat] or 0.0)
    if sw <= 0:
        return None, None
    eff = (num_s + K_EFF[stat] * LEAGUE_EFF[stat][pg]) / (num_o + K_EFF[stat])
    return num_o / sw, eff


# Usage trend (MintyBot 2026-11-01, Austin "NFL player props is something we gotta work on"): a player's snap share
# over his last 2 games vs his EWMA share (half-life 6) scales his projected opportunities: exp(TREND_B * log(last2/ewma)).
# Walk-forward 2024-25 fit -> 2026 test: carries RMSE 5.04 -> 4.92, rush-yds MAE 22.97 -> 22.67; targets RMSE 2.92 -> 2.89,
# rec-yds MAE 23.76 -> 23.67. Game script (implied team total / spread vs his usual) added ~0 and is NOT used.
# Research: Beelink C:/Users/Austin/nfl_usage*.py.
TREND_B = {"rec_yds": 0.3, "receptions": 0.3, "rush_yds": 0.2, "pass_yds": 0.0, "carries": 0.2}
TREND_HL = 6.0


def _trend_mult(prior, stat) -> float:
    b = TREND_B.get(stat, 0.0)
    sh = [g["share"] for g in prior if g.get("share") is not None]
    if not b or len(sh) < 2:
        return 1.0
    w = [0.5 ** ((len(sh) - 1 - i) / TREND_HL) for i in range(len(sh))]
    ew = sum(x * y for x, y in zip(sh, w)) / sum(w)
    last2 = (sh[-1] + sh[-2]) / 2
    return float(math.exp(b * math.log(max(last2, 0.05) / max(ew, 0.05))))


# Combo stats (2026-11-01): projected as the sum of the component projections; the pool pairs that sum with the
# actual sum, so the distribution is calibrated the same way. Only for position groups listed.
COMBOS = {"rr_yds": {"parts": ("rush_yds", "rec_yds"), "pgs": ("RB",), "gate": "rush_yds"}}
_SQL_EXPR = {"rr_yds": "(COALESCE(rush_yds, 0) + COALESCE(rec_yds, 0))"}


def _proj_one(data, prior, stat, pg, opp, season, date, floor=True):
    el = ELIGIBLE.get(stat, {}).get(pg)
    if el is None:
        return None
    o_hat, eff = _base(prior, stat, pg, season)
    if o_hat is None or (floor and o_hat < el):
        return None
    return o_hat * eff * _matchup(data, stat, pg, opp, season, date) * _trend_mult(prior, stat)


def _proj_combo(data, prior, stat, pg, opp, season, date):
    cfg = COMBOS[stat]
    if pg not in cfg["pgs"]:
        return None
    if _proj_one(data, prior, cfg["gate"], pg, opp, season, date, floor=True) is None:
        return None
    parts = [_proj_one(data, prior, x, pg, opp, season, date, floor=False) for x in cfg["parts"]]
    return None if any(v is None for v in parts) else sum(parts)


def _build(rows) -> dict:
    players = defaultdict(list)
    dsum = defaultdict(lambda: defaultdict(float))
    for r in rows:
        pg = _POS_GROUP.get((r["position"] or "").upper())
        if not pg or r["season"] is None:
            continue
        r["pg"] = pg
        players[int(r["espn_id"])].append(r)
        s = dsum[(r["opp"], pg, int(r["season"]), r["game_id"], r["date"])]
        for c in _STAT_COLS:
            s[c] += r[c] or 0.0
    def_items, lg_items = defaultdict(list), defaultdict(list)
    for (opp, pg, season, _gid, date), s in dsum.items():
        s = dict(s)
        s["n"] = 1.0
        def_items[(opp, pg, season)].append((date, s))
        lg_items[(pg, season)].append((date, s))
    data = {"players": players,
            "def": {k: _Cum(v) for k, v in def_items.items()},
            "lg": {k: _Cum(v) for k, v in lg_items.items()}}
    team_snaps = defaultdict(float)
    for games in players.values():
        for g in games:
            k = (g["game_id"], g["team"])
            team_snaps[k] = max(team_snaps[k], float(g.get("offense_snaps") or 0))
    for games in players.values():
        games.sort(key=lambda g: (g["date"], g["game_id"]))
        for g in games:
            ts = team_snaps.get((g["game_id"], g["team"])) or 0
            g["share"] = (float(g.get("offense_snaps") or 0) / ts) if ts and g.get("offense_snaps") is not None else None
    # Walk-forward pools: every historical player-game's projection (prior games only)
    # next to what he actually did.
    tmp = defaultdict(list)
    for games in players.values():
        for j in range(1, len(games)):
            cur = games[j]
            prior = games[max(0, j - MAX_GAMES):j]
            season = int(cur["season"])
            for stat, el in ELIGIBLE.items():
                floor = el.get(cur["pg"])
                if floor is None:
                    continue
                o_hat, eff = _base(prior, stat, cur["pg"], season)
                if o_hat is None or o_hat < floor:
                    continue
                proj = o_hat * eff * _matchup(data, stat, cur["pg"], cur["opp"], season, cur["date"]) * _trend_mult(prior, stat)
                tmp[(stat, cur["pg"])].append((proj, float(cur[stat] or 0.0), season))
            for stat in COMBOS:
                proj = _proj_combo(data, prior, stat, cur["pg"], cur["opp"], season, cur["date"])
                if proj is not None:
                    tmp[(stat, cur["pg"])].append((proj, float(cur.get(stat) or 0.0), season))
    pools = {}
    for k, v in tmp.items():
        v.sort(key=lambda x: x[0])
        pools[k] = ([x[0] for x in v], [x[1] for x in v], [x[2] for x in v])
    data["pools"] = pools
    return data


def _load() -> dict | None:
    from .player_props import _conn, _ensure, last_refresh
    key = last_refresh()
    st = _STATE
    if st["data"] is not None and st["key"] == key and time.time() - st["ts"] < _TTL:
        return st["data"]
    with _LOCK:
        if st["data"] is not None and st["key"] == key and time.time() - st["ts"] < _TTL:
            return st["data"]
        t0 = time.time()
        c = _conn()
        _ensure(c)
        rows = [dict(r) for r in c.execute(
            "SELECT espn_id, game_id, date, player_name, team, position, season, opp, "
            "pass_yds, pass_att, completions, rush_yds, carries, rec_yds, receptions, targets, offense_snaps "
            "FROM player_prop_logs WHERE season IS NOT NULL").fetchall()]
        c.close()
        if not rows:
            return None
        for r in rows:
            r["rr_yds"] = float(r.get("rush_yds") or 0) + float(r.get("rec_yds") or 0)
        data = _build(rows)
        st.update(key=key, ts=time.time(), data=data)
        logger.info("nfl prop_model: %d player-games, pools %s (%.1fs)", len(rows),
                    {f"{s}/{p}": len(v[0]) for (s, p), v in data["pools"].items()}, time.time() - t0)
        return data


def project(espn_id: int, stat: str, opp_espn: str, date: str, season: int) -> dict | None:
    """Projection for one upcoming player-game (prior games only). None when the
    player has no history or doesn't meet the opportunity floor for this stat."""
    data = _load()
    if not data:
        return None
    games = data["players"].get(int(espn_id))
    if not games:
        return None
    prior = [g for g in games if g["date"] < date][-MAX_GAMES:]
    if not prior:
        return None
    pg = prior[-1]["pg"]
    if stat in COMBOS:
        opp = ESPN_TO_NFLV.get(opp_espn, opp_espn)
        pv = _proj_combo(data, prior, stat, pg, opp, season, date)
        if pv is None:
            return None
        return {"proj": round(pv, 2), "base": round(pv, 2), "matchup": 1.0, "trend": 1.0, "opportunities": None,
                "efficiency": None, "pg": pg, "games": len(prior),
                "games_season": sum(1 for g in prior if g["season"] == season),
                "team": NFLV_TO_ESPN.get(prior[-1]["team"], prior[-1]["team"]), "last_date": prior[-1]["date"]}
    floor = ELIGIBLE.get(stat, {}).get(pg)
    if floor is None:
        return None
    o_hat, eff = _base(prior, stat, pg, season)
    if o_hat is None or o_hat < floor:
        return None
    opp = ESPN_TO_NFLV.get(opp_espn, opp_espn)
    mm = _matchup(data, stat, pg, opp, season, date)
    tm = _trend_mult(prior, stat)
    return {"proj": round(o_hat * eff * mm * tm, 2), "base": round(o_hat * eff, 2), "matchup": round(mm, 3), "trend": round(tm, 3),
            "opportunities": round(o_hat, 2), "efficiency": round(eff, 4), "pg": pg,
            "games": len(prior), "games_season": sum(1 for g in prior if g["season"] == season),
            "team": NFLV_TO_ESPN.get(prior[-1]["team"], prior[-1]["team"]), "last_date": prior[-1]["date"]}


def probs(stat: str, pg: str, proj: float, line: float, season: int):
    """(P(stat > line), P(stat < line), pool size) from the nearest walk-forward
    projections in the (stat, position group) pool; None if the pool is thin."""
    data = _load()
    pool = (data or {}).get("pools", {}).get((stat, pg))
    if not pool:
        return None
    cs, acts, seasons = pool
    rad = max(0.5, 0.05 * proj)
    while True:
        lo = bisect.bisect_left(cs, proj - rad)
        hi = bisect.bisect_right(cs, proj + rad)
        if hi - lo >= POOL_MIN or rad > 5 * proj + 50:
            break
        rad *= 1.25
    if hi - lo < 30:
        return None
    wsum = over = under = 0.0
    for i in range(lo, hi):
        back = season - seasons[i]
        w = POOL_SEASON_W[back] if 0 <= back < len(POOL_SEASON_W) else 0.0
        if w <= 0:
            continue
        wsum += w
        if acts[i] > line:
            over += w
        elif acts[i] < line:
            under += w
    if wsum <= 0:
        return None
    return over / wsum, under / wsum, hi - lo


def _game(game_id: str):
    from engine.football._db import get_conn
    return get_conn("nfl").execute(
        "SELECT g.game_id, g.date, g.start_time, g.status, ht.abbreviation AS home, "
        "at.abbreviation AS away FROM games g JOIN teams ht ON ht.id = g.home_team_id "
        "JOIN teams at ON at.id = g.away_team_id WHERE g.game_id = ?", (str(game_id),)).fetchone()


# ESPN box-score fallback (MintyBot 2026-10-27, Austin "Add in the ESPN fall back"): nflverse
# posts a game's play-by-play hours after the final (Monday night lands overnight), so props
# waited on it. ESPN's summary has the official box line minutes after the whistle.
_ESPN_STAT = {
    "pass_yds":   ("passing", "passingYards"),
    "rush_yds":   ("rushing", "rushingYards"),
    "rec_yds":    ("receiving", "receivingYards"),
    "receptions": ("receiving", "receptions"),
    "carries":    ("rushing", "rushingAttempts"),
}
_ESPN_BOX: dict = {}          # game_id -> (fetched_at, {espn_id: {category: {key: cell}}} | None)
_ESPN_RETRY_S = 600


def _espn_box(game_id: str):
    """{espn_id: {category: {key: cell}}} for a FINAL game, else None. A final box is kept for
    the process lifetime; a miss (not final yet / fetch failed) is retried after 10 min."""
    hit = _ESPN_BOX.get(str(game_id))
    if hit and (hit[1] is not None or time.time() - hit[0] < _ESPN_RETRY_S):
        return hit[1]
    from .player_logs import NFL_SUMMARY, _fetch_json, _is_final
    box = None
    d = _fetch_json(NFL_SUMMARY.format(game_id=game_id))
    if d and _is_final(d):
        box = {}
        for blk in (d.get("boxscore") or {}).get("players") or []:
            for cat in blk.get("statistics") or []:
                keys = cat.get("keys") or []
                for a in cat.get("athletes") or []:
                    try:
                        aid = int((a.get("athlete") or {}).get("id"))
                    except (TypeError, ValueError):
                        continue
                    box.setdefault(aid, {})[(cat.get("name") or "").lower()] = dict(zip(keys, a.get("stats") or []))
    _ESPN_BOX[str(game_id)] = (time.time(), box)
    return box


def _espn_value(g, espn_id: int, game_id: str, stat: str):
    """The player's stat from ESPN's final box score, or None (game not final / not ~3h past
    kickoff, or the player has no stat line anywhere -- DNP and a silent game look the same
    there, so that call stays with nflverse snap counts). A player with a line in another
    category (a TE with only a rush) played: 0 for this stat."""
    if stat not in _ESPN_STAT:
        return None
    if str(g["status"] or "").lower() != "final":
        try:
            from datetime import datetime as _dt, timezone as _tz
            ko = _dt.fromisoformat(str(g["start_time"]).replace("Z", "+00:00"))
            if (_dt.now(_tz.utc) - ko).total_seconds() < 3 * 3600:
                return None
        except (TypeError, ValueError):
            return None
    try:
        box = _espn_box(game_id)
    except Exception as e:
        logger.debug("ESPN box fallback failed for %s: %s", game_id, e)
        return None
    p = (box or {}).get(int(espn_id))
    if not p:
        return None
    cat, key = _ESPN_STAT[stat]
    if cat not in p:
        return 0.0
    cell = p[cat].get(key)
    if cell is None:
        return None
    from .player_logs import _int
    return float(_int(cell))


def settle_lookup(espn_id: int, game_id: str, stat: str):
    """Settlement read for a v2 pick (game_id = ESPN event id) -> (status, value):
    ('ok', v) the player's stat in THAT game (0 if he took snaps without touching the
    ball) -- from nflverse, else ESPN's final box score; ('dnp', None) the game is in the
    logs with snap counts and he isn't (or snaps never arrived 4+ days on); ('pending',
    None) not ingested yet."""
    g = _game(game_id)
    if not g:
        return "pending", None
    from datetime import date as _d, timedelta as _td
    from .player_props import _conn, _ensure
    d = _d.fromisoformat(str(g["date"])[:10])
    lo, hi = (d - _td(days=1)).isoformat(), (d + _td(days=1)).isoformat()
    teams = (ESPN_TO_NFLV.get(g["home"], g["home"]), ESPN_TO_NFLV.get(g["away"], g["away"]))
    c = _conn()
    _ensure(c)
    try:
        r = c.execute("SELECT %s v FROM player_prop_logs WHERE espn_id = ? AND date BETWEEN ? AND ? "
                      "AND team IN (?, ?) ORDER BY date LIMIT 1" % _SQL_EXPR.get(stat, stat),
                      (int(espn_id), lo, hi) + teams).fetchone()
        if r is not None and r["v"] is not None:
            return "ok", float(r["v"])
        gm = c.execute("SELECT COUNT(*) n, SUM(CASE WHEN offense_snaps IS NOT NULL THEN 1 ELSE 0 END) s "
                       "FROM player_prop_logs WHERE date BETWEEN ? AND ? AND team IN (?, ?)",
                       (lo, hi) + teams).fetchone()
    finally:
        c.close()
    v = _espn_value(g, espn_id, game_id, stat)
    if v is not None:
        return "ok", v
    if not gm or not gm["n"]:
        return "pending", None
    if gm["s"]:
        return "dnp", None
    return ("dnp", None) if (_d.today() - d).days >= 4 else ("pending", None)


_L10: dict = {}
_STAMP = {"ts": 0.0, "v": None}


def last10(espn_id: int, stat: str) -> list[float]:
    """The player's last (up to) 10 values of ``stat`` across seasons, oldest first.
    Cached per logs refresh: the props tracker asks for every settled pick's L10 on
    each load (400+ rows), so one query each would cost seconds."""
    from .player_props import _conn, last_refresh
    if stat not in _OPP_COL:
        return []
    key = (int(espn_id), stat)
    if time.time() - _STAMP["ts"] > 60:
        _STAMP.update(ts=time.time(), v=last_refresh())
    stamp = _STAMP["v"]
    hit = _L10.get(key)
    if hit is not None and hit[0] == stamp:
        return hit[1]
    data = _STATE["data"] if _STATE["key"] == stamp else None
    if data is not None:
        vals = [float(g.get(stat) or 0.0) for g in data["players"].get(int(espn_id), [])[-10:]]
    else:
        c = _conn()
        try:
            rows = c.execute("SELECT %s v FROM player_prop_logs WHERE espn_id = ? ORDER BY date DESC LIMIT 10"
                             % _SQL_EXPR.get(stat, stat), (int(espn_id),)).fetchall()
        finally:
            c.close()
        vals = [float(r["v"] or 0.0) for r in reversed(rows)]
    _L10[key] = (stamp, vals)
    return vals


__all__ = ["MARKETS", "project", "probs", "settle_lookup", "last10", "ESPN_TO_NFLV", "NFLV_TO_ESPN"]
