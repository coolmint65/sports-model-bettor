"""NBA player-prop projection v2 (MintyBot 2026-10-26, Austin "...then work on its player props").

v1 (engine.sports.nba.prop_picks before today) sampled each player's last 60 days of raw
stat lines through a fitted distribution and a pinned post-hoc curve, and priced every
alt-line rung. It also had no box scores this season (they were only ingested to settle
pending picks), so 2026-27 opened with zero NBA props.

v2 projects each stat as

    mu = projected minutes  x  per-minute rate  x  (opponent allowed / league)^0.7

* minutes: EWMA of the player's minutes (half-life 4 games), seeded at the season
  start by last season's level (worth 3 games);
* per-minute rate: minutes-weighted EWMA (half-life 12 games), seeded by last season's
  rate shrunk 10% toward the league's (worth 250 minutes);
* opponent: what each defense allowed per team-game this season (EWMA, half-life 15)
  over the league average;
* preseason games never count; playoff games do.

The outcome is NegBin with variance a*mu + b*mu^2 per stat (fit on 56,489 player-games
2024-25..2025-26). Combos (PRA, P+R, P+A, R+A) sum the component means with their own
fitted variance.

Backtest vs DraftKings' NBA prop lines (ESPN core API: n=10,515 main O/U lines, Jan 2026
+ playoffs). Each stat is a fixed line on our side; the market log-loss is .6806 and the
model's .6975, so on its own the model is WORSE than DK. The best model share in a blend
(w*, log-loss) is ~.05 for points/PRA, .25 rebounds, .20 assists, 0 threes, .50 steals
(n=822), .20 blocks. Against ESPN BET's early-season lines (Oct-Nov 2025) the model would
have crushed assists/rebounds, but that feed's over/under order is unreliable, so it is
not evidence. engine.sports.nba.prop_eval therefore starts each stat's market weight from
the DK fit (not the shared 0.35) and refits it weekly on graded Hard Rock lines. DK's own
line is logged next to every HR line (dk_*), so "is HR softer than DK?" gets measured
from day one. Backtest scripts: Pi scratchpad nbaprops/bt/.
"""
from __future__ import annotations

import json
import logging
import math
import threading
import time
import urllib.request
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)

BASIC = ("pts", "reb", "ast", "tpm", "stl", "blk", "to", "ftm")
COMBOS = {"pra": ("pts", "reb", "ast"), "pr": ("pts", "reb"), "pa": ("pts", "ast"),
          "ra": ("reb", "ast")}
# Hard Rock bet type -> stat key (engine.player_props_tracker._NBA_STAT_KEY is the settle map)
MARKETS = {
    "Player Points": "pts", "Player Rebounds": "reb", "Player Assists": "ast",
    "Player 3PM": "tpm", "Player PRA": "pra", "Player Steals": "stl", "Player Blocks": "blk",
    "Player Turnovers": "to", "Player FT Made": "ftm",
}
# Residual variance a*mu + b*mu^2 per stat (walk-forward projections, 2024-25..2025-26).
VAR = {"pts": (4.1349, -0.0515), "reb": (1.608, -0.01775), "ast": (1.357, -0.00773),
       "tpm": (1.466, -0.11765), "stl": (1.2902, -0.17155), "blk": (1.1172, 0.01598),
       "to": (1.1633, -0.04873), "ftm": (2.1554, -0.01323), "pra": (4.6127, -0.04971),
       "pr": (4.484, -0.05333), "pa": (4.151, -0.04717), "ra": (1.8895, -0.02019)}
PARAMS = {"hm": 4.0, "hr": 12.0, "prior_min_games": 3.0, "prior_rate_min": 250.0,
          "reg_min": 1.0, "reg_rate": 0.9, "def_hl": 15.0, "alpha": 0.7}
# Regular-season openers (preseason before, playoffs after the last regular-season day).
_REG_START = {2023: "2023-10-24", 2024: "2024-10-22", 2025: "2025-10-21", 2026: "2026-10-20"}

_LOCK = threading.Lock()
_STATE: dict = {}          # asof -> (key, built_ts, state)
_STATE_TTL_S = 1800


def season_of(d: str) -> int:
    y, m = int(d[:4]), int(d[5:7])
    return y if m >= 8 else y - 1


def _reg_start(season: int) -> str:
    # Unknown future season: the Tuesday on/after Oct 20 (the league's recent pattern).
    s = _REG_START.get(season)
    if s:
        return s
    d = datetime(season, 10, 20)
    while d.weekday() != 1:
        d = d.replace(day=d.day + 1)
    return d.strftime("%Y-%m-%d")


def _conn():
    from ...player_props_db import _conn_for
    return _conn_for("nba")


def _load(asof: str):
    """Played (min > 0) regular-season + playoff player-games before ``asof``, three seasons."""
    s0 = season_of(asof) - 2
    rows = _conn().execute(
        "SELECT date, game_id, player_id, team_id, opp_team_id, stats_json "
        "FROM player_game_logs WHERE date >= ? AND date < ? ORDER BY date, game_id",
        (f"{s0}-08-01", asof)).fetchall()
    out = []
    for r in rows:
        d = str(r["date"])[:10]
        if d < _reg_start(season_of(d)):
            continue                                   # preseason
        try:
            st = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        if (st.get("min") or 0) <= 0:
            continue
        out.append((d, str(r["game_id"]), int(r["player_id"]), int(r["team_id"] or 0),
                    int(r["opp_team_id"] or 0), st))
    return out


def _new_season(P: dict) -> None:
    """Season turnover: every player restarts from last season's level (minutes worth 3
    games, per-minute rates shrunk 10% toward the league's, worth 250 minutes)."""
    p = PARAMS
    lr = {k: 0.0 for k in BASIC}
    lm = lmin = 0.0
    nl = 0
    for st in P.values():
        if st["rm"] > 0:
            for k in BASIC:
                lr[k] += st["rs"][k]
            lm += st["rm"]
            lmin += st["min_last"]
            nl += 1
    for st in P.values():
        st["min_w"] = p["prior_min_games"]
        st["min_s"] = (p["reg_min"] * st["min_last"]
                       + (1 - p["reg_min"]) * (lmin / max(nl, 1))) * p["prior_min_games"]
        for k in BASIC:
            rate = (st["rs"][k] / st["rm"]) if st["rm"] > 0 else 0.0
            lrate = lr[k] / lm if lm > 0 else rate
            st["rs"][k] = (p["reg_rate"] * rate + (1 - p["reg_rate"]) * lrate) * p["prior_rate_min"]
        st["rm"] = p["prior_rate_min"]
        st["n_season"] = 0


def _build(rows, asof: str) -> dict:
    """Replay the walk-forward state (player minutes / per-minute rates, team defense,
    league averages) through every game before ``asof``."""
    p = PARAMS
    dm, dr, dd = 0.5 ** (1 / p["hm"]), 0.5 ** (1 / p["hr"]), 0.5 ** (1 / p["def_hl"])
    P: dict = {}
    T: dict = {}
    L = defaultdict(float)
    Ln = 0.0
    last_season = None
    bydate = defaultdict(list)
    for r in rows:
        bydate[r[0]].append(r)
    for d in sorted(bydate):
        s = season_of(d)
        if s != last_season:
            _new_season(P)
            T = {}
            L = defaultdict(float)
            Ln = 0.0
            last_season = s
        team_tot = defaultdict(lambda: defaultdict(float))
        for (_d, gid, pid, tid, oid, st) in bydate[d]:
            m = float(st.get("min") or 0)
            ps = P.get(pid)
            if ps is None:
                ps = P[pid] = {"min_s": 0.0, "min_w": 0.0, "rs": {k: 0.0 for k in BASIC}, "rm": 0.0,
                               "min_last": m, "n_season": 0, "n_total": 0, "team": tid, "last": d}
            ps["min_s"] = ps["min_s"] * dm + m
            ps["min_w"] = ps["min_w"] * dm + 1.0
            for k in BASIC:
                ps["rs"][k] = ps["rs"][k] * dr + float(st.get(k) or 0)
            ps["rm"] = ps["rm"] * dr + m
            ps["min_last"] = ps["min_s"] / ps["min_w"]
            ps["n_season"] += 1
            ps["n_total"] += 1
            ps["team"], ps["last"] = tid, d
            for k in BASIC:
                team_tot[(gid, tid, oid)][k] += float(st.get(k) or 0)
        for (gid, tid, oid), tot in team_tot.items():
            t = T.get(oid)
            if t is None:
                t = T[oid] = {"s": {k: 0.0 for k in BASIC}, "w": 0.0}
            for k in BASIC:
                t["s"][k] = t["s"][k] * dd + tot[k]
                L[k] += tot[k]
            t["w"] = t["w"] * dd + 1.0
            Ln += 1.0
    if last_season is not None and season_of(asof) != last_season:
        _new_season(P)                       # opening night: no new-season games yet
        T, L, Ln, last_season = {}, defaultdict(float), 0.0, season_of(asof)
    lavg = {k: (L[k] / Ln) if Ln > 0 else None for k in BASIC}
    return {"P": P, "T": T, "lavg": lavg, "season": last_season, "asof": asof}


_RECHECK_S = 120            # how often a cached state re-counts the box scores behind it


def state(asof: str) -> dict:
    """Cached walk-forward state for projections ON ``asof`` (games before it). Rebuilt when
    new box scores dated before ``asof`` land (re-counted at most every 2 min) or after 30 min."""
    with _LOCK:
        hit = _STATE.get(asof)
        now = time.time()
        if hit and now - hit[3] < _RECHECK_S and now - hit[1] < _STATE_TTL_S:
            return hit[2]
        n = _conn().execute("SELECT COUNT(*) FROM player_game_logs WHERE date < ?",
                            (asof,)).fetchone()[0]
        if hit and hit[0] == n and now - hit[1] < _STATE_TTL_S:
            _STATE[asof] = (hit[0], hit[1], hit[2], now)
            return hit[2]
        st = _build(_load(asof), asof)
        if len(_STATE) > 8:
            _STATE.clear()
        _STATE[asof] = (n, now, st, now)
        return st


# Teammate-out usage boost (MintyBot 2026-11-01, Austin: Doink's "without players" view). When
# regulars (>= 12 projected min, played in the last 12 days, >= 3 games this season) are Out, their
# projected production is handed to the teammates who play, in proportion to each one's share:
#     mu_k += TMO_B[k] * (sum of the absent regulars' mu_k) * (player's mu_k / team's playing mu_k)
# Fit by least squares on 53.9k walk-forward player-games 2024-25..2026-27 (Beelink
# C:/Users/Austin/tmo_*.py). Train 2024-25 / test 2025-27: points RMSE 6.031 -> 5.975, and the
# projection bias when a big piece is out falls from +0.71 pts to -0.05 (rebounds +.19 -> .00,
# assists +.15 -> -.02). Against DraftKings' lines (n=11.5k) it adds ~nothing (log-loss .6884 ->
# .6881; blended .6860 either way): DK already prices teammate absences, so this fixes our
# projections (no more fake Unders on the guys who inherit the shots), it is not an edge.
TMO_B = {"pts": 0.245, "reb": 0.186, "ast": 0.234, "tpm": 0.149, "stl": 0.042, "blk": 0.065,
         "to": 0.204, "ftm": 0.247}
TMO_MIN_B = 0.179
TMO_REG_MIN, TMO_RECENT_D, TMO_MIN_GAMES = 12.0, 12, 3


def _days(a: str, b: str) -> int:
    return (datetime.strptime(b[:10], "%Y-%m-%d") - datetime.strptime(a[:10], "%Y-%m-%d")).days


def _raw_mu(ps: dict) -> tuple:
    m = ps["min_s"] / ps["min_w"]
    return m, {k: m * ps["rs"][k] / ps["rm"] for k in BASIC}


def _opp_factor(st: dict, opp_team_id: int) -> dict:
    t = st["T"].get(int(opp_team_id))
    out = {}
    for k in BASIC:
        f = 1.0
        if t is not None and t["w"] > 0 and st["lavg"].get(k):
            f = ((t["s"][k] / t["w"]) / st["lavg"][k]) ** PARAMS["alpha"]
        out[k] = f
    return out


def _team_split(st: dict, team: int, asof: str, out_ids: set) -> tuple:
    """(absent regulars' minutes, their mu{k}, playing teammates' raw mu{k} sums, minutes sum)."""
    season = season_of(asof)
    a_min, a_mu = 0.0, {k: 0.0 for k in BASIC}
    p_min, p_mu = 0.0, {k: 0.0 for k in BASIC}
    for pid, ps in st["P"].items():
        if ps["team"] != team or ps["min_w"] <= 0 or ps["rm"] <= 0 or not ps.get("last"):
            continue
        if ps["last"] < _reg_start(season) or ps["n_season"] < TMO_MIN_GAMES:
            continue
        if _days(ps["last"], asof) > TMO_RECENT_D:
            continue
        m, mu = _raw_mu(ps)
        if pid in out_ids:
            if m >= TMO_REG_MIN:
                a_min += m
                for k in BASIC:
                    a_mu[k] += mu[k]
        else:
            p_min += m
            for k in BASIC:
                p_mu[k] += mu[k]
    return a_min, a_mu, p_min, p_mu


def project(player_id: int, opp_team_id: int, asof: str, out_ids=None) -> dict | None:
    """{min, mu{stat}, n_season, n_total, team, tmo} for a player's next game vs ``opp_team_id``.
    ``out_ids``: player ids listed Out for this game -- their teammates get the usage boost."""
    st = state(asof)
    ps = st["P"].get(int(player_id))
    if ps is None or ps["min_w"] <= 0 or ps["rm"] <= 0:
        return None
    mproj, raw = _raw_mu(ps)
    f = _opp_factor(st, opp_team_id)
    mu = {k: raw[k] * f[k] for k in BASIC}
    tmo = None
    if out_ids:
        a_min, a_mu, p_min, p_mu = _team_split(st, ps["team"], asof, set(out_ids))
        if a_min > 0:
            # the opponent factor is one team-wide multiplier per stat, so shares can use raw mu
            for k in BASIC:
                if p_mu[k] > 0:
                    mu[k] += TMO_B[k] * a_mu[k] * f[k] * (raw[k] / p_mu[k])
            if p_min > 0:
                mproj += TMO_MIN_B * a_min * (mproj / p_min)
            tmo = {"out_min": round(a_min, 1), "out_pts": round(a_mu["pts"], 1)}
    for c, parts in COMBOS.items():
        mu[c] = sum(mu[x] for x in parts)
    in_season = ps["last"] >= _reg_start(season_of(asof)) if ps.get("last") else False
    return {"min": mproj, "mu": mu, "n_season": ps["n_season"] if in_season else 0,
            "n_total": ps["n_total"], "team": ps["team"], "tmo": tmo}


def _nb_cdf(x: int, mu: float, var: float) -> float:
    if x < 0:
        return 0.0
    var = max(var, mu * 1.05)
    k = mu * mu / (var - mu)
    q = mu / (k + mu)
    pmf = (k / (k + mu)) ** k
    c = pmf
    for i in range(int(x)):
        pmf *= (i + k) / (i + 1) * q
        c += pmf
    return min(1.0, c)


# Platt correction on the NegBin tail (MintyBot 2026-10-28, Austin backlog #8 "the model rating every
# Over 3-8 points too low"): the projections are unbiased (2026-27 actual/projected minutes 1.02,
# points .98, rebounds 1.03, assists 1.00, threes 1.00) but the NegBin tail is too right-skewed, so
# with the mean right on the book's line it still priced the Over at .44-.47 while Overs hit ~.50.
# logit p = a + b * logit(p_NegBin), fit per stat on DraftKings opening lines 2025-26 (60% time
# split; Pi scratchpad 2d30bad6.../nbaprops/bt/cal_fit.py). Held-out log-loss, raw -> calibrated:
# pts .7139 -> .6893, reb .7013 -> .6911, pr .7041 -> .6921, pra .7015 -> .6929. The small slopes
# (points .16, combos .02-.05) are the honest read: the model carries little information on points.
PLATT = {"ast": (-0.2156, 0.5687), "blk": (0.1475, 0.8077), "pa": (-0.2251, 0.0303),
         "pr": (-0.1853, 0.0173), "pra": (-0.2114, 0.0523), "pts": (-0.0866, 0.1609),
         "ra": (-0.0544, 0.5502), "reb": (0.0343, 0.4928), "stl": (0.0845, 1.1896),
         "tpm": (-0.0717, 0.4619)}


def p_over_raw(stat: str, mu: float, line: float) -> float | None:
    """P(stat > line) from the NegBin alone (half-point lines; pushes excluded by the grader)."""
    if stat not in VAR or mu is None:
        return None
    mu = max(float(mu), 0.05)
    a, b = VAR[stat]
    return 1.0 - _nb_cdf(int(math.floor(line)), mu, max(a * mu + b * mu * mu, mu * 1.05))


def p_over(stat: str, mu: float, line: float) -> float | None:
    """Calibrated P(stat > line): the NegBin tail through the per-stat Platt map (PLATT)."""
    p = p_over_raw(stat, mu, line)
    if p is None or stat not in PLATT:
        return p
    p = min(max(p, 1e-4), 1 - 1e-4)
    a, b = PLATT[stat]
    return 1.0 / (1.0 + math.exp(-(a + b * math.log(p / (1 - p)))))


# ── DraftKings reference (ESPN core API) ─────────────────────────
_DK_TYPES = {"Total Points": "pts", "Total Rebounds": "reb", "Total Assists": "ast",
             "Total 3-Point Field Goals": "tpm", "Total Steals": "stl", "Total Blocks": "blk",
             "Total Points, Rebounds, and Assists": "pra", "Total Points and Rebounds": "pr",
             "Total Points and Assists": "pa", "Total Assists and Rebounds": "ra",
             "Total Turnovers": "to"}
_DK_CACHE: dict = {}
_DK_TTL_S = 600


def _imp(o):
    try:
        o = float(str(o).replace("+", ""))
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    return 100.0 / (o + 100.0) if o > 0 else -o / (-o + 100.0)


def _am(x):
    try:
        return int(float(str(x).replace("+", "")))
    except (TypeError, ValueError):
        return None


def dk_lines(game_id: str) -> dict:
    """{(athlete_id, stat): (line, over_odds, under_odds)} -- DraftKings' main two-way
    line per player-stat for an ESPN event (first of each priced pair is the Over,
    verified on 10.5k graded lines). {} when ESPN has none. Cached 10 min."""
    gid = str(game_id)
    hit = _DK_CACHE.get(gid)
    if hit and time.time() - hit[0] < _DK_TTL_S:
        return hit[1]
    out: dict = {}
    try:
        url = (f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{gid}"
               f"/competitions/{gid}/odds/100/propBets?lang=en&region=us&limit=1000")
        req = urllib.request.Request(url, headers={"User-Agent": "curl/8.4.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            items = (json.loads(r.read()) or {}).get("items") or []
        grp = defaultdict(lambda: defaultdict(list))
        for k, it in enumerate(items):
            stat = _DK_TYPES.get((it.get("type") or {}).get("name"))
            ath = ((it.get("athlete") or {}).get("$ref") or "").split("/athletes/")[-1].split("?")[0]
            o = it.get("odds") or {}
            line = (o.get("total") or {}).get("value")
            am = (o.get("american") or {}).get("value")
            if stat and ath.isdigit() and line is not None and am is not None:
                grp[(int(ath), stat)][str(line)].append((k, _am(am)))
        for key, bylines in grp.items():
            best = None
            for ln, v in bylines.items():
                if len(v) != 2 or v[0][1] is None or v[1][1] is None:
                    continue
                v = sorted(v)
                io, iu = _imp(v[0][1]), _imp(v[1][1])
                if io is None or iu is None:
                    continue
                bal = abs(io / (io + iu) - 0.5)
                if best is None or bal < best[0]:
                    best = (bal, (float(ln), v[0][1], v[1][1]))
            if best:
                out[key] = best[1]
    except Exception as e:
        logger.debug("nba dk_lines %s failed: %s", gid, e)
    _DK_CACHE[gid] = (time.time(), out)
    return out


__all__ = ["MARKETS", "BASIC", "COMBOS", "state", "project", "p_over", "dk_lines", "season_of"]
