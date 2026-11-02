"""College basketball team ratings -- the NCAAM / NCAAW predictor (MintyBot 2026-10-26,
Austin "fix up college basketball ... the same standard as NBA").

Replaces the framework's points-per-game factor model for leagues whose registry entry carries
``"model": "ratings"``. The old path rated teams on raw season PPG (no opponent adjustment),
blended 70% with the last 10 games of ANY season, read last season's stats all of this season
(the DB labels college seasons by END year), and added a 7.8-pt home edge to every game,
neutral sites included. Leak-free replay of 2025-26 vs the DraftKings / ESPN BET closing lines:
margin RMSE 15.48 (close 11.25), total 18.21 (close 16.86) -- its "edges" were noise.

Model. Every team carries an offensive (O) and defensive (D) points rating:

    home pts = mu + O_home - D_away + h1 * home_court
    away pts = mu + O_away - D_home + h2 * home_court

fit by weighted ridge regression on every final of the season before the game's date, shrunk
toward a preseason prior (0.85 x last season's final rating + 0.10 x the season before; the prior
counts as 4 games). home_court is split conference / non-conference (non-conference "buy games"
carry the bigger edge: men's ~4.4 vs ~2.6 pts), neutral sites (ESPN neutralSite) get no home
edge and their own total shift; those globals shrink toward last season's values. Opponents
outside Division I share one pseudo-team. Finals decided by more than 30 points get weight
30/|margin| so garbage-time blowouts don't swing ratings.

Walk-forward 2023-24..2025-26, every game predicted from games before its date only:
  NCAAM 2025-26, n=5,577 with closing lines: margin RMSE 11.72 (close 11.25), total 17.44
    (close 16.86). No information beyond the CLOSE (slope -0.04), but it predicts the
    open->close MOVE: slope +0.23 (t 28); |model - open| >= 3 pts -> +0.76 pts CLV (the line
    moved our way 45%, against 29%).
  NCAAW 2025-26, n=903: margin 12.80 (close 12.35), total 15.89 (close 15.77); open->close
    spread slope +0.53 (t 21).
Probabilities: margin ~ N(mu, sigma) with sigma by season phase (Nov / Dec / Jan on, widening
with the projected margin), fit on D1-vs-D1 walk-forward residuals; win% reliability is within
~3pp per decile from January. It also beat the framework GBM on the GBM's own holdout (late Nov
2025 on): NCAAM margin RMSE 11.88 vs 12.50, total 17.30 vs 17.47, win Brier .180 vs .192;
NCAAW 12.70 vs 14.07, 16.36 vs 16.58, .152 vs .169 -- so the GBM leg is logged, not blended.
Backtest + fit scripts: Pi scratchpad ncaab/bt/.
"""
from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import numpy as np

from ._config import get_league_config, season_label
from ._db import get_conn, games_table

logger = logging.getLogger(__name__)

_DATA = Path(__file__).resolve().parent.parent.parent / "data" / "basketball"

PARAMS = {
    "lam": 4.0,          # prior strength, in games
    "rho": 0.85,         # weight on last season's final rating
    "rho2": 0.10,        # weight on the season before
    "huber": 30.0,       # finals decided by more than this get weight huber/|margin|
    "d1_min": 15,        # finals in a past season that make a team Division I for the prior chain
    # globals (GLOBALS order) and how hard each shrinks toward last season's value, in games
    "lamg": (5.0, 300.0, 300.0, 300.0, 300.0, 300.0),
    "g_init": (70.0, 1.7, -1.7, 1.7, -1.7, 0.0),
    "nd_prior": (-8.2, -8.5),   # the non-D1 pseudo-team's (O, D) prior
}
GLOBALS = ("mu", "h1c", "h2c", "h1n", "h2n", "tneu")
# Per-league overrides of PARAMS. Australia NBL (MintyBot 2026-10-29, Austin "up to standard fully"): heavy import
# turnover, so last season carries less (rho .70, no second season) and team ratings shrink harder to it (lam 8).
# Walk-forward 2022-27 on the deduped DB (n=651; Pi scratchpad nbl/nbl_wf.py): margin RMSE 14.52 vs the default
# PARAMS' 14.59. Against the closing line (bet365, Stake for 2025-26; BetExplorer, n=418): margin 14.44 vs 14.34,
# ML log-loss .647 vs .633, total 17.71 vs 17.96 (shift below fit in-sample). Projected margins are calibrated
# (final margin on projected: slope .998).
# total_shift: NBL games out-scored the walk-forward projection every season (+3.2 / +4.4 / +2.7 / +3.7 points,
# 2023-26; bet365's closing total ran +2.8 low on the same games), so projected points get +1.7 a side.
LEAGUE_PARAMS = {
    "australia_nbl": {"rho": 0.70, "rho2": 0.0, "lam": 8.0, "total_shift": 3.4},
}


def _par(league, key):
    return (LEAGUE_PARAMS.get(league) or {}).get(key, PARAMS.get(key, 0.0))

# Residual model, D1-vs-D1 walk-forward games 2023-24..2025-26 (phase = game month):
# margin variance = a + b * projected_margin^2, total sd flat.
SIGMA = {
    "ncaam": {"nov": (157.99, 0.0377, 17.71), "dec": (133.65, 0.0356, 16.71),
              "late": (122.34, 0.0392, 16.92)},
    "ncaaw": {"nov": (174.30, 0.0899, 16.90), "dec": (147.73, 0.0518, 16.14),
              "late": (137.59, 0.0057, 16.57)},
    # China CBA (MintyBot 2026-10-28, Austin backlog #5): walk-forward residuals over 1,433 games
    # 2023-26 (Sofascore history, Pi scratchpad cba/cba_bt.py) -- flat margin sd 13.05 (variance does
    # not grow with the projected margin), total sd 18.74. Margin RMSE 12.95 vs the spread line's
    # 12.55; blended 30-50% with the opening moneyline it beats the open (log-loss .5086 -> .5051).
    "china_cba": {"nov": (170.36, 0.0, 18.74), "dec": (170.36, 0.0, 18.74),
                  "late": (170.36, 0.0, 18.74)},
    # Australia NBL (2026-10-29): walk-forward residuals 2022-27 (n=651, deduped DB), margin var 204.70 + .167 m^2,
    # total sd 18.07.
    "australia_nbl": {"nov": (204.70, 0.1668, 18.07), "dec": (204.70, 0.1668, 18.07),
                      "late": (204.70, 0.1668, 18.07)},
}
# The first period HR prices (NCAAM: 1st half, stored in the home_q1 columns; NCAAW: 1st
# quarter), off the same walk-forward: period margin = k_m * margin, period total = c_t + k_t *
# total, residual sds. Men's first half runs 58% of the final margin (leads get padded early,
# trimmed late), 47% of the points.
PERIOD = {
    "ncaam": {"label": "H1", "k_m": 0.583, "c_t": 7.96, "k_t": 0.416, "sd_m": 9.54, "sd_t": 10.27},
    "ncaaw": {"label": "Q1", "k_m": 0.320, "c_t": 2.39, "k_t": 0.220, "sd_m": 6.88, "sd_t": 6.85},
    "china_cba": {"label": "Q1", "k_m": 0.318, "c_t": -0.36, "k_t": 0.258, "sd_m": 7.58, "sd_t": 7.76},
    "australia_nbl": {"label": "Q1", "k_m": 0.128, "c_t": -6.66, "k_t": 0.290, "sd_m": 7.63, "sd_t": 7.92},
}

_LOCK = threading.Lock()
_CHAIN: dict = {}      # league -> (key, chain) ; chain = priors for the current season
_FITS: dict = {}       # (league, asof) -> (key, fit)
_FIT_TTL_S = 600
_D1_TTL_S = 7 * 86400


def uses_ratings(league) -> bool:
    return (get_league_config(league) or {}).get("model") == "ratings"


def _ncdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


# ── data ────────────────────────────────────────────────────────

_ROWS: dict = {}       # league -> (ts, rows); a slate prices ~180 games off one read
_ROWS_TTL_S = 60


def _load(league):
    hit = _ROWS.get(league)
    if hit and time.time() - hit[0] < _ROWS_TTL_S:
        return hit[1]
    rows = _load_db(league)
    _ROWS[league] = (time.time(), rows)
    return rows


def _load_db(league):
    """Every game row: (date, season, home_id, away_id, hs|None, as|None, neutral, conf).
    Scores only for real finals -- ESPN lands a cancelled game as a 0-0 'final'."""
    conn = get_conn(league)
    tbl = games_table(league)
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({tbl})")}
    neu = "neutral" if "neutral" in cols else "NULL"
    cnf = "conf_game" if "conf_game" in cols else "NULL"
    rows = conn.execute(
        f"SELECT date, season, home_team_id, away_team_id, home_score, away_score, status, "
        f"{neu} AS neutral, {cnf} AS conf_game FROM {tbl} "
        f"WHERE season IS NOT NULL AND home_team_id IS NOT NULL AND away_team_id IS NOT NULL "
        f"  AND COALESCE(status, '') NOT IN ('cancelled', 'postponed', 'voided')").fetchall()
    out = []
    for r in rows:
        hs, as_ = r["home_score"], r["away_score"]
        fin = r["status"] == "final" and hs is not None and as_ is not None and (hs + as_) > 0
        out.append((str(r["date"])[:10], int(r["season"]), int(r["home_team_id"]),
                    int(r["away_team_id"]), float(hs) if fin else None,
                    float(as_) if fin else None, bool(r["neutral"]), bool(r["conf_game"])))
    return out


def espn_d1_teams(league) -> list[dict]:
    """ESPN's Division I list for the league (cached 7 days on disk): id, abbreviation and
    every name form the book might use. [] when ESPN is unreachable and nothing is cached."""
    path = _DATA / f"{league}_d1_teams.json"
    cached = None
    try:
        cached = json.loads(path.read_text(encoding="utf-8"))
        if time.time() - float(cached.get("fetched_ts", 0)) < _D1_TTL_S and cached.get("teams"):
            return cached["teams"]
    except Exception:
        cached = None
    try:
        from ._espn_ingest import _espn_base, _fetch
        data = _fetch(f"{_espn_base(league)}/teams?groups=50&limit=500")
        lg = ((data or {}).get("sports") or [{}])[0].get("leagues") or [{}]
        teams = []
        for t in lg[0].get("teams") or []:
            t = t.get("team") or {}
            if t.get("id"):
                teams.append({k: t.get(k) for k in ("id", "abbreviation", "displayName",
                                                     "shortDisplayName", "location", "name",
                                                     "nickname", "slug")})
        if len(teams) >= 300:
            path.write_text(json.dumps({"fetched_ts": time.time(), "teams": teams}),
                            encoding="utf-8")
            return teams
        logger.warning("[%s] ESPN D1 list came back with %d teams; keeping cache", league,
                       len(teams))
    except Exception as e:
        logger.warning("[%s] ESPN D1 list fetch failed: %s", league, e)
    return (cached or {}).get("teams") or []


# ── fitting ─────────────────────────────────────────────────────

def _z(neutral, conf, home_side):
    z = np.zeros(len(GLOBALS))
    z[0] = 1.0
    if neutral:
        z[5] = 1.0
    elif conf:
        z[1 if home_side else 2] = 1.0
    else:
        z[3 if home_side else 4] = 1.0
    return z


def _fit(rows, d1set, prior, g0, asof=None, lam=None):
    """Ridge fit on ``rows`` (one season) using finals dated before ``asof`` (None = all).
    ``prior``: team -> (O, D). Returns {ix, nd, O, D, G, n_final, games}."""
    teams = sorted({g[2] for g in rows} | {g[3] for g in rows})
    ix = {t: i for i, t in enumerate(sorted(t for t in teams if t in d1set))}
    nd = len(ix)
    for t in teams:
        ix.setdefault(t, nd)
    n = nd + 1
    o0 = np.zeros(n); d0 = np.zeros(n)
    for t, i in ix.items():
        if i != nd and t in prior:
            o0[i], d0[i] = prior[t]
    o0[nd], d0[nd] = PARAMS["nd_prior"]
    k = len(GLOBALS)
    m = 2 * n + k
    A = np.zeros((m, m)); b = np.zeros(m)
    fin = [g for g in rows if g[4] is not None and (asof is None or g[0] < asof)]
    games = Counter()
    if fin:
        L = len(fin)
        hi = np.array([ix[g[2]] for g in fin]); ai = np.array([ix[g[3]] for g in fin])
        hs = np.array([g[4] for g in fin]); as_ = np.array([g[5] for g in fin])
        neu = np.array([g[6] for g in fin], dtype=bool)
        cnf = np.array([g[7] for g in fin], dtype=bool)
        mg = np.abs(hs - as_)
        hub = PARAMS["huber"]
        w = np.where(mg > hub, hub / np.maximum(mg, 1e-9), 1.0)
        for (t, o, y, home_side) in ((hi, ai, hs, True), (ai, hi, as_, False)):
            Z = np.zeros((L, k))                        # vectorized _z()
            Z[:, 0] = 1.0
            Z[neu, 5] = 1.0
            Z[~neu & cnf, 1 if home_side else 2] = 1.0
            Z[~neu & ~cnf, 3 if home_side else 4] = 1.0
            idx = np.concatenate([t[:, None], (n + o)[:, None],
                                  2 * n + np.tile(np.arange(k), (L, 1))], 1)
            val = np.concatenate([np.ones((L, 1)), -np.ones((L, 1)), Z], 1)
            wv = val * w[:, None]
            # A += sum over rows of w * outer(val, val) at (idx, idx): one bincount
            flat = (idx[:, :, None] * m + idx[:, None, :]).ravel()
            A += np.bincount(flat, weights=(wv[:, :, None] * val[:, None, :]).ravel(),
                             minlength=m * m).reshape(m, m)
            b += np.bincount(idx.ravel(), weights=(wv * y[:, None]).ravel(), minlength=m)
        for g in fin:
            games[g[2]] += 1
            games[g[3]] += 1
    lam = PARAMS["lam"] if lam is None else lam
    r = np.arange(n)
    A[r, r] += lam
    A[n + r, n + r] += lam
    b[:n] += lam * o0
    b[n:2 * n] += lam * d0
    gi = 2 * n + np.arange(k)
    lamg = np.asarray(PARAMS["lamg"], dtype=float)
    A[gi, gi] += lamg
    b[gi] += lamg * np.asarray(g0, dtype=float)
    x = np.linalg.solve(A, b)
    return {"ix": ix, "nd": nd, "O": x[:n], "D": x[n:2 * n], "G": x[2 * n:],
            "n_final": len(fin), "games": games}


def _chain(league, season, rows):
    """Priors for ``season``: replay every earlier season's end-of-season fit in order (each
    seeded by the one before), exactly as the walk-forward that chose the parameters."""
    by = {}
    for g in rows:
        by.setdefault(g[1], []).append(g)
    past = sorted(s for s in by if s < season)
    key = (season, tuple((s, len(by[s]), sum(1 for g in by[s] if g[4] is not None)) for s in past))
    hit = _CHAIN.get(league)
    if hit and hit[0] == key:
        return hit[1]
    last, last2 = {}, {}
    g0 = np.asarray(PARAMS["g_init"], dtype=float)
    rho, rho2 = _par(league, "rho"), _par(league, "rho2")
    for s in past:
        srows = by[s]
        cnt = Counter()
        for g in srows:
            if g[4] is not None:
                cnt[g[2]] += 1
                cnt[g[3]] += 1
        teams = {g[2] for g in srows} | {g[3] for g in srows}
        d1 = {t for t in teams if cnt[t] >= PARAMS["d1_min"] or t in last}
        prior = _blend_prior(last, last2, rho, rho2)
        f = _fit(srows, d1, prior, g0, lam=_par(league, "lam"))
        last2, last = last, {t: (float(f["O"][f["ix"][t]]), float(f["D"][f["ix"][t]])) for t in d1}
        g0 = f["G"].copy()
    chain = {"last": last, "last2": last2, "g0": g0, "seasons": past}
    _CHAIN[league] = (key, chain)
    return chain


def _blend_prior(last, last2, rho, rho2):
    out = {}
    for t, (o, d) in last.items():
        o2, d2 = last2.get(t, (o, d))
        out[t] = (rho * o + rho2 * o2, rho * d + rho2 * d2)
    return out


def fit_asof(league, asof):
    """Ratings for the season ``asof`` falls in, from that season's finals dated before it."""
    season = season_label(league, asof)
    with _LOCK:
        rows = _load(league)
        srows = [g for g in rows if g[1] == season]
        key = (season, len(srows), sum(1 for g in srows if g[4] is not None and g[0] < asof))
        hit = _FITS.get((league, asof))
        if hit and hit[0] == key and time.time() - hit[2] < _FIT_TTL_S:
            return hit[1]
        ch = _chain(league, season, rows)
        cnt = Counter()
        for g in srows:
            if g[4] is not None and g[0] < asof:
                cnt[g[2]] += 1
                cnt[g[3]] += 1
        d1 = set(ch["last"]) | {t for t, c in cnt.items() if c >= PARAMS["d1_min"]}
        for t in espn_d1_teams(league):
            try:
                d1.add(int(t["id"]))
            except (TypeError, ValueError, KeyError):
                pass
        prior = _blend_prior(ch["last"], ch["last2"], _par(league, "rho"), _par(league, "rho2"))
        f = _fit(srows, d1, prior, ch["g0"], asof=asof, lam=_par(league, "lam"))
        f.update(season=season, asof=asof, prior=prior, d1=d1)
        if len(_FITS) > 64:
            _FITS.clear()
        _FITS[(league, asof)] = (key, f, time.time())
        return f


# ── prediction ──────────────────────────────────────────────────

def _phase(date_str):
    m = int(date_str[5:7])
    return "nov" if m == 11 else "dec" if m == 12 else "late"


def predict(league, home_id, away_id, *, game_date, neutral=False, conf=False):
    """Expected points, margin, total, probabilities and period lines for one game."""
    f = fit_asof(league, game_date)
    O, D, G, ix, nd = f["O"], f["D"], f["G"], f["ix"], f["nd"]

    def od(t):
        i = ix.get(int(t))
        if i is not None:
            return float(O[i]), float(D[i]), f["games"].get(int(t), 0)
        if int(t) in f["d1"]:          # D1 team not on this season's schedule yet
            o, d = f["prior"].get(int(t), (0.0, 0.0))
            return o, d, 0
        return float(O[nd]), float(D[nd]), 0

    oh, dh, nh = od(home_id)
    oa, da, na = od(away_id)
    shift = _par(league, "total_shift") / 2.0
    ph = float(np.dot(_z(neutral, conf, True), G)) + oh - da + shift
    pa = float(np.dot(_z(neutral, conf, False), G)) + oa - dh + shift
    margin, total = ph - pa, ph + pa
    var_a, var_b, sd_t = SIGMA.get(league, SIGMA["ncaam"])[_phase(game_date)]
    sd_m = math.sqrt(var_a + var_b * margin * margin)
    per = PERIOD.get(league) or PERIOD["ncaam"]
    hfa = 0.0 if neutral else (G[1] - G[2]) if conf else (G[3] - G[4])
    return {
        "home_expected": ph, "away_expected": pa,
        "margin": margin, "total": total,
        "ml_home": _ncdf(margin / sd_m),
        "margin_std": sd_m, "total_std": sd_t,
        "home_rating": {"off": oh, "def": dh, "net": oh + dh, "games": nh},
        "away_rating": {"off": oa, "def": da, "net": oa + da, "games": na},
        "home_court": round(float(hfa), 2), "neutral": bool(neutral), "conf_game": bool(conf),
        "season": f["season"], "phase": _phase(game_date), "fit_finals": f["n_final"],
        "globals": {k: round(float(v), 3) for k, v in zip(GLOBALS, G)},
        "period": {
            "label": per["label"],
            "margin": per["k_m"] * margin, "total": per["c_t"] + per["k_t"] * total,
            "margin_std": per["sd_m"], "total_std": per["sd_t"],
        },
    }


def cover_prob(margin, sd, home_pt):
    """P(home covers ``home_pt``) -- home spread negative when home is favored."""
    return _ncdf((margin + float(home_pt)) / sd)


def over_prob(total, sd, line):
    return _ncdf((total - float(line)) / sd)


# ── diagnostics ─────────────────────────────────────────────────

def top_teams(league, asof=None, n=25):
    """Best ``n`` D1 teams by net rating (O + D), as of ``asof`` (default today ET)."""
    if asof is None:
        from .._tz import et_today_str
        asof = et_today_str()
    f = fit_asof(league, asof)
    conn = get_conn(league)
    names = {int(r[0]): r[1] for r in conn.execute("SELECT id, name FROM teams")}
    rows = [(t, float(f["O"][i]) + float(f["D"][i]), float(f["O"][i]), float(f["D"][i]),
             f["games"].get(t, 0)) for t, i in f["ix"].items() if i != f["nd"]]
    rows.sort(key=lambda r: -r[1])
    return [{"team": names.get(t, str(t)), "net": round(net, 1), "off": round(o, 1),
             "def": round(d, 1), "games": gm} for t, net, o, d, gm in rows[:n]]


if __name__ == "__main__":
    import sys
    lg = sys.argv[1] if len(sys.argv) > 1 else "ncaam"
    asof = sys.argv[2] if len(sys.argv) > 2 else datetime.now().strftime("%Y-%m-%d")
    t0 = time.time()
    for r in top_teams(lg, asof, 25):
        print(r)
    print("fit", round(time.time() - t0, 2), "s;", fit_asof(lg, asof)["G"])
