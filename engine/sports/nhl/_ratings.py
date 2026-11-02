"""NHL ratings model (MintyBot 2026-10-26, Austin "Let's make sure NHL is up to snuff").

The source of every NHL market probability. It replaces the factor/MC/GBM blend, whose legs
were broken: the factor model's home-win ran .23-.69 and its totals 3-18 goals (12% of
predictions above 8), while the MC and GBM legs were near-constant (home ~.51, total ~6.35).
On October 2026's 173 games the old blend's moneyline log-loss was .70, worse than a coin flip
(this model .656, DraftKings' close .650).

  1. Goal ratings: weighted ridge Poisson on REGULATION goals (periods 1-3) with team attack /
     defense and home ice. Time decay half-life 180 d, prior-season games x0.3, ridge 25.
  2. Shot ratings: the same fit on shots on goal (half-life 120 d, prior season x0.5, ridge 8).
  3. Stack (Poisson GLM; offset = the goal fit's league rate):
        log lam = log mu + b0 + b1 log(goal-rating ratio) + b2 log(shot-rating ratio)
                  + b3 back-to-back(self) + b4 back-to-back(opponent)
  4. Score grid: independent Poisson at s*lam, then a late-game transform -- empty-net goals and
     late tying goals from 1- and 2-goal leads, fit by likelihood (plain Poisson put 30% of games
     at a 1-goal regulation margin; the league plays 18%). Regulation ties go to OT/SO, home .533.
  5. Periods: P1/P2/P3 = share x s*lam, Poisson.
  6. Injuries (2026-10-26 evening, Austin "NHL has injuries and stuff right?"): for games from today on,
     regulars on ESPN's injury report scale both teams' rates (engine.sports.nhl.lineups -- coefficients
     fit to the closing line on 3 seasons of actual lineups). Not applied to past dates (no historical
     injury report). INJURY_LAYER = False reverts.

Walk-forward backtest 2023-11..2026-10 against DraftKings / ESPN BET closing lines (Pi scratchpad
nhlpa/, holdout Oct 2025-Oct 2026, n=1,575): moneyline log-loss .6778 vs the close .6773, puck
line .6270 vs .6270, totals .6956 vs .6898. It also predicts the open->close move (moneyline
slope .25, t=15; totals .43, t=24). Parameters below are the all-games refit (4,136 games).
"""
from __future__ import annotations

import logging
import math
import sqlite3
import threading
import time
from datetime import date, timedelta

import numpy as np

logger = logging.getLogger(__name__)

GOALS = {"H": 180.0, "delta": 0.3, "kappa": 25.0}
SHOTS = {"H": 120.0, "delta": 0.5, "kappa": 8.0}
WINDOW_DAYS = 900
BETA = (0.0084, 0.4167, 0.6483, -0.0773, 0.0477)      # b0, goals, shots, b2b self, b2b opp
LATE = (0.9611, 0.2552, 0.1840, 0.3918, 0.0627)       # s, ENG@1, tie@1, ENG@2, trailer@2
PERIOD_SHARE = (0.3068, 0.3608, 0.3729)
P_OT_HOME = 0.533
REFIT_S = 1800
INJURY_LAYER = True

K = 13
_IDX = np.arange(K + 1)
_LGK = np.array([math.lgamma(k + 1) for k in range(K + 1)])
_DIFF = _IDX[:, None] - _IDX[None, :]

# Arizona (53) -> Utah Hockey Club (59) -> Utah Mammoth (68) is one franchise; ESPN spellings.
_UTAH_IDS = {53, 59, 68}
_ALIAS = {"LA": "LAK", "NJ": "NJD", "SJ": "SJS", "TB": "TBL", "UTAH": "UTA", "ARI": "UTA",
          "PHX": "UTA", "MON": "MTL", "WAS": "WSH", "NAS": "NSH", "CLB": "CBJ", "CAL": "CGY"}

_LOCK = threading.Lock()
_STATE: dict = {}


def canon(abbr: str | None) -> str:
    a = str(abbr or "").upper().strip()
    return _ALIAS.get(a, a)


def _season_of(d: date) -> int:
    return d.year if d.month >= 8 else d.year - 1


def _connect():
    from .db import DB_PATH
    c = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def _fit(Xh, Xa, yh, ya, w, kappa, x0=None, iters=14):
    """Newton on the weighted ridge Poisson (team params shrink to 0; mu/home ice free)."""
    P = Xh.shape[1]
    pen = np.full(P, float(kappa)); pen[:2] = 1e-6
    x = np.zeros(P) if x0 is None else x0.copy()
    if x0 is None:
        x[0] = math.log(max(1e-3, ((yh + ya) * w).sum()) / (2 * w.sum()))
    for _ in range(iters):
        lh = np.exp(Xh @ x); la = np.exp(Xa @ x)
        g = Xh.T @ (w * (yh - lh)) + Xa.T @ (w * (ya - la)) - pen * x
        Hm = (Xh * (w * lh)[:, None]).T @ Xh + (Xa * (w * la)[:, None]).T @ Xa
        Hm[np.diag_indices(P)] += pen
        step = np.linalg.solve(Hm, g)
        x += step
        if np.abs(step).max() < 1e-8:
            break
    return x


def _build(target: date) -> dict | None:
    """Fit both rating sets on every final before ``target`` (inside the window)."""
    con = _connect()
    try:
        abbr_by_id = {r["id"]: canon(r["abbreviation"]) for r in con.execute(
            "SELECT id, abbreviation FROM nhl_teams")}
        for t in _UTAH_IDS:
            abbr_by_id[t] = "UTA"
        cols = {r[1] for r in con.execute("PRAGMA table_info(nhl_games)")}
        neu = "COALESCE(neutral, 0)" if "neutral" in cols else "0"
        start = (target - timedelta(days=WINDOW_DAYS)).isoformat()
        rows = con.execute(
            f"SELECT date, season, home_team_id, away_team_id, {neu} AS neutral, "
            "home_p1 + home_p2 + home_p3 AS rh, away_p1 + away_p2 + away_p3 AS ra, "
            "home_shots, away_shots FROM nhl_games "
            "WHERE status = 'final' AND game_type IN (2, 3) AND date < ? AND date >= ? "
            "AND home_p1 IS NOT NULL AND home_p2 IS NOT NULL AND home_p3 IS NOT NULL "
            "AND away_p1 IS NOT NULL AND away_p2 IS NOT NULL AND away_p3 IS NOT NULL "
            "AND home_shots > 0 AND away_shots > 0 ORDER BY date",
            (target.isoformat(), start)).fetchall()
        last = {}
        for r in con.execute(
                "SELECT home_team_id, away_team_id, MAX(date) d FROM nhl_games "
                "WHERE game_type IN (2, 3) AND date < ? AND date >= ? "
                "GROUP BY home_team_id, away_team_id",
                (target.isoformat(), (target - timedelta(days=10)).isoformat())):
            for t in (r["home_team_id"], r["away_team_id"]):
                a = abbr_by_id.get(t)
                if a and (a not in last or r["d"] > last[a]):
                    last[a] = r["d"]
    finally:
        con.close()
    if len(rows) < 200:
        logger.warning("nhl ratings: only %d usable finals before %s", len(rows), target)
        return None
    teams = sorted({abbr_by_id.get(r["home_team_id"]) for r in rows}
                   | {abbr_by_id.get(r["away_team_id"]) for r in rows} - {None})
    idx = {t: i for i, t in enumerate(teams)}
    n = len(teams)
    keep = [r for r in rows if abbr_by_id.get(r["home_team_id"]) in idx
            and abbr_by_id.get(r["away_team_id"]) in idx]
    G, P = len(keep), 2 + 2 * n
    hi = np.array([idx[abbr_by_id[r["home_team_id"]]] for r in keep])
    ai = np.array([idx[abbr_by_id[r["away_team_id"]]] for r in keep])
    neutral = np.array([float(r["neutral"] or 0) for r in keep])
    g = np.arange(G)
    Xh = np.zeros((G, P)); Xa = np.zeros((G, P))
    Xh[:, 0] = 1; Xh[:, 1] = 1 - neutral; Xh[g, 2 + hi] = 1; Xh[g, 2 + n + ai] = -1
    Xa[:, 0] = 1; Xa[g, 2 + ai] = 1; Xa[g, 2 + n + hi] = -1
    tn = target.toordinal()
    age = np.array([tn - date.fromisoformat(r["date"]).toordinal() for r in keep], float)
    cur = np.array([int(r["season"] or 0) == _season_of(target) for r in keep])
    wg = 0.5 ** (age / GOALS["H"]) * np.where(cur, 1.0, GOALS["delta"])
    ws = 0.5 ** (age / SHOTS["H"]) * np.where(cur, 1.0, SHOTS["delta"])
    xg = _fit(Xh, Xa, np.array([r["rh"] for r in keep], float), np.array([r["ra"] for r in keep], float),
              wg, GOALS["kappa"])
    xs = _fit(Xh, Xa, np.array([r["home_shots"] for r in keep], float),
              np.array([r["away_shots"] for r in keep], float), ws, SHOTS["kappa"])
    return {"xg": xg, "xs": xs, "idx": idx, "n": n, "games": G, "last": last,
            "target": target, "t": time.time()}


def state(target: date) -> dict | None:
    with _LOCK:
        st = _STATE.get(target)
        if st is not None and time.time() - st["t"] < REFIT_S:
            return st
        try:
            st = _build(target)
        except Exception as e:
            logger.warning("nhl ratings fit failed for %s: %s", target, e)
            st = None
        if st is not None:
            _STATE.clear()          # one live date at a time
            _STATE[target] = st
        return st


def _lam(x, n, i, j, neutral):
    eh = x[0] + x[1] * (1 - neutral) + x[2 + i] - x[2 + n + j]
    ea = x[0] + x[2 + j] - x[2 + n + i]
    return math.exp(eh), math.exp(ea)


def lambdas(home: str, away: str, game_date, neutral: int = 0) -> dict | None:
    """Regulation scoring rates for one matchup on its game date (strictly pre-game data)."""
    from .predict import _resolve_b2b_date
    d = _resolve_b2b_date(game_date)
    st = state(d)
    h, a = canon(home), canon(away)
    if st is None or h not in st["idx"] or a not in st["idx"]:
        return None
    n, i, j = st["n"], st["idx"][h], st["idx"][a]
    gh, ga = _lam(st["xg"], n, i, j, neutral)
    sh, sa = _lam(st["xs"], n, i, j, neutral)
    mu_g, mu_s = math.exp(st["xg"][0]), math.exp(st["xs"][0])

    def rest(t):
        ld = st["last"].get(t)
        return (d - date.fromisoformat(ld)).days if ld else 99

    b2b_h, b2b_a = (1.0 if rest(h) == 1 else 0.0), (1.0 if rest(a) == 1 else 0.0)
    b0, b1, b2, b3, b4 = BETA
    lh = mu_g * math.exp(b0 + b1 * math.log(gh / mu_g) + b2 * math.log(sh / mu_s) + b3 * b2b_h + b4 * b2b_a)
    la = mu_g * math.exp(b0 + b1 * math.log(ga / mu_g) + b2 * math.log(sa / mu_s) + b3 * b2b_a + b4 * b2b_h)
    injury = None
    if INJURY_LAYER:
        try:
            from ..._tz import et_today_str
            if d.isoformat() >= et_today_str():
                from ...injuries import fetch_nhl_injuries
                from .lineups import injury_multipliers
                report = {canon(k): v for k, v in (fetch_nhl_injuries() or {}).items()}
                mh, ma, injury = injury_multipliers(h, a, d.isoformat(), report.get(h, []), report.get(a, []))
                lh, la = lh * mh, la * ma
        except Exception as e:
            logger.warning("nhl injury layer skipped for %s @ %s: %s", a, h, e)
    return {"home": lh, "away": la, "goal_rates": (gh, ga), "shot_rates": (sh, sa),
            "mu_goals": mu_g, "mu_shots": mu_s, "b2b": (b2b_h, b2b_a), "date": d.isoformat(),
            "fit_games": st["games"], "injury": injury}


def _grid(lh: float, la: float) -> np.ndarray:
    s, a1, b1, a2, b2 = LATE
    mh, ma = lh * s, la * s
    lp = (-mh + _IDX[:, None] * math.log(mh) - _LGK[:, None]
          - ma + _IDX[None, :] * math.log(ma) - _LGK[None, :])
    M = np.exp(lp)
    M /= M.sum()
    out = M.copy()
    for lead, a, b in ((1, a1, b1), (2, a2, b2)):
        hm, am = (_DIFF == lead), (_DIFF == -lead)
        ha, hb, aa, ab = M * hm * a, M * hm * b, M * am * a, M * am * b
        out -= ha + hb + aa + ab
        out[1:, :] += ha[:-1, :] + ab[:-1, :]
        out[:, 1:] += hb[:, :-1] + aa[:, :-1]
    return out


def _pois(lam: float, kmax: int = 10) -> np.ndarray:
    k = np.arange(kmax + 1)
    return np.exp(-lam + k * math.log(lam) - np.array([math.lgamma(i + 1) for i in k]))


def markets(lh: float, la: float) -> dict:
    """Every NHL market probability from one regulation score grid (+ OT/SO, + periods)."""
    M = _grid(lh, la)
    reg_h = float(M[_DIFF > 0].sum()); reg_a = float(M[_DIFF < 0].sum()); tie = float(M[_DIFF == 0].sum())
    # Final score distribution: a regulation tie i-i ends (i+1)-i home .533, i-(i+1) away.
    F = np.zeros((K + 2, K + 2))
    F[:K + 1, :K + 1] += M * (_DIFF != 0)
    for i in range(K + 1):
        F[i + 1, i] += M[i, i] * P_OT_HOME
        F[i, i + 1] += M[i, i] * (1 - P_OT_HOME)
    FI = np.arange(K + 2)
    tot = np.bincount((FI[:, None] + FI[None, :]).ravel(), weights=F.ravel(), minlength=2 * K + 5)
    fmarg = {}
    for i in range(K + 2):
        for j in range(K + 2):
            if F[i, j] > 0:
                fmarg[i - j] = fmarg.get(i - j, 0.0) + float(F[i, j])
    hgo, ago = F.sum(axis=1), F.sum(axis=0)          # final goals per team
    ks = np.arange(len(tot))
    exp_tot = float((tot * ks).sum())

    def two_way(pmf, line):
        o = float(pmf[np.arange(len(pmf)) > line].sum()); u = float(pmf[np.arange(len(pmf)) < line].sum())
        return o, u

    ou = {}
    for line in np.arange(3.5, 10.0, 0.5):
        o, u = two_way(tot, line)
        ou[f"{line:.1f}"] = {"over": round(o / (o + u), 4), "under": round(u / (o + u), 4),
                             "push": round(1 - o - u, 4)}

    def cover(home_point):
        """P(home final margin + home_point > 0), pushes excluded (integer points)."""
        w = sum(p for m, p in fmarg.items() if m + home_point > 0)
        l = sum(p for m, p in fmarg.items() if m + home_point < 0)
        return w / (w + l) if (w + l) > 0 else 0.5

    pl = {f"home_{pt:+.1f}": round(cover(pt), 4) for pt in (-3.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.5)}

    def tt(pmf):
        out = {}
        for line in (0.5, 1.5, 2.5, 3.5, 4.5, 5.5):
            o, u = two_way(pmf, line)
            out[f"{line:.1f}"] = {"over": round(o, 4), "under": round(u, 4)}
        return out

    s = LATE[0]
    periods, per_period = [], {}
    for k, share in enumerate(PERIOD_SHARE, start=1):
        ph, pa = lh * s * share, la * s * share
        P = np.outer(_pois(ph), _pois(pa))
        D = np.arange(11)[:, None] - np.arange(11)[None, :]
        T = np.bincount((np.arange(11)[:, None] + np.arange(11)[None, :]).ravel(), weights=P.ravel())
        periods.append({"period": f"P{k}", "home": round(ph, 3), "away": round(pa, 3),
                        "total": round(ph + pa, 3)})
        per_period[str(k)] = {
            "expected_total": round(ph + pa, 3),
            "over_0_5": round(float(T[1:].sum()), 4), "over_1_5": round(float(T[2:].sum()), 4),
            "over_2_5": round(float(T[3:].sum()), 4), "over_3_5": round(float(T[4:].sum()), 4),
            "bts_yes": round(float((1 - math.exp(-ph)) * (1 - math.exp(-pa))), 4),
            "winner": {"home": round(float(P[D > 0].sum()), 4), "away": round(float(P[D < 0].sum()), 4),
                       "tie": round(float(P[D == 0].sum()), 4)},
        }
    p1 = per_period["1"]
    odd = float(tot[1::2].sum())
    bts_full = float(F[1:, 1:].sum())
    scores = sorted(((float(F[i, j]), i, j) for i in range(K + 2) for j in range(K + 2)), reverse=True)[:5]
    eh, ea = float((hgo * FI).sum()), float((ago * FI).sum())
    return {
        "expected_score": {"home": round(eh, 2), "away": round(ea, 2)},
        "total": round(exp_tot, 2),
        "spread": round(ea - eh, 2),
        "win_prob": {"home": round(reg_h + tie * P_OT_HOME, 4), "away": round(reg_a + tie * (1 - P_OT_HOME), 4)},
        "regulation": {"home": round(reg_h, 4), "tie": round(tie, 4), "away": round(reg_a, 4)},
        "regulation_draw_prob": round(tie, 4),
        "regulation_tie_prob": round(tie, 4),
        "puck_line": {"home_minus_1_5": pl["home_-1.5"], "home_plus_1_5": pl["home_+1.5"],
                      "away_minus_1_5": round(1 - pl["home_+1.5"], 4),
                      "away_plus_1_5": round(1 - pl["home_-1.5"], 4)},
        "spread_cover": pl,
        "margin_probs": {str(m): round(p, 4) for m, p in sorted(fmarg.items()) if p >= 1e-4},
        "total_probs": {str(t): round(float(tot[t]), 4) for t in range(len(tot)) if tot[t] >= 1e-4},
        "over_under": ou,
        "periods": periods,
        "first_period": {
            "over_05": p1["over_0_5"], "under_05": round(1 - p1["over_0_5"], 4),
            "over_15": p1["over_1_5"], "under_15": round(1 - p1["over_1_5"], 4),
            "over_25": p1["over_2_5"], "under_25": round(1 - p1["over_2_5"], 4),
            "expected_home": periods[0]["home"], "expected_away": periods[0]["away"],
            "expected_total": periods[0]["total"], "home_win": p1["winner"]["home"],
            "away_win": p1["winner"]["away"], "tie": p1["winner"]["tie"], "data_source": "ratings",
        },
        "correct_scores": [{"score": f"{i}-{j}", "prob": round(p, 4)} for p, i, j in scores],
        # Same shape as engine.mc_nhl's summary, so the derivative pickers read one distribution.
        "dist": {
            "team_totals": {"home": {"expected": round(eh, 3), "lines": tt(hgo)},
                            "away": {"expected": round(ea, 3), "lines": tt(ago)}},
            "per_period": per_period,
            "total_oe": {"odd": round(odd, 4), "even": round(1 - odd, 4)},
            "bts_full": {"yes": round(bts_full, 4), "no": round(1 - bts_full, 4)},
            "overtime": {"yes": round(tie, 4), "no": round(1 - tie, 4)},
            "first_period": {"home_win": p1["winner"]["home"], "away_win": p1["winner"]["away"],
                             "expected_total": p1["expected_total"]},
        },
    }


def predict(home: str, away: str, game_date=None, neutral: int = 0) -> dict | None:
    lam = lambdas(home, away, game_date, neutral)
    if not lam:
        return None
    out = markets(lam["home"], lam["away"])
    out["ratings"] = {"lambda_home": round(lam["home"], 4), "lambda_away": round(lam["away"], 4),
                      "goal_rates": [round(v, 4) for v in lam["goal_rates"]],
                      "shot_rates": [round(v, 2) for v in lam["shot_rates"]],
                      "b2b": list(lam["b2b"]), "date": lam["date"], "fit_games": lam["fit_games"],
                      "injury": lam.get("injury")}
    return out


__all__ = ["predict", "lambdas", "markets", "state", "canon"]
