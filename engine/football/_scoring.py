"""Opponent-adjusted offense/defense SCORING ratings for football totals.

Elo (``_elo.py``) rates relative strength for who-WINS (margin / win-prob); it is
the wrong tool for totals -- it pins expected_total to the league average, so every
game projects ~53 regardless of matchup. This module fits a per-team OFFENSE and
DEFENSE points rating by ridge regression over the game-score history, so a game's
total = home_off vs away_def + away_off vs home_def -- a real per-game number that
spreads with the matchup. Ridge regularizes: thin-sample teams shrink toward the
league mean (correct -- we don't yet know they're extreme) while teams with a real
record differentiate. Keep Elo for the margin; use this for the TOTAL only.
(MintyBot 2026-10-20, Austin: "elo's the wrong call for scoring." Right.)

Recency-weighted (half-life ~ one season) so current form dominates. Cached on the
finalized-game count like ``_elo.replay`` -- refits only when a new game lands.
Holdout (last 14d, 2026-10-20): total RMSE 17.23 vs 17.85 league-avg baseline; the
ratings reproduce the actual elite O's/D's; projected totals spread 37-74 not ~53.
"""
from __future__ import annotations
import datetime
from ._db import get_conn

_LAM = 5.0          # ridge strength (CFB holdout-best 2026-10-20; tunable)
_HL_DAYS = 150.0    # recency half-life (~ one CFB season)
_CACHE: dict = {}   # league -> (n_final_games, model)


def _pdate(s):
    try:
        return datetime.date.fromisoformat(str(s)[:10])
    except Exception:
        return None


def _fit(rows, lam=_LAM, hl=_HL_DAYS):
    import numpy as np
    games = []
    teams: dict = {}
    for r in rows:
        d = _pdate(r[4])
        if d is None:
            continue
        h, a = int(r[0]), int(r[1])
        games.append((h, a, int(r[2]), int(r[3]), d))
        teams.setdefault(h, len(teams))
        teams.setdefault(a, len(teams))
    if not games:
        return None
    T = len(teams)
    latest = max(g[4] for g in games)
    ncol = 2 + 2 * T
    def oi(t): return 2 + teams[t]
    def di(t): return 2 + T + teams[t]
    n = len(games)
    X = np.zeros((2 * n, ncol)); y = np.zeros(2 * n); W = np.zeros(2 * n)
    for k, (h, a, hs, as_, d) in enumerate(games):
        w = 0.5 ** ((latest - d).days / hl)
        i = 2 * k; j = 2 * k + 1
        X[i, 0] = 1; X[i, 1] = 1; X[i, oi(h)] = 1; X[i, di(a)] = 1; y[i] = hs; W[i] = w
        X[j, 0] = 1; X[j, oi(a)] = 1; X[j, di(h)] = 1; y[j] = as_; W[j] = w
    R = np.ones(ncol); R[0] = 0; R[1] = 0           # don't penalize mu / hfa
    beta = np.linalg.solve(X.T @ (X * W[:, None]) + lam * np.diag(R), X.T @ (W * y))
    return {"mu": float(beta[0]), "hfa": float(beta[1]),
            "off": {t: float(beta[oi(t)]) for t in teams},
            "def": {t: float(beta[di(t)]) for t in teams}}


def replay_scoring(league, conn=None):
    """Fit (or return cached) offense/defense scoring ratings for ``league``.
    Returns {mu, hfa, off:{team_id:rating}, def:{team_id:rating}} or None."""
    if conn is None:
        conn = get_conn(league)
    try:
        n = conn.execute("SELECT COUNT(*) FROM games WHERE status='final' "
                         "AND home_score IS NOT NULL AND away_score IS NOT NULL").fetchone()[0]
    except Exception:
        n = None
    c = _CACHE.get(league)
    if c is not None and n is not None and c[0] == n:
        return c[1]
    rows = conn.execute(
        "SELECT home_team_id,away_team_id,home_score,away_score,date FROM games "
        "WHERE status='final' AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date ASC").fetchall()
    model = _fit(rows)
    if model is not None and n is not None:
        _CACHE[league] = (n, model)
    return model


def expected_scoring(league, home_id, away_id, conn=None):
    """(home_pts, away_pts) from the scoring ratings, or None if unavailable.
    HFA is a constant on the home score (absorbed by the league calibration -- it
    shifts the home/away split, not the cross-game TOTAL spread this feeds)."""
    m = replay_scoring(league, conn)
    if not m:
        return None
    off = m["off"]; dfn = m["def"]; mu = m["mu"]; hfa = m["hfa"]
    hp = mu + off.get(int(home_id), 0.0) + dfn.get(int(away_id), 0.0) + hfa
    ap = mu + off.get(int(away_id), 0.0) + dfn.get(int(home_id), 0.0)
    return (hp, ap)
