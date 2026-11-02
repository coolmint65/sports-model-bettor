"""Real quarter splits for football period pricing (MintyBot 2026-10-25, Austin "A B and C").

The old period pricer drew each team's period points from a Normal (full-game mean x
points-fraction, sigma x sqrt(fraction)) and rounded. Football quarters are LUMPY -- NFL Q1
totals land on 7 (26%), 10 (16%), 3 (12%), 14 (12%), 0 (10%) -- so the Normal mispriced every
low-scoring period: on 102 NFL 2026 games it put Q1 ties at 8% (actual 22%), both teams
scoring in Q1 at 69% (actual 41%) and the Q1 mean at 10.5 (actual 8.75).

Now each simulated full-game TEAM score borrows the quarter split of a REAL team-game that
finished on (nearly) the same points. The pool is every settlement-grade team-game in
``game_stats`` (ESPN box scores, quarter points in ``facts[side]["q_pts"]``); a target score
of v draws from the team-games within +-r points of v, r the smallest radius holding
>= MIN_POOL of them. So period scores are actual football numbers, sum to the sim's final
(exactly whenever r=0), and a team's quarter shape moves with how much it scores. Home and
away draw independently (validated: NFL Q1 tie / both-score rates come out right).

Period conventions match the settler (``_tracker.period_points``): Q1..Q4 = that quarter
(regulation only), H1 = Q1+Q2, H2 = Q3+Q4 PLUS overtime.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

import numpy as np

_DATA = Path(__file__).resolve().parent.parent.parent / "data" / "football"
MIN_POOL = 25        # team-games behind every target score (the +-r window widens until reached)
MAX_PTS = 90
_TTL_S = 6 * 3600    # rebuild from game_stats at most every 6h (new finals trickle in)
_CACHE: dict = {}    # data league -> (built_at, pool or None)
_LOCK = threading.Lock()
_COLS = {"Q1": (0,), "Q2": (1,), "Q3": (2,), "Q4": (3,), "H1": (0, 1), "H2": (2, 3, 4)}


def _data_league(league) -> str:
    lg = str(league or "").lower()
    return "cfb" if lg.startswith("cfb") else lg     # cfb_fcs (edge-proven segment) shares cfb's history


def build_pool(team_games):
    """``team_games``: iterable of (final_pts, [q1, q2, q3, q4, ot...]). Returns the sampling
    pool, or None when there isn't enough history to trust it."""
    pts, qs = [], []
    for p, q in team_games:
        if p is None or not q or len(q) < 4:
            continue
        q = [int(x or 0) for x in q]
        pts.append(int(p))
        qs.append(q[:4] + [sum(q[4:])])
    if len(pts) < 4 * MIN_POOL:
        return None
    pts = np.asarray(pts)
    idx = []
    for v in range(MAX_PTS + 1):
        r = 0
        sel = np.flatnonzero(pts == v)
        while len(sel) < MIN_POOL and r < MAX_PTS:
            r += 1
            sel = np.flatnonzero(np.abs(pts - v) <= r)
        idx.append(sel)
    return {"q": np.asarray(qs, dtype=float), "idx": idx, "n": len(pts)}


def load_pool(league):
    """The league's pool from ``game_stats`` (cached; None if the history is missing/thin)."""
    lg = _data_league(league)
    c = _CACHE.get(lg)
    if c is not None and time.time() - c[0] < _TTL_S:
        return c[1]
    with _LOCK:
        c = _CACHE.get(lg)
        if c is not None and time.time() - c[0] < _TTL_S:
            return c[1]
        pool = None
        try:
            conn = sqlite3.connect(f"file:{_DATA / (lg + '.db')}?mode=ro", uri=True)
            try:
                rows = conn.execute(
                    "SELECT facts FROM game_stats WHERE final=1 AND facts IS NOT NULL").fetchall()
            finally:
                conn.close()
            team_games = []
            for (f,) in rows:
                try:
                    f = json.loads(f)
                except Exception:
                    continue
                for side in ("home", "away"):
                    s = f.get(side) or {}
                    team_games.append((s.get("pts"), s.get("q_pts")))
            pool = build_pool(team_games)
        except Exception:
            pool = None
        _CACHE[lg] = (time.time(), pool)
        return pool


def split(hh, aa, pool, seed=1337):
    """Per-sim quarter splits for home and away: two (n, 5) arrays of [q1, q2, q3, q4, ot]."""
    rng = np.random.default_rng(seed)
    out = []
    for arr in (hh, aa):
        v = np.clip(np.rint(np.asarray(arr, dtype=float)), 0, MAX_PTS).astype(int)
        pick = np.empty(len(v), dtype=int)
        for val in np.unique(v):
            m = v == val
            cand = pool["idx"][val]
            pick[m] = cand[rng.integers(0, len(cand), int(m.sum()))]
        out.append(pool["q"][pick])
    return out[0], out[1]


def period_points(hq, aq, period):
    """(home, away) points in ``period`` from the per-sim splits."""
    cols = list(_COLS[period])
    return hq[:, cols].sum(axis=1), aq[:, cols].sum(axis=1)


def period_scores(hh, aa, period, league, seed=1337):
    """(home, away) period points per sim off the full-game samples, or None when the league
    has no usable split history (the caller falls back to its Normal scaling)."""
    if period not in _COLS:
        return None
    pool = load_pool(league)
    if pool is None:
        return None
    hq, aq = split(hh, aa, pool, seed)
    return period_points(hq, aq, period)


def quarter_splits(hh, aa, league, seed=1337):
    """Raw per-sim splits (for lead-after-every-quarter), or None without history."""
    pool = load_pool(league)
    if pool is None:
        return None
    return split(hh, aa, pool, seed)
