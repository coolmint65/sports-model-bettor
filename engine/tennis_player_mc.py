"""Tennis serve-level Monte Carlo composer (Stage 0).

Replaces the surface-Elo predictor's match-win-prob output with a
serve-point simulator. Per-player serve-point win rate (from match-level
Sackmann stats) drives a point→game→set→match Monte Carlo. Markets fall
out of sim samples directly:

    p1_win              = fraction of sims P1 wins
    total_games         = mean total games across sims
    total_games_dist    = empirical CDF for over/under markets
    p1_total_games      = mean games won by P1

PIT correctness
---------------
Every aggregate is computed from matches strictly before ``cutoff_date``.
The harness backtest passes the historical match's tourney_date so the
per-player serve stats reflect "what we knew before this match", not a
look-ahead leak.

Cold-start
----------
Players with < MIN_SURFACE_POINTS serve points on the surface fall back
to (a) their cross-surface aggregate, then (b) league-average for the
surface. Same hierarchy as the Bayesian calibration prior — let the
small samples shrink to the population.

Cache
-----
Per-(tour, player_id, surface, cutoff_date) serve stats cached in
process. Backtest runs over hundreds of games — without caching, every
match re-aggregates from 200k+ rows.
"""

from __future__ import annotations

import logging
import math
import random
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


# ── Constants ──────────────────────────────────────────────────

# Below this serve-point sample, fall back up the hierarchy.
MIN_SURFACE_POINTS = 200
MIN_ALL_POINTS = 100

# Bayesian shrinkage: pull observed serve-win rate toward surface
# prior with weight equivalent to PRIOR_N points. Stops a player with
# 50 surve pts from showing 75% serve win and dominating the sim.
PRIOR_N = 100

# Surface-level serve-win priors (computed from full Sackmann corpus
# 2026-05-03 — see backtest_harness analysis). Falls back to 0.60
# for unknown surfaces.
SURFACE_PRIOR = {
    "Hard":   0.608,
    "Clay":   0.591,
    "Grass":  0.631,
    "Carpet": 0.634,
}
DEFAULT_PRIOR = 0.60


# ── Per-player serve aggregates ────────────────────────────────

@dataclass
class ServeStats:
    """Player's serve-point win rate on a (tour, surface) before a date.

    ``shrunk`` is the Bayesian posterior — observed rate pulled toward
    the surface prior with PRIOR_N effective trials. That's what the
    sim consumes; ``raw`` is kept for diagnostics.

    ``opp_adjusted`` is the Stage 0.5 fix: removes the bias from
    "what opponents did this player face" so two players' serve
    rates are comparable on a same-opponent basis."""
    n_points: int
    raw_win_rate: float
    shrunk_win_rate: float
    opp_adjusted_win_rate: float
    source: str  # 'surface' | 'all_surfaces' | 'prior'


_SERVE_CACHE: dict[tuple, ServeStats] = {}
_RETURN_CACHE: dict[tuple, float] = {}  # (tour, player_id, surface, cutoff) → raw return rate


def _shrink(observed: float, n: int, prior: float, prior_n: int = PRIOR_N) -> float:
    """Beta-style shrinkage: posterior = (n*observed + prior_n*prior) / (n + prior_n).
    Equivalent to a Beta(prior_n*prior, prior_n*(1-prior)) prior with
    n Bernoulli trials. Same trick the empirical calibration uses."""
    if n <= 0:
        return prior
    return (n * observed + prior_n * prior) / (n + prior_n)


def _player_return_rate(tour: str, player_id: int, surface: str | None,
                         cutoff_date: str | None) -> float:
    """Player's overall return-point win rate. Used to score opponent
    return strength when adjusting another player's serve rate.

    Returns the surface-specific rate when sample is healthy; falls
    back to all-surface; ultimately to 1 - league_serve_prior."""
    cache_key = (tour, player_id, surface, cutoff_date)
    if cache_key in _RETURN_CACHE:
        return _RETURN_CACHE[cache_key]

    from .tennis_db import get_conn
    conn = get_conn()

    args: list = [tour, player_id, player_id]
    where = ("WHERE tour = ? AND (winner_id = ? OR loser_id = ?) "
             "AND w_svpt IS NOT NULL AND l_svpt IS NOT NULL")
    if surface:
        where += " AND surface = ?"
        args.append(surface)
    if cutoff_date:
        where += " AND tourney_date < ?"
        args.append(cutoff_date)

    rows = conn.execute(
        f"SELECT winner_id, w_svpt, w_1stWon, w_2ndWon, "
        f"       loser_id,  l_svpt, l_1stWon, l_2ndWon "
        f"FROM tennis_matches {where}",
        args,
    ).fetchall()

    return_pts_faced = 0
    return_pts_won = 0
    for r in rows:
        if r["winner_id"] == player_id:
            # Player won the match → opponent served as loser. Player's
            # return points = opponent's serve points NOT won.
            opp_pts = int(r["l_svpt"] or 0)
            opp_won = int((r["l_1stWon"] or 0) + (r["l_2ndWon"] or 0))
        elif r["loser_id"] == player_id:
            opp_pts = int(r["w_svpt"] or 0)
            opp_won = int((r["w_1stWon"] or 0) + (r["w_2ndWon"] or 0))
        else:
            continue
        return_pts_faced += opp_pts
        return_pts_won += (opp_pts - opp_won)

    if return_pts_faced > 0 and return_pts_faced >= MIN_ALL_POINTS:
        rate = return_pts_won / return_pts_faced
    elif surface:
        # Fall back to cross-surface
        rate = _player_return_rate(tour, player_id, None, cutoff_date)
    else:
        # League prior: 1 - serve_prior. Use surface if known, else default.
        prior = SURFACE_PRIOR.get(surface, DEFAULT_PRIOR) if surface else DEFAULT_PRIOR
        rate = 1.0 - prior

    _RETURN_CACHE[cache_key] = rate
    return rate


def _load_player_serve(tour: str, player_id: int, surface: str,
                        cutoff_date: str | None) -> ServeStats:
    """Aggregate the player's serve-point stats from ``tennis_matches``
    on the given surface, using only matches strictly before
    ``cutoff_date``. Falls back the hierarchy on small sample.

    Stage 0.5 fix: also computes ``opp_adjusted_win_rate`` —
    the player's serve rate if they had faced an average-tour returner
    every match, removing opponent-quality bias. This is what the sim
    consumes; raw + shrunk are kept for diagnostics."""
    cache_key = (tour, player_id, surface, cutoff_date)
    cached = _SERVE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    from .tennis_db import get_conn
    conn = get_conn()

    # Pull every match the player played on this surface with full
    # opponent ID + per-match serve stats. We need opponent ids so we
    # can score the return strength they brought.
    base_args: list = [tour, player_id, player_id]
    base_where = ("WHERE tour = ? AND (winner_id = ? OR loser_id = ?) "
                  "AND surface = ? "
                  "AND w_svpt IS NOT NULL AND l_svpt IS NOT NULL")
    args = base_args + [surface]
    if cutoff_date:
        base_where += " AND tourney_date < ?"
        args.append(cutoff_date)

    rows = conn.execute(
        f"SELECT winner_id, w_svpt, w_1stWon, w_2ndWon, "
        f"       loser_id,  l_svpt, l_1stWon, l_2ndWon, "
        f"       tourney_date "
        f"FROM tennis_matches {base_where}",
        args,
    ).fetchall()

    prior = SURFACE_PRIOR.get(surface, DEFAULT_PRIOR)
    tour_avg_return = 1.0 - prior  # league average return win rate on this surface

    pts = won = 0
    # Per-match adjustments — collect deviations to compute opp-adjusted rate.
    adj_deviations: list[tuple[float, int]] = []  # (per_match_adjustment, weight)
    for r in rows:
        if r["winner_id"] == player_id:
            opp_id = r["loser_id"]
            p_pts = int(r["w_svpt"] or 0)
            p_won = int((r["w_1stWon"] or 0) + (r["w_2ndWon"] or 0))
        elif r["loser_id"] == player_id:
            opp_id = r["winner_id"]
            p_pts = int(r["l_svpt"] or 0)
            p_won = int((r["l_1stWon"] or 0) + (r["l_2ndWon"] or 0))
        else:
            continue
        pts += p_pts
        won += p_won
        if p_pts <= 0 or opp_id is None:
            continue
        # Opponent's overall return strength (single number per opponent),
        # cached so this isn't quadratic on backtest. Adjustment = match
        # serve rate corrected for "what avg returner would have done".
        opp_return_rate = _player_return_rate(tour, opp_id, surface,
                                               cutoff_date)
        # Additive correction: per-match adjusted serve rate ≈
        # raw_match_rate + (opp_return_rate - tour_avg_return). Positive
        # correction means opponent was tougher than average → boost the
        # player's score upward.
        match_rate = p_won / p_pts
        adj = match_rate + (opp_return_rate - tour_avg_return)
        adj_deviations.append((adj, p_pts))

    if pts >= MIN_SURFACE_POINTS:
        raw = won / pts
        shrunk = _shrink(raw, pts, prior)
        if adj_deviations:
            tot_w = sum(w for _, w in adj_deviations)
            adj_avg = sum(a * w for a, w in adj_deviations) / tot_w
            opp_adj = _shrink(adj_avg, pts, prior)
        else:
            opp_adj = shrunk
        out = ServeStats(n_points=pts, raw_win_rate=raw,
                         shrunk_win_rate=shrunk,
                         opp_adjusted_win_rate=opp_adj,
                         source="surface")
        _SERVE_CACHE[cache_key] = out
        return out

    # Cross-surface fallback. Same query without the surface filter.
    args_all = base_args[:]
    where_all = ("WHERE tour = ? AND (winner_id = ? OR loser_id = ?) "
                 "AND w_svpt IS NOT NULL AND l_svpt IS NOT NULL")
    if cutoff_date:
        where_all += " AND tourney_date < ?"
        args_all.append(cutoff_date)
    rows_all = conn.execute(
        f"SELECT winner_id, w_svpt, w_1stWon, w_2ndWon, "
        f"       loser_id,  l_svpt, l_1stWon, l_2ndWon "
        f"FROM tennis_matches {where_all}",
        args_all,
    ).fetchall()

    pts_all = won_all = 0
    adj_dev_all: list[tuple[float, int]] = []
    for r in rows_all:
        if r["winner_id"] == player_id:
            opp_id = r["loser_id"]
            p_pts = int(r["w_svpt"] or 0)
            p_won = int((r["w_1stWon"] or 0) + (r["w_2ndWon"] or 0))
        elif r["loser_id"] == player_id:
            opp_id = r["winner_id"]
            p_pts = int(r["l_svpt"] or 0)
            p_won = int((r["l_1stWon"] or 0) + (r["l_2ndWon"] or 0))
        else:
            continue
        pts_all += p_pts
        won_all += p_won
        if p_pts <= 0 or opp_id is None:
            continue
        opp_return_rate = _player_return_rate(tour, opp_id, None, cutoff_date)
        match_rate = p_won / p_pts
        adj_dev_all.append(
            (match_rate + (opp_return_rate - (1.0 - DEFAULT_PRIOR)), p_pts)
        )

    if pts_all >= MIN_ALL_POINTS:
        raw = won_all / pts_all
        shrunk = _shrink(raw, pts_all, prior)
        if adj_dev_all:
            tot_w = sum(w for _, w in adj_dev_all)
            adj_avg = sum(a * w for a, w in adj_dev_all) / tot_w
            opp_adj = _shrink(adj_avg, pts_all, prior)
        else:
            opp_adj = shrunk
        out = ServeStats(n_points=pts_all, raw_win_rate=raw,
                         shrunk_win_rate=shrunk,
                         opp_adjusted_win_rate=opp_adj,
                         source="all_surfaces")
    else:
        out = ServeStats(n_points=pts_all, raw_win_rate=prior,
                         shrunk_win_rate=prior,
                         opp_adjusted_win_rate=prior, source="prior")
    _SERVE_CACHE[cache_key] = out
    return out


def invalidate_cache() -> None:
    """Drop the in-process serve-stats cache. Use between separate
    runs that span very different cutoff dates (e.g. backtest re-runs)."""
    _SERVE_CACHE.clear()


# ── Match simulator ───────────────────────────────────────────

def _simulate_game(p_serve_win: float, rng: random.Random) -> tuple[int, int]:
    """One service game from server's perspective. Returns
    (server_points, returner_points). Standard scoring with deuce —
    win game when first to 4 with margin >= 2."""
    s = r = 0
    while True:
        if rng.random() < p_serve_win:
            s += 1
        else:
            r += 1
        if s >= 4 and s - r >= 2:
            return s, r
        if r >= 4 and r - s >= 2:
            return s, r


def _simulate_tiebreak(p_a_serve_win: float, p_b_serve_win: float,
                        a_serves_first: bool,
                        rng: random.Random) -> tuple[int, int]:
    """First to 7 with margin >= 2. A serves first point, then B
    serves 2, A serves 2, alternating pairs. Returns (a_points, b_points)."""
    a = b = 0
    served = 0
    a_turn = a_serves_first  # A's turn to serve this point
    while True:
        # Determine who is serving this point
        # First point: a_serves_first; then alternate every 2 points
        # Actual pattern: A,BB,AA,BB,AA,...
        # Implement as: first point is a if a_serves_first; then groups of 2
        served += 1
        # Serving rules per ITF: server 1 serves point 1; server 2 serves
        # points 2-3; server 1 serves 4-5; server 2 serves 6-7; etc.
        # equivalent: ((served-1) // 2) % 2 == 0 → first server's pair
        first_pair = ((served - 1) // 2) % 2 == 0
        # Adjusted for who served first
        if a_serves_first:
            a_serving = (served == 1) or (not first_pair)
        else:
            a_serving = (served != 1) and (not first_pair)
        # Simpler: who gets the point based on serve%
        p_serve = p_a_serve_win if a_serving else p_b_serve_win
        if rng.random() < p_serve:
            if a_serving: a += 1
            else: b += 1
        else:
            if a_serving: b += 1
            else: a += 1
        if a >= 7 and a - b >= 2:
            return a, b
        if b >= 7 and b - a >= 2:
            return a, b


def _simulate_set(p1_serve: float, p2_serve: float,
                   p1_serves_first: bool, rng: random.Random) -> tuple[int, int, bool]:
    """Standard set: first to 6 games, margin >= 2, tiebreak at 6-6.
    Returns (p1_games, p2_games, p1_wins_set)."""
    p1_g = p2_g = 0
    p1_serving = p1_serves_first
    while True:
        if p1_serving:
            s, r = _simulate_game(p1_serve, rng)
            if s > r: p1_g += 1
            else:     p2_g += 1
        else:
            s, r = _simulate_game(p2_serve, rng)
            if s > r: p2_g += 1
            else:     p1_g += 1
        p1_serving = not p1_serving
        # Set won?
        if p1_g >= 6 and p1_g - p2_g >= 2:
            return p1_g, p2_g, True
        if p2_g >= 6 and p2_g - p1_g >= 2:
            return p1_g, p2_g, False
        # Tiebreak at 6-6
        if p1_g == 6 and p2_g == 6:
            tb_p1, tb_p2 = _simulate_tiebreak(
                p1_serve, p2_serve, p1_serving, rng,
            )
            if tb_p1 > tb_p2:
                return 7, 6, True
            else:
                return 6, 7, False


def _simulate_match(p1_serve: float, p2_serve: float,
                     best_of: int, rng: random.Random) -> dict:
    sets_to_win = 3 if best_of == 5 else 2
    p1_sets = p2_sets = 0
    p1_total_games = p2_total_games = 0
    set_scores = []
    p1_serves_first_set = True   # alternate per set
    while p1_sets < sets_to_win and p2_sets < sets_to_win:
        p1_g, p2_g, p1_won = _simulate_set(p1_serve, p2_serve,
                                            p1_serves_first_set, rng)
        p1_total_games += p1_g
        p2_total_games += p2_g
        if p1_won: p1_sets += 1
        else:      p2_sets += 1
        set_scores.append((p1_g, p2_g))
        # Server alternates each set start; flip whoever served LAST in
        # the set by counting games played.
        if (p1_g + p2_g) % 2 == 1:
            p1_serves_first_set = not p1_serves_first_set
    return {
        "p1_won":         p1_sets > p2_sets,
        "p1_sets":        p1_sets,
        "p2_sets":        p2_sets,
        "p1_total_games": p1_total_games,
        "p2_total_games": p2_total_games,
        "total_games":    p1_total_games + p2_total_games,
        "set_scores":     set_scores,
    }


# ── Public API ────────────────────────────────────────────────

def predict_match_mc(tour: str, p1_id: int, p2_id: int, *,
                      surface: str, best_of: int,
                      cutoff_date: str | None = None,
                      n_sims: int = 10_000,
                      seed: int | None = None,
                      use_bradley_terry: bool = True) -> dict:
    """Run serve-level MC and return market probabilities + summary.

    use_bradley_terry: if True (default, Stage 0.7), use the fitted
    serve+return ratings from engine/tennis_serve_skill instead of the
    raw aggregates. Fits per (tour, surface, year_month) snapshot to
    avoid look-ahead. Falls back to opp_adjusted aggregate when the
    pair isn't in the fit (small-sample players)."""
    p1_stats = _load_player_serve(tour, p1_id, surface, cutoff_date)
    p2_stats = _load_player_serve(tour, p2_id, surface, cutoff_date)

    if use_bradley_terry and cutoff_date:
        from .tennis_serve_skill import serve_win_rate_against
        prior = SURFACE_PRIOR.get(surface, DEFAULT_PRIOR)
        # Each player's serve rate is conditional on the SPECIFIC opponent —
        # so we ask BT for "P(p1 wins serve point | facing p2)" directly,
        # not for an "opp-neutral" rate. This fully consumes the rating's
        # head-to-head structure.
        p1_serve = serve_win_rate_against(tour, p1_id, p2_id, surface,
                                          cutoff_date, prior)
        p2_serve = serve_win_rate_against(tour, p2_id, p1_id, surface,
                                          cutoff_date, prior)
        skill_source = "bradley_terry"
    else:
        # Stage 0.5 fallback: opp_adjusted aggregate.
        p1_serve = p1_stats.opp_adjusted_win_rate
        p2_serve = p2_stats.opp_adjusted_win_rate
        skill_source = "opp_adjusted"

    rng = random.Random(seed) if seed is not None else random.Random()
    p1_wins = 0
    total_games_sum = 0.0
    p1_games_sum = 0.0
    set_diff_sum = 0.0
    total_games_samples: list[int] = []
    for _ in range(n_sims):
        m = _simulate_match(p1_serve, p2_serve, best_of, rng)
        if m["p1_won"]:
            p1_wins += 1
        total_games_sum += m["total_games"]
        p1_games_sum += m["p1_total_games"]
        set_diff_sum += (m["p1_sets"] - m["p2_sets"])
        total_games_samples.append(m["total_games"])

    p1_win_prob = p1_wins / n_sims
    return {
        "p1_win":              p1_win_prob,
        "p2_win":              1.0 - p1_win_prob,
        "total_games":         total_games_sum / n_sims,
        "p1_total_games":      p1_games_sum / n_sims,
        "p2_total_games":      total_games_sum / n_sims - p1_games_sum / n_sims,
        "set_margin_expected": set_diff_sum / n_sims,
        "total_games_samples": total_games_samples,
        "factors": {
            "p1_serve_win_rate": round(p1_serve, 4),
            "p2_serve_win_rate": round(p2_serve, 4),
            "p1_n_points":       p1_stats.n_points,
            "p2_n_points":       p2_stats.n_points,
            "p1_source":         p1_stats.source,
            "p2_source":         p2_stats.source,
            "skill_source":      skill_source,
        },
    }


def prob_total_games_over(samples: list[int], line: float) -> float:
    """For a posted line like 22.5, fraction of sims with total > line."""
    if not samples:
        return 0.5
    return sum(1 for s in samples if s > line) / len(samples)


__all__ = [
    "predict_match_mc",
    "prob_total_games_over",
    "ServeStats",
    "invalidate_cache",
]
