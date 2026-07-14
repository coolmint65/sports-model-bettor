"""Tennis serve+return skill ratings — Bradley-Terry style fit (Stage 0.7).

Each player has two latent parameters per (tour, surface):

    s_i = serve skill   (high → wins more serve points vs avg returner)
    r_i = return skill  (high → wins more return points vs avg server)

Match-level likelihood for a serve point with player i serving and j
returning::

    P(i wins serve point) = sigmoid(s_i - r_j)

Fit by gradient ascent on the binomial log-likelihood across every
match in the corpus, with L2 regularization to pull rare-sample
players toward 0 (= league average).

Why this exists (Stage 0/0.5 failure)
-------------------------------------
Naive serve-rate aggregates and one-pass opponent-adjustment both
failed Stage 0 vs the existing Elo predictor (Brier 0.27 vs 0.22).
Root cause: opponents' RAW return rates are themselves biased
(top players' opponents have artificially low return rates because
THOSE opponents face other top servers). One-pass adjustment can't
decorrelate. Bradley-Terry fits all players simultaneously so the
opponent-quality bias washes out at convergence.

PIT correctness
---------------
Fits are bucketed by (tour, surface, year_month). The backtest harness
asks for ratings as of a match's date; we return the most recent
snapshot strictly BEFORE that month. No look-ahead.

Cache + storage
---------------
Per-snapshot fit is in-process cached. No persistent storage in this
first cut — each fresh process re-fits as needed. ~1k active players
× 2 params × 4 surfaces is a few seconds; cheap relative to the
backtest itself.

CLI::

    python -m engine.tennis_serve_skill --fit atp Hard 2026-05-01
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Iterable

logger = logging.getLogger(__name__)


# ── Constants ──────────────────────────────────────────────────

# Per-player minimum serve-point sample to participate in the fit.
# Below this they fall back to (zeros, league average sigmoid) at
# predict time — no point fitting a noisy 50-pt rating.
MIN_FIT_POINTS = 200

# Gradient-ascent hyperparameters. Step size has to reach the
# converged MLE for serve/return skill — earlier scaling-by-len(kept)
# made effective lr ~2e-5 which left Sinner/Fils-class players still
# pinned near 0 after 30 iters (the simulator then output a 70/65 gap
# instead of the realistic 75/55). Per-observation gradient + a flat
# small lr is the right shape.
LEARNING_RATE = 1e-4
L2_REG = 0.005
N_ITERATIONS = 200
CONVERGENCE_TOL = 1e-4

# Cutoff bucket granularity. Year-month is the right balance —
# refitting per match is too expensive; per year is too coarse.
def _month_bucket(date_str: str) -> str:
    return date_str[:7]  # 'YYYY-MM'


# ── Sigmoid + numerics ─────────────────────────────────────────

def _sigmoid(z: float) -> float:
    if z >= 0:
        ez = math.exp(-z)
        return 1.0 / (1.0 + ez)
    ez = math.exp(z)
    return ez / (1.0 + ez)


# ── Fit ────────────────────────────────────────────────────────

@dataclass
class SkillSnapshot:
    """Per-player (s, r) ratings for a (tour, surface, cutoff_month)."""
    cutoff_month: str
    surface: str
    tour: str
    n_players: int
    n_matches: int
    n_iterations: int
    final_log_likelihood: float
    skills: dict[int, tuple[float, float]]  # player_id → (s, r)


_FIT_CACHE: dict[tuple, SkillSnapshot] = {}


def _load_fit_corpus(tour: str, surface: str,
                      cutoff_date: str) -> list[tuple]:
    """Pull every (i serves, j returns, n_pts, n_won) tuple — both
    halves of each match — for the (tour, surface) corpus before
    cutoff_date. Returns list of (server_id, returner_id, n_pts, n_won)."""
    from .tennis_db import get_conn
    rows = get_conn().execute(
        "SELECT winner_id, loser_id, "
        "       w_svpt, w_1stWon, w_2ndWon, "
        "       l_svpt, l_1stWon, l_2ndWon "
        "FROM tennis_matches "
        "WHERE tour = ? AND surface = ? AND tourney_date < ? "
        "  AND w_svpt IS NOT NULL AND l_svpt IS NOT NULL "
        "  AND winner_id IS NOT NULL AND loser_id IS NOT NULL",
        (tour, surface, cutoff_date),
    ).fetchall()
    out = []
    for r in rows:
        # Two serve sequences per match — winner serving vs loser
        # returning, and the reverse. Both contribute to the fit.
        w_pts = int(r["w_svpt"] or 0)
        if w_pts > 0:
            w_won = int((r["w_1stWon"] or 0) + (r["w_2ndWon"] or 0))
            out.append((r["winner_id"], r["loser_id"], w_pts, w_won))
        l_pts = int(r["l_svpt"] or 0)
        if l_pts > 0:
            l_won = int((r["l_1stWon"] or 0) + (r["l_2ndWon"] or 0))
            out.append((r["loser_id"], r["winner_id"], l_pts, l_won))
    return out


def _filter_active(records: list[tuple]) -> tuple[list[tuple], set[int]]:
    """Keep only players with >= MIN_FIT_POINTS as either server or
    returner. Filtered records exclude any tuple where either side is
    below threshold so the fit doesn't wobble on noise."""
    served_pts: dict[int, int] = {}
    returned_pts: dict[int, int] = {}
    for srv, rtn, n, _ in records:
        served_pts[srv] = served_pts.get(srv, 0) + n
        returned_pts[rtn] = returned_pts.get(rtn, 0) + n
    active = {pid for pid, n in served_pts.items() if n >= MIN_FIT_POINTS}
    active |= {pid for pid, n in returned_pts.items() if n >= MIN_FIT_POINTS}
    kept = [(s, r, n, w) for (s, r, n, w) in records
            if s in active and r in active]
    return kept, active


def fit_snapshot(tour: str, surface: str, cutoff_month: str) -> SkillSnapshot:
    """Fit (s, r) per player using all (tour, surface) matches strictly
    before ``cutoff_month`` (YYYY-MM string).

    cutoff_date is treated as start of the month: matches with
    tourney_date < {cutoff_month}-01 are included.
    """
    cache_key = (tour, surface, cutoff_month)
    cached = _FIT_CACHE.get(cache_key)
    if cached is not None:
        return cached

    cutoff_date = f"{cutoff_month}-01"
    records = _load_fit_corpus(tour, surface, cutoff_date)
    if not records:
        out = SkillSnapshot(cutoff_month=cutoff_month, surface=surface,
                            tour=tour, n_players=0, n_matches=0,
                            n_iterations=0, final_log_likelihood=0.0,
                            skills={})
        _FIT_CACHE[cache_key] = out
        return out

    kept, active = _filter_active(records)
    if not kept:
        out = SkillSnapshot(cutoff_month=cutoff_month, surface=surface,
                            tour=tour, n_players=0, n_matches=0,
                            n_iterations=0, final_log_likelihood=0.0,
                            skills={})
        _FIT_CACHE[cache_key] = out
        return out

    s = {pid: 0.0 for pid in active}
    r = {pid: 0.0 for pid in active}

    last_ll = -float("inf")
    iters_run = 0
    for it in range(N_ITERATIONS):
        # Compute gradients in one pass per record. Both s_srv and
        # r_rtn move per record. Using per-record updates would be
        # SGD; doing a full-batch update is cleaner and converges
        # for problems this small.
        ds = {pid: 0.0 for pid in active}
        dr = {pid: 0.0 for pid in active}
        ll = 0.0
        for srv, rtn, n, w in kept:
            z = s[srv] - r[rtn]
            p = _sigmoid(z)
            # Binomial log-likelihood contribution
            if 0.0 < p < 1.0:
                ll += w * math.log(p) + (n - w) * math.log(1.0 - p)
            # Gradient: d/ds = (w - n*p), d/dr = -(w - n*p)
            grad = w - n * p
            ds[srv] += grad
            dr[rtn] -= grad

        # L2 regularization (pull toward 0)
        for pid in active:
            ds[pid] -= L2_REG * s[pid]
            dr[pid] -= L2_REG * r[pid]

        # Apply update — per-observation step (no bizarre divisor that
        # collapsed effective lr by 100x when matches grew).
        for pid in active:
            s[pid] += LEARNING_RATE * ds[pid]
            r[pid] += LEARNING_RATE * dr[pid]

        iters_run += 1
        improvement = ll - last_ll
        last_ll = ll
        if it > 5 and abs(improvement) < CONVERGENCE_TOL * max(1.0, abs(ll)):
            break

    skills = {pid: (s[pid], r[pid]) for pid in active}
    out = SkillSnapshot(
        cutoff_month=cutoff_month,
        surface=surface,
        tour=tour,
        n_players=len(active),
        n_matches=len(kept),
        n_iterations=iters_run,
        final_log_likelihood=last_ll,
        skills=skills,
    )
    _FIT_CACHE[cache_key] = out
    logger.info(
        "fit_snapshot %s %s %s: n_players=%d n_matches=%d iters=%d ll=%.1f",
        tour, surface, cutoff_month, out.n_players, out.n_matches,
        out.n_iterations, out.final_log_likelihood,
    )
    return out


# ── Public API used by tennis_player_mc ────────────────────────

def get_skill(tour: str, player_id: int, surface: str,
              match_date: str | None = None) -> tuple[float, float] | None:
    """Return (s, r) for a player on a (tour, surface) as of
    match_date. Uses the most recent month-bucket snapshot strictly
    before match_date's month so there's no look-ahead leak.

    Returns None if the player isn't in the fit (sample too small).
    Caller falls back to surface league average."""
    if not match_date:
        return None
    month = _month_bucket(match_date)
    snap = fit_snapshot(tour, surface, month)
    if not snap.skills:
        return None
    return snap.skills.get(int(player_id))


def serve_win_rate_against(tour: str, server_id: int, returner_id: int,
                            surface: str, match_date: str | None,
                            league_prior: float) -> float:
    """P(server wins serve point | facing returner) per the fitted
    model. Uses (s_server - r_returner) sigmoid with the league prior
    baked into both ratings (sigmoid(0)≈0.5; we shift by logit(prior)
    so missing players collapse to the right baseline)."""
    s_pair = get_skill(tour, server_id, surface, match_date)
    r_pair = get_skill(tour, returner_id, surface, match_date)
    s_val = s_pair[0] if s_pair else 0.0
    r_val = r_pair[1] if r_pair else 0.0
    # logit(league_prior) is the baseline — sigmoid maps (s - r + logit(prior)) to a probability.
    logit_prior = math.log(league_prior / (1.0 - league_prior))
    return _sigmoid(s_val - r_val + logit_prior)


def invalidate_cache() -> None:
    _FIT_CACHE.clear()


# ── CLI ────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="Tennis serve/return skill fit")
    p.add_argument("--fit", nargs=3, metavar=("TOUR", "SURFACE", "CUTOFF_MONTH"),
                   help="Fit a snapshot. CUTOFF_MONTH=YYYY-MM")
    p.add_argument("--top", type=int, default=10,
                   help="Show top-N players by serve skill")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    if args.fit:
        tour, surface, month = args.fit
        snap = fit_snapshot(tour, surface, month)
        print(f"\n=== {tour.upper()} {surface} as of {month} ===")
        print(f"  n_players={snap.n_players}  n_matches={snap.n_matches}")
        print(f"  iterations={snap.n_iterations}  log_likelihood={snap.final_log_likelihood:.1f}")
        if not snap.skills:
            return
        ranked = sorted(snap.skills.items(), key=lambda kv: -kv[1][0])
        # Player names lookup
        from .tennis_db import get_conn
        conn = get_conn()
        print(f"\n  Top-{args.top} by serve skill:")
        for pid, (s, r) in ranked[:args.top]:
            row = conn.execute(
                "SELECT name FROM tennis_players WHERE player_id = ? LIMIT 1",
                (pid,),
            ).fetchone()
            name = row["name"] if row else f"pid={pid}"
            print(f"    {name:25s}  s={s:+.3f}  r={r:+.3f}")
        print(f"\n  Top-{args.top} by return skill:")
        for pid, (s, r) in sorted(snap.skills.items(),
                                    key=lambda kv: -kv[1][1])[:args.top]:
            row = conn.execute(
                "SELECT name FROM tennis_players WHERE player_id = ? LIMIT 1",
                (pid,),
            ).fetchone()
            name = row["name"] if row else f"pid={pid}"
            print(f"    {name:25s}  s={s:+.3f}  r={r:+.3f}")


if __name__ == "__main__":
    main()
