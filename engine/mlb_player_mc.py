"""
Per-player MC sampler for MLB prop pricing (Phase 2g-ii).

For each (player, stat) combo with enough recent history, samples
``n_sims`` realizations from the locked parametric family
(``_MLB_STAT_DISTRIBUTIONS``) using the player's rolling mean μ.
Returns sorted sample arrays so ``player_prob(line, side)`` is a
single ``searchsorted`` call at pick-evaluation time — no per-line
recomputation.

Why a separate module from ``engine.mlb_scoring`` (the team xR
analytics): scoring uses analytic Poisson/NegBin matrices for the
joint score distribution, which is the right tool for ML / spread /
total. Player props need per-player tail probabilities at arbitrary
thresholds (Over 5.5, 6.5, 7.5, etc.) and a sample-then-CDF approach
costs less than building a parametric CDF per (player, stat, line).

Distribution shape per stat is the locked pooled choice; the
player's own data drives μ — and (for NegBin) drives a per-player
dispersion override when the player has ≥10 observations. Low-data
players fall back to the pooled k.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any

import numpy as np
from scipy import stats as _stats

from .player_props_db import _conn_for
from .distribution_fit import get_distribution

logger = logging.getLogger(__name__)


# ── Stat → (role-filter, name) ────────────────────────────────
# Two-way players (Ohtani) carry both pitching and batting lines.
# The role filter selects which subset of game-log rows feeds μ:
#   "pitcher" → require the pitcher stat to be present (k_p exists)
#   "batter"  → require the batter stat to be present (any of pa/h/hr exists)
# This keeps a pitcher's batter-prop μ from picking up his
# "didn't bat at all" 0s and vice versa.
_PITCHER_STATS = {"k_p", "bb_p", "outs", "er", "h_allowed"}
_BATTER_STATS = {"hr", "h", "tb", "rbi", "r", "sb", "bb_b", "k_b"}


def _is_present(stats: dict, stat_key: str) -> bool:
    """Returns True if this game-log row carries an observation for
    ``stat_key``. For role-bound stats, also requires the player
    actually filled that role this game (k_p missing on bat-only
    rows, h missing on pitch-only rows for non-Ohtani players)."""
    if stat_key not in stats:
        return False
    if stat_key in _PITCHER_STATS:
        # Pitcher stats only count when the player pitched. ``outs``
        # is the most reliable indicator (always > 0 if pitched).
        return stats.get("outs", 0) > 0
    if stat_key in _BATTER_STATS:
        # Batter stats only count when the player had a plate
        # appearance. ``pa`` from the boxscore.
        return stats.get("pa", 0) > 0
    return True


# ── Sampling per family ───────────────────────────────────────

def _sample_negbin(mu: float, k: float, n: int, rng: np.random.Generator) -> np.ndarray:
    """NegBin parameterized as (r, p) with r=k, mean=μ.
    p = k/(k+μ) so mean = r(1-p)/p = μ. Returns int array."""
    if mu <= 0 or k <= 0:
        return np.zeros(n, dtype=np.int64)
    p = k / (k + mu)
    return _stats.nbinom.rvs(k, p, size=n, random_state=rng)


def _sample_poisson(mu: float, n: int, rng: np.random.Generator) -> np.ndarray:
    if mu <= 0:
        return np.zeros(n, dtype=np.int64)
    return rng.poisson(mu, size=n)


def _sample_geometric(mu: float, n: int, rng: np.random.Generator) -> np.ndarray:
    """Geometric on {0, 1, 2, ...} with mean μ. p = 1/(1+μ)."""
    if mu <= 0:
        return np.zeros(n, dtype=np.int64)
    p = 1.0 / (1.0 + mu)
    # scipy geom is 1-indexed; subtract 1 for {0, 1, 2, ...} support.
    return _stats.geom.rvs(p, size=n, random_state=rng) - 1


def _sample_zip(mu: float, pi: float, n: int,
                rng: np.random.Generator) -> np.ndarray:
    """Zero-inflated Poisson. With prob π emit 0, else Poisson(λ)
    where λ = μ/(1-π) so the mixture mean equals μ."""
    if mu <= 0:
        return np.zeros(n, dtype=np.int64)
    if pi <= 0 or pi >= 1:
        return rng.poisson(mu, size=n)
    lam = mu / (1.0 - pi)
    base = rng.poisson(lam, size=n)
    mask = rng.random(n) < pi
    base[mask] = 0
    return base


def _sample(observations: np.ndarray, dist_choice: dict, n: int,
            rng: np.random.Generator) -> np.ndarray:
    """Sample ``n`` realizations from the player's distribution.

    ``observations`` are the player's recent values for this stat
    (used to derive μ and, for NegBin with ≥10 games, a player-
    specific dispersion that overrides the pooled k from the
    locked decision)."""
    family = dist_choice.get("family", "poisson")
    mu = float(np.mean(observations))

    if family == "negbin":
        # Player-specific dispersion when sample is big enough; otherwise
        # the pooled k from the locked decision. var = μ + μ²/k → k =
        # μ²/(var-μ) when var > μ.
        if len(observations) >= 10:
            var = float(np.var(observations))
            if var > mu and mu > 0:
                k = (mu * mu) / (var - mu)
            else:
                # Player isn't actually overdispersed — degrade to pooled k
                # rather than blow up to k=infinity (which collapses to
                # Poisson and ignores expected tail behavior across the rest
                # of the league).
                k = float(dist_choice.get("dispersion_k", 1.0))
        else:
            k = float(dist_choice.get("dispersion_k", 1.0))
        return _sample_negbin(mu, k, n, rng)

    if family == "poisson":
        return _sample_poisson(mu, n, rng)

    if family == "geometric":
        return _sample_geometric(mu, n, rng)

    if family == "zip":
        # Empirical π = excess zeros above what Poisson(μ) predicts.
        # Falls back to Poisson when no excess zeros (or μ too small).
        p_zero_obs = float(np.mean(observations == 0))
        p_zero_poiss = float(np.exp(-mu)) if mu > 0 else 1.0
        pi = max(0.0, (p_zero_obs - p_zero_poiss) / max(1.0 - p_zero_poiss, 1e-9))
        pi = min(pi, 0.95)
        return _sample_zip(mu, pi, n, rng)

    return _sample_poisson(mu, n, rng)


# ── Public builder ────────────────────────────────────────────

def _player_observations(sport: str, player_id: int, stat_key: str,
                          since_date: str) -> np.ndarray:
    """Pull this player's observations for a single stat from
    ``player_game_logs`` since ``since_date``."""
    conn = _conn_for(sport)
    rows = conn.execute(
        "SELECT stats_json FROM player_game_logs "
        "WHERE player_id = ? AND date >= ? "
        "ORDER BY date",
        (int(player_id), since_date),
    ).fetchall()
    out = []
    for r in rows:
        try:
            stats = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        if not _is_present(stats, stat_key):
            continue
        try:
            out.append(float(stats[stat_key]))
        except (TypeError, ValueError):
            continue
    return np.asarray(out, dtype=float)


def build_player_mc(player_id: int,
                    stats: list[str] | None = None,
                    *,
                    sport: str = "mlb",
                    n_sims: int = 10_000,
                    lookback_days: int = 60,
                    min_games: int = 5,
                    seed: int | None = None) -> dict[str, np.ndarray]:
    """Build sorted sample arrays for every stat we have enough history
    on. Returns ``{stat_key: sorted_samples}`` — only includes stats
    where the player has ``min_games`` observations.

    Seeded RNG so two builds for the same player on the same date
    give identical CDFs (matches the deterministic-MC pattern from
    ``engine.derivative_tracker._mc_seed``).
    """
    target_stats = stats or sorted(_PITCHER_STATS | _BATTER_STATS)
    since = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rng = np.random.default_rng(seed if seed is not None else
                                hash((player_id, since)) & 0xFFFFFFFF)
    out: dict[str, np.ndarray] = {}
    for stat_key in target_stats:
        dist = get_distribution(sport, stat_key)
        if dist is None:
            continue
        obs = _player_observations(sport, player_id, stat_key, since)
        if len(obs) < min_games:
            continue
        samples = _sample(obs, dist, n_sims, rng)
        # Sort once so prob_over / prob_under are O(log N).
        out[stat_key] = np.sort(samples)
    return out


def prob_over(samples: np.ndarray, line: float) -> float:
    """P(observed > line) from a sorted sample array.

    Standard Over/Under semantics: ``actual > line`` wins. Sample
    probabilities use the empirical CDF — the fraction of samples
    strictly greater than line."""
    if samples is None or len(samples) == 0:
        return 0.0
    # Count of samples <= line, using the right edge so ties go to
    # "Under" (matches the push convention: equal value pushes, so
    # the Over side does NOT include the line itself).
    n_le = int(np.searchsorted(samples, line, side="right"))
    return 1.0 - (n_le / len(samples))


def prob_under(samples: np.ndarray, line: float) -> float:
    """P(observed < line). Mirrors ``prob_over`` — pushes (equal
    to line) belong to neither side."""
    if samples is None or len(samples) == 0:
        return 0.0
    n_lt = int(np.searchsorted(samples, line, side="left"))
    return n_lt / len(samples)


def player_summary(samples: np.ndarray) -> dict:
    """Quick stats on the sample distribution — used for debugging
    and the optional reasoning text in pick output."""
    if samples is None or len(samples) == 0:
        return {}
    return {
        "n": int(len(samples)),
        "mean": float(np.mean(samples)),
        "median": float(np.median(samples)),
        "std": float(np.std(samples)),
        "p10": float(np.percentile(samples, 10)),
        "p90": float(np.percentile(samples, 90)),
    }


__all__ = [
    "build_player_mc",
    "prob_over", "prob_under", "player_summary",
]
