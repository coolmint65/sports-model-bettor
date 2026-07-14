"""
Per-stat distribution fitter for the per-player MC extension
(Phase 2g-ii / 2h-ii / 2i-ii).

Decides which parametric family best describes a player-stat's
per-game distribution, so the MC loop can sample from the right shape
when projecting prop probabilities. Defaults like "Poisson for batter
HR" looked plausible but missed the over-dispersion that drives both
the zero-inflation and the multi-HR tail — wrong family means our
prop edges are wrong by 5-10% for the high-stakes stats.

Approach: pool every (player, game) observation for a stat, fit
candidate families on the pooled data, score by AIC, and check
goodness of fit with K-S. The mean variance budget is what matters
for MC (each player gets their own μ from rolling history; the family
+ dispersion describes shape around that μ).

Entry point::

    summarize_stat(sport, stat_key) -> dict

prints / returns the leaderboard so the model author can see the gap
between candidates instead of trusting one number.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from typing import Any, Callable

logger = logging.getLogger(__name__)

try:
    import numpy as np
    from scipy import stats as _stats
    from scipy.optimize import minimize
    _HAVE_SCIPY = True
except ImportError:
    _HAVE_SCIPY = False


def _conn_for(sport: str) -> sqlite3.Connection:
    if sport == "mlb":
        from .db import get_conn
    elif sport == "nhl":
        from .nhl_db import get_conn
    elif sport == "nba":
        from .nba_db import get_conn
    else:
        raise ValueError(f"unknown sport: {sport}")
    return get_conn()


def pull_observations(sport: str, stat_key: str,
                      min_games_per_player: int = 5,
                      since_date: str | None = None) -> "np.ndarray":
    """Returns a 1-D numpy array of every observed value for `stat_key`
    across all (player, game) rows in player_game_logs. Filters out
    players with fewer than ``min_games_per_player`` observations so
    the fit doesn't get dragged by tiny-sample noise."""
    conn = _conn_for(sport)
    where = []
    params: list = []
    if since_date:
        where.append("date >= ?")
        params.append(since_date)
    sql = "SELECT player_id, stats_json FROM player_game_logs"
    if where:
        sql += " WHERE " + " AND ".join(where)
    rows = conn.execute(sql, params).fetchall()

    by_player: dict[int, list[float]] = {}
    for r in rows:
        pid = r["player_id"] if hasattr(r, "keys") else r[0]
        sj = r["stats_json"] if hasattr(r, "keys") else r[1]
        try:
            stats = json.loads(sj or "{}")
        except (TypeError, ValueError):
            continue
        if stat_key not in stats:
            continue
        v = stats[stat_key]
        if v is None:
            continue
        try:
            by_player.setdefault(pid, []).append(float(v))
        except (TypeError, ValueError):
            continue

    pooled: list[float] = []
    for pid, vals in by_player.items():
        if len(vals) >= min_games_per_player:
            pooled.extend(vals)
    return np.asarray(pooled, dtype=float) if _HAVE_SCIPY else pooled


# ── Candidate families ────────────────────────────────────────
# Each candidate returns (name, neg_log_likelihood, aic, bic, params,
# pmf_callable). pmf_callable maps int k → P(X=k) for K-S testing
# against the observed empirical CDF.

def _fit_poisson(data) -> dict:
    mu = float(np.mean(data))
    if mu <= 0:
        return {"name": "Poisson", "ll": -float("inf"), "aic": float("inf"),
                "bic": float("inf"), "params": {"mu": mu}, "pmf": None}
    ll = float(np.sum(_stats.poisson.logpmf(data, mu)))
    k = 1
    n = len(data)
    return {
        "name": "Poisson",
        "ll": ll,
        "aic": 2 * k - 2 * ll,
        "bic": k * math.log(n) - 2 * ll,
        "params": {"mu": mu},
        "pmf": lambda x: _stats.poisson.pmf(x, mu),
    }


def _fit_negbin(data) -> dict:
    """NegBin(r, p) parametrization. r = dispersion (smaller r =
    more overdispersed). Solved via MLE on (r, p) jointly."""
    mu = float(np.mean(data))
    var = float(np.var(data))
    if var <= mu or mu <= 0:
        # No overdispersion — NegBin reduces to Poisson, will lose
        # by AIC penalty for the extra parameter.
        return {"name": "NegBin", "ll": -float("inf"), "aic": float("inf"),
                "bic": float("inf"), "params": {}, "pmf": None}
    # Method-of-moments init
    r0 = mu * mu / (var - mu)
    p0 = r0 / (r0 + mu)
    def neg_ll(params):
        r, p = params
        if r <= 0 or p <= 0 or p >= 1:
            return 1e10
        return -float(np.sum(_stats.nbinom.logpmf(data, r, p)))
    res = minimize(neg_ll, [r0, p0], method="Nelder-Mead",
                   options={"xatol": 1e-4, "fatol": 1e-4, "maxiter": 500})
    r, p = res.x
    if r <= 0 or p <= 0 or p >= 1:
        return {"name": "NegBin", "ll": -float("inf"), "aic": float("inf"),
                "bic": float("inf"), "params": {}, "pmf": None}
    ll = -float(res.fun)
    k = 2
    n = len(data)
    return {
        "name": "NegBin",
        "ll": ll,
        "aic": 2 * k - 2 * ll,
        "bic": k * math.log(n) - 2 * ll,
        "params": {"r": float(r), "p": float(p),
                   "mu": float(r * (1 - p) / p),
                   "dispersion_k": float(r)},
        "pmf": lambda x: _stats.nbinom.pmf(x, r, p),
    }


def _fit_zip(data) -> dict:
    """Zero-inflated Poisson. Mixture: with prob π emit 0; else draw
    from Poisson(λ). Useful when data has more zeros than Poisson
    expects (rare-event stats like HR, SB)."""
    n = len(data)
    if n == 0:
        return {"name": "ZIP", "ll": -float("inf"), "aic": float("inf"),
                "bic": float("inf"), "params": {}, "pmf": None}
    # MLE init: π = excess zeros above Poisson, λ = mean of non-zero block
    p0_obs = float(np.mean(data == 0))
    mu0 = float(np.mean(data))
    if mu0 <= 0 or p0_obs <= math.exp(-mu0):
        # No excess zeros — ZIP reduces to Poisson
        return {"name": "ZIP", "ll": -float("inf"), "aic": float("inf"),
                "bic": float("inf"), "params": {}, "pmf": None}
    pi0 = (p0_obs - math.exp(-mu0)) / (1 - math.exp(-mu0))
    lam0 = mu0 / (1 - pi0) if pi0 < 1 else mu0
    def neg_ll(params):
        pi, lam = params
        if pi < 0 or pi >= 1 or lam <= 0:
            return 1e10
        zero_prob = pi + (1 - pi) * math.exp(-lam)
        if zero_prob <= 0:
            return 1e10
        ll_z = math.log(zero_prob) * float(np.sum(data == 0))
        nz = data[data > 0]
        ll_nz = float(np.sum(np.log(1 - pi) + _stats.poisson.logpmf(nz, lam)))
        return -(ll_z + ll_nz)
    res = minimize(neg_ll, [max(pi0, 0.01), max(lam0, 0.01)],
                   method="Nelder-Mead",
                   options={"xatol": 1e-4, "fatol": 1e-4, "maxiter": 500})
    pi, lam = res.x
    if pi < 0 or pi >= 1 or lam <= 0:
        return {"name": "ZIP", "ll": -float("inf"), "aic": float("inf"),
                "bic": float("inf"), "params": {}, "pmf": None}
    ll = -float(res.fun)
    k = 2
    return {
        "name": "ZIP",
        "ll": ll,
        "aic": 2 * k - 2 * ll,
        "bic": k * math.log(n) - 2 * ll,
        "params": {"pi": float(pi), "lambda": float(lam),
                   "mu": float((1 - pi) * lam)},
        "pmf": lambda x: (pi if x == 0 else 0.0) + (1 - pi) * float(_stats.poisson.pmf(x, lam)),
    }


def _fit_geometric(data) -> dict:
    """Geometric for stats where most observations are 0 with
    a long tail (rarely used but worth checking for SB/HR)."""
    mu = float(np.mean(data))
    if mu <= 0:
        return {"name": "Geometric", "ll": -float("inf"), "aic": float("inf"),
                "bic": float("inf"), "params": {}, "pmf": None}
    p = 1 / (1 + mu)
    # scipy.stats.geom is 1-indexed; we use the "number of failures"
    # convention which is k=0,1,2,...
    ll = float(np.sum(np.log(p) + data * np.log(1 - p)))
    k = 1
    n = len(data)
    return {
        "name": "Geometric",
        "ll": ll,
        "aic": 2 * k - 2 * ll,
        "bic": k * math.log(n) - 2 * ll,
        "params": {"p": float(p), "mu": mu},
        "pmf": lambda x: float(p * (1 - p) ** x),
    }


def _ks_test(data, pmf: Callable[[int], float], max_k: int = 50) -> float:
    """One-sample K-S statistic between empirical CDF and the fitted
    family's CDF. Lower is better; D < 0.05 is "very close fit."""
    if pmf is None:
        return float("inf")
    n = len(data)
    sorted_data = np.sort(data)
    # Build fitted CDF
    fitted_pmf = np.array([pmf(k) for k in range(max_k + 1)])
    fitted_cdf = np.cumsum(fitted_pmf)
    # For each observation, compare empirical vs fitted CDF
    max_diff = 0.0
    for k_int in range(max_k + 1):
        empirical = float(np.sum(sorted_data <= k_int)) / n
        fitted = float(fitted_cdf[k_int]) if k_int < len(fitted_cdf) else 1.0
        max_diff = max(max_diff, abs(empirical - fitted))
    return max_diff


_CANDIDATES = [_fit_poisson, _fit_negbin, _fit_zip, _fit_geometric]


def fit_all(data) -> list[dict]:
    """Fit every candidate, return list sorted by AIC (lowest first)."""
    results = []
    for fitter in _CANDIDATES:
        try:
            r = fitter(data)
        except Exception as e:
            logger.warning("%s failed: %s", fitter.__name__, e)
            continue
        if r["pmf"] is not None:
            r["ks"] = _ks_test(data, r["pmf"])
        else:
            r["ks"] = float("inf")
        results.append(r)
    return sorted(results, key=lambda r: r["aic"])


def summarize_stat(sport: str, stat_key: str,
                   min_games_per_player: int = 5,
                   since_date: str | None = None) -> dict:
    """Print + return the leaderboard for one (sport, stat). Includes
    descriptive stats so you can sanity-check the fit choice."""
    if not _HAVE_SCIPY:
        raise RuntimeError("scipy + numpy required")
    data = pull_observations(sport, stat_key, min_games_per_player, since_date)
    if len(data) < 30:
        return {"stat": stat_key, "n": len(data),
                "warning": "insufficient data (<30 observations)"}

    desc = {
        "n": int(len(data)),
        "mean": float(np.mean(data)),
        "var": float(np.var(data)),
        "p_zero": float(np.mean(data == 0)),
        "max": int(np.max(data)),
    }
    desc["overdispersion"] = round(desc["var"] / desc["mean"], 2) if desc["mean"] > 0 else float("inf")
    fits = fit_all(data)
    return {
        "sport": sport, "stat": stat_key,
        "descriptive": desc,
        "leaderboard": [
            {"family": f["name"], "aic": round(f["aic"], 1),
             "ks": round(f["ks"], 4), "params": f["params"]}
            for f in fits
        ],
        "best": fits[0]["name"] if fits else None,
    }


def summarize_all_mlb_stats(min_games_per_player: int = 5) -> list[dict]:
    """Run the leaderboard on every MLB stat the props pipeline tracks."""
    stats = [
        # Pitcher
        "k_p", "bb_p", "outs", "er", "h_allowed",
        # Batter
        "hr", "h", "tb", "rbi", "r", "sb", "bb_b", "k_b",
    ]
    return [summarize_stat("mlb", s, min_games_per_player) for s in stats]


def summarize_all_nba_stats(min_games_per_player: int = 5) -> list[dict]:
    """Run the leaderboard on every NBA stat the props pipeline tracks."""
    stats = ["pts", "reb", "ast", "tpm", "ftm", "to", "stl", "blk"]
    return [summarize_stat("nba", s, min_games_per_player) for s in stats]


def summarize_all_nhl_stats(min_games_per_player: int = 5) -> list[dict]:
    """Run the leaderboard on every NHL stat the props pipeline tracks.
    Skater + goalie stats live in the same table — goalies just don't
    have skater fields populated and vice versa."""
    stats = [
        "g", "a", "sog", "hits", "blocks",          # Skater
        "saves", "shots_against", "ga",             # Goalie
    ]
    return [summarize_stat("nhl", s, min_games_per_player) for s in stats]


# ── Locked decisions ──────────────────────────────────────────
# Source of truth for the MC sampler in 2g-ii / 2h-ii / 2i-ii. Each
# entry: which parametric family to use, and (for NegBin) the pooled
# dispersion ``k`` to combine with each player's rolling mean μ.
#
# Per-player μ comes from rolling history at MC time. The dispersion
# ``k`` is shared across players within a stat — the data shows a
# stable shape parameter even when means vary 5-10x, so a pooled fit
# is the right granularity. If a stat shows mean-dependent shape we
# can revisit by tier.
#
# Family meanings:
#   poisson   → sample ~ Poisson(μ)
#   negbin    → sample ~ NegBin(r=k, p=k/(k+μ)); var = μ + μ²/k
#   geometric → sample ~ Geom(p=1/(1+μ)) on {0, 1, 2, ...}
#
# Decisions locked from pooled MLB fit on 2026-04-26 (n≥3319 per
# pitcher stat, n=8411 per batter stat). Re-run summarize_all_mlb_stats
# quarterly and update if the AIC leader changes.

# Refit 2026-04-27 against full multi-season backfill (~243k MLB / 162k
# NHL / 90k NBA log rows). Five family changes promoted by AIC on the
# bigger pool: mlb.hr poisson→negbin, mlb.r poisson→zip, mlb.sb
# geometric→negbin, mlb.bb_b poisson→zip, nhl.a zip→negbin. All
# negbin dispersion_k values refit pooled. Re-run summarize_all_*_stats
# quarterly and update if the AIC leader changes.
_NBA_STAT_DISTRIBUTIONS: dict[str, dict] = {
    "pts":  {"family": "negbin", "dispersion_k": 1.25},
    "reb":  {"family": "negbin", "dispersion_k": 2.11},
    "ast":  {"family": "negbin", "dispersion_k": 1.37},
    "tpm":  {"family": "negbin", "dispersion_k": 1.29},
    "ftm":  {"family": "negbin", "dispersion_k": 0.58,
             "note": "heavy overdispersion -- foul-line games"},
    "to":   {"family": "negbin", "dispersion_k": 2.19},
    "stl":  {"family": "negbin", "dispersion_k": 2.72},
    "blk":  {"family": "geometric",
             "note": "Geom edges NegBin r~0.97 by ~1 AIC at 86k samples"},
}

_NHL_STAT_DISTRIBUTIONS: dict[str, dict] = {
    # Skaters: g, a, sog, hits, blocks
    "g":      {"family": "negbin", "dispersion_k": 2.12,
               "note": "k shifted +26% on full pool vs original 191-game fit"},
    "a":      {"family": "negbin", "dispersion_k": 2.89,
               "note": "promoted from zip; NegBin edges ZIP by ~30 AIC at 152k samples"},
    "sog":    {"family": "negbin", "dispersion_k": 3.86},
    "hits":   {"family": "negbin", "dispersion_k": 1.63},
    "blocks": {"family": "negbin", "dispersion_k": 1.96},
    # Goalies: saves, shots_against, ga
    "saves":         {"family": "negbin", "dispersion_k": 12.15},
    "shots_against": {"family": "negbin", "dispersion_k": 15.08},
    "ga":            {"family": "poisson",
                      "note": "under-dispersed -- Poisson tighter than NegBin/Geom"},
}


_MLB_STAT_DISTRIBUTIONS: dict[str, dict] = {
    # Pitcher
    "k_p":       {"family": "negbin",   "dispersion_k": 1.62},
    "bb_p":      {"family": "negbin",   "dispersion_k": 1.63},
    "outs":      {"family": "negbin",   "dispersion_k": 1.83,
                  "note": "bimodal SP/RP -- fit per-role in 2g-ii (KS=0.22 pooled)"},
    "er":        {"family": "negbin",   "dispersion_k": 0.51,
                  "note": "heavy overdispersion -- Poisson would underprice tail"},
    "h_allowed": {"family": "negbin",   "dispersion_k": 1.06},
    # Batter
    "hr":        {"family": "negbin",   "dispersion_k": 5.60,
                  "note": "promoted from poisson; NegBin/Poisson AICs tied at "
                          "121k but the negbin shape captures the long tail"},
    "h":         {"family": "poisson",
                  "note": "under-dispersed; Poisson tightest"},
    "tb":        {"family": "negbin",   "dispersion_k": 0.91},
    "rbi":       {"family": "negbin",   "dispersion_k": 0.63},
    "r":         {"family": "zip",
                  "note": "promoted from poisson; ZIP edges Poisson by ~12 AIC "
                          "(pi~0.027) -- the few players who never score in "
                          "back-to-back games drove the inflation"},
    "sb":        {"family": "negbin",   "dispersion_k": 0.55,
                  "note": "promoted from geometric -- NegBin edges Geometric by "
                          "~100 AIC at 171k samples"},
    "bb_b":      {"family": "zip",
                  "note": "promoted from poisson; pi~0.075 captures excess zeros "
                          "from non-walking-style hitters"},
    "k_b":       {"family": "poisson"},
}


def get_distribution(sport: str, stat_key: str) -> dict | None:
    """Locked per-stat distribution choice for the MC sampler. Returns
    ``{family, dispersion_k?, note?}`` or None when stat isn't known."""
    if sport == "mlb":
        return _MLB_STAT_DISTRIBUTIONS.get(stat_key)
    if sport == "nba":
        return _NBA_STAT_DISTRIBUTIONS.get(stat_key)
    if sport == "wnba":
        # Reuse NBA's locked dispersion shapes as priors — WNBA box-stat
        # families look identical (counting stats, NegBin-leaning) but
        # the dispersion_k may shift modestly. TODO: refit pooled k from
        # WNBA player_game_logs once the backfill seeds ~50k stat-rows.
        return _NBA_STAT_DISTRIBUTIONS.get(stat_key)
    if sport == "nhl":
        return _NHL_STAT_DISTRIBUTIONS.get(stat_key)
    return None


# Per-stat probability shrinkage factor — pulls model_prob toward 50%
# to correct overconfidence found by replay calibration. Only stats
# with statistically meaningful overconfidence get a non-1.0 entry;
# defaults to 1.0 for everything else.
#
# Locked from replay calibration on 2026-04-26 across 30 days of
# game logs per sport. Re-run engine.player_props_calibration
# periodically and update if avg_delta drifts.
#
# Direction: model_prob = 0.5 + (model_prob - 0.5) * shrink. shrink<1
# pulls toward 50% (less aggressive); shrink=1 means no adjustment.
# Underconfident stats (MLB outs, NHL g) intentionally NOT inflated
# — expanding probability is dangerous (could push picks above the
# edge floor that should have been filtered).
# Updated 2026-04-27 from a 365-day walk-forward calibration over the
# full backfilled pool (~243k MLB / 162k NHL / 90k NBA log rows). The
# 30-day window we'd previously calibrated against was noisy enough to
# over-shrink several stats — bigger sample showed they're well-
# calibrated long-term:
#   mlb.tb           0.92 -> 1.00  (365d gap negligible)
#   mlb.h_allowed    0.94 -> 1.00  (365d gap negligible)
#   nba.ftm          0.90 -> 0.93  (365d gap -3.6pp, was -4.4pp)
#   nhl.saves        0.77 -> 1.00  (365d gap -0.8pp, 30d was misleading)
#   nhl.shots_against 0.89 -> 1.00 (365d gap -1.3pp)
# nba.pts stays at 0.89 — 365d still shows -3.1pp overconfidence.
_PROB_SHRINK: dict[tuple[str, str], float] = {
    ("nba", "pts"):             0.89,
    ("nba", "ftm"):             0.93,
}


# Learned shrinkage table — populated from the calibration replay tool
# (engine.player_props_calibration.refresh_shrinkage). Lives on disk at
# ``data/prop_shrinkage.json`` so retrains and the props_backtest CLI
# can persist updates that survive process restarts.
#
# Layered with _PROB_SHRINK so the hardcoded entries remain the floor
# of last resort: learned values win when present, hardcoded values
# fill any stat the learner hasn't yet covered (typically because
# sample size hasn't crossed min_samples_per_stat for that sport).
import os as _os
_LEARNED_SHRINK: dict[tuple[str, str], float] = {}
_LEARNED_LOADED = False
_SHRINKAGE_PATH = _os.path.join(
    _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
    "data", "prop_shrinkage.json",
)


def _load_learned_shrinkage() -> None:
    """Hydrate ``_LEARNED_SHRINK`` from the JSON file. Idempotent — only
    reads on the first call (subsequent calls no-op until ``reload=True``
    is forced via ``reload_learned_shrinkage()``).

    File format::

        {
          "mlb": {"k_p": {"shrink": 0.92, "n": 1234, "avg_delta": -0.04, ...},
                  ...},
          "nba": {...},
          "nhl": {...}
        }
    """
    global _LEARNED_LOADED
    if _LEARNED_LOADED:
        return
    _LEARNED_LOADED = True  # Mark even on miss so we don't retry every call.
    try:
        if not _os.path.exists(_SHRINKAGE_PATH):
            return
        with open(_SHRINKAGE_PATH, "r", encoding="utf-8") as f:
            blob = json.load(f) or {}
    except Exception as e:
        logger.warning("learned shrinkage load failed (%s): %s",
                       _SHRINKAGE_PATH, e)
        return
    for sport, stats in blob.items():
        if not isinstance(stats, dict):
            continue
        for stat_key, info in stats.items():
            try:
                shrink = float(info.get("shrink", 1.0)) if isinstance(info, dict) \
                          else float(info)
            except (TypeError, ValueError):
                continue
            # Clamp to (0, 1]: shrink > 1 would inflate confidence and
            # push picks above the edge floor that the picker filtered.
            if shrink <= 0.0:
                continue
            shrink = min(1.0, shrink)
            _LEARNED_SHRINK[(sport, stat_key)] = shrink


def reload_learned_shrinkage() -> None:
    """Clear and re-read the learned-shrinkage cache. Call after any
    refresh that rewrites prop_shrinkage.json so the live picker picks
    the new values up without a process restart."""
    global _LEARNED_LOADED
    _LEARNED_SHRINK.clear()
    _LEARNED_LOADED = False
    _load_learned_shrinkage()


def save_learned_shrinkage(sport: str, recs: dict) -> str:
    """Merge ``recs`` (the dict returned by
    ``player_props_calibration.shrinkage_recommendation``) into the
    persisted ``data/prop_shrinkage.json`` for ``sport``. Returns the
    path that was written.

    Each entry is stored with its diagnostic fields (n, avg_delta,
    note) so a later inspection can tell why a stat was shrunk
    without re-running the replay.
    """
    _os.makedirs(_os.path.dirname(_SHRINKAGE_PATH), exist_ok=True)
    blob: dict[str, dict] = {}
    if _os.path.exists(_SHRINKAGE_PATH):
        try:
            with open(_SHRINKAGE_PATH, "r", encoding="utf-8") as f:
                blob = json.load(f) or {}
        except Exception as e:
            logger.warning("read existing shrinkage file failed: %s "
                           "— overwriting", e)
            blob = {}
    sport_block = blob.setdefault(sport, {})
    from datetime import datetime as _dt
    stamp = _dt.utcnow().isoformat(timespec="seconds") + "Z"
    for stat_key, info in (recs or {}).items():
        if not isinstance(info, dict):
            continue
        sport_block[stat_key] = {
            "shrink": round(float(info.get("shrink", 1.0)), 3),
            "avg_delta": info.get("avg_delta"),
            "note": info.get("note"),
            "updated_at": stamp,
        }
    with open(_SHRINKAGE_PATH, "w", encoding="utf-8") as f:
        json.dump(blob, f, indent=2, sort_keys=True)
    reload_learned_shrinkage()
    return _SHRINKAGE_PATH


def get_prob_shrink(sport: str, stat_key: str) -> float:
    """Returns the per-stat shrinkage multiplier in (0, 1]. Defaults
    to 1.0 (no shrinkage) when the stat isn't in the calibrated list.

    Lookup order:
      1. learned table (data/prop_shrinkage.json — refreshed by the
         calibration replay tool)
      2. hardcoded ``_PROB_SHRINK`` (fallback for stats the learner
         hasn't covered yet)
      3. 1.0 (passthrough)
    """
    _load_learned_shrinkage()
    learned = _LEARNED_SHRINK.get((sport, stat_key))
    if learned is not None:
        return learned
    return _PROB_SHRINK.get((sport, stat_key), 1.0)


__all__ = [
    "summarize_stat",
    "summarize_all_mlb_stats", "summarize_all_nba_stats", "summarize_all_nhl_stats",
    "fit_all", "pull_observations",
    "get_distribution", "get_prob_shrink",
    "save_learned_shrinkage", "reload_learned_shrinkage",
    "_MLB_STAT_DISTRIBUTIONS", "_NBA_STAT_DISTRIBUTIONS", "_NHL_STAT_DISTRIBUTIONS",
    "_PROB_SHRINK",
]
