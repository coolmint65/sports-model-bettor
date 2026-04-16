"""
Shared Monte Carlo utilities.

Sport-specific simulators (``engine.mc_mlb``, ``engine.mc_nhl``,
``engine.mc_nba``) import from here for the common math. Nothing
sport-specific lives in this module.
"""

from __future__ import annotations

import numpy as np


def log5(batter_rate: float, pitcher_rate: float, league_rate: float) -> float:
    """Bill James' log5 formula for two-player rate blending.

    P(outcome | batter faces pitcher) = (B*P/L) / (B*P/L + (1-B)*(1-P)/(1-L))

    The textbook result: two players with rates equal to league average
    produce a league-average joint rate; above-average batter vs
    below-average pitcher produces a higher-than-either joint rate.
    Falls back to the simple average when inputs push toward 0 or 1.
    """
    b = _clip(batter_rate)
    p = _clip(pitcher_rate)
    lg = _clip(league_rate)
    num = b * p / lg
    denom = num + (1.0 - b) * (1.0 - p) / (1.0 - lg)
    if denom <= 0:
        return (b + p) / 2
    return num / denom


def _clip(x: float, lo: float = 0.001, hi: float = 0.999) -> float:
    """Clamp a rate to an open interval so log5 doesn't divide by zero."""
    if x is None:
        return lo
    return max(lo, min(hi, float(x)))


def log5_vec(batter_rates: np.ndarray, pitcher_rate: float,
              league_rate: float) -> np.ndarray:
    """Vectorized log5 for a lineup vs. one pitcher. Returns a rate array."""
    b = np.clip(batter_rates, 0.001, 0.999)
    p = _clip(pitcher_rate)
    lg = _clip(league_rate)
    num = b * p / lg
    denom = num + (1.0 - b) * (1.0 - p) / (1.0 - lg)
    return np.where(denom > 0, num / denom, (b + p) / 2)


# ── Convenience: aggregate simulated outcomes into market probabilities ──

def aggregate_mc_outcomes(home_runs_per_sim: np.ndarray,
                           away_runs_per_sim: np.ndarray,
                           home_inning_runs: np.ndarray | None = None,
                           away_inning_runs: np.ndarray | None = None,
                           ) -> dict:
    """Produce the standard market probabilities from a matrix of simulated
    game outcomes.

    Args:
        home_runs_per_sim / away_runs_per_sim: 1-D arrays of length N (sims).
        home_inning_runs / away_inning_runs: optional 2-D arrays of shape
            (N, 9) for per-inning breakdowns. When absent, F5 / NRFI stats
            are skipped.

    Returns a dict with keys:
        n_sims, win_prob, expected_runs, totals (dict of line -> probs),
        run_line (dict of point -> probs), f5 (optional), nrfi (optional),
        correct_scores (list of top-N), margin_std.
    """
    n = len(home_runs_per_sim)
    home_wins = home_runs_per_sim > away_runs_per_sim
    away_wins = away_runs_per_sim > home_runs_per_sim
    ties = ~(home_wins | away_wins)

    home_win_pct = float(home_wins.sum()) / n
    away_win_pct = float(away_wins.sum()) / n
    tie_pct = float(ties.sum()) / n

    totals = home_runs_per_sim + away_runs_per_sim
    margins = home_runs_per_sim.astype(int) - away_runs_per_sim.astype(int)

    # Totals: probabilities across integer + half-point lines around the mean
    mean_total = float(totals.mean())
    anchor = round(mean_total * 2) / 2
    ou: dict = {}
    for offset in (-2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2):
        line = anchor + offset
        if line < 0.5:
            continue
        over = float((totals > line).sum()) / n
        under = float((totals < line).sum()) / n
        ou[f"{line:.1f}"] = {
            "over": round(over, 4),
            "under": round(under, 4),
            "push": round(1.0 - over - under, 4),
        }

    # Run line distribution
    rl: dict = {}
    for point in (-2.5, -1.5, -0.5, 0.5, 1.5, 2.5):
        home_cov = float((margins > -point).sum()) / n
        rl[f"home_{point}"] = round(home_cov, 4)

    # Correct scores top-10
    score_counts: dict[tuple[int, int], int] = {}
    for h, a in zip(home_runs_per_sim.astype(int), away_runs_per_sim.astype(int)):
        key = (int(h), int(a))
        score_counts[key] = score_counts.get(key, 0) + 1
    top_scores = sorted(score_counts.items(), key=lambda kv: -kv[1])[:10]
    correct_scores = [
        {"home": h, "away": a, "prob": round(c / n, 4)}
        for (h, a), c in top_scores
    ]

    result = {
        "n_sims": n,
        "win_prob": {
            "home": round(home_win_pct, 4),
            "away": round(away_win_pct, 4),
            "tie":  round(tie_pct, 4),
        },
        "expected_runs": {
            "home": round(float(home_runs_per_sim.mean()), 3),
            "away": round(float(away_runs_per_sim.mean()), 3),
            "total": round(mean_total, 3),
        },
        "margin_std": round(float(margins.std()), 3),
        "total_std": round(float(totals.std()), 3),
        "over_under": ou,
        "run_line": rl,
        "correct_scores": correct_scores,
    }

    # F5 / NRFI require per-inning breakdowns
    if home_inning_runs is not None and away_inning_runs is not None:
        f5_home = home_inning_runs[:, :5].sum(axis=1)
        f5_away = away_inning_runs[:, :5].sum(axis=1)
        f5_total = f5_home + f5_away
        f5_margin = f5_home.astype(int) - f5_away.astype(int)

        f5_mean_total = float(f5_total.mean())
        f5_anchor = round(f5_mean_total * 2) / 2
        f5_ou: dict = {}
        for offset in (-1, -0.5, 0, 0.5, 1):
            line = f5_anchor + offset
            if line < 0.5:
                continue
            f5_ou[f"{line:.1f}"] = {
                "over": round(float((f5_total > line).sum()) / n, 4),
                "under": round(float((f5_total < line).sum()) / n, 4),
            }

        result["f5"] = {
            "expected_runs": {
                "home": round(float(f5_home.mean()), 3),
                "away": round(float(f5_away.mean()), 3),
                "total": round(f5_mean_total, 3),
            },
            "win_prob": {
                "home": round(float((f5_margin > 0).sum()) / n, 4),
                "away": round(float((f5_margin < 0).sum()) / n, 4),
                "tie":  round(float((f5_margin == 0).sum()) / n, 4),
            },
            "over_under": f5_ou,
            "run_line": {
                "home_-0.5": round(float((f5_margin > 0).sum()) / n, 4),
                "home_+0.5": round(float((f5_margin >= 0).sum()) / n, 4),
            },
        }

        # NRFI: P(both teams score 0 in inning 1)
        nrfi_hits = (home_inning_runs[:, 0] == 0) & (away_inning_runs[:, 0] == 0)
        result["nrfi"] = {
            "nrfi": round(float(nrfi_hits.sum()) / n, 4),
            "yrfi": round(1.0 - float(nrfi_hits.sum()) / n, 4),
        }

        # F3 for completeness
        f3_home = home_inning_runs[:, :3].sum(axis=1)
        f3_away = away_inning_runs[:, :3].sum(axis=1)
        f3_total = f3_home + f3_away
        result["f3"] = {
            "expected_total": round(float(f3_total.mean()), 3),
            "over_2_5": round(float((f3_total > 2.5).sum()) / n, 4),
            "under_2_5": round(float((f3_total < 2.5).sum()) / n, 4),
        }

    return result
