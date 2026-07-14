"""Monte Carlo sampler for the basketball framework.

Per-game simulation: draw N=10k correlated samples of (home_score,
away_score) using the factor model's expected scores as means + per-team
score variance fitted from box-score data. Returns sample arrays the
caller can convert into any market prob (ML / spread / total / alts)
without a per-market analytical formula.

The value over the analytical Normal CDF is correlation: real basketball
games have positively-correlated (home, away) scoring (a fast-paced game
inflates both totals; a defensive slugfest deflates both). The factor
model's separate sigma_margin / sigma_total don't capture this; MC does
it directly via a shared pace shock.

This is a structural model, not a per-team-trained one. Tuning happens
by adjusting the pace-shock variance per league based on residuals from
the GBM holdout (B4 work).
"""
from __future__ import annotations

import logging
import math
import random
from typing import Iterable

from ._config import get_league_config
from ._team_stats import team_rates

logger = logging.getLogger(__name__)


_DEFAULT_N_SIMS = 10_000

# Per-league shared pace-shock std — captures the "is this a track meet
# or a defensive slog" axis. Default to 5% of league avg total; refits
# in B4 when we have walk-forward residuals.
_PACE_SHOCK_PCT = 0.05


def simulate(league: str, home_id: int, away_id: int,
              home_expected: float, away_expected: float,
              n_sims: int = _DEFAULT_N_SIMS,
              seed: int | None = None) -> dict:
    """Run N MC sims for one game. Returns sample arrays + summary stats.

    Args:
        league:         registry key
        home_id, away_id: team ids (used to pull per-team variance)
        home_expected:  factor model's home expected points (passed in
                         so MC inherits all its adjustments — pace,
                         ratings, home boost, injuries, recent form)
        away_expected:  same for away
        n_sims:         number of simulations (default 10k)
        seed:           reproducibility

    Returns:
        ``{
          home_samples, away_samples, margin_samples, total_samples,
          mean_margin, mean_total,
          p_home_win, ...
        }``
        Caller derives market-specific probs by counting samples meeting
        the cutoff (e.g. ``sum(s > line for s in total_samples) / n``).
    """
    cfg = get_league_config(league)
    league_avg_total = cfg.get("league_avg_total") or 200.0

    # Per-team scoring std — fit from box-scores so a high-variance team
    # (think early-season WNBA expansion club) gets wider sims.
    home_sigma = _team_score_std(league, home_id) or _league_team_sigma(cfg)
    away_sigma = _team_score_std(league, away_id) or _league_team_sigma(cfg)

    # Shared pace shock — adds correlation to the joint distribution.
    pace_sigma = league_avg_total * _PACE_SHOCK_PCT * 0.5

    rng = random.Random(seed)
    home_samples: list[float] = []
    away_samples: list[float] = []
    for _ in range(n_sims):
        shock = rng.gauss(0, pace_sigma)
        h = rng.gauss(home_expected + shock, home_sigma)
        a = rng.gauss(away_expected + shock, away_sigma)
        home_samples.append(h)
        away_samples.append(a)

    margin_samples = [h - a for h, a in zip(home_samples, away_samples)]
    total_samples = [h + a for h, a in zip(home_samples, away_samples)]

    p_home_win = sum(1 for m in margin_samples if m > 0) / n_sims
    return {
        "n_sims": n_sims,
        "home_samples": home_samples,
        "away_samples": away_samples,
        "margin_samples": margin_samples,
        "total_samples": total_samples,
        "mean_margin": sum(margin_samples) / n_sims,
        "mean_total": sum(total_samples) / n_sims,
        "p_home_win": p_home_win,
    }


def market_probs(sims: dict, *, spread: float | None = None,
                  total: float | None = None) -> dict:
    """Convert sample arrays into market probabilities."""
    n = sims["n_sims"]
    out = {"p_home_win": sims["p_home_win"]}
    if spread is not None:
        ms = sims["margin_samples"]
        # P(home covers): margin > -spread (home spread is negative when fav)
        out["p_home_cover"] = sum(1 for m in ms if m > -spread) / n
    if total is not None:
        ts = sims["total_samples"]
        out["p_over"] = sum(1 for t in ts if t > total) / n
    return out


# ── Per-team variance ────────────────────────────────────────

_SIGMA_CACHE: dict[tuple[str, int], float] = {}


def _team_score_std(league: str, team_id: int) -> float | None:
    """Stdev of per-game points scored. Cached per (league, team)."""
    key = (league, int(team_id))
    if key in _SIGMA_CACHE:
        return _SIGMA_CACHE[key]
    from ._db import get_conn
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT points FROM game_team_stats "
        "WHERE team_id = ? AND points IS NOT NULL",
        (team_id,),
    ).fetchall()
    if len(rows) < 5:
        return None
    pts = [r["points"] for r in rows]
    mean = sum(pts) / len(pts)
    var = sum((p - mean) ** 2 for p in pts) / len(pts)
    sigma = var ** 0.5
    _SIGMA_CACHE[key] = sigma
    return sigma


def _league_team_sigma(cfg: dict) -> float:
    """Fallback per-team sigma when a team has no box-score history.
    Derived from the league total_std so the joint MC matches the
    factor model's analytical width on average."""
    total_std = cfg.get("total_std") or 21.0
    # Total std ~= sqrt(2) * team_std assuming uncorrelated; we have
    # correlation via pace shock so use a slightly tighter per-team std.
    return total_std / math.sqrt(2)


def reset_cache() -> None:
    _SIGMA_CACHE.clear()
