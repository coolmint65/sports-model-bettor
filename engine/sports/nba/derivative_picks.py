"""
NBA Q1 derivative-market pick generators.

Pure probability extraction from the existing ``predict_q1`` output —
no new factors stacked on the model. Each market reuses what the Q1
predictor already produced (per-team Q1 expected points + the discrete
margin / total distributions) and prices the HR-quoted line off that.

Wired in from ``engine.nba_picks.generate_q1_picks`` after the Q1 ML
block; respects ``ENABLE_NBA_DERIVATIVES`` plus per-market gates.

Storage shape on the ``odds`` dict (set by ``scrapers.hardrock_odds``):
  q1_team_total_home / q1_team_total_away = {line, over_odds, under_odds}
  q1_total_oe                             = {odd_odds, even_odds}

Per-team Q1 sigma derivation: predict_q1 exposes Q1_STD_DEV (margin
std). Under independence between the two teams, sigma_team ≈
Q1_STD_DEV / sqrt(2) ≈ 6.1 — close enough for a v1; the empirical
calibration loop will correct any systematic bias as live samples
accumulate per bet_type.
"""

from __future__ import annotations

import math

from ...config import NBA_JUICE_WALL as JUICE_WALL, get_flag
from ...mc_constants import nba_q1_margin_std


def _team_q1_sigma() -> float:
    """Per-team Q1 sigma under the independence assumption between the two
    teams: sigma_team ≈ Q1_margin_sigma / sqrt(2). Resolved per-call so
    fresh calibrations propagate without a restart."""
    return nba_q1_margin_std() / math.sqrt(2.0)


def _implied(ml: int) -> float:
    if ml < 0:
        return abs(ml) / (abs(ml) + 100)
    return 100 / (ml + 100)


def _valid_odds(ml) -> bool:
    if ml is None:
        return False
    try:
        ml = int(ml)
    except (TypeError, ValueError):
        return False
    return abs(ml) >= 100


def _ou_edge(prob: float, ml) -> float | None:
    if not _valid_odds(ml):
        return None
    if ml < JUICE_WALL:
        return None
    return (prob - _implied(int(ml))) * 100


def _score(bet_type: str, pick_text: str, prob: float, ml) -> dict | None:
    """Local wrapper around picks_core.score_pick. Centralizes the
    NBA juice wall + pipeline gates for derivatives."""
    if not _valid_odds(ml):
        return None
    from ...picks_core import score_pick as _score_pick
    return _score_pick({
        "type": bet_type,
        "pick": pick_text,
        "raw_prob": prob,
        "odds": int(ml),
    }, sport="nba", juice_wall=JUICE_WALL)


def _norm_cdf(z: float) -> float:
    """Standard normal CDF via erf — same approach as nba_q1_predict."""
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _gaussian_p_over(mu: float, sigma: float, line: float) -> float:
    """P(X > line) for X ~ N(mu, sigma^2). HR lines are .5 so this is
    the standard tail probability."""
    if sigma <= 0:
        return 1.0 if mu > line else 0.0
    z = (line - mu) / sigma
    return 1.0 - _norm_cdf(z)


def _append_q1_team_totals(picks: list, pred: dict, odds: dict,
                              h_abbr: str, a_abbr: str) -> None:
    if not get_flag("ENABLE_NBA_Q1_TEAM_TOTAL", True):
        return
    mc = pred.get("mc") or {}
    mc_tt = (mc.get("team_totals") or {}) if "error" not in mc else {}
    for side, abbr, key, mu_key in (
        ("home", h_abbr, "q1_team_total_home", "home_q1_expected"),
        ("away", a_abbr, "q1_team_total_away", "away_q1_expected"),
    ):
        market = odds.get(key)
        mu = pred.get(mu_key)
        if not isinstance(market, dict) or mu is None:
            continue
        line = market.get("line")
        if line is None:
            continue
        try:
            line = float(line)
        except (TypeError, ValueError):
            continue
        # Prefer MC's empirical Q1 team distribution (100k possession-
        # level sims) over factor's Gaussian tail. MC captures the
        # fat-tailed nature of Q1 scoring (3-pointer streaks, foul
        # trouble) the Gaussian assumption misses.
        mc_lines = ((mc_tt.get(side) or {}).get("lines") or {})
        line_str = f"{line:.1f}"
        bucket = mc_lines.get(line_str) if mc_lines else None
        if bucket and bucket.get("over") is not None:
            p_over = float(bucket["over"])
        else:
            p_over = _gaussian_p_over(float(mu), _team_q1_sigma(), line)
        p_under = 1.0 - p_over
        for direction, prob, ml in (
            ("Over",  p_over,  market.get("over_odds")),
            ("Under", p_under, market.get("under_odds")),
        ):
            scored = _score("Q1 Team Total",
                             f"{abbr} Q1 {direction} {line}", prob, ml)
            if scored:
                picks.append(scored)


def _append_q1_total_oe(picks: list, pred: dict, odds: dict,
                          h_abbr: str, a_abbr: str) -> None:
    if not get_flag("ENABLE_NBA_Q1_TOTAL_OE", True):
        return
    market = odds.get("q1_total_oe")
    if not isinstance(market, dict):
        return
    # Prefer MC's empirical odd/even split — direct count of sim totals'
    # parity. Factor's discretized Gaussian split is an approximation.
    mc = pred.get("mc") or {}
    mc_oe = (mc.get("total_oe") or {}) if "error" not in mc else {}
    if mc_oe.get("odd") is not None and mc_oe.get("even") is not None:
        p_odd = float(mc_oe["odd"])
        p_even = float(mc_oe["even"])
    else:
        total_probs = pred.get("total_probs")
        if not isinstance(total_probs, dict):
            return
        p_odd = 0.0
        p_even = 0.0
        for k, v in total_probs.items():
            try:
                t = int(k)
            except (TypeError, ValueError):
                continue
            if t % 2 == 1:
                p_odd += float(v)
            else:
                p_even += float(v)
        norm = p_odd + p_even
        if norm <= 0:
            return
        p_odd /= norm
        p_even /= norm
    for label, prob, ml in (
        ("Odd",  p_odd,  market.get("odd_odds")),
        ("Even", p_even, market.get("even_odds")),
    ):
        scored = _score("Q1 Total O/E", f"Q1 Total {label}", prob, ml)
        if scored:
            picks.append(scored)


def append_derivative_picks(picks: list, pred: dict, odds: dict,
                             h_abbr: str, a_abbr: str) -> None:
    """Append all enabled NBA Q1 derivative picks in-place. Master gate
    is ``ENABLE_NBA_DERIVATIVES``; each market then has its own flag."""
    if not get_flag("ENABLE_NBA_DERIVATIVES", True):
        return
    if not isinstance(odds, dict) or not pred:
        return
    _append_q1_team_totals(picks, pred, odds, h_abbr, a_abbr)
    _append_q1_total_oe(picks, pred, odds, h_abbr, a_abbr)
