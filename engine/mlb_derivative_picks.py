"""
MLB derivative-market pick generators.

Pure probability extraction from the existing ``pred`` dict — no new
factors stacked on the model. Each market reuses what the prediction
already produced (per-team xR, per-inning xR, F5 split, total/margin
distributions) and prices the HR-quoted line off that.

Wired in from ``engine.picks.generate_picks`` after the F5 block and
before empirical recalibration; the orchestrator ``append_derivative_picks``
respects ``ENABLE_MLB_DERIVATIVES`` plus per-market gates so a single
misbehaving market can be turned off without losing the others.

Storage shape on the ``odds`` dict (set by ``scrapers.hardrock_odds``):
  team_total_home / team_total_away      = {line, over_odds, under_odds}
  f5_team_total_home / f5_team_total_away = {line, over_odds, under_odds}
  inning_totals                          = {"1": {line, over_odds, under_odds}, ...}
  inning_bts                             = {"1": {yes_odds, no_odds}, ...}
  inning_winner                          = {home_ml, away_ml, tie_ml?}
  f5_winner                              = {home_ml, away_ml, tie_ml?}
  total_oe                               = {odd_odds, even_odds}
  extra_innings                          = {yes_odds, no_odds}
"""

from __future__ import annotations

import math
from typing import Iterable

from .config import MLB_JUICE_WALL as JUICE_WALL, get_flag


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


def _poisson_pmf(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def _poisson_p_over(lam: float, line: float, max_k: int = 25) -> float:
    """P(X > line) where X ~ Poisson(lam). Lines are typically .5, so
    'over 3.5' = P(X >= 4)."""
    if lam <= 0:
        return 0.0
    threshold = math.floor(line) + 1
    return sum(_poisson_pmf(lam, k) for k in range(threshold, max_k + 1))


def _mc_p_over(mc_lines: dict, line: float, fallback_mean: float) -> float:
    """Pull MC's empirical P(over `line`) when the line matches a
    bucket MC computed. Falls back to Poisson tail off the MC mean
    when the line isn't in MC's window. MC's empirical dist is
    strictly more informative than Poisson — at-bat-level sims
    capture clustering/bimodality the factor's Poisson assumes away.
    """
    if mc_lines:
        line_str = f"{line:.1f}"
        bucket = mc_lines.get(line_str)
        if bucket and bucket.get("over") is not None:
            return float(bucket["over"])
    return _poisson_p_over(float(fallback_mean), line)


def _poisson_p_zero(lam: float) -> float:
    if lam <= 0:
        return 1.0
    return math.exp(-lam)


def _ou_edge(prob: float, ml) -> float | None:
    if not _valid_odds(ml):
        return None
    if ml < JUICE_WALL:
        return None
    return (prob - _implied(int(ml))) * 100


def _append_team_total(picks: list, pred: dict, odds: dict,
                        h_abbr: str, a_abbr: str) -> None:
    if not get_flag("ENABLE_MLB_TEAM_TOTAL", True):
        return
    es = pred.get("expected_score") or {}
    mc = pred.get("mc") or {}
    mc_tt = (mc.get("team_totals") or {}) if "error" not in mc else {}
    for side, abbr, key in (
        ("home", h_abbr, "team_total_home"),
        ("away", a_abbr, "team_total_away"),
    ):
        market = odds.get(key)
        lam = es.get(side)
        if not isinstance(market, dict) or lam is None:
            continue
        line = market.get("line")
        if line is None:
            continue
        try:
            line = float(line)
        except (TypeError, ValueError):
            continue
        # Prefer MC's empirical distribution when available — at-bat-level
        # sims (100k) capture run-clustering the Poisson tail flattens.
        mc_lines = ((mc_tt.get(side) or {}).get("lines") or {})
        mc_mean = (mc_tt.get(side) or {}).get("expected") or float(lam)
        p_over = _mc_p_over(mc_lines, line, mc_mean)
        p_under = 1.0 - p_over
        for direction, prob, ml in (
            ("Over",  p_over,  market.get("over_odds")),
            ("Under", p_under, market.get("under_odds")),
        ):
            edge = _ou_edge(prob, ml)
            if edge is None or edge <= 0:
                continue
            picks.append({
                "type": "Team Total",
                "pick": f"{abbr} {direction} {line}",
                "prob": round(prob, 4),
                "edge": round(edge, 1),
                "odds": int(ml),
            })


def _append_f5_team_total(picks: list, pred: dict, odds: dict,
                            h_abbr: str, a_abbr: str) -> None:
    if not get_flag("ENABLE_MLB_F5_TEAM_TOTAL", True):
        return
    f5 = pred.get("f5") or {}
    mc = pred.get("mc") or {}
    mc_f5_tt = (mc.get("f5_team_totals") or {}) if "error" not in mc else {}
    for side, abbr, key in (
        ("home", h_abbr, "f5_team_total_home"),
        ("away", a_abbr, "f5_team_total_away"),
    ):
        market = odds.get(key)
        lam = f5.get(side)
        if not isinstance(market, dict) or lam is None:
            continue
        line = market.get("line")
        if line is None:
            continue
        try:
            line = float(line)
        except (TypeError, ValueError):
            continue
        mc_lines = ((mc_f5_tt.get(side) or {}).get("lines") or {})
        mc_mean = (mc_f5_tt.get(side) or {}).get("expected") or float(lam)
        p_over = _mc_p_over(mc_lines, line, mc_mean)
        p_under = 1.0 - p_over
        for direction, prob, ml in (
            ("Over",  p_over,  market.get("over_odds")),
            ("Under", p_under, market.get("under_odds")),
        ):
            edge = _ou_edge(prob, ml)
            if edge is None or edge <= 0:
                continue
            picks.append({
                "type": "F5 Team Total",
                "pick": f"{abbr} F5 {direction} {line}",
                "prob": round(prob, 4),
                "edge": round(edge, 1),
                "odds": int(ml),
            })


def _append_inning_totals(picks: list, pred: dict, odds: dict,
                            h_abbr: str, a_abbr: str) -> None:
    if not get_flag("ENABLE_MLB_INNING_TOTAL", True):
        return
    market = odds.get("inning_totals")
    innings = pred.get("innings") or []
    if not isinstance(market, dict):
        return
    by_inning = {str(i["inning"]): i for i in innings if isinstance(i, dict)}
    mc = pred.get("mc") or {}
    mc_per_inning = (mc.get("per_inning") or {}) if "error" not in mc else {}
    for period_key, mkt in market.items():
        inn = by_inning.get(str(period_key))
        if not inn or not isinstance(mkt, dict):
            continue
        line = mkt.get("line")
        if line is None:
            continue
        try:
            line = float(line)
        except (TypeError, ValueError):
            continue
        # MC per-inning has direct over_0_5/1_5/2_5 probs. Use the
        # closest line bucket; otherwise fall back to factor Poisson.
        mc_inn = mc_per_inning.get(str(period_key))
        p_over = None
        if mc_inn:
            line_key = f"over_{str(line).replace('.', '_')}"
            if line_key in mc_inn:
                p_over = float(mc_inn[line_key])
        if p_over is None:
            lam = float(inn.get("total", 0.0))
            p_over = _poisson_p_over(lam, line)
        p_under = 1.0 - p_over
        for direction, prob, ml in (
            ("Over",  p_over,  mkt.get("over_odds")),
            ("Under", p_under, mkt.get("under_odds")),
        ):
            edge = _ou_edge(prob, ml)
            if edge is None or edge <= 0:
                continue
            picks.append({
                "type": "Inning Total",
                "pick": f"Inn {period_key} {direction} {line}",
                "prob": round(prob, 4),
                "edge": round(edge, 1),
                "odds": int(ml),
            })


def _append_inning_bts(picks: list, pred: dict, odds: dict,
                        h_abbr: str, a_abbr: str) -> None:
    if not get_flag("ENABLE_MLB_INNING_BTS", True):
        return
    market = odds.get("inning_bts")
    innings = pred.get("innings") or []
    if not isinstance(market, dict):
        return
    by_inning = {str(i["inning"]): i for i in innings if isinstance(i, dict)}
    mc = pred.get("mc") or {}
    mc_per_inning = (mc.get("per_inning") or {}) if "error" not in mc else {}
    for period_key, mkt in market.items():
        inn = by_inning.get(str(period_key))
        if not isinstance(mkt, dict):
            continue
        # Prefer MC's empirical bts_yes — captures inning-clustering
        # the Poisson independence assumption misses (e.g. rallies
        # tend to score multiple runs by both teams).
        mc_inn = mc_per_inning.get(str(period_key))
        if mc_inn and mc_inn.get("bts_yes") is not None:
            p_yes = float(mc_inn["bts_yes"])
        elif inn:
            lam_h = float(inn.get("home", 0.0))
            lam_a = float(inn.get("away", 0.0))
            p_yes = (1.0 - _poisson_p_zero(lam_h)) * (1.0 - _poisson_p_zero(lam_a))
        else:
            continue
        p_no = 1.0 - p_yes
        for direction, prob, ml in (
            ("Yes", p_yes, mkt.get("yes_odds")),
            ("No",  p_no,  mkt.get("no_odds")),
        ):
            edge = _ou_edge(prob, ml)
            if edge is None or edge <= 0:
                continue
            picks.append({
                "type": "Inning BTS",
                "pick": f"Inn {period_key} BTS {direction}",
                "prob": round(prob, 4),
                "edge": round(edge, 1),
                "odds": int(ml),
            })


def _first_inning_three_way(lam_h: float, lam_a: float,
                             max_runs: int = 8) -> tuple[float, float, float]:
    """Return (p_home_wins_inn, p_away_wins_inn, p_tie) for a single
    inning with the given per-team Poisson means."""
    p_home = 0.0
    p_away = 0.0
    p_tie = 0.0
    for h in range(max_runs + 1):
        ph = _poisson_pmf(lam_h, h)
        for a in range(max_runs + 1):
            pa = _poisson_pmf(lam_a, a)
            joint = ph * pa
            if h > a:
                p_home += joint
            elif a > h:
                p_away += joint
            else:
                p_tie += joint
    return p_home, p_away, p_tie


def _append_inning_winner(picks: list, pred: dict, odds: dict,
                            h_abbr: str, a_abbr: str) -> None:
    if not get_flag("ENABLE_MLB_INNING_WINNER", True):
        return
    market = odds.get("inning_winner")
    fi = pred.get("first_inning") or {}
    if not isinstance(market, dict):
        return
    # Prefer MC's per-inning winner distribution (inning 1) when avail.
    mc = pred.get("mc") or {}
    mc_inn1 = ((mc.get("per_inning") or {}).get("1")) if "error" not in mc else None
    if mc_inn1 and isinstance(mc_inn1.get("winner"), dict):
        p_home = float(mc_inn1["winner"].get("home", 0))
        p_away = float(mc_inn1["winner"].get("away", 0))
        p_tie = float(mc_inn1["winner"].get("tie", 0))
    else:
        lam_h = fi.get("home_xr_1st")
        lam_a = fi.get("away_xr_1st")
        if lam_h is None or lam_a is None:
            return
        p_home, p_away, p_tie = _first_inning_three_way(float(lam_h), float(lam_a))
    for label, prob, ml in (
        (h_abbr, p_home, market.get("home_ml")),
        (a_abbr, p_away, market.get("away_ml")),
        ("Tie",  p_tie,  market.get("tie_ml")),
    ):
        edge = _ou_edge(prob, ml)
        if edge is None or edge <= 0:
            continue
        picks.append({
            "type": "1st Inn Winner",
            "pick": f"1st Inn {label}",
            "prob": round(prob, 4),
            "edge": round(edge, 1),
            "odds": int(ml),
        })


def _append_f5_winner(picks: list, pred: dict, odds: dict,
                        h_abbr: str, a_abbr: str) -> None:
    """3-way F5 winner. The 2-way F5 ML lives in the existing F5 block;
    this only fires when HR offers a 3-way book with a tie price."""
    if not get_flag("ENABLE_MLB_F5_WINNER_3WAY", True):
        return
    market = odds.get("f5_winner")
    f5 = pred.get("f5") or {}
    if not isinstance(market, dict):
        return
    if market.get("tie_ml") is None:
        # 2-way is already handled by the F5 ML pick generator; skip.
        return
    wp = f5.get("win_prob") or {}
    p_home = wp.get("home")
    p_away = wp.get("away")
    if p_home is None or p_away is None:
        return
    p_home = float(p_home)
    p_away = float(p_away)
    # _compute_f5's win_prob only splits home/away (no tie); the residual
    # is the tie probability. Floor at 0 in case rounding makes it slightly
    # negative.
    p_tie = max(0.0, 1.0 - p_home - p_away)
    for label, prob, ml in (
        (h_abbr, p_home, market.get("home_ml")),
        (a_abbr, p_away, market.get("away_ml")),
        ("Tie",  p_tie,  market.get("tie_ml")),
    ):
        edge = _ou_edge(prob, ml)
        if edge is None or edge <= 0:
            continue
        picks.append({
            "type": "F5 Winner",
            "pick": f"F5 {label}",
            "prob": round(prob, 4),
            "edge": round(edge, 1),
            "odds": int(ml),
        })


def _append_total_oe(picks: list, pred: dict, odds: dict,
                      h_abbr: str, a_abbr: str) -> None:
    if not get_flag("ENABLE_MLB_TOTAL_OE", True):
        return
    market = odds.get("total_oe")
    if not isinstance(market, dict):
        return
    # Prefer MC's empirical odd/even split — counts each sim's actual
    # total parity, no Poisson approximation.
    mc = pred.get("mc") or {}
    mc_oe = (mc.get("total_oe") or {}) if "error" not in mc else {}
    if mc_oe.get("odd") is not None and mc_oe.get("even") is not None:
        p_odd = float(mc_oe["odd"])
        p_even = float(mc_oe["even"])
    else:
        rl = pred.get("run_line") or {}
        total_probs = rl.get("total_probs")
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
        edge = _ou_edge(prob, ml)
        if edge is None or edge <= 0:
            continue
        picks.append({
            "type": "Total O/E",
            "pick": f"Total {label}",
            "prob": round(prob, 4),
            "edge": round(edge, 1),
            "odds": int(ml),
        })


def _append_extra_innings(picks: list, pred: dict, odds: dict,
                            h_abbr: str, a_abbr: str) -> None:
    if not get_flag("ENABLE_MLB_EXTRA_INNINGS", True):
        return
    market = odds.get("extra_innings")
    if not isinstance(market, dict):
        return
    # Prefer MC's empirical extra-innings rate — direct count of sims
    # where regulation ended tied. Better than the factor's
    # P(margin == 0) because MC simulates inning-by-inning.
    mc = pred.get("mc") or {}
    mc_xi = (mc.get("extra_innings") or {}) if "error" not in mc else {}
    if mc_xi.get("yes") is not None:
        p_yes = float(mc_xi["yes"])
    else:
        rl = pred.get("run_line") or {}
        margin_probs = rl.get("margin_probs")
        if not isinstance(margin_probs, dict):
            return
        p_yes = float(margin_probs.get(0, 0.0))
    if p_yes <= 0:
        return
    p_no = max(0.0, 1.0 - p_yes)
    for label, prob, ml in (
        ("Yes", p_yes, market.get("yes_odds")),
        ("No",  p_no,  market.get("no_odds")),
    ):
        edge = _ou_edge(prob, ml)
        if edge is None or edge <= 0:
            continue
        picks.append({
            "type": "Extra Innings",
            "pick": f"Extra Innings {label}",
            "prob": round(prob, 4),
            "edge": round(edge, 1),
            "odds": int(ml),
        })


def append_derivative_picks(picks: list, pred: dict, odds: dict,
                             h_abbr: str, a_abbr: str) -> None:
    """Append all enabled MLB derivative picks in-place. Master gate
    is ``ENABLE_MLB_DERIVATIVES``; each market then has its own flag."""
    if not get_flag("ENABLE_MLB_DERIVATIVES", True):
        return
    if not isinstance(odds, dict) or not pred:
        return
    _append_team_total(picks, pred, odds, h_abbr, a_abbr)
    _append_f5_team_total(picks, pred, odds, h_abbr, a_abbr)
    _append_inning_totals(picks, pred, odds, h_abbr, a_abbr)
    _append_inning_bts(picks, pred, odds, h_abbr, a_abbr)
    _append_inning_winner(picks, pred, odds, h_abbr, a_abbr)
    _append_f5_winner(picks, pred, odds, h_abbr, a_abbr)
    _append_total_oe(picks, pred, odds, h_abbr, a_abbr)
    _append_extra_innings(picks, pred, odds, h_abbr, a_abbr)
