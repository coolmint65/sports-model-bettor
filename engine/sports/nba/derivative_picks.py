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
from ._market_anchor import anchor as _anchor
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
        "deriv_market_key": "nba:" + str(bet_type),
        "odds": int(ml),
    }, sport="nba", juice_wall=JUICE_WALL)


RAW_EDGE_FLOOR = 3.0  # %% new/unproven NBA markets: capture leans for shadow-tracking


def _score_raw(bet_type: str, pick_text: str, prob: float, ml) -> dict | None:
    """Raw-edge scoring for NEW NBA markets so they GENERATE + shadow-track +
    graduate (calibrated score_pick defers unproven markets -> ~0 edge -> no
    pick). Juice wall + valid odds + modest floor only; no calibration."""
    if not _valid_odds(ml):
        return None
    if int(ml) < JUICE_WALL:
        return None
    imp = _implied(int(ml))
    edge = (float(prob) - imp) * 100.0
    if edge < RAW_EDGE_FLOOR:
        return None
    return {"type": bet_type, "pick": pick_text, "prob": round(float(prob), 4),
            "edge": round(edge, 1), "odds": int(ml), "confidence": "lean",
            "deriv_market_key": "nba:" + str(bet_type)}


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
        for direction, prob, ml, opp in (
            ("Over",  p_over,  market.get("over_odds"), market.get("under_odds")),
            ("Under", 1.0 - p_over, market.get("under_odds"), market.get("over_odds")),
        ):
            label = f"{abbr} Q1 {direction} {line}"
            scored = _score("Q1 Team Total", label,
                            _anchor(prob, ml, opp, bet_type="Q1 Team Total", pick=label), ml)
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
    for label, prob, ml, opp in (
        ("Odd",  p_odd,  market.get("odd_odds"), market.get("even_odds")),
        ("Even", 1.0 - p_odd, market.get("even_odds"), market.get("odd_odds")),
    ):
        text = f"Q1 Total {label}"
        scored = _score("Q1 Total O/E", text,
                        _anchor(prob, ml, opp, bet_type="Q1 Total O/E", pick=text), ml)
        if scored:
            picks.append(scored)


def _team_full_sigma() -> float:
    """Per-team FULL-game sigma (fallback only; the MC team distribution is
    primary). NBA team score std ~ full-margin_std / sqrt(2) ~ 12."""
    return 12.0


def _append_team_totals(picks: list, pred: dict, odds: dict,
                         h_abbr: str, a_abbr: str) -> None:
    """Full-game per-team totals. Primary = the full MC's empirical per-team
    over/under at the posted line (pred['mc_full']['team_totals']); gaussian
    fallback off the team's expected points. Mirrors _append_q1_team_totals."""
    if not get_flag("ENABLE_NBA_TEAM_TOTAL", True):
        return
    mc = pred.get("mc_full") or {}
    mc_tt = (mc.get("team_totals") or {}) if "error" not in mc else {}
    for side, abbr, key in (("home", h_abbr, "team_total_home"),
                            ("away", a_abbr, "team_total_away")):
        market = odds.get(key)
        if not isinstance(market, dict):
            continue
        line = market.get("line")
        if line is None:
            continue
        try:
            line = float(line)
        except (TypeError, ValueError):
            continue
        tt = mc_tt.get(side) or {}
        bucket = (tt.get("lines") or {}).get(f"{line:.1f}")
        if bucket and bucket.get("over") is not None:
            p_over = float(bucket["over"])
        else:
            mu = tt.get("expected")
            if mu is None:
                continue
            p_over = _gaussian_p_over(float(mu), _team_full_sigma(), line)
        for direction, prob, ml, opp in (
                ("Over", p_over, market.get("over_odds"), market.get("under_odds")),
                ("Under", 1.0 - p_over, market.get("under_odds"), market.get("over_odds"))):
            label = f"{abbr} {direction} {line}"
            scored = _score("TEAM_TOTAL", label,
                            _anchor(prob, ml, opp, bet_type="TEAM_TOTAL", pick=label), ml)
            if scored:
                picks.append(scored)


# Half distribution shape MEASURED on 4,181 games 2023-26 (nba_games quarter scores,
# MintyBot 2026-10-26). sigma_full/sqrt2 assumed independent halves: H1 total came out
# 15.0 vs 12.85 real (17% too wide -> mispriced every half total), H1 margin 11.4 vs 12.0.
# H2 carries overtime (wider). Margin shares stay 0.5 (slope H1~full measured 0.52).
_HALF_SIGMA = {"H1": (12.01, 12.85), "H2": (11.55, 14.57)}   # (margin, total)
_HALF_TOTAL_SHARE = {"H1": 0.5014, "H2": 0.4986}


def _half_margin_std(code: str = "H1") -> float:
    return _HALF_SIGMA.get(code, _HALF_SIGMA["H1"])[0]


def _half_total_std(code: str = "H1") -> float:
    return _HALF_SIGMA.get(code, _HALF_SIGMA["H1"])[1]


def _append_half_picks(picks: list, pred: dict, odds: dict,
                       h_abbr: str, a_abbr: str) -> None:
    """Append NBA 1st-half / 2nd-half ML / spread / total picks.

    HR posts H1/H2 ML+spread+total; the scraper files them nested at
    ``odds['periods']['H1'|'H2']`` (full-game key shape). We price each
    half by SCALING the full-game prediction — margin ~ mu_full*0.5,
    total ~ mu_full*(0.505 H1 / 0.495 H2), Gaussian with sigma_full/sqrt2 —
    then anchor to HR's half line and raw-edge score (new market ->
    shadow-track -> graduate, same path as TEAM_TOTAL). No factor stacking."""
    if not get_flag("ENABLE_NBA_HALVES", True):
        return
    periods = (odds.get("periods") or {}) if isinstance(odds, dict) else {}
    if not isinstance(periods, dict) or not periods:
        return
    fp = pred.get("full") if isinstance(pred, dict) and isinstance(pred.get("full"), dict) else pred
    if not isinstance(fp, dict):
        return
    mu_margin = fp.get("predicted_margin")
    mu_total = fp.get("predicted_total")
    if mu_margin is None:
        mp = fp.get("margin_probs") or {}
        try:
            mu_margin = sum(float(k) * float(v) for k, v in mp.items()) if mp else None
        except Exception:
            mu_margin = None
    if mu_total is None:
        tp = fp.get("total_probs") or {}
        try:
            mu_total = sum(float(k) * float(v) for k, v in tp.items()) if tp else None
        except Exception:
            mu_total = None
    if mu_margin is None or mu_total is None:
        return
    try:
        mu_margin = float(mu_margin); mu_total = float(mu_total)
    except (TypeError, ValueError):
        return
    for code, lbl in (("H1", "1H"), ("H2", "2H")):
        hb = periods.get(code)
        if not isinstance(hb, dict):
            continue
        sig_m, sig_t = _half_margin_std(code), _half_total_std(code)
        h_margin = mu_margin * 0.5           # margin accrues ~evenly across halves
        h_total = mu_total * _HALF_TOTAL_SHARE[code]

        # Half Moneyline (HR posts draw-no-bet; price P(half margin > 0)).
        home_ml, away_ml = hb.get("home_ml"), hb.get("away_ml")
        if _valid_odds(home_ml) and _valid_odds(away_ml):
            p_home = _norm_cdf(h_margin / sig_m)
            for abbr, prob, ml, opp in ((h_abbr, p_home, home_ml, away_ml),
                                        (a_abbr, 1.0 - p_home, away_ml, home_ml)):
                label = f"{abbr} {lbl} ML"
                sc = _score(code + "_ML", label,
                            _anchor(prob, ml, opp, bet_type=code + "_ML", pick=label), ml)
                if sc:
                    picks.append(sc)

        # Half Spread (home_spread_point = home handicap; cover if margin > -pt).
        hp = hb.get("home_spread_point")
        try:
            hp = float(hp) if hp is not None else None
        except (TypeError, ValueError):
            hp = None
        if hp is not None:
            h_odds, a_odds = hb.get("home_spread_odds"), hb.get("away_spread_odds")
            try:
                ap = float(hb.get("away_spread_point")) if hb.get("away_spread_point") is not None else -hp
            except (TypeError, ValueError):
                ap = -hp
            cover = _norm_cdf((h_margin + hp) / sig_m)
            for abbr, pt, prob, ml, opp in ((h_abbr, hp, cover, h_odds, a_odds),
                                            (a_abbr, ap, 1.0 - cover, a_odds, h_odds)):
                label = f"{abbr} {pt:+.1f} {lbl}"
                sc = _score(code + "_SPREAD", label,
                            _anchor(prob, ml, opp, bet_type=code + "_SPREAD", pick=label), ml)
                if sc:
                    picks.append(sc)

        # Half Total.
        ou = hb.get("over_under")
        try:
            ou = float(ou) if ou is not None else None
        except (TypeError, ValueError):
            ou = None
        if ou is not None:
            o_odds, u_odds = hb.get("over_odds"), hb.get("under_odds")
            p_over = _norm_cdf((h_total - ou) / sig_t)
            for label, prob, ml, opp in ((f"Over {ou:g} {lbl}", p_over, o_odds, u_odds),
                                         (f"Under {ou:g} {lbl}", 1.0 - p_over, u_odds, o_odds)):
                sc = _score(code + "_TOTAL", label,
                            _anchor(prob, ml, opp, bet_type=code + "_TOTAL", pick=label), ml)
                if sc:
                    picks.append(sc)


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
