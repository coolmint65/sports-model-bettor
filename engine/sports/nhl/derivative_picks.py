"""
NHL derivative-market pick generators.

Pure probability extraction from the existing ``pred`` dict — no new
factors stacked on the model. Each market reuses what predict_matchup
already produced (per-team xG, per-period xG split, total/margin
distributions, regulation-draw probability) and prices the HR-quoted
line off that.

Wired in from ``engine.nhl_picks.generate_nhl_picks_with_context`` after
the PL block and before CI band annotation; the orchestrator
``append_derivative_picks`` respects ``ENABLE_NHL_DERIVATIVES`` plus
per-market gates so a single misbehaving market can be turned off
without losing the others.

Storage shape on the ``odds`` dict (set by ``scrapers.hardrock_odds``):
  team_total_home / team_total_away = {line, over_odds, under_odds}
  period_totals                     = {"1": {line, over_odds, under_odds}, ...}
  period_bts                        = {"1": {yes_odds, no_odds}, ...}
  period_dnb                        = {"1": {home_ml, away_ml}, ...}
  total_oe                          = {odd_odds, even_odds}
  overtime                          = {yes_odds, no_odds}
  bts_full                          = {yes_odds, no_odds}

NHL prediction periods are keyed as "P1"/"P2"/"P3" while HR ships
"1"/"2"/"3" — `_period_match` strips the "P" prefix when joining.
"""

from __future__ import annotations

import math

from ...config import NHL_JUICE_WALL as JUICE_WALL, get_flag
from . import _market_anchor as _ma


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


def _score_anchored(bet_type: str, pick_text: str, prob: float, ml) -> dict | None:
    """picks_core.score_pick on an already-anchored probability (juice wall, odds cap, edge
    ceiling, default floor). NHL calibrates once (calibration_gate.anchor_only), so no
    derivative curve is stacked on top."""
    if not _valid_odds(ml) or prob is None:
        return None
    from ...picks_core import score_pick as _score_pick
    out = _score_pick({
        "type": bet_type,
        "pick": pick_text,
        "raw_prob": float(prob),
        "odds": int(ml),
    }, sport="nhl", juice_wall=JUICE_WALL)
    return out


def _score(bet_type: str, pick_text: str, prob: float, ml, opp_ml=None) -> dict | None:
    """Two-way derivative: anchor to the de-vigged pair with this candidate's Pick Authority
    weight (engine.sports.nhl._market_anchor), then score (MintyBot 2026-10-26)."""
    if not _valid_odds(ml) or prob is None:
        return None
    out = _score_anchored(bet_type, pick_text, _ma.anchor(float(prob), ml, opp_ml, bet_type, pick_text), ml)
    if out:
        out["prob_model"] = round(float(prob), 4)
    return out


def _score_3way(bet_type: str, outcomes: list) -> list:
    """Three-way result market [(pick_text, model_prob, odds), ...]: de-vig all three prices
    together, anchor each outcome with its own weight, score. The old path scored these RAW
    (no anchor, 3pp floor) and they shadow-tracked at -24% to -30%."""
    if len(outcomes) != 3 or not all(_valid_odds(o) and p is not None for _, p, o in outcomes):
        return []
    anchored = _ma.anchor_multi([float(p) for _, p, _ in outcomes], [o for _, _, o in outcomes],
                                bet_type, [t for t, _, _ in outcomes])
    out = []
    for (txt, p, o), a in zip(outcomes, anchored):
        sc = _score_anchored(bet_type, txt, a, o)
        if sc:
            sc["prob_model"] = round(float(p), 4)
            out.append(sc)
    return out


def _dist(pred: dict) -> dict:
    """The score distribution the derivative markets read: the ratings model's exact one
    (pred["dist"], MC-shaped) when present, else the Monte Carlo block."""
    d = pred.get("dist")
    if isinstance(d, dict) and d:
        return d
    mc = pred.get("mc") or {}
    return {} if "error" in mc else mc


def _ou_edge(prob: float, ml) -> float | None:
    if not _valid_odds(ml):
        return None
    if ml < JUICE_WALL:
        return None
    return (prob - _implied(int(ml))) * 100


def _poisson_pmf(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def _poisson_p_over(lam: float, line: float, max_k: int = 20) -> float:
    if lam <= 0:
        return 0.0
    threshold = math.floor(line) + 1
    return sum(_poisson_pmf(lam, k) for k in range(threshold, max_k + 1))


def _poisson_p_zero(lam: float) -> float:
    if lam <= 0:
        return 1.0
    return math.exp(-lam)


def _strip_period_prefix(key: str) -> str:
    """Map both 'P1' and '1' to '1' so HR (numeric) and pred ('P'-prefixed)
    period keys can be joined."""
    s = str(key).strip().upper()
    if s.startswith("P"):
        s = s[1:]
    return s


def _period_lookup(periods: list[dict]) -> dict[str, dict]:
    """Index pred['periods'] by stripped period key."""
    out: dict[str, dict] = {}
    for p in periods or []:
        if not isinstance(p, dict):
            continue
        k = p.get("period")
        if k is None:
            continue
        out[_strip_period_prefix(k)] = p
    return out


def _per_period_three_way(lam_h: float, lam_a: float,
                           max_g: int = 8) -> tuple[float, float, float]:
    """Single-period three-way (P(home wins period), P(away wins), P(tie))."""
    p_home = 0.0
    p_away = 0.0
    p_tie = 0.0
    for h in range(max_g + 1):
        ph = _poisson_pmf(lam_h, h)
        for a in range(max_g + 1):
            joint = ph * _poisson_pmf(lam_a, a)
            if h > a:
                p_home += joint
            elif a > h:
                p_away += joint
            else:
                p_tie += joint
    return p_home, p_away, p_tie


def _append_team_total(picks: list, pred: dict, odds: dict,
                        h_abbr: str, a_abbr: str) -> None:
    if not get_flag("ENABLE_NHL_TEAM_TOTAL", True):
        return
    es = pred.get("expected_score") or {}
    mc_tt = _dist(pred).get("team_totals") or {}
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
        # Prefer MC's empirical team-total distribution.
        mc_lines = ((mc_tt.get(side) or {}).get("lines") or {})
        line_str = f"{line:.1f}"
        bucket = mc_lines.get(line_str) if mc_lines else None
        if bucket and bucket.get("over") is not None:
            p_over = float(bucket["over"])
        else:
            p_over = _poisson_p_over(float(lam), line)
        p_under = 1.0 - p_over
        for direction, prob, ml, opp in (
            ("Over",  p_over,  market.get("over_odds"), market.get("under_odds")),
            ("Under", p_under, market.get("under_odds"), market.get("over_odds")),
        ):
            scored = _score("Team Total", f"{abbr} {direction} {line}",
                             prob, ml, opp)
            if scored:
                picks.append(scored)


def _append_period_totals(picks: list, pred: dict, odds: dict,
                            h_abbr: str, a_abbr: str) -> None:
    if not get_flag("ENABLE_NHL_PERIOD_TOTAL", True):
        return
    market = odds.get("period_totals")
    periods = _period_lookup(pred.get("periods") or [])
    if not isinstance(market, dict):
        return
    mc_per_period = _dist(pred).get("per_period") or {}
    for period_key, mkt in market.items():
        per = periods.get(_strip_period_prefix(period_key))
        if not isinstance(mkt, dict):
            continue
        line = mkt.get("line")
        if line is None:
            continue
        try:
            line = float(line)
        except (TypeError, ValueError):
            continue
        # MC has direct over_X_5 probs per period.
        mc_per = mc_per_period.get(_strip_period_prefix(period_key))
        p_over = None
        if mc_per:
            line_key = f"over_{str(line).replace('.', '_')}"
            if line_key in mc_per:
                p_over = float(mc_per[line_key])
        if p_over is None:
            if not per:
                continue
            lam = float(per.get("total", 0.0))
            p_over = _poisson_p_over(lam, line)
        p_under = 1.0 - p_over
        label = _strip_period_prefix(period_key)
        for direction, prob, ml, opp in (
            ("Over",  p_over,  mkt.get("over_odds"), mkt.get("under_odds")),
            ("Under", p_under, mkt.get("under_odds"), mkt.get("over_odds")),
        ):
            scored = _score("Period Total",
                             f"P{label} {direction} {line}",
                             prob, ml, opp)
            if scored:
                picks.append(scored)


def _append_period_bts(picks: list, pred: dict, odds: dict,
                        h_abbr: str, a_abbr: str) -> None:
    if not get_flag("ENABLE_NHL_PERIOD_BTS", True):
        return
    market = odds.get("period_bts")
    periods = _period_lookup(pred.get("periods") or [])
    if not isinstance(market, dict):
        return
    mc_per_period = _dist(pred).get("per_period") or {}
    for period_key, mkt in market.items():
        per = periods.get(_strip_period_prefix(period_key))
        if not isinstance(mkt, dict):
            continue
        mc_per = mc_per_period.get(_strip_period_prefix(period_key))
        if mc_per and mc_per.get("bts_yes") is not None:
            p_yes = float(mc_per["bts_yes"])
        elif per:
            lam_h = float(per.get("home", 0.0))
            lam_a = float(per.get("away", 0.0))
            p_yes = (1.0 - _poisson_p_zero(lam_h)) * (1.0 - _poisson_p_zero(lam_a))
        else:
            continue
        p_no = 1.0 - p_yes
        label = _strip_period_prefix(period_key)
        for direction, prob, ml, opp in (
            ("Yes", p_yes, mkt.get("yes_odds"), mkt.get("no_odds")),
            ("No",  p_no,  mkt.get("no_odds"), mkt.get("yes_odds")),
        ):
            scored = _score("Period BTS",
                             f"P{label} BTS {direction}",
                             prob, ml, opp)
            if scored:
                picks.append(scored)


def _append_period_dnb(picks: list, pred: dict, odds: dict,
                        h_abbr: str, a_abbr: str) -> None:
    """Draw-No-Bet per period: P(home wins period | not tied)."""
    if not get_flag("ENABLE_NHL_PERIOD_DNB", True):
        return
    market = odds.get("period_dnb")
    periods = _period_lookup(pred.get("periods") or [])
    if not isinstance(market, dict):
        return
    mc_per_period = _dist(pred).get("per_period") or {}
    for period_key, mkt in market.items():
        per = periods.get(_strip_period_prefix(period_key))
        if not isinstance(mkt, dict):
            continue
        mc_per = mc_per_period.get(_strip_period_prefix(period_key))
        if mc_per and isinstance(mc_per.get("winner"), dict):
            wnr = mc_per["winner"]
            p_h = float(wnr.get("home", 0))
            p_a = float(wnr.get("away", 0))
        elif per:
            lam_h = float(per.get("home", 0.0))
            lam_a = float(per.get("away", 0.0))
            p_h, p_a, _ = _per_period_three_way(lam_h, lam_a)
        else:
            continue
        decided = p_h + p_a
        if decided <= 0:
            continue
        p_home_dnb = p_h / decided
        p_away_dnb = p_a / decided
        label = _strip_period_prefix(period_key)
        for who, prob, ml, opp in (
            (h_abbr, p_home_dnb, mkt.get("home_ml"), mkt.get("away_ml")),
            (a_abbr, p_away_dnb, mkt.get("away_ml"), mkt.get("home_ml")),
        ):
            # bet_type stays "Period DNB" so settler + reliability
            # tables don't churn. Pick text drops "DNB" jargon —
            # user reads "P{n} {TEAM}" as the period winner.
            scored = _score("Period DNB", f"P{label} {who}", prob, ml, opp)
            if scored:
                picks.append(scored)


def _append_total_oe(picks: list, pred: dict, odds: dict,
                      h_abbr: str, a_abbr: str) -> None:
    if not get_flag("ENABLE_NHL_TOTAL_OE", True):
        return
    market = odds.get("total_oe")
    if not isinstance(market, dict):
        return
    mc_oe = _dist(pred).get("total_oe") or {}
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
        ("Even", p_even, market.get("even_odds"), market.get("odd_odds")),
    ):
        scored = _score("Total O/E", f"Total {label}", prob, ml, opp)
        if scored:
            picks.append(scored)


def _append_overtime(picks: list, pred: dict, odds: dict,
                      h_abbr: str, a_abbr: str) -> None:
    """Yes/No on the game going to overtime — fed by MC's empirical
    regulation-tie rate when available, factor's draw_prob otherwise."""
    if not get_flag("ENABLE_NHL_OVERTIME", True):
        return
    market = odds.get("overtime")
    if not isinstance(market, dict):
        return
    mc_ot = _dist(pred).get("overtime") or {}
    if mc_ot.get("yes") is not None:
        p_yes = float(mc_ot["yes"])
    else:
        p_ot = pred.get("regulation_draw_prob")
        if p_ot is None:
            return
        p_yes = float(p_ot)
    p_no = max(0.0, 1.0 - p_yes)
    for direction, prob, ml, opp in (
        ("Yes", p_yes, market.get("yes_odds"), market.get("no_odds")),
        ("No",  p_no,  market.get("no_odds"), market.get("yes_odds")),
    ):
        scored = _score("Overtime", f"Overtime {direction}", prob, ml, opp)
        if scored:
            picks.append(scored)


def _append_bts_full(picks: list, pred: dict, odds: dict,
                      h_abbr: str, a_abbr: str) -> None:
    """Full-game both-teams-to-score. Prefers MC's empirical bts_full
    (counts sims where both teams scored) over the Poisson assumption."""
    if not get_flag("ENABLE_NHL_BTS_FULL", True):
        return
    market = odds.get("bts_full")
    if not isinstance(market, dict):
        return
    mc_bts = _dist(pred).get("bts_full") or {}
    if mc_bts.get("yes") is not None:
        p_yes = float(mc_bts["yes"])
    else:
        es = pred.get("expected_score") or {}
        lam_h = es.get("home")
        lam_a = es.get("away")
        if lam_h is None or lam_a is None:
            return
        p_yes = (1.0 - _poisson_p_zero(float(lam_h))) * \
                (1.0 - _poisson_p_zero(float(lam_a)))
    p_no = max(0.0, 1.0 - p_yes)
    for direction, prob, ml, opp in (
        ("Yes", p_yes, market.get("yes_odds"), market.get("no_odds")),
        ("No",  p_no,  market.get("no_odds"), market.get("yes_odds")),
    ):
        scored = _score("BTS", f"BTS {direction}", prob, ml, opp)
        if scored:
            picks.append(scored)


def _period_spread_cover(lam_h: float, lam_a: float, home_point: float,
                         max_g: int = 12) -> float:
    """P(home covers a period spread: home_goals + home_point > away_goals)."""
    p = 0.0
    for h in range(max_g + 1):
        ph = _poisson_pmf(lam_h, h)
        if ph <= 0:
            continue
        for a in range(max_g + 1):
            if (h + home_point) > a:
                p += ph * _poisson_pmf(lam_a, a)
    return p


def _append_regulation_3way(picks: list, pred: dict, odds: dict,
                            h_abbr: str, a_abbr: str) -> None:
    """Full-game 3-way in regulation (home / tie / away). Model: full-game
    win_prob (incl OT) minus the OT coin-flip on the regulation-tie mass."""
    if not get_flag("ENABLE_NHL_REG_3WAY", True):
        return
    market = odds.get("regulation_3way")
    if not isinstance(market, dict):
        return
    reg = pred.get("regulation") or {}
    if all(reg.get(k) is not None for k in ("home", "tie", "away")):
        p_home, p_tie, p_away = float(reg["home"]), float(reg["tie"]), float(reg["away"])
    else:
        wp = pred.get("win_prob") or {}
        reg_tie = pred.get("regulation_tie_prob")
        if reg_tie is None:
            reg_tie = pred.get("regulation_draw_prob")
        try:
            win_h = float(wp.get("home")); win_a = float(wp.get("away"))
            reg_tie = float(reg_tie)
        except (TypeError, ValueError):
            return
        p_home = max(0.0, win_h - 0.5 * reg_tie)
        p_away = max(0.0, win_a - 0.5 * reg_tie)
        tot = p_home + p_away + reg_tie
        if tot <= 0:
            return
        p_home, p_away, p_tie = p_home / tot, p_away / tot, reg_tie / tot
    picks.extend(_score_3way("Regulation 3-Way", [
        (f"REG {h_abbr}", p_home, market.get("home_ml")),
        ("REG Tie", p_tie, market.get("tie_ml")),
        (f"REG {a_abbr}", p_away, market.get("away_ml"))]))


def _append_period_3way(picks: list, pred: dict, odds: dict,
                        h_abbr: str, a_abbr: str) -> None:
    """Per-period 3-way result (home / tie / away) from the period scoring rates."""
    if not get_flag("ENABLE_NHL_PERIOD_3WAY", True):
        return
    market = odds.get("period_3way")
    periods = _period_lookup(pred.get("periods") or [])
    if not isinstance(market, dict):
        return
    for period_key, mkt in market.items():
        per = periods.get(_strip_period_prefix(period_key))
        if not per or not isinstance(mkt, dict):
            continue
        lam_h = float(per.get("home", 0.0)); lam_a = float(per.get("away", 0.0))
        p_h, p_a, p_t = _per_period_three_way(lam_h, lam_a)
        label = _strip_period_prefix(period_key)
        picks.extend(_score_3way("Period 3-Way", [
            (f"P{label} {h_abbr}", p_h, mkt.get("home_ml")),
            (f"P{label} Tie", p_t, mkt.get("tie_ml")),
            (f"P{label} {a_abbr}", p_a, mkt.get("away_ml"))]))


def _append_period_spread(picks: list, pred: dict, odds: dict,
                         h_abbr: str, a_abbr: str) -> None:
    """Per-period puck line (home_goals + point vs away_goals), Poisson margin."""
    if not get_flag("ENABLE_NHL_PERIOD_SPREAD", True):
        return
    market = odds.get("period_spread")
    periods = _period_lookup(pred.get("periods") or [])
    if not isinstance(market, dict):
        return
    for period_key, mkt in market.items():
        per = periods.get(_strip_period_prefix(period_key))
        if not per or not isinstance(mkt, dict):
            continue
        lam_h = float(per.get("home", 0.0)); lam_a = float(per.get("away", 0.0))
        label = _strip_period_prefix(period_key)
        hp, ap = mkt.get("home_point"), mkt.get("away_point")
        if hp is not None:
            p_home = _period_spread_cover(lam_h, lam_a, float(hp))
            scored = _score("Period Spread", f"P{label} {h_abbr} {float(hp):+g}",
                            p_home, mkt.get("home_odds"), mkt.get("away_odds"))
            if scored:
                picks.append(scored)
        if ap is not None:
            p_away = _period_spread_cover(lam_a, lam_h, float(ap))
            scored = _score("Period Spread", f"P{label} {a_abbr} {float(ap):+g}",
                            p_away, mkt.get("away_odds"), mkt.get("home_odds"))
            if scored:
                picks.append(scored)


def append_derivative_picks(picks: list, pred: dict, odds: dict,
                             h_abbr: str, a_abbr: str) -> None:
    """Append all enabled NHL derivative picks in-place. Master gate is
    ``ENABLE_NHL_DERIVATIVES``; each market then has its own flag."""
    if not get_flag("ENABLE_NHL_DERIVATIVES", True):
        return
    if not isinstance(odds, dict) or not pred:
        return
    _append_team_total(picks, pred, odds, h_abbr, a_abbr)
    _append_period_totals(picks, pred, odds, h_abbr, a_abbr)
    _append_period_bts(picks, pred, odds, h_abbr, a_abbr)
    _append_period_dnb(picks, pred, odds, h_abbr, a_abbr)
    _append_total_oe(picks, pred, odds, h_abbr, a_abbr)
    _append_overtime(picks, pred, odds, h_abbr, a_abbr)
    _append_bts_full(picks, pred, odds, h_abbr, a_abbr)
    _append_regulation_3way(picks, pred, odds, h_abbr, a_abbr)
    _append_period_3way(picks, pred, odds, h_abbr, a_abbr)
    _append_period_spread(picks, pred, odds, h_abbr, a_abbr)
