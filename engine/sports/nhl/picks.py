"""
NHL bet selection.

Separated from nhl_predict.py to keep game prediction and market
decision-making in distinct modules. This file converts model
probabilities into concrete betting picks by comparing against
real odds and applying edge/priority filtering.

Used by: Best Bets, NHL Pick Tracker, Game Detail sidebar.
"""

import logging

from ...config import (
    NHL_JUICE_WALL as JUICE_WALL,
    NHL_BET_RELIABILITY,
    ENABLE_NHL_ML,
    ENABLE_NHL_OU,
    ENABLE_NHL_PL,
)

logger = logging.getLogger(__name__)


def _implied(ml: int) -> float:
    """American odds to implied probability."""
    if ml < 0:
        return abs(ml) / (abs(ml) + 100)
    return 100 / (ml + 100)


def _valid_odds(ml) -> bool:
    """|ml| >= 100 shape check. See engine/picks.py for rationale."""
    if ml is None:
        return False
    try:
        ml = int(ml)
    except (TypeError, ValueError):
        return False
    return abs(ml) >= 100


def _sanitize_odds(odds: dict | None) -> dict:
    """Null out _ml / _odds fields that aren't valid American prices."""
    if not odds:
        return {}
    cleaned = dict(odds)
    for k, v in list(cleaned.items()):
        if (k.endswith("_ml") or k.endswith("_odds")) and not _valid_odds(v):
            cleaned[k] = None
    return cleaned


def generate_nhl_picks(home_key: str, away_key: str,
                       odds: dict | None = None) -> list[dict]:
    """
    Generate all NHL picks for a matchup with edge calculations.

    Backtesting shows ML is -6% ROI while O/U is +19% and PL is +12%.
    Picks are prioritised: O/U (1) > PL (2) > ML (3) so that the best
    pick per game is O/U or PL unless neither has any edge.
    """
    picks, _ = generate_nhl_picks_with_context(home_key, away_key, odds)
    return picks


def generate_nhl_picks_with_context(home_key: str, away_key: str,
                                    odds: dict | None = None,
                                    pred: dict | None = None,
                                    ) -> tuple[list[dict], dict]:
    """
    Generate NHL picks and return both the picks list and a context dict
    with rest, injuries, and other metadata so callers can surface b2b
    warnings and injury impact without re-running the full prediction.

    Pick Authority (MintyBot 2026-10-26, Austin "Let's make sure NHL is up to snuff"): every
    market prices off ONE score distribution -- the ratings model (engine.sports.nhl._ratings,
    overlaid on ``pred`` by predict_matchup) -- and each candidate anchors to the de-vigged
    market with its own Pick Authority weight (engine.sports.nhl._market_anchor; 0.50 proven
    / 0.15 unproven / 1.0 inside shadow_mode). That anchor is the only calibration
    (calibration_gate.anchor_only("nhl")). Both sides of every two-way market are scored; HR
    lines the model has no number for are skipped rather than guessed (the old path invented
    a +170/-200 puck line from the moneyline).
    """
    from .predict import predict_matchup
    from . import _market_anchor as _ma

    if pred is None:
        pred = predict_matchup(home_key, away_key)
    if not pred:
        return [], {}

    try:
        from ...config import NHL_MODEL as _NHL_MODEL
    except Exception:
        _NHL_MODEL = "ratings"
    _ratings = pred.get("model") == "ratings"
    if _ratings:
        # One model -> no blend; keep the ensemble block for readers that expect it.
        pred["ensemble"] = {"home_win": float(pred["win_prob"]["home"]),
                            "total_expected": float(pred.get("total") or 0.0),
                            "weights_used": {"ratings": 1.0}}
    else:
        try:
            from ...ensemble import ensemble_nhl
            pred["ensemble"] = ensemble_nhl(pred)
        except Exception as e:
            logger.debug("NHL ensemble blend failed: %s", e)
            pred["ensemble"] = {}

    context = {
        "rest": pred.get("rest", {}),
        "injuries": {
            "home_impact": pred.get("injuries", {}).get("home_impact", 1.0),
            "away_impact": pred.get("injuries", {}).get("away_impact", 1.0),
        },
        "win_prob": pred.get("win_prob", {}),
        "expected_score": pred.get("expected_score", {}),
        "factors": pred.get("factors", {}),
        "season_context": pred.get("season_context", {}),
        "granular": pred.get("granular", {}),
        "model": pred.get("model"),
    }
    if _NHL_MODEL == "ratings" and not _ratings:
        # The ratings model couldn't price this game; never fall back to the factor numbers.
        return [], context

    odds = _sanitize_odds(odds)
    wp = pred["win_prob"]
    if not _ratings:
        ens = pred.get("ensemble") or {}
        if ens.get("home_win") is not None:
            wp = {"home": float(ens["home_win"]), "away": 1.0 - float(ens["home_win"])}

    h_abbr = pred["home"]["abbreviation"]
    a_abbr = pred["away"]["abbreviation"]
    from ...picks_core import score_pick as _score_pick

    def _emit(bet_type, pick_text, prob, ml, opp_ml):
        if prob is None or ml is None:
            return
        scored = _score_pick({
            "type": bet_type, "pick": pick_text,
            "raw_prob": _ma.anchor(float(prob), ml, opp_ml, bet_type, pick_text),
            "odds": int(ml),
        }, sport="nhl", juice_wall=JUICE_WALL)
        if scored:
            scored["prob_model"] = round(float(prob), 4)
            picks.append(scored)

    picks = []

    # ── Moneyline (both sides) ──
    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")
    if ENABLE_NHL_ML and home_ml is not None and away_ml is not None:
        _emit("ML", h_abbr, wp["home"], home_ml, away_ml)
        _emit("ML", a_abbr, wp["away"], away_ml, home_ml)

    # ── Totals (both sides, HR's own line) ──
    vegas_total = odds.get("over_under")
    ou_dist = pred.get("over_under") or {}
    if ENABLE_NHL_OU and vegas_total is not None and ou_dist:
        try:
            vt = float(vegas_total)
        except (TypeError, ValueError):
            vt = None
        cell = ou_dist.get(f"{vt:.1f}") if vt is not None else None
        if cell is None and vt is not None and not _ratings:
            # Factor fallback: nearest line in its per-line table.
            k = min(ou_dist, key=lambda x: abs(float(x) - vt), default=None)
            cell = ou_dist.get(k) if k is not None else None
        oo, uo = odds.get("over_odds"), odds.get("under_odds")
        if cell and oo is not None and uo is not None:
            _emit("O/U", f"Over {vt}", cell.get("over"), oo, uo)
            _emit("O/U", f"Under {vt}", cell.get("under"), uo, oo)

    # Alt totals (MintyBot 2026-10-27, Austin "It should be looking at alt lines"): HR's alt
    # ladder, both sides, off the same score grid. Own bet type (ALT O/U -> the ALT_TOTAL cell) so
    # the Pick Authority grades the ladder as its own market, never inside the main-line O/U cell.
    if ENABLE_NHL_OU and ou_dist and _ratings:
        try:
            _main_t = float(vegas_total) if vegas_total is not None else None
        except (TypeError, ValueError):
            _main_t = None
        for alt in odds.get("alt_totals") or []:
            try:
                lt = float(alt.get("line"))
            except (TypeError, ValueError):
                continue
            acell = ou_dist.get(f"{lt:.1f}") if lt != _main_t else None
            aoo, auo = alt.get("over_odds"), alt.get("under_odds")
            if acell and _valid_odds(aoo) and _valid_odds(auo):
                _emit("ALT O/U", f"Over {lt}", acell.get("over"), aoo, auo)
                _emit("ALT O/U", f"Under {lt}", acell.get("under"), auo, aoo)

    # ── Puck line (both sides, HR's own points) ──
    hp, ap = odds.get("home_spread_point"), odds.get("away_spread_point")
    ho, ao = odds.get("home_spread_odds"), odds.get("away_spread_odds")
    if ENABLE_NHL_PL and hp is not None and ap is not None and ho is not None and ao is not None:
        cov = pred.get("spread_cover") or {}
        pl = pred.get("puck_line") or {}
        try:
            hp, ap = float(hp), float(ap)
        except (TypeError, ValueError):
            hp = ap = None
        if hp is not None:
            h_prob = cov.get(f"home_{hp:+.1f}")
            if h_prob is None and abs(hp) == 1.5:
                h_prob = pl.get("home_minus_1_5" if hp < 0 else "home_plus_1_5")
            a_prob = (1.0 - cov[f"home_{-ap:+.1f}"]) if f"home_{-ap:+.1f}" in cov else (
                pl.get("away_minus_1_5" if ap < 0 else "away_plus_1_5") if abs(ap) == 1.5 else None)
            _emit("PL", f"{h_abbr} {hp:+.1f}", h_prob, ho, ao)
            _emit("PL", f"{a_abbr} {ap:+.1f}", a_prob, ao, ho)

    # Alt puck lines (MintyBot 2026-10-27): HR's alt_spreads, both sides; "point" is the HOME
    # point. Half-points only -- a whole-number rung can push, and 1 - P(home covers) would
    # misprice the away side.
    if ENABLE_NHL_PL and _ratings:
        cov = pred.get("spread_cover") or {}
        try:
            _main_hp = float(odds.get("home_spread_point"))
        except (TypeError, ValueError):
            _main_hp = None
        for alt in odds.get("alt_spreads") or []:
            try:
                pt = float(alt.get("point"))
            except (TypeError, ValueError):
                continue
            if pt == _main_hp or pt % 1 == 0:
                continue
            h_alt = cov.get(f"home_{pt:+.1f}")
            aho, aao = alt.get("home_odds"), alt.get("away_odds")
            if h_alt is None or not (_valid_odds(aho) and _valid_odds(aao)):
                continue
            _emit("ALT PL", f"{h_abbr} {pt:+.1f}", h_alt, aho, aao)
            _emit("ALT PL", f"{a_abbr} {-pt:+.1f}", 1.0 - float(h_alt), aao, aho)

    # ── Derivatives (team totals, periods, 3-ways, BTS, OT, odd/even) ──
    from .derivative_picks import append_derivative_picks
    append_derivative_picks(picks, pred, odds, h_abbr, a_abbr)

    # CI band on the win-probability point estimate (data-quality uncertainty).
    ci_hw = (pred.get("confidence") or {}).get("ci_half_width", 0.05)
    for p in picks:
        prob = p.get("prob")
        if prob is None:
            continue
        p["prob_low"] = round(max(0.0, prob - ci_hw), 4)
        p["prob_high"] = round(min(1.0, prob + ci_hw), 4)
        p["ci_half_width"] = ci_hw

    picks = [p for p in picks if (p.get("edge") or 0) > 0]

    from ...dynamic_reliability import get_reliability as _get_reliability
    for p in picks:
        reliability = _get_reliability("nhl", p["type"])
        p["adjusted_ev"] = round(p["edge"] * reliability, 2)
    picks.sort(key=lambda p: -p["adjusted_ev"])

    # The legacy per-cell edge floors (engine.edge_floors) were fit on the retired model's
    # record; the Pick Authority (edge_proven) is the gate now, so they no longer apply.
    from ..._pick_helpers import tag_confidence
    tag_confidence(picks)
    return picks, context
