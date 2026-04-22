"""
NHL bet selection.

Separated from nhl_predict.py to keep game prediction and market
decision-making in distinct modules. This file converts model
probabilities into concrete betting picks by comparing against
real odds and applying edge/priority filtering.

Used by: Best Bets, NHL Pick Tracker, Game Detail sidebar.
"""

import logging

from .config import (
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

    When `pred` is provided the factor pipeline is skipped; callers that
    pre-compute MC + GBM alongside the factor model pass the augmented
    dict in so ensemble_nhl() blends all three signals. Without it we
    fall back to factor-only (legacy behavior).
    """
    from .nhl_predict import predict_matchup

    if pred is None:
        pred = predict_matchup(home_key, away_key)
    if not pred:
        return [], {}

    # Compute the ensemble blend. pred may already carry factor + MC +
    # GBM (when the backend helper ran them); if only factor is present
    # ensemble_nhl collapses to factor-only.
    try:
        from .ensemble import ensemble_nhl
        pred["ensemble"] = ensemble_nhl(pred)
    except Exception as e:
        logger.debug("NHL ensemble blend failed: %s", e)
        pred["ensemble"] = {}

    odds = _sanitize_odds(odds)
    wp = pred["win_prob"]
    pl = pred["puck_line"]

    # Route ML home WP through the ensemble when present; otherwise
    # the original factor-only win_prob dict is used. O/U and PL
    # distributions stay on the factor model.
    ens = pred.get("ensemble") or {}
    if ens.get("home_win") is not None:
        wp = {"home": float(ens["home_win"]),
              "away": 1.0 - float(ens["home_win"])}

    h_abbr = pred["home"]["abbreviation"]
    a_abbr = pred["away"]["abbreviation"]

    picks = []

    # ── Moneyline ──
    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")

    # ML picks
    if ENABLE_NHL_ML:
        if home_ml and home_ml >= JUICE_WALL:
            edge = (wp["home"] - _implied(home_ml)) * 100
            if edge > 0:
                picks.append({
                    "type": "ML", "pick": h_abbr, "prob": round(wp["home"], 4),
                    "edge": round(edge, 1), "odds": home_ml,
                })

        if away_ml and away_ml >= JUICE_WALL:
            edge = (wp["away"] - _implied(away_ml)) * 100
            if edge > 0:
                picks.append({
                    "type": "ML", "pick": a_abbr, "prob": round(wp["away"], 4),
                    "edge": round(edge, 1), "odds": away_ml,
                })

    # ── Totals (O/U) ──
    vegas_total = odds.get("over_under")
    if ENABLE_NHL_OU and vegas_total and pred.get("over_under"):
        vt = float(vegas_total)
        # Find closest line
        best_key = None
        best_diff = 999
        for k in pred["over_under"]:
            diff = abs(float(k) - vt)
            if diff < best_diff:
                best_diff = diff
                best_key = k

        if best_key:
            ou = pred["over_under"][best_key]
            pick_over = ou["over"] > ou["under"]
            prob = max(ou["over"], ou["under"])
            label = f"{'Over' if pick_over else 'Under'} {vt}"

            real_odds = odds.get("over_odds") if pick_over else odds.get("under_odds")
            if real_odds:
                implied = _implied(real_odds)
            else:
                implied = 0.524
                real_odds = -110

            edge = (prob - implied) * 100
            if edge > 0 and real_odds >= JUICE_WALL:
                picks.append({
                    "type": "O/U", "pick": label, "prob": round(prob, 4),
                    "edge": round(edge, 1), "odds": real_odds,
                })

    # ── Puck Line ──
    home_pl_odds = odds.get("home_spread_odds")
    away_pl_odds = odds.get("away_spread_odds")
    home_pl_point = odds.get("home_spread_point")
    away_pl_point = odds.get("away_spread_point")

    if not ENABLE_NHL_PL:
        # Skip PL entirely when disabled (still computed above for display parity)
        home_pl_point = None
        away_pl_point = None

    # Derive from ML if no puck line data
    if home_pl_point is None and home_ml and away_ml:
        home_is_fav = home_ml < away_ml
        if home_is_fav:
            home_pl_point = -1.5
            away_pl_point = 1.5
            home_pl_odds = home_pl_odds or 170
            away_pl_odds = away_pl_odds or -200
        else:
            home_pl_point = 1.5
            away_pl_point = -1.5
            home_pl_odds = home_pl_odds or -200
            away_pl_odds = away_pl_odds or 170

    if home_pl_point is not None:
        # Home puck line
        if home_pl_point < 0:
            h_pl_prob = pl["home_minus_1_5"]
        else:
            h_pl_prob = pl["home_plus_1_5"]

        if home_pl_odds and home_pl_odds >= JUICE_WALL:
            h_edge = (h_pl_prob - _implied(home_pl_odds)) * 100
            if h_edge > 0:
                picks.append({
                    "type": "PL", "pick": f"{h_abbr} {home_pl_point:+.1f}",
                    "prob": round(h_pl_prob, 4),
                    "edge": round(h_edge, 1), "odds": home_pl_odds,
                })

    if away_pl_point is not None:
        if away_pl_point < 0:
            a_pl_prob = pl["away_minus_1_5"]
        else:
            a_pl_prob = pl["away_plus_1_5"]

        if away_pl_odds and away_pl_odds >= JUICE_WALL:
            a_edge = (a_pl_prob - _implied(away_pl_odds)) * 100
            if a_edge > 0:
                picks.append({
                    "type": "PL", "pick": f"{a_abbr} {away_pl_point:+.1f}",
                    "prob": round(a_pl_prob, 4),
                    "edge": round(a_edge, 1), "odds": away_pl_odds,
                })

    # CI band on the win-probability point estimate (data-quality
    # uncertainty). Same shape as MLB so the shared ProbHistogram
    # component renders identically across all three sports.
    ci_hw = (pred.get("confidence") or {}).get("ci_half_width", 0.05)
    for p in picks:
        prob = p.get("prob")
        if prob is None:
            continue
        p["prob_low"] = round(max(0.0, prob - ci_hw), 4)
        p["prob_high"] = round(min(1.0, prob + ci_hw), 4)
        p["ci_half_width"] = ci_hw

    # Empirical recalibration. Same shape as engine/picks.py: replace
    # each pick's prob with the bucket's empirical NHL win-rate from the
    # nhl_picks tracker, then recompute edge against the calibrated
    # prob. Buckets without enough samples (MIN_BUCKET_N) pass through
    # unchanged so cold-start picks aren't penalised.
    from .empirical_calibration import calibrate as _calibrate
    for p in picks:
        prob = p.get("prob")
        odds = p.get("odds")
        if prob is None:
            continue
        cal = _calibrate(
            p["type"], float(prob), sport="nhl",
            edge=p.get("edge"), odds=odds,
        )
        p["prob_raw"] = round(float(prob), 4)
        p["prob"] = round(float(cal), 4)
        if odds is not None and _valid_odds(odds):
            p["edge"] = round((cal - _implied(int(odds))) * 100, 1)

    # Keep pred["win_prob"] consistent with the calibrated ML pick prob
    # so the Projected Outcome panel and the pick card display the same
    # number.
    wp = pred.get("win_prob") or {}
    home_wp = wp.get("home")
    if home_wp is not None:
        pred.setdefault("factor_win_prob", dict(wp))
        cal_home = float(_calibrate("ML", float(home_wp), sport="nhl"))
        pred["win_prob"] = {
            "home": round(cal_home, 4),
            "away": round(1.0 - cal_home, 4),
        }

    # Drop picks that flipped to negative edge after calibration.
    picks = [p for p in picks if (p.get("edge") or 0) > 0]

    # Conservatism ladder: swap low-probability picks to their safest
    # qualifying same-direction sibling so WR% rides higher through
    # variance. No-op for picks already above the activation threshold.
    try:
        from .conservatism import apply_ladder as _conservatism_ladder
        picks = _conservatism_ladder(picks, pred, odds, "nhl", h_abbr, a_abbr)
    except Exception as e:
        logger.warning("NHL conservatism ladder error: %s", e)

    # Adjusted EV: edge * reliability weight
    for p in picks:
        reliability = NHL_BET_RELIABILITY.get(p["type"], 0.5)
        p["adjusted_ev"] = round(p["edge"] * reliability, 2)
    picks.sort(key=lambda p: -p["adjusted_ev"])

    # Assign confidence (thresholds centralised in engine.config)
    from .config import EDGE_STRONG, EDGE_MODERATE, EDGE_LEAN, EDGE_SKIP
    for p in picks:
        e = p["edge"]
        if e >= EDGE_STRONG:
            p["confidence"] = "strong"
        elif e >= EDGE_MODERATE:
            p["confidence"] = "moderate"
        elif e >= EDGE_LEAN:
            p["confidence"] = "lean"
        else:
            p["confidence"] = "skip"
        if e < EDGE_SKIP:
            p["confidence"] = "skip"

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
    }
    return picks, context
