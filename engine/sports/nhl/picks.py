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

    When `pred` is provided the factor pipeline is skipped; callers that
    pre-compute MC + GBM alongside the factor model pass the augmented
    dict in so ensemble_nhl() blends all three signals. Without it we
    fall back to factor-only (legacy behavior).
    """
    from .predict import predict_matchup

    if pred is None:
        pred = predict_matchup(home_key, away_key)
    if not pred:
        return [], {}

    # Compute the ensemble blend. pred may already carry factor + MC +
    # GBM (when the backend helper ran them); if only factor is present
    # ensemble_nhl collapses to factor-only.
    try:
        from ...ensemble import ensemble_nhl
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
    # Migrated 2026-05-02 to engine.picks_core.score_pick. The shared
    # core handles juice wall + odds cap + belief gate + calibration
    # + edge ceiling + edge floor in one place. To add a new gate
    # that should apply across sports, edit picks_core, not here.
    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")

    if ENABLE_NHL_ML:
        from ...picks_core import score_pick as _score_pick
        if home_ml is not None:
            scored = _score_pick({
                "type": "ML", "pick": h_abbr,
                "raw_prob": wp["home"], "odds": home_ml,
            }, sport="nhl", juice_wall=JUICE_WALL)
            if scored:
                picks.append(scored)
        if away_ml is not None:
            scored = _score_pick({
                "type": "ML", "pick": a_abbr,
                "raw_prob": wp["away"], "odds": away_ml,
            }, sport="nhl", juice_wall=JUICE_WALL)
            if scored:
                picks.append(scored)

    # ── Totals (O/U) ──  Migrated 2026-05-02 to picks_core.score_pick.
    vegas_total = odds.get("over_under")
    if ENABLE_NHL_OU and vegas_total and pred.get("over_under"):
        vt = float(vegas_total)
        # Find closest line in the pred's per-line distribution
        best_key = min(pred["over_under"],
                        key=lambda k: abs(float(k) - vt),
                        default=None)
        if best_key:
            ou = pred["over_under"][best_key]
            pick_over = ou["over"] > ou["under"]
            prob = max(ou["over"], ou["under"])
            label = f"{'Over' if pick_over else 'Under'} {vt}"
            real_odds = (odds.get("over_odds") if pick_over
                         else odds.get("under_odds")) or -110
            from ...picks_core import score_pick as _score_pick
            scored = _score_pick({
                "type": "O/U",
                "pick": label,
                "raw_prob": prob,
                "odds": real_odds,
            }, sport="nhl", juice_wall=JUICE_WALL)
            if scored:
                picks.append(scored)

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

    # Migrated 2026-05-02 to engine.picks_core.score_pick.
    from ...picks_core import score_pick as _score_pick
    if home_pl_point is not None and home_pl_odds:
        h_pl_prob = (pl["home_minus_1_5"] if home_pl_point < 0
                     else pl["home_plus_1_5"])
        scored = _score_pick({
            "type": "PL",
            "pick": f"{h_abbr} {home_pl_point:+.1f}",
            "raw_prob": h_pl_prob,
            "odds": home_pl_odds,
        }, sport="nhl", juice_wall=JUICE_WALL)
        if scored:
            picks.append(scored)
    if away_pl_point is not None and away_pl_odds:
        a_pl_prob = (pl["away_minus_1_5"] if away_pl_point < 0
                     else pl["away_plus_1_5"])
        scored = _score_pick({
            "type": "PL",
            "pick": f"{a_abbr} {away_pl_point:+.1f}",
            "raw_prob": a_pl_prob,
            "odds": away_pl_odds,
        }, sport="nhl", juice_wall=JUICE_WALL)
        if scored:
            picks.append(scored)

    # ── Phase 1 derivatives ──
    # Team totals, per-period totals/BTS/DNB, total odd/even, overtime
    # yes/no, full-game BTS. All pure probability extraction from the
    # existing pred data — no factor stacking. Master gate +
    # per-market gates live in engine/config.py.
    from .derivative_picks import append_derivative_picks
    append_derivative_picks(picks, pred, odds, h_abbr, a_abbr)

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

    # NOTE: pred["win_prob"] used to get re-stomped here with the
    # calibrated ML prob so the "Projected Outcome" panel matched the
    # pick card. That was wrong on two layers:
    #   (1) the calibrators are trained on winning-side prob and
    #       inverted the bar when home was the dog (PHI 98% bug);
    #   (2) NHL blend_calibration heavily compresses favorites
    #       (raw 0.59 → 0.40), making the bar show the dog as
    #       favored — also inverted from the market.
    # Per-pick prob in the loop above is already calibrated, so the
    # pick badge shows truthful odds. Leave pred["win_prob"] as the
    # raw factor/ensemble output for the display bar.

    # Drop picks that flipped to negative edge after calibration.
    picks = [p for p in picks if (p.get("edge") or 0) > 0]

    # Adjusted EV: edge * reliability weight. Reliability is auto-tuned
    # from settled NHL tracker history when volume permits, falls back to
    # NHL_BET_RELIABILITY for cold-start bet types. See engine.dynamic_reliability.
    from ...dynamic_reliability import get_reliability as _get_reliability
    for p in picks:
        reliability = _get_reliability("nhl", p["type"])
        p["adjusted_ev"] = round(p["edge"] * reliability, 2)
    picks.sort(key=lambda p: -p["adjusted_ev"])

    from ..._pick_helpers import tag_confidence, filter_by_data_driven_floors
    picks = filter_by_data_driven_floors("nhl", picks)
    tag_confidence(picks)

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
