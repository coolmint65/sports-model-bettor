"""
NBA Q1 bet selection.

Separated from nba_q1_predict.py to keep game prediction and market
decision-making in distinct modules. This file converts Q1 model
probabilities into concrete betting picks by comparing against
real odds and applying edge/priority filtering.

Used by: NBA Best Bets, NBA Q1 Pick Tracker.
"""

import logging

from .config import NBA_BET_RELIABILITY

logger = logging.getLogger(__name__)


def _implied_prob(american_odds: int) -> float:
    """Convert American odds to implied probability."""
    if american_odds < 0:
        return abs(american_odds) / (abs(american_odds) + 100)
    return 100 / (american_odds + 100)


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


def generate_q1_picks(home_abbr: str, away_abbr: str,
                      odds: dict | None = None,
                      season: int | None = None,
                      pred: dict | None = None) -> list[dict]:
    """Generate Q1 spread, Q1 total, and Q1 ML picks with edges.

    Args:
        home_abbr: Home team abbreviation
        away_abbr: Away team abbreviation
        odds: Optional dict with Q1 odds
        season: Season year override
        pred: Optional pre-computed factor prediction (may carry mc / gbm
            subkeys). When provided we skip the inline predict_q1 call so
            ensemble_nba() blends factor + MC + GBM rather than re-running
            the factor model.

    Returns:
        List of pick dicts sorted by priority then edge.
    """
    from .nba_q1_predict import predict_q1

    odds = _sanitize_odds(odds)
    q1_spread = odds.get("q1_spread")
    q1_total = odds.get("q1_total")

    if pred is None:
        pred = predict_q1(home_abbr, away_abbr,
                          spread=q1_spread, total=q1_total, season=season)

    # Compute the ensemble blend. pred may already carry factor + MC +
    # GBM (when the backend helper ran them); if only factor is present
    # ensemble_nba collapses to factor-only.
    try:
        from .ensemble import ensemble_nba
        pred["ensemble"] = ensemble_nba(pred)
    except Exception as e:
        logger.debug("NBA ensemble blend failed: %s", e)
        pred["ensemble"] = {}

    # Route Q1 ML through the ensemble when present. Spread cover
    # and total over/under distributions stay on the factor model
    # (ensemble only blends scalar home_win + total_expected).
    ens = pred.get("ensemble") or {}
    if ens.get("q1_home_win") is not None:
        pred["q1_ml_home"] = float(ens["q1_home_win"])
        pred["q1_ml_away"] = 1.0 - float(ens["q1_home_win"])

    picks = []

    # ── Q1 Spread ──
    if q1_spread is not None:
        h_spread_odds = odds.get("q1_spread_home_odds", -110)
        a_spread_odds = odds.get("q1_spread_away_odds", -110)

        cover_prob = pred["spread_cover_prob"]
        if cover_prob is not None:
            implied = _implied_prob(h_spread_odds)
            edge = (cover_prob - implied) * 100
            if edge > 0:
                picks.append({
                    "type": "Q1_SPREAD",
                    "pick": f"{home_abbr} {q1_spread:+.1f} Q1",
                    "prob": round(cover_prob, 4),
                    "edge": round(edge, 1),
                    "odds": h_spread_odds,
                })

            away_cover_prob = 1 - cover_prob
            away_implied = _implied_prob(a_spread_odds)
            away_edge = (away_cover_prob - away_implied) * 100
            if away_edge > 0:
                picks.append({
                    "type": "Q1_SPREAD",
                    "pick": f"{away_abbr} {-q1_spread:+.1f} Q1",
                    "prob": round(away_cover_prob, 4),
                    "edge": round(away_edge, 1),
                    "odds": a_spread_odds,
                })

    # ── Q1 Total ──
    if q1_total is not None:
        over_odds = odds.get("q1_over_odds", -110)
        under_odds = odds.get("q1_under_odds", -110)

        over_prob = pred["over_prob"]
        if over_prob is not None:
            over_implied = _implied_prob(over_odds)
            over_edge = (over_prob - over_implied) * 100
            if over_edge > 0:
                picks.append({
                    "type": "Q1_TOTAL",
                    "pick": f"Over {q1_total} Q1",
                    "prob": round(over_prob, 4),
                    "edge": round(over_edge, 1),
                    "odds": over_odds,
                })

            under_prob = 1 - over_prob
            under_implied = _implied_prob(under_odds)
            under_edge = (under_prob - under_implied) * 100
            if under_edge > 0:
                picks.append({
                    "type": "Q1_TOTAL",
                    "pick": f"Under {q1_total} Q1",
                    "prob": round(under_prob, 4),
                    "edge": round(under_edge, 1),
                    "odds": under_odds,
                })

    # ── Q1 Moneyline ──
    # Use Q1-specific ML odds, not full-game ML. Full-game ML has
    # much wider spreads that create phantom edge when compared
    # against the Q1 model's tighter probabilities.
    home_ml_odds = odds.get("q1_home_ml") or odds.get("home_ml")
    away_ml_odds = odds.get("q1_away_ml") or odds.get("away_ml")

    if home_ml_odds is not None:
        home_ml_prob = pred["q1_ml_home"]
        implied = _implied_prob(home_ml_odds)
        edge = (home_ml_prob - implied) * 100
        if edge > 0:
            picks.append({
                "type": "Q1_ML",
                "pick": f"{home_abbr} Q1 ML",
                "prob": round(home_ml_prob, 4),
                "edge": round(edge, 1),
                "odds": home_ml_odds,
            })

    if away_ml_odds is not None:
        away_ml_prob = pred["q1_ml_away"]
        implied = _implied_prob(away_ml_odds)
        edge = (away_ml_prob - implied) * 100
        if edge > 0:
            picks.append({
                "type": "Q1_ML",
                "pick": f"{away_abbr} Q1 ML",
                "prob": round(away_ml_prob, 4),
                "edge": round(edge, 1),
                "odds": away_ml_odds,
            })

    # Empirical recalibration from the nba_picks tracker. See
    # engine/picks.py for the same pattern applied to MLB.
    from .empirical_calibration import calibrate as _calibrate
    for p in picks:
        prob = p.get("prob")
        odds = p.get("odds")
        if prob is None:
            continue
        cal = _calibrate(p["type"], float(prob), sport="nba")
        p["prob_raw"] = round(float(prob), 4)
        p["prob"] = round(float(cal), 4)
        if odds is not None and _valid_odds(odds):
            p["edge"] = round((cal - _implied_prob(int(odds))) * 100, 1)

    # Keep pred's Q1 win prob consistent with the calibrated Q1 ML
    # pick so Projected Outcome and the pick card agree. predict_q1
    # exposes q1_ml_home / q1_ml_away as top-level fields.
    q1_home = pred.get("q1_ml_home")
    if q1_home is not None:
        # Preserve the pre-calibration factor value so Model Signals'
        # Factor column reads the true factor output.
        pred.setdefault("factor_q1_ml_home", q1_home)
        cal_q1 = float(_calibrate("Q1 ML", float(q1_home), sport="nba"))
        pred["q1_ml_home"] = round(cal_q1, 4)
        pred["q1_ml_away"] = round(1.0 - cal_q1, 4)

    picks = [p for p in picks if (p.get("edge") or 0) > 0]

    # Adjusted EV: edge * reliability weight
    for p in picks:
        reliability = NBA_BET_RELIABILITY.get(p["type"], 0.5)
        p["adjusted_ev"] = round(p["edge"] * reliability, 2)
    picks.sort(key=lambda p: -p["adjusted_ev"])

    return picks


def generate_q1_picks_with_context(home_abbr: str, away_abbr: str,
                                   odds: dict | None = None,
                                   season: int | None = None
                                   ) -> tuple[list[dict], dict]:
    """Generate Q1 picks and return both picks and the full prediction context."""
    from .nba_q1_predict import predict_q1

    odds = _sanitize_odds(odds)
    q1_spread = odds.get("q1_spread")
    q1_total = odds.get("q1_total")

    pred = predict_q1(home_abbr, away_abbr,
                      spread=q1_spread, total=q1_total, season=season)
    picks = generate_q1_picks(home_abbr, away_abbr, odds, season)

    return picks, pred
