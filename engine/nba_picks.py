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
    from .config import NBA_JUICE_WALL as JUICE_WALL

    # ── Q1 Spread ──
    if q1_spread is not None:
        h_spread_odds = odds.get("q1_spread_home_odds", -110)
        a_spread_odds = odds.get("q1_spread_away_odds", -110)

        cover_prob = pred["spread_cover_prob"]
        if cover_prob is not None:
            implied = _implied_prob(h_spread_odds)
            edge = (cover_prob - implied) * 100
            if edge > 0 and h_spread_odds >= JUICE_WALL:
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
            if away_edge > 0 and a_spread_odds >= JUICE_WALL:
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
            if over_edge > 0 and over_odds >= JUICE_WALL:
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
            if under_edge > 0 and under_odds >= JUICE_WALL:
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

    if home_ml_odds is not None and home_ml_odds >= JUICE_WALL:
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

    if away_ml_odds is not None and away_ml_odds >= JUICE_WALL:
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

    # ── Q1 Alt Line Shopping ──
    # Check Q1 alt spreads and totals for better edges than primary.
    # Prefer the exact discretized-Gaussian distributions exposed by
    # nba_q1_predict (margin_probs / total_probs) over ad-hoc CDF /
    # logistic approximations — the latter drifted ~5-8% at alt lines
    # more than 1 std away from the model spread.
    predicted_total = pred.get("predicted_total", 0)
    predicted_margin = pred.get("predicted_margin", 0)
    margin_probs = pred.get("margin_probs") or {}
    total_probs_dist = pred.get("total_probs") or {}
    q1_std = 8.67  # Q1 scoring margin std dev (calibrated from 2,714 games)

    q1_alt_spreads = odds.get("q1_alt_spreads", [])
    q1_alt_totals = odds.get("q1_alt_totals", [])
    juice_wall = JUICE_WALL

    # Best existing Q1 spread/total edge for comparison.
    # Use EDGE_LEAN (4%) as floor so alts only surface when they
    # beat the playability threshold, not just beat zero.
    from .config import EDGE_LEAN as _EDGE_LEAN
    best_spread_edge = max(
        (p["edge"] for p in picks if "SPREAD" in p.get("type", "")),
        default=_EDGE_LEAN)
    best_total_edge = max(
        (p["edge"] for p in picks if "TOTAL" in p.get("type", "")),
        default=_EDGE_LEAN)

    # Collect alt candidates, then keep only the single best per market
    alt_spread_candidates = []
    alt_total_candidates = []

    import math
    for alt in q1_alt_spreads:
        point = alt.get("point")
        home_odds = alt.get("home_odds")
        away_odds = alt.get("away_odds")
        if point is None:
            continue
        # P(home covers spread X) = P(home_margin > -X). Uses the exact
        # discretized distribution from nba_q1_predict when available so
        # alt edges at distant lines aren't subject to CDF approximation
        # error. Falls back to the normal CDF when the dict isn't present
        # (e.g. upstream pred was produced by an older code path).
        if margin_probs:
            threshold = -point
            home_cover = sum(p for m, p in margin_probs.items() if m > threshold)
        else:
            z = (predicted_margin - (-point)) / q1_std
            home_cover = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        home_cover = max(0.05, min(0.95, home_cover))

        if home_odds is not None and abs(home_odds) >= 100 and home_odds >= juice_wall:
            edge = (home_cover - _implied_prob(home_odds)) * 100
            if edge > 0 and edge > best_spread_edge + 3.0:
                sign = "+" if point > 0 else ""
                alt_spread_candidates.append({
                    "type": "Q1_SPREAD",
                    "pick": f"{home_abbr} {sign}{point} Q1",
                    "prob": round(home_cover, 4),
                    "edge": round(edge, 1),
                    "odds": home_odds,
                    "is_alt": True,
                })
        if away_odds is not None and abs(away_odds) >= 100 and away_odds >= juice_wall:
            away_cover = 1.0 - home_cover
            edge = (away_cover - _implied_prob(away_odds)) * 100
            if edge > 0 and edge > best_spread_edge + 3.0:
                sign = "+" if -point > 0 else ""
                alt_spread_candidates.append({
                    "type": "Q1_SPREAD",
                    "pick": f"{away_abbr} {sign}{-point} Q1",
                    "prob": round(away_cover, 4),
                    "edge": round(edge, 1),
                    "odds": away_odds,
                    "is_alt": True,
                })

    for alt in q1_alt_totals:
        line = alt.get("line")
        over_odds = alt.get("over_odds")
        under_odds = alt.get("under_odds")
        if line is None or not predicted_total:
            continue
        # P(total > line) from the exact discretized Gaussian distribution.
        # The previous fallback used a logistic with a 0.6 scale fudge which
        # systematically under-estimated tail mass; switching to the proper
        # CDF fixes alt-total edges at the extremes (O 50.5 / U 68.5 etc).
        total_std = 8.5  # Q1 total std dev (calibrated from 2,714 games)
        if total_probs_dist:
            over_prob = sum(p for t, p in total_probs_dist.items() if t > line)
        else:
            z = (predicted_total - line) / total_std
            over_prob = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        under_prob = 1.0 - over_prob

        if over_odds is not None and abs(over_odds) >= 100 and over_odds >= juice_wall and over_prob > 0.5:
            edge = (over_prob - _implied_prob(over_odds)) * 100
            if edge > 0 and edge > best_total_edge + 3.0:
                alt_total_candidates.append({
                    "type": "Q1_TOTAL", "pick": f"Over {line} Q1",
                    "prob": round(over_prob, 4), "edge": round(edge, 1),
                    "odds": over_odds, "is_alt": True,
                })
        if under_odds is not None and abs(under_odds) >= 100 and under_odds >= juice_wall and under_prob > 0.5:
            edge = (under_prob - _implied_prob(under_odds)) * 100
            if edge > 0 and edge > best_total_edge + 3.0:
                alt_total_candidates.append({
                    "type": "Q1_TOTAL", "pick": f"Under {line} Q1",
                    "prob": round(under_prob, 4), "edge": round(edge, 1),
                    "odds": under_odds, "is_alt": True,
                })

    # Keep only the single best alt per market type
    if alt_spread_candidates:
        alt_spread_candidates.sort(key=lambda p: -p["edge"])
        picks.append(alt_spread_candidates[0])
    if alt_total_candidates:
        alt_total_candidates.sort(key=lambda p: -p["edge"])
        picks.append(alt_total_candidates[0])

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

    # Confidence tiers (same thresholds as MLB)
    from .config import EDGE_STRONG, EDGE_MODERATE, EDGE_LEAN, EDGE_SKIP
    for p in picks:
        e = p.get("edge", 0)
        if e >= EDGE_STRONG:
            p["confidence"] = "strong"
        elif e >= EDGE_MODERATE:
            p["confidence"] = "moderate"
        elif e >= EDGE_LEAN:
            p["confidence"] = "lean"
        else:
            p["confidence"] = "skip"

    # Filter out skips
    picks = [p for p in picks if p["confidence"] != "skip"]

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
