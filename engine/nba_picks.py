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


def generate_q1_picks(home_abbr: str, away_abbr: str,
                      odds: dict | None = None,
                      season: int | None = None) -> list[dict]:
    """Generate Q1 spread, Q1 total, and Q1 ML picks with edges.

    Args:
        home_abbr: Home team abbreviation
        away_abbr: Away team abbreviation
        odds: Optional dict with Q1 odds
        season: Season year override

    Returns:
        List of pick dicts sorted by priority then edge.
    """
    from .nba_q1_predict import predict_q1

    odds = odds or {}
    q1_spread = odds.get("q1_spread")
    q1_total = odds.get("q1_total")

    pred = predict_q1(home_abbr, away_abbr,
                      spread=q1_spread, total=q1_total, season=season)

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
    home_ml_odds = odds.get("home_ml")
    away_ml_odds = odds.get("away_ml")

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

    odds = odds or {}
    q1_spread = odds.get("q1_spread")
    q1_total = odds.get("q1_total")

    pred = predict_q1(home_abbr, away_abbr,
                      spread=q1_spread, total=q1_total, season=season)
    picks = generate_q1_picks(home_abbr, away_abbr, odds, season)

    return picks, pred
