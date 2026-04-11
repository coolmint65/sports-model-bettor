"""
NHL bet selection.

Separated from nhl_predict.py to keep game prediction and market
decision-making in distinct modules. This file converts model
probabilities into concrete betting picks by comparing against
real odds and applying edge/priority filtering.

Used by: Best Bets, NHL Pick Tracker, Game Detail sidebar.
"""

import logging

from .config import NHL_JUICE_WALL as JUICE_WALL, NHL_BET_RELIABILITY

logger = logging.getLogger(__name__)


def _implied(ml: int) -> float:
    """American odds to implied probability."""
    if ml < 0:
        return abs(ml) / (abs(ml) + 100)
    return 100 / (ml + 100)


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
                                    odds: dict | None = None
                                    ) -> tuple[list[dict], dict]:
    """
    Generate NHL picks and return both the picks list and a context dict
    with rest, injuries, and other metadata so callers can surface b2b
    warnings and injury impact without re-running the full prediction.
    """
    from .nhl_predict import predict_matchup

    pred = predict_matchup(home_key, away_key)
    if not pred:
        return [], {}

    odds = odds or {}
    wp = pred["win_prob"]
    pl = pred["puck_line"]

    h_abbr = pred["home"]["abbreviation"]
    a_abbr = pred["away"]["abbreviation"]

    picks = []

    # ── Moneyline ──
    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")

    # ML picks are generated but given lower priority (priority 3).
    # Backtesting shows ML is -6% ROI — O/U and PL are preferred.
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
    if vegas_total and pred.get("over_under"):
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

    # Adjusted EV: edge * reliability weight
    for p in picks:
        reliability = NHL_BET_RELIABILITY.get(p["type"], 0.5)
        p["adjusted_ev"] = round(p["edge"] * reliability, 2)
    picks.sort(key=lambda p: -p["adjusted_ev"])

    # Assign confidence
    for p in picks:
        if p["edge"] >= 8:
            p["confidence"] = "strong"
        elif p["edge"] >= 4:
            p["confidence"] = "moderate"
        else:
            p["confidence"] = "lean"

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
