"""
Bankroll management and bet sizing.

Uses the Kelly Criterion to calculate optimal bet sizes based on
the edge between model probability and implied probability from odds.
Includes fractional Kelly for risk management.
"""


def ml_to_implied_prob(ml: int) -> float:
    """Convert American moneyline to implied probability."""
    if ml < 0:
        return abs(ml) / (abs(ml) + 100)
    return 100 / (ml + 100)


def ml_to_decimal(ml: int) -> float:
    """Convert American moneyline to decimal odds."""
    if ml < 0:
        return 1 + (100 / abs(ml))
    return 1 + (ml / 100)


def find_edge(model_prob: float, ml: int) -> float:
    """
    Calculate edge in percentage points.
    Positive = value bet (model says this is more likely than odds imply).
    """
    implied = ml_to_implied_prob(ml)
    return (model_prob - implied) * 100


def kelly_fraction(model_prob: float, ml: int) -> float:
    """
    Full Kelly Criterion bet sizing.

    Returns fraction of bankroll to wager (0 to 1).
    Negative result means don't bet.

    Formula: f* = (bp - q) / b
      where b = decimal odds - 1, p = probability of winning, q = 1-p
    """
    decimal_odds = ml_to_decimal(ml)
    b = decimal_odds - 1
    p = model_prob
    q = 1 - p

    if b <= 0:
        return 0

    f = (b * p - q) / b
    return max(0, f)


def fractional_kelly(model_prob: float, ml: int, fraction: float = 0.25) -> float:
    """
    Fractional Kelly — more conservative bet sizing.

    Default 0.25 = quarter Kelly, widely recommended to reduce variance.
    Returns fraction of bankroll to wager.
    """
    return kelly_fraction(model_prob, ml) * fraction


def analyze_bet(model_prob: float, ml: int, bankroll: float = 1000,
                kelly_frac: float = 0.25) -> dict:
    """
    Full bet analysis for a single wager.

    Returns edge, Kelly fraction, recommended bet size, expected value,
    and a confidence rating.
    """
    edge = find_edge(model_prob, ml)
    implied = ml_to_implied_prob(ml)
    full_kelly = kelly_fraction(model_prob, ml)
    frac_kelly = full_kelly * kelly_frac
    bet_size = round(bankroll * frac_kelly, 2)
    decimal_odds = ml_to_decimal(ml)

    # Expected value per dollar wagered
    ev = model_prob * (decimal_odds - 1) - (1 - model_prob)

    # Confidence rating
    if edge > 8:
        rating = "strong"
    elif edge > 4:
        rating = "moderate"
    elif edge > 1.5:
        rating = "lean"
    else:
        rating = "no_bet"

    return {
        "edge_pct": round(edge, 2),
        "implied_prob": round(implied, 4),
        "model_prob": round(model_prob, 4),
        "full_kelly_pct": round(full_kelly * 100, 2),
        "frac_kelly_pct": round(frac_kelly * 100, 2),
        "bet_size": bet_size,
        "ev_per_dollar": round(ev, 4),
        "decimal_odds": round(decimal_odds, 3),
        "rating": rating,
    }


def analyze_game_bets(prediction: dict, odds: dict | None = None,
                       bankroll: float = 1000) -> dict:
    """
    Analyze all bet types for a game: ML, run line, totals.

    Args:
        prediction: Output from predict_matchup()
        odds: {home_ml, away_ml, total, over_odds, under_odds, spread, ...}
        bankroll: Current bankroll

    Returns dict of bet recommendations.
    """
    if not odds:
        return {"bets": [], "message": "No odds available"}

    bets = []
    wp = prediction.get("win_prob", {})
    total = prediction.get("total", 0)
    ou = prediction.get("over_under", {})
    rl = prediction.get("run_line", {})

    # Moneyline bets
    if odds.get("home_ml") and wp.get("home"):
        analysis = analyze_bet(wp["home"], odds["home_ml"], bankroll)
        if analysis["rating"] != "no_bet":
            bets.append({
                "type": "moneyline",
                "side": prediction["home"]["abbreviation"],
                "line": odds["home_ml"],
                **analysis,
            })

    if odds.get("away_ml") and wp.get("away"):
        analysis = analyze_bet(wp["away"], odds["away_ml"], bankroll)
        if analysis["rating"] != "no_bet":
            bets.append({
                "type": "moneyline",
                "side": prediction["away"]["abbreviation"],
                "line": odds["away_ml"],
                **analysis,
            })

    # Over/Under — find the line closest to the posted total
    posted_total = odds.get("total")
    if posted_total and ou:
        total_str = str(float(posted_total))
        if total_str in ou:
            p_over = ou[total_str]["over"]
            p_under = ou[total_str]["under"]

            over_odds = odds.get("over_odds", -110)
            under_odds = odds.get("under_odds", -110)

            over_analysis = analyze_bet(p_over, over_odds, bankroll)
            if over_analysis["rating"] != "no_bet":
                bets.append({
                    "type": "total",
                    "side": f"Over {posted_total}",
                    "line": over_odds,
                    **over_analysis,
                })

            under_analysis = analyze_bet(p_under, under_odds, bankroll)
            if under_analysis["rating"] != "no_bet":
                bets.append({
                    "type": "total",
                    "side": f"Under {posted_total}",
                    "line": under_odds,
                    **under_analysis,
                })

    # Run line
    if rl and odds.get("home_spread_odds"):
        p_home_cover = rl.get("home_minus_1_5", 0)
        rl_analysis = analyze_bet(p_home_cover, odds["home_spread_odds"], bankroll)
        if rl_analysis["rating"] != "no_bet":
            bets.append({
                "type": "run_line",
                "side": f"{prediction['home']['abbreviation']} -1.5",
                "line": odds["home_spread_odds"],
                **rl_analysis,
            })

    if rl and odds.get("away_spread_odds"):
        p_away_cover = rl.get("away_plus_1_5", 0)
        rl_analysis = analyze_bet(p_away_cover, odds["away_spread_odds"], bankroll)
        if rl_analysis["rating"] != "no_bet":
            bets.append({
                "type": "run_line",
                "side": f"{prediction['away']['abbreviation']} +1.5",
                "line": odds["away_spread_odds"],
                **rl_analysis,
            })

    # Sort by edge
    bets.sort(key=lambda b: b["edge_pct"], reverse=True)

    return {
        "bets": bets,
        "best_bet": bets[0] if bets else None,
        "total_action": sum(b["bet_size"] for b in bets),
    }
