"""
Confidence Calibration & Bet Sizing.

Tracks prediction accuracy over time by grouping predictions into
probability buckets and comparing predicted vs actual win rates.
Also provides Kelly Criterion bet sizing.

Usage:
    python -m engine.accuracy --mlb
    python -m engine.accuracy --nhl
    python -m engine.accuracy --mlb --nhl
"""

import logging
import math

logger = logging.getLogger(__name__)

# Probability bucket boundaries (lower bound inclusive)
_BUCKET_EDGES = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90]


def compute_calibration(sport: str = "mlb") -> dict:
    """Analyze how well calibrated our predictions are.

    Groups predictions into probability buckets (50-55%, 55-60%, etc.)
    and checks actual win rate in each bucket.

    Args:
        sport: "mlb" or "nhl"

    Returns:
        {
            "buckets": [
                {"range": "50-55%", "predicted": 0.525, "actual": 0.51, "count": 45},
                {"range": "55-60%", "predicted": 0.575, "actual": 0.58, "count": 38},
                ...
            ],
            "brier_score": 0.21,  # Lower is better
            "total_picks": 200,
            "overall_accuracy": 0.574,
            "overconfident": True/False,  # Do we predict higher than we achieve?
        }
    """
    picks = _load_settled_picks(sport)

    if not picks:
        return {
            "buckets": [],
            "brier_score": None,
            "total_picks": 0,
            "overall_accuracy": None,
            "overconfident": None,
        }

    # Build buckets
    buckets = []
    for i, lower in enumerate(_BUCKET_EDGES):
        upper = _BUCKET_EDGES[i + 1] if i + 1 < len(_BUCKET_EDGES) else 1.0
        label = f"{int(lower * 100)}-{int(upper * 100)}%"

        in_bucket = [p for p in picks if lower <= p["model_prob"] < upper]
        if not in_bucket:
            continue

        count = len(in_bucket)
        avg_predicted = sum(p["model_prob"] for p in in_bucket) / count
        wins = sum(1 for p in in_bucket if p["result"] == "W")
        actual_rate = wins / count

        buckets.append({
            "range": label,
            "predicted": round(avg_predicted, 4),
            "actual": round(actual_rate, 4),
            "count": count,
        })

    # Brier score: mean squared error of probabilistic predictions
    # outcome = 1 for win, 0 for loss
    brier_sum = 0.0
    total_wins = 0
    for p in picks:
        outcome = 1.0 if p["result"] == "W" else 0.0
        total_wins += int(outcome)
        brier_sum += (p["model_prob"] - outcome) ** 2

    total_picks = len(picks)
    brier_score = brier_sum / total_picks if total_picks > 0 else None
    overall_accuracy = total_wins / total_picks if total_picks > 0 else None

    # Overconfidence check: is our average predicted prob higher than
    # our actual win rate?
    avg_predicted_all = sum(p["model_prob"] for p in picks) / total_picks
    overconfident = avg_predicted_all > (overall_accuracy or 0)

    return {
        "buckets": buckets,
        "brier_score": round(brier_score, 4) if brier_score is not None else None,
        "total_picks": total_picks,
        "overall_accuracy": round(overall_accuracy, 4) if overall_accuracy is not None else None,
        "overconfident": overconfident,
    }


def compute_kelly_fraction(edge: float, odds: int) -> float:
    """Compute Kelly Criterion bet sizing.

    Args:
        edge: decimal edge (e.g. 0.05 for 5%)
        odds: American odds (e.g. -110, +150)

    Returns:
        Fraction of bankroll to bet (0.0 to 0.25 max).
        Uses quarter-Kelly for safety.
    """
    if edge <= 0:
        return 0.0

    # Convert American odds to decimal payout (profit per $1 wagered)
    if odds > 0:
        decimal_payout = odds / 100.0
    elif odds < 0:
        decimal_payout = 100.0 / abs(odds)
    else:
        return 0.0

    # Implied probability from the model (our estimated true prob)
    # edge = model_prob - implied_prob, so model_prob = implied_prob + edge
    # implied_prob from odds: for -110 => 110/210 = 0.5238
    if odds > 0:
        implied_prob = 100.0 / (odds + 100.0)
    else:
        implied_prob = abs(odds) / (abs(odds) + 100.0)

    win_prob = implied_prob + edge

    # Kelly formula: f* = (bp - q) / b
    # where b = decimal payout, p = win probability, q = 1 - p
    b = decimal_payout
    p = win_prob
    q = 1.0 - p

    if b <= 0:
        return 0.0

    kelly = (b * p - q) / b

    # Quarter-Kelly for safety
    quarter_kelly = kelly / 4.0

    # Clamp to [0.0, 0.25]
    return max(0.0, min(0.25, quarter_kelly))


def _load_settled_picks(sport: str) -> list[dict]:
    """Load settled picks with model_prob and result from the appropriate DB."""
    picks = []

    if sport == "mlb":
        try:
            from .db import get_conn
            conn = get_conn()
            rows = conn.execute("""
                SELECT model_prob, result
                FROM picks
                WHERE result IN ('W', 'L')
                  AND model_prob IS NOT NULL
                  AND model_prob >= 0.50
                ORDER BY date
            """).fetchall()
            picks = [{"model_prob": r["model_prob"], "result": r["result"]} for r in rows]
        except Exception as e:
            logger.warning("Failed to load MLB picks: %s", e)

    elif sport == "nhl":
        try:
            from .nhl_tracker import _get_nhl_db
            conn = _get_nhl_db()
            rows = conn.execute("""
                SELECT model_prob, result
                FROM nhl_picks
                WHERE result IN ('W', 'L')
                  AND model_prob IS NOT NULL
                  AND model_prob >= 0.50
                ORDER BY date
            """).fetchall()
            picks = [{"model_prob": r["model_prob"], "result": r["result"]} for r in rows]
        except Exception as e:
            logger.warning("Failed to load NHL picks: %s", e)

    else:
        logger.error("Unknown sport: %s", sport)

    return picks


def _format_calibration(sport: str, cal: dict) -> str:
    """Format calibration results for terminal output."""
    lines = [f"\n{'=' * 60}", f"  {sport.upper()} Calibration Report", f"{'=' * 60}"]

    if cal["total_picks"] == 0:
        lines.append("  No settled picks found.")
        return "\n".join(lines)

    lines.append(f"  Total picks:      {cal['total_picks']}")
    lines.append(f"  Overall accuracy: {cal['overall_accuracy']:.1%}")
    lines.append(f"  Brier score:      {cal['brier_score']:.4f}  (lower is better)")
    lines.append(f"  Overconfident:    {'YES' if cal['overconfident'] else 'No'}")

    if cal["buckets"]:
        lines.append(f"\n  {'Bucket':<12} {'Predicted':>10} {'Actual':>10} {'Count':>8} {'Delta':>8}")
        lines.append(f"  {'-' * 48}")
        for b in cal["buckets"]:
            delta = b["actual"] - b["predicted"]
            sign = "+" if delta >= 0 else ""
            lines.append(
                f"  {b['range']:<12} {b['predicted']:>9.1%} {b['actual']:>9.1%} "
                f"{b['count']:>8} {sign}{delta:>6.1%}"
            )

    lines.append(f"{'=' * 60}\n")
    return "\n".join(lines)


if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Prediction calibration analysis")
    parser.add_argument("--mlb", action="store_true", help="Analyze MLB picks")
    parser.add_argument("--nhl", action="store_true", help="Analyze NHL picks")
    args = parser.parse_args()

    if not args.mlb and not args.nhl:
        parser.print_help()
        sys.exit(1)

    if args.mlb:
        cal = compute_calibration("mlb")
        print(_format_calibration("mlb", cal))

    if args.nhl:
        cal = compute_calibration("nhl")
        print(_format_calibration("nhl", cal))
