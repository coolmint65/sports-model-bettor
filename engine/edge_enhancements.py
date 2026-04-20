"""Edge enhancement signals for the pick generation pipeline.

Four independent signals that modify pick confidence and edge:

1. **Alt Line Shopping** — Iterate alt spreads/totals to find the line
   with the best edge, not just the primary.

2. **Dynamic Ensemble Weighting** — Shift ensemble blend weights toward
   components that have been accurate recently (rolling window).

3. **CLV Confidence** — Historical closing line value by market type.
   Positive avg CLV = real edge (not variance). Boosts reliability.

4. **Line Movement Signal** — Sharp money detection. Lines moving toward
   your pick = market confirmation. Moving against = warning.

All signals are composable and toggle-able. They modify the pick dict
after initial generation (edge, adjusted_ev, confidence annotations).
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ── Toggles ───────────────────────────────────────────────────
ENABLE_ALT_LINE_SHOPPING = True
ENABLE_DYNAMIC_ENSEMBLE = True
ENABLE_CLV_CONFIDENCE = True
ENABLE_LINE_MOVEMENT_SIGNAL = True

# ── Alt Line Shopping ─────────────────────────────────────────
# Minimum edge improvement over primary to recommend an alt line.
ALT_LINE_MIN_EDGE_IMPROVEMENT = 3.0  # percentage points above primary RL edge
ALT_LINE_JUICE_WALL = -200  # max vig on alt picks


def _implied(ml: int) -> float:
    """American odds to implied probability."""
    if ml < 0:
        return abs(ml) / (abs(ml) + 100)
    return 100 / (ml + 100)


def shop_alt_spreads(pred: dict, odds: dict, h_abbr: str, a_abbr: str,
                     existing_picks: list[dict]) -> list[dict]:
    """Find alt spread lines with better edge than the primary.

    Checks the model's spread probability distribution against each
    alt spread's odds to find the best edge. Only recommends an alt
    if it beats the primary RL pick's edge by ALT_LINE_MIN_EDGE_IMPROVEMENT.

    Returns new pick dicts to append (type="ALT RL").
    """
    if not ENABLE_ALT_LINE_SHOPPING:
        return []

    alt_spreads = odds.get("alt_spreads", [])
    if not alt_spreads:
        return []

    spreads_dist = (pred.get("run_line") or {}).get("spreads", {})
    if not spreads_dist:
        return []

    # Find existing RL pick's edge (if any) as baseline
    existing_rl_edge = 0.0
    for p in existing_picks:
        if p.get("type") == "RL" and (p.get("edge") or 0) > existing_rl_edge:
            existing_rl_edge = p["edge"]

    new_picks = []

    # Use the standard run line probabilities as reference.
    # home_minus_1_5 = P(home wins by 2+) = home -1.5 cover
    # home_plus_1_5  = P(home loses by 0 or 1, or wins) = home +1.5 cover
    rl_home_minus = pred.get("run_line", {}).get("home_minus_1_5")
    rl_home_plus = pred.get("run_line", {}).get("home_plus_1_5")
    model_spread = pred.get("run_line", {}).get("model_spread", 0)

    if rl_home_minus is None or rl_home_plus is None:
        return []

    for alt in alt_spreads:
        point = alt.get("point")
        home_odds = alt.get("home_odds")
        away_odds = alt.get("away_odds")
        if point is None:
            continue

        # Estimate cover probability using normal CDF centered on
        # the model's projected spread. Standard deviations calibrated
        # from historical margin distributions:
        #   MLB: ~3.8 runs (regular season avg margin std dev)
        #   NHL: ~2.2 goals
        #   NBA: ~11.5 points
        import math
        _MARGIN_STD = {"mlb": 3.8, "nhl": 2.2, "nba": 11.5}
        # Detect sport from prediction context
        sport_key = "mlb"  # default; could be enhanced to detect
        if pred.get("puck_line") or pred.get("regulation_draw_prob"):
            sport_key = "nhl"
        elif pred.get("q1_ml_home") or pred.get("predicted_margin"):
            sport_key = "nba"
        std = _MARGIN_STD.get(sport_key, 3.8)

        z = (model_spread - (-point)) / std
        home_cover_prob = 0.5 * (1 + math.erf(z / math.sqrt(2)))

        # Sanity clamp
        home_cover_prob = max(0.05, min(0.95, home_cover_prob))
        away_cover_prob = 1.0 - home_cover_prob

        # Home side: home team at this spread
        if home_odds is not None and abs(home_odds) >= 100 and home_odds >= ALT_LINE_JUICE_WALL:
            # point < 0 = home favorite (needs to win by more)
            # point > 0 = home underdog (can lose by less)
            prob = home_cover_prob
            edge = (prob - _implied(home_odds)) * 100
            if edge > 0 and edge > existing_rl_edge + ALT_LINE_MIN_EDGE_IMPROVEMENT:
                sign = "+" if point > 0 else ""
                new_picks.append({
                    "type": "ALT RL",
                    "pick": f"{h_abbr} {sign}{point}",
                    "prob": round(prob, 4),
                    "edge": round(edge, 1),
                    "odds": home_odds,
                    "is_alt": True,
                    "alt_vs_primary_improvement": round(edge - existing_rl_edge, 1),
                })

        # Away side: away team at opposite spread
        if away_odds is not None and abs(away_odds) >= 100 and away_odds >= ALT_LINE_JUICE_WALL:
            prob = away_cover_prob
            edge = (prob - _implied(away_odds)) * 100
            if edge > 0 and edge > existing_rl_edge + ALT_LINE_MIN_EDGE_IMPROVEMENT:
                sign = "+" if -point > 0 else ""
                new_picks.append({
                    "type": "ALT RL",
                    "pick": f"{a_abbr} {sign}{-point}",
                    "prob": round(prob, 4),
                    "edge": round(edge, 1),
                    "odds": away_odds,
                    "is_alt": True,
                    "alt_vs_primary_improvement": round(edge - existing_rl_edge, 1),
                })

    # Return only the single best alt (highest edge)
    if new_picks:
        new_picks.sort(key=lambda p: -p["edge"])
        return [new_picks[0]]
    return []


def shop_alt_totals(pred: dict, odds: dict,
                    existing_picks: list[dict]) -> list[dict]:
    """Find alt total lines with better edge than the primary O/U.

    Uses the model's over/under probability distribution to check
    each alt total line against its odds.

    Returns new pick dicts to append (type="ALT O/U").
    """
    if not ENABLE_ALT_LINE_SHOPPING:
        return []

    alt_totals = odds.get("alt_totals", [])
    if not alt_totals:
        return []

    ou_dist = pred.get("over_under")
    if not ou_dist:
        return []

    # Find existing O/U pick's edge as baseline
    existing_ou_edge = 0.0
    for p in existing_picks:
        if p.get("type") == "O/U" and (p.get("edge") or 0) > existing_ou_edge:
            existing_ou_edge = p["edge"]

    model_total = pred.get("total", 0)
    if not model_total:
        return []

    new_picks = []

    for alt in alt_totals:
        line = alt.get("line")
        over_odds = alt.get("over_odds")
        under_odds = alt.get("under_odds")
        if line is None:
            continue

        # Probability estimate using logistic function with
        # sport-appropriate total standard deviations:
        #   MLB: ~3.2 runs, NHL: ~1.8 goals, NBA: ~10.5 pts
        diff = model_total - line
        import math
        _TOTAL_STD = {"mlb": 3.2, "nhl": 1.8, "nba": 10.5}
        sport_key = "mlb"
        if model_total > 100:
            sport_key = "nba"
        elif model_total < 12:
            sport_key = "nhl"
        std = _TOTAL_STD.get(sport_key, 3.2)
        over_prob = 1.0 / (1.0 + math.exp(-diff / std))
        under_prob = 1.0 - over_prob

        # Over side
        if over_odds is not None and abs(over_odds) >= 100 and over_odds >= ALT_LINE_JUICE_WALL and over_prob > 0.5:
            edge = (over_prob - _implied(over_odds)) * 100
            if edge > 0 and edge > existing_ou_edge + ALT_LINE_MIN_EDGE_IMPROVEMENT:
                new_picks.append({
                    "type": "ALT O/U",
                    "pick": f"Over {line}",
                    "prob": round(over_prob, 4),
                    "edge": round(edge, 1),
                    "odds": over_odds,
                    "is_alt": True,
                    "alt_vs_primary_improvement": round(edge - existing_ou_edge, 1),
                })

        # Under side
        if under_odds is not None and abs(under_odds) >= 100 and under_odds >= ALT_LINE_JUICE_WALL and under_prob > 0.5:
            edge = (under_prob - _implied(under_odds)) * 100
            if edge > 0 and edge > existing_ou_edge + ALT_LINE_MIN_EDGE_IMPROVEMENT:
                new_picks.append({
                    "type": "ALT O/U",
                    "pick": f"Under {line}",
                    "prob": round(under_prob, 4),
                    "edge": round(edge, 1),
                    "odds": under_odds,
                    "is_alt": True,
                    "alt_vs_primary_improvement": round(edge - existing_ou_edge, 1),
                })

    if new_picks:
        new_picks.sort(key=lambda p: -p["edge"])
        return [new_picks[0]]
    return []


# ── Dynamic Ensemble Weighting ────────────────────────────────

def get_dynamic_weights(sport: str, market: str,
                        window_days: int = 14) -> dict[str, float] | None:
    """Compute rolling accuracy per ensemble component for a market.

    Queries prediction_signals to find which component (factor, mc, gbm)
    has been most accurate recently, and returns adjusted weights.

    Returns None if not enough data (falls back to static weights).
    """
    if not ENABLE_DYNAMIC_ENSEMBLE:
        return None

    db_path = _DATA_DIR / "mlb.db"
    if not db_path.exists():
        return None

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        # prediction_signals stores component values per prediction
        # Check if the table exists
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "prediction_signals" not in tables:
            conn.close()
            return None

        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=window_days)).strftime("%Y-%m-%d")

        # Get signals joined with outcomes
        rows = conn.execute("""
            SELECT ps.factor_value, ps.mc_value, ps.gbm_value,
                   p.result, p.model_prob
            FROM prediction_signals ps
            JOIN picks p ON ps.game_id = p.game_id
                        AND ps.market = ?
            WHERE p.date >= ? AND p.result IS NOT NULL
              AND ps.factor_value IS NOT NULL
        """, (market, cutoff)).fetchall()
        conn.close()

        if len(rows) < 20:  # need at least 20 samples
            return None

        # Compute accuracy (Brier score) for each component
        def brier(predicted: float, actual: bool) -> float:
            return (predicted - (1.0 if actual else 0.0)) ** 2

        scores = {"factor": [], "mc": [], "gbm": []}
        for row in rows:
            won = row["result"] == "W"
            if row["factor_value"] is not None:
                scores["factor"].append(brier(row["factor_value"], won))
            if row["mc_value"] is not None:
                scores["mc"].append(brier(row["mc_value"], won))
            if row["gbm_value"] is not None:
                scores["gbm"].append(brier(row["gbm_value"], won))

        # Convert Brier scores to weights (lower Brier = better = higher weight)
        avg_scores = {}
        for comp, vals in scores.items():
            if len(vals) >= 10:
                avg_scores[comp] = sum(vals) / len(vals)

        if len(avg_scores) < 2:
            return None

        # Invert: weight = 1/brier (normalized)
        inv = {k: 1.0 / max(v, 0.01) for k, v in avg_scores.items()}
        total = sum(inv.values())
        weights = {k: round(v / total, 3) for k, v in inv.items()}

        logger.info("Dynamic ensemble weights for %s/%s (last %dd, %d samples): %s",
                     sport, market, window_days, len(rows), weights)
        return weights

    except Exception as e:
        logger.debug("Dynamic ensemble weights error: %s", e)
        return None


# ── CLV Confidence Signal ─────────────────────────────────────

def get_clv_reliability(sport: str, market_type: str,
                        min_samples: int = 15) -> float | None:
    """Get average CLV for a market type from historical picks.

    Positive CLV = model consistently beats closing line = real edge.
    Returns a reliability multiplier:
      - avg_clv > 2.0 → 1.15 (strong CLV history, boost confidence)
      - avg_clv > 0.5 → 1.05 (mild positive CLV)
      - avg_clv < -1.0 → 0.90 (negative CLV, fade confidence)
      - else → 1.0 (neutral)

    Returns None if not enough data.
    """
    if not ENABLE_CLV_CONFIDENCE:
        return None

    db_map = {"mlb": "mlb.db", "nhl": "nhl.db", "nba": "nba.db"}
    db_path = _DATA_DIR / db_map.get(sport, "mlb.db")
    if not db_path.exists():
        return None

    try:
        conn = sqlite3.connect(str(db_path))
        table = f"{sport}_picks" if sport != "mlb" else "picks"

        # Check if table exists
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if table not in tables:
            table = "picks"  # fallback
            if table not in tables:
                conn.close()
                return None

        rows = conn.execute(f"""
            SELECT odds, closing_odds FROM {table}
            WHERE result IS NOT NULL
              AND odds IS NOT NULL AND closing_odds IS NOT NULL
              AND bet_type = ?
        """, (market_type,)).fetchall()
        conn.close()

        if len(rows) < min_samples:
            return None

        clv_values = []
        for odds_val, closing in rows:
            if not odds_val or not closing:
                continue
            bet_imp = abs(odds_val) / (abs(odds_val) + 100) if odds_val < 0 else 100 / (odds_val + 100)
            close_imp = abs(closing) / (abs(closing) + 100) if closing < 0 else 100 / (closing + 100)
            clv_values.append((close_imp - bet_imp) * 100)

        if len(clv_values) < min_samples:
            return None

        avg_clv = sum(clv_values) / len(clv_values)

        if avg_clv > 2.0:
            mult = 1.15
        elif avg_clv > 0.5:
            mult = 1.05
        elif avg_clv < -1.0:
            mult = 0.90
        else:
            mult = 1.0

        logger.info("CLV for %s/%s: avg=%.2f%% (%d samples) → reliability x%.2f",
                     sport, market_type, avg_clv, len(clv_values), mult)
        return mult

    except Exception as e:
        logger.debug("CLV reliability error: %s", e)
        return None


# ── Line Movement Signal ──────────────────────────────────────

def annotate_line_movement(picks: list[dict], sport: str,
                           matchup_key: str, odds: dict) -> None:
    """Annotate picks with line movement data and confidence modifier.

    Adds to each pick:
      - line_move: dict with movement data
      - line_move_signal: "confirming" | "contrary" | "neutral"
      - line_move_modifier: multiplier on adjusted_ev

    Sharp money moving WITH your pick = confirming (boost confidence).
    Sharp money moving AGAINST = contrary (reduce confidence).
    """
    if not ENABLE_LINE_MOVEMENT_SIGNAL:
        return

    try:
        from .line_movement import get_line_movement, track_opening_odds

        # Track opening odds if first time seeing this game
        track_opening_odds(sport, matchup_key, odds)

        movement = get_line_movement(sport, matchup_key, odds)
        if not movement:
            return

        direction = movement.get("direction", "neutral")
        significance = movement.get("significance", "none")

        if significance == "none":
            return

        for pick in picks:
            pick_type = pick.get("type", "")
            pick_label = pick.get("pick", "")

            signal = "neutral"
            modifier = 1.0

            if pick_type in ("ML", "RL", "ALT RL"):
                # Determine if pick is on home or away side
                # Convention: pick label starts with team abbr
                h_abbr = pick_label.split()[0] if pick_label else ""

                # Check if line moved toward or against our pick
                if direction == "home":
                    # Money on home — confirming if we picked home
                    if pick.get("pick", "").startswith(h_abbr):
                        signal = "confirming"
                    else:
                        signal = "contrary"
                elif direction == "away":
                    if not pick.get("pick", "").startswith(h_abbr):
                        signal = "confirming"
                    else:
                        signal = "contrary"

            elif pick_type in ("O/U", "ALT O/U"):
                total_move = movement.get("total_move", 0)
                if total_move:
                    pick_is_over = "Over" in pick_label
                    if (total_move > 0 and pick_is_over) or \
                       (total_move < 0 and not pick_is_over):
                        signal = "confirming"
                    elif total_move != 0:
                        signal = "contrary"

            # Apply modifier based on signal + significance
            if signal == "confirming":
                if significance == "major":
                    modifier = 1.10  # +10% confidence boost
                elif significance == "moderate":
                    modifier = 1.05
                else:
                    modifier = 1.02
            elif signal == "contrary":
                if significance == "major":
                    modifier = 0.85  # -15% confidence reduction
                elif significance == "moderate":
                    modifier = 0.92
                else:
                    modifier = 0.97

            pick["line_move"] = movement
            pick["line_move_signal"] = signal
            pick["line_move_modifier"] = modifier

    except Exception as e:
        logger.debug("Line movement annotation error: %s", e)
