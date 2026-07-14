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

    rl_data = pred.get("run_line") or {}
    model_spread = rl_data.get("model_spread", 0)
    # Prefer MLB's Poisson margin_probs inside run_line; fall back to the
    # top-level key used by NHL (Poisson puck line) and NBA Q1 (discretized
    # Gaussian). Both expose the same {margin: prob} shape.
    margin_probs = rl_data.get("margin_probs") or pred.get("margin_probs")

    if not rl_data.get("home_minus_1_5"):
        return []

    for alt in alt_spreads:
        point = alt.get("point")
        home_odds = alt.get("home_odds")
        away_odds = alt.get("away_odds")
        if point is None:
            continue

        import math

        if margin_probs:
            # Exact probability from Poisson score matrix (MLB).
            # P(home covers spread X) = P(margin > -X)
            # For half-point lines: margin > -point is unambiguous
            threshold = -point
            home_cover_prob = sum(
                p for m, p in margin_probs.items() if m > threshold
            )
        else:
            # Fallback: normal CDF for NHL/NBA or missing Poisson data
            _MARGIN_STD = {"nhl": 2.6, "nba": 11.5}
            sport_key = "nhl" if pred.get("puck_line") else (
                "nba" if pred.get("q1_ml_home") else "mlb")
            std = _MARGIN_STD.get(sport_key, 4.5)
            z = (model_spread - (-point)) / std
            home_cover_prob = 0.5 * (1 + math.erf(z / math.sqrt(2)))

        # Sanity clamp
        home_cover_prob = max(0.05, min(0.95, home_cover_prob))
        away_cover_prob = 1.0 - home_cover_prob

        # Migrated to picks_core.score_pick. The +X improvement floor
        # vs the existing primary RL edge stays here as alt-vs-main
        # comparison logic.
        from .picks_core import score_pick as _score_pick
        for abbr_lbl, prob, side_odds, side_point in [
            (h_abbr, home_cover_prob, home_odds, point),
            (a_abbr, away_cover_prob, away_odds, -point),
        ]:
            if side_odds is None or abs(side_odds) < 100:
                continue
            sign = "+" if side_point > 0 else ""
            scored = _score_pick({
                "type": "ALT RL",
                "pick": f"{abbr_lbl} {sign}{side_point}",
                "raw_prob": prob,
                "odds": side_odds,
            }, sport="mlb", juice_wall=ALT_LINE_JUICE_WALL)
            if scored is None:
                continue
            if scored["edge"] <= existing_rl_edge + ALT_LINE_MIN_EDGE_IMPROVEMENT:
                continue
            scored["is_alt"] = True
            scored["alt_vs_primary_improvement"] = round(
                scored["edge"] - existing_rl_edge, 1)
            new_picks.append(scored)

    # Return only the single best alt (highest edge)
    if new_picks:
        new_picks.sort(key=lambda p: -p["edge"])
        return [new_picks[0]]
    return []


def _passes_alt_ou_ceiling(edge: float) -> bool:
    """Drop ALT O/U picks above MAIN_EDGE_CEILING['mlb']['ALT O/U'].

    Tracker autopsy 2026-04-30 showed direction-aware overshoot:
    Under at 15-20%/20%+ edge buckets ROI'd -37%/-57%. The high-edge
    tail is where the model's probability is most miscalibrated.
    Higher edges => worse outcomes across both Over and Under.
    Returns True when the pick should be kept.
    """
    try:
        from .config import MAIN_EDGE_CEILING
        ceiling = (MAIN_EDGE_CEILING.get("mlb") or {}).get("ALT O/U")
    except Exception:
        ceiling = None
    return ceiling is None or float(edge) < float(ceiling)


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

    # Direction gates: MLB_ALLOW_OU_OVER / MLB_ALLOW_OU_UNDER must
    # also apply to ALT lines. The primary O/U gate alone left a
    # leak the user surfaced on 2026-04-28 — with Unders disabled,
    # the picker still emitted ALT O/U Under picks (9-12, -$585) for
    # exactly the reason MLB_ALLOW_OU_UNDER was set to False. Honour
    # the same flag here. shop_alt_totals is only wired into MLB's
    # generate_picks (NHL/NBA have their own ALT line shoppers in
    # nhl_picks.py / nba_picks.py), so the MLB-flag lookup is safe.
    try:
        from .config import get_flag
        allow_over = bool(get_flag("MLB_ALLOW_OU_OVER", True))
        allow_under = bool(get_flag("MLB_ALLOW_OU_UNDER", True))
    except Exception:
        allow_over = True
        allow_under = True

    for alt in alt_totals:
        line = alt.get("line")
        over_odds = alt.get("over_odds")
        under_odds = alt.get("under_odds")
        if line is None:
            continue

        import math
        diff = model_total - line

        # Use exact total distribution when available.
        # MLB nests it in run_line; NHL/NBA put it at top level.
        # Gives exact P(total > line) instead of a normal-CDF approximation.
        total_probs = (pred.get("run_line") or {}).get("total_probs") \
            or pred.get("total_probs")
        if total_probs:
            # P(over X.5) = sum of P(total >= X+1)
            # For half-point lines, no push possible
            over_prob = sum(
                p for t, p in total_probs.items() if t > line
            )
            under_prob = 1.0 - over_prob
        else:
            # Fallback: logistic function for NHL/NBA
            _TOTAL_STD = {"nhl": 2.3, "nba": 10.5}
            sport_key = "nba" if model_total > 100 else (
                "nhl" if model_total < 12 else "mlb")
            std = _TOTAL_STD.get(sport_key, 4.5)
            over_prob = 1.0 / (1.0 + math.exp(-diff / std))
            under_prob = 1.0 - over_prob

        # Migrated to picks_core.score_pick. picks_core's edge ceiling
        # check covers the MAIN_EDGE_CEILING['mlb']['ALT O/U'] guard;
        # _passes_alt_ou_ceiling() local helper is now redundant but
        # left in place for any external caller still importing it.
        from .picks_core import score_pick as _score_pick
        for label, prob, side_odds, allowed in [
            (f"Over {line}",  over_prob,  over_odds,  allow_over),
            (f"Under {line}", under_prob, under_odds, allow_under),
        ]:
            if not allowed:
                continue
            if side_odds is None or abs(side_odds) < 100:
                continue
            scored = _score_pick({
                "type": "ALT O/U",
                "pick": label,
                "raw_prob": prob,
                "odds": side_odds,
            }, sport="mlb", juice_wall=ALT_LINE_JUICE_WALL)
            if scored is None:
                continue
            if scored["edge"] <= existing_ou_edge + ALT_LINE_MIN_EDGE_IMPROVEMENT:
                continue
            scored["is_alt"] = True
            scored["alt_vs_primary_improvement"] = round(
                scored["edge"] - existing_ou_edge, 1)
            new_picks.append(scored)

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

def _direction_label_for_clv(bet_type: str, pick: str | None) -> str | None:
    """Mirrors clv_diagnostics. Over/Under for totals, NRFI/YRFI for
    first inning. Returned None means the bet type isn't direction-
    splittable, so the caller falls back to a single-cell aggregate."""
    if not pick:
        return None
    pk = pick.strip().lower()
    if pk.startswith("over"):
        return "over"
    if pk.startswith("under"):
        return "under"
    bt = (bet_type or "").strip().lower()
    if bt in ("1st inn", "1stinn", "nrfi"):
        if "nrfi" in pk:
            return "nrfi"
        if "yrfi" in pk:
            return "yrfi"
    return None


def get_clv_reliability(sport: str, market_type: str,
                        pick_text: str | None = None,
                        min_samples: int = 15) -> float | None:
    """Get average CLV multiplier for the (sport, market_type, direction)
    cell, where direction is derived from ``pick_text`` (Over/Under,
    NRFI/YRFI). Direction-blind aggregates were averaging Over+Under,
    masking the asymmetry that motivated edge_floors. Pick-level CLV
    must use the same direction split.

    Returns a reliability multiplier:
      - avg_clv > 2.0 → 1.15 (strong CLV history, boost confidence)
      - avg_clv > 0.5 → 1.05 (mild positive CLV)
      - avg_clv < -1.0 → 0.90 (negative CLV, fade confidence)
      - else → 1.0 (neutral)

    Cutoff aware — only counts post-MC/GBM picks
    (engine.model_cutoffs). None when sample is below ``min_samples``.
    """
    if not ENABLE_CLV_CONFIDENCE:
        return None

    # Canonical DB connection per sport (WAL-enabled, schema-correct).
    try:
        if sport == "mlb":
            from .db import get_conn
            table = "picks"
        elif sport == "nhl":
            from .nhl_db import get_conn
            table = "nhl_picks"
        elif sport == "nba":
            from .nba_db import get_conn
            table = "nba_picks"
        else:
            return None
        conn = get_conn()
    except Exception as e:
        logger.debug("CLV reliability: cannot open %s db: %s", sport, e)
        return None

    try:
        from .model_cutoffs import get_cutoff
        cutoff = get_cutoff(sport)
        params: list = [market_type]
        date_clause = ""
        if cutoff:
            date_clause = " AND date >= ?"
            params.append(cutoff)

        rows = conn.execute(f"""
            SELECT odds, closing_odds, pick FROM {table}
            WHERE result IS NOT NULL
              AND odds IS NOT NULL AND closing_odds IS NOT NULL
              AND bet_type = ?{date_clause}
        """, tuple(params)).fetchall()
    except Exception as e:
        logger.debug("CLV reliability query error: %s", e)
        return None

    if len(rows) < min_samples:
        return None

    target_direction = _direction_label_for_clv(market_type, pick_text)
    clv_values: list[float] = []
    for r in rows:
        odds_val = r["odds"] if hasattr(r, "keys") else r[0]
        closing = r["closing_odds"] if hasattr(r, "keys") else r[1]
        row_pick = r["pick"] if hasattr(r, "keys") else r[2]
        if not odds_val or not closing:
            continue
        # If caller's pick has a direction, only pool same-direction
        # historical rows. Direction-less markets (ML, RL, PL, spreads
        # without home/away cue) collapse the filter.
        if target_direction is not None:
            row_dir = _direction_label_for_clv(market_type, row_pick)
            if row_dir != target_direction:
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

    cell = f"{market_type}/{target_direction}" if target_direction else market_type
    logger.info("CLV for %s/%s: avg=%.2f%% (%d samples) -> reliability x%.2f",
                 sport, cell, avg_clv, len(clv_values), mult)
    return mult


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
