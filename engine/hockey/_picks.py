"""Generic hockey picks generator — takes a prediction + HR odds dict
and produces ML / PL / O/U picks scored through ``picks_core.score_pick``.

Same shape NHL's ``generate_nhl_picks`` returns so the frontend GameCard
+ EdgeBadge primitives work without per-league branching. Used for AHL +
PWHL today; future hockey leagues plug in by giving us the same
prediction shape (home/away expected goals + ml_home).
"""
from __future__ import annotations

import logging

from ..picks_core import score_pick
from ..config import HOCKEY_FRAMEWORK_JUICE_WALL as JUICE_WALL

logger = logging.getLogger(__name__)


def _implied(american: int) -> float:
    if american < 0:
        return abs(american) / (abs(american) + 100)
    return 100 / (american + 100)


# Per-league margin sigma cache. Fitted sigma lives in the league's
# calibration constants JSON; cold-start leagues fall back to a
# hockey-typical 2.0 goals. Cached to avoid re-reading the JSON on
# every pick.
_MARGIN_SIGMA_CACHE: dict[str, float] = {}
_DEFAULT_MARGIN_SIGMA = 2.0


def _margin_sigma_for(sport: str) -> float:
    """Per-league margin sigma fitted from the games table. AHL games
    are tighter than NHL (~2.0 vs ~2.5 historically); PWHL tighter
    still. Cold-start leagues fall back to 2.0.

    Computed once per process and cached — the games table is the
    ground truth so a per-tick refit isn't worth the IO."""
    if sport in _MARGIN_SIGMA_CACHE:
        return _MARGIN_SIGMA_CACHE[sport]
    sigma = _DEFAULT_MARGIN_SIGMA
    try:
        import sqlite3
        from pathlib import Path
        db_path = Path(__file__).resolve().parent.parent.parent \
                    / "data" / "hockey" / f"{sport}.db"
        if db_path.exists():
            conn = sqlite3.connect(str(db_path), timeout=5)
            # Sample fresh history first (last 2 seasons); fall back to
            # whole-table when the dataset is thin.
            row = conn.execute("""
                SELECT
                  COUNT(*) AS n,
                  AVG(CAST(home_score AS REAL) - away_score) AS mean,
                  AVG((CAST(home_score AS REAL) - away_score) *
                      (CAST(home_score AS REAL) - away_score)) AS sq
                FROM games
                WHERE status='final'
                  AND home_score IS NOT NULL AND away_score IS NOT NULL
            """).fetchone()
            conn.close()
            if row and row[0] and row[0] >= 50:
                import math as _m
                n, mean, sq = int(row[0]), float(row[1]), float(row[2])
                var = max(sq - mean * mean, 0.25)
                sigma = _m.sqrt(var)
    except Exception as e:
        logger.debug("[%s] margin sigma fit failed, using default 2.0: %s",
                      sport, e)
    _MARGIN_SIGMA_CACHE[sport] = sigma
    return sigma


def generate_picks(prediction: dict, odds: dict, *, sport: str) -> list[dict]:
    """Score every available core market (ML / PL / O/U) for one game.

    ``prediction`` is the predictor's output dict
    (``home_expected``/``away_expected``/``ml_home``).
    ``odds`` is the HR odds dict
    (``home_ml``/``away_ml``/``home_spread_*``/``away_spread_*``/
    ``over_under``/``over_odds``/``under_odds``).
    ``sport`` is the registry key (``ahl`` / ``pwhl``).

    Returns picks sorted descending by edge. Empty list when neither
    odds nor predictions cover any core market.
    """
    if not prediction or not odds:
        return []

    # Pull the real team abbreviations from the odds dict — picks_core
    # passes ``pick`` straight through to display, so "Home" / "Away"
    # would render as literal strings on the card. Fall back to the
    # generic words only when the odds dict is missing them (defensive).
    home_abbr = odds.get("home_abbr") or "Home"
    away_abbr = odds.get("away_abbr") or "Away"

    out: list[dict] = []

    # ── Moneyline ──
    ml_home_prob = prediction.get("ml_home")
    ml_away_prob = prediction.get("ml_away")
    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")
    if ml_home_prob is not None and home_ml is not None:
        scored = score_pick({
            "type": "ML", "pick": home_abbr,
            "raw_prob": ml_home_prob, "odds": int(home_ml),
        }, sport=sport, juice_wall=JUICE_WALL)
        if scored:
            out.append(scored)
    if ml_away_prob is not None and away_ml is not None:
        scored = score_pick({
            "type": "ML", "pick": away_abbr,
            "raw_prob": ml_away_prob, "odds": int(away_ml),
        }, sport=sport, juice_wall=JUICE_WALL)
        if scored:
            out.append(scored)

    # ── Puck Line (1.5) ──
    # Margin convention from the predictor: ``margin = away_xg - home_xg``
    # (negative when home is favored, see engine/sports/ahl/predict.py
    # line 160). Assume the residual is N(margin, sigma) — sigma read
    # from the per-league calibration constants when available, else
    # falls back to the hockey-typical 2.0 goals.
    margin = prediction.get("margin")
    if margin is not None:
        import math
        sigma = _margin_sigma_for(sport)
        # "home -1.5" pays when home wins by 2+ goals, i.e.
        # home_score - away_score > 1.5, i.e. -margin > 1.5,
        # i.e. margin < -1.5. So P(home covers -1.5) is the LOWER tail
        # of the margin distribution: Φ((-1.5 - margin) / sigma).
        # (Previous implementation inverted the tail — in an even
        # matchup both sides came out to 77% covering -1.5, summing
        # to 154%, which over-fired every PL favorite-cover pick.)
        z_home = (-1.5 - margin) / sigma
        p_home_minus_15 = 0.5 * (1 + math.erf(z_home / math.sqrt(2)))
        p_away_plus_15 = 1 - p_home_minus_15
        # "away -1.5" pays when away wins by 2+ goals, i.e.
        # away - home > 1.5, i.e. margin > 1.5 — UPPER tail.
        z_away = (1.5 - margin) / sigma
        p_away_minus_15 = 1 - 0.5 * (1 + math.erf(z_away / math.sqrt(2)))
        p_home_plus_15 = 1 - p_away_minus_15
        h_pl_pt = odds.get("home_spread_point")
        h_pl_odds = odds.get("home_spread_odds")
        a_pl_pt = odds.get("away_spread_point")
        a_pl_odds = odds.get("away_spread_odds")
        if h_pl_pt is not None and h_pl_odds is not None:
            prob = p_home_minus_15 if h_pl_pt < 0 else p_home_plus_15
            scored = score_pick({
                "type": "PL", "pick": f"{home_abbr} {h_pl_pt:+.1f}",
                "raw_prob": prob, "odds": int(h_pl_odds),
            }, sport=sport, juice_wall=JUICE_WALL)
            if scored:
                out.append(scored)
        if a_pl_pt is not None and a_pl_odds is not None:
            prob = p_away_minus_15 if a_pl_pt < 0 else p_away_plus_15
            scored = score_pick({
                "type": "PL", "pick": f"{away_abbr} {a_pl_pt:+.1f}",
                "raw_prob": prob, "odds": int(a_pl_odds),
            }, sport=sport, juice_wall=JUICE_WALL)
            if scored:
                out.append(scored)

    # ── Total Goals (O/U) ──
    # Use a Poisson-style sum tail. Our predictor returns total = home_xg + away_xg.
    # P(total >= line) is the upper tail of a Poisson(total) by line.
    total = prediction.get("total")
    line = odds.get("over_under")
    if total is not None and line is not None:
        # Sum of two independent Poissons is Poisson(total). Approximate
        # P(>line) via the Skellam-like CDF; for integer line+0.5 the
        # upper tail is sum k > line of Poisson(total, k).
        import math
        def _poisson_pmf(lam, k):
            return (lam ** k) * math.exp(-lam) / math.factorial(k)
        p_over = sum(_poisson_pmf(total, k)
                      for k in range(int(line) + 1, 20))
        p_under = 1 - p_over
        over_odds = odds.get("over_odds")
        under_odds = odds.get("under_odds")
        if over_odds is not None:
            scored = score_pick({
                "type": "OU", "pick": f"Over {line}",
                "raw_prob": p_over, "odds": int(over_odds),
            }, sport=sport, juice_wall=JUICE_WALL)
            if scored:
                out.append(scored)
        if under_odds is not None:
            scored = score_pick({
                "type": "OU", "pick": f"Under {line}",
                "raw_prob": p_under, "odds": int(under_odds),
            }, sport=sport, juice_wall=JUICE_WALL)
            if scored:
                out.append(scored)

    # Sort by edge descending — same convention NHL/basketball use.
    out.sort(key=lambda p: -(p.get("edge") or 0))
    # Tag confidence so the EdgeBadge color story matches every other
    # sport — strong / moderate / lean / skip cutoffs are sport-agnostic.
    try:
        from .._pick_helpers import tag_confidence
        tag_confidence(out)
    except Exception as e:
        logger.debug("[%s] tag_confidence failed: %s", sport, e)
    return out
