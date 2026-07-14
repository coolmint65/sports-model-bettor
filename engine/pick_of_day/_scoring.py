"""POTD candidate scoring.

The single function that decides "which pick across the slate is THE
play of the day". Combines four signals — capped raw edge, model
confidence, per-market historical reliability, and primary/alt/derivative
class weighting — into a single score the cross-game ranking sorts on.

History: the POTD started as raw-edge selection (sometimes Kelly-weighted,
sometimes EV-weighted) and burned itself on alt-line longshots. Current
formula is "conviction-weighted" — see _score_pick docstring for why
each component is bounded the way it is. Kelly is explicitly rejected
in scoring per the user's directive; see feedback_no_kelly.md.
"""

from __future__ import annotations
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

MIN_EDGE = 5.0  # Minimum edge % to qualify as POTD candidate

# Minimum model probability to qualify as POTD candidate. User
# directive 2026-04-30: POTD should be the bet that wins ~80% of the
# time. A 12%-edge longshot at 0.45 prob doesn't fit that — it's a
# +EV play, not a "lock". The 0.65 floor pushes POTD onto medium-to-
# heavy favorites. Combined with the new prob-weighted scoring
# (below), POTD lands on the highest-conviction qualifying pick.
MIN_PROB = 0.65

# Deprecated 2026-04-24 — POTD now considers every pick from
# `all_picks`, ranked by `_score_pick`. Kept here as documentation of
# the historical whitelist so the intent isn't lost.
MLB_ALLOWED_TYPES_LEGACY = {"RL", "ML"}
NHL_ALLOWED_TYPES_LEGACY = {"O/U", "PL"}
NBA_ALLOWED_TYPES_LEGACY = {"Q1_SPREAD", "Q1_TOTAL", "Q1_ML"}


_PRIMARY_MARKET_TYPES = {
    "mlb": {"ML", "RL", "O/U", "F5 ML", "F5 O/U", "F5 RL"},
    "nhl": {"ML", "PL", "O/U"},
    "nba": {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"},
}
_ALT_MARKET_TYPES = {
    "mlb": {"ALT RL", "ALT O/U", "ALT ML"},
    "nhl": {"ALT PL", "ALT O/U"},
    "nba": set(),
}

# Break-even hit rate at -110 (the standard juice). Used as the
# baseline that a market's recent win rate is divided against to get
# a reliability multiplier.
_BREAK_EVEN_HIT_RATE = 0.5238


def _kelly_fraction(prob: float, odds: int) -> float:
    """Quarter-Kelly fraction for bet sizing. Used for the displayed
    `kelly_pct` field on the POTD card; not used for selection."""
    if not odds or prob is None or prob <= 0 or prob >= 1:
        return 0.0
    decimal = (odds / 100) + 1 if odds > 0 else (100 / abs(odds)) + 1
    b = decimal - 1
    if b <= 0:
        return 0.0
    q = 1 - prob
    kelly = (b * prob - q) / b
    if kelly <= 0:
        return 0.0
    return max(0.0, min(0.25, kelly * 0.25))


def _get_market_win_rate(sport: str, bet_type: str, days: int = 30) -> float:
    """Get recent win rate for a bet type from the tracker DB.
    Returns win rate (0.0-1.0) or 0.5 if insufficient data (<5 settled)."""
    try:
        from ._storage import _get_conn
        conn = _get_conn(sport)
        table = {"mlb": "picks", "nhl": "nhl_picks", "nba": "nba_picks"}.get(sport, "picks")
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        row = conn.execute(
            f"SELECT COUNT(*) as total, "
            f"SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins "
            f"FROM {table} WHERE bet_type = ? AND date >= ? AND result IS NOT NULL",
            (bet_type, cutoff),
        ).fetchone()
        total = row["total"] if row else 0
        wins = row["wins"] if row else 0
        if total >= 5:
            return wins / total
    except Exception as e:
        logger.debug("market WR lookup failed for %s/%s: %s", sport, bet_type, e)
    return 0.5  # neutral when no data


def _market_class_factor(sport: str, bet_type: str) -> float:
    """Primary > alt > derivative weighting. The discount for alts and
    derivatives is intentionally aggressive — a POTD on an alt-line
    longshot doesn't read as 'THE bet of the day', regardless of edge.
    Tuned so a primary market clearing 8–10% edge with prob>=0.55
    reliably outranks a 15%+ alt edge."""
    primary = _PRIMARY_MARKET_TYPES.get(sport, set())
    alt = _ALT_MARKET_TYPES.get(sport, set())
    if bet_type in primary:
        return 1.00
    if bet_type in alt:
        return 0.65
    return 0.55  # derivatives (Period Total, Q1 Team Total, BTS, etc.)


def _score_pick(pick: dict, sport: str = "mlb") -> float:
    """Confidence-led POTD score. Edge term dropped 2026-05-01.

    Per "be right, not edge-first" directive (memory:
    feedback_be_right_first): the model's purpose is accurate
    prediction of reality. POTD ranks by confidence in being right.
    Edge stays as a FILTER (MIN_EDGE = 5.0 at selection time) but
    not as a RANKER — a thicker edge does not promote a pick over
    a higher-confidence one. A 80%-prob pick at +5% edge ranks above
    a 70%-prob pick at +12% edge, full stop.

    Formula::

        score = prob ^ 2                  (squared confidence)
                × reliability_factor      (per-market historical WR)
                × class_factor            (primary > alt > derivative)

    Why each piece:
      - prob ^ 2: squaring amplifies the gap between 0.65 and 0.85.
        Drives the "lock pick" semantics — POTD goes to the bet we're
        most confident about, not the one with the biggest market
        disagreement.
      - reliability_factor [0.85, 1.15]: bet-type with historical WR
        well above break-even gets a small boost; a market we
        consistently misprice gets shrunk. Calibration-derived, not
        edge-derived.
      - class_factor: primary 1.00, alt 0.65, derivative 0.55.

    Candidates below MIN_EDGE / MIN_PROB are rejected at selection
    time, not via the score — caller filters before sorting.
    """
    edge = float(pick.get("edge", 0) or 0)
    if edge <= 0:
        return 0.0

    prob = float(pick.get("prob", 0) or 0)
    if prob <= 0:
        return 0.0

    confidence_factor = prob * prob

    bet_type = pick.get("type", "") or ""
    try:
        wr = _get_market_win_rate(sport, bet_type, days=30)
    except Exception as e:
        logger.debug("reliability lookup crashed for %s/%s: %s", sport, bet_type, e)
        wr = 0.5
    reliability_factor = max(0.85, min(1.15, wr / _BREAK_EVEN_HIT_RATE))

    class_factor = _market_class_factor(sport, bet_type)

    return confidence_factor * reliability_factor * class_factor
