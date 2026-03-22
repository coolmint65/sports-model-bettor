"""
Fractional Kelly criterion bet sizing.

Converts model edge and confidence into recommended unit sizes.
Full Kelly is mathematically optimal but too aggressive for real-world
variance — fractional Kelly (0.25–0.5x) provides most of the growth
with significantly reduced drawdown risk.

Also includes bankroll management: drawdown circuit breakers,
max concurrent exposure caps, and streak-aware gating.
"""

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import Integer, and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
#  Kelly sizing configuration
# ---------------------------------------------------------------------------

@dataclass
class KellyConfig:
    """Tunable parameters for Kelly criterion sizing."""

    # Fraction of full Kelly to use (0.25 = quarter-Kelly).
    # Quarter-Kelly captures ~75% of growth with ~50% less variance.
    kelly_fraction: float = 0.25

    # Minimum unit size (floor — never recommend less than this)
    min_units: float = 0.5

    # Maximum unit size (ceiling — never recommend more than this)
    max_units: float = 5.0

    # Default bankroll in units (used if no explicit bankroll is set)
    default_bankroll_units: float = 100.0

    # Maximum total units at risk across all open bets
    max_concurrent_exposure: float = 15.0

    # Drawdown circuit breaker: if rolling P/L drops below this %
    # of bankroll in the lookback window, reduce all sizes by this factor.
    drawdown_threshold_pct: float = -15.0  # -15% of bankroll
    drawdown_lookback_days: int = 14
    drawdown_reduction_factor: float = 0.5  # halve unit sizes

    # Streak gating: consecutive losses before triggering size reduction
    streak_loss_threshold: int = 8
    streak_reduction_factor: float = 0.5

    # Edge tiers for non-Kelly flat sizing fallback
    edge_tier_small: float = 0.03   # 3-5% edge → 1 unit
    edge_tier_medium: float = 0.05  # 5-8% edge → 1.5 units
    edge_tier_large: float = 0.08   # 8%+ edge → 2 units


# Global config instance
kelly_config = KellyConfig()


# ---------------------------------------------------------------------------
#  Core Kelly calculation
# ---------------------------------------------------------------------------

def kelly_criterion(
    win_probability: float,
    odds: float,
    fraction: float = 0.25,
) -> float:
    """Compute fractional Kelly bet size.

    The Kelly criterion gives the optimal fraction of bankroll to wager:
        f* = (b*p - q) / b
    where:
        b = net odds (decimal odds - 1)
        p = probability of winning
        q = 1 - p (probability of losing)

    Args:
        win_probability: Model's estimated probability of winning (0-1).
        odds: American odds for the bet.
        fraction: Kelly fraction (0.25 = quarter-Kelly).

    Returns:
        Recommended bet size as fraction of bankroll (0.0 if no edge).
    """
    if win_probability <= 0 or win_probability >= 1:
        return 0.0
    if odds == 0:
        return 0.0

    # Convert American odds to decimal payout (net of stake)
    if odds > 0:
        b = odds / 100.0
    else:
        b = 100.0 / abs(odds)

    p = win_probability
    q = 1.0 - p

    # Kelly formula
    full_kelly = (b * p - q) / b

    if full_kelly <= 0:
        return 0.0  # No edge — don't bet

    return full_kelly * fraction


def compute_recommended_units(
    confidence: float,
    edge: float,
    odds: float,
    bet_confidence: Optional[float] = None,
    *,
    bankroll_units: Optional[float] = None,
    current_exposure: float = 0.0,
    drawdown_active: bool = False,
    streak_active: bool = False,
) -> Dict[str, Any]:
    """Compute recommended unit size for a bet.

    Uses fractional Kelly criterion as the primary method, with
    edge-tier flat sizing as a secondary signal. The final recommendation
    blends both, capped by bankroll constraints.

    Args:
        confidence: Model win probability (calibrated).
        edge: Model edge (confidence - implied_prob).
        odds: American odds for the bet.
        bet_confidence: Conviction score (0.3-0.92) from conviction.py.
        bankroll_units: Total bankroll in units.
        current_exposure: Total units currently at risk.
        drawdown_active: Whether drawdown circuit breaker is triggered.
        streak_active: Whether losing streak reduction is active.

    Returns:
        Dict with recommended_units, kelly_units, tier_units,
        sizing_method, and any active limiters.
    """
    cfg = kelly_config
    bankroll = bankroll_units or cfg.default_bankroll_units

    # --- Kelly sizing ---
    kelly_fraction_of_bankroll = kelly_criterion(
        confidence, odds, cfg.kelly_fraction
    )
    kelly_units = round(kelly_fraction_of_bankroll * bankroll, 2)

    # --- Edge-tier flat sizing (fallback / secondary signal) ---
    if edge >= cfg.edge_tier_large:
        tier_units = 2.0
    elif edge >= cfg.edge_tier_medium:
        tier_units = 1.5
    else:
        tier_units = 1.0

    # --- Conviction boost ---
    # High conviction (>0.70) gets a modest boost; low conviction (<0.50) gets a haircut.
    conviction_multiplier = 1.0
    if bet_confidence is not None:
        if bet_confidence >= 0.75:
            conviction_multiplier = 1.2
        elif bet_confidence >= 0.65:
            conviction_multiplier = 1.1
        elif bet_confidence < 0.45:
            conviction_multiplier = 0.8

    # --- Blend Kelly + tier ---
    # Primary: Kelly (70%), Secondary: tier (30%)
    # Kelly captures the mathematical edge; tier provides a floor
    # when Kelly produces very small sizes on low-odds bets.
    if kelly_units > 0:
        raw_units = 0.7 * kelly_units + 0.3 * tier_units
        sizing_method = "kelly_blend"
    else:
        raw_units = tier_units
        sizing_method = "edge_tier"

    raw_units *= conviction_multiplier

    # --- Apply limiters ---
    limiters = []

    # Drawdown circuit breaker
    if drawdown_active:
        raw_units *= cfg.drawdown_reduction_factor
        limiters.append("drawdown_breaker")

    # Losing streak reduction
    if streak_active:
        raw_units *= cfg.streak_reduction_factor
        limiters.append("streak_gate")

    # Max concurrent exposure
    remaining_exposure = max(0, cfg.max_concurrent_exposure - current_exposure)
    if raw_units > remaining_exposure:
        raw_units = remaining_exposure
        limiters.append("exposure_cap")

    # Clamp to configured range
    final_units = round(max(cfg.min_units, min(cfg.max_units, raw_units)), 1)

    # If exposure is fully used up, return 0
    if remaining_exposure <= 0:
        final_units = 0.0
        limiters.append("exposure_exhausted")

    return {
        "recommended_units": final_units,
        "kelly_units": round(kelly_units, 2),
        "tier_units": tier_units,
        "conviction_multiplier": conviction_multiplier,
        "sizing_method": sizing_method,
        "limiters": limiters,
        "kelly_fraction": cfg.kelly_fraction,
    }


# ---------------------------------------------------------------------------
#  Bankroll state helpers (require DB access)
# ---------------------------------------------------------------------------

async def check_drawdown_breaker(db: AsyncSession) -> bool:
    """Check if the drawdown circuit breaker should be active.

    Looks at rolling P/L over the lookback window. If cumulative losses
    exceed the threshold percentage of starting bankroll, returns True.
    """
    from app.models.prediction import BetResult, Prediction

    cfg = kelly_config
    cutoff = datetime.now(timezone.utc) - timedelta(days=cfg.drawdown_lookback_days)

    stmt = (
        select(func.sum(BetResult.profit_loss).label("rolling_pl"))
        .join(Prediction, BetResult.prediction_id == Prediction.id)
        .where(
            and_(
                Prediction.phase == "prematch",
                BetResult.settled_at >= cutoff,
            )
        )
    )
    result = await db.execute(stmt)
    row = result.one_or_none()
    rolling_pl = row.rolling_pl if row and row.rolling_pl else 0.0

    threshold = cfg.default_bankroll_units * (cfg.drawdown_threshold_pct / 100.0)

    if rolling_pl <= threshold:
        logger.warning(
            "Drawdown breaker ACTIVE: rolling P/L %.1f units "
            "(threshold: %.1f units over %d days)",
            rolling_pl, threshold, cfg.drawdown_lookback_days,
        )
        return True

    return False


async def check_streak_gate(db: AsyncSession) -> bool:
    """Check if the losing streak gate should be active.

    Looks at the most recent N settled predictions. If the last
    streak_loss_threshold predictions are all losses, returns True.
    """
    from app.models.prediction import BetResult, Prediction

    cfg = kelly_config

    stmt = (
        select(BetResult.was_correct)
        .join(Prediction, BetResult.prediction_id == Prediction.id)
        .where(Prediction.phase == "prematch")
        .order_by(BetResult.settled_at.desc())
        .limit(cfg.streak_loss_threshold)
    )
    result = await db.execute(stmt)
    rows = result.scalars().all()

    if len(rows) < cfg.streak_loss_threshold:
        return False

    # All must be losses for streak to trigger
    all_losses = all(not was_correct for was_correct in rows)

    if all_losses:
        logger.warning(
            "Streak gate ACTIVE: last %d bets were all losses",
            cfg.streak_loss_threshold,
        )

    return all_losses


async def get_current_exposure(db: AsyncSession) -> float:
    """Sum up units on all unsettled tracked bets."""
    from app.models.prediction import TrackedBet

    stmt = (
        select(func.sum(TrackedBet.units).label("total_exposure"))
        .where(TrackedBet.result.is_(None))
    )
    result = await db.execute(stmt)
    row = result.one_or_none()
    return float(row.total_exposure) if row and row.total_exposure else 0.0


def compute_expected_value(
    confidence: float,
    odds: float,
    units: float = 1.0,
) -> float:
    """Compute expected value for a bet.

    EV = (probability * potential_profit) - ((1 - probability) * stake)

    Args:
        confidence: Calibrated win probability.
        odds: American odds.
        units: Stake in units.

    Returns:
        Expected value in units.
    """
    if odds == 0 or confidence <= 0:
        return 0.0

    if odds > 0:
        potential_profit = units * (odds / 100.0)
    else:
        potential_profit = units * (100.0 / abs(odds))

    ev = (confidence * potential_profit) - ((1.0 - confidence) * units)
    return round(ev, 4)
