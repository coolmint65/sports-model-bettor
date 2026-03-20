"""
CLV-based meta-signal engine for confidence weighting.

Uses historical Closing Line Value (CLV) and hit rate by bet type
to dynamically adjust prediction confidence. Bet types where the
model consistently beats the closing line deserve a confidence boost;
bet types where it consistently loses to the close deserve a dampen.

This turns CLV from a passive metric into an active feedback signal.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy import Integer, and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.prediction import BetResult, Prediction

logger = logging.getLogger(__name__)

# Minimum sample size before applying any adjustment.
MIN_SAMPLE_SIZE = 30

# CLV and hit-rate thresholds for boost/dampen decisions.
CLV_POSITIVE_THRESHOLD = 0.02
CLV_NEGATIVE_THRESHOLD = -0.02
HIT_RATE_POSITIVE_THRESHOLD = 0.52
HIT_RATE_NEGATIVE_THRESHOLD = 0.48

# Maximum confidence adjustment magnitude (10%).
MAX_ADJUSTMENT = 0.10


class MetaSignalEngine:
    """Computes confidence adjustments from historical CLV performance."""

    async def get_bet_type_performance(
        self, db: AsyncSession
    ) -> Dict[str, Dict[str, Any]]:
        """Aggregate CLV and hit-rate stats for each bet type.

        Queries all settled prematch predictions that have CLV data,
        groups by bet_type, and computes performance metrics plus a
        confidence adjustment multiplier.

        Returns:
            Dict keyed by bet_type (e.g. "ml", "total", "spread") with:
                avg_clv, hit_rate, roi, sample_size, confidence_adjustment
        """
        return await self._query_performance(db, window_days=None)

    async def get_rolling_performance(
        self, db: AsyncSession, window_days: int = 30
    ) -> Dict[str, Dict[str, Any]]:
        """Same as get_bet_type_performance but limited to the last N days.

        Captures recent model drift so the adjustment reacts to changing
        market conditions rather than being anchored to all-time stats.
        """
        return await self._query_performance(db, window_days=window_days)

    # ------------------------------------------------------------------ #
    #  Private implementation                                              #
    # ------------------------------------------------------------------ #

    async def _query_performance(
        self,
        db: AsyncSession,
        window_days: Optional[int] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Core query logic shared by all-time and rolling methods."""

        # Build WHERE filters: settled prematch predictions with CLV data.
        filters = [
            BetResult.clv.isnot(None),
            Prediction.phase == "prematch",
        ]

        if window_days is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
            filters.append(BetResult.settled_at >= cutoff)

        stmt = (
            select(
                Prediction.bet_type,
                func.count(BetResult.id).label("sample_size"),
                func.avg(BetResult.clv).label("avg_clv"),
                func.sum(
                    func.cast(
                        BetResult.was_correct,
                        Integer,
                    )
                ).label("wins"),
                func.sum(BetResult.profit_loss).label("total_profit"),
            )
            .join(Prediction, BetResult.prediction_id == Prediction.id)
            .where(and_(*filters))
            .group_by(Prediction.bet_type)
        )

        result = await db.execute(stmt)
        rows = result.all()

        performance: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            sample_size = row.sample_size or 0
            wins = row.wins or 0
            total_profit = row.total_profit or 0.0
            avg_clv = float(row.avg_clv) if row.avg_clv is not None else 0.0

            hit_rate = wins / sample_size if sample_size > 0 else 0.0
            roi = total_profit / sample_size if sample_size > 0 else 0.0

            confidence_adjustment = self._compute_adjustment(
                avg_clv, hit_rate, sample_size
            )

            performance[row.bet_type] = {
                "avg_clv": round(avg_clv, 4),
                "hit_rate": round(hit_rate, 4),
                "roi": round(roi, 4),
                "sample_size": sample_size,
                "confidence_adjustment": round(confidence_adjustment, 4),
            }

        return performance

    @staticmethod
    def _compute_adjustment(
        avg_clv: float, hit_rate: float, sample_size: int
    ) -> float:
        """Derive a confidence multiplier from CLV and hit-rate data.

        Rules:
        - Boost (up to +10%) when avg_clv > 0.02 AND hit_rate > 0.52
        - Dampen (up to -10%) when avg_clv < -0.02 AND hit_rate < 0.48
        - Neutral (1.0) otherwise or when sample_size < 30
        """
        if sample_size < MIN_SAMPLE_SIZE:
            return 1.0

        if avg_clv > CLV_POSITIVE_THRESHOLD and hit_rate > HIT_RATE_POSITIVE_THRESHOLD:
            boost = min(avg_clv * 2, MAX_ADJUSTMENT)
            return 1.0 + boost

        if avg_clv < CLV_NEGATIVE_THRESHOLD and hit_rate < HIT_RATE_NEGATIVE_THRESHOLD:
            dampen = min(abs(avg_clv) * 2, MAX_ADJUSTMENT)
            return 1.0 - dampen

        return 1.0
