"""
Closing Line Value (CLV) analysis engine.

CLV is the single best predictor of long-term betting profitability.
A bettor who consistently gets better odds than the closing line has
a real edge, regardless of short-term variance in hit rate.

This module provides:
1. Per-bet CLV tracking (already in BetResult.clv)
2. Rolling CLV analysis by bet type, sport, and confidence tier
3. CLV-based model trust scoring (which bet types have real edge?)
4. Alerts when CLV trends negative (edge decay detection)
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import Integer, and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.prediction import BetResult, Prediction

logger = logging.getLogger(__name__)


@dataclass
class CLVAlert:
    """An alert when CLV trends negative over a rolling window."""
    bet_type: str
    window_days: int
    avg_clv: float
    sample_size: int
    severity: str  # "warning" or "critical"
    message: str


class CLVAnalyzer:
    """Comprehensive CLV analysis for model edge validation."""

    # ---------------------------------------------------------------------------
    #  Per bet-type CLV analysis
    # ---------------------------------------------------------------------------

    async def analyze_by_bet_type(
        self,
        db: AsyncSession,
        window_days: Optional[int] = None,
        sport: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Detailed CLV analysis broken down by bet type.

        Returns per-bet-type metrics including avg CLV, CLV hit rate
        (% of bets that beat the close), and correlation between CLV
        and actual outcomes.
        """
        filters = [
            BetResult.clv.isnot(None),
            Prediction.phase == "prematch",
        ]
        if window_days:
            cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
            filters.append(BetResult.settled_at >= cutoff)
        if sport:
            from app.models.game import Game
            filters.append(Game.sport == sport)

        # Build join chain
        join_chain = (
            select(
                Prediction.bet_type,
                func.count(BetResult.id).label("sample_size"),
                func.avg(BetResult.clv).label("avg_clv"),
                func.sum(
                    case(
                        (BetResult.clv > 0, 1),
                        else_=0,
                    )
                ).label("positive_clv_count"),
                func.sum(
                    func.cast(BetResult.was_correct, Integer)
                ).label("wins"),
                func.sum(BetResult.profit_loss).label("total_profit"),
                func.min(BetResult.clv).label("min_clv"),
                func.max(BetResult.clv).label("max_clv"),
            )
            .join(Prediction, BetResult.prediction_id == Prediction.id)
        )

        if sport:
            from app.models.game import Game
            join_chain = join_chain.join(Game, Prediction.game_id == Game.id)

        stmt = join_chain.where(and_(*filters)).group_by(Prediction.bet_type)

        result = await db.execute(stmt)
        rows = result.all()

        analysis = {}
        for row in rows:
            n = row.sample_size or 0
            if n == 0:
                continue

            wins = row.wins or 0
            positive_clv = row.positive_clv_count or 0
            total_profit = row.total_profit or 0.0

            analysis[row.bet_type] = {
                "sample_size": n,
                "avg_clv": round(float(row.avg_clv or 0), 4),
                "clv_hit_rate": round(positive_clv / n, 4),  # % of bets beating the close
                "hit_rate": round(wins / n, 4),
                "roi": round(total_profit / n, 4),
                "total_profit": round(total_profit, 2),
                "min_clv": round(float(row.min_clv or 0), 4),
                "max_clv": round(float(row.max_clv or 0), 4),
                "edge_verdict": self._edge_verdict(
                    float(row.avg_clv or 0), positive_clv / n if n > 0 else 0, n
                ),
            }

        return analysis

    # ---------------------------------------------------------------------------
    #  CLV by confidence tier
    # ---------------------------------------------------------------------------

    async def analyze_by_confidence_tier(
        self,
        db: AsyncSession,
        window_days: Optional[int] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """CLV analysis broken down by model confidence tiers.

        Helps answer: does higher confidence actually correlate with
        better CLV (real edge) or just overconfidence?
        """
        tiers = [
            ("50-55%", 0.50, 0.55),
            ("55-60%", 0.55, 0.60),
            ("60-65%", 0.60, 0.65),
            ("65-70%", 0.65, 0.70),
            ("70%+", 0.70, 1.01),
        ]

        results = {}
        for label, low, high in tiers:
            filters = [
                BetResult.clv.isnot(None),
                Prediction.phase == "prematch",
                Prediction.confidence >= low,
                Prediction.confidence < high,
            ]
            if window_days:
                cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
                filters.append(BetResult.settled_at >= cutoff)

            stmt = (
                select(
                    func.count(BetResult.id).label("n"),
                    func.avg(BetResult.clv).label("avg_clv"),
                    func.sum(
                        case((BetResult.clv > 0, 1), else_=0)
                    ).label("positive_clv"),
                    func.sum(
                        func.cast(BetResult.was_correct, Integer)
                    ).label("wins"),
                    func.sum(BetResult.profit_loss).label("profit"),
                )
                .join(Prediction, BetResult.prediction_id == Prediction.id)
                .where(and_(*filters))
            )

            row = (await db.execute(stmt)).one_or_none()
            n = row.n if row and row.n else 0
            if n == 0:
                results[label] = {"sample_size": 0}
                continue

            wins = row.wins or 0
            positive_clv = row.positive_clv or 0
            profit = row.profit or 0.0

            results[label] = {
                "sample_size": n,
                "avg_clv": round(float(row.avg_clv or 0), 4),
                "clv_hit_rate": round(positive_clv / n, 4),
                "hit_rate": round(wins / n, 4),
                "roi": round(profit / n, 4),
            }

        return results

    # ---------------------------------------------------------------------------
    #  Rolling CLV trend (time series)
    # ---------------------------------------------------------------------------

    async def get_rolling_clv_trend(
        self,
        db: AsyncSession,
        window_size: int = 50,
        bet_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Compute a rolling average CLV over time.

        Returns a time series of rolling CLV averages, useful for
        detecting when model edge is decaying or improving.
        """
        filters = [
            BetResult.clv.isnot(None),
            Prediction.phase == "prematch",
        ]
        if bet_type:
            filters.append(Prediction.bet_type == bet_type)

        stmt = (
            select(
                BetResult.settled_at,
                BetResult.clv,
                Prediction.bet_type,
            )
            .join(Prediction, BetResult.prediction_id == Prediction.id)
            .where(and_(*filters))
            .order_by(BetResult.settled_at.asc())
        )

        result = await db.execute(stmt)
        rows = result.all()

        if len(rows) < window_size:
            return []

        # Compute rolling average
        trend = []
        clv_values = [float(r.clv) for r in rows]
        dates = [r.settled_at for r in rows]

        for i in range(window_size - 1, len(clv_values)):
            window = clv_values[i - window_size + 1: i + 1]
            avg = sum(window) / len(window)
            trend.append({
                "date": dates[i].isoformat() if dates[i] else None,
                "rolling_avg_clv": round(avg, 4),
                "window_size": window_size,
                "sample_index": i + 1,
            })

        return trend

    # ---------------------------------------------------------------------------
    #  CLV alerts (edge decay detection)
    # ---------------------------------------------------------------------------

    async def check_clv_alerts(
        self,
        db: AsyncSession,
    ) -> List[CLVAlert]:
        """Check for CLV alerts across all bet types.

        Generates warnings when rolling CLV trends negative, indicating
        the model's edge may be decaying for certain bet types.
        """
        alerts = []

        # Check 14-day and 30-day windows
        for window_days in [14, 30]:
            by_type = await self.analyze_by_bet_type(db, window_days=window_days)

            for bt, data in by_type.items():
                if data["sample_size"] < 10:
                    continue

                avg_clv = data["avg_clv"]
                clv_hit_rate = data["clv_hit_rate"]

                # Critical: consistently negative CLV with low hit rate
                if avg_clv < -0.03 and clv_hit_rate < 0.40:
                    alerts.append(CLVAlert(
                        bet_type=bt,
                        window_days=window_days,
                        avg_clv=avg_clv,
                        sample_size=data["sample_size"],
                        severity="critical",
                        message=(
                            f"{bt.upper()} bets have avg CLV of {avg_clv:.1%} "
                            f"over last {window_days} days ({data['sample_size']} bets). "
                            f"Only {clv_hit_rate:.0%} beat the closing line. "
                            f"Consider disabling {bt} bets."
                        ),
                    ))
                # Warning: mildly negative CLV
                elif avg_clv < -0.01 and clv_hit_rate < 0.45:
                    alerts.append(CLVAlert(
                        bet_type=bt,
                        window_days=window_days,
                        avg_clv=avg_clv,
                        sample_size=data["sample_size"],
                        severity="warning",
                        message=(
                            f"{bt.upper()} bets showing negative CLV trend: "
                            f"{avg_clv:.1%} avg over {window_days} days "
                            f"({data['sample_size']} bets). Monitor closely."
                        ),
                    ))

        return alerts

    # ---------------------------------------------------------------------------
    #  Model trust scoring
    # ---------------------------------------------------------------------------

    async def compute_model_trust(
        self,
        db: AsyncSession,
    ) -> Dict[str, Dict[str, Any]]:
        """Score how much to trust the model for each bet type.

        Combines CLV performance, hit rate, and ROI into a single
        trust score (0-100). Used to weight bet-type confidence.

        Trust score formula:
            40% CLV score + 30% hit rate score + 30% ROI score
        """
        all_time = await self.analyze_by_bet_type(db)
        recent = await self.analyze_by_bet_type(db, window_days=30)

        trust_scores = {}
        for bt in set(list(all_time.keys()) + list(recent.keys())):
            at_data = all_time.get(bt, {})
            rc_data = recent.get(bt, {})

            # CLV score (0-100)
            avg_clv = at_data.get("avg_clv", 0)
            clv_score = max(0, min(100, 50 + avg_clv * 1000))  # ±5% CLV = 0-100

            # Hit rate score (0-100)
            hit_rate = at_data.get("hit_rate", 0.5)
            hr_score = max(0, min(100, (hit_rate - 0.45) * 500))  # 45-65% = 0-100

            # ROI score (0-100)
            roi = at_data.get("roi", 0)
            roi_score = max(0, min(100, 50 + roi * 500))  # ±10% ROI = 0-100

            trust = 0.40 * clv_score + 0.30 * hr_score + 0.30 * roi_score

            # Recent trend modifier: if recent performance diverges significantly
            # from all-time, adjust trust
            recent_clv = rc_data.get("avg_clv", avg_clv)
            if rc_data.get("sample_size", 0) >= 10:
                if recent_clv < avg_clv - 0.02:
                    trust *= 0.85  # Decaying edge
                elif recent_clv > avg_clv + 0.02:
                    trust *= 1.10  # Improving edge

            trust_scores[bt] = {
                "trust_score": round(min(100, max(0, trust)), 1),
                "clv_score": round(clv_score, 1),
                "hit_rate_score": round(hr_score, 1),
                "roi_score": round(roi_score, 1),
                "all_time_sample": at_data.get("sample_size", 0),
                "recent_sample": rc_data.get("sample_size", 0),
                "trend": (
                    "improving" if recent_clv > avg_clv + 0.01
                    else "declining" if recent_clv < avg_clv - 0.01
                    else "stable"
                ),
            }

        return trust_scores

    # ---------------------------------------------------------------------------
    #  Private helpers
    # ---------------------------------------------------------------------------

    @staticmethod
    def _edge_verdict(avg_clv: float, clv_hit_rate: float, sample_size: int) -> str:
        """Human-readable verdict on whether the model has real edge."""
        if sample_size < 20:
            return "insufficient_data"
        if avg_clv > 0.02 and clv_hit_rate > 0.55:
            return "strong_edge"
        if avg_clv > 0.01 and clv_hit_rate > 0.50:
            return "moderate_edge"
        if avg_clv > 0 and clv_hit_rate > 0.48:
            return "slight_edge"
        if avg_clv < -0.02 and clv_hit_rate < 0.40:
            return "no_edge"
        if avg_clv < -0.01:
            return "losing_edge"
        return "neutral"
