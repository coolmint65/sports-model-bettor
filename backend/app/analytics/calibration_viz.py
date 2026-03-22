"""
Calibration visualization and diagnostics.

Generates calibration plot data (predicted probability vs actual win rate)
for verifying that the model's confidence scores are well-calibrated.
A perfectly calibrated model has all points on the y=x line.

Also includes:
- Brier score computation (proper scoring rule)
- Confidence-weighted reliability diagrams
- Per-sport and per-bet-type calibration breakdowns
"""

import logging
import math
from typing import Any, Dict, List, Optional

from sqlalchemy import Integer, and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.prediction import BetResult, Prediction

logger = logging.getLogger(__name__)


class CalibrationDiagnostics:
    """Generates calibration data for visualization and model validation."""

    async def get_calibration_plot_data(
        self,
        db: AsyncSession,
        bet_type: Optional[str] = None,
        sport: Optional[str] = None,
        n_bins: int = 10,
    ) -> Dict[str, Any]:
        """Generate calibration plot data (reliability diagram).

        Bins predictions by confidence and computes actual win rate per bin.
        Perfect calibration = every bin's actual rate matches its predicted rate.

        Args:
            db: Database session.
            bet_type: Filter by bet type (e.g., "ml", "total", "spread").
            sport: Filter by sport.
            n_bins: Number of confidence bins.

        Returns:
            Dict with bins, overall metrics, and calibration quality score.
        """
        filters = [
            Prediction.phase == "prematch",
        ]
        if bet_type:
            filters.append(Prediction.bet_type == bet_type)

        # Build query
        query = (
            select(Prediction.confidence, BetResult.was_correct, BetResult.profit_loss)
            .join(BetResult, BetResult.prediction_id == Prediction.id)
        )

        if sport:
            from app.models.game import Game
            query = query.join(Game, Prediction.game_id == Game.id)
            filters.append(Game.sport == sport)

        query = query.where(and_(*filters))
        result = await db.execute(query)
        rows = result.all()

        if not rows:
            return {
                "bins": [],
                "total_predictions": 0,
                "brier_score": None,
                "calibration_error": None,
                "calibration_quality": "no_data",
            }

        # Compute bin edges
        bin_width = 1.0 / n_bins
        bins_data = []

        for i in range(n_bins):
            bin_low = i * bin_width
            bin_high = (i + 1) * bin_width
            bin_center = (bin_low + bin_high) / 2

            # Filter rows in this bin
            bin_rows = [
                r for r in rows
                if bin_low <= r.confidence < bin_high
            ]
            # Include predictions at exactly 1.0 in the last bin
            if i == n_bins - 1:
                bin_rows.extend(r for r in rows if r.confidence == 1.0)

            n = len(bin_rows)
            if n == 0:
                continue

            wins = sum(1 for r in bin_rows if r.was_correct)
            actual_rate = wins / n
            avg_predicted = sum(r.confidence for r in bin_rows) / n
            avg_profit = sum(r.profit_loss for r in bin_rows) / n

            bins_data.append({
                "bin_low": round(bin_low, 3),
                "bin_high": round(bin_high, 3),
                "bin_center": round(bin_center, 3),
                "avg_predicted": round(avg_predicted, 4),
                "actual_rate": round(actual_rate, 4),
                "count": n,
                "wins": wins,
                "losses": n - wins,
                "avg_profit": round(avg_profit, 4),
                "calibration_error": round(abs(actual_rate - avg_predicted), 4),
            })

        # Overall metrics
        total = len(rows)
        total_wins = sum(1 for r in rows if r.was_correct)
        overall_hit_rate = total_wins / total if total > 0 else 0

        # Brier score (lower is better, 0 = perfect, 0.25 = random)
        brier = sum(
            (r.confidence - (1.0 if r.was_correct else 0.0)) ** 2
            for r in rows
        ) / total

        # Expected Calibration Error (ECE)
        # Weighted average of per-bin calibration errors
        ece = sum(
            b["count"] / total * b["calibration_error"]
            for b in bins_data
        ) if bins_data else 0

        # Maximum Calibration Error (MCE)
        mce = max(
            (b["calibration_error"] for b in bins_data), default=0
        )

        # Calibration quality assessment
        if ece < 0.02:
            quality = "excellent"
        elif ece < 0.05:
            quality = "good"
        elif ece < 0.10:
            quality = "fair"
        else:
            quality = "poor"

        return {
            "bins": bins_data,
            "total_predictions": total,
            "total_wins": total_wins,
            "overall_hit_rate": round(overall_hit_rate, 4),
            "brier_score": round(brier, 4),
            "expected_calibration_error": round(ece, 4),
            "max_calibration_error": round(mce, 4),
            "calibration_quality": quality,
            "filter": {
                "bet_type": bet_type,
                "sport": sport,
                "n_bins": n_bins,
            },
        }

    async def get_all_calibration_data(
        self,
        db: AsyncSession,
    ) -> Dict[str, Any]:
        """Generate calibration data for all bet types and overall.

        Convenience method that runs calibration analysis for each
        bet type plus an overall aggregate.
        """
        overall = await self.get_calibration_plot_data(db)

        by_bet_type = {}
        for bt in ["ml", "total", "spread"]:
            data = await self.get_calibration_plot_data(db, bet_type=bt)
            if data["total_predictions"] > 0:
                by_bet_type[bt] = data

        return {
            "overall": overall,
            "by_bet_type": by_bet_type,
        }

    async def get_sharpness_analysis(
        self,
        db: AsyncSession,
    ) -> Dict[str, Any]:
        """Analyze model sharpness (how decisive are the predictions?).

        A sharp model makes confident predictions (far from 50%).
        A blunt model makes wishy-washy predictions near 50%.
        You want sharp AND calibrated — sharp but miscalibrated is dangerous.
        """
        filters = [
            Prediction.phase == "prematch",
        ]

        stmt = (
            select(Prediction.confidence, BetResult.was_correct)
            .join(BetResult, BetResult.prediction_id == Prediction.id)
            .where(and_(*filters))
        )
        result = await db.execute(stmt)
        rows = result.all()

        if not rows:
            return {"total": 0}

        confidences = [r.confidence for r in rows]
        avg_confidence = sum(confidences) / len(confidences)
        avg_distance_from_50 = sum(abs(c - 0.5) for c in confidences) / len(confidences)

        # Distribution of confidence values
        tier_counts = {
            "50-55%": 0, "55-60%": 0, "60-65%": 0,
            "65-70%": 0, "70-75%": 0, "75%+": 0,
        }
        for c in confidences:
            if c < 0.55:
                tier_counts["50-55%"] += 1
            elif c < 0.60:
                tier_counts["55-60%"] += 1
            elif c < 0.65:
                tier_counts["60-65%"] += 1
            elif c < 0.70:
                tier_counts["65-70%"] += 1
            elif c < 0.75:
                tier_counts["70-75%"] += 1
            else:
                tier_counts["75%+"] += 1

        # Sharpness score (0-100)
        # avg_distance_from_50: 0 = completely blunt, 0.25 = maximally sharp
        sharpness = min(100, round(avg_distance_from_50 / 0.25 * 100, 1))

        return {
            "total_predictions": len(rows),
            "avg_confidence": round(avg_confidence, 4),
            "avg_distance_from_50": round(avg_distance_from_50, 4),
            "sharpness_score": sharpness,
            "confidence_distribution": tier_counts,
            "assessment": (
                "sharp" if sharpness > 40
                else "moderate" if sharpness > 20
                else "blunt"
            ),
        }
