"""
Rolling calibration for model probabilities.

Instead of static shrinkage toward 50%, this module fits a calibration
curve from historical prediction results.  When enough settled prematch
predictions exist, raw model probabilities are mapped to empirically
observed win rates via binned interpolation.

Falls back to static shrinkage (BettingModel.calibrate_probability) when
insufficient data is available.
"""

import logging
from typing import Any, Dict, Optional

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.prediction import BetResult, Prediction

logger = logging.getLogger(__name__)

# Confidence bin edges: [0.50, 0.55, 0.60, 0.65, 0.70, 0.75]
# Bin centers:          [0.525, 0.575, 0.625, 0.675, 0.725, 0.775]
_BIN_EDGES = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75]
_BIN_CENTERS = [
    round((lo + hi) / 2, 4)
    for lo, hi in zip(_BIN_EDGES, _BIN_EDGES[1:])
]
# Last bin: 0.75+ (center = 0.775 as a reasonable representative)
_BIN_CENTERS.append(0.775)


class RollingCalibrator:
    """Empirically-fit calibration from historical prediction results.

    Replaces static shrinkage with a lookup table that maps model
    confidence bins to actual observed win rates.  Uses linear
    interpolation between the two nearest bins for smooth output.
    """

    def __init__(self) -> None:
        self._bins: Optional[Dict[float, float]] = None
        self._sample_size: int = 0
        self._fitted: bool = False

    @property
    def is_fitted(self) -> bool:
        return self._fitted

    # ------------------------------------------------------------------ #
    #  Fit from historical data                                           #
    # ------------------------------------------------------------------ #

    async def fit_from_history(
        self,
        db: AsyncSession,
        min_predictions: int = 100,
    ) -> Optional[Dict[str, Any]]:
        """Query settled prematch results and build a calibration map.

        Args:
            db: Async database session.
            min_predictions: Minimum number of settled prematch predictions
                required before fitting.  If fewer exist, returns None and
                the calibrator stays unfitted.

        Returns:
            A summary dict when fitted, or None if insufficient data.
        """
        # Count settled prematch predictions
        count_stmt = (
            select(func.count(BetResult.id))
            .join(Prediction, BetResult.prediction_id == Prediction.id)
            .where(Prediction.phase == "prematch")
        )
        count_result = await db.execute(count_stmt)
        total_settled = count_result.scalar() or 0

        if total_settled < min_predictions:
            logger.info(
                "Rolling calibrator: only %d settled predictions "
                "(need %d) — staying unfitted",
                total_settled,
                min_predictions,
            )
            return None

        # Fetch all settled prematch (confidence, was_correct) pairs
        data_stmt = (
            select(Prediction.confidence, BetResult.was_correct)
            .join(BetResult, BetResult.prediction_id == Prediction.id)
            .where(Prediction.phase == "prematch")
        )
        data_result = await db.execute(data_stmt)
        rows = data_result.all()

        # Group into bins
        bins: Dict[float, list] = {center: [] for center in _BIN_CENTERS}

        for confidence, was_correct in rows:
            # Determine which bin this prediction falls into
            assigned = False
            for i, edge in enumerate(_BIN_EDGES[:-1]):
                next_edge = _BIN_EDGES[i + 1]
                if edge <= confidence < next_edge:
                    bins[_BIN_CENTERS[i]].append(1 if was_correct else 0)
                    assigned = True
                    break
            if not assigned:
                # Falls into the last bin (0.75+) or below 0.50
                if confidence >= _BIN_EDGES[-1]:
                    bins[_BIN_CENTERS[-1]].append(1 if was_correct else 0)
                elif confidence < _BIN_EDGES[0]:
                    # Below 0.50 — assign to the lowest bin
                    bins[_BIN_CENTERS[0]].append(1 if was_correct else 0)

        # Compute actual win rate per bin
        calibration_map: Dict[float, float] = {}
        for center in _BIN_CENTERS:
            outcomes = bins[center]
            if len(outcomes) >= 5:  # need a minimum per bin for stability
                win_rate = sum(outcomes) / len(outcomes)
                calibration_map[center] = round(win_rate, 4)

        if len(calibration_map) < 2:
            logger.info(
                "Rolling calibrator: not enough bins populated "
                "(%d bins with 5+ predictions) — staying unfitted",
                len(calibration_map),
            )
            return None

        self._bins = calibration_map
        self._sample_size = total_settled
        self._fitted = True

        # Convert keys to strings for JSON-safe summary
        bins_summary = {str(k): v for k, v in calibration_map.items()}

        logger.info(
            "Rolling calibrator fitted: %d samples, %d bins: %s",
            total_settled,
            len(calibration_map),
            bins_summary,
        )

        return {
            "fitted": True,
            "bins": bins_summary,
            "sample_size": total_settled,
        }

    # ------------------------------------------------------------------ #
    #  Calibrate a single probability                                     #
    # ------------------------------------------------------------------ #

    def calibrate(self, raw_prob: float, bet_type: str = "ml") -> float:
        """Calibrate a raw model probability using the fitted map.

        If the calibrator is fitted, finds the two nearest bin centers
        and linearly interpolates to produce the calibrated probability.

        If not fitted, falls back to the static shrinkage method.

        Args:
            raw_prob: Raw model probability (0-1).
            bet_type: Bet type string (e.g. "ml", "spread", "total").

        Returns:
            Calibrated probability, clamped to [0.01, 0.99].
        """
        if not self._fitted or not self._bins:
            # Fall back to static calibration
            from app.analytics.models import BettingModel
            return BettingModel.calibrate_probability(raw_prob, bet_type)

        sorted_centers = sorted(self._bins.keys())

        # Clamp to the range of fitted bins
        if raw_prob <= sorted_centers[0]:
            calibrated = self._bins[sorted_centers[0]]
        elif raw_prob >= sorted_centers[-1]:
            calibrated = self._bins[sorted_centers[-1]]
        else:
            # Find the two nearest bin centers for interpolation
            lower_center = sorted_centers[0]
            upper_center = sorted_centers[-1]
            for i in range(len(sorted_centers) - 1):
                if sorted_centers[i] <= raw_prob <= sorted_centers[i + 1]:
                    lower_center = sorted_centers[i]
                    upper_center = sorted_centers[i + 1]
                    break

            # Linear interpolation
            span = upper_center - lower_center
            if span > 0:
                t = (raw_prob - lower_center) / span
                lower_rate = self._bins[lower_center]
                upper_rate = self._bins[upper_center]
                calibrated = lower_rate + t * (upper_rate - lower_rate)
            else:
                calibrated = self._bins[lower_center]

        return round(max(0.01, min(0.99, calibrated)), 4)

    # ------------------------------------------------------------------ #
    #  Refresh (re-fit from latest data)                                  #
    # ------------------------------------------------------------------ #

    async def refresh(self, db: AsyncSession) -> None:
        """Re-fit calibration from current historical data.

        Call this after settlement to keep the calibration map current.
        """
        logger.info("Refreshing rolling calibrator...")
        await self.fit_from_history(db)
