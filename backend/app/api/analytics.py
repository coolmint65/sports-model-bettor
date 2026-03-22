"""
Analytics API routes.

Provides endpoints for advanced model analytics including:
- CLV analysis and alerts
- Calibration diagnostics and visualization data
- Model trust scores by bet type
- Season phase information
- Kelly sizing configuration
- Steam detection status
"""

import logging
from datetime import date
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


# ---------------------------------------------------------------------------
#  CLV Analysis
# ---------------------------------------------------------------------------

@router.get("/clv/by-bet-type")
async def get_clv_by_bet_type(
    window_days: Optional[int] = Query(default=None, description="Rolling window in days"),
    sport: Optional[str] = Query(default=None, description="Filter by sport"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get CLV analysis broken down by bet type."""
    from app.analytics.clv_analyzer import CLVAnalyzer
    analyzer = CLVAnalyzer()
    return await analyzer.analyze_by_bet_type(session, window_days=window_days, sport=sport)


@router.get("/clv/by-confidence-tier")
async def get_clv_by_confidence_tier(
    window_days: Optional[int] = Query(default=None, description="Rolling window in days"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get CLV analysis broken down by model confidence tier."""
    from app.analytics.clv_analyzer import CLVAnalyzer
    analyzer = CLVAnalyzer()
    return await analyzer.analyze_by_confidence_tier(session, window_days=window_days)


@router.get("/clv/trend")
async def get_clv_trend(
    window_size: int = Query(default=50, description="Rolling window size"),
    bet_type: Optional[str] = Query(default=None, description="Filter by bet type"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get rolling CLV trend over time."""
    from app.analytics.clv_analyzer import CLVAnalyzer
    analyzer = CLVAnalyzer()
    trend = await analyzer.get_rolling_clv_trend(session, window_size=window_size, bet_type=bet_type)
    return {"trend": trend, "window_size": window_size, "bet_type": bet_type}


@router.get("/clv/alerts")
async def get_clv_alerts(
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Check for CLV alerts (edge decay detection)."""
    from app.analytics.clv_analyzer import CLVAnalyzer
    analyzer = CLVAnalyzer()
    alerts = await analyzer.check_clv_alerts(session)
    return {
        "alerts": [
            {
                "bet_type": a.bet_type,
                "window_days": a.window_days,
                "avg_clv": a.avg_clv,
                "sample_size": a.sample_size,
                "severity": a.severity,
                "message": a.message,
            }
            for a in alerts
        ],
        "alert_count": len(alerts),
    }


@router.get("/clv/model-trust")
async def get_model_trust(
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get model trust scores by bet type (based on CLV + hit rate + ROI)."""
    from app.analytics.clv_analyzer import CLVAnalyzer
    analyzer = CLVAnalyzer()
    return await analyzer.compute_model_trust(session)


# ---------------------------------------------------------------------------
#  Calibration Diagnostics
# ---------------------------------------------------------------------------

@router.get("/calibration")
async def get_calibration_data(
    bet_type: Optional[str] = Query(default=None, description="Filter by bet type"),
    sport: Optional[str] = Query(default=None, description="Filter by sport"),
    n_bins: int = Query(default=10, description="Number of calibration bins"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get calibration plot data for reliability diagram visualization."""
    from app.analytics.calibration_viz import CalibrationDiagnostics
    diag = CalibrationDiagnostics()
    return await diag.get_calibration_plot_data(session, bet_type=bet_type, sport=sport, n_bins=n_bins)


@router.get("/calibration/all")
async def get_all_calibration_data(
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get calibration data for all bet types plus overall."""
    from app.analytics.calibration_viz import CalibrationDiagnostics
    diag = CalibrationDiagnostics()
    return await diag.get_all_calibration_data(session)


@router.get("/calibration/sharpness")
async def get_sharpness_analysis(
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Analyze model prediction sharpness (decisiveness)."""
    from app.analytics.calibration_viz import CalibrationDiagnostics
    diag = CalibrationDiagnostics()
    return await diag.get_sharpness_analysis(session)


# ---------------------------------------------------------------------------
#  Season Phase
# ---------------------------------------------------------------------------

@router.get("/season-phase")
async def get_season_phase_info(
    sport: str = Query(default="nhl", description="Sport"),
    game_date: Optional[str] = Query(default=None, description="Date (YYYY-MM-DD)"),
) -> Dict[str, Any]:
    """Get current season phase and adaptive threshold information."""
    from app.analytics.season_phase import apply_season_thresholds, get_season_phase
    from app.config import settings

    d = date.fromisoformat(game_date) if game_date else date.today()
    phase = get_season_phase(sport, d)
    thresholds = apply_season_thresholds(settings.min_edge, settings.min_confidence, phase)

    return {
        "phase": phase.phase,
        "games_played": phase.games_played,
        "description": phase.description,
        "base_min_edge": settings.min_edge,
        "base_min_confidence": settings.min_confidence,
        "adjusted_min_edge": thresholds["min_edge"],
        "adjusted_min_confidence": thresholds["min_confidence"],
        "home_ice_adjustment": phase.home_ice_adjustment,
        "scoring_variance_factor": phase.scoring_variance_factor,
    }


# ---------------------------------------------------------------------------
#  Kelly Sizing / Bankroll
# ---------------------------------------------------------------------------

@router.get("/bankroll/status")
async def get_bankroll_status(
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """Get current bankroll status including exposure and circuit breakers."""
    from app.analytics.kelly import (
        check_drawdown_breaker,
        check_streak_gate,
        get_current_exposure,
        kelly_config,
    )

    drawdown_active = await check_drawdown_breaker(session)
    streak_active = await check_streak_gate(session)
    current_exposure = await get_current_exposure(session)

    return {
        "current_exposure": round(current_exposure, 1),
        "max_exposure": kelly_config.max_concurrent_exposure,
        "exposure_pct": round(current_exposure / kelly_config.max_concurrent_exposure * 100, 1),
        "drawdown_breaker_active": drawdown_active,
        "streak_gate_active": streak_active,
        "kelly_fraction": kelly_config.kelly_fraction,
        "unit_range": {
            "min": kelly_config.min_units,
            "max": kelly_config.max_units,
        },
    }
