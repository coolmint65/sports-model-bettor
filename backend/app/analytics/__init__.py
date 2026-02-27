"""
Analytics and prediction engine for sports betting model.

This package provides:
- FeatureEngine: Extracts and engineers features from historical game data
- BettingModel: Statistical prediction models (Poisson-based) for various bet types
- PredictionManager: Orchestrates feature extraction, prediction, and evaluation
"""

from app.analytics.features import FeatureEngine
from app.analytics.models import BettingModel
from app.analytics.predictions import PredictionManager

__all__ = [
    "FeatureEngine",
    "BettingModel",
    "PredictionManager",
]
