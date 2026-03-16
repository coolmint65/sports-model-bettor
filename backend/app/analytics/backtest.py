"""
Parameter backtesting framework for the Poisson prediction model.

Runs historical predictions against completed games to evaluate model
accuracy under different parameter configurations. Supports grid search
over key parameters and reports hit rate, ROI, and log-loss metrics.

Usage:
    python -m app.analytics.backtest          # run with defaults
    python -m app.analytics.backtest --grid   # run grid search
"""

import asyncio
import itertools
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.analytics.features import FeatureEngine
from app.analytics.models import BettingModel
from app.config import settings
from app.constants import GAME_FINAL_STATUSES
from app.database import get_session_context
from app.models.game import Game

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  Odds helpers                                                       #
# ------------------------------------------------------------------ #

def _compute_profit(odds: float, won: bool) -> float:
    """
    Calculate profit/loss for a 1-unit bet using American odds.

    Win at +150 → 1.50 units profit.
    Win at -200 → 0.50 units profit.
    Loss → -1.0 units (always lose the stake).
    """
    if won:
        if odds > 0:
            return odds / 100.0
        else:
            return 100.0 / abs(odds)
    return -1.0


def _extract_odds_for_prediction(
    bet_type: str,
    prediction_value: str,
    features: Dict[str, Any],
) -> Optional[float]:
    """
    Look up the American odds that correspond to a specific prediction
    from the pre-built features dict.

    Returns None when odds data is unavailable.
    """
    odds_data = features.get("odds")
    if not odds_data or not isinstance(odds_data, dict):
        return None

    home_abbr = features.get("home_team_abbr", "")

    if bet_type == "ml":
        is_home = prediction_value == home_abbr or prediction_value == "home"
        key = "home_moneyline" if is_home else "away_moneyline"
        return odds_data.get(key)

    if bet_type == "total":
        direction = prediction_value.split("_")[0] if "_" in prediction_value else prediction_value
        if direction == "over":
            return odds_data.get("over_price")
        elif direction == "under":
            return odds_data.get("under_price")
        return None

    if bet_type == "spread":
        # prediction_value is like "EDM_-1.5" or "home_-1.5"
        parts = prediction_value.split("_", 1)
        team_part = parts[0] if len(parts) > 1 else prediction_value
        is_home = (team_part == home_abbr) or ("home" in prediction_value.lower())
        key = "home_spread_price" if is_home else "away_spread_price"
        return odds_data.get(key)

    return None


# ------------------------------------------------------------------ #
#  Data structures                                                    #
# ------------------------------------------------------------------ #

@dataclass
class BacktestResult:
    """Results from a single backtest run."""
    params: Dict[str, float]
    total_predictions: int = 0
    correct_predictions: int = 0
    total_profit: float = 0.0
    flat_profit: float = 0.0
    bets_with_odds: int = 0
    log_loss_sum: float = 0.0
    ml_correct: int = 0
    ml_total: int = 0
    total_correct: int = 0
    total_total: int = 0
    spread_correct: int = 0
    spread_total: int = 0

    @property
    def hit_rate(self) -> float:
        return self.correct_predictions / self.total_predictions if self.total_predictions > 0 else 0.0

    @property
    def odds_roi(self) -> float:
        return self.total_profit / self.total_predictions if self.total_predictions > 0 else 0.0

    @property
    def flat_roi(self) -> float:
        return self.flat_profit / self.total_predictions if self.total_predictions > 0 else 0.0

    @property
    def avg_log_loss(self) -> float:
        return self.log_loss_sum / self.total_predictions if self.total_predictions > 0 else 999.0

    @property
    def ml_hit_rate(self) -> float:
        return self.ml_correct / self.ml_total if self.ml_total > 0 else 0.0

    @property
    def total_hit_rate(self) -> float:
        return self.total_correct / self.total_total if self.total_total > 0 else 0.0

    def summary(self) -> Dict[str, Any]:
        return {
            "params": self.params,
            "total_predictions": self.total_predictions,
            "hit_rate": round(self.hit_rate, 4),
            "odds_roi": round(self.odds_roi, 4),
            "flat_roi": round(self.flat_roi, 4),
            "total_profit": round(self.total_profit, 2),
            "flat_profit": round(self.flat_profit, 2),
            "bets_with_odds": self.bets_with_odds,
            "avg_log_loss": round(self.avg_log_loss, 4),
            "ml_hit_rate": round(self.ml_hit_rate, 4),
            "total_hit_rate": round(self.total_hit_rate, 4),
            "ml_total": self.ml_total,
            "total_total": self.total_total,
        }


# ------------------------------------------------------------------ #
#  Grid search parameter space                                        #
# ------------------------------------------------------------------ #

# Each key maps to a list of values to try.
# Keep the grid small to avoid combinatorial explosion.
DEFAULT_GRID = {
    "league_avg_goals": [3.00, 3.05, 3.10, 3.15],
    "home_ice_advantage": [0.10, 0.15, 0.20],
    "weight_form_5": [0.40, 0.50, 0.60],
    "defensive_regression": [0.50, 0.60, 0.70],
    "goalie_factor": [0.15, 0.20, 0.25],
    "mean_regression": [0.15, 0.20, 0.25],
    "scoring_correlation": [0.08, 0.12, 0.16],
}

# Focused grid for quick runs
QUICK_GRID = {
    "league_avg_goals": [3.00, 3.10],
    "home_ice_advantage": [0.12, 0.18],
    "defensive_regression": [0.55, 0.65],
    "mean_regression": [0.18, 0.22],
}


# ------------------------------------------------------------------ #
#  Backtester                                                         #
# ------------------------------------------------------------------ #

class Backtester:
    """Run historical backtests with configurable parameters."""

    def __init__(self) -> None:
        self.feature_engine = FeatureEngine()

    async def get_completed_games(
        self,
        db: AsyncSession,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
    ) -> List[Game]:
        """Fetch completed games with scores for backtesting."""
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=90)

        stmt = (
            select(Game)
            .where(
                and_(
                    func.lower(Game.status).in_(GAME_FINAL_STATUSES),
                    Game.date >= start_date,
                    Game.date <= end_date,
                    Game.home_score.isnot(None),
                    Game.away_score.isnot(None),
                )
            )
            .order_by(Game.date.desc())
            .limit(limit)
        )
        result = await db.execute(stmt)
        return list(result.scalars().all())

    async def run_backtest(
        self,
        db: AsyncSession,
        params: Dict[str, float],
        games: Optional[List[Game]] = None,
        features_cache: Optional[Dict[int, Dict]] = None,
        ml_model=None,
    ) -> BacktestResult:
        """
        Run a backtest with given parameters against historical games.

        Temporarily overrides model config, generates predictions for each
        game, then compares against actual outcomes.

        Args:
            features_cache: Pre-built features keyed by game ID. When
                provided, skips the expensive build_game_features DB
                queries (used by grid_search to avoid re-querying for
                each parameter combination).
            ml_model: Optional MLModel instance for ensemble backtesting.
        """
        import math

        if games is None:
            games = await self.get_completed_games(db)

        model = BettingModel(ml_model=ml_model)

        # Override settings temporarily via standard setattr (runs Pydantic validation)
        original_values = {}
        for key, val in params.items():
            if hasattr(settings.model, key):
                original_values[key] = getattr(settings.model, key)
                setattr(settings.model, key, val)

        result = BacktestResult(params=params)

        try:
            for game in games:
                try:
                    if features_cache and game.id in features_cache:
                        features = features_cache[game.id]
                    else:
                        features = await self.feature_engine.build_game_features(db, game.id)
                    predictions = await model.predict_all(features)

                    for pred in predictions:
                        bt = pred.get("bet_type")
                        pv = pred.get("prediction", "")
                        conf = pred.get("confidence", 0.5)

                        was_correct = self._check_outcome(
                            bt, pv, game, features
                        )
                        if was_correct is None:
                            continue

                        result.total_predictions += 1

                        # Flat P/L (old method, kept for comparison)
                        if was_correct:
                            result.correct_predictions += 1
                            result.flat_profit += 1.0
                        else:
                            result.flat_profit -= 1.0

                        # Odds-based P/L (real payouts)
                        bet_odds = _extract_odds_for_prediction(bt, pv, features)
                        if bet_odds is not None:
                            result.bets_with_odds += 1
                            result.total_profit += _compute_profit(bet_odds, was_correct)
                        else:
                            # Fall back to flat ±1 when odds unavailable
                            result.total_profit += 1.0 if was_correct else -1.0

                        # Log loss: -log(p) if correct, -log(1-p) if wrong
                        p = max(min(conf, 0.999), 0.001)
                        if was_correct:
                            result.log_loss_sum -= math.log(p)
                        else:
                            result.log_loss_sum -= math.log(1 - p)

                        # Per bet-type tracking
                        if bt == "ml":
                            result.ml_total += 1
                            if was_correct:
                                result.ml_correct += 1
                        elif bt == "total":
                            result.total_total += 1
                            if was_correct:
                                result.total_correct += 1
                        elif bt == "spread":
                            result.spread_total += 1
                            if was_correct:
                                result.spread_correct += 1

                except Exception as e:
                    logger.debug("Backtest failed for game %d: %s", game.id, e)
                    continue
        finally:
            # Restore original settings
            for key, val in original_values.items():
                setattr(settings.model, key, val)

        return result

    async def grid_search(
        self,
        db: AsyncSession,
        grid: Optional[Dict[str, List[float]]] = None,
        games: Optional[List[Game]] = None,
        metric: str = "log_loss",
    ) -> List[BacktestResult]:
        """
        Run a grid search over parameter combinations.

        Args:
            grid: Parameter grid (defaults to QUICK_GRID for speed).
            games: Preloaded games (fetched once, reused for each combo).
            metric: Optimization target: "log_loss", "hit_rate", or "roi".

        Returns:
            List of BacktestResult sorted by the chosen metric (best first).
        """
        if grid is None:
            grid = QUICK_GRID

        if games is None:
            games = await self.get_completed_games(db, limit=200)

        if not games:
            logger.warning("No completed games found for backtesting")
            return []

        logger.info(
            "Grid search: %d games, %d parameter combos",
            len(games),
            self._grid_size(grid),
        )

        # Pre-build features for all games once. Features are pure historical
        # data that don't change with tuning parameters, so caching avoids
        # re-querying ~27 DB calls per game per parameter combination.
        features_cache: Dict[int, Dict] = {}
        for game in games:
            try:
                features_cache[game.id] = await self.feature_engine.build_game_features(
                    db, game.id
                )
            except Exception as e:
                logger.debug("Feature build failed for game %d: %s", game.id, e)

        logger.info("Pre-built features for %d/%d games", len(features_cache), len(games))

        # Generate all combinations
        keys = list(grid.keys())
        values = list(grid.values())
        results: List[BacktestResult] = []

        for combo in itertools.product(*values):
            params = dict(zip(keys, combo))
            bt_result = await self.run_backtest(db, params, games, features_cache)
            results.append(bt_result)
            logger.info(
                "  params=%s -> hit=%.3f, roi=%.3f, ll=%.3f (%d preds)",
                {k: round(v, 3) for k, v in params.items()},
                bt_result.hit_rate,
                bt_result.odds_roi,
                bt_result.avg_log_loss,
                bt_result.total_predictions,
            )

        # Sort by metric
        if metric == "log_loss":
            results.sort(key=lambda r: r.avg_log_loss)
        elif metric == "roi":
            results.sort(key=lambda r: r.odds_roi, reverse=True)
        else:
            results.sort(key=lambda r: r.hit_rate, reverse=True)

        return results

    # ------------------------------------------------------------------ #
    #  Outcome checking                                                  #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _check_outcome(
        bet_type: str,
        prediction_value: str,
        game: Game,
        features: Dict[str, Any],
    ) -> Optional[bool]:
        """Check if a prediction was correct given actual game results."""
        from app.services.grading import check_outcome

        home_abbr = features.get("home_team_abbr", "")
        return check_outcome(bet_type, prediction_value, game, home_abbr)

    @staticmethod
    def _grid_size(grid: Dict[str, List[float]]) -> int:
        size = 1
        for vals in grid.values():
            size *= len(vals)
        return size


# ------------------------------------------------------------------ #
#  API endpoint support                                                #
# ------------------------------------------------------------------ #

async def run_backtest_api(
    db: AsyncSession,
    days_back: int = 90,
    limit: int = 200,
) -> Dict[str, Any]:
    """Run a backtest with current parameters (for API endpoint)."""
    bt = Backtester()
    end = date.today()
    start = end - timedelta(days=days_back)
    games = await bt.get_completed_games(db, start, end, limit)

    if not games:
        return {"error": "No completed games found", "games_checked": 0}

    result = await bt.run_backtest(db, {}, games)
    return {
        "games_checked": len(games),
        "date_range": f"{start} to {end}",
        **result.summary(),
    }


async def run_grid_search_api(
    db: AsyncSession,
    days_back: int = 90,
    limit: int = 200,
    quick: bool = True,
) -> Dict[str, Any]:
    """Run grid search (for API endpoint)."""
    bt = Backtester()
    end = date.today()
    start = end - timedelta(days=days_back)
    games = await bt.get_completed_games(db, start, end, limit)

    if not games:
        return {"error": "No completed games found", "games_checked": 0}

    grid = QUICK_GRID if quick else DEFAULT_GRID
    results = await bt.grid_search(db, grid, games)

    return {
        "games_checked": len(games),
        "date_range": f"{start} to {end}",
        "combinations_tested": bt._grid_size(grid),
        "best_params": results[0].summary() if results else None,
        "top_5": [r.summary() for r in results[:5]],
        "current_params": {
            k: getattr(settings.model, k)
            for k in grid.keys()
            if hasattr(settings.model, k)
        },
    }


async def run_ml_comparison_api(
    db: AsyncSession,
    days_back: int = 90,
    limit: int = 200,
) -> Dict[str, Any]:
    """Compare Poisson-only vs ML-blended vs ML-only backtests.

    Runs three backtests on the same games:
    1. Poisson-only (ml_blend_weight=0)
    2. Ensemble (ml_blend_weight from config, default 0.3)
    3. ML-heavy (ml_blend_weight=0.8)

    Returns side-by-side metrics for comparison.
    """
    from app.analytics.ml_model import MLModel

    bt = Backtester()
    end = date.today()
    start = end - timedelta(days=days_back)
    games = await bt.get_completed_games(db, start, end, limit)

    if not games:
        return {"error": "No completed games found", "games_checked": 0}

    # Pre-build feature cache
    features_cache: Dict[int, Dict] = {}
    for game in games:
        try:
            features_cache[game.id] = await bt.feature_engine.build_game_features(
                db, game.id
            )
        except Exception as e:
            logger.debug("Feature build failed for game %d: %s", game.id, e)

    # Load ML model
    ml_model = MLModel()
    ml_loaded = ml_model.load(settings.model.ml_model_path)

    # 1. Poisson-only
    original_blend = settings.model.ml_blend_weight
    settings.model.ml_blend_weight = 0.0
    poisson_result = await bt.run_backtest(db, {}, games, features_cache)

    results = {
        "games_checked": len(games),
        "date_range": f"{start} to {end}",
        "poisson_only": poisson_result.summary(),
    }

    if ml_loaded:
        # 2. Ensemble (configured blend weight)
        settings.model.ml_blend_weight = original_blend
        ensemble_result = await bt.run_backtest(db, {}, games, features_cache, ml_model=ml_model)
        results["ensemble"] = {
            **ensemble_result.summary(),
            "blend_weight": original_blend,
        }

        # 3. ML-heavy
        settings.model.ml_blend_weight = 0.8
        ml_heavy_result = await bt.run_backtest(db, {}, games, features_cache, ml_model=ml_model)
        results["ml_heavy"] = {
            **ml_heavy_result.summary(),
            "blend_weight": 0.8,
        }
    else:
        results["ml_status"] = "No trained ML model found. Run POST /api/ml/train first."

    # Restore original setting
    settings.model.ml_blend_weight = original_blend

    return results


# ------------------------------------------------------------------ #
#  CLI entry point                                                     #
# ------------------------------------------------------------------ #

async def _main():
    """CLI entry point for running backtests."""
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    from app.database import init_db

    await init_db()

    async with get_session_context() as db:
        if "--grid" in sys.argv:
            print("Running grid search...")
            result = await run_grid_search_api(db, days_back=90, quick=True)
        elif "--ml-compare" in sys.argv:
            print("Running ML comparison backtest...")
            result = await run_ml_comparison_api(db, days_back=90)
        else:
            print("Running backtest with current parameters...")
            result = await run_backtest_api(db, days_back=90)

        import json
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(_main())
