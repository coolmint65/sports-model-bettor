"""
Prediction manager -- orchestrates feature extraction, model prediction,
persistence, and evaluation for sports betting predictions.

This is the main entry point for generating and managing predictions.
It wires together the FeatureEngine and BettingModel to produce
actionable betting recommendations.
"""

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.analytics.features import FeatureEngine
from app.analytics.models import BettingModel
from app.analytics.signals import SignalGenerator
from app.config import settings
from app.constants import GAME_PREDICTABLE_STATUSES, MARKET_BET_TYPES, composite_pick_score
from app.models.game import Game
from app.models.prediction import BetResult, Prediction

logger = logging.getLogger(__name__)


class PredictionManager:
    """
    Orchestrates the full prediction pipeline:
      1. Identify upcoming/today's games
      2. Build feature sets for each game
      3. Run the BettingModel on the features
      4. Score, rank, and persist predictions
      5. Evaluate past predictions against actual results
    """

    def __init__(self) -> None:
        self.feature_engine = FeatureEngine()
        self.ml_model = self._load_ml_model()
        self.model = BettingModel(ml_model=self.ml_model)

    @staticmethod
    def _load_ml_model():
        """Attempt to load a trained ML model from disk."""
        try:
            from app.analytics.ml_model import MLModel
            model = MLModel()
            if model.load(settings.model.ml_model_path):
                logger.info("ML model loaded for prediction blending")
                return model
        except Exception as e:
            logger.debug("ML model not available: %s", e)
        return None

    # ------------------------------------------------------------------ #
    #  Generate predictions for a date's games                            #
    # ------------------------------------------------------------------ #

    async def generate_predictions(
        self,
        db: AsyncSession,
        target_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate predictions for all games on a given date.

        Args:
            db: Async database session.
            target_date: Date string in YYYY-MM-DD format. Defaults to today.

        Returns:
            List of prediction dicts, each containing:
              - game_id, game_info (teams, date)
              - predictions: list of individual bet predictions
              - top_pick: the highest-confidence pick for the game
        """
        game_date = self._parse_date(target_date)
        logger.info("Generating predictions for %s", game_date)

        # Fetch all non-final games for the target date.
        # Uses the shared GAME_PREDICTABLE_STATUSES constant so that
        # every status that can be deleted is also regenerated — no
        # game falls through the cracks.
        stmt = (
            select(Game)
            .where(
                and_(
                    Game.date == game_date,
                    func.lower(Game.status).in_(GAME_PREDICTABLE_STATUSES),
                )
            )
            .order_by(Game.start_time)
        )
        result = await db.execute(stmt)
        games = result.scalars().all()

        if not games:
            logger.info("No predictable games found for %s", game_date)
            return []

        logger.info("Found %d games for %s", len(games), game_date)

        all_game_predictions: List[Dict[str, Any]] = []

        for game in games:
            try:
                game_preds = await self._predict_game(db, game)
                all_game_predictions.append(game_preds)
            except Exception as e:
                logger.error(
                    "Failed to generate predictions for game %d (%s): %s",
                    game.id,
                    game.external_id,
                    e,
                )

        # Sort games by the confidence of their top pick (descending)
        all_game_predictions.sort(
            key=lambda g: g.get("top_pick", {}).get("confidence", 0),
            reverse=True,
        )

        return all_game_predictions

    # ------------------------------------------------------------------ #
    #  Get best bets                                                      #
    # ------------------------------------------------------------------ #

    async def get_best_bets(
        self,
        db: AsyncSession,
        target_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Filter and rank predictions to identify the top 1-3 best bets.

        Criteria:
          - Confidence > settings.min_confidence (default 0.55)
          - Edge > settings.min_edge (default 0.03)
          - Sorted by edge descending
          - Top 3 returned, with #1 flagged as best_bet

        Args:
            db: Async database session.
            target_date: Date string in YYYY-MM-DD format. Defaults to today.

        Returns:
            List of best bet dicts with is_best_bet flag on the top pick.
        """
        all_predictions = await self.generate_predictions(db, target_date)

        if not all_predictions:
            return []

        # Bet types that have real market odds from sportsbooks.
        ODDS_BET_TYPES = set(MARKET_BET_TYPES)

        # Flatten all individual predictions across all games and persist ALL
        all_flat: List[Dict[str, Any]] = []
        candidates: List[Dict[str, Any]] = []
        for game_data in all_predictions:
            game_info = game_data.get("game_info", {})
            for pred in game_data.get("predictions", []):
                confidence = pred.get("confidence", 0)
                # Use real odds-based implied probability if available
                implied_prob = pred.get("implied_probability")
                odds = pred.get("odds")
                has_real_odds = implied_prob is not None

                # Only compute edge when we have real sportsbook odds.
                # Without real odds there is no market to compare against,
                # so edge is meaningless and should not be persisted.
                if has_real_odds:
                    edge = confidence - implied_prob
                    # Cap edge at 25% — anything higher signals a model/data issue
                    edge = min(edge, 0.25)
                else:
                    edge = None

                flat = {
                    "game_id": game_data.get("game_id"),
                    "game_info": game_info,
                    "bet_type": pred["bet_type"],
                    "prediction": pred["prediction"],
                    "confidence": confidence,
                    "probability": pred.get("probability", confidence),
                    "edge": round(edge, 4) if edge is not None else None,
                    "implied_probability": round(implied_prob, 4) if has_real_odds else None,
                    "odds": odds,
                    "reasoning": pred.get("reasoning", ""),
                    "is_best_bet": False,
                    "phase": "live" if (game_data.get("status") or "").lower() in ("in_progress", "live") else "prematch",
                }
                all_flat.append(flat)

                # Only consider bets where we have real market odds so we
                # can calculate genuine edge. Props without odds data would
                # get a fake 50% implied prob and dominate with inflated edges.
                #
                # Juice filter: exclude heavy favorites from best bets.
                # A -278 puck line is terrible value even if the model likes
                # it.  Best bets should have reasonable juice where the
                # payout justifies the risk.
                bet_type = pred["bet_type"]
                odds_val = odds or 0
                has_good_juice = (
                    odds_val >= settings.best_bet_max_favorite  # e.g. >= -170 (less steep)
                    or odds_val > 0  # all plus-money is fine
                )
                if (
                    bet_type in ODDS_BET_TYPES
                    and has_real_odds
                    and has_good_juice
                    and confidence >= settings.min_confidence
                    and edge >= settings.min_edge
                    and implied_prob < settings.best_bet_max_implied
                ):
                    candidates.append(flat)

        # Sort by composite score (confidence + edge + juice quality)
        candidates.sort(
            key=lambda c: composite_pick_score(
                c["confidence"], c["edge"], c["implied_probability"]
            ),
            reverse=True,
        )

        # Take top 3
        best_bets = candidates[:3]

        # Flag the #1 pick
        if best_bets:
            best_bets[0]["is_best_bet"] = True

        # Persist ALL predictions to database so they appear in game details
        for pred_data in all_flat:
            await self._persist_prediction(db, pred_data)

        try:
            await db.flush()
        except Exception as e:
            logger.error("Failed to flush predictions: %s", e)

        return best_bets

    # ------------------------------------------------------------------ #
    #  Evaluate past predictions                                          #
    # ------------------------------------------------------------------ #

    async def evaluate_predictions(
        self,
        db: AsyncSession,
    ) -> Dict[str, Any]:
        """
        Evaluate all past predictions against actual game results.

        Looks for predictions on completed games that have not yet been
        graded, grades them, and returns aggregate statistics.

        Returns:
            dict with: total_predictions, total_graded, wins, losses,
            pushes, hit_rate, roi, by_bet_type breakdown.
        """
        # Grade any unsettled predictions via the settlement service
        from app.services.settlement import settle_completed_games
        settlement = await settle_completed_games(db)
        graded_count = settlement["predictions_graded"]

        # Compute aggregate statistics across prematch graded predictions
        stats_stmt = (
            select(
                func.count(BetResult.id).label("total"),
                func.sum(
                    func.cast(BetResult.was_correct, type_=func.literal(1).type)
                ).label("wins"),
                func.sum(BetResult.profit_loss).label("total_profit"),
            )
            .join(Prediction, BetResult.prediction_id == Prediction.id)
            .where(Prediction.phase == "prematch")
        )
        stats_result = await db.execute(stats_stmt)
        row = stats_result.one_or_none()

        total = row.total if row and row.total else 0
        wins = row.wins if row and row.wins else 0
        total_profit = row.total_profit if row and row.total_profit else 0.0
        losses = total - wins

        hit_rate = round(wins / total, 4) if total > 0 else 0.0
        roi = round(total_profit / total, 4) if total > 0 else 0.0

        # Breakdown by bet type (prematch only)
        by_type_stmt = (
            select(
                Prediction.bet_type,
                func.count(BetResult.id).label("total"),
                func.sum(
                    func.cast(BetResult.was_correct, type_=func.literal(1).type)
                ).label("wins"),
                func.sum(BetResult.profit_loss).label("profit"),
            )
            .join(Prediction, BetResult.prediction_id == Prediction.id)
            .where(Prediction.phase == "prematch")
            .group_by(Prediction.bet_type)
        )
        by_type_result = await db.execute(by_type_stmt)
        by_type_rows = by_type_result.all()

        by_bet_type = {}
        for btr in by_type_rows:
            bt_total = btr.total or 0
            bt_wins = btr.wins or 0
            bt_profit = btr.profit or 0.0
            by_bet_type[btr.bet_type] = {
                "total": bt_total,
                "wins": bt_wins,
                "losses": bt_total - bt_wins,
                "hit_rate": round(bt_wins / bt_total, 4) if bt_total > 0 else 0.0,
                "profit": round(bt_profit, 2),
                "roi": round(bt_profit / bt_total, 4) if bt_total > 0 else 0.0,
            }

        # CLV aggregate: average CLV across prematch graded predictions with CLV data
        clv_stmt = (
            select(
                func.count(BetResult.id).label("clv_count"),
                func.avg(BetResult.clv).label("avg_clv"),
            )
            .join(Prediction, BetResult.prediction_id == Prediction.id)
            .where(
                and_(
                    BetResult.clv.isnot(None),
                    Prediction.phase == "prematch",
                )
            )
        )
        clv_result = await db.execute(clv_stmt)
        clv_row = clv_result.one_or_none()
        clv_count = clv_row.clv_count if clv_row and clv_row.clv_count else 0
        avg_clv = round(clv_row.avg_clv, 4) if clv_row and clv_row.avg_clv else None

        # Confidence-tier breakdown: how does ROI vary by model confidence?
        # Tiers: 50-60%, 60-70%, 70%+
        confidence_tiers = {}
        tier_boundaries = [
            ("50-60%", 0.50, 0.60),
            ("60-70%", 0.60, 0.70),
            ("70%+", 0.70, 1.01),
        ]
        for label, low, high in tier_boundaries:
            tier_stmt = (
                select(
                    func.count(BetResult.id).label("total"),
                    func.sum(
                        func.cast(BetResult.was_correct, type_=func.literal(1).type)
                    ).label("wins"),
                    func.sum(BetResult.profit_loss).label("profit"),
                )
                .join(Prediction, BetResult.prediction_id == Prediction.id)
                .where(
                    and_(
                        Prediction.confidence >= low,
                        Prediction.confidence < high,
                        Prediction.phase == "prematch",
                    )
                )
            )
            tier_result = await db.execute(tier_stmt)
            tier_row = tier_result.one_or_none()
            t_total = tier_row.total if tier_row and tier_row.total else 0
            t_wins = tier_row.wins if tier_row and tier_row.wins else 0
            t_profit = tier_row.profit if tier_row and tier_row.profit else 0.0
            confidence_tiers[label] = {
                "total": t_total,
                "wins": t_wins,
                "losses": t_total - t_wins,
                "hit_rate": round(t_wins / t_total, 4) if t_total > 0 else 0.0,
                "profit": round(t_profit, 2),
                "roi": round(t_profit / t_total, 4) if t_total > 0 else 0.0,
            }

        return {
            "total_predictions": total,
            "newly_graded": graded_count,
            "wins": wins,
            "losses": losses,
            "hit_rate": hit_rate,
            "total_profit": round(total_profit, 2),
            "roi": roi,
            "by_bet_type": by_bet_type,
            "by_confidence_tier": confidence_tiers,
            "clv": {
                "predictions_with_clv": clv_count,
                "avg_clv": avg_clv,
            },
        }

    # ------------------------------------------------------------------ #
    #  Private: predict a single game                                     #
    # ------------------------------------------------------------------ #

    async def _predict_game(
        self,
        db: AsyncSession,
        game: Game,
    ) -> Dict[str, Any]:
        """
        Build features and run all predictions for a single game.

        Returns a structured dict with game info and all predictions.
        """
        features = await self.feature_engine.build_game_features(db, game.id)

        predictions = await self.model.predict_all(features)

        # For live games, adjust predictions based on current score and
        # time remaining so confidence reflects the actual game state
        # rather than stale pre-game projections.
        if game.status and game.status.lower() in ("in_progress", "live") and (
            game.home_score is not None or game.away_score is not None
        ):
            live_state = {
                "home_score": game.home_score or 0,
                "away_score": game.away_score or 0,
                "period": getattr(game, "period", None) or 1,
                "clock": getattr(game, "clock", None),
                "period_type": getattr(game, "period_type", None),
            }
            predictions = self.model.adjust_for_live_state(
                predictions, features, live_state
            )
            logger.info(
                "Live-adjusted predictions for game %d (%s@%s): score %d-%d, "
                "period %s, remaining %.0f%%",
                game.id,
                features.get("away_team_abbr", "?"),
                features.get("home_team_abbr", "?"),
                live_state["away_score"],
                live_state["home_score"],
                live_state["period"],
                self.model._calc_remaining_fraction(live_state) * 100,
            )

        # Build game info summary
        game_info = {
            "game_id": game.id,
            "external_id": game.external_id,
            "game_date": str(game.date),
            "start_time": game.start_time.isoformat() if game.start_time else None,
            "home_team": features.get("home_team_name", "Unknown"),
            "away_team": features.get("away_team_name", "Unknown"),
            "home_abbr": features.get("home_team_abbr", "UNK"),
            "away_abbr": features.get("away_team_abbr", "UNK"),
            "venue": game.venue,
        }

        # Find top pick
        top_pick = predictions[0] if predictions else {}

        # Generate analysis signals
        signal_gen = SignalGenerator()
        signals = signal_gen.generate(features, predictions)

        # Enrich each prediction's reasoning with relevant signals.
        # Signals provide clean, Buddy-style bullets; the old reasoning
        # from _build_clean_reasons is kept as fallback only.
        home_abbr_lower = features.get("home_team_abbr", "").lower()
        away_abbr_lower = features.get("away_team_abbr", "").lower()

        for pred in predictions:
            pick_val = (pred.get("prediction") or "").lower()
            bt = pred.get("bet_type", "")
            # Determine opponent for this pick
            if pick_val == home_abbr_lower:
                opp_val = away_abbr_lower
            elif pick_val == away_abbr_lower:
                opp_val = home_abbr_lower
            else:
                opp_val = ""

            # Collect signals relevant to this prediction
            relevant = []
            for sig in signals:
                sig_team = (sig.get("team") or "").lower()
                impact = sig.get("impact", "")
                include = False
                if bt in ("ml", "spread"):
                    # Include: positive signals for picked team,
                    # negative signals about opponent, and game-level signals
                    if sig_team == pick_val and impact in ("positive", "neutral"):
                        include = True
                    elif sig_team == opp_val and impact == "negative":
                        include = True
                    elif sig_team == "":
                        include = True
                else:
                    include = True
                if include:
                    text = sig["text"]
                    # Embed team marker so the frontend can show a team logo
                    if sig.get("team"):
                        text = f"{{{{team:{sig['team']}}}}} {text}"
                    # Embed tooltip marker so the frontend can show an info icon
                    if sig.get("tooltip"):
                        text = f"{text} {{{{tooltip:{sig['tooltip']}}}}}"
                    relevant.append(text)
            if relevant:
                pred["reasoning"] = "; ".join(relevant[:8])

        return {
            "game_id": game.id,
            "status": game.status,
            "game_info": game_info,
            "predictions": predictions,
            "top_pick": top_pick,
            "signals": signals,
            "features_summary": {
                "home_form_5_wr": features["home_form_5"]["win_rate"],
                "away_form_5_wr": features["away_form_5"]["win_rate"],
                "home_season_gf": features["home_season"]["goals_for_pg"],
                "away_season_gf": features["away_season"]["goals_for_pg"],
                "h2h_games": features["h2h"]["games_found"],
                "home_goalie": features["home_goalie"]["goalie_name"],
                "away_goalie": features["away_goalie"]["goalie_name"],
                # Injury summaries
                "home_injured_count": features.get("home_injuries", {}).get("injured_count", 0),
                "away_injured_count": features.get("away_injuries", {}).get("injured_count", 0),
                "home_injury_xg_reduction": features.get("home_injuries", {}).get("xg_reduction", 0),
                "away_injury_xg_reduction": features.get("away_injuries", {}).get("xg_reduction", 0),
                # Schedule
                "home_b2b": features.get("home_schedule", {}).get("is_back_to_back", False),
                "away_b2b": features.get("away_schedule", {}).get("is_back_to_back", False),
                "home_days_rest": features.get("home_schedule", {}).get("days_rest", 0),
                "away_days_rest": features.get("away_schedule", {}).get("days_rest", 0),
                # Matchups
                "home_matchup_boost": features.get("home_player_matchup", {}).get("matchup_boost", 0),
                "away_matchup_boost": features.get("away_player_matchup", {}).get("matchup_boost", 0),
                "matchup_avg_total": features.get("team_matchup", {}).get("avg_total_goals"),
                # 5v5 possession (MoneyPuck)
                "home_ev_cf_pct": features.get("home_ev_possession", {}).get("ev_cf_pct"),
                "away_ev_cf_pct": features.get("away_ev_possession", {}).get("ev_cf_pct"),
                # Close-game possession
                "home_close_cf_pct": features.get("home_close_possession", {}).get("close_cf_pct"),
                "away_close_cf_pct": features.get("away_close_possession", {}).get("close_cf_pct"),
                # Goalie tiers
                "home_goalie_tier": features.get("home_goalie", {}).get("tier"),
                "away_goalie_tier": features.get("away_goalie", {}).get("tier"),
                # Starter confidence
                "home_starter_confidence": features.get("home_starter_status", {}).get("starter_confidence"),
                "away_starter_confidence": features.get("away_starter_status", {}).get("starter_confidence"),
                # Composite edge
                "composite_edge_score": top_pick.get("composite_edge", {}).get("composite_score"),
                "composite_edge_grade": top_pick.get("composite_edge", {}).get("composite_grade"),
            },
        }

    # ------------------------------------------------------------------ #
    #  Private: persist a prediction to the database                      #
    # ------------------------------------------------------------------ #

    async def _persist_prediction(
        self,
        db: AsyncSession,
        bet: Dict[str, Any],
    ) -> Optional[Prediction]:
        """
        Save a prediction to the database.

        Checks for duplicates before inserting. Returns the Prediction
        instance or None if it was a duplicate.
        """
        game_id = bet.get("game_id")
        bet_type = bet.get("bet_type")
        prediction_value = bet.get("prediction")

        if not game_id or not bet_type or not prediction_value:
            return None

        reasoning = bet.get("reasoning", "")

        # Check for existing prediction within the same phase.
        # Prematch and live predictions coexist as separate rows so
        # the /schedule/today bet tracker always has the original
        # prematch pick available even after live predictions appear.
        phase = bet.get("phase", "prematch")
        existing_stmt = select(Prediction).where(
            and_(
                Prediction.game_id == game_id,
                Prediction.bet_type == bet_type,
                Prediction.prediction_value == prediction_value,
                Prediction.phase == phase,
            )
        )
        existing_result = await db.execute(existing_stmt)
        existing = existing_result.scalars().first()

        if existing:
            # Update existing prediction with latest data
            existing.confidence = bet.get("confidence", existing.confidence)
            existing.odds_implied_prob = bet.get("implied_probability", existing.odds_implied_prob)
            existing.edge = bet.get("edge", existing.edge)
            existing.reasoning = reasoning or existing.reasoning
            impl = existing.odds_implied_prob
            existing.recommended = (
                existing.confidence >= settings.min_confidence
                and (existing.edge or 0) >= settings.min_edge
                and impl is not None
                and impl < settings.best_bet_max_implied
            )
            existing.best_bet = (
                bet.get("is_best_bet", False)
                and (existing.edge or 0) >= settings.best_bet_edge
            )
            logger.debug(
                "Updated existing prediction: game=%d, type=%s, value=%s",
                game_id, bet_type, prediction_value,
            )
            return existing

        # ---- Prematch lock: never add NEW prematch predictions to a game
        # that already has prematch predictions.  The original prematch set
        # is generated once and locked; later model runs (scheduler regen,
        # restarts, odds changes) must not sneak in extra bet types.
        if phase == "prematch":
            lock_stmt = select(func.count(Prediction.id)).where(
                Prediction.game_id == game_id,
                Prediction.phase == "prematch",
            )
            lock_result = await db.execute(lock_stmt)
            if (lock_result.scalar() or 0) > 0:
                logger.debug(
                    "Prematch locked — skipping new prediction: game=%d, type=%s, value=%s",
                    game_id, bet_type, prediction_value,
                )
                return None

        confidence = bet.get("confidence", 0.0)
        edge = bet.get("edge")
        implied_prob = bet.get("implied_probability")
        is_best = bet.get("is_best_bet", False)

        prediction = Prediction(
            game_id=game_id,
            bet_type=bet_type,
            prediction_value=prediction_value,
            confidence=confidence,
            odds_implied_prob=implied_prob,
            edge=edge,
            recommended=(
                confidence >= settings.min_confidence
                and (edge or 0) >= settings.min_edge
                and implied_prob is not None
                and implied_prob < settings.best_bet_max_implied
            ),
            best_bet=is_best and (edge or 0) >= settings.best_bet_edge,
            reasoning=reasoning,
            phase=phase,
        )
        db.add(prediction)
        logger.debug(
            "Persisted prediction: game=%d, type=%s, value=%s, conf=%.3f, edge=%s",
            game_id, bet_type, prediction_value, confidence, edge,
        )
        return prediction

    # ------------------------------------------------------------------ #
    #  Private: utility methods                                           #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _parse_date(date_str) -> date:
        """Parse a date string, date object, or return today's date."""
        if date_str is None:
            return date.today()
        if isinstance(date_str, date):
            return date_str
        try:
            return date.fromisoformat(str(date_str))
        except (ValueError, TypeError):
            logger.warning(
                "Invalid date format '%s', using today", date_str
            )
        return date.today()
