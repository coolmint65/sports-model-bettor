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

from sqlalchemy import and_, desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.analytics.features import FeatureEngine
from app.analytics.models import BettingModel
from app.config import settings
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
        self.model = BettingModel()

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

        # Fetch scheduled or in-progress games for the target date
        stmt = (
            select(Game)
            .where(
                and_(
                    Game.date == game_date,
                    Game.status.in_(["scheduled", "in_progress", "preview"]),
                )
            )
            .order_by(Game.start_time)
        )
        result = await db.execute(stmt)
        games = result.scalars().all()

        if not games:
            logger.info("No scheduled games found for %s", game_date)
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

        # Bet types that have real market odds from The Odds API.
        # Only these are eligible for "best bets" since we can calculate
        # true edge. Props (BTTS, first_goal, overtime, odd_even) don't
        # have market odds and would show inflated fake edges.
        ODDS_BET_TYPES = {"ml", "total", "spread"}

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
                    odds_val >= settings.best_bet_max_favorite  # e.g. >= -200
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

        # Sort by edge descending - this now reflects real value
        # since edge = model_confidence - market_implied_probability
        candidates.sort(key=lambda c: c["edge"], reverse=True)

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
        # Find ungraded predictions for completed games
        ungraded_stmt = (
            select(Prediction)
            .join(Game, Prediction.game_id == Game.id)
            .outerjoin(BetResult, BetResult.prediction_id == Prediction.id)
            .where(
                and_(
                    Game.status == "final",
                    BetResult.id.is_(None),
                )
            )
        )
        result = await db.execute(ungraded_stmt)
        ungraded = result.scalars().all()

        graded_count = 0
        for prediction in ungraded:
            try:
                graded = await self._grade_prediction(db, prediction)
                if graded:
                    graded_count += 1
            except Exception as e:
                logger.error(
                    "Failed to grade prediction %d: %s", prediction.id, e
                )

        if graded_count > 0:
            try:
                await db.flush()
            except Exception as e:
                logger.error("Failed to flush graded results: %s", e)

        # Compute aggregate statistics across ALL graded predictions
        stats_stmt = (
            select(
                func.count(BetResult.id).label("total"),
                func.sum(
                    func.cast(BetResult.was_correct, type_=func.literal(1).type)
                ).label("wins"),
                func.sum(BetResult.profit_loss).label("total_profit"),
            )
        )
        stats_result = await db.execute(stats_stmt)
        row = stats_result.one_or_none()

        total = row.total if row and row.total else 0
        wins = row.wins if row and row.wins else 0
        total_profit = row.total_profit if row and row.total_profit else 0.0
        losses = total - wins

        hit_rate = round(wins / total, 4) if total > 0 else 0.0
        roi = round(total_profit / total, 4) if total > 0 else 0.0

        # Breakdown by bet type
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

        return {
            "total_predictions": total,
            "newly_graded": graded_count,
            "wins": wins,
            "losses": losses,
            "hit_rate": hit_rate,
            "total_profit": round(total_profit, 2),
            "roi": roi,
            "by_bet_type": by_bet_type,
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

        return {
            "game_id": game.id,
            "game_info": game_info,
            "predictions": predictions,
            "top_pick": top_pick,
            "features_summary": {
                "home_form_5_wr": features["home_form_5"]["win_rate"],
                "away_form_5_wr": features["away_form_5"]["win_rate"],
                "home_season_gf": features["home_season"]["goals_for_pg"],
                "away_season_gf": features["away_season"]["goals_for_pg"],
                "h2h_games": features["h2h"]["games_found"],
                "home_goalie": features["home_goalie"]["goalie_name"],
                "away_goalie": features["away_goalie"]["goalie_name"],
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

        # Build reasoning text with odds info
        reasoning = bet.get("reasoning", "")
        odds = bet.get("odds")
        if odds is not None and reasoning:
            odds_str = f"+{int(odds)}" if odds > 0 else str(int(odds))
            reasoning = f"{reasoning} (Odds: {odds_str})"

        # Check for existing prediction
        existing_stmt = select(Prediction).where(
            and_(
                Prediction.game_id == game_id,
                Prediction.bet_type == bet_type,
                Prediction.prediction_value == prediction_value,
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
            existing.recommended = (
                existing.confidence >= settings.min_confidence
                and (existing.edge or 0) >= settings.min_edge
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
            recommended=confidence >= settings.min_confidence and (edge or 0) >= settings.min_edge,
            best_bet=is_best and (edge or 0) >= settings.best_bet_edge,
            reasoning=reasoning,
        )
        db.add(prediction)
        logger.info(
            "Persisted prediction: game=%d, type=%s, value=%s, conf=%.3f, edge=%s",
            game_id, bet_type, prediction_value, confidence, edge,
        )
        return prediction

    # ------------------------------------------------------------------ #
    #  Private: grade a single prediction                                 #
    # ------------------------------------------------------------------ #

    async def _grade_prediction(
        self,
        db: AsyncSession,
        prediction: Prediction,
    ) -> Optional[BetResult]:
        """
        Grade a prediction against actual game results.

        Determines if the prediction was correct, calculates profit/loss
        using a flat-bet model (+1 for win, -1 for loss), and persists
        a BetResult record.

        Returns:
            BetResult instance if graded, None if grading was not possible.
        """
        # Fetch the game with team relationships for spread grading
        game_stmt = (
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(Game.id == prediction.game_id)
        )
        game_result = await db.execute(game_stmt)
        game = game_result.scalars().first()

        if not game or game.status != "final":
            return None

        if game.home_score is None or game.away_score is None:
            return None

        actual_outcome = self._determine_actual_outcome(game, prediction)
        if actual_outcome is None:
            return None

        was_correct = self._check_prediction_correct(
            prediction.bet_type,
            prediction.prediction_value,
            actual_outcome,
            game,
        )

        # Flat-bet P/L: +1.0 for win, -1.0 for loss
        profit_loss = 1.0 if was_correct else -1.0

        bet_result = BetResult(
            prediction_id=prediction.id,
            actual_outcome=actual_outcome,
            was_correct=was_correct,
            profit_loss=profit_loss,
            settled_at=datetime.now(timezone.utc),
        )
        db.add(bet_result)

        logger.info(
            "Graded prediction %d: %s (correct=%s, P/L=%.1f)",
            prediction.id, actual_outcome, was_correct, profit_loss,
        )
        return bet_result

    # ------------------------------------------------------------------ #
    #  Private: determine actual game outcome for grading                  #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _determine_actual_outcome(
        game: Game,
        prediction: Prediction,
    ) -> Optional[str]:
        """
        Determine the actual outcome string for a prediction's bet type.

        Returns a string that can be compared against the prediction value.
        """
        home_score = game.home_score
        away_score = game.away_score
        total = home_score + away_score
        margin = home_score - away_score
        bet_type = prediction.bet_type

        if bet_type == "ml":
            return "home" if home_score > away_score else "away"

        elif bet_type == "total":
            # e.g., prediction_value = "over_5.5" or "under_5.5"
            return f"total_{total}"

        elif bet_type == "spread":
            return f"margin_{margin}"

        elif bet_type == "first_goal":
            # We need period data to determine this properly
            # Approximation: team that has more first-period goals
            return "home" if home_score > 0 else "away"

        elif bet_type == "both_score":
            return "yes" if home_score > 0 and away_score > 0 else "no"

        elif bet_type == "overtime":
            return "yes" if game.went_to_overtime else "no"

        elif bet_type == "odd_even":
            return "odd" if total % 2 == 1 else "even"

        elif bet_type == "period_winner":
            # Would need period_scores parsed
            return None

        elif bet_type == "period_total":
            return None

        return None

    @staticmethod
    def _check_prediction_correct(
        bet_type: str,
        prediction_value: str,
        actual_outcome: str,
        game: Game,
    ) -> bool:
        """
        Check whether a prediction was correct given the actual outcome.

        Handles the comparison logic for each bet type.
        """
        if bet_type == "ml":
            return prediction_value == actual_outcome

        elif bet_type == "total":
            # prediction_value e.g. "over_5.5", actual_outcome e.g. "total_6"
            try:
                actual_total = int(actual_outcome.split("_")[1])
                if "over" in prediction_value:
                    line = float(prediction_value.split("_")[1])
                    return actual_total > line
                elif "under" in prediction_value:
                    line = float(prediction_value.split("_")[1])
                    return actual_total < line
            except (ValueError, IndexError):
                pass
            return False

        elif bet_type == "spread":
            # prediction_value e.g. "EDM_-1.5" or "SJS_+1.5"
            # actual_outcome e.g. "margin_2" (home_score - away_score)
            try:
                actual_margin = int(actual_outcome.split("_")[1])
                # Determine if this prediction is for the home or away team
                # by matching the team abbreviation prefix
                pred_parts = prediction_value.split("_", 1)
                team_abbr = pred_parts[0] if len(pred_parts) > 1 else ""
                spread_str = pred_parts[1] if len(pred_parts) > 1 else prediction_value

                # Check if the predicted team is the home team
                home_team_obj = getattr(game, "home_team", None)
                home_abbr = getattr(home_team_obj, "abbreviation", "") if home_team_obj else ""
                is_home_team = (team_abbr == home_abbr) or ("home" in prediction_value.lower())

                spread_val = float(spread_str)
                if is_home_team:
                    # Home team spread: covers if margin > |spread| (for -1.5)
                    # or margin > -|spread| (for +1.5)
                    return actual_margin > -spread_val if spread_val > 0 else actual_margin > abs(spread_val)
                else:
                    # Away team spread: covers if -margin > |spread| (for -1.5)
                    # or -margin > -|spread| (for +1.5)
                    away_margin = -actual_margin
                    return away_margin > -spread_val if spread_val > 0 else away_margin > abs(spread_val)
            except (ValueError, IndexError):
                pass
            return False

        elif bet_type in ("first_goal", "both_score", "overtime", "odd_even"):
            return prediction_value == actual_outcome

        return False

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
