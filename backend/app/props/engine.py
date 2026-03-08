"""PropEngine — orchestrates prediction across all registered prop types."""

import logging
from typing import Any, Dict, List

from app.props.types import PROP_REGISTRY

logger = logging.getLogger(__name__)


class PropEngine:
    """
    Run all registered prop types against a game's features.

    Each prop independently predicts → filters → maps odds.
    The engine collects results into a flat list of prediction dicts
    compatible with the main predict_all() output format.
    """

    def run(
        self,
        features: Dict[str, Any],
        odds_data: Dict[str, Any],
        matrix: List[List[float]],
        home_xg: float,
        away_xg: float,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        for prop_cls in PROP_REGISTRY:
            prop = prop_cls()
            try:
                candidates = prop.predict(features, matrix, home_xg, away_xg)
                if not candidates:
                    continue
                filtered = prop.filter(candidates)
                if not filtered:
                    continue
                with_odds = prop.map_odds(filtered, odds_data)

                for c in with_odds:
                    results.append({
                        "bet_type": prop.bet_type,
                        "prediction": c["side"],
                        "confidence": c["confidence"],
                        "probability": c["confidence"],
                        "implied_probability": c.get("implied_probability"),
                        "odds": c.get("odds"),
                        "edge": c.get("edge"),
                        "reasoning": c["reasoning"],
                    })
            except Exception:
                logger.exception("Prop %s failed", prop.bet_type)

        return results
