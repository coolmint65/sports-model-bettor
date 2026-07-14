"""Tennis sport module (A6 first port).

Wraps the legacy ``engine.tennis_*`` files behind a Predictor adapter
so the unified protocol works for ATP / WTA matches. Actual file moves
(``engine/tennis_predict.py`` → ``engine/sports/tennis/predict.py``)
happen in a follow-on session — this adapter ships first because it's
the minimum-risk structural change that unblocks the orchestrator.

Public surface:
    TennisPredictor  — Predictor protocol implementation. Registers
                        itself for sport='tennis' on import.

GameContext usage for tennis:
    GameContext(sport='tennis', home=p1_name, away=p2_name,
                extras={'tour': 'atp'|'wta', 'surface': 'Hard|Clay|...',
                         'best_of': 3 | 5})

Tennis is unique among the sports here — no team abbreviations, just
player names. The protocol's ``home``/``away`` fields hold p1/p2
names; the adapter calls the legacy ``predict_match_by_name`` to
resolve and predict.
"""
from __future__ import annotations

import logging
from typing import Any

from ...predictor import GameContext, Prediction, register

logger = logging.getLogger(__name__)


class TennisPredictor:
    """Adapter around ``engine.tennis_predict.predict_match_by_name``.

    Returns Prediction with tennis-shaped fields:
      - ml_home / ml_away → p1 / p2 win probability
      - margin / total / spread_cover_prob — N/A for tennis (left None)
      - signals carries the rich tennis prediction dict (set probs,
        total games, surface adjustments, etc) so callers that need
        the per-market detail still get it
    """
    name = "tennis_factor"

    def predict(self, game: GameContext) -> Prediction:
        from ...tennis_predict import predict_match_by_name
        tour = (game.extras or {}).get("tour")
        if not tour:
            return Prediction(
                sport="tennis", home=game.home, away=game.away,
                source="factor",
                error="GameContext.extras must include 'tour' (atp/wta) "
                       "for tennis predictions",
            )
        try:
            raw = predict_match_by_name(
                tour=tour,
                p1_name=game.home,
                p2_name=game.away,
                surface=(game.extras or {}).get("surface", "Hard"),
                best_of=int((game.extras or {}).get("best_of", 3)),
            )
        except Exception as e:
            logger.warning("TennisPredictor failed for %s vs %s: %s",
                           game.home, game.away, e)
            return Prediction(
                sport="tennis", home=game.home, away=game.away,
                source="factor", error=str(e),
            )
        if not raw:
            return Prediction(
                sport="tennis", home=game.home, away=game.away,
                source="factor",
                error=f"player(s) not found ({game.home} / {game.away})",
            )
        # Map tennis prediction into the standard Prediction shape.
        # ml_home = p1 win prob; tennis has no spread/total margin
        # concept at the match level (sets/games are separate markets).
        p1_win = raw.get("p1_win_prob")
        return Prediction(
            sport="tennis",
            home=game.home,
            away=game.away,
            ml_home=p1_win,
            ml_away=(1 - p1_win) if p1_win is not None else None,
            margin=None,                      # tennis: not applicable
            total=raw.get("expected_total_games"),
            home_expected=raw.get("p1_expected_games"),
            away_expected=raw.get("p2_expected_games"),
            signals={
                "tennis_pred": raw,
                "tour": tour,
                "surface": (game.extras or {}).get("surface", "Hard"),
                "best_of": int((game.extras or {}).get("best_of", 3)),
            },
            reasoning=raw.get("reasoning") or [],
            source="factor",
        )


# Register on import — predictor.predict(GameContext(sport='tennis',...))
# now dispatches here without the caller doing setup.
register("tennis", TennisPredictor())
