"""Football picks engine.

Routes ML / SPREAD / TOTAL candidates through ``engine.picks_core``
so the football leagues get the same juice-wall + odds-cap + edge-
floor + edge-ceiling gates the rest of the sports use.

Standard output dict shape::

    {"type", "pick", "raw_prob", "prob", "odds", "edge",
     "stake_units", "confidence", "side"}
"""
from __future__ import annotations

import logging
from typing import Any

from ..config import (
    NHL_JUICE_WALL,  # -200; matches the cross-sport baseline.
)
from ..picks_core import score_pick
from ._predict import predict_match

logger = logging.getLogger(__name__)


# Reuse the cross-sport juice wall. Football books are typically -110
# both sides on spread/total, so -200 catches the rare juicy ML
# favorites without blocking standard spread/total picks.
_JUICE_WALL = NHL_JUICE_WALL


def generate_picks(league: str, pred: dict, odds: dict | None,
                    *, game_id: str | None = None) -> list[Any]:
    """Return a list of edged picks for one game. Empty when no odds
    or no edge candidates clear the gates."""
    if not odds:
        return []
    picks: list[dict] = []

    matchup = f"{odds.get('away_abbr')}@{odds.get('home_abbr')}"

    # ── Moneyline ──
    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")
    if home_ml is not None and away_ml is not None:
        for side, prob, american in (
            ("home", pred["p_home"], home_ml),
            ("away", pred["p_away"], away_ml),
        ):
            label = (odds.get("home_abbr") if side == "home"
                      else odds.get("away_abbr"))
            scored = score_pick({
                "type": "ML",
                "pick": label,
                "raw_prob": prob,
                "odds": american,
                "matchup": matchup,
                "game_id": game_id,
            }, sport=f"football_{league}", juice_wall=_JUICE_WALL)
            if scored:
                # picks_core.score_pick drops anything not in its
                # canonical output dict — re-attach the matchup +
                # game_id so the tracker can persist them.
                scored["matchup"] = matchup
                scored["game_id"] = game_id
                scored["side"] = side
                picks.append(scored)

    # ── Spread ──
    spread = odds.get("home_spread_point")
    if (spread is not None
            and odds.get("home_spread_odds") is not None
            and odds.get("away_spread_odds") is not None):
        # Recompute cover probs against the actual posted line.
        from ._predict import predict_match as _pm
        with_spread = _pm(
            league, pred["home_team_id"], pred["away_team_id"],
            spread=spread,
        )
        for side, line, prob, american in (
            ("home",  spread, with_spread["p_home_cover"],
             odds["home_spread_odds"]),
            ("away", -spread, with_spread["p_away_cover"],
             odds["away_spread_odds"]),
        ):
            label_team = (odds.get("home_abbr") if side == "home"
                            else odds.get("away_abbr"))
            sign = "+" if line > 0 else ""
            scored = score_pick({
                "type": "SPREAD",
                "pick": f"{label_team} {sign}{line}",
                "raw_prob": prob,
                "odds": american,
                "matchup": matchup,
                "game_id": game_id,
            }, sport=f"football_{league}", juice_wall=_JUICE_WALL)
            if scored:
                # picks_core.score_pick drops anything not in its
                # canonical output dict — re-attach the matchup +
                # game_id so the tracker can persist them.
                scored["matchup"] = matchup
                scored["game_id"] = game_id
                scored["side"] = side
                picks.append(scored)

    # ── Total ──
    total = odds.get("over_under")
    if (total is not None
            and odds.get("over_odds") is not None
            and odds.get("under_odds") is not None):
        with_total = predict_match(
            league, pred["home_team_id"], pred["away_team_id"],
            total_line=total,
        )
        for side, prob, american in (
            ("over",  with_total["p_over"], odds["over_odds"]),
            ("under", with_total["p_under"], odds["under_odds"]),
        ):
            label = ("Over" if side == "over" else "Under") + f" {total}"
            scored = score_pick({
                "type": "TOTAL",
                "pick": label,
                "raw_prob": prob,
                "odds": american,
                "matchup": matchup,
                "game_id": game_id,
            }, sport=f"football_{league}", juice_wall=_JUICE_WALL)
            if scored:
                # picks_core.score_pick drops anything not in its
                # canonical output dict — re-attach the matchup +
                # game_id so the tracker can persist them.
                scored["matchup"] = matchup
                scored["game_id"] = game_id
                scored["side"] = side
                picks.append(scored)

    # Standard layout: one pick per game on the card. The picker keeps
    # all positive-edge candidates so the tracker can record them all,
    # but the headline is the top-edge candidate.
    picks.sort(key=lambda p: -(p.get("edge") or 0))
    return picks
