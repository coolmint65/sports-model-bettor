"""
Human-readable analysis signal generator.

Takes game features and prediction results and produces a list of
analysis bullets suitable for display in the frontend UI, similar
to "Buddy's Analysis" style betting factor breakdowns.

Each signal includes a category, human-readable text, which team
it favors, and a strength score used for ordering.
"""

import logging
from typing import Any, Dict, List, Optional

from app.config import settings

logger = logging.getLogger(__name__)
_mc = settings.model


class SignalGenerator:
    """Generates human-readable analysis signals from features and predictions."""

    def generate(
        self,
        features: Dict[str, Any],
        predictions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Generate all signals for a game.

        Returns a list of signal dicts sorted by strength descending.
        Only signals with strength >= 0.2 are included.
        """
        home_abbr = features.get("home_team_abbr", "HOM")
        away_abbr = features.get("away_team_abbr", "AWY")
        home_name = features.get("home_team_name", "Home")
        away_name = features.get("away_team_name", "Away")

        signals: List[Dict[str, Any]] = []

        signals.extend(self._form_signals(features, home_abbr, away_abbr, home_name, away_name))
        signals.extend(self._goalie_signals(features, home_abbr, away_abbr))
        signals.extend(self._starter_signals(features, home_abbr, away_abbr))
        signals.extend(self._ev_possession_signals(features, home_abbr, away_abbr))
        signals.extend(self._close_game_signals(features, home_abbr, away_abbr))
        signals.extend(self._schedule_signals(features, home_abbr, away_abbr))
        signals.extend(self._injury_signals(features, home_abbr, away_abbr))
        signals.extend(self._special_teams_signals(features, home_abbr, away_abbr))
        signals.extend(self._matchup_signals(features, home_abbr, away_abbr))
        signals.extend(self._market_signals(features, predictions, home_abbr, away_abbr))
        signals.extend(self._composite_signals(predictions, home_abbr, away_abbr))

        # Filter noise and sort by strength
        signals = [s for s in signals if s.get("strength", 0) >= 0.2]
        signals.sort(key=lambda s: s["strength"], reverse=True)

        return signals

    # ------------------------------------------------------------------ #
    #  Form signals                                                       #
    # ------------------------------------------------------------------ #

    def _form_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
        home_name: str,
        away_name: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_f5 = features.get("home_form_5", {})
        away_f5 = features.get("away_form_5", {})
        home_wr = home_f5.get("win_rate", 0.5)
        away_wr = away_f5.get("win_rate", 0.5)
        home_games = home_f5.get("games_found", 0)
        away_games = away_f5.get("games_found", 0)

        # Hot streak
        if home_wr >= 0.80 and home_games >= 5:
            w = int(home_wr * home_games)
            l = home_games - w
            signals.append(_signal(
                "form",
                f"{home_name} on hot streak ({w}-{l} L5)",
                "positive", home_abbr, 0.75,
                icon="fire",
            ))
        if away_wr >= 0.80 and away_games >= 5:
            w = int(away_wr * away_games)
            l = away_games - w
            signals.append(_signal(
                "form",
                f"{away_name} on hot streak ({w}-{l} L5)",
                "positive", away_abbr, 0.75,
                icon="fire",
            ))

        # Struggling
        if home_wr <= 0.20 and home_games >= 5:
            signals.append(_signal(
                "form", f"{home_name} struggling", "negative", home_abbr, 0.65,
            ))
        if away_wr <= 0.20 and away_games >= 5:
            signals.append(_signal(
                "form", "Opponent struggling", "negative", away_abbr, 0.65,
            ))

        return signals

    # ------------------------------------------------------------------ #
    #  Goalie signals                                                     #
    # ------------------------------------------------------------------ #

    def _goalie_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_g = features.get("home_goalie", {})
        away_g = features.get("away_goalie", {})
        home_tier = home_g.get("tier", "starter")
        away_tier = away_g.get("tier", "starter")
        home_rank = home_g.get("tier_rank", 2)
        away_rank = away_g.get("tier_rank", 2)

        # Tier mismatch
        if abs(home_rank - away_rank) >= 1:
            if home_rank > away_rank:
                better = home_abbr
                worse_tier = away_tier
                better_tier = home_tier
            else:
                better = away_abbr
                worse_tier = home_tier
                better_tier = away_tier

            signals.append(_signal(
                "goalie",
                f"Goalie tier mismatch: {worse_tier} vs {better_tier}",
                "positive", better,
                0.70 if abs(home_rank - away_rank) >= 2 else 0.50,
                icon="shield",
            ))

        # Elite goalie advantage
        for abbr, g in [(home_abbr, home_g), (away_abbr, away_g)]:
            if g.get("tier") == "elite":
                sv = g.get("season_save_pct", 0.900)
                signals.append(_signal(
                    "goalie",
                    f"Elite goalie: {g.get('goalie_name', 'Unknown')} (.{int(sv*1000)} SV%)",
                    "positive", abbr, 0.55,
                    icon="shield",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Starter confidence signals                                         #
    # ------------------------------------------------------------------ #

    def _starter_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        for abbr, key in [(home_abbr, "home_starter_status"), (away_abbr, "away_starter_status")]:
            status = features.get(key, {})
            conf = status.get("starter_confidence", 1.0)
            level = status.get("confidence_level", "high")

            if level != "high":
                reasons = status.get("confidence_reasons", [])
                reason_text = "; ".join(reasons) if reasons else "uncertain"
                signals.append(_signal(
                    "goalie",
                    f"Starter status unconfirmed; goalie edge discounted",
                    "neutral", abbr,
                    0.55 if level == "low" else 0.40,
                    icon="warning",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  5v5 Possession signals                                             #
    # ------------------------------------------------------------------ #

    def _ev_possession_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_ev = features.get("home_ev_possession", {})
        away_ev = features.get("away_ev_possession", {})
        threshold = _mc.ev_corsi_significance_threshold

        home_cf = home_ev.get("ev_cf_pct", 50.0)
        away_cf = away_ev.get("ev_cf_pct", 50.0)

        diff = abs(home_cf - away_cf)
        if diff >= threshold:
            if home_cf > away_cf:
                signals.append(_signal(
                    "possession",
                    f"Home team has significant 5v5 possession edge ({diff:.1f}%)",
                    "positive", home_abbr,
                    min(0.85, 0.50 + diff / 20.0),
                    icon="chart",
                ))
            else:
                signals.append(_signal(
                    "possession",
                    f"Away team has significant 5v5 possession edge ({diff:.1f}%)",
                    "positive", away_abbr,
                    min(0.85, 0.50 + diff / 20.0),
                    icon="chart",
                ))

        # General possession note
        for abbr, ev in [(home_abbr, home_ev), (away_abbr, away_ev)]:
            cf = ev.get("ev_cf_pct", 50.0)
            if cf >= 54.0 and ev.get("games_found", 0) >= _mc.ev_corsi_min_games:
                signals.append(_signal(
                    "possession",
                    f"Possession advantage in recent games",
                    "positive", abbr, 0.40,
                    icon="chart",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Close-game possession signals                                      #
    # ------------------------------------------------------------------ #

    def _close_game_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_close = features.get("home_close_possession", {})
        away_close = features.get("away_close_possession", {})

        for abbr, close, label in [
            (home_abbr, home_close, "Home team"),
            (away_abbr, away_close, "Away team"),
        ]:
            cf = close.get("close_cf_pct", 50.0)
            diff = close.get("close_cf_differential", 0.0)
            found = close.get("close_games_found", 0)

            if found >= _mc.close_game_min_games and abs(diff) >= 3.0:
                if diff > 0:
                    signals.append(_signal(
                        "possession",
                        f"{label} dominates in close games ({diff:.1f}% CF edge)",
                        "positive", abbr,
                        min(0.75, 0.45 + abs(diff) / 20.0),
                        icon="chart",
                    ))
                else:
                    signals.append(_signal(
                        "possession",
                        f"{label} struggles in close games ({diff:.1f}% CF)",
                        "negative", abbr,
                        min(0.60, 0.30 + abs(diff) / 20.0),
                    ))

        # Both 5v5 and close-game favor same team
        home_ev_cf = features.get("home_ev_possession", {}).get("ev_cf_pct", 50.0)
        away_ev_cf = features.get("away_ev_possession", {}).get("ev_cf_pct", 50.0)
        home_close_cf = home_close.get("close_cf_pct", 50.0)
        away_close_cf = away_close.get("close_cf_pct", 50.0)

        if home_ev_cf > away_ev_cf and home_close_cf > away_close_cf:
            if (home_ev_cf - away_ev_cf) >= 3.0:
                signals.append(_signal(
                    "possession",
                    "Both 5v5 and close-game possession favor pick",
                    "positive", home_abbr, 0.60,
                    icon="chart",
                ))
        elif away_ev_cf > home_ev_cf and away_close_cf > home_close_cf:
            if (away_ev_cf - home_ev_cf) >= 3.0:
                signals.append(_signal(
                    "possession",
                    "Both 5v5 and close-game possession favor pick",
                    "positive", away_abbr, 0.60,
                    icon="chart",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Schedule signals                                                   #
    # ------------------------------------------------------------------ #

    def _schedule_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_sched = features.get("home_schedule", {})
        away_sched = features.get("away_schedule", {})

        # Back-to-back
        if home_sched.get("is_back_to_back", False):
            signals.append(_signal(
                "schedule", f"{home_abbr} on B2B", "negative", home_abbr, 0.50,
            ))
        if away_sched.get("is_back_to_back", False):
            signals.append(_signal(
                "schedule", f"{away_abbr} on B2B", "negative", away_abbr, 0.50,
            ))

        # Rest advantage
        home_rest = home_sched.get("days_rest", 1)
        away_rest = away_sched.get("days_rest", 1)
        if home_rest >= 3 and away_rest <= 1:
            signals.append(_signal(
                "schedule",
                f"{home_abbr} well rested ({home_rest} days off)",
                "positive", home_abbr, 0.40,
            ))
        elif away_rest >= 3 and home_rest <= 1:
            signals.append(_signal(
                "schedule",
                f"{away_abbr} well rested ({away_rest} days off)",
                "positive", away_abbr, 0.40,
            ))

        # Lookahead / letdown
        if home_sched.get("is_lookahead", False):
            signals.append(_signal(
                "schedule", f"{home_abbr} in lookahead spot", "negative", home_abbr, 0.35,
            ))
        if away_sched.get("is_lookahead", False):
            signals.append(_signal(
                "schedule", f"{away_abbr} in lookahead spot", "negative", away_abbr, 0.35,
            ))

        # Road trip fatigue
        away_road = away_sched.get("consecutive_road_games", 0)
        if away_road > _mc.road_trip_fatigue_threshold:
            signals.append(_signal(
                "schedule",
                f"{away_abbr} on extended road trip ({away_road} games)",
                "negative", away_abbr, 0.35,
            ))

        return signals

    # ------------------------------------------------------------------ #
    #  Injury signals                                                     #
    # ------------------------------------------------------------------ #

    def _injury_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        for abbr, key in [(home_abbr, "home_injuries"), (away_abbr, "away_injuries")]:
            inj = features.get(key, {})
            count = inj.get("injured_count", 0)
            ppg = inj.get("total_missing_ppg", 0.0)
            xg_red = inj.get("xg_reduction", 0.0)

            if count >= 2 and ppg >= 1.0:
                signals.append(_signal(
                    "injuries",
                    f"{abbr} missing {count} regulars ({ppg:.1f} PPG absent)",
                    "negative", abbr,
                    min(0.70, 0.35 + xg_red * 2.0),
                ))

            if inj.get("goalie_injured", False):
                signals.append(_signal(
                    "injuries",
                    f"{abbr} starting goalie injured",
                    "negative", abbr, 0.65,
                    icon="warning",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Special teams signals                                              #
    # ------------------------------------------------------------------ #

    def _special_teams_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_st = features.get("home_special_teams", {})
        away_st = features.get("away_special_teams", {})

        home_pp = home_st.get("pp_pct", 20.0)
        home_pk = home_st.get("pk_pct", 80.0)
        away_pp = away_st.get("pp_pct", 20.0)
        away_pk = away_st.get("pk_pct", 80.0)

        # Elite PP vs poor PK
        if home_pp >= 25.0 and away_pk <= 77.0:
            signals.append(_signal(
                "special_teams",
                f"{home_abbr} elite PP vs poor PK",
                "positive", home_abbr, 0.55,
            ))
        if away_pp >= 25.0 and home_pk <= 77.0:
            signals.append(_signal(
                "special_teams",
                f"{away_abbr} elite PP vs poor PK",
                "positive", away_abbr, 0.55,
            ))

        return signals

    # ------------------------------------------------------------------ #
    #  Matchup signals                                                    #
    # ------------------------------------------------------------------ #

    def _matchup_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        h2h = features.get("h2h", {})

        if h2h.get("games_found", 0) >= 5:
            wr = h2h.get("team1_win_rate", 0.5)
            if wr >= 0.70:
                signals.append(_signal(
                    "matchup",
                    f"{home_abbr} dominates H2H ({wr:.0%} win rate)",
                    "positive", home_abbr, 0.40,
                ))
            elif wr <= 0.30:
                signals.append(_signal(
                    "matchup",
                    f"{away_abbr} dominates H2H ({1-wr:.0%} win rate)",
                    "positive", away_abbr, 0.40,
                ))

        # PDO regression
        for abbr, form_key in [(home_abbr, "home_form_10"), (away_abbr, "away_form_10")]:
            pdo = features.get(form_key, {}).get("pdo", 1.0)
            if pdo > 1.02:
                signals.append(_signal(
                    "matchup",
                    f"{abbr} due for regression (high PDO: {pdo:.3f})",
                    "negative", abbr, 0.35,
                    icon="chart",
                ))
            elif pdo < 0.98:
                signals.append(_signal(
                    "matchup",
                    f"{abbr} due for positive regression (low PDO: {pdo:.3f})",
                    "positive", abbr, 0.35,
                    icon="chart",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Market / model edge signals                                        #
    # ------------------------------------------------------------------ #

    def _market_signals(
        self,
        features: Dict[str, Any],
        predictions: List[Dict[str, Any]],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        for pred in predictions:
            if pred.get("bet_type") != "ml":
                continue
            conf = pred.get("confidence", 0.5)
            implied = pred.get("implied_probability")
            details = pred.get("details", {})
            home_xg = details.get("home_xg", 0)
            away_xg = details.get("away_xg", 0)

            if home_xg and away_xg:
                xg_diff = abs(home_xg - away_xg)
                if xg_diff >= 0.30:
                    favored = home_abbr if home_xg > away_xg else away_abbr
                    signals.append(_signal(
                        "model",
                        f"xG model projects {xg_diff:.2f} goal advantage",
                        "positive", favored,
                        min(0.70, 0.40 + xg_diff / 2.0),
                        icon="chart",
                    ))

            if implied is not None and conf > implied:
                edge = conf - implied
                if edge >= 0.05:
                    signals.append(_signal(
                        "model",
                        f"Model finds {edge:.1%} edge over market",
                        "positive", pred.get("prediction", ""),
                        min(0.80, 0.45 + edge * 3.0),
                    ))

        return signals

    # ------------------------------------------------------------------ #
    #  Composite edge signals                                             #
    # ------------------------------------------------------------------ #

    def _composite_signals(
        self,
        predictions: List[Dict[str, Any]],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        # Use the top ML prediction's composite edge
        for pred in predictions:
            if pred.get("bet_type") != "ml":
                continue
            comp = pred.get("composite_edge", {})
            score = comp.get("composite_score", 50.0)
            if score >= 60.0:
                signals.append(_signal(
                    "composite",
                    f"V2 composite edge: +{score:.1f}",
                    "positive", pred.get("prediction", ""),
                    min(0.85, 0.50 + (score - 50) / 50.0),
                    icon="chart",
                ))
            break

        return signals


# ------------------------------------------------------------------ #
#  Helper                                                             #
# ------------------------------------------------------------------ #

def _signal(
    category: str,
    text: str,
    impact: str,
    team: str,
    strength: float,
    icon: str = "",
) -> Dict[str, Any]:
    """Create a signal dict."""
    return {
        "category": category,
        "text": text,
        "impact": impact,
        "team": team,
        "strength": round(min(1.0, max(0.0, strength)), 3),
        "icon": icon,
    }
