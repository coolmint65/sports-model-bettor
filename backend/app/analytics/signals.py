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
        signals.extend(self._ev_possession_signals(features, home_abbr, away_abbr, home_name, away_name))
        signals.extend(self._close_game_signals(features, home_abbr, away_abbr, home_name, away_name))
        signals.extend(self._schedule_signals(features, home_abbr, away_abbr))
        signals.extend(self._injury_signals(features, home_abbr, away_abbr))
        signals.extend(self._special_teams_signals(features, home_abbr, away_abbr))
        signals.extend(self._matchup_signals(features, home_abbr, away_abbr))
        signals.extend(self._market_signals(features, predictions, home_abbr, away_abbr))
        signals.extend(self._composite_signals(predictions, home_abbr, away_abbr))
        signals.extend(self._high_danger_signals(features, home_abbr, away_abbr))
        signals.extend(self._splits_signals(features, home_abbr, away_abbr, home_name, away_name))
        signals.extend(self._divisional_signals(features))
        signals.extend(self._goalie_form_signals(features, home_abbr, away_abbr))
        signals.extend(self._goalie_vs_team_signals(features, home_abbr, away_abbr))
        signals.extend(self._goalie_venue_signals(features, home_abbr, away_abbr))
        signals.extend(self._goalie_workload_signals(features, home_abbr, away_abbr))
        signals.extend(self._pace_signals(features, home_abbr, away_abbr, home_name, away_name))
        signals.extend(self._score_close_signals(features, home_abbr, away_abbr, home_name, away_name))
        signals.extend(self._starter_confirmed_signals(features, home_abbr, away_abbr))
        signals.extend(self._scoring_mismatch_signals(features, home_abbr, away_abbr, home_name, away_name))
        signals.extend(self._shot_volume_signals(features, home_abbr, away_abbr))
        signals.extend(self._first_period_signals(features, home_abbr, away_abbr, home_name, away_name))
        signals.extend(self._overtime_signals(features, home_abbr, away_abbr, home_name, away_name))
        signals.extend(self._offensive_depth_signals(features, home_abbr, away_abbr, home_name, away_name))
        signals.extend(self._player_matchup_signals(features, home_abbr, away_abbr, home_name, away_name))
        signals.extend(self._pp_opportunity_signals(features, home_abbr, away_abbr))
        signals.extend(self._shot_quality_signals(features, home_abbr, away_abbr))
        signals.extend(self._line_stability_signals(features, home_abbr, away_abbr, home_name, away_name))
        signals.extend(self._h2h_recency_signals(features, home_abbr, away_abbr, home_name, away_name))
        signals.extend(self._consensus_signals(features, home_abbr, away_abbr))
        signals.extend(self._line_movement_signals(features, home_abbr, away_abbr, home_name, away_name))

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

        # Hot streak (4-1 or 5-0)
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

        # Strong recent form (3-2 or better)
        if 0.60 <= home_wr < 0.80 and home_games >= 5:
            w = int(home_wr * home_games)
            l = home_games - w
            signals.append(_signal(
                "form",
                f"{home_name} solid recent form ({w}-{l} L5)",
                "positive", home_abbr, 0.45,
                icon="chart",
            ))
        if 0.60 <= away_wr < 0.80 and away_games >= 5:
            w = int(away_wr * away_games)
            l = away_games - w
            signals.append(_signal(
                "form",
                f"{away_name} solid recent form ({w}-{l} L5)",
                "positive", away_abbr, 0.45,
                icon="chart",
            ))

        # Struggling (0-1 wins)
        if home_wr <= 0.20 and home_games >= 5:
            signals.append(_signal(
                "form", f"{home_name} struggling in recent games", "negative", home_abbr, 0.65,
            ))
        if away_wr <= 0.20 and away_games >= 5:
            signals.append(_signal(
                "form", f"{away_name} struggling in recent games", "negative", away_abbr, 0.65,
            ))

        # Cold stretch (1-4 or worse)
        if 0.20 < home_wr <= 0.40 and home_games >= 5:
            w = int(home_wr * home_games)
            l = home_games - w
            signals.append(_signal(
                "form",
                f"{home_name} in cold stretch ({w}-{l} L5)",
                "negative", home_abbr, 0.45,
            ))
        if 0.20 < away_wr <= 0.40 and away_games >= 5:
            w = int(away_wr * away_games)
            l = away_games - w
            signals.append(_signal(
                "form",
                f"{away_name} in cold stretch ({w}-{l} L5)",
                "negative", away_abbr, 0.45,
            ))

        # Form comparison when there's a notable difference
        if home_games >= 3 and away_games >= 3:
            diff = home_wr - away_wr
            if abs(diff) >= 0.20:
                better = home_name if diff > 0 else away_name
                better_abbr = home_abbr if diff > 0 else away_abbr
                signals.append(_signal(
                    "form",
                    f"{better} has stronger recent form",
                    "positive", better_abbr, 0.35,
                ))

        # L10 form context
        home_f10 = features.get("home_form_10", {})
        away_f10 = features.get("away_form_10", {})
        h10_wr = home_f10.get("win_rate", 0.5)
        a10_wr = away_f10.get("win_rate", 0.5)
        h10_games = home_f10.get("games_found", 0)
        a10_games = away_f10.get("games_found", 0)
        if h10_games >= 8 and a10_games >= 8 and abs(h10_wr - a10_wr) >= 0.25:
            better = home_name if h10_wr > a10_wr else away_name
            better_abbr = home_abbr if h10_wr > a10_wr else away_abbr
            h10_w = int(h10_wr * h10_games) if h10_wr > a10_wr else int(a10_wr * a10_games)
            h10_l = (h10_games if h10_wr > a10_wr else a10_games) - h10_w
            signals.append(_signal(
                "form",
                f"{better} stronger over last 10 games ({h10_w}-{h10_l})",
                "positive", better_abbr, 0.40,
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
                    f"Elite goalie: {g.get('goalie_name', 'Unknown')} ({abbr} Goalie) (.{int(sv*1000)} SV%)",
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
                    f"Starter status unconfirmed | goalie edge discounted",
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
        home_name: str = "Home",
        away_name: str = "Away",
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
                    f"{home_name} has significant 5v5 possession edge ({diff:.1f}%)",
                    "positive", home_abbr,
                    min(0.85, 0.50 + diff / 20.0),
                    icon="chart",
                    tooltip="5v5 possession is measured by Corsi For % (CF%) — the share of all shot attempts (goals, saves, misses, blocks) a team generates at even strength. Higher CF% = more time controlling the puck.",
                ))
            else:
                signals.append(_signal(
                    "possession",
                    f"{away_name} has significant 5v5 possession edge ({diff:.1f}%)",
                    "positive", away_abbr,
                    min(0.85, 0.50 + diff / 20.0),
                    icon="chart",
                    tooltip="5v5 possession is measured by Corsi For % (CF%) — the share of all shot attempts (goals, saves, misses, blocks) a team generates at even strength. Higher CF% = more time controlling the puck.",
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
        home_name: str = "Home",
        away_name: str = "Away",
    ) -> List[Dict[str, Any]]:
        signals = []
        home_close = features.get("home_close_possession", {})
        away_close = features.get("away_close_possession", {})

        for abbr, close, label in [
            (home_abbr, home_close, home_name),
            (away_abbr, away_close, away_name),
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
                        tooltip="CF (Corsi For) measures shot attempt share. Close-game CF% filters to games decided by 1 goal or OT, removing blowout noise for a more predictive possession signal.",
                    ))
                else:
                    signals.append(_signal(
                        "possession",
                        f"{label} struggles in close games ({diff:.1f}% CF)",
                        "negative", abbr,
                        min(0.60, 0.30 + abs(diff) / 20.0),
                        tooltip="CF (Corsi For) measures shot attempt share. Close-game CF% filters to games decided by 1 goal or OT, removing blowout noise for a more predictive possession signal.",
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
            if not away_sched.get("is_back_to_back", False):
                signals.append(_signal(
                    "schedule",
                    f"{home_abbr} on back-to-back (fatigue advantage for {away_abbr})",
                    "positive", away_abbr, 0.45,
                ))
        if away_sched.get("is_back_to_back", False):
            signals.append(_signal(
                "schedule", f"{away_abbr} on B2B", "negative", away_abbr, 0.50,
            ))
            if not home_sched.get("is_back_to_back", False):
                signals.append(_signal(
                    "schedule",
                    f"{away_abbr} on back-to-back (fatigue advantage for {home_abbr})",
                    "positive", home_abbr, 0.45,
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
    #  Travel signals                                                     #
    # ------------------------------------------------------------------ #

    def _travel_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals: List[Dict[str, Any]] = []
        travel = features.get("travel", {})
        if not travel:
            return signals

        distance = travel.get("distance_miles", 0)
        tz_delta = travel.get("timezone_delta", 0)
        fatigue = travel.get("fatigue_score", 0)
        is_cross_country = travel.get("is_cross_country", False)
        is_tz_mismatch = travel.get("is_timezone_mismatch", False)

        # Long road trip with timezone shift
        if is_cross_country and is_tz_mismatch:
            direction = "east" if tz_delta > 0 else "west"
            signals.append(_signal(
                "travel",
                f"Long road trip: {distance:,.0f} miles with "
                f"{abs(tz_delta)}-hour timezone shift ({direction})",
                "negative", away_abbr,
                min(0.85, 0.40 + fatigue * 0.45),
                icon="plane",
                tooltip=(
                    f"{away_abbr} traveling {distance:,.0f} miles across "
                    f"{abs(tz_delta)} time zones — fatigue score {fatigue:.2f}"
                ),
            ))
        elif is_cross_country:
            # Cross-country but same or close timezone (e.g. north-south)
            signals.append(_signal(
                "travel",
                f"Cross-country travel: {distance:,.0f} miles",
                "negative", away_abbr,
                min(0.60, 0.30 + fatigue * 0.30),
                icon="plane",
                tooltip=f"{away_abbr} traveling {distance:,.0f} miles",
            ))
        elif is_tz_mismatch:
            # Short distance but timezone mismatch (rare but possible)
            direction = "east" if tz_delta > 0 else "west"
            signals.append(_signal(
                "travel",
                f"{abs(tz_delta)}-hour timezone shift ({direction}) for {away_abbr}",
                "negative", away_abbr,
                min(0.50, 0.25 + fatigue * 0.25),
                icon="clock",
                tooltip=(
                    f"{away_abbr} crossing {abs(tz_delta)} time zones — "
                    f"body clock adjustment needed"
                ),
            ))

        # Home team benefits from minimal travel
        if fatigue >= 0.4:
            signals.append(_signal(
                "travel",
                f"{home_abbr} at home — no travel fatigue",
                "positive", home_abbr,
                min(0.45, 0.20 + fatigue * 0.25),
                icon="home",
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
        h2h_games = h2h.get("games_found", 0)

        if h2h_games >= 3:
            wr = h2h.get("team1_win_rate", 0.5)
            if wr >= 0.65:
                signals.append(_signal(
                    "matchup",
                    f"{home_abbr} dominates H2H ({wr:.0%} win rate in {h2h_games} meetings)",
                    "positive", home_abbr, 0.45,
                ))
            elif wr <= 0.35:
                signals.append(_signal(
                    "matchup",
                    f"{away_abbr} dominates H2H ({1-wr:.0%} win rate in {h2h_games} meetings)",
                    "positive", away_abbr, 0.45,
                ))
            elif h2h_games >= 5:
                # Evenly matched — still useful context
                signals.append(_signal(
                    "matchup",
                    f"Even H2H record ({h2h_games} meetings) | competitive matchup",
                    "neutral", "", 0.25,
                ))

        # H2H scoring context
        h2h_avg_total = h2h.get("avg_total_goals")
        if h2h_avg_total and h2h_games >= 3:
            if h2h_avg_total >= 6.5:
                signals.append(_signal(
                    "matchup",
                    f"H2H games average {h2h_avg_total:.1f} goals | high-scoring matchup",
                    "neutral", "", 0.35,
                ))
            elif h2h_avg_total <= 4.5:
                signals.append(_signal(
                    "matchup",
                    f"H2H games average {h2h_avg_total:.1f} goals | low-scoring matchup",
                    "neutral", "", 0.35,
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
                    tooltip="PDO = shooting% + save%. League average is ~1.000. A PDO above 1.020 suggests the team is running hot (lucky bounces) and likely to regress back toward average.",
                ))
            elif pdo < 0.98:
                signals.append(_signal(
                    "matchup",
                    f"{abbr} due for positive regression (low PDO: {pdo:.3f})",
                    "positive", abbr, 0.35,
                    icon="chart",
                    tooltip="PDO = shooting% + save%. League average is ~1.000. A PDO below 0.980 suggests the team has been unlucky and is due to bounce back toward average performance.",
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

        # Edge vs market odds
        for pred in predictions:
            bt = pred.get("bet_type", "")
            edge = pred.get("edge", 0) or 0
            if bt == "ml" and abs(edge) >= 0.03:
                team = pred.get("prediction", "")
                signals.append(_signal(
                    "market",
                    f"Model sees {edge*100:.1f}% edge vs market odds",
                    "positive" if edge > 0 else "neutral", team,
                    min(0.70, 0.35 + abs(edge) * 3),
                    icon="chart",
                    tooltip="Edge is the difference between the model's win probability and the sportsbook's implied probability. Positive edge means the model thinks the bet is undervalued.",
                ))
                break

        # Line movement signal
        lm = features.get("line_movement", {})
        sharp = lm.get("sharp_signal", "neutral")
        if sharp != "neutral":
            direction = "home" if sharp == "sharp_home" else "away"
            abbr = home_abbr if sharp == "sharp_home" else away_abbr
            signals.append(_signal(
                "market",
                f"Sharp money moving toward {direction} side",
                "positive", abbr, 0.55,
                icon="chart",
                tooltip="Sharp money refers to bets from professional/informed bettors. Significant line movement suggests informed money is being placed.",
            ))

        # Season record context
        home_season = features.get("home_season", {})
        away_season = features.get("away_season", {})
        home_gf = home_season.get("goals_for_pg", 0)
        away_gf = away_season.get("goals_for_pg", 0)
        home_ga = home_season.get("goals_against_pg", 0)
        away_ga = away_season.get("goals_against_pg", 0)

        if home_gf > 0 and away_gf > 0:
            home_diff = home_gf - home_ga
            away_diff = away_gf - away_ga
            if abs(home_diff - away_diff) >= 0.4:
                better_name = features.get("home_team_name", home_abbr) if home_diff > away_diff else features.get("away_team_name", away_abbr)
                better_abbr = home_abbr if home_diff > away_diff else away_abbr
                better_diff = max(home_diff, away_diff)
                signals.append(_signal(
                    "season",
                    f"{better_name} has better goal differential ({better_diff:+.2f}/game)",
                    "positive", better_abbr, 0.40,
                    icon="chart",
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
                    tooltip="V2 composite edge combines multiple model factors (possession, goaltending, form, matchup) into a single score. Higher values indicate a stronger overall edge.",
                ))
            break

        return signals

    # ------------------------------------------------------------------ #
    #  High-danger chances signals                                        #
    # ------------------------------------------------------------------ #

    def _high_danger_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_adv = features.get("home_advanced", {})
        away_adv = features.get("away_advanced", {})
        home_hd = home_adv.get("high_danger_proxy", 5.0)
        away_hd = away_adv.get("high_danger_proxy", 5.0)

        diff = abs(home_hd - away_hd)
        if diff >= 1.5 and home_adv.get("games_found", 0) >= 5 and away_adv.get("games_found", 0) >= 5:
            better = home_abbr if home_hd > away_hd else away_abbr
            signals.append(_signal(
                "quality",
                f"{diff:.1f} more high-danger chances per game",
                "positive", better,
                min(0.75, 0.40 + diff / 10.0),
                icon="chart",
                tooltip="High-danger chances are scoring opportunities from the slot or close to the net, where goals are most likely to be scored.",
            ))

        return signals

    # ------------------------------------------------------------------ #
    #  Home/away splits signals                                           #
    # ------------------------------------------------------------------ #

    def _splits_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
        home_name: str,
        away_name: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_splits = features.get("home_splits", {})
        away_splits = features.get("away_splits", {})

        home_wr = home_splits.get("win_rate", 0.5)
        away_wr = away_splits.get("win_rate", 0.5)
        home_games = home_splits.get("games_found", 0)
        away_games = away_splits.get("games_found", 0)

        # Strong home team
        if home_wr >= 0.70 and home_games >= 8:
            signals.append(_signal(
                "splits",
                f"{home_name} dominant at home ({home_wr:.0%} win rate)",
                "positive", home_abbr, 0.50,
            ))
        elif home_wr <= 0.35 and home_games >= 8:
            signals.append(_signal(
                "splits",
                f"{home_name} weak at home ({home_wr:.0%} win rate)",
                "negative", home_abbr, 0.40,
            ))

        # Away team road performance
        if away_wr >= 0.65 and away_games >= 8:
            signals.append(_signal(
                "splits",
                f"{away_name} strong on the road ({away_wr:.0%} win rate)",
                "positive", away_abbr, 0.45,
            ))
        elif away_wr <= 0.30 and away_games >= 8:
            signals.append(_signal(
                "splits",
                f"{away_name} struggles on the road ({away_wr:.0%} win rate)",
                "negative", away_abbr, 0.40,
            ))

        return signals

    # ------------------------------------------------------------------ #
    #  Divisional matchup signals                                         #
    # ------------------------------------------------------------------ #

    def _divisional_signals(
        self,
        features: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        signals = []

        if features.get("is_divisional", False):
            signals.append(_signal(
                "matchup",
                "Divisional matchup | historically tighter games",
                "neutral", "",
                0.30,
            ))

        return signals

    # ------------------------------------------------------------------ #
    #  Goalie recent form signals                                         #
    # ------------------------------------------------------------------ #

    def _goalie_form_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        for abbr, key in [(home_abbr, "home_goalie"), (away_abbr, "away_goalie")]:
            g = features.get(key, {})
            season_sv = g.get("season_save_pct", 0.900)
            last5_sv = g.get("last5_save_pct", 0.900)
            name = g.get("goalie_name", "Goalie")

            # Need meaningful season baseline
            if season_sv <= 0.0:
                continue

            diff = last5_sv - season_sv
            if diff >= 0.015:
                signals.append(_signal(
                    "goalie",
                    f"{name} ({abbr} Goalie) on hot streak (.{int(last5_sv*1000)} SV% L5 vs .{int(season_sv*1000)} SV% season)",
                    "positive", abbr,
                    min(0.65, 0.35 + diff * 10.0),
                    icon="fire",
                ))
            elif diff <= -0.015:
                signals.append(_signal(
                    "goalie",
                    f"{name} ({abbr} Goalie) struggling recently (.{int(last5_sv*1000)} SV% L5 vs .{int(season_sv*1000)} SV% season)",
                    "negative", abbr,
                    min(0.60, 0.35 + abs(diff) * 10.0),
                    icon="warning",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Goalie vs. specific opponent signals                               #
    # ------------------------------------------------------------------ #

    def _goalie_vs_team_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        for goalie_key, gvt_key, opp_abbr, own_abbr in [
            ("home_goalie", "home_goalie_vs_team", away_abbr, home_abbr),
            ("away_goalie", "away_goalie_vs_team", home_abbr, away_abbr),
        ]:
            gvt = features.get(gvt_key, {})
            goalie = features.get(goalie_key, {})
            if not gvt.get("significant", False):
                continue

            name = goalie.get("goalie_name", "Goalie")
            vs_sv = gvt.get("vs_save_pct", 0.900)
            season_sv = goalie.get("season_save_pct", 0.900)
            record = gvt.get("vs_record", "0-0-0")
            games = gvt.get("vs_games", 0)
            sv_diff = vs_sv - season_sv

            # Goalie struggles against this opponent (SV% notably below season avg)
            if sv_diff <= -0.015:
                signals.append(_signal(
                    "goalie",
                    (
                        f"{name} ({own_abbr} Goalie) struggles vs {opp_abbr}: "
                        f".{int(vs_sv*1000)} SV% in {games} GP ({record})"
                    ),
                    "positive", opp_abbr,
                    min(0.70, 0.40 + abs(sv_diff) * 10.0),
                    icon="shield",
                ))
            # Goalie dominates this opponent
            elif sv_diff >= 0.015:
                signals.append(_signal(
                    "goalie",
                    (
                        f"{name} ({own_abbr} Goalie) dominates vs {opp_abbr}: "
                        f".{int(vs_sv*1000)} SV% in {games} GP ({record})"
                    ),
                    "positive", own_abbr,
                    min(0.70, 0.40 + sv_diff * 10.0),
                    icon="shield",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Confirmed starter signals                                          #
    # ------------------------------------------------------------------ #

    def _starter_confirmed_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        for abbr, key in [(home_abbr, "home_goalie"), (away_abbr, "away_goalie")]:
            goalie = features.get(key, {})
            if goalie.get("starter_confirmed", False):
                name = goalie.get("goalie_name", "Unknown")
                signals.append(_signal(
                    "goalie",
                    f"{name} ({abbr} Goalie) confirmed as starter",
                    "neutral", abbr, 0.30,
                    icon="shield",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Goalie venue performance signals                                   #
    # ------------------------------------------------------------------ #

    def _goalie_venue_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        for goalie_key, venue_key, abbr, venue_label in [
            ("home_goalie", "home_goalie_venue", home_abbr, "at home"),
            ("away_goalie", "away_goalie_venue", away_abbr, "on the road"),
        ]:
            venue = features.get(venue_key, {})
            goalie = features.get(goalie_key, {})
            if not venue.get("significant", False):
                continue

            name = goalie.get("goalie_name", "Goalie")
            venue_sv = venue.get("venue_save_pct", 0.900)
            season_sv = goalie.get("season_save_pct", 0.900)
            record = venue.get("venue_record", "0-0")
            sv_diff = venue_sv - season_sv

            if sv_diff >= 0.012:
                signals.append(_signal(
                    "goalie",
                    f"{name} ({abbr} Goalie) excels {venue_label}: .{int(venue_sv*1000)} SV% ({record})",
                    "positive", abbr,
                    min(0.60, 0.35 + sv_diff * 10.0),
                    icon="shield",
                ))
            elif sv_diff <= -0.012:
                opp = away_abbr if abbr == home_abbr else home_abbr
                signals.append(_signal(
                    "goalie",
                    f"{name} ({abbr} Goalie) weaker {venue_label}: .{int(venue_sv*1000)} SV% ({record})",
                    "positive", opp,
                    min(0.55, 0.35 + abs(sv_diff) * 10.0),
                    icon="warning",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Goalie workload signals                                            #
    # ------------------------------------------------------------------ #

    def _goalie_workload_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        for goalie_key, wl_key, abbr in [
            ("home_goalie", "home_goalie_workload", home_abbr),
            ("away_goalie", "away_goalie_workload", away_abbr),
        ]:
            wl = features.get(wl_key, {})
            goalie = features.get(goalie_key, {})
            if not wl.get("heavy_workload", False):
                continue

            name = goalie.get("goalie_name", "Goalie")
            avg_shots = wl.get("avg_shots_per_start", 30.0)
            shots_3g = wl.get("recent_shots_3g", 0)
            opp = away_abbr if abbr == home_abbr else home_abbr
            signals.append(_signal(
                "goalie",
                f"{name} ({abbr} Goalie) heavy workload: {shots_3g} shots faced in last 3 starts ({avg_shots:.0f}/game avg)",
                "positive", opp,
                min(0.55, 0.35 + (avg_shots - 30) / 30.0),
                icon="warning",
            ))

        return signals

    # ------------------------------------------------------------------ #
    #  Pace / tempo matchup signals                                       #
    # ------------------------------------------------------------------ #

    def _pace_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
        home_name: str,
        away_name: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_pace = features.get("home_pace", {})
        away_pace = features.get("away_pace", {})

        if (home_pace.get("games_found", 0) < _mc.pace_min_games or
                away_pace.get("games_found", 0) < _mc.pace_min_games):
            return signals

        home_cat = home_pace.get("pace_category", "average")
        away_cat = away_pace.get("pace_category", "average")
        home_p = home_pace.get("pace", 60.0)
        away_p = away_pace.get("pace", 60.0)

        # Two fast teams = over lean
        if home_cat == "fast" and away_cat == "fast":
            signals.append(_signal(
                "pace",
                f"High-tempo matchup: both teams play fast ({home_p:.0f} + {away_p:.0f} shots/game)",
                "neutral", "",
                0.55,
                icon="fire",
                tooltip="Both teams generate and allow high shot volume. When two fast-paced teams meet, total goals tend to exceed individual averages.",
            ))
        # Two slow teams = under lean
        elif home_cat == "slow" and away_cat == "slow":
            signals.append(_signal(
                "pace",
                f"Low-tempo matchup: both teams play slow ({home_p:.0f} + {away_p:.0f} shots/game)",
                "neutral", "",
                0.50,
                icon="chart",
                tooltip="Both teams suppress shot volume. When two defensive-minded teams meet, total goals tend to come in under individual averages.",
            ))
        # One fast, one slow = pace mismatch
        elif (home_cat == "fast" and away_cat == "slow") or (home_cat == "slow" and away_cat == "fast"):
            fast_team = home_name if home_cat == "fast" else away_name
            fast_abbr = home_abbr if home_cat == "fast" else away_abbr
            signals.append(_signal(
                "pace",
                f"Pace mismatch: {fast_team} plays fast vs slower opponent",
                "positive", fast_abbr,
                0.35,
                icon="chart",
            ))

        return signals

    # ------------------------------------------------------------------ #
    #  Score-close performance signals                                    #
    # ------------------------------------------------------------------ #

    def _score_close_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
        home_name: str,
        away_name: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        home_sc = features.get("home_score_close", {})
        away_sc = features.get("away_score_close", {})

        for abbr, name, sc in [
            (home_abbr, home_name, home_sc),
            (away_abbr, away_name, away_sc),
        ]:
            if sc.get("close_games_found", 0) < _mc.score_close_min_games:
                continue

            close_gf = sc.get("close_gf_pg", 3.0)
            close_ga = sc.get("close_ga_pg", 3.0)
            close_diff = close_gf - close_ga

            if close_diff >= 0.5:
                signals.append(_signal(
                    "matchup",
                    f"{name} outscores opponents in close games ({close_gf:.1f}-{close_ga:.1f} GF/GA)",
                    "positive", abbr,
                    min(0.55, 0.35 + close_diff / 3.0),
                    icon="chart",
                    tooltip="Score-close games (decided by 1 goal or OT) filter out blowout noise. Teams that outperform in tight games demonstrate true competitive quality.",
                ))
            elif close_diff <= -0.5:
                signals.append(_signal(
                    "matchup",
                    f"{name} outscored in close games ({close_gf:.1f}-{close_ga:.1f} GF/GA)",
                    "negative", abbr,
                    min(0.50, 0.30 + abs(close_diff) / 3.0),
                    icon="warning",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Scoring mismatch signals                                           #
    # ------------------------------------------------------------------ #

    def _scoring_mismatch_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
        home_name: str,
        away_name: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_season = features.get("home_season", {})
        away_season = features.get("away_season", {})

        home_gf = home_season.get("goals_for_pg", 3.0)
        home_ga = home_season.get("goals_against_pg", 3.0)
        away_gf = away_season.get("goals_for_pg", 3.0)
        away_ga = away_season.get("goals_against_pg", 3.0)

        # Home offense vs away defense
        home_off_edge = home_gf - away_ga
        if home_off_edge >= 0.5:
            signals.append(_signal(
                "matchup",
                f"{home_name} offense ({home_gf:.1f} GF/g) vs weak defense ({away_ga:.1f} GA/g)",
                "positive", home_abbr,
                min(0.65, 0.35 + home_off_edge / 3.0),
                icon="chart",
            ))

        # Away offense vs home defense
        away_off_edge = away_gf - home_ga
        if away_off_edge >= 0.5:
            signals.append(_signal(
                "matchup",
                f"{away_name} offense ({away_gf:.1f} GF/g) vs weak defense ({home_ga:.1f} GA/g)",
                "positive", away_abbr,
                min(0.65, 0.35 + away_off_edge / 3.0),
                icon="chart",
            ))

        return signals

    # ------------------------------------------------------------------ #
    #  Shot volume signals                                                #
    # ------------------------------------------------------------------ #

    def _shot_volume_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_adv = features.get("home_advanced", {})
        away_adv = features.get("away_advanced", {})

        home_share = home_adv.get("shot_share", 50.0)
        away_share = away_adv.get("shot_share", 50.0)

        diff = abs(home_share - away_share)
        if diff >= 6.0 and home_adv.get("games_found", 0) >= 5 and away_adv.get("games_found", 0) >= 5:
            better = home_abbr if home_share > away_share else away_abbr
            better_share = max(home_share, away_share)
            signals.append(_signal(
                "possession",
                f"Significant shot volume advantage ({better_share:.0f}% shot share)",
                "positive", better,
                min(0.60, 0.35 + diff / 20.0),
                icon="chart",
            ))

        return signals

    # ------------------------------------------------------------------ #
    #  First period tendency signals                                      #
    # ------------------------------------------------------------------ #

    def _first_period_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
        home_name: str,
        away_name: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        for abbr, name, key in [
            (home_abbr, home_name, "home_periods"),
            (away_abbr, away_name, "away_periods"),
        ]:
            periods = features.get(key, {})
            p1_rate = periods.get("first_period_scoring_rate", 0.0)
            p1_for = periods.get("avg_p1_for", 0.0)
            games = periods.get("games_found", 0)

            if games < 10:
                continue

            if p1_rate >= 0.70:
                signals.append(_signal(
                    "periods",
                    f"{name} scores first in {p1_rate:.0%} of games",
                    "positive", abbr, 0.40,
                ))
            elif p1_rate <= 0.30 and games >= 10:
                signals.append(_signal(
                    "periods",
                    f"{name} slow starters | score first in only {p1_rate:.0%} of games",
                    "negative", abbr, 0.35,
                ))

            if p1_for >= 1.2:
                signals.append(_signal(
                    "periods",
                    f"{name} strong first period scoring ({p1_for:.1f} goals/game P1)",
                    "positive", abbr, 0.35,
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Overtime tendency signals                                          #
    # ------------------------------------------------------------------ #

    def _overtime_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
        home_name: str,
        away_name: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_ot = features.get("home_ot", {})
        away_ot = features.get("away_ot", {})

        # Both teams frequently go to OT
        home_ot_pct = home_ot.get("ot_pct", 0.0)
        away_ot_pct = away_ot.get("ot_pct", 0.0)
        home_games = home_ot.get("games_found", 0)
        away_games = away_ot.get("games_found", 0)

        if (home_ot_pct >= 0.30 and away_ot_pct >= 0.30
                and home_games >= 15 and away_games >= 15):
            avg_pct = (home_ot_pct + away_ot_pct) / 2
            signals.append(_signal(
                "overtime",
                f"Both teams frequently go to OT ({avg_pct:.0%} avg OT rate)",
                "neutral", "",
                0.40,
            ))

        for abbr, name, ot in [
            (home_abbr, home_name, home_ot),
            (away_abbr, away_name, away_ot),
        ]:
            ot_pct = ot.get("ot_pct", 0.0)
            ot_wr = ot.get("ot_win_rate", 0.5)
            games = ot.get("games_found", 0)

            if games < 15:
                continue

            if ot_wr >= 0.70 and ot_pct >= 0.20:
                signals.append(_signal(
                    "overtime",
                    f"{name} strong OT closer ({ot_wr:.0%} OT win rate)",
                    "positive", abbr, 0.35,
                ))
            elif ot_wr <= 0.30 and ot_pct >= 0.20:
                signals.append(_signal(
                    "overtime",
                    f"{name} struggles in OT ({ot_wr:.0%} OT win rate)",
                    "negative", abbr, 0.30,
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Offensive depth signals                                            #
    # ------------------------------------------------------------------ #

    def _offensive_depth_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
        home_name: str,
        away_name: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        for abbr, name, key in [
            (home_abbr, home_name, "home_skaters"),
            (away_abbr, away_name, "away_skaters"),
        ]:
            skaters = features.get(key, {})
            top6 = skaters.get("top6_fwd_ppg", 0.0)
            star = skaters.get("star_ppg", 0.0)
            team_avg = skaters.get("team_skater_ppg", 0.0)
            games = skaters.get("games_found", 0)

            if games < 5:
                continue

            # Deep scoring: high top-6 production spread evenly
            if top6 >= 0.80 and star <= top6 * 2.0:
                signals.append(_signal(
                    "talent",
                    f"{name} has deep forward scoring ({top6:.2f} PPG top-6 avg)",
                    "positive", abbr, 0.40,
                ))

            # Star-dependent: single player dominates
            if star >= 1.5 and team_avg > 0 and star / team_avg >= 3.0:
                signals.append(_signal(
                    "talent",
                    f"{name} heavily reliant on top scorer ({star:.2f} PPG)",
                    "negative", abbr, 0.35,
                    icon="warning",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Player matchup advantage signals                                   #
    # ------------------------------------------------------------------ #

    def _player_matchup_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
        home_name: str,
        away_name: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        for abbr, name, key in [
            (home_abbr, home_name, "home_player_matchup"),
            (away_abbr, away_name, "away_player_matchup"),
        ]:
            matchup = features.get(key, {})
            boost = matchup.get("matchup_boost", 0.0)
            players = matchup.get("players_with_data", 0)
            games = matchup.get("games_analyzed", 0)

            if players < 3 or games < 5:
                continue

            if boost >= 0.15:
                signals.append(_signal(
                    "matchup",
                    f"{name} key players perform well vs this opponent (+{boost:.2f} PPG)",
                    "positive", abbr,
                    min(0.55, 0.30 + boost),
                    icon="fire",
                ))
            elif boost <= -0.15:
                signals.append(_signal(
                    "matchup",
                    f"{name} key players underperform vs this opponent ({boost:.2f} PPG)",
                    "negative", abbr,
                    min(0.50, 0.30 + abs(boost)),
                    icon="warning",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Feature #6: PP opportunity rate vs opponent                        #
    # ------------------------------------------------------------------ #

    def _pp_opportunity_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_pp = features.get("home_pp_opportunity", {})
        away_pp = features.get("away_pp_opportunity", {})

        if home_pp.get("games_found", 0) >= 5:
            home_net = home_pp.get("net_pp_impact", 0.0)
            if home_net > 0.15:
                signals.append(_signal(
                    "special_teams",
                    f"{home_abbr} PP opportunity edge: undisciplined opponent "
                    f"gives {home_pp.get('pp_opportunities_for', 0):.1f} PP/game "
                    f"vs {home_pp.get('team_pp_pct', 20):.0f}% PP conversion",
                    "positive", home_abbr,
                    min(0.7, 0.3 + home_net),
                    icon="zap",
                ))
            elif home_net < -0.15:
                signals.append(_signal(
                    "special_teams",
                    f"{home_abbr} faces PP disadvantage: draws {home_pp.get('pp_opportunities_against', 0):.1f} "
                    f"penalties/game vs opponent's {home_pp.get('opponent_pp_pct', 20):.0f}% PP",
                    "negative", home_abbr,
                    min(0.7, 0.3 + abs(home_net)),
                    icon="alert-triangle",
                ))

        if away_pp.get("games_found", 0) >= 5:
            away_net = away_pp.get("net_pp_impact", 0.0)
            if away_net > 0.15:
                signals.append(_signal(
                    "special_teams",
                    f"{away_abbr} PP opportunity edge: undisciplined opponent "
                    f"gives {away_pp.get('pp_opportunities_for', 0):.1f} PP/game "
                    f"vs {away_pp.get('team_pp_pct', 20):.0f}% PP conversion",
                    "positive", away_abbr,
                    min(0.7, 0.3 + away_net),
                    icon="zap",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Feature #7: Shooting quality against                               #
    # ------------------------------------------------------------------ #

    def _shot_quality_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        home_sq = features.get("home_shot_quality", {})
        away_sq = features.get("away_shot_quality", {})

        for sq, abbr in [(home_sq, home_abbr), (away_sq, away_abbr)]:
            if sq.get("games_found", 0) < 8:
                continue
            gsae_pg = sq.get("goals_saved_above_expected", 0.0) / max(sq["games_found"], 1)
            sqi = sq.get("shot_quality_index", 1.0)
            if gsae_pg > 0.15 and sqi > 1.05:
                signals.append(_signal(
                    "goalie",
                    f"{abbr} defense stops quality shots: {gsae_pg:+.2f} GSAE/game "
                    f"despite facing {sqi:.0%} shot quality index",
                    "positive", abbr,
                    min(0.7, 0.3 + gsae_pg),
                    icon="shield",
                    tooltip="Goals Saved Above Expected per game, adjusted for shot difficulty",
                ))
            elif gsae_pg < -0.15:
                signals.append(_signal(
                    "goalie",
                    f"{abbr} defense leaking goals: {gsae_pg:+.2f} GSAE/game "
                    f"(allowing more than expected)",
                    "negative", abbr,
                    min(0.7, 0.3 + abs(gsae_pg)),
                    icon="alert-circle",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Feature #9: Line stability signals                                 #
    # ------------------------------------------------------------------ #

    def _line_stability_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
        home_name: str,
        away_name: str,
    ) -> List[Dict[str, Any]]:
        signals = []

        for ls, abbr, name in [
            (features.get("home_line_stability", {}), home_abbr, home_name),
            (features.get("away_line_stability", {}), away_abbr, away_name),
        ]:
            if ls.get("games_found", 0) < 5:
                continue
            stability = ls.get("top6_stability", 1.0)
            if stability < 0.65:
                signals.append(_signal(
                    "lineup",
                    f"{name} forward lines disrupted: top-6 stability at {stability:.0%} "
                    f"(chemistry concerns)",
                    "negative", abbr,
                    min(0.6, 0.3 + (0.75 - stability)),
                    icon="users",
                ))
            elif stability > 0.92:
                signals.append(_signal(
                    "lineup",
                    f"{name} forward lines locked in: top-6 stability at {stability:.0%}",
                    "positive", abbr,
                    0.25,
                    icon="users",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Feature #11: Recency-weighted H2H                                  #
    # ------------------------------------------------------------------ #

    def _h2h_recency_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
        home_name: str,
        away_name: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        h2h_w = features.get("h2h_weighted", {})
        if h2h_w.get("games_found", 0) < 3:
            return signals

        shift = h2h_w.get("recency_shift", 0.0)
        # A shift of 0.15+ means recent games are very different from the full H2H
        if abs(shift) >= 0.10:
            trending_team = home_name if shift > 0 else away_name
            trending_abbr = home_abbr if shift > 0 else away_abbr
            signals.append(_signal(
                "matchup",
                f"H2H trend shift: {trending_team} dominating recent meetings "
                f"(recency-weighted WR {shift:+.0%} vs raw H2H)",
                "positive", trending_abbr,
                min(0.6, 0.3 + abs(shift)),
                icon="trending-up",
                tooltip="Recent head-to-head results diverge from historical average",
            ))

        return signals

    # ------------------------------------------------------------------ #
    #  Feature #13: Consensus line signals                                #
    # ------------------------------------------------------------------ #

    def _consensus_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
    ) -> List[Dict[str, Any]]:
        signals = []
        consensus = features.get("consensus_line", {})
        odds = features.get("odds", {})

        if consensus.get("sources_count", 0) < 2:
            return signals

        # Check if the single-book line differs significantly from consensus
        single_home_ml = odds.get("home_moneyline")
        consensus_home_ml = consensus.get("consensus_home_ml")

        if single_home_ml and consensus_home_ml:
            diff = abs(single_home_ml - consensus_home_ml)
            if diff >= 15:
                outlier_side = "higher" if single_home_ml > consensus_home_ml else "lower"
                signals.append(_signal(
                    "market",
                    f"Line outlier: {home_abbr} ML {single_home_ml:+.0f} vs consensus "
                    f"{consensus_home_ml:+.0f} ({consensus.get('sources_count', 0)} books) — "
                    f"possible value if book is {outlier_side}",
                    "neutral", home_abbr,
                    min(0.5, 0.25 + diff / 100),
                    icon="bar-chart",
                    tooltip=f"Consensus across {consensus.get('sources_count', 0)} sportsbooks",
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Line movement signals                                              #
    # ------------------------------------------------------------------ #

    def _line_movement_signals(
        self,
        features: Dict[str, Any],
        home_abbr: str,
        away_abbr: str,
        home_name: str,
        away_name: str,
    ) -> List[Dict[str, Any]]:
        """Generate signals from opening-vs-current line movement."""
        signals: List[Dict[str, Any]] = []
        lm = features.get("line_movement", {})

        snapshots = lm.get("snapshots_count", 0)
        if snapshots < 2:
            return signals

        ml_shift = lm.get("ml_implied_shift", 0.0) or 0.0
        ml_movement = lm.get("ml_movement", 0.0) or 0.0
        ou_movement = lm.get("ou_movement", 0.0) or 0.0
        spread_movement = lm.get("spread_movement", 0.0) or 0.0
        is_reverse = lm.get("is_reverse_line_movement", False)
        open_ml = lm.get("home_ml_open")
        curr_ml = lm.get("home_ml_current")

        # --- Moneyline movement signal ---
        if abs(ml_shift) >= _mc.line_movement_min_shift and open_ml is not None and curr_ml is not None:
            if ml_movement < 0:
                # Home becoming more favored
                favored_team = home_name
                favored_abbr = home_abbr
                impact = "positive"
            else:
                favored_team = away_name
                favored_abbr = away_abbr
                impact = "positive"

            # Strength scales with magnitude of implied shift
            strength = min(0.85, 0.35 + abs(ml_shift) * 5.0)

            signals.append(_signal(
                "market",
                f"Line moving toward {favored_team} "
                f"(opened {open_ml:+.0f}, now {curr_ml:+.0f})",
                impact, favored_abbr, strength,
                icon="trending-up" if ml_movement < 0 else "trending-down",
                tooltip=(
                    f"Implied probability shifted {abs(ml_shift):.1%} toward "
                    f"{favored_team} across {snapshots} snapshots"
                ),
            ))

        # --- Reverse line movement (sharp money indicator) ---
        if is_reverse and abs(ml_shift) >= _mc.line_movement_min_shift:
            if ml_movement < 0:
                sharp_side = home_name
                sharp_abbr = home_abbr
            else:
                sharp_side = away_name
                sharp_abbr = away_abbr

            signals.append(_signal(
                "market",
                f"Reverse line movement detected — sharp money on {sharp_side}",
                "positive", sharp_abbr,
                min(0.90, 0.50 + abs(ml_shift) * 4.0),
                icon="alert-triangle",
                tooltip=(
                    "Line moved against expected public betting direction. "
                    "This often indicates professional/sharp bettors have taken a position."
                ),
            ))

        # --- Over/under movement signal ---
        if abs(ou_movement) >= 0.5:
            direction = "down" if ou_movement < 0 else "up"
            open_ou = lm.get("total_open")
            curr_ou = lm.get("total_current")
            ou_text = f"Total line moved {direction}"
            if open_ou is not None and curr_ou is not None:
                ou_text = f"Total line moved {direction} (opened {open_ou}, now {curr_ou})"

            ou_impact = "under" if ou_movement < 0 else "over"
            signals.append(_signal(
                "market",
                ou_text,
                "neutral", home_abbr,
                min(0.65, 0.30 + abs(ou_movement) * 0.3),
                icon="activity",
                tooltip=f"Over/under shifted {ou_movement:+.1f} goals from opening",
            ))

        # --- Spread movement signal ---
        if abs(spread_movement) >= 0.5:
            open_spread = lm.get("spread_open")
            curr_spread = lm.get("spread_current")
            if open_spread is not None and curr_spread is not None:
                if spread_movement < 0:
                    spread_team = home_name
                    spread_abbr = home_abbr
                else:
                    spread_team = away_name
                    spread_abbr = away_abbr

                signals.append(_signal(
                    "market",
                    f"Puck line moved toward {spread_team} "
                    f"(opened {open_spread:+.1f}, now {curr_spread:+.1f})",
                    "neutral", spread_abbr,
                    min(0.55, 0.25 + abs(spread_movement) * 0.25),
                    icon="bar-chart",
                    tooltip=f"Spread shifted {spread_movement:+.1f} from opening",
                ))

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
    tooltip: str = "",
) -> Dict[str, Any]:
    """Create a signal dict."""
    sig: Dict[str, Any] = {
        "category": category,
        "text": text,
        "impact": impact,
        "team": team,
        "strength": round(min(1.0, max(0.0, strength)), 3),
        "icon": icon,
    }
    if tooltip:
        sig["tooltip"] = tooltip
    return sig
