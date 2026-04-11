"""
MLB Prediction Engine.

Generates matchup predictions using:
  1. Starting pitcher quality (ERA, FIP, xFIP, WHIP, K/9, recent form)
  2. Team offense strength (OPS, wRC+, ISO, K%, BB%)
  3. Bullpen strength & fatigue
  4. Batter-vs-pitcher H2H matchups
  5. Park factors (run environment)
  6. Home/away splits & recent form
  7. Platoon advantages (L/R matchups)
  8. Umpire tendencies (HP umpire run factor)
  9. Weather conditions (temperature, wind, precipitation)
  10. Travel fatigue (schedule density, road trips)
  11. Simplified platoon splits (LHP suppression)

Output: expected runs, win probability, run line, O/U, F5, inning
breakdown, and edge analysis vs Vegas.
"""

import logging
from datetime import datetime

from .db import (
    get_conn, get_team_by_id, get_team_record,
    get_pitcher_season, get_bullpen, get_park_factor,
    get_recent_games, get_team_h2h_vs_pitcher,
)

# ── Scoring / probability functions (re-exported for external callers) ──
from .mlb_scoring import (
    MLB_AVG_RPG, MLB_AVG_ERA, MLB_AVG_OPS, MLB_AVG_FIP,
    MLB_AVG_WHIP, MLB_AVG_K9, MLB_AVG_BB9, MLB_AVG_WRC_PLUS,
    MLB_HOME_EDGE,
    _poisson_prob, _build_uncertain_matrix, _build_score_matrix,
    _win_probs_from_matrix, _generate_ou_lines, _run_line_probs,
    _compute_f5, _inning_breakdown, _top_correct_scores,
    ml_to_implied_prob, find_edge,
)

# ── Factor computation functions (re-exported for external callers) ──
from .mlb_factors import (
    _team_offense_rating, _blended_offense, _blended_pitcher,
    _pitcher_factor, _bullpen_factor, _bullpen_fatigue_penalty,
    _compute_lineup_strength, _h2h_adjustment, _summarize_h2h,
    _form_adjustment, _compute_first_inning, _compute_confidence,
    _pitcher_detail, _build_reasoning,
)

logger = logging.getLogger(__name__)

SEASON = datetime.now().year

# Calibrated weights — loaded from DB on first use, updated by calibration system
_cached_weights = None

def _get_weights() -> dict:
    """Load calibrated weights (cached after first call)."""
    global _cached_weights
    if _cached_weights is None:
        from .calibration import get_weights
        _cached_weights = get_weights()
    return _cached_weights

def reload_weights():
    """Force reload weights from DB (call after calibration)."""
    global _cached_weights
    _cached_weights = None


# ── Core prediction ──────────────────────────────────────────

def predict_matchup(home_team_id: int, away_team_id: int,
                    home_pitcher_id: int | None = None,
                    away_pitcher_id: int | None = None,
                    venue: str | None = None) -> dict:
    """
    Full MLB matchup prediction.

    Returns dict with expected_score, win_prob, spread, total,
    over_under, f5, inning breakdown, pitcher detail, and reasoning.
    """
    # ── Load data ──
    home_team = get_team_by_id(home_team_id)
    away_team = get_team_by_id(away_team_id)
    if not home_team or not away_team:
        return {"error": "Team not found"}

    home_stats = get_team_record(home_team_id, SEASON) or {}
    away_stats = get_team_record(away_team_id, SEASON) or {}

    home_sp = get_pitcher_season(home_pitcher_id, SEASON) if home_pitcher_id else None
    away_sp = get_pitcher_season(away_pitcher_id, SEASON) if away_pitcher_id else None

    home_bullpen = get_bullpen(home_team_id, SEASON) or {}
    away_bullpen = get_bullpen(away_team_id, SEASON) or {}

    park = get_park_factor(venue, SEASON) if venue else None

    # ── Point-in-time stats from game history ──
    # These are always available after a quick sync and are more
    # accurate than season-aggregate stats from the full sync
    from .pit_stats import compute_team_stats_at_date, compute_pitcher_stats_at_date
    today = datetime.now().strftime("%Y-%m-%d")

    home_pit = compute_team_stats_at_date(home_team_id, today, SEASON)
    away_pit = compute_team_stats_at_date(away_team_id, today, SEASON)

    home_sp_pit = None
    away_sp_pit = None
    if home_pitcher_id:
        home_sp_pit = compute_pitcher_stats_at_date(home_pitcher_id, today, SEASON)
    if away_pitcher_id:
        away_sp_pit = compute_pitcher_stats_at_date(away_pitcher_id, today, SEASON)

    # ── Step 1: Baseline expected runs ──
    # Blend PIT data with league average based on sample size.
    # Early season: lean on league avg. Mid-season: lean on PIT data.
    home_off = _blended_offense(home_pit, home_stats)
    away_off = _blended_offense(away_pit, away_stats)

    # ── Step 2: Starting pitcher adjustment ──
    home_sp_factor = _blended_pitcher(home_sp_pit, home_sp)
    away_sp_factor = _blended_pitcher(away_sp_pit, away_sp)

    # Home offense scores against away SP, away offense against home SP
    home_xr = home_off * away_sp_factor
    away_xr = away_off * home_sp_factor

    # ── Step 2b: Lineup-level offense adjustment ──
    # Uses individual batter wRC+/OPS when available
    home_lineup_str = _compute_lineup_strength(home_team_id, SEASON)
    away_lineup_str = _compute_lineup_strength(away_team_id, SEASON)
    home_xr *= home_lineup_str
    away_xr *= away_lineup_str

    # ── Step 3: Per-team adjustments ──
    # Each team has learned factors from their actual performance
    from .team_calibration import get_team_adjustment
    home_adj = get_team_adjustment(home_team_id, SEASON)
    away_adj = get_team_adjustment(away_team_id, SEASON)

    # Apply offense/defense factors
    if home_adj["games_analyzed"] >= 3:
        home_xr *= home_adj["offense_factor"]
        away_xr *= home_adj["defense_factor"]  # Home defense affects away scoring
    if away_adj["games_analyzed"] >= 3:
        away_xr *= away_adj["offense_factor"]
        home_xr *= away_adj["defense_factor"]  # Away defense affects home scoring

    # Home/away split factors
    if home_adj["games_analyzed"] >= 5:
        home_xr *= home_adj["home_factor"]
    if away_adj["games_analyzed"] >= 5:
        away_xr *= away_adj["away_factor"]

    # ── Step 4: Bullpen adjustment ──
    home_bp_factor = _bullpen_factor(home_bullpen)
    away_bp_factor = _bullpen_factor(away_bullpen)

    # Bullpen covers ~35% of the game (last 3-4 innings)
    home_xr *= (1 + 0.35 * (away_bp_factor - 1))
    away_xr *= (1 + 0.35 * (home_bp_factor - 1))

    # ── Step 4a: Enhanced bullpen fatigue weighting ──
    # Tired bullpen = more opponent runs in late innings.
    # Check recent game history to detect heavy usage patterns.
    home_recent = get_recent_games(home_team_id, 3)
    away_recent = get_recent_games(away_team_id, 3)

    home_bp_fatigue = _bullpen_fatigue_penalty(home_bullpen, home_recent)
    away_bp_fatigue = _bullpen_fatigue_penalty(away_bullpen, away_recent)

    # Fatigue in home bullpen means away scores more (and vice versa)
    away_xr *= home_bp_fatigue
    home_xr *= away_bp_fatigue

    # ── Step 4b: Park factor ──
    park_run_factor = 1.0
    if park:
        park_run_factor = park.get("run_factor", 1.0) or 1.0
    home_xr *= park_run_factor
    away_xr *= park_run_factor

    # Coors Field specific correction — standard park factor underestimates
    if venue and "coors" in venue.lower():
        coors_boost = 1.08  # Additional 8% on top of park factor
        home_xr *= coors_boost
        away_xr *= coors_boost

    # ── Step 5: Home advantage ──
    w = _get_weights()
    home_edge = w.get("home_edge", MLB_HOME_EDGE)
    home_xr += home_edge / 2
    away_xr -= home_edge / 2

    # ── Step 6: Situational adjustments ──
    # Weather, rest/fatigue, pitcher rest, lineup strength, platoon
    from .situational import compute_all_adjustments
    today = datetime.now().strftime("%Y-%m-%d")

    # Try to get pitcher handedness
    home_throws = None
    away_throws = None
    if home_sp:
        conn = get_conn()
        row = conn.execute("SELECT throws FROM players WHERE mlb_id = ?",
                          (home_pitcher_id,)).fetchone()
        if row:
            home_throws = row["throws"]
    if away_sp:
        conn = get_conn()
        row = conn.execute("SELECT throws FROM players WHERE mlb_id = ?",
                          (away_pitcher_id,)).fetchone()
        if row:
            away_throws = row["throws"]

    # Try to get lineups for today's game
    home_lineup = None
    away_lineup = None
    if venue:
        # Check if game is today — try fetching lineup
        game_row = get_conn().execute("""
            SELECT mlb_game_id, weather_temp, weather_wind FROM games
            WHERE home_team_id = ? AND away_team_id = ? AND date = ?
            LIMIT 1
        """, (home_team_id, away_team_id, today)).fetchone()

        game_temp = None
        game_wind = None
        if game_row:
            game_temp = game_row["weather_temp"]
            game_wind = game_row["weather_wind"]

            try:
                from scrapers.mlb_stats import fetch_game_lineups
                lineups = fetch_game_lineups(game_row["mlb_game_id"])
                if lineups:
                    home_lineup = lineups.get("home_lineup")
                    away_lineup = lineups.get("away_lineup")
            except Exception:
                pass
    else:
        game_temp = None
        game_wind = None

    sit = compute_all_adjustments(
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        home_pitcher_id=home_pitcher_id,
        away_pitcher_id=away_pitcher_id,
        game_date=today,
        venue=venue,
        weather_temp=game_temp,
        weather_wind=game_wind,
        home_lineup=home_lineup,
        away_lineup=away_lineup,
        home_pitcher_throws=home_throws,
        away_pitcher_throws=away_throws,
    )

    home_xr *= sit["home_multiplier"]
    away_xr *= sit["away_multiplier"]

    # ── Step 6b: Umpire tendency ──
    umpire_factor = 1.0
    umpire_name = None
    if venue:
        try:
            from .umpire import compute_umpire_adjustment
            # Check if umpire is stored on today's game row
            ump_row = get_conn().execute(
                "SELECT umpire FROM games WHERE home_team_id = ? AND away_team_id = ? AND date = ? LIMIT 1",
                (home_team_id, away_team_id, today),
            ).fetchone()
            if ump_row and ump_row["umpire"]:
                umpire_name = ump_row["umpire"]
                umpire_factor = compute_umpire_adjustment(umpire_name)
                home_xr *= umpire_factor
                away_xr *= umpire_factor
        except Exception as e:
            logger.warning("Umpire adjustment failed: %s", e)

    # ── Step 6c: Weather (Open-Meteo) ──
    weather_adj = 1.0
    try:
        from .weather import get_weather_for_venue, compute_weather_adjustment, DOMED_STADIUMS
        if venue and venue not in DOMED_STADIUMS:
            wx_data, is_domed = get_weather_for_venue(venue)
            if wx_data and not is_domed:
                weather_adj = compute_weather_adjustment(wx_data, venue)
                home_xr *= weather_adj
                away_xr *= weather_adj
    except Exception as e:
        logger.warning("Weather adjustment failed: %s", e)

    # ── Step 6d: Travel fatigue ──
    home_travel = 1.0
    away_travel = 1.0
    try:
        from .travel import compute_travel_fatigue
        home_travel = compute_travel_fatigue(home_team_id, today, SEASON)
        away_travel = compute_travel_fatigue(away_team_id, today, SEASON)
        home_xr *= home_travel
        away_xr *= away_travel
    except Exception as e:
        logger.warning("Travel fatigue adjustment failed: %s", e)

    # ── Step 6e: Platoon splits (simplified) ──
    # LHP slightly suppresses offense vs average lineup composition.
    # This is a coarse adjustment on top of the lineup-based platoon
    # factor in situational.py (which requires confirmed lineups).
    platoon_home_adj = 1.0
    platoon_away_adj = 1.0
    if away_throws and away_throws.upper() == "L":
        platoon_home_adj = 0.97  # LHP suppresses home offense slightly
        home_xr *= platoon_home_adj
    if home_throws and home_throws.upper() == "L":
        platoon_away_adj = 0.97  # LHP suppresses away offense slightly
        away_xr *= platoon_away_adj

    # ── Step 7: Matchup interaction ──
    # Compound effects: how do these two specific teams interact TODAY
    from .matchup import compute_matchup_interaction, get_h2h_history
    matchup = compute_matchup_interaction(
        home_team_id, away_team_id,
        home_pitcher_id, away_pitcher_id,
        home_pit, away_pit, home_sp_pit, away_sp_pit,
        home_adj, away_adj, venue,
    )
    home_xr *= matchup["home_interaction"]
    away_xr *= matchup["away_interaction"]

    # H2H historical record for display
    h2h_history = get_h2h_history(home_team_id, away_team_id)

    # ── Step 8: Batter vs pitcher H2H ──
    h2h_adj_home, h2h_adj_away = 0.0, 0.0
    h2h_data = {}
    if away_pitcher_id:
        home_h2h = get_team_h2h_vs_pitcher(home_team_id, away_pitcher_id)
        h2h_adj_home = _h2h_adjustment(home_h2h)
        if home_h2h:
            h2h_data["home_vs_sp"] = _summarize_h2h(home_h2h)
    if home_pitcher_id:
        away_h2h = get_team_h2h_vs_pitcher(away_team_id, home_pitcher_id)
        h2h_adj_away = _h2h_adjustment(away_h2h)
        if away_h2h:
            h2h_data["away_vs_sp"] = _summarize_h2h(away_h2h)

    home_xr += h2h_adj_home
    away_xr += h2h_adj_away

    # ── Step 8: Recent form ──
    home_form = _form_adjustment(home_team_id)
    away_form = _form_adjustment(away_team_id)
    home_xr *= (1 + home_form)
    away_xr *= (1 + away_form)

    # ── Injury adjustment ──
    injury_data = {"home": [], "away": []}
    try:
        from .injuries import fetch_mlb_injuries, compute_mlb_injury_impact
        mlb_injuries = fetch_mlb_injuries()
        h_abbr = home_team["abbreviation"] if home_team else ""
        a_abbr = away_team["abbreviation"] if away_team else ""

        # Try alternate abbreviations (CWS/CHW, WSH/WAS, ARI/AZ, etc.)
        _MLB_ALT = {"CWS": "CHW", "CHW": "CWS", "WSH": "WAS", "WAS": "WSH",
                     "ARI": "AZ", "AZ": "ARI", "SF": "SFG", "SFG": "SF",
                     "SD": "SDP", "SDP": "SD", "TB": "TBR", "TBR": "TB",
                     "KC": "KCR", "KCR": "KC"}
        h_injuries = mlb_injuries.get(h_abbr, []) or mlb_injuries.get(_MLB_ALT.get(h_abbr, ""), [])
        a_injuries = mlb_injuries.get(a_abbr, []) or mlb_injuries.get(_MLB_ALT.get(a_abbr, ""), [])

        if h_injuries:
            h_impact = compute_mlb_injury_impact(home_team_id, h_injuries)
            home_xr *= h_impact
            injury_data["home"] = h_injuries[:5]

        if a_injuries:
            a_impact = compute_mlb_injury_impact(away_team_id, a_injuries)
            away_xr *= a_impact
            injury_data["away"] = a_injuries[:5]
    except Exception as e:
        logger.debug("MLB injury data unavailable: %s", e)

    # ── Floor + cap ──
    # The prediction has ~17 multiplicative factors. When they all compound
    # in the same direction, expected runs can blow out to 7-8 runs, which
    # creates unrealistic 93-97% ML win probabilities. Real MLB run distributions
    # rarely exceed 6.5 per team and never drop below 2.0.
    home_xr = max(2.0, min(6.5, home_xr))
    away_xr = max(2.0, min(6.5, away_xr))

    total = home_xr + away_xr
    spread = away_xr - home_xr  # Negative = home favored

    # ── Win probability (Poisson-based) ──
    conf = _compute_confidence(home_pit, away_pit, home_sp_pit, away_sp_pit)

    matrix = _build_score_matrix(home_xr, away_xr, max_runs=15)
    p_home, p_away = _win_probs_from_matrix(matrix)

    # Calibration cap: MLB win probabilities rarely exceed 75% even for
    # heavy favorites. Our backtest showed 57% actual win rate on ML picks
    # that were "displayed" at 80-97% confidence — a clear miscalibration.
    # Cap raw probabilities to 0.30-0.72 range to match reality.
    # The raw matrix still computes accurately; this just prevents the
    # display from showing overconfident numbers.
    p_home = max(0.30, min(0.72, p_home))
    p_away = 1 - p_home

    # ── Over/Under lines ──
    ou_lines = _generate_ou_lines(total, matrix)

    # ── Run line probabilities ──
    run_line = _run_line_probs(matrix, home_xr, away_xr)

    # ── F5 (First 5 innings) ──
    f5 = _compute_f5(home_xr, away_xr, home_sp_factor, away_sp_factor)

    # ── Inning breakdown ──
    innings = _inning_breakdown(home_xr, away_xr)

    # ── NRFI / First Inning ──
    first_inning = _compute_first_inning(
        home_xr, away_xr, home_sp_factor, away_sp_factor,
        home_pitcher_id, away_pitcher_id, home_team_id, away_team_id)

    # ── Correct scores ──
    correct_scores = _top_correct_scores(matrix, n=8)

    # ── Build reasoning ──
    reasoning = _build_reasoning(
        home_team, away_team, home_stats, away_stats,
        home_sp, away_sp, home_xr, away_xr,
        park_run_factor, home_form, away_form, h2h_data,
    )

    # ── Pitcher detail for frontend ──
    home_pitcher_detail = _pitcher_detail(home_sp, home_pitcher_id)
    away_pitcher_detail = _pitcher_detail(away_sp, away_pitcher_id)

    # ── Assemble result ──
    home_record = f"{home_stats.get('wins', 0)}-{home_stats.get('losses', 0)}"
    away_record = f"{away_stats.get('wins', 0)}-{away_stats.get('losses', 0)}"

    return {
        "home": {
            "team_id": home_team_id,
            "name": home_team["name"],
            "abbreviation": home_team["abbreviation"],
            "record": home_record,
            "streak": home_stats.get("streak", ""),
            "pitcher": home_pitcher_detail,
        },
        "away": {
            "team_id": away_team_id,
            "name": away_team["name"],
            "abbreviation": away_team["abbreviation"],
            "record": away_record,
            "streak": away_stats.get("streak", ""),
            "pitcher": away_pitcher_detail,
        },
        "expected_score": {
            "home": round(home_xr, 1),
            "away": round(away_xr, 1),
        },
        "total": round(total, 1),
        "spread": round(spread, 1),
        "win_prob": {
            "home": round(p_home, 4),
            "away": round(p_away, 4),
        },
        "park_factor": round(park_run_factor, 3),
        "situational": sit,
        "umpire": {"name": umpire_name, "factor": round(umpire_factor, 4)},
        "weather_adj": round(weather_adj, 4),
        "travel": {"home": round(home_travel, 4), "away": round(away_travel, 4)},
        "platoon_adj": {"home": round(platoon_home_adj, 4), "away": round(platoon_away_adj, 4)},
        "confidence": _compute_confidence(home_pit, away_pit, home_sp_pit, away_sp_pit),
        "over_under": ou_lines,
        "run_line": run_line,
        "f5": f5,
        "first_inning": first_inning,
        "innings": innings,
        "correct_scores": correct_scores,
        "h2h": h2h_data,
        "h2h_history": h2h_history,
        "injuries": injury_data,
        "matchup_insights": matchup.get("insights", []),
        "reasoning": reasoning,
    }
