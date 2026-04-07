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
import math
from datetime import datetime

from .db import (
    get_conn, get_team_by_id, get_team_record,
    get_pitcher_season, get_bullpen, get_park_factor,
    get_recent_games, get_team_h2h_vs_pitcher,
)

logger = logging.getLogger(__name__)

SEASON = datetime.now().year

# ── League-wide baselines ────────────────────────────────────

MLB_AVG_RPG = 4.5          # Average runs per game per team
MLB_AVG_ERA = 4.10
MLB_AVG_OPS = .720
MLB_AVG_FIP = 4.10
MLB_AVG_WHIP = 1.28
MLB_AVG_K9 = 8.5
MLB_AVG_BB9 = 3.2
MLB_AVG_WRC_PLUS = 100     # By definition
MLB_HOME_EDGE = 0.28       # ~0.28 runs home advantage

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

    # ── Step 4: Park factor ──
    park_run_factor = 1.0
    if park:
        park_run_factor = park.get("run_factor", 1.0) or 1.0
    home_xr *= park_run_factor
    away_xr *= park_run_factor

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

    # ── Floor ──
    home_xr = max(home_xr, 1.5)
    away_xr = max(away_xr, 1.5)

    total = home_xr + away_xr
    spread = away_xr - home_xr  # Negative = home favored

    # ── Win probability (Poisson-based) ──
    conf = _compute_confidence(home_pit, away_pit, home_sp_pit, away_sp_pit)

    # No dampening — raw model output compared against real market odds.
    # The DraftKings odds are the calibration, not artificial dampening.
    matrix = _build_score_matrix(home_xr, away_xr, max_runs=15)
    p_home, p_away = _win_probs_from_matrix(matrix)

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


# ── Rating components ────────────────────────────────────────

def _team_offense_rating(stats: dict) -> float:
    """
    Estimate runs/game from team stats.
    Weighted blend of actual R/G + advanced metrics.
    """
    runs_pg = stats.get("runs_pg")
    if runs_pg and runs_pg > 0:
        base = runs_pg
    else:
        base = MLB_AVG_RPG

    wrc_plus = stats.get("wrc_plus")
    ops = stats.get("ops")

    if wrc_plus and wrc_plus > 0:
        wrc_factor = wrc_plus / 100
        base = MLB_AVG_RPG * wrc_factor * 0.6 + base * 0.4
    elif ops and ops > 0:
        ops_factor = ops / MLB_AVG_OPS
        base = MLB_AVG_RPG * ops_factor * 0.5 + base * 0.5

    return base


def _compute_confidence(home_pit, away_pit, home_sp_pit, away_sp_pit) -> dict:
    """
    Compute prediction confidence based on data quality.
    Returns 0-100 score and a label.
    """
    score = 0
    max_score = 0

    # Team games played (0-25 pts each)
    for pit in [home_pit, away_pit]:
        max_score += 25
        gp = pit.get("games_played", 0) if pit else 0
        if gp >= 50:
            score += 25
        elif gp >= 30:
            score += 20
        elif gp >= 15:
            score += 15
        elif gp >= 5:
            score += 8
        elif gp > 0:
            score += 3

    # Pitcher starts (0-25 pts each)
    for sp in [home_sp_pit, away_sp_pit]:
        max_score += 25
        starts = sp.get("games_started", 0) if sp else 0
        if starts >= 10:
            score += 25
        elif starts >= 5:
            score += 18
        elif starts >= 3:
            score += 12
        elif starts >= 1:
            score += 5

    pct = round(score / max_score * 100) if max_score > 0 else 0

    if pct >= 80:
        label = "high"
    elif pct >= 50:
        label = "medium"
    elif pct >= 25:
        label = "low"
    else:
        label = "very_low"

    return {"score": pct, "label": label}


def _blended_offense(pit: dict | None, stats: dict) -> float:
    """
    Blend PIT runs/game with league average based on sample size.
    Small sample: mostly league avg. Large sample: mostly PIT data.
    """
    base = _team_offense_rating(stats)  # From team_stats table or league avg

    if not pit or not pit.get("runs_pg"):
        return base

    gp = pit.get("games_played", 0)
    pit_rpg = pit["runs_pg"]

    if gp == 0:
        return base
    elif gp < 5:
        # Very small sample — 20% PIT, 80% baseline
        return pit_rpg * 0.20 + base * 0.80
    elif gp < 15:
        # Small sample — 50% PIT, 50% baseline
        return pit_rpg * 0.50 + base * 0.50
    elif gp < 30:
        # Moderate — 75% PIT, 25% baseline
        return pit_rpg * 0.75 + base * 0.25
    else:
        # Full confidence in PIT data
        return pit_rpg


def _blended_pitcher(sp_pit: dict | None, sp_db: dict | None) -> float:
    """
    Blend PIT pitcher ERA with DB stats or league average.
    Small sample: mostly league avg. More starts: mostly PIT.
    """
    db_factor = _pitcher_factor(sp_db)

    if not sp_pit or not sp_pit.get("era"):
        return db_factor

    starts = sp_pit.get("games_started", 0)
    pit_factor = sp_pit["era"] / MLB_AVG_ERA
    pit_factor = max(0.60, min(1.50, pit_factor))

    if starts == 0:
        return db_factor
    elif starts == 1:
        # One start — 20% PIT, 80% baseline
        return pit_factor * 0.20 + db_factor * 0.80
    elif starts <= 3:
        # 50/50 blend
        return pit_factor * 0.50 + db_factor * 0.50
    elif starts <= 8:
        # 75% PIT
        return pit_factor * 0.75 + db_factor * 0.25
    else:
        return pit_factor


def _pitcher_factor(sp: dict | None) -> float:
    """
    Starting pitcher quality as run multiplier.
    <1 = suppresses runs (ace), >1 = inflates runs (bad starter).
    Uses FIP > xFIP > ERA hierarchy for predictive accuracy.
    """
    if not sp:
        return 1.0

    innings = sp.get("innings") or 0
    if innings < 10:
        return 1.0

    # Primary: best available run prevention metric
    fip = sp.get("fip")
    x_fip = sp.get("x_fip")
    era = sp.get("era")
    whip = sp.get("whip")
    k_per_9 = sp.get("k_per_9")
    bb_per_9 = sp.get("bb_per_9")

    if fip is not None and fip > 0:
        primary = fip
        baseline = MLB_AVG_FIP
    elif era is not None and era > 0:
        primary = era
        baseline = MLB_AVG_ERA
    else:
        return 1.0

    run_factor = primary / baseline

    # Secondary adjustments
    adj = 0.0
    if whip and whip > 0:
        adj += ((whip - MLB_AVG_WHIP) / MLB_AVG_WHIP) * 0.10
    if k_per_9 and k_per_9 > 0:
        adj -= ((k_per_9 - MLB_AVG_K9) / MLB_AVG_K9) * 0.08
    if bb_per_9 and bb_per_9 > 0:
        adj += ((bb_per_9 - MLB_AVG_BB9) / MLB_AVG_BB9) * 0.06
    if x_fip and fip:
        adj += ((x_fip - fip) / baseline) * 0.05

    run_factor += adj
    return max(0.60, min(1.50, run_factor))


def _bullpen_factor(bp: dict) -> float:
    """
    Bullpen quality factor.
    <1 = good pen, >1 = bad pen. Includes fatigue.
    """
    if not bp:
        return 1.0

    era = bp.get("era")
    if not era or era <= 0:
        return 1.0

    factor = era / MLB_AVG_ERA

    innings_3d = bp.get("innings_last_3d", 0) or 0
    if innings_3d > 10:
        factor *= 1.08
    elif innings_3d > 7:
        factor *= 1.04

    return max(0.70, min(1.40, factor))


def _h2h_adjustment(h2h_matchups: list[dict]) -> float:
    """Runs adjustment from H2H batter-vs-pitcher history (-0.5 to +0.5)."""
    if not h2h_matchups:
        return 0.0

    total_ab = sum(m.get("at_bats", 0) or 0 for m in h2h_matchups)
    total_hits = sum(m.get("hits", 0) or 0 for m in h2h_matchups)
    total_hr = sum(m.get("home_runs", 0) or 0 for m in h2h_matchups)

    if total_ab < 20:
        return 0.0

    h2h_avg = total_hits / total_ab
    avg_diff = h2h_avg - 0.250
    confidence = min(1.0, total_ab / 100)
    hr_rate = total_hr / total_ab
    hr_bonus = (hr_rate - 0.03) * 5

    adjustment = (avg_diff * 3.0 + hr_bonus * 0.5) * confidence
    return max(-0.5, min(0.5, adjustment))


def _summarize_h2h(matchups: list[dict]) -> dict:
    """Summarize H2H data for display."""
    total_ab = sum(m.get("at_bats", 0) or 0 for m in matchups)
    total_hits = sum(m.get("hits", 0) or 0 for m in matchups)
    total_hr = sum(m.get("home_runs", 0) or 0 for m in matchups)
    total_k = sum(m.get("strikeouts", 0) or 0 for m in matchups)
    avg = round(total_hits / total_ab, 3) if total_ab > 0 else 0

    return {
        "at_bats": total_ab, "hits": total_hits,
        "home_runs": total_hr, "strikeouts": total_k,
        "avg": avg, "batters": len(matchups),
    }


def _form_adjustment(team_id: int) -> float:
    """Recent form factor from last 10 games. Returns -0.10 to +0.10."""
    recent = get_recent_games(team_id, 10)
    if len(recent) < 5:
        return 0.0

    wins = 0
    total_margin = 0
    for g in recent:
        is_home = g.get("home_team_id") == team_id
        ts = g.get("home_score", 0) if is_home else g.get("away_score", 0)
        os = g.get("away_score", 0) if is_home else g.get("home_score", 0)
        if ts is not None and os is not None:
            if ts > os:
                wins += 1
            total_margin += (ts - os)

    n = len(recent)
    win_adj = (wins / n - 0.5) * 0.15
    margin_adj = max(-0.05, min(0.05, (total_margin / n) * 0.005))
    return max(-0.10, min(0.10, win_adj + margin_adj))


# ── Poisson & probability ────────────────────────────────────

def _poisson_prob(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def _build_uncertain_matrix(home_xr: float, away_xr: float,
                            confidence: int, max_runs: int = 15) -> list[list[float]]:
    """
    Build score matrix with uncertainty baked in.

    Instead of a single Poisson(lambda), we average over multiple
    lambdas drawn from a range around the point estimate. The range
    is wider when confidence is low.

    At 100% confidence: single Poisson (standard).
    At 0% confidence: average over lambda ± 2.0 runs (very uncertain).

    This naturally produces probabilities closer to 50% when we
    don't have good data, preventing fake 84% edges.
    """
    if confidence >= 90:
        # High confidence — use standard single Poisson
        return _build_score_matrix(home_xr, away_xr, max_runs)

    # Uncertainty: at low confidence, each team's true scoring rate could
    # be significantly different from our estimate. We model this by
    # averaging over a range of possible lambdas.
    # 0% conf = ±3.0 runs uncertainty, 50% = ±1.5, 90% = ±0.0
    uncertainty = 3.0 * (1 - confidence / 100) ** 0.7

    # Generate 9 scenarios across the uncertainty range
    n_scenarios = 9
    combined = [[0.0] * (max_runs + 1) for _ in range(max_runs + 1)]

    for i in range(n_scenarios):
        frac = (i / (n_scenarios - 1)) - 0.5  # -0.5 to +0.5
        h_off = frac * 2 * uncertainty
        a_off = frac * 2 * uncertainty
        h_lambda = max(1.5, home_xr + h_off)
        a_lambda = max(1.5, away_xr + a_off)
        m = _build_score_matrix(h_lambda, a_lambda, max_runs)
        for h in range(max_runs + 1):
            for a in range(max_runs + 1):
                combined[h][a] += m[h][a]

    for h in range(max_runs + 1):
        for a in range(max_runs + 1):
            combined[h][a] /= n_scenarios

    return combined


def _build_score_matrix(home_xr: float, away_xr: float,
                        max_runs: int = 15) -> list[list[float]]:
    matrix = []
    for h in range(max_runs + 1):
        row = []
        for a in range(max_runs + 1):
            row.append(_poisson_prob(home_xr, h) * _poisson_prob(away_xr, a))
        matrix.append(row)
    return matrix


def _win_probs_from_matrix(matrix: list[list[float]]) -> tuple[float, float]:
    p_home = p_away = p_tie = 0.0
    for h in range(len(matrix)):
        for a in range(len(matrix[0])):
            if h > a:
                p_home += matrix[h][a]
            elif a > h:
                p_away += matrix[h][a]
            else:
                p_tie += matrix[h][a]

    # Distribute ties proportionally (extra innings)
    if p_tie > 0:
        total = p_home + p_away
        if total > 0:
            p_home += p_tie * (p_home / total)
            p_away += p_tie * (p_away / total)
        else:
            p_home += p_tie / 2
            p_away += p_tie / 2

    return p_home, p_away


# ── Over/Under ───────────────────────────────────────────────

def _generate_ou_lines(total: float, matrix: list[list[float]]) -> dict:
    base = round(total * 2) / 2
    lines = [base - 2, base - 1, base - 0.5, base, base + 0.5, base + 1, base + 2]
    lines = [l for l in lines if 4.5 <= l <= 16.5]

    result = {}
    for line in lines:
        p_over = sum(matrix[h][a] for h in range(len(matrix))
                     for a in range(len(matrix[0])) if (h + a) > line)
        result[str(line)] = {
            "over": round(p_over, 4),
            "under": round(1 - p_over, 4),
        }
    return result


# ── Run Line ─────────────────────────────────────────────────

def _run_line_probs(matrix: list[list[float]], home_xr: float = 0,
                     away_xr: float = 0) -> dict:
    """
    Compute run line probabilities for multiple spreads.
    Includes standard -1.5 plus the model's projected spread.
    """
    # Calculate probability for each possible margin
    margin_probs = {}
    for h in range(len(matrix)):
        for a in range(len(matrix[0])):
            margin = h - a  # Positive = home wins by N
            margin_probs[margin] = margin_probs.get(margin, 0) + matrix[h][a]

    # Standard -1.5 run line
    p_home_15 = sum(p for m, p in margin_probs.items() if m >= 2)
    p_away_15 = sum(p for m, p in margin_probs.items() if m <= 1)

    # Model's projected spread (rounded to 0.5)
    model_spread = round((home_xr - away_xr) * 2) / 2
    if model_spread == 0:
        model_spread = 0.5 if home_xr > away_xr else -0.5

    # Generate lines: -1.5, model spread, and a couple around it
    lines = sorted(set([-1.5, 1.5, model_spread]))
    # Add +/- 0.5 around model spread
    for offset in [-1, -0.5, 0.5, 1]:
        lines.append(model_spread + offset)
    lines = sorted(set(l for l in lines if -6 <= l <= 6))

    spreads = {}
    for line in lines:
        # P(home covers line): home margin > line
        if line > 0:
            # Home -line: home must win by more than line
            p_cover = sum(p for m, p in margin_probs.items() if m > line)
            label = f"home_{line:+.1f}".replace("+", "minus_").replace("-", "plus_").replace(".", "_")
        else:
            # Home +line (underdog): home can lose by less than |line|
            p_cover = sum(p for m, p in margin_probs.items() if m > line)
            label = f"home_{line:+.1f}".replace("+", "minus_").replace("-", "plus_").replace(".", "_")

        spreads[str(line)] = {
            "home_cover": round(p_cover, 4),
            "away_cover": round(1 - p_cover, 4),
        }

    return {
        "home_minus_1_5": round(p_home_15, 4),
        "away_plus_1_5": round(p_away_15, 4),
        "model_spread": model_spread,
        "spreads": spreads,
    }


# ── First 5 Innings ──────────────────────────────────────────

def _compute_f5(home_xr: float, away_xr: float,
                home_sp_factor: float, away_sp_factor: float) -> dict:
    """F5 prediction. Starters account for ~58-62% of runs."""
    sp_depth_home = 0.62 if home_sp_factor < 0.90 else 0.58
    sp_depth_away = 0.62 if away_sp_factor < 0.90 else 0.58

    f5_home = round(home_xr * sp_depth_away, 1)
    f5_away = round(away_xr * sp_depth_home, 1)
    f5_total = round(f5_home + f5_away, 1)

    f5_matrix = _build_score_matrix(f5_home, f5_away, max_runs=10)
    f5_p_home, f5_p_away = _win_probs_from_matrix(f5_matrix)

    return {
        "home": f5_home, "away": f5_away, "total": f5_total,
        "win_prob": {"home": round(f5_p_home, 4), "away": round(f5_p_away, 4)},
    }


# ── Inning Breakdown ─────────────────────────────────────────

def _compute_first_inning(home_xr: float, away_xr: float,
                           home_sp_factor: float, away_sp_factor: float,
                           home_pitcher_id: int | None = None,
                           away_pitcher_id: int | None = None,
                           home_team_id: int | None = None,
                           away_team_id: int | None = None) -> dict:
    """
    First inning analysis for NRFI/YRFI betting.

    Uses pitcher-specific first-inning history when available,
    falls back to Poisson estimates from expected runs.
    """
    first_inning_weight = 0.105

    # Try to get pitcher first-inning scoreless rates from PIT data
    from .pit_stats import compute_pitcher_stats_at_date, compute_team_stats_at_date
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    season = datetime.now().year

    # Always compute expected 1st inning runs (needed for Poisson baseline)
    away_1st_xr = away_xr * first_inning_weight * (0.85 + 0.15 * home_sp_factor)
    home_1st_xr = home_xr * first_inning_weight * (0.85 + 0.15 * away_sp_factor)

    # Poisson baseline (always computed)
    p_away_zero_poisson = _poisson_prob(away_1st_xr, 0)
    p_home_zero_poisson = _poisson_prob(home_1st_xr, 0)

    # Start with Poisson
    p_away_zero = p_away_zero_poisson
    p_home_zero = p_home_zero_poisson

    # Blend with pitcher first-inning data when available
    # Weight by sample size: more starts = more trust in pitcher data
    if home_pitcher_id:
        sp_pit = compute_pitcher_stats_at_date(home_pitcher_id, today, season)
        if sp_pit and sp_pit.get("first_inning_scoreless_pct") is not None and sp_pit.get("first_inning_starts", 0) >= 3:
            starts = sp_pit["first_inning_starts"]
            pit_weight = min(0.7, starts / 30)  # Max 70% pitcher data, even with 30+ starts
            p_away_zero = (pit_weight * sp_pit["first_inning_scoreless_pct"]
                          + (1 - pit_weight) * p_away_zero_poisson)

    if away_pitcher_id:
        sp_pit = compute_pitcher_stats_at_date(away_pitcher_id, today, season)
        if sp_pit and sp_pit.get("first_inning_scoreless_pct") is not None and sp_pit.get("first_inning_starts", 0) >= 3:
            starts = sp_pit["first_inning_starts"]
            pit_weight = min(0.7, starts / 30)
            p_home_zero = (pit_weight * sp_pit["first_inning_scoreless_pct"]
                          + (1 - pit_weight) * p_home_zero_poisson)

    # Cap at realistic bounds — no pitcher is truly 100% or 0%
    p_away_zero = max(0.40, min(0.92, p_away_zero))
    p_home_zero = max(0.40, min(0.92, p_home_zero))

    # NRFI = both teams score 0 in the first
    nrfi = p_home_zero * p_away_zero
    yrfi = 1 - nrfi

    # P(exactly 1 run total in 1st)
    p_home_one = _poisson_prob(home_1st_xr, 1)
    p_away_one = _poisson_prob(away_1st_xr, 1)
    p_exactly_one = (p_home_one * p_away_zero) + (p_home_zero * p_away_one)

    # Away team bats first — P(away scores in top 1st)
    p_away_scores_1st = 1 - p_away_zero
    p_home_scores_1st = 1 - p_home_zero

    return {
        "nrfi": round(nrfi, 4),
        "yrfi": round(yrfi, 4),
        "home_scores_1st": round(p_home_scores_1st, 4),
        "away_scores_1st": round(p_away_scores_1st, 4),
        "home_xr_1st": round(home_1st_xr, 3),
        "away_xr_1st": round(away_1st_xr, 3),
        "exactly_one_run": round(p_exactly_one, 4),
    }


def _inning_breakdown(home_xr: float, away_xr: float) -> list[dict]:
    """Expected runs by inning with typical MLB scoring distribution."""
    weights = [0.105, 0.100, 0.105, 0.110, 0.100, 0.105, 0.115, 0.120, 0.140]
    return [{
        "inning": i + 1,
        "home": round(home_xr * w, 2),
        "away": round(away_xr * w, 2),
        "total": round((home_xr + away_xr) * w, 2),
    } for i, w in enumerate(weights)]


# ── Correct Scores ───────────────────────────────────────────

def _top_correct_scores(matrix: list[list[float]], n: int = 8) -> list[dict]:
    scores = []
    for h in range(min(len(matrix), 12)):
        for a in range(min(len(matrix[0]), 12)):
            if h == a:
                continue
            scores.append({"home": h, "away": a, "prob": round(matrix[h][a], 4)})
    scores.sort(key=lambda x: x["prob"], reverse=True)
    return scores[:n]


# ── Pitcher detail for frontend ──────────────────────────────

def _pitcher_detail(sp: dict | None, pitcher_id: int | None) -> dict | None:
    if not sp and not pitcher_id:
        return None

    conn = get_conn()
    if not sp:
        row = conn.execute("SELECT name, throws FROM players WHERE mlb_id = ?",
                          (pitcher_id,)).fetchone()
        if row:
            return {"name": row["name"], "throws": row["throws"], "id": pitcher_id}
        return {"name": "TBD", "id": pitcher_id}

    row = conn.execute("SELECT name, throws FROM players WHERE mlb_id = ?",
                      (sp["player_id"],)).fetchone()
    name = row["name"] if row else "Unknown"
    throws = row["throws"] if row else ""
    w = sp.get("wins", 0) or 0
    l = sp.get("losses", 0) or 0

    return {
        "id": sp["player_id"], "name": name, "throws": throws,
        "record": f"{w}-{l}",
        "era": sp.get("era"), "fip": sp.get("fip"), "x_fip": sp.get("x_fip"),
        "whip": sp.get("whip"), "k_per_9": sp.get("k_per_9"),
        "bb_per_9": sp.get("bb_per_9"), "innings": sp.get("innings"),
        "k_pct": sp.get("k_pct"), "babip": sp.get("babip"),
        "hr_per_9": sp.get("hr_per_9"), "avg_velocity": sp.get("avg_velocity"),
        "barrel_pct_against": sp.get("barrel_pct_against"),
    }


# ── Reasoning ────────────────────────────────────────────────

def _build_reasoning(home_team, away_team, home_stats, away_stats,
                     home_sp, away_sp, home_xr, away_xr,
                     park_factor, home_form, away_form, h2h_data) -> list[str]:
    hn = home_team.get("abbreviation", "HOME")
    an = away_team.get("abbreviation", "AWAY")
    reasons = []

    reasons.append(f"Model projects {hn} {round(home_xr)} - {round(away_xr)} {an} (expected runs: {home_xr:.1f} - {away_xr:.1f})")

    if home_sp:
        era = home_sp.get("era") or home_sp.get("fip")
        if era:
            reasons.append(f"{hn} SP: {era:.2f} ERA, "
                          f"{home_sp.get('k_per_9', 0):.1f} K/9, "
                          f"{home_sp.get('whip', 0):.2f} WHIP")
    if away_sp:
        era = away_sp.get("era") or away_sp.get("fip")
        if era:
            reasons.append(f"{an} SP: {era:.2f} ERA, "
                          f"{away_sp.get('k_per_9', 0):.1f} K/9, "
                          f"{away_sp.get('whip', 0):.2f} WHIP")

    h_ops = home_stats.get("ops")
    a_ops = away_stats.get("ops")
    if h_ops and a_ops:
        reasons.append(f"Team OPS: {hn} {h_ops:.3f} | {an} {a_ops:.3f}")

    h_wrc = home_stats.get("wrc_plus")
    a_wrc = away_stats.get("wrc_plus")
    if h_wrc and a_wrc:
        reasons.append(f"wRC+: {hn} {h_wrc:.0f} | {an} {a_wrc:.0f}")

    if park_factor and park_factor != 1.0:
        if park_factor > 1.03:
            reasons.append(f"Park factor {park_factor:.2f} — hitter-friendly venue")
        elif park_factor < 0.97:
            reasons.append(f"Park factor {park_factor:.2f} — pitcher-friendly venue")

    if abs(home_form) > 0.03:
        reasons.append(f"{hn} running {'hot' if home_form > 0 else 'cold'} (form {home_form:+.1%})")
    if abs(away_form) > 0.03:
        reasons.append(f"{an} running {'hot' if away_form > 0 else 'cold'} (form {away_form:+.1%})")

    if "home_vs_sp" in h2h_data:
        h2h = h2h_data["home_vs_sp"]
        if h2h["at_bats"] >= 20:
            reasons.append(
                f"{hn} batters vs {an} SP: {h2h['avg']:.3f} AVG "
                f"({h2h['hits']}/{h2h['at_bats']}, {h2h['home_runs']} HR)")

    hw = home_stats.get("wins", 0)
    hl = home_stats.get("losses", 0)
    aw = away_stats.get("wins", 0)
    al = away_stats.get("losses", 0)
    if hw + hl > 0 and aw + al > 0:
        reasons.append(f"Records: {hn} {hw}-{hl} | {an} {aw}-{al}")

    return reasons


# ── Utility: odds conversion ─────────────────────────────────

def ml_to_implied_prob(ml: int) -> float:
    """Convert American moneyline to implied probability."""
    if ml > 0:
        return 100 / (ml + 100)
    else:
        return abs(ml) / (abs(ml) + 100)


def find_edge(model_prob: float, ml: int) -> float:
    """Model prob minus implied prob. Positive = value bet."""
    return (model_prob - ml_to_implied_prob(ml)) * 100
