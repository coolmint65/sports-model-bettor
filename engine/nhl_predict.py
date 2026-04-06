"""
NHL Prediction Engine.

Uses team stats from ESPN JSON files + Poisson distribution to model
expected goals and derive win probabilities, puck line, totals, and
period breakdowns.

Enhanced with special teams (PP/PK), goaltending (save%), shot volume,
faceoff dominance, form, and home/away splits.
"""

import math
import logging
from datetime import datetime
from .data import load_team, list_teams, get_league_averages

logger = logging.getLogger(__name__)

LEAGUE = "NHL"
HOME_EDGE = 0.15  # ~0.15 goal home-ice advantage
MAX_GOALS = 10


def poisson(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def _score_matrix(home_xg: float, away_xg: float) -> list[list[float]]:
    """Build probability matrix[home][away] via independent Poisson."""
    matrix = []
    for h in range(MAX_GOALS + 1):
        row = []
        for a in range(MAX_GOALS + 1):
            row.append(poisson(home_xg, h) * poisson(away_xg, a))
        matrix.append(row)
    return matrix


def _expected_goals(off: float, opp_def: float, league_avg: float) -> float:
    """Attack * defense / league_avg formula."""
    if league_avg <= 0:
        return off
    return (off * opp_def) / league_avg


def _form_factor(team: dict) -> float:
    """Recent form adjustment (-0.12 to +0.12)."""
    sos = team.get("strength_of_schedule", {})
    recent = sos.get("recent_games", 0)
    if recent < 3:
        return 0.0
    wr = sos.get("recent_wins", 0) / recent
    margin = sos.get("avg_margin", 0)
    win_adj = (wr - 0.5) * 0.16
    margin_adj = max(-0.04, min(0.04, margin * 0.004))
    return max(-0.12, min(0.12, win_adj + margin_adj))


def _split_adj(team: dict, is_home: bool) -> float:
    """Home/away split multiplier."""
    splits = team.get("home_away_splits", {})
    if not splits:
        return 1.0
    key = "home_ppg" if is_home else "away_ppg"
    split_ppg = splits.get(key, 0)
    if split_ppg <= 0:
        return 1.0
    overall = team.get("stats", {}).get("goals_for_avg", 0)
    if overall <= 0:
        return 1.0
    return max(0.88, min(1.12, split_ppg / overall))


def predict_matchup(home_key: str, away_key: str,
                    home_goalie_id: int | None = None,
                    away_goalie_id: int | None = None) -> dict | None:
    """
    Run full NHL matchup prediction.

    Args:
        home_key: team JSON file stem (e.g. "bruins")
        away_key: team JSON file stem (e.g. "maple_leafs")
        home_goalie_id: optional starting goalie ID for home team
        away_goalie_id: optional starting goalie ID for away team

    Returns prediction dict or None on failure.
    """
    home = load_team(LEAGUE, home_key)
    away = load_team(LEAGUE, away_key)
    if not home or not away:
        logger.warning("Could not load teams: %s / %s", home_key, away_key)
        return None

    la = get_league_averages(LEAGUE)
    hs = home.get("stats", {})
    as_ = away.get("stats", {})

    # Try to enrich with DB data if available
    db_home_stats, db_away_stats = None, None
    h2h_data = {}
    home_goalie_info, away_goalie_info = None, None

    try:
        from .nhl_db import (get_nhl_team_by_abbr, get_team_goalies,
                             get_h2h_nhl, get_recent_nhl_games, get_goalie_stats)
        from .nhl_calibration import get_calibrated_home_edge, get_total_adjustment

        h_abbr = home.get("abbreviation", "")
        a_abbr = away.get("abbreviation", "")

        db_home = get_nhl_team_by_abbr(h_abbr)
        db_away = get_nhl_team_by_abbr(a_abbr)

        if db_home and db_away:
            # H2H history
            h2h_data = get_h2h_nhl(db_home["id"], db_away["id"])

            # Goalie matchup
            season = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1
            season_str = f"{season}{season+1}"

            if home_goalie_id:
                home_goalie_info = get_goalie_stats(home_goalie_id, int(season_str))
            else:
                # Get team's best goalie by games played
                goalies = get_team_goalies(db_home["id"], int(season_str))
                if goalies:
                    home_goalie_info = dict(goalies[0])

            if away_goalie_id:
                away_goalie_info = get_goalie_stats(away_goalie_id, int(season_str))
            else:
                goalies = get_team_goalies(db_away["id"], int(season_str))
                if goalies:
                    away_goalie_info = dict(goalies[0])

            # Use calibrated home edge if available
            calibrated_he = get_calibrated_home_edge()
            if calibrated_he:
                global HOME_EDGE
                HOME_EDGE = calibrated_he

    except Exception as e:
        logger.debug("DB enrichment unavailable: %s", e)

    avg_gf = la.get("goals_for_avg", 3.0)
    avg_ga = la.get("goals_against_avg", 3.0)

    # Base expected goals
    home_off = hs.get("goals_for_avg", avg_gf)
    home_def = hs.get("goals_against_avg", avg_ga)
    away_off = as_.get("goals_for_avg", avg_gf)
    away_def = as_.get("goals_against_avg", avg_ga)

    home_xg = _expected_goals(home_off, away_def, avg_ga) + HOME_EDGE / 2
    away_xg = _expected_goals(away_off, home_def, avg_ga) - HOME_EDGE / 2

    # ── Special teams adjustment ──
    league_pp = la.get("pp_pct", 0.20)
    league_pk = la.get("pk_pct", 0.80)

    # Home PP vs away PK
    h_pp = hs.get("pp_pct", league_pp)
    a_pk = as_.get("pk_pct", league_pk)
    if h_pp and a_pk:
        pp_edge = (h_pp - league_pp) + (league_pk - a_pk)
        home_xg += pp_edge * 2.5  # ~3 PP chances per game, scaled

    # Away PP vs home PK
    a_pp = as_.get("pp_pct", league_pp)
    h_pk = hs.get("pk_pct", league_pk)
    if a_pp and h_pk:
        pp_edge = (a_pp - league_pp) + (league_pk - h_pk)
        away_xg += pp_edge * 2.5

    # ── Goaltending / save% adjustment ──
    league_sv = la.get("save_pct", 0.905)
    h_sv = hs.get("save_pct", league_sv)
    a_sv = as_.get("save_pct", league_sv)
    # Better save% suppresses opponent's expected goals
    if h_sv and league_sv:
        away_xg *= max(0.85, min(1.15, league_sv / h_sv))
    if a_sv and league_sv:
        home_xg *= max(0.85, min(1.15, league_sv / a_sv))

    # ── Shot volume adjustment ──
    league_shots = la.get("shots_per_game", 30.0)
    h_shots = hs.get("shots_per_game", league_shots)
    a_shots = as_.get("shots_per_game", league_shots)
    h_shots_against = hs.get("shots_against_per_game", league_shots)
    a_shots_against = as_.get("shots_against_per_game", league_shots)

    if league_shots > 0:
        # More shots = more goals; combine with opponent allowing shots
        h_shot_factor = ((h_shots / league_shots) + (a_shots_against / league_shots)) / 2
        a_shot_factor = ((a_shots / league_shots) + (h_shots_against / league_shots)) / 2
        home_xg *= max(0.90, min(1.10, h_shot_factor))
        away_xg *= max(0.90, min(1.10, a_shot_factor))

    # ── Faceoff adjustment ──
    h_fo = hs.get("faceoff_pct", 0.50)
    a_fo = as_.get("faceoff_pct", 0.50)
    fo_diff = (h_fo - a_fo)
    home_xg += fo_diff * 0.3  # Small but real possession edge
    away_xg -= fo_diff * 0.3

    # ── Form + splits ──
    home_xg *= (1 + _form_factor(home))
    away_xg *= (1 + _form_factor(away))
    home_xg *= _split_adj(home, is_home=True)
    away_xg *= _split_adj(away, is_home=False)

    # ── Goalie matchup adjustment ──
    # If we have specific goalie data from the DB, use it to override
    # the generic team save% adjustment
    goalie_factor = {"home": None, "away": None}
    if home_goalie_info:
        g_sv = home_goalie_info.get("save_pct") or 0
        g_gaa = home_goalie_info.get("gaa") or 0
        if g_sv > 0 and league_sv > 0:
            # Better goalie suppresses opponent scoring
            goalie_adj = league_sv / g_sv
            away_xg *= max(0.82, min(1.18, goalie_adj))
            goalie_factor["home"] = {
                "name": home_goalie_info.get("name", ""),
                "save_pct": round(g_sv, 3),
                "gaa": round(g_gaa, 2),
            }

    if away_goalie_info:
        g_sv = away_goalie_info.get("save_pct") or 0
        g_gaa = away_goalie_info.get("gaa") or 0
        if g_sv > 0 and league_sv > 0:
            goalie_adj = league_sv / g_sv
            home_xg *= max(0.82, min(1.18, goalie_adj))
            goalie_factor["away"] = {
                "name": away_goalie_info.get("name", ""),
                "save_pct": round(g_sv, 3),
                "gaa": round(g_gaa, 2),
            }

    # ── H2H adjustment ──
    h2h_adj = 0
    if h2h_data and h2h_data.get("games", 0) >= 3:
        h2h_wr = h2h_data.get("team1_wins", 0) / h2h_data["games"]
        # Small adjustment based on H2H dominance
        h2h_adj = (h2h_wr - 0.5) * 0.08
        home_xg *= (1 + h2h_adj)
        away_xg *= (1 - h2h_adj)

    # ── Calibrated total adjustment ──
    try:
        from .nhl_calibration import get_total_adjustment
        total_adj = get_total_adjustment()
        if total_adj:
            home_xg += total_adj / 2
            away_xg += total_adj / 2
    except Exception:
        pass

    # Floor
    home_xg = max(home_xg, 1.0)
    away_xg = max(away_xg, 1.0)

    # ── Poisson matrix ──
    matrix = _score_matrix(home_xg, away_xg)

    p_home = sum(matrix[h][a] for h in range(MAX_GOALS + 1) for a in range(MAX_GOALS + 1) if h > a)
    p_away = sum(matrix[h][a] for h in range(MAX_GOALS + 1) for a in range(MAX_GOALS + 1) if a > h)
    p_draw = sum(matrix[i][i] for i in range(MAX_GOALS + 1))

    # In NHL, ties go to OT — split ~50/50 with slight home edge
    p_home_ml = p_home + p_draw * 0.52
    p_away_ml = p_away + p_draw * 0.48

    # ── Puck line (±1.5) ──
    p_home_m15 = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                     for a in range(MAX_GOALS + 1) if h - a >= 2)
    p_away_p15 = 1 - p_home_m15

    p_away_m15 = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                     for a in range(MAX_GOALS + 1) if a - h >= 2)
    p_home_p15 = 1 - p_away_m15

    # ── Totals (must account for OT goal) ──
    # NHL O/U includes overtime. Tied games go to OT where exactly 1 more
    # goal is scored. So for each tie scenario (h == a), the actual total
    # is h + a + 1, not h + a.
    ou_lines = {}
    for line in [4.5, 5.0, 5.5, 6.0, 6.5, 7.0]:
        p_over = 0
        for h in range(MAX_GOALS + 1):
            for a in range(MAX_GOALS + 1):
                if h == a:
                    # Game goes to OT — total becomes h + a + 1
                    actual_total = h + a + 1
                else:
                    actual_total = h + a
                if actual_total > line:
                    p_over += matrix[h][a]
        ou_lines[str(line)] = {
            "over": round(p_over, 4),
            "under": round(1 - p_over, 4),
        }

    # ── Period breakdown ──
    weights = [0.33, 0.34, 0.33]
    periods = []
    for i, label in enumerate(["P1", "P2", "P3"]):
        periods.append({
            "period": label,
            "home": round(home_xg * weights[i], 2),
            "away": round(away_xg * weights[i], 2),
            "total": round((home_xg + away_xg) * weights[i], 2),
        })

    # ── First period scoreless (NRFI equivalent) ──
    p1_home = home_xg * weights[0]
    p1_away = away_xg * weights[0]
    p_scoreless_p1 = poisson(p1_home, 0) * poisson(p1_away, 0)

    # ── Top correct scores ──
    scores = []
    for h in range(MAX_GOALS + 1):
        for a in range(MAX_GOALS + 1):
            scores.append({"home": h, "away": a, "prob": matrix[h][a]})
    scores.sort(key=lambda s: s["prob"], reverse=True)
    top_scores = [{"score": f"{s['home']}-{s['away']}", "prob": round(s["prob"], 4)}
                  for s in scores[:5]]

    return {
        "home": {
            "name": home.get("name", home_key),
            "abbreviation": home.get("abbreviation", ""),
            "record": home.get("record", ""),
            "key": home_key,
        },
        "away": {
            "name": away.get("name", away_key),
            "abbreviation": away.get("abbreviation", ""),
            "record": away.get("record", ""),
            "key": away_key,
        },
        "expected_score": {
            "home": round(home_xg, 2),
            "away": round(away_xg, 2),
        },
        "total": round(home_xg + away_xg + p_draw, 2),  # +p_draw accounts for OT goal
        "spread": round(away_xg - home_xg, 1),
        "win_prob": {
            "home": round(p_home_ml, 4),
            "away": round(p_away_ml, 4),
        },
        "regulation_draw_prob": round(p_draw, 4),
        "puck_line": {
            "home_minus_1_5": round(p_home_m15, 4),
            "away_plus_1_5": round(p_away_p15, 4),
            "away_minus_1_5": round(p_away_m15, 4),
            "home_plus_1_5": round(p_home_p15, 4),
        },
        "over_under": ou_lines,
        "periods": periods,
        "first_period": {
            "scoreless": round(p_scoreless_p1, 4),
            "scoring": round(1 - p_scoreless_p1, 4),
        },
        "correct_scores": top_scores,
        "factors": {
            "home_pp": round(h_pp, 3) if h_pp else None,
            "away_pp": round(a_pp, 3) if a_pp else None,
            "home_pk": round(h_pk, 3) if h_pk else None,
            "away_pk": round(a_pk, 3) if a_pk else None,
            "home_sv": round(h_sv, 3) if h_sv else None,
            "away_sv": round(a_sv, 3) if a_sv else None,
            "home_shots": round(h_shots, 1),
            "away_shots": round(a_shots, 1),
            "home_fo": round(h_fo, 3),
            "away_fo": round(a_fo, 3),
        },
        "goalie_matchup": goalie_factor,
        "h2h": h2h_data,
    }


def generate_nhl_picks(home_key: str, away_key: str,
                       odds: dict | None = None) -> list[dict]:
    """
    Generate all NHL picks for a matchup with edge calculations.
    Similar to MLB's engine/picks.py but for hockey.
    """
    pred = predict_matchup(home_key, away_key)
    if not pred:
        return []

    odds = odds or {}
    wp = pred["win_prob"]
    pl = pred["puck_line"]

    h_abbr = pred["home"]["abbreviation"]
    a_abbr = pred["away"]["abbreviation"]

    JUICE_WALL = -200  # NHL juice wall

    def _implied(ml: int) -> float:
        if ml < 0:
            return abs(ml) / (abs(ml) + 100)
        return 100 / (ml + 100)

    picks = []

    # ── Moneyline ──
    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")

    if home_ml and home_ml >= JUICE_WALL:
        edge = (wp["home"] - _implied(home_ml)) * 100
        if edge > 0:
            picks.append({
                "type": "ML", "pick": h_abbr, "prob": round(wp["home"], 4),
                "edge": round(edge, 1), "odds": home_ml,
            })

    if away_ml and away_ml >= JUICE_WALL:
        edge = (wp["away"] - _implied(away_ml)) * 100
        if edge > 0:
            picks.append({
                "type": "ML", "pick": a_abbr, "prob": round(wp["away"], 4),
                "edge": round(edge, 1), "odds": away_ml,
            })

    # ── Totals (O/U) ──
    vegas_total = odds.get("over_under")
    if vegas_total and pred.get("over_under"):
        vt = float(vegas_total)
        # Find closest line
        best_key = None
        best_diff = 999
        for k in pred["over_under"]:
            diff = abs(float(k) - vt)
            if diff < best_diff:
                best_diff = diff
                best_key = k

        if best_key:
            ou = pred["over_under"][best_key]
            pick_over = ou["over"] > ou["under"]
            prob = max(ou["over"], ou["under"])
            label = f"{'Over' if pick_over else 'Under'} {vt}"

            real_odds = odds.get("over_odds") if pick_over else odds.get("under_odds")
            if real_odds:
                implied = _implied(real_odds)
            else:
                implied = 0.524
                real_odds = -110

            edge = (prob - implied) * 100
            if edge > 0 and real_odds >= JUICE_WALL:
                picks.append({
                    "type": "O/U", "pick": label, "prob": round(prob, 4),
                    "edge": round(edge, 1), "odds": real_odds,
                })

    # ── Puck Line ──
    home_pl_odds = odds.get("home_spread_odds")
    away_pl_odds = odds.get("away_spread_odds")
    home_pl_point = odds.get("home_spread_point")
    away_pl_point = odds.get("away_spread_point")

    # Derive from ML if no puck line data
    if home_pl_point is None and home_ml and away_ml:
        home_is_fav = home_ml < away_ml
        if home_is_fav:
            home_pl_point = -1.5
            away_pl_point = 1.5
            home_pl_odds = home_pl_odds or 170
            away_pl_odds = away_pl_odds or -200
        else:
            home_pl_point = 1.5
            away_pl_point = -1.5
            home_pl_odds = home_pl_odds or -200
            away_pl_odds = away_pl_odds or 170

    if home_pl_point is not None:
        # Home puck line
        if home_pl_point < 0:
            h_pl_prob = pl["home_minus_1_5"]
        else:
            h_pl_prob = pl["home_plus_1_5"]

        if home_pl_odds and home_pl_odds >= JUICE_WALL:
            h_edge = (h_pl_prob - _implied(home_pl_odds)) * 100
            if h_edge > 0:
                picks.append({
                    "type": "PL", "pick": f"{h_abbr} {home_pl_point:+.1f}",
                    "prob": round(h_pl_prob, 4),
                    "edge": round(h_edge, 1), "odds": home_pl_odds,
                })

    if away_pl_point is not None:
        if away_pl_point < 0:
            a_pl_prob = pl["away_minus_1_5"]
        else:
            a_pl_prob = pl["away_plus_1_5"]

        if away_pl_odds and away_pl_odds >= JUICE_WALL:
            a_edge = (a_pl_prob - _implied(away_pl_odds)) * 100
            if a_edge > 0:
                picks.append({
                    "type": "PL", "pick": f"{a_abbr} {away_pl_point:+.1f}",
                    "prob": round(a_pl_prob, 4),
                    "edge": round(a_edge, 1), "odds": away_pl_odds,
                })

    # Sort by edge
    picks.sort(key=lambda p: p["edge"], reverse=True)

    # Assign confidence
    for p in picks:
        if p["edge"] >= 8:
            p["confidence"] = "strong"
        elif p["edge"] >= 4:
            p["confidence"] = "moderate"
        else:
            p["confidence"] = "lean"

    return picks
