"""Point-in-time predictor + market-baseline + B2B detection.

The PIT (point-in-time) model is the heart of the leak-free backtest.
Stats come strictly from games before the target date — no lookahead.
The output mirrors the production engine's prediction shape so the
runner can score it the same way it scores live picks.

Three pieces:
  - _compute_pit_stats: rolling 20-game window per team
  - _compute_market_prob: simpler model representing what bookmakers
    price; our edge is computed against this baseline
  - _pit_predict: the production-equivalent factor chain (xG, special
    teams, save%, momentum, win%, faceoffs, PK, L5 totals blending)
  - _check_b2b_from_games: detect back-to-back travel from the games
    list itself (no API call)
  - _predict_game: live-model fallback for the no-PIT path
"""

from __future__ import annotations
import logging
from datetime import datetime, timedelta

from ._helpers import _score_matrix, MAX_GOALS, _abbr_to_team_key

logger = logging.getLogger(__name__)


def _compute_market_prob(home_stats: dict, away_stats: dict) -> float:
    """Simulate what the market would price the home team at.

    Uses a simpler model than our prediction (just win%, goals) to represent
    what an average bettor/market maker would see. Our enhanced model should
    find edges OVER this baseline.
    """
    # Simple market: weighted average of win% and goals-based estimate
    home_win_pct = home_stats.get("win_pct", 0.5)
    away_win_pct = away_stats.get("win_pct", 0.5)
    wp_estimate = 0.5 + (home_win_pct - away_win_pct) * 0.6

    # Goals-based estimate
    home_gf = home_stats.get("goals_for_avg", 3.0)
    away_gf = away_stats.get("goals_for_avg", 3.0)
    home_ga = home_stats.get("goals_against_avg", 3.0)
    away_ga = away_stats.get("goals_against_avg", 3.0)

    home_xg = (home_gf + away_ga) / 2
    away_xg = (away_gf + home_ga) / 2
    if home_xg + away_xg > 0:
        goals_estimate = home_xg / (home_xg + away_xg)
    else:
        goals_estimate = 0.5

    market = wp_estimate * 0.5 + goals_estimate * 0.5
    return max(0.25, min(0.75, market))


def _compute_pit_stats(conn, team_id: int, game_date: str,
                       window: int = 20) -> dict | None:
    """Compute point-in-time stats for a team using only games before this date.

    Uses a rolling window of recent games from the nhl_games table so the
    backtest never peeks at future data.
    """
    rows = conn.execute("""
        SELECT home_team_id, away_team_id, home_score, away_score,
               home_pp_goals, home_pp_opps, away_pp_goals, away_pp_opps,
               home_shots, away_shots, home_faceoff_pct, away_faceoff_pct,
               date
        FROM nhl_games
        WHERE status = 'final' AND date < ?
          AND (home_team_id = ? OR away_team_id = ?)
        ORDER BY date DESC LIMIT ?
    """, (game_date, team_id, team_id, window)).fetchall()

    if len(rows) < 5:
        return None

    gf, ga, wins, losses = 0, 0, 0, 0
    home_gf, home_ga, home_games = 0, 0, 0
    away_gf, away_ga, away_games = 0, 0, 0
    pp_goals, pp_opps = 0, 0
    pk_goals_against, pk_opps_against = 0, 0
    shots_for, shots_against = 0, 0
    fo_sum, fo_count = 0.0, 0
    recent_wins = 0
    l5_gf, l5_ga = 0, 0
    l5_count = 0
    last_game_date = None

    for i, r in enumerate(rows):
        is_home = r["home_team_id"] == team_id
        if is_home:
            my_score = r["home_score"] or 0
            opp_score = r["away_score"] or 0
            home_gf += my_score
            home_ga += opp_score
            home_games += 1
            pp_goals += r["home_pp_goals"] or 0
            pp_opps += r["home_pp_opps"] or 0
            pk_goals_against += r["away_pp_goals"] or 0
            pk_opps_against += r["away_pp_opps"] or 0
            shots_for += r["home_shots"] or 0
            shots_against += r["away_shots"] or 0
            if r["home_faceoff_pct"]:
                fo_sum += r["home_faceoff_pct"]
                fo_count += 1
        else:
            my_score = r["away_score"] or 0
            opp_score = r["home_score"] or 0
            away_gf += my_score
            away_ga += opp_score
            away_games += 1
            pp_goals += r["away_pp_goals"] or 0
            pp_opps += r["away_pp_opps"] or 0
            pk_goals_against += r["home_pp_goals"] or 0
            pk_opps_against += r["home_pp_opps"] or 0
            shots_for += r["away_shots"] or 0
            shots_against += r["home_shots"] or 0
            if r["away_faceoff_pct"]:
                fo_sum += r["away_faceoff_pct"]
                fo_count += 1

        gf += my_score
        ga += opp_score
        won = my_score > opp_score
        if won:
            wins += 1
            if i < 5:
                recent_wins += 1
        else:
            losses += 1

        if i == 0 and r.get("date"):
            last_game_date = r["date"]

        if i < 5:
            l5_gf += my_score
            l5_ga += opp_score
            l5_count += 1

    n = len(rows)
    pp_pct = pp_goals / pp_opps if pp_opps > 0 else 0.20
    pk_pct = 1.0 - (pk_goals_against / pk_opps_against) if pk_opps_against > 0 else 0.80
    shots_pg = shots_for / n if shots_for > 0 else 30.0
    shots_against_pg = shots_against / n if shots_against > 0 else 30.0
    fo_pct = fo_sum / fo_count if fo_count > 0 else 50.0

    total_shots_against = shots_against
    if total_shots_against > 0:
        save_pct_proxy = 1.0 - (ga / total_shots_against)
    else:
        save_pct_proxy = 0.905

    return {
        "goals_for_avg": gf / n,
        "goals_against_avg": ga / n,
        "win_pct": wins / n,
        "home_gf_avg": home_gf / home_games if home_games > 0 else gf / n,
        "home_ga_avg": home_ga / home_games if home_games > 0 else ga / n,
        "away_gf_avg": away_gf / away_games if away_games > 0 else gf / n,
        "away_ga_avg": away_ga / away_games if away_games > 0 else ga / n,
        "pp_pct": pp_pct,
        "pk_pct": pk_pct,
        "faceoff_pct": fo_pct,
        "shots_per_game": shots_pg,
        "shots_against_pg": shots_against_pg,
        "save_pct_proxy": save_pct_proxy,
        "momentum": recent_wins / min(5, n),
        "l5_gf_avg": l5_gf / l5_count if l5_count > 0 else gf / n,
        "l5_ga_avg": l5_ga / l5_count if l5_count > 0 else ga / n,
        "games": n,
        "last_game_date": last_game_date,
    }


def _pit_predict(home_stats: dict, away_stats: dict) -> dict | None:
    """Enhanced Poisson prediction using point-in-time stats.

    Faithfully reproduces the production model's factor chain using only
    historical PIT data:
      1. Base xG from home/away splits (attack * defense / league_avg)
      2. Home ice edge (+0.15)
      3. Special teams (PP% vs league avg, ~2.5 PP/game weight)
      4. Goaltending / save% (suppress opponent xG)
      5. Shot volume adjustment
      6. Momentum (L5 win rate)
      7. Win% quality gap
      8. Poisson matrix -> ML (with OT home edge), PL, O/U
      9. Separate O/U xG using L5 scoring blend (40% recent, 60% season)
    """
    league_avg = 3.0
    home_edge = 0.15

    # ── 1. Base expected goals (home/away splits) ──
    home_off = home_stats["home_gf_avg"]
    home_def = home_stats["home_ga_avg"]
    away_off = away_stats["away_gf_avg"]
    away_def = away_stats["away_ga_avg"]

    home_xg = (home_off * away_def) / league_avg + home_edge / 2
    away_xg = (away_off * home_def) / league_avg - home_edge / 2

    # ── 2. Special teams: PP% vs league average ──
    league_pp = 0.20
    pp_weight = 2.5
    home_pp_edge = (home_stats["pp_pct"] - league_pp) * pp_weight
    away_pp_edge = (away_stats["pp_pct"] - league_pp) * pp_weight
    home_xg += home_pp_edge
    away_xg += away_pp_edge

    # ── 3. Goaltending / save% adjustment ──
    league_sv = 0.905
    h_sv = home_stats.get("save_pct_proxy", league_sv)
    a_sv = away_stats.get("save_pct_proxy", league_sv)
    if h_sv and h_sv > 0:
        away_xg *= max(0.85, min(1.15, league_sv / h_sv))
    if a_sv and a_sv > 0:
        home_xg *= max(0.85, min(1.15, league_sv / a_sv))

    # ── 4. Shot volume adjustment ──
    league_shots = 30.0
    if home_stats["shots_per_game"] > 0 and away_stats["shots_against_pg"] > 0:
        h_shot_factor = ((home_stats["shots_per_game"] / league_shots) +
                        (away_stats["shots_against_pg"] / league_shots)) / 2
        a_shot_factor = ((away_stats["shots_per_game"] / league_shots) +
                        (home_stats["shots_against_pg"] / league_shots)) / 2
        home_xg *= max(0.90, min(1.10, h_shot_factor))
        away_xg *= max(0.90, min(1.10, a_shot_factor))

    # ── 5. Momentum: L5 win rate ──
    home_momentum = (home_stats["momentum"] - 0.5) * 0.08
    away_momentum = (away_stats["momentum"] - 0.5) * 0.08
    home_xg *= (1 + home_momentum)
    away_xg *= (1 + away_momentum)

    # ── 6. Win% quality gap ──
    quality_diff = home_stats["win_pct"] - away_stats["win_pct"]
    home_xg *= (1 + quality_diff * 0.15)
    away_xg *= (1 - quality_diff * 0.15)

    # ── 7. Faceoff % adjustment ──
    league_fo = 50.0
    h_fo = home_stats.get("faceoff_pct", league_fo)
    a_fo = away_stats.get("faceoff_pct", league_fo)
    home_xg *= max(0.97, min(1.03, 1.0 + (h_fo - league_fo) * 0.003))
    away_xg *= max(0.97, min(1.03, 1.0 + (a_fo - league_fo) * 0.003))

    # ── 8. Penalty kill cross-reference ──
    league_pk = 0.80
    h_pk = home_stats.get("pk_pct", league_pk)
    a_pk = away_stats.get("pk_pct", league_pk)
    if away_stats["pp_pct"] > league_pp and h_pk < league_pk:
        pk_penalty = (league_pk - h_pk) * (away_stats["pp_pct"] / league_pp) * 0.5
        away_xg += min(0.15, pk_penalty)
    if home_stats["pp_pct"] > league_pp and a_pk < league_pk:
        pk_penalty = (league_pk - a_pk) * (home_stats["pp_pct"] / league_pp) * 0.5
        home_xg += min(0.15, pk_penalty)

    # ── Floor ──
    home_xg = max(home_xg, 1.0)
    away_xg = max(away_xg, 1.0)

    # ── O/U-specific xG: blend season avg with L5 scoring ──
    ou_home_xg = home_xg
    ou_away_xg = away_xg

    season_gf_h = home_stats["goals_for_avg"]
    l5_gf_h = home_stats.get("l5_gf_avg", season_gf_h)
    if season_gf_h > 0:
        ou_home_xg = ou_home_xg * 0.6 + (ou_home_xg * l5_gf_h / season_gf_h) * 0.4

    season_ga_h = home_stats["goals_against_avg"]
    l5_ga_h = home_stats.get("l5_ga_avg", season_ga_h)
    if season_ga_h > 0:
        ou_away_xg = ou_away_xg * 0.6 + (ou_away_xg * l5_ga_h / season_ga_h) * 0.4

    season_gf_a = away_stats["goals_for_avg"]
    l5_gf_a = away_stats.get("l5_gf_avg", season_gf_a)
    if season_gf_a > 0:
        ou_away_xg = ou_away_xg * 0.6 + (ou_away_xg * l5_gf_a / season_gf_a) * 0.4

    season_ga_a = away_stats["goals_against_avg"]
    l5_ga_a = away_stats.get("l5_ga_avg", season_ga_a)
    if season_ga_a > 0:
        ou_home_xg = ou_home_xg * 0.6 + (ou_home_xg * l5_ga_a / season_ga_a) * 0.4

    # ── Goalie impact on totals (amplified vs ML) ──
    if home_stats.get("save_pct_proxy") and away_stats.get("save_pct_proxy"):
        h_sv = home_stats["save_pct_proxy"]
        a_sv = away_stats["save_pct_proxy"]
        if abs(h_sv - league_sv) > 0.005:
            h_sv_factor = max(0.85, min(1.15, league_sv / h_sv))
            ou_away_xg *= h_sv_factor
        if abs(a_sv - league_sv) > 0.005:
            a_sv_factor = max(0.85, min(1.15, league_sv / a_sv))
            ou_home_xg *= a_sv_factor

    ou_home_xg = max(ou_home_xg, 1.0)
    ou_away_xg = max(ou_away_xg, 1.0)

    # ── Poisson matrix for ML and puck line ──
    matrix = _score_matrix(home_xg, away_xg)
    p_home = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                 for a in range(MAX_GOALS + 1) if h > a)
    p_away = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                 for a in range(MAX_GOALS + 1) if a > h)
    p_draw = sum(matrix[i][i] for i in range(MAX_GOALS + 1))

    # OT split: slight home edge (matches production exactly)
    p_home_ml = p_home + p_draw * 0.52
    p_away_ml = p_away + p_draw * 0.48

    # ── Puck line (±1.5) ──
    p_home_cover = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                       for a in range(MAX_GOALS + 1) if (h - a) >= 2)

    # ── O/U: use separate ou_matrix with L5-blended xGs ──
    ou_matrix = _score_matrix(ou_home_xg, ou_away_xg)
    ou_pred_total = ou_home_xg + ou_away_xg
    ou_p_draw = sum(ou_matrix[i][i] for i in range(MAX_GOALS + 1))
    ou_line = 5.5

    p_over = 0.0
    for h in range(MAX_GOALS + 1):
        for a in range(MAX_GOALS + 1):
            eff_total = (h + a + 1) if h == a else (h + a)
            if eff_total > ou_line:
                p_over += ou_matrix[h][a]

    return {
        "home_xg": home_xg,
        "away_xg": away_xg,
        "ou_home_xg": ou_home_xg,
        "ou_away_xg": ou_away_xg,
        "p_home": p_home_ml,
        "p_away": p_away_ml,
        "total": ou_pred_total + ou_p_draw,
        "p_over": p_over,
        "ou_line": ou_line,
        "p_home_cover": p_home_cover,
        "puck_line": {
            "home_minus_1_5": p_home_cover,
            "away_plus_1_5": 1 - p_home_cover,
        },
        "over_under": {},
    }


def _predict_game(home_abbr: str, away_abbr: str) -> dict | None:
    """Run the NHL prediction model for a matchup by abbreviation.
    Live-model path used when --no-pit is set."""
    from ..nhl_predict import predict_matchup

    home_key = _abbr_to_team_key(home_abbr)
    away_key = _abbr_to_team_key(away_abbr)

    if not home_key or not away_key:
        return None

    pred = predict_matchup(home_key, away_key)
    if not pred:
        return None

    home_xg = pred["expected_score"]["home"]
    away_xg = pred["expected_score"]["away"]

    return {
        "home_xg": home_xg,
        "away_xg": away_xg,
        "p_home": pred["win_prob"]["home"],
        "p_away": pred["win_prob"]["away"],
        "total": pred["total"],
        "puck_line": pred["puck_line"],
        "over_under": pred.get("over_under", {}),
    }


def _check_b2b_from_games(games: list, game_idx: int, team_id: int) -> bool:
    """Check if team_id played yesterday by looking at previous games in the list."""
    current_date = games[game_idx].get("date", "")
    if not current_date:
        return False
    yesterday = (datetime.strptime(current_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    for i in range(game_idx - 1, max(0, game_idx - 20), -1):
        g = games[i]
        if g.get("date") == yesterday:
            if g.get("home_team_id") == team_id or g.get("away_team_id") == team_id:
                return True
        elif g.get("date") < yesterday:
            break
    return False
