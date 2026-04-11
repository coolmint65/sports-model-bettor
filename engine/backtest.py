"""
MLB Model Backtester — v2.

Runs the prediction model against historical games with:
- Point-in-time stats (only data available before each game)
- Real inning-by-inning NRFI validation from linescore data
- Per-category results (ML, O/U, NRFI, RL all shown independently)
- Realistic odds (-140 avg favorite, -110 for RL/OU, -120 NRFI)

Usage:
    python -m engine.backtest                    # Current season
    python -m engine.backtest --season 2025      # Specific season
    python -m engine.backtest --min-edge 3       # Only 3%+ edge bets
"""

import json
import logging
import math
from datetime import datetime

from .db import get_conn, get_team_by_id
from .mlb_predict import (
    predict_matchup, _poisson_prob, _build_score_matrix,
    _win_probs_from_matrix, MLB_AVG_RPG, MLB_HOME_EDGE,
)
from .pit_stats import compute_team_stats_at_date, compute_pitcher_stats_at_date

logger = logging.getLogger(__name__)

SEASON = datetime.now().year

# Synthetic average MLB odds (used as FALLBACK when real historical odds are unavailable)
AVG_FAV_ODDS = -140      # Average favorite line
AVG_DOG_ODDS = 120       # Corresponding underdog
RL_ODDS = -110           # Run line standard
OU_ODDS = -110           # Over/under standard
NRFI_ODDS = -120         # NRFI standard


def _implied(ml: int) -> float:
    """Convert American odds to implied probability."""
    if ml < 0:
        return abs(ml) / (abs(ml) + 100)
    return 100 / (ml + 100)


def _load_mlb_odds_map(conn) -> dict:
    """Pre-load all historical MLB odds keyed by mlb_game_id.

    Returns a dict: mlb_game_id -> odds row dict.
    """
    try:
        rows = conn.execute("SELECT * FROM odds").fetchall()
    except Exception:
        return {}

    odds_map = {}
    for r in rows:
        row = dict(r)
        gid = row.get("game_id")
        if gid is not None:
            odds_map[gid] = row
    return odds_map


def run_backtest(season: int | None = None, days: int | None = None,
                 min_edge: float = 0.0, use_pit: bool = True) -> dict:
    """
    Run the model against completed games.

    Args:
        season: Which season to test
        days: Only last N days
        min_edge: Minimum edge % to count as a bet
        use_pit: Use point-in-time stats (slower but accurate)
    """
    conn = get_conn()
    yr = season or SEASON

    if days:
        from datetime import timedelta
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        games = conn.execute("""
            SELECT * FROM games
            WHERE status = 'final' AND date >= ? AND season = ?
            ORDER BY date
        """, (start_date, yr)).fetchall()
    else:
        games = conn.execute("""
            SELECT * FROM games
            WHERE status = 'final' AND season = ?
            ORDER BY date
        """, (yr,)).fetchall()

    games = [dict(g) for g in games]

    if not games:
        return {"error": "No completed games found", "games_tested": 0}

    # Pre-load historical odds for all games
    odds_map = _load_mlb_odds_map(conn)
    odds_real_count = 0
    odds_synthetic_count = 0

    results = {
        "season": yr,
        "games_tested": 0,
        "games_skipped": 0,
        "moneyline": _empty_cat(),
        "over_under": _empty_cat(),
        "nrfi": _empty_cat(),
        "run_line": _empty_cat(),
        # Best-bet-per-game summary
        "best_bet": _empty_cat(),
    }

    # Cache PIT stats to avoid recomputing for the same team/date
    pit_cache = {}

    for i, game in enumerate(games):
        home_id = game.get("home_team_id")
        away_id = game.get("away_team_id")
        home_score = game.get("home_score")
        away_score = game.get("away_score")
        game_date = game.get("date", "")

        if not home_id or not away_id or home_score is None or away_score is None:
            results["games_skipped"] += 1
            continue

        # ── Point-in-time stats ──
        if use_pit and game_date:
            home_pit = _cached_pit(pit_cache, home_id, game_date, yr)
            away_pit = _cached_pit(pit_cache, away_id, game_date, yr)
            home_sp_pit = None
            away_sp_pit = None
            if game.get("home_pitcher_id"):
                home_sp_pit = compute_pitcher_stats_at_date(
                    game["home_pitcher_id"], game_date, yr)
            if game.get("away_pitcher_id"):
                away_sp_pit = compute_pitcher_stats_at_date(
                    game["away_pitcher_id"], game_date, yr)
        else:
            home_pit = away_pit = home_sp_pit = away_sp_pit = None

        # Skip early-season games with no history
        if use_pit and home_pit and away_pit:
            if home_pit.get("games_played", 0) < 10 or away_pit.get("games_played", 0) < 10:
                results["games_skipped"] += 1
                continue

        # ── Build prediction from PIT data ──
        home_xr, away_xr = _predict_from_pit(
            home_pit, away_pit, home_sp_pit, away_sp_pit)

        # ── Situational adjustments ──
        from .situational import weather_factor, rest_fatigue_factor, pitcher_rest_factor

        wx = weather_factor(game.get("weather_temp"), game.get("weather_wind"),
                           game.get("venue"))
        home_rest = rest_fatigue_factor(home_id, game_date, yr)
        away_rest = rest_fatigue_factor(away_id, game_date, yr)
        home_sp_rest = pitcher_rest_factor(game.get("home_pitcher_id"), game_date, yr) if game.get("home_pitcher_id") else 1.0
        away_sp_rest = pitcher_rest_factor(game.get("away_pitcher_id"), game_date, yr) if game.get("away_pitcher_id") else 1.0

        home_xr *= wx * home_rest * away_sp_rest
        away_xr *= wx * away_rest * home_sp_rest

        total_pred = home_xr + away_xr
        matrix = _build_score_matrix(home_xr, away_xr, max_runs=15)
        p_home, p_away = _win_probs_from_matrix(matrix)

        results["games_tested"] += 1
        game_bets = []  # Track (edge, correct, odds) for best-bet selection
        actual_total = home_score + away_score
        home_won = home_score > away_score
        margin = home_score - away_score

        home_team = get_team_by_id(home_id)
        away_team = get_team_by_id(away_id)
        h_abbr = home_team["abbreviation"] if home_team else str(home_id)
        a_abbr = away_team["abbreviation"] if away_team else str(away_id)

        # ── Look up real historical odds for this game ──
        game_mlb_id = game.get("mlb_game_id")
        real_odds = odds_map.get(game_mlb_id) if game_mlb_id else None
        game_used_real_odds = real_odds is not None

        # ── Moneyline ──
        fav_home = p_home > p_away
        if fav_home:
            ml_prob = p_home
            if real_odds and real_odds.get("home_ml") is not None:
                ml_odds = real_odds["home_ml"]
                ml_implied = _implied(ml_odds)
            else:
                ml_odds = AVG_FAV_ODDS
                ml_implied = abs(AVG_FAV_ODDS) / (abs(AVG_FAV_ODDS) + 100)
        else:
            ml_prob = p_away
            if real_odds and real_odds.get("away_ml") is not None:
                ml_odds = real_odds["away_ml"]
                ml_implied = _implied(ml_odds)
            else:
                ml_odds = AVG_DOG_ODDS
                ml_implied = 100 / (AVG_DOG_ODDS + 100)

        ml_edge = (ml_prob - ml_implied) * 100
        if ml_edge >= min_edge:
            ml_correct = (fav_home and home_won) or (not fav_home and not home_won)
            _record_bet(results["moneyline"], ml_correct, ml_odds)
            game_bets.append((ml_edge, ml_correct, ml_odds))

        # ── Over/Under ──
        # Use real O/U line if available, otherwise derive from model
        if real_odds and real_odds.get("total") is not None:
            ou_line = real_odds["total"]
        else:
            ou_line = round(total_pred * 2) / 2  # Round to nearest 0.5

        # Model probability for the side it picks
        p_over = 0.0
        for h in range(len(matrix)):
            for a in range(len(matrix[0])):
                if (h + a) > ou_line:
                    p_over += matrix[h][a]
        ou_pick_over = p_over > 0.50
        ou_prob = p_over if ou_pick_over else (1 - p_over)

        # Use real O/U odds if available
        if real_odds and ou_pick_over and real_odds.get("over_odds") is not None:
            ou_odds = real_odds["over_odds"]
            ou_implied = _implied(ou_odds)
        elif real_odds and not ou_pick_over and real_odds.get("under_odds") is not None:
            ou_odds = real_odds["under_odds"]
            ou_implied = _implied(ou_odds)
        else:
            ou_odds = OU_ODDS
            ou_implied = abs(OU_ODDS) / (abs(OU_ODDS) + 100)

        ou_edge = (ou_prob - ou_implied) * 100

        if ou_edge >= min_edge:
            if actual_total == ou_line:
                pass  # Push, skip
            elif ou_pick_over:
                ou_correct = actual_total > ou_line
                _record_bet(results["over_under"], ou_correct, ou_odds)
                game_bets.append((ou_edge, ou_correct, ou_odds))
            else:
                ou_correct = actual_total < ou_line
                _record_bet(results["over_under"], ou_correct, ou_odds)
                game_bets.append((ou_edge, ou_correct, ou_odds))

        # ── NRFI (uses pitcher-specific + team-specific first-inning data) ──
        home_ls_raw = game.get("home_linescore")
        away_ls_raw = game.get("away_linescore")
        has_linescore = home_ls_raw and away_ls_raw

        if has_linescore:
            try:
                h_inn = json.loads(home_ls_raw)
                a_inn = json.loads(away_ls_raw)
                if len(h_inn) > 0 and len(a_inn) > 0:
                    actual_nrfi = (h_inn[0] == 0 and a_inn[0] == 0)

                    # Build NRFI probability from multiple factors:
                    # 1. Pitcher's first-inning scoreless rate
                    # 2. Team's first-inning scoring tendency
                    # 3. Generic Poisson as fallback

                    # P(away scores 0 in top 1st) — driven by home SP
                    # P(home scores 0 in bot 1st) — driven by away SP
                    p_away_zero = _nrfi_half_prob(
                        away_pit, home_sp_pit, away_xr)
                    p_home_zero = _nrfi_half_prob(
                        home_pit, away_sp_pit, home_xr)

                    model_nrfi = p_home_zero * p_away_zero

                    nrfi_pick = model_nrfi > 0.50
                    nrfi_prob = model_nrfi if nrfi_pick else (1 - model_nrfi)
                    nrfi_implied = abs(NRFI_ODDS) / (abs(NRFI_ODDS) + 100)
                    nrfi_edge = (nrfi_prob - nrfi_implied) * 100

                    if nrfi_edge >= min_edge:
                        nrfi_correct = (nrfi_pick and actual_nrfi) or \
                                       (not nrfi_pick and not actual_nrfi)
                        _record_bet(results["nrfi"], nrfi_correct, NRFI_ODDS)
                        game_bets.append((nrfi_edge, nrfi_correct, NRFI_ODDS))
            except (json.JSONDecodeError, IndexError):
                pass

        # ── Run Line ──
        p_home_cover = 0.0
        for h in range(len(matrix)):
            for a in range(len(matrix[0])):
                if (h - a) >= 2:
                    p_home_cover += matrix[h][a]
        p_away_cover = 1 - p_home_cover  # Away +1.5

        rl_pick_home = p_home_cover > 0.50
        rl_prob = p_home_cover if rl_pick_home else p_away_cover

        # Use real run line odds if available
        if rl_pick_home and real_odds and real_odds.get("home_spread_odds") is not None:
            rl_odds = real_odds["home_spread_odds"]
            rl_implied = _implied(rl_odds)
        elif not rl_pick_home and real_odds and real_odds.get("away_spread_odds") is not None:
            rl_odds = real_odds["away_spread_odds"]
            rl_implied = _implied(rl_odds)
        else:
            rl_odds = RL_ODDS
            rl_implied = abs(RL_ODDS) / (abs(RL_ODDS) + 100)

        rl_edge = (rl_prob - rl_implied) * 100

        if rl_edge >= min_edge:
            if rl_pick_home:
                rl_correct = margin >= 2
            else:
                rl_correct = margin <= 1
            _record_bet(results["run_line"], rl_correct, rl_odds)
            game_bets.append((rl_edge, rl_correct, rl_odds))

        # ── Best bet per game ──
        if game_bets:
            game_bets.sort(key=lambda x: x[0], reverse=True)
            best_edge, best_correct, best_odds = game_bets[0]
            _record_bet(results["best_bet"], best_correct, best_odds)

        # Track real vs synthetic odds usage for this game
        if game_bets:
            if game_used_real_odds:
                odds_real_count += 1
            else:
                odds_synthetic_count += 1

    # ── Compute summaries ──
    for cat in ["moneyline", "over_under", "nrfi", "run_line", "best_bet"]:
        _summarize(results[cat])

    # Odds source disclosure
    total_odds_games = odds_real_count + odds_synthetic_count
    results["odds_real_count"] = odds_real_count
    results["odds_synthetic_count"] = odds_synthetic_count
    if total_odds_games > 0:
        results["odds_real_pct"] = round(odds_real_count / total_odds_games * 100, 1)
        results["odds_synthetic_pct"] = round(odds_synthetic_count / total_odds_games * 100, 1)
    else:
        results["odds_real_pct"] = 0.0
        results["odds_synthetic_pct"] = 0.0

    return results


def _predict_from_pit(home_pit, away_pit, home_sp_pit, away_sp_pit):
    """
    Generate expected runs from point-in-time stats.
    Falls back to league averages when data is missing.
    """
    # Team offense
    if home_pit and home_pit.get("runs_pg"):
        home_off = home_pit["runs_pg"]
    else:
        home_off = MLB_AVG_RPG

    if away_pit and away_pit.get("runs_pg"):
        away_off = away_pit["runs_pg"]
    else:
        away_off = MLB_AVG_RPG

    # Pitcher adjustment
    home_sp_factor = 1.0
    away_sp_factor = 1.0

    if home_sp_pit and home_sp_pit.get("era") and home_sp_pit["games_started"] >= 3:
        home_sp_factor = home_sp_pit["era"] / 4.10  # vs league avg ERA
        home_sp_factor = max(0.60, min(1.50, home_sp_factor))

    if away_sp_pit and away_sp_pit.get("era") and away_sp_pit["games_started"] >= 3:
        away_sp_factor = away_sp_pit["era"] / 4.10
        away_sp_factor = max(0.60, min(1.50, away_sp_factor))

    # Home scores against away SP, away scores against home SP
    home_xr = home_off * away_sp_factor
    away_xr = away_off * home_sp_factor

    # Home edge
    home_xr += MLB_HOME_EDGE / 2
    away_xr -= MLB_HOME_EDGE / 2

    # Floor
    home_xr = max(home_xr, 1.5)
    away_xr = max(away_xr, 1.5)

    return home_xr, away_xr


def _cached_pit(cache, team_id, date, season):
    """Cache point-in-time stats to avoid recomputing."""
    key = (team_id, date)
    if key not in cache:
        cache[key] = compute_team_stats_at_date(team_id, date, season)
    return cache[key]


def _nrfi_half_prob(batting_team_pit, opp_pitcher_pit, team_xr):
    """
    Probability that a team scores 0 runs in their half of the 1st inning.

    Uses three signals blended by confidence:
    1. Pitcher's first-inning scoreless % (most predictive, direct measurement)
    2. Team's first-inning scoring % (how aggressive is their lineup in 1st)
    3. Poisson from expected runs (generic fallback)
    """
    # Weight allocation: pitcher > team > generic
    # Pitcher first-inning data is most predictive because the same pitcher
    # faces the same slot in the lineup every start
    signals = []
    weights = []

    # Signal 1: Pitcher's first-inning scoreless rate
    if opp_pitcher_pit and opp_pitcher_pit.get("first_inning_scoreless_pct") is not None:
        starts = opp_pitcher_pit.get("first_inning_starts", 0)
        if starts >= 5:  # Need decent sample
            p_scoreless = opp_pitcher_pit["first_inning_scoreless_pct"]
            signals.append(p_scoreless)
            weights.append(min(starts / 15, 1.0) * 0.50)  # Up to 50% weight

    # Signal 2: Team's first-inning scoring tendency
    if batting_team_pit and batting_team_pit.get("first_inning_score_pct") is not None:
        fi_games = batting_team_pit.get("first_inning_games", 0)
        if fi_games >= 15:
            p_team_scores = batting_team_pit["first_inning_score_pct"]
            p_team_zero = 1 - p_team_scores
            signals.append(p_team_zero)
            weights.append(min(fi_games / 50, 1.0) * 0.30)  # Up to 30% weight

    # Signal 3: Generic Poisson from expected runs
    first_inn_xr = team_xr * 0.105
    generic_p_zero = _poisson_prob(first_inn_xr, 0)
    signals.append(generic_p_zero)
    weights.append(0.20)  # Always 20% weight for generic

    # Weighted blend
    total_weight = sum(weights)
    if total_weight == 0:
        return generic_p_zero

    blended = sum(s * w for s, w in zip(signals, weights)) / total_weight
    return max(0.01, min(0.99, blended))


def _empty_cat():
    return {"wins": 0, "losses": 0, "pushes": 0, "profit": 0.0}


def _record_bet(cat, won, odds):
    if won:
        cat["wins"] += 1
        if odds > 0:
            cat["profit"] += odds
        else:
            cat["profit"] += (100 / abs(odds)) * 100
    else:
        cat["losses"] += 1
        cat["profit"] -= 100


def _summarize(cat):
    total = cat["wins"] + cat["losses"]
    cat["total_bets"] = total
    cat["win_pct"] = round(cat["wins"] / total * 100, 1) if total > 0 else 0
    cat["roi"] = round(cat["profit"] / (total * 100) * 100, 1) if total > 0 else 0
    cat["profit"] = round(cat["profit"], 2)


def print_backtest(results: dict) -> None:
    if "error" in results:
        print(f"Error: {results['error']}")
        return

    print(f"\n{'='*60}")
    print(f"  MLB MODEL BACKTEST — {results.get('season', '?')} Season")
    print(f"{'='*60}")
    print(f"  Games tested: {results['games_tested']}")
    print(f"  Games skipped: {results['games_skipped']}")
    real_pct = results.get('odds_real_pct', 0)
    synth_pct = results.get('odds_synthetic_pct', 0)
    real_n = results.get('odds_real_count', 0)
    synth_n = results.get('odds_synthetic_count', 0)
    print(f"  Odds source: {real_pct}% real historical ({real_n} games), "
          f"{synth_pct}% synthetic fallback ({synth_n} games)")
    if synth_n > 0 and real_n == 0:
        print(f"  NOTE: No historical odds found in DB. All evaluations use synthetic pricing.")
        print(f"        NRFI odds always use synthetic (-120) as no NRFI odds are stored.")
    print()

    for name, label in [("moneyline", "Moneyline"), ("over_under", "Over/Under"),
                         ("nrfi", "NRFI/YRFI"), ("run_line", "Run Line")]:
        bt = results[name]
        if bt["total_bets"] == 0:
            print(f"  {label}: No qualifying bets")
            continue
        status = "PROFITABLE" if bt["profit"] > 0 else "LOSING"
        print(f"  {label}:")
        print(f"    Record: {bt['wins']}-{bt['losses']} ({bt['win_pct']}%)")
        print(f"    Profit: ${bt['profit']:+.2f} per $100 flat bets")
        print(f"    ROI: {bt['roi']:+.1f}% [{status}]")
        print()

    print(f"{'='*60}")


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.WARNING)

    args = sys.argv[1:]
    season = None
    min_edge = 3.0

    i = 0
    while i < len(args):
        if args[i] == "--season" and i + 1 < len(args):
            season = int(args[i + 1])
            i += 2
        elif args[i] == "--min-edge" and i + 1 < len(args):
            min_edge = float(args[i + 1])
            i += 2
        elif args[i] == "--days" and i + 1 < len(args):
            i += 2  # days handled by run_backtest
        else:
            i += 1

    days_val = None
    for j, a in enumerate(args):
        if a == "--days" and j + 1 < len(args):
            days_val = int(args[j + 1])

    print(f"Running backtest (season={season or SEASON}, min_edge={min_edge}%)...",
          flush=True)
    results = run_backtest(season=season, days=days_val, min_edge=min_edge)
    print_backtest(results)
