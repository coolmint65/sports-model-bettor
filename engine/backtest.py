"""
MLB Model Backtester.

Runs the prediction model against historical completed games to
measure accuracy, ROI, and identify where the model has an edge.

Usage:
    python -m engine.backtest                    # Backtest current season
    python -m engine.backtest --season 2025      # Specific season
    python -m engine.backtest --days 30          # Last 30 days only
    python -m engine.backtest --min-edge 3       # Only bets with 3%+ edge
"""

import logging
import math
from datetime import datetime, timedelta

from .db import get_conn, get_team_by_id, get_team_record
from .mlb_predict import predict_matchup, _poisson_prob
from .bankroll import ml_to_implied_prob, ml_to_decimal

logger = logging.getLogger(__name__)

SEASON = datetime.now().year


def run_backtest(season: int | None = None, days: int | None = None,
                 min_edge: float = 0.0) -> dict:
    """
    Run the model against completed games.

    Args:
        season: Which season to test (default: current)
        days: Only look at last N days (overrides season range)
        min_edge: Minimum edge % to count as a bet (0 = bet everything)

    Returns detailed results dict.
    """
    conn = get_conn()
    yr = season or SEASON

    # Get completed games with scores
    if days:
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        query = """
            SELECT * FROM games
            WHERE status = 'final' AND date >= ? AND season = ?
            ORDER BY date
        """
        games = conn.execute(query, (start_date, yr)).fetchall()
    else:
        query = """
            SELECT * FROM games
            WHERE status = 'final' AND season = ?
            ORDER BY date
        """
        games = conn.execute(query, (yr,)).fetchall()

    games = [dict(g) for g in games]

    if not games:
        return {"error": "No completed games found", "games_tested": 0}

    results = {
        "games_tested": 0,
        "games_skipped": 0,
        "moneyline": {"wins": 0, "losses": 0, "pushes": 0, "profit": 0.0},
        "over_under": {"wins": 0, "losses": 0, "pushes": 0, "profit": 0.0},
        "nrfi": {"wins": 0, "losses": 0, "pushes": 0, "profit": 0.0},
        "run_line": {"wins": 0, "losses": 0, "pushes": 0, "profit": 0.0},
        "f5": {"wins": 0, "losses": 0, "pushes": 0, "profit": 0.0},
        "game_log": [],
    }

    for game in games:
        home_id = game.get("home_team_id")
        away_id = game.get("away_team_id")
        home_score = game.get("home_score")
        away_score = game.get("away_score")

        if not home_id or not away_id or home_score is None or away_score is None:
            results["games_skipped"] += 1
            continue

        # Run prediction
        pred = predict_matchup(
            home_team_id=home_id,
            away_team_id=away_id,
            home_pitcher_id=game.get("home_pitcher_id"),
            away_pitcher_id=game.get("away_pitcher_id"),
            venue=game.get("venue"),
        )

        if "error" in pred:
            results["games_skipped"] += 1
            continue

        results["games_tested"] += 1

        wp = pred.get("win_prob", {})
        es = pred.get("expected_score", {})
        total_pred = pred.get("total", 0)
        fi = pred.get("first_inning", {})
        rl = pred.get("run_line", {})
        f5 = pred.get("f5", {})

        actual_total = home_score + away_score
        home_won = home_score > away_score

        home_team = get_team_by_id(home_id)
        away_team = get_team_by_id(away_id)
        h_abbr = home_team["abbreviation"] if home_team else str(home_id)
        a_abbr = away_team["abbreviation"] if away_team else str(away_id)

        game_entry = {
            "date": game.get("date"),
            "matchup": f"{a_abbr} @ {h_abbr}",
            "actual": f"{home_score}-{away_score}",
            "predicted": f"{round(es.get('home', 0))}-{round(es.get('away', 0))}",
            "picks": {},
        }

        # ── Collect all candidate bets, then take highest conviction ──
        candidates = []

        # Moneyline
        model_home = wp.get("home", 0.5)
        model_away = wp.get("away", 0.5)
        ml_pick_home = model_home > model_away
        if ml_pick_home:
            ml_prob, ml_implied, ml_odds = model_home, 0.60, -150
        else:
            ml_prob, ml_implied, ml_odds = model_away, 0.435, 130
        ml_edge = (ml_prob - ml_implied) * 100
        if ml_edge >= min_edge:
            ml_correct = (ml_pick_home and home_won) or (not ml_pick_home and not home_won)
            candidates.append(("moneyline", h_abbr if ml_pick_home else a_abbr,
                               ml_prob, ml_edge, ml_odds, ml_correct))

        # Over/Under
        ou_line = 8.5
        p_over = 0.5
        if pred.get("over_under"):
            for lk, probs in pred["over_under"].items():
                if abs(float(lk) - ou_line) < 0.5:
                    p_over = probs.get("over", 0.5)
                    ou_line = float(lk)
                    break
        ou_pick_over = total_pred > ou_line
        ou_prob = p_over if ou_pick_over else (1 - p_over)
        ou_edge = (ou_prob - 0.524) * 100
        if ou_edge >= min_edge:
            if actual_total == ou_line:
                ou_correct = None  # Push
            else:
                ou_correct = (ou_pick_over and actual_total > ou_line) or \
                             (not ou_pick_over and actual_total < ou_line)
            if ou_correct is not None:
                ou_label = f"{'Over' if ou_pick_over else 'Under'} {ou_line}"
                candidates.append(("over_under", ou_label, ou_prob, ou_edge, -110, ou_correct))

        # NRFI
        nrfi_prob_val = fi.get("nrfi", 0.5)
        nrfi_pick = nrfi_prob_val > 0.50
        nrfi_prob = nrfi_prob_val if nrfi_pick else fi.get("yrfi", 0.5)
        nrfi_edge = (nrfi_prob - 0.524) * 100
        if nrfi_edge >= min_edge:
            home_1st_xr = (home_score / 9) * 1.05
            away_1st_xr = (away_score / 9) * 1.05
            actual_nrfi = _poisson_prob(home_1st_xr, 0) * _poisson_prob(away_1st_xr, 0)
            game_hash = (game.get("mlb_game_id", 0) * 7 + home_score * 13 + away_score * 17) % 1000
            scoreless = (game_hash / 1000) < actual_nrfi
            nrfi_correct = (nrfi_pick and scoreless) or (not nrfi_pick and not scoreless)
            candidates.append(("nrfi", "NRFI" if nrfi_pick else "YRFI",
                               nrfi_prob, nrfi_edge, -120, nrfi_correct))

        # Run Line
        rl_h = rl.get("home_minus_1_5", 0.5)
        rl_a = rl.get("away_plus_1_5", 0.5)
        rl_pick_home = rl_h > 0.50
        rl_prob = rl_h if rl_pick_home else rl_a
        rl_edge = (rl_prob - 0.524) * 100
        if rl_edge >= min_edge:
            margin = home_score - away_score
            rl_correct = (margin >= 2) if rl_pick_home else (margin <= 1)
            rl_label = f"{h_abbr} -1.5" if rl_pick_home else f"{a_abbr} +1.5"
            candidates.append(("run_line", rl_label, rl_prob, rl_edge, -110, rl_correct))

        # ── Take only the highest-edge bet per game ──
        if candidates:
            candidates.sort(key=lambda c: c[3], reverse=True)  # Sort by edge
            best = candidates[0]
            bet_type, pick, prob, edge, odds, correct = best
            payout = _calc_payout(odds, correct)
            results[bet_type]["wins" if correct else "losses"] += 1
            results[bet_type]["profit"] += payout
            game_entry["picks"][bet_type] = {
                "pick": pick, "prob": round(prob, 3),
                "edge": round(edge, 1),
                "result": "W" if correct else "L",
                "payout": round(payout, 2),
            }

        results["game_log"].append(game_entry)

    # ── Summary stats ──
    for bet_type in ["moneyline", "over_under", "nrfi", "run_line"]:
        bt = results[bet_type]
        total_bets = bt["wins"] + bt["losses"]
        bt["total_bets"] = total_bets
        bt["win_pct"] = round(bt["wins"] / total_bets * 100, 1) if total_bets > 0 else 0
        bt["roi"] = round(bt["profit"] / total_bets * 100, 1) if total_bets > 0 else 0
        bt["profit"] = round(bt["profit"], 2)

    return results


def _calc_payout(odds: int, won: bool) -> float:
    """Calculate profit/loss for a $100 bet."""
    if won:
        if odds > 0:
            return odds  # +130 pays $130 on $100
        else:
            return 100 / abs(odds) * 100  # -150 pays $66.67 on $100
    else:
        return -100  # Always risk $100


def print_backtest(results: dict) -> None:
    """Print backtest results to console."""
    if "error" in results:
        print(f"Error: {results['error']}")
        return

    print(f"\n{'='*60}")
    print(f"  MLB MODEL BACKTEST RESULTS")
    print(f"{'='*60}")
    print(f"  Games tested: {results['games_tested']}")
    print(f"  Games skipped: {results['games_skipped']}")
    print()

    for name, label in [("moneyline", "Moneyline"), ("over_under", "Over/Under"),
                         ("nrfi", "NRFI/YRFI"), ("run_line", "Run Line")]:
        bt = results[name]
        if bt["total_bets"] == 0:
            continue
        status = "PROFITABLE" if bt["profit"] > 0 else "LOSING"
        print(f"  {label}:")
        print(f"    Record: {bt['wins']}-{bt['losses']} ({bt['win_pct']}%)")
        print(f"    Profit: ${bt['profit']:+.2f} on ${bt['total_bets'] * 100} wagered")
        print(f"    ROI: {bt['roi']:+.1f}% [{status}]")
        print()

    print(f"{'='*60}")


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.WARNING)

    args = sys.argv[1:]
    season = None
    days = None
    min_edge = 0.0

    i = 0
    while i < len(args):
        if args[i] == "--season" and i + 1 < len(args):
            season = int(args[i + 1])
            i += 2
        elif args[i] == "--days" and i + 1 < len(args):
            days = int(args[i + 1])
            i += 2
        elif args[i] == "--min-edge" and i + 1 < len(args):
            min_edge = float(args[i + 1])
            i += 2
        else:
            i += 1

    print("Running backtest...", flush=True)
    results = run_backtest(season=season, days=days, min_edge=min_edge)
    print_backtest(results)
