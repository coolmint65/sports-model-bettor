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

        # ── Moneyline ──
        model_home = wp.get("home", 0.5)
        model_away = wp.get("away", 0.5)
        ml_pick_home = model_home > model_away

        # Use -150/+130 as standard MLB favorite/dog odds
        if ml_pick_home:
            pick_prob = model_home
            implied = 0.60  # ~-150 implied
            odds = -150
        else:
            pick_prob = model_away
            implied = 0.435  # ~+130 implied
            odds = 130

        edge = (pick_prob - implied) * 100

        if edge >= min_edge:
            pick_correct = (ml_pick_home and home_won) or (not ml_pick_home and not home_won)
            payout = _calc_payout(odds, pick_correct)
            results["moneyline"]["wins" if pick_correct else "losses"] += 1
            results["moneyline"]["profit"] += payout
            game_entry["picks"]["ml"] = {
                "pick": h_abbr if ml_pick_home else a_abbr,
                "prob": round(pick_prob, 3),
                "edge": round(edge, 1),
                "result": "W" if pick_correct else "L",
                "payout": round(payout, 2),
            }

        # ── Over/Under ──
        # Use 8.5 as a standard total if no specific line
        ou_line = 8.5
        ou_pick_over = total_pred > ou_line
        p_over = 0.5
        if pred.get("over_under"):
            # Find closest line to 8.5
            for line_key, probs in pred["over_under"].items():
                if abs(float(line_key) - ou_line) < 0.5:
                    p_over = probs.get("over", 0.5)
                    ou_line = float(line_key)
                    break

        ou_pick_prob = p_over if ou_pick_over else (1 - p_over)
        ou_edge = (ou_pick_prob - 0.524) * 100  # -110 implied = 52.4%

        if ou_edge >= min_edge:
            if actual_total > ou_line:
                ou_correct = ou_pick_over
            elif actual_total < ou_line:
                ou_correct = not ou_pick_over
            else:
                results["over_under"]["pushes"] += 1
                ou_correct = None

            if ou_correct is not None:
                payout = _calc_payout(-110, ou_correct)
                results["over_under"]["wins" if ou_correct else "losses"] += 1
                results["over_under"]["profit"] += payout
                game_entry["picks"]["ou"] = {
                    "pick": f"{'Over' if ou_pick_over else 'Under'} {ou_line}",
                    "prob": round(ou_pick_prob, 3),
                    "edge": round(ou_edge, 1),
                    "result": "W" if ou_correct else "L",
                    "payout": round(payout, 2),
                }

        # ── NRFI ──
        nrfi_prob = fi.get("nrfi", 0.5)
        nrfi_pick = nrfi_prob > 0.50
        nrfi_pick_prob = nrfi_prob if nrfi_pick else fi.get("yrfi", 0.5)
        nrfi_edge = (nrfi_pick_prob - 0.524) * 100

        if nrfi_edge >= min_edge:
            # Check if first inning had runs (we don't have inning data,
            # so estimate from total - a rough proxy)
            # For proper backtesting, we'd need inning-by-inning scores
            # For now, use Poisson estimate of P(0 runs in 1st) from actual total
            actual_rpg = actual_total / 2
            first_inn_xr = actual_rpg * 0.105
            p_zero_actual = _poisson_prob(first_inn_xr, 0)
            # Simulate: if actual total was high, likely scored in 1st
            first_inning_scoreless = actual_total <= 6  # Rough heuristic

            nrfi_correct = (nrfi_pick and first_inning_scoreless) or \
                           (not nrfi_pick and not first_inning_scoreless)
            payout = _calc_payout(-120, nrfi_correct)
            results["nrfi"]["wins" if nrfi_correct else "losses"] += 1
            results["nrfi"]["profit"] += payout
            game_entry["picks"]["nrfi"] = {
                "pick": "NRFI" if nrfi_pick else "YRFI",
                "prob": round(nrfi_pick_prob, 3),
                "edge": round(nrfi_edge, 1),
                "result": "W" if nrfi_correct else "L",
                "payout": round(payout, 2),
            }

        # ── Run Line ──
        rl_home_cover = rl.get("home_minus_1_5", 0.5)
        rl_away_cover = rl.get("away_plus_1_5", 0.5)
        rl_pick_home = rl_home_cover > 0.50
        rl_pick_prob = rl_home_cover if rl_pick_home else rl_away_cover
        rl_edge = (rl_pick_prob - 0.524) * 100

        if rl_edge >= min_edge:
            margin = home_score - away_score
            if rl_pick_home:
                rl_correct = margin >= 2
            else:
                rl_correct = margin <= 1
            payout = _calc_payout(-110, rl_correct)
            results["run_line"]["wins" if rl_correct else "losses"] += 1
            results["run_line"]["profit"] += payout
            game_entry["picks"]["rl"] = {
                "pick": f"{h_abbr} -1.5" if rl_pick_home else f"{a_abbr} +1.5",
                "prob": round(rl_pick_prob, 3),
                "edge": round(rl_edge, 1),
                "result": "W" if rl_correct else "L",
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
