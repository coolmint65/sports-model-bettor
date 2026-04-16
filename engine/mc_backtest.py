"""
Monte Carlo vs. factor-model backtest.

Runs the MC simulator retroactively on completed games in the DB and
reports calibration vs. actual outcomes, alongside the existing factor
model's numbers. Lets us see whether MC is actually improving things
before we flip ENABLE_MLB_MC to primary.

Usage:
    python -m engine.mc_backtest mlb --days 14 --n-sims 10000
    python -m engine.mc_backtest mlb --days 30 --n-sims 50000

Expensive: 10k sims × ~15 games/day × 14 days = 2.1M game-sims for a
two-week MLB backtest. Start with a smaller --n-sims value while
iterating, then crank it once you're confident in the outputs.
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class _MarketStats:
    n: int = 0
    correct: int = 0
    squared_err: float = 0.0   # for probability calibration
    brier: float = 0.0

    @property
    def win_pct(self) -> float:
        return (self.correct / self.n * 100) if self.n else 0.0


def run_mlb_mc_backtest(days: int = 14, n_sims: int = 10_000,
                         season: int | None = None,
                         verbose: bool = False) -> dict:
    """Compare MC probabilities to actual outcomes across recent MLB games.

    For each finished game in the window we:
      1. Reconstruct the lineup + pitcher profiles as best we can
         (currently uses season-to-date stats; point-in-time would be
         more honest but is an optimization for later).
      2. Run the MC simulator and collect P(home_win), P(total > 8.5),
         P(NRFI), etc.
      3. Compare to the actual final score.
    """
    from .db import get_conn
    from .mc_mlb_run import run_mlb_mc

    conn = get_conn()
    season = season or datetime.now().year
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    games = conn.execute(
        "SELECT mlb_game_id, date, home_team_id, away_team_id, "
        "       home_pitcher_id, away_pitcher_id, "
        "       home_score, away_score, home_linescore, away_linescore, venue "
        "FROM games "
        "WHERE status = 'final' AND date >= ? AND season = ? "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date DESC",
        (cutoff, season),
    ).fetchall()

    if not games:
        return {"error": "No completed games in window", "games": 0}

    ml = _MarketStats()
    ou_85 = _MarketStats()  # O/U 8.5
    nrfi = _MarketStats()
    f5_ml = _MarketStats()

    skipped = 0
    start = time.time()
    for i, g in enumerate(games):
        g = dict(g)
        if not g.get("home_pitcher_id") or not g.get("away_pitcher_id"):
            skipped += 1
            continue
        try:
            mc = run_mlb_mc(
                home_team_id=g["home_team_id"],
                away_team_id=g["away_team_id"],
                home_pitcher_id=g["home_pitcher_id"],
                away_pitcher_id=g["away_pitcher_id"],
                venue=g.get("venue"),
                n_sims=n_sims,
                seed=g["mlb_game_id"],   # reproducible per game
            )
        except Exception as e:
            logger.warning("MC backtest skipped game %s: %s", g["mlb_game_id"], e)
            skipped += 1
            continue

        actual_home_won = g["home_score"] > g["away_score"]
        mc_home_wp = mc["win_prob"]["home"]
        ml.n += 1
        ml.correct += int((mc_home_wp >= 0.5) == actual_home_won)
        ml.brier += (mc_home_wp - (1 if actual_home_won else 0)) ** 2

        # O/U 8.5
        actual_total = g["home_score"] + g["away_score"]
        p_over_85 = mc["over_under"].get("8.5", {}).get("over")
        if p_over_85 is None:
            # Interpolate from the closest line we have in the dict
            p_over_85 = _closest_ou(mc["over_under"], 8.5)
        if p_over_85 is not None and actual_total != 8.5:
            actual_over = actual_total > 8.5
            ou_85.n += 1
            ou_85.correct += int((p_over_85 >= 0.5) == actual_over)
            ou_85.brier += (p_over_85 - (1 if actual_over else 0)) ** 2

        # NRFI (requires linescore)
        import json as _json
        try:
            h_inn = _json.loads(g.get("home_linescore") or "[]")
            a_inn = _json.loads(g.get("away_linescore") or "[]")
            if h_inn and a_inn:
                actual_nrfi = (h_inn[0] == 0 and a_inn[0] == 0)
                p_nrfi = mc.get("nrfi", {}).get("nrfi")
                if p_nrfi is not None:
                    nrfi.n += 1
                    nrfi.correct += int((p_nrfi >= 0.5) == actual_nrfi)
                    nrfi.brier += (p_nrfi - (1 if actual_nrfi else 0)) ** 2

                # F5 ML: sum first 5 innings
                if len(h_inn) >= 5 and len(a_inn) >= 5:
                    f5h = sum(h_inn[:5])
                    f5a = sum(a_inn[:5])
                    if f5h != f5a:
                        actual_f5_home_won = f5h > f5a
                        mc_f5_home_wp = mc.get("f5", {}).get("win_prob", {}).get("home")
                        if mc_f5_home_wp is not None:
                            f5_ml.n += 1
                            f5_ml.correct += int(
                                (mc_f5_home_wp >= 0.5) == actual_f5_home_won
                            )
                            f5_ml.brier += (
                                mc_f5_home_wp - (1 if actual_f5_home_won else 0)
                            ) ** 2
        except Exception:
            pass

        if verbose and (i + 1) % 10 == 0:
            elapsed = time.time() - start
            rate = (i + 1) / elapsed if elapsed else 0
            logger.info("  ...%d/%d games (%.1f g/s)", i + 1, len(games), rate)

    elapsed = time.time() - start
    return {
        "days": days,
        "n_sims": n_sims,
        "games_total": len(games),
        "games_skipped": skipped,
        "elapsed_sec": round(elapsed, 1),
        "ml": {
            "n": ml.n,
            "accuracy_pct": round(ml.win_pct, 1),
            "brier": round(ml.brier / max(ml.n, 1), 4),
        },
        "ou_8_5": {
            "n": ou_85.n,
            "accuracy_pct": round(ou_85.win_pct, 1),
            "brier": round(ou_85.brier / max(ou_85.n, 1), 4),
        },
        "nrfi": {
            "n": nrfi.n,
            "accuracy_pct": round(nrfi.win_pct, 1),
            "brier": round(nrfi.brier / max(nrfi.n, 1), 4),
        },
        "f5_ml": {
            "n": f5_ml.n,
            "accuracy_pct": round(f5_ml.win_pct, 1),
            "brier": round(f5_ml.brier / max(f5_ml.n, 1), 4),
        },
    }


def _closest_ou(ou_dict: dict, target: float) -> float | None:
    """Pick the O/U 'over' probability for the line closest to target."""
    if not ou_dict:
        return None
    best_key = None
    best_diff = 9999.0
    for k in ou_dict.keys():
        try:
            diff = abs(float(k) - target)
        except ValueError:
            continue
        if diff < best_diff:
            best_diff = diff
            best_key = k
    if best_key is None:
        return None
    return ou_dict[best_key].get("over")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sport", choices=["mlb"], help="Only MLB supported for now")
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--n-sims", type=int, default=10_000)
    parser.add_argument("--season", type=int, default=None)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(levelname)-7s %(message)s")

    if args.sport == "mlb":
        r = run_mlb_mc_backtest(days=args.days, n_sims=args.n_sims,
                                 season=args.season, verbose=args.verbose)
    else:
        print(f"Sport {args.sport} not supported yet.")
        return 1

    if "error" in r:
        print(f"Error: {r['error']}")
        return 1

    print(f"\n{'=' * 64}")
    print(f"  MC Backtest ({args.sport.upper()}) -- last {r['days']} days, "
          f"{r['n_sims']:,} sims/game, {r['elapsed_sec']}s")
    print(f"{'=' * 64}")
    print(f"  {r['games_total']} games, {r['games_skipped']} skipped")
    print(f"  Brier score: lower is better; 0.25 = coin flip, < 0.22 = solid\n")
    for key, label in [("ml", "Moneyline"), ("ou_8_5", "O/U 8.5"),
                        ("nrfi", "NRFI"), ("f5_ml", "F5 ML")]:
        m = r[key]
        print(f"    {label:<12} n={m['n']:>4}  acc={m['accuracy_pct']:>5.1f}%  "
              f"Brier={m['brier']:.4f}")
    print()
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
