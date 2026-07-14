"""
Live predictor backtest — Phase 3b validation.

Replays every historical NBA game (final + complete quarter linescores)
as a series of synthetic "live states" at quarter breaks (end-of-Q1,
end-of-Q2, end-of-Q3) and feeds them through `engine.live._predict`.

Why quarter breaks: live betting backtests need play-by-play + tick-
resolution historical odds, neither of which we collected during Phase
2. End-of-quarter is the best clean snapshot we have — linescores are
authoritative, and the "remaining game" prediction at that point is
the highest-impact moment for live picks (longest remaining time
window where edges are still actionable).

Three validation slices:

  1. **Home-win calibration** — bin predictions into 5%-buckets and
     report observed win rate per bucket. If `home_win_prob` is well
     calibrated, predicted-vs-observed should sit on the diagonal.
     Brier score gives a single-number summary.

  2. **Total prediction RMSE / bias** — for full game and per-period
     (Q4, H2 from end-of-Q2). Compares `total_mean` against the actual
     final / period total. Bias > 0 means we systematically project
     too high.

  3. **Synthetic ROI** — at synthetic -110 juice we score "would the
     picker have made money" for ML and TOTAL across edge thresholds
     {4, 6, 8, 10, 12, 15}%. Synthetic juice means we're testing
     predictor quality, not whether the live HR market actually
     mispriced things — a real live-odds backtest needs Phase 4+.

Run::

    python -m engine.live_backtest [--limit N] [--season YYYY]

CLI flags scope which games to include. Defaults: all final games with
complete linescores, ~4100 games × 3 snapshots = ~12k samples.
"""

from __future__ import annotations
import argparse
import logging
import math
from dataclasses import dataclass, field
from typing import Iterable

from .nba_db import get_conn
from .live._predict import (
    predict_live_nba_full,
    predict_live_nba_period,
    total_over_prob,
    NBA_QUARTER_SECONDS,
    NBA_REGULATION_SECONDS,
)

logger = logging.getLogger(__name__)


# - State synthesis -

def _synth_state_at_break(game_row, after_quarter: int) -> dict:
    """Build a live-state dict as if ESPN reported the game right after
    the end of `after_quarter` (1, 2, or 3).

    Linescores include only completed quarters; period field is set to
    after_quarter+1 with clock at 12:00 (full quarter ahead). Status
    detail mimics the pre-Q-tipoff ESPN string for completeness even
    though the predictor doesn't read `detail`.
    """
    home_ls = []
    away_ls = []
    for q in range(1, after_quarter + 1):
        home_ls.append(game_row[f"home_q{q}"])
        away_ls.append(game_row[f"away_q{q}"])

    home_score = sum(home_ls)
    away_score = sum(away_ls)

    # cur_period = after_quarter + 1 means we just stepped into the
    # next quarter at 12:00 on the clock.
    return {
        "sport": "nba",
        "game_id": game_row["game_id"],
        "matchup": f"{game_row['away_team_id']} @ {game_row['home_team_id']}",
        "home": {"abbr": "HOME", "score": home_score, "name": ""},
        "away": {"abbr": "AWAY", "score": away_score, "name": ""},
        "status": {
            "state": "in",
            "completed": False,
            "period": after_quarter + 1,
            "clock": "12:00",
            "clock_secs": NBA_QUARTER_SECONDS,
            "detail": f"Start of Q{after_quarter + 1}",
        },
        "linescores": {"home": home_ls, "away": away_ls},
    }


# - Aggregator dataclasses -

@dataclass
class CalibBin:
    n: int = 0
    wins: int = 0
    pred_sum: float = 0.0


@dataclass
class TotalErrors:
    n: int = 0
    sum_err: float = 0.0
    sum_sq: float = 0.0

    def add(self, predicted: float, actual: float) -> None:
        err = predicted - actual
        self.n += 1
        self.sum_err += err
        self.sum_sq += err * err

    @property
    def bias(self) -> float:
        return self.sum_err / self.n if self.n else 0.0

    @property
    def rmse(self) -> float:
        return math.sqrt(self.sum_sq / self.n) if self.n else 0.0


@dataclass
class RoiBucket:
    """Synthetic-juice tracker. -110 standard juice -> win pays +0.909,
    loss pays -1.000."""
    edge_floor: float
    n_picks: int = 0
    wins: int = 0
    profit: float = 0.0          # in units of 1.0 stake

    def record(self, model_prob: float, won: bool) -> None:
        # Edge vs implied prob at -110:  implied = 110/210 = 0.5238
        implied = 110.0 / 210.0
        edge_pct = (model_prob - implied) * 100.0
        if edge_pct < self.edge_floor:
            return
        self.n_picks += 1
        if won:
            self.wins += 1
            self.profit += 100.0 / 110.0      # +0.909
        else:
            self.profit += -1.0


@dataclass
class BacktestResults:
    home_win_calib: dict[int, CalibBin] = field(
        default_factory=lambda: {b: CalibBin() for b in range(20)})
    full_total_err_q1: TotalErrors = field(default_factory=TotalErrors)
    full_total_err_q2: TotalErrors = field(default_factory=TotalErrors)
    full_total_err_q3: TotalErrors = field(default_factory=TotalErrors)
    q4_total_err: TotalErrors = field(default_factory=TotalErrors)
    h2_total_err: TotalErrors = field(default_factory=TotalErrors)
    brier_sum: float = 0.0
    brier_n: int = 0

    # ROI buckets per edge floor for ML bets at end-of-Q3 (highest-
    # signal moment). Floors in % units.
    ml_roi: list[RoiBucket] = field(
        default_factory=lambda: [RoiBucket(f) for f in (4, 6, 8, 10, 12, 15)])
    # ROI for TOTAL bets vs the actual final total. Synthetic line =
    # full predicted_total at end-of-Q1 (no real prematch line). This
    # tests the predictor's drift, not market mispricing.
    total_roi: list[RoiBucket] = field(
        default_factory=lambda: [RoiBucket(f) for f in (4, 6, 8, 10, 12, 15)])


# - Main loop -

def run_backtest(season: int | None = None,
                 limit: int | None = None) -> BacktestResults:
    conn = get_conn()
    where = ["status = 'final'",
             "home_q1 IS NOT NULL", "home_q2 IS NOT NULL",
             "home_q3 IS NOT NULL", "home_q4 IS NOT NULL"]
    args: list = []
    if season:
        where.append("season = ?")
        args.append(season)
    sql = ("SELECT * FROM nba_games WHERE " + " AND ".join(where)
           + " ORDER BY date")
    if limit:
        sql += f" LIMIT {int(limit)}"
    rows = conn.execute(sql, tuple(args)).fetchall()
    logger.info("Backtest: %d games to replay", len(rows))

    res = BacktestResults()

    for row in rows:
        actual_home_total = row["home_score"]
        actual_away_total = row["away_score"]
        actual_total = actual_home_total + actual_away_total
        home_won = actual_home_total > actual_away_total

        # Iterate quarter-break snapshots. We capture predictions at
        # end-of-Q1 (most uncertainty), end-of-Q2 (half), end-of-Q3
        # (1 quarter remaining = highest model confidence).
        for after_q in (1, 2, 3):
            state = _synth_state_at_break(row, after_q)
            full = predict_live_nba_full(state)
            if not full:
                continue

            # Total RMSE bucket per snapshot
            err_target = {1: res.full_total_err_q1,
                          2: res.full_total_err_q2,
                          3: res.full_total_err_q3}[after_q]
            err_target.add(full["total_mean"], actual_total)

            # Brier + calibration only at end-of-Q3 (the highest-stakes
            # late-game decision point)
            if after_q == 3:
                p = full["home_win_prob"]
                res.brier_sum += (p - (1.0 if home_won else 0.0)) ** 2
                res.brier_n += 1
                bin_idx = min(19, int(p * 20))
                cb = res.home_win_calib[bin_idx]
                cb.n += 1
                cb.pred_sum += p
                if home_won:
                    cb.wins += 1

                # ML synthetic-ROI buckets (end-of-Q3 only)
                for bucket in res.ml_roi:
                    bucket.record(p, home_won)

            # TOTAL synthetic-ROI: at end-of-Q1, project the full-game
            # total and compare to actual. Picker would bet "Over" if
            # predicted > some line; here the synthetic line = predicted
            # mean (so probability of Over is ~50% by construction at
            # zero-edge). To get a useful ROI we'd need to bet against
            # a different line — defer this to a later iteration once
            # we record real prematch lines per game.
            #
            # For now compute at end-of-Q3: if predicted_total has high
            # variance vs actual we'll see RMSE, and the ROI buckets
            # below sit on the H2 prediction.
            if after_q == 2:
                # End of Q2 = predict H2 (Q3+Q4)
                pred_h2 = predict_live_nba_period(state, "H2")
                if pred_h2 and pred_h2.get("status") in ("current", "future"):
                    actual_h2 = (row["home_q3"] + row["away_q3"]
                                 + row["home_q4"] + row["away_q4"])
                    res.h2_total_err.add(pred_h2["total_mean"], actual_h2)
            if after_q == 3:
                # End of Q3 = predict Q4
                pred_q4 = predict_live_nba_period(state, "Q4")
                if pred_q4 and pred_q4.get("status") in ("current", "future"):
                    actual_q4 = row["home_q4"] + row["away_q4"]
                    res.q4_total_err.add(pred_q4["total_mean"], actual_q4)

    return res


def report(res: BacktestResults) -> str:
    out: list[str] = []
    out.append("=" * 70)
    out.append("LIVE PREDICTOR BACKTEST — NBA")
    out.append("=" * 70)

    out.append("\n# Full-game total prediction (RMSE / bias)")
    out.append(f"{'Snapshot':<22} {'N':>6}  {'RMSE':>7}  {'Bias':>7}")
    for label, e in (("end-of-Q1 -> final", res.full_total_err_q1),
                     ("end-of-Q2 -> final", res.full_total_err_q2),
                     ("end-of-Q3 -> final", res.full_total_err_q3)):
        out.append(f"{label:<22} {e.n:>6}  {e.rmse:>7.2f}  {e.bias:>+7.2f}")

    out.append("\n# Per-period total prediction")
    out.append(f"{'Period':<22} {'N':>6}  {'RMSE':>7}  {'Bias':>7}")
    out.append(f"{'H2 from end-of-Q2':<22} {res.h2_total_err.n:>6}  "
               f"{res.h2_total_err.rmse:>7.2f}  {res.h2_total_err.bias:>+7.2f}")
    out.append(f"{'Q4 from end-of-Q3':<22} {res.q4_total_err.n:>6}  "
               f"{res.q4_total_err.rmse:>7.2f}  {res.q4_total_err.bias:>+7.2f}")

    out.append("\n# Home-win calibration (predictions at end-of-Q3)")
    out.append(f"Brier score: {res.brier_sum / max(1, res.brier_n):.4f} "
               f"(lower = better; baseline 0.25 for 50/50 guesses)")
    out.append(f"{'Bucket':<14} {'N':>6}  {'Pred':>7}  {'Obs':>7}  {'delta':>7}")
    for b in range(20):
        cb = res.home_win_calib[b]
        if cb.n == 0:
            continue
        pred = cb.pred_sum / cb.n
        obs = cb.wins / cb.n
        out.append(f"{b*5:>3}-{b*5+5:<3}%      {cb.n:>6}  "
                   f"{pred:>7.3f}  {obs:>7.3f}  {(obs-pred):>+7.3f}")

    out.append("\n# Synthetic ROI at -110 juice (end-of-Q3 home ML)")
    out.append("Predictor-quality test, NOT market-mispricing test:")
    out.append("uses model home_win_prob vs implied 0.524, ignores actual")
    out.append("HR live line. Tests whether the predictor itself is")
    out.append("better than 53.8% accuracy; live-market profitability")
    out.append("requires real historical live odds (Phase 4+).")
    out.append("")
    out.append(f"{'Edge floor':>10}  {'N':>6}  {'WR%':>6}  {'ROI%':>7}  {'Profit':>8}")
    for bucket in res.ml_roi:
        if bucket.n_picks == 0:
            out.append(f"{bucket.edge_floor:>9.0f}%  {0:>6}  -     -       -")
            continue
        wr = bucket.wins / bucket.n_picks * 100
        roi = bucket.profit / bucket.n_picks * 100
        out.append(f"{bucket.edge_floor:>9.0f}%  {bucket.n_picks:>6}  "
                   f"{wr:>5.1f}%  {roi:>+6.1f}%  {bucket.profit:>+8.2f}")

    return "\n".join(out)


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Live NBA predictor backtest")
    parser.add_argument("--season", type=int, help="Restrict to one season")
    parser.add_argument("--limit", type=int, help="Cap N games")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    res = run_backtest(season=args.season, limit=args.limit)
    print(report(res))


if __name__ == "__main__":
    _cli()
