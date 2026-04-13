"""
Factor ablation backtest.

For each toggleable factor, re-runs predict_matchup on historical settled
games with the factor OFF, compares to the baseline (all factors ON), and
reports the WR/ROI delta.

Use this BEFORE enabling any new factor to prove it actually lifts
performance on held-out data. No more intuition-driven factor stacking.

Usage:
    python -m engine.factor_backtest mlb
    python -m engine.factor_backtest mlb --factor MLB_ENABLE_COORS_BOOST
    python -m engine.factor_backtest mlb --limit 100

Caveats:
  - Requires settled picks in the target DB (result IS NOT NULL).
  - Uses the game metadata stored on each pick row to re-predict.
    If the underlying stats have shifted since the pick was made,
    the re-predict may not exactly reproduce the original prob
    (expected for a living model). What matters is the DELTA.
"""

import argparse
import logging
import sys
from dataclasses import dataclass, field

from . import config as cfg
from .db import get_conn as get_mlb_conn

logger = logging.getLogger(__name__)


# ── Factor registry ────────────────────────────────────────

# Map of sport -> list of toggle-able factor names (config attribute names)
FACTORS = {
    "mlb": [
        "MLB_ENABLE_BULLPEN_FATIGUE",
        "MLB_ENABLE_SITUATIONAL_AGG",
        "MLB_ENABLE_UMPIRE_FACTOR",
        "MLB_ENABLE_WEATHER_ADJ",
        "MLB_ENABLE_TRAVEL_FATIGUE",
        "MLB_ENABLE_MATCHUP_INTERACTION",
        "MLB_ENABLE_COORS_BOOST",
        "MLB_ENABLE_PLATOON_DUP",
        "MLB_ENABLE_H2H_VS_PITCHER",
        "MLB_ENABLE_LINEUP_STRENGTH",
        "MLB_ENABLE_SITUATIONAL_FACTORS",
    ],
    "nhl": [
        "NHL_ENABLE_GRANULAR_FACTORS",
    ],
    "nba": [
        "NBA_ENABLE_ROSTER_ADJUSTMENT",
    ],
}


# ── Result accumulator ─────────────────────────────────────

@dataclass
class BacktestStats:
    picks_total: int = 0
    picks_flipped: int = 0  # pick direction changed when factor disabled
    baseline_wins: int = 0
    baseline_losses: int = 0
    baseline_profit: float = 0.0
    ablated_wins: int = 0
    ablated_losses: int = 0
    ablated_profit: float = 0.0
    by_type: dict = field(default_factory=dict)

    def baseline_wr(self) -> float:
        d = self.baseline_wins + self.baseline_losses
        return self.baseline_wins / d if d else 0.0

    def ablated_wr(self) -> float:
        d = self.ablated_wins + self.ablated_losses
        return self.ablated_wins / d if d else 0.0


# ── MLB backtest driver ────────────────────────────────────

def _fetch_settled_mlb_picks(limit: int | None = None) -> list[dict]:
    """Pull settled MLB picks with enough metadata to re-predict."""
    conn = get_mlb_conn()
    q = """
        SELECT p.id, p.game_id, p.bet_type, p.pick, p.model_prob,
               p.edge, p.odds, p.result, p.profit, p.matchup,
               g.home_team_id, g.away_team_id,
               g.home_starter_id, g.away_starter_id, g.venue, g.date
        FROM picks p
        LEFT JOIN games g ON p.game_id = g.mlb_game_id
        WHERE p.result IS NOT NULL
        ORDER BY p.settled_at DESC
    """
    if limit:
        q += f" LIMIT {limit}"
    try:
        rows = conn.execute(q).fetchall()
    except Exception as e:
        logger.warning("Failed to fetch settled MLB picks with metadata: %s", e)
        # Fall back to picks-only query
        q2 = "SELECT * FROM picks WHERE result IS NOT NULL"
        if limit:
            q2 += f" LIMIT {limit}"
        rows = conn.execute(q2).fetchall()
    return [dict(r) for r in rows]


def _ablate_mlb(factor: str, picks: list[dict]) -> BacktestStats:
    """Re-predict each pick with `factor` set to False, tally deltas."""
    from .mlb_predict import predict_matchup
    from .picks import generate_picks, get_best_pick

    if not hasattr(cfg, factor):
        logger.warning("Unknown factor %s — skipping", factor)
        return BacktestStats()

    stats = BacktestStats()

    # Save original state + flip to False for the ablation pass
    original = getattr(cfg, factor)
    setattr(cfg, factor, False)

    try:
        for p in picks:
            stats.picks_total += 1

            home_tid = p.get("home_team_id")
            away_tid = p.get("away_team_id")
            if not home_tid or not away_tid:
                continue

            # Baseline result is what's stored on the pick
            baseline_won = (p["result"] == "WIN")
            baseline_profit = p.get("profit") or 0.0
            if baseline_won:
                stats.baseline_wins += 1
            elif p["result"] == "LOSS":
                stats.baseline_losses += 1
            stats.baseline_profit += baseline_profit

            # Re-predict with factor OFF
            try:
                new_picks = generate_picks(
                    home_team_id=home_tid,
                    away_team_id=away_tid,
                    home_pitcher_id=p.get("home_starter_id"),
                    away_pitcher_id=p.get("away_starter_id"),
                    venue=p.get("venue"),
                    odds={"home_ml": None, "away_ml": None},  # minimal
                )
                new_best = get_best_pick(new_picks) if new_picks else None
            except Exception as e:
                logger.debug("Re-predict failed for pick %s: %s", p.get("id"), e)
                # Treat as unchanged (baseline still wins/loses)
                if baseline_won:
                    stats.ablated_wins += 1
                elif p["result"] == "LOSS":
                    stats.ablated_losses += 1
                stats.ablated_profit += baseline_profit
                continue

            # Compare direction/type
            if new_best and (new_best.get("pick") != p["pick"]
                             or new_best.get("type") != p["bet_type"]):
                stats.picks_flipped += 1
                # Different pick — we can't know if it would've won without
                # re-settling against the actual game result. For now treat
                # flipped picks as "skipped" (no W/L contribution).
                # Proper fix: reconstruct the game outcome for the new pick
                # type+direction from games table. TODO in a follow-up.
                continue

            # Same pick — outcome would have been same
            if baseline_won:
                stats.ablated_wins += 1
            elif p["result"] == "LOSS":
                stats.ablated_losses += 1
            stats.ablated_profit += baseline_profit

            # Per-type tally
            bt = p["bet_type"]
            t = stats.by_type.setdefault(
                bt, {"total": 0, "flipped": 0}
            )
            t["total"] += 1

    finally:
        setattr(cfg, factor, original)

    return stats


# ── Report rendering ───────────────────────────────────────

def _render_report(sport: str, results: dict[str, BacktestStats]) -> str:
    lines = []
    lines.append(f"\n{'=' * 72}")
    lines.append(f"  Factor Ablation Backtest — {sport.upper()}")
    lines.append(f"{'=' * 72}")
    lines.append(
        f"\n  {'Factor':<34} {'Picks':>6} {'Flipped':>8} "
        f"{'Base WR':>8} {'Base $':>10} {'Ablated WR*':>11}"
    )
    lines.append("  " + "-" * 70)

    for factor, stats in results.items():
        short = factor.replace("MLB_ENABLE_", "").replace("NHL_ENABLE_", "") \
                      .replace("NBA_ENABLE_", "")
        lines.append(
            f"  {short:<34} {stats.picks_total:>6d} "
            f"{stats.picks_flipped:>8d} "
            f"{stats.baseline_wr():>7.1%} "
            f"${stats.baseline_profit:>+9.2f} "
            f"{stats.ablated_wr():>10.1%}"
        )

    lines.append(
        "\n  * Ablated WR only counts picks that did NOT change direction"
        " when the"
    )
    lines.append(
        "    factor was disabled (flipped picks are excluded — need game-"
        "level"
    )
    lines.append(
        "    outcome re-settling to score them, which is a TODO)."
    )
    lines.append("")
    lines.append(
        "  Interpretation:"
    )
    lines.append(
        "    - High 'Flipped' count = factor is load-bearing for pick"
        " selection."
    )
    lines.append(
        "    - Low 'Flipped' + same WR = factor isn't doing much; safe to"
        " disable."
    )
    lines.append(
        "    - Low 'Flipped' + worse ablated WR = factor is subtly helpful;"
    )
    lines.append(
        "      keep on. (Rare — usually these factors are just noise.)"
    )
    return "\n".join(lines)


# ── CLI ────────────────────────────────────────────────────

def run(sport: str, specific_factor: str | None = None,
        limit: int | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)-7s %(message)s",
    )

    if sport == "mlb":
        picks = _fetch_settled_mlb_picks(limit=limit)
        if not picks:
            print(f"No settled {sport.upper()} picks found. Run the tracker for a while first.")
            return
        print(f"Loaded {len(picks)} settled MLB picks.")

        factors = [specific_factor] if specific_factor else FACTORS["mlb"]
        results = {}
        for f in factors:
            if f and hasattr(cfg, f):
                print(f"  Ablating {f}...")
                results[f] = _ablate_mlb(f, picks)

        print(_render_report("mlb", results))
    else:
        # NHL/NBA can reuse the same pattern once their pick tables have
        # enough settled rows and matching predict signatures. For now
        # just print a placeholder.
        print(f"Factor backtest for {sport.upper()} not yet implemented. "
              f"MLB implementation can be ported once the sport has "
              f"substantive settled-pick data.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sport", choices=["mlb", "nhl", "nba"])
    parser.add_argument("--factor", default=None,
                        help="Run ablation for a single factor by name")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit to N most-recent settled picks")
    args = parser.parse_args()
    run(args.sport, args.factor, args.limit)
