"""
MLB derivative market backtest.

Replays every completed game with linescore data through the
derivative pick generators, settles each pick against the real
outcome, and reports calibration + WR/ROI per bet_type.

Why this exists: the main `engine.backtest` / `mlb_retrobt` modules
iterate a hardcoded list of core bet types (ML / RL / F5 / O/U /
1st INN). They don't know how to generate or settle derivative
picks (Team Total, Inning Total, 1st Inn Winner, etc), so we had
zero retrospective data on derivative performance at launch. With
~8000 games of linescore history in the DB, this module produces
the same "BY BET TYPE" WR/ROI numbers the core markets get — but
pulled from historical replay instead of waiting weeks for the
paper-bet tracker to accumulate.

Caveats:
  - Historical HR derivative odds aren't stored (we started scraping
    them this week). Edge/ROI uses SYNTHETIC odds where real ones
    aren't available:
        Totals (Team / Inning / F5)  → -110 / -110
        3-way winners (1st Inn / F5) → +180 / +180 / -150 tie
        Yes/No (BTS / Extra)         → inferred from historical hit
                                        rate + 10% juice
  - Predictions use factor model only (no MC/GBM) for speed —
    ~50ms per game × 8000 games = ~7 min full run. MC/GBM wouldn't
    change pick direction materially on derivative markets.

Usage:
    python -m engine.mlb_deriv_retrobt                  # full report
    python -m engine.mlb_deriv_retrobt --markets Team Total,F5 Winner
    python -m engine.mlb_deriv_retrobt --min-edge 4.0  # playability filter
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ── Synthetic odds per bet type ───────────────────────────────
# Used when historical HR odds aren't stored. 10% juice applied
# symmetrically — the goal is to measure the model's edge against a
# fair book, not to replicate any specific sportsbook's pricing.

def _american_from_prob(prob: float, juice: float = 0.10) -> int:
    """Return American odds implying `prob` + juice. Caps at +/- 2000
    to avoid noise on extreme events."""
    if prob <= 0.01:
        return 2000
    if prob >= 0.99:
        return -10000
    # Apply juice: true prob p gets rescaled to p * (1 + juice)
    implied = min(0.99, prob * (1 + juice))
    if implied >= 0.5:
        american = -round(100 * implied / (1 - implied))
        return max(american, -2000)
    american = round(100 * (1 - implied) / implied)
    return min(american, 2000)


def _implied(ml: int) -> float:
    if ml < 0:
        return abs(ml) / (abs(ml) + 100)
    return 100 / (ml + 100)


def _payout(odds: int, won: bool) -> float:
    if not won:
        return -100.0
    if odds > 0:
        return float(odds)
    return 100.0 / abs(odds) * 100.0


# ── Per-pick settlers (mirror engine.tracker logic exactly) ────
# Each returns (result, settled_bool). result is 'W' / 'L' / 'P'.
# If the game didn't reach the required depth (e.g., F5 pick on a
# rain-shortened 4-inning game), settled_bool is False and the pick
# is dropped from the backtest sample.

def _settle_team_total(pk: str, hs: int, as_: int, h: str, a: str) -> tuple[str, bool]:
    parts = pk.split()
    if len(parts) < 3:
        return "", False
    team, direction, line_s = parts[0], parts[1].lower(), parts[2]
    try:
        line = float(line_s)
    except ValueError:
        return "", False
    team_runs = hs if team == h else as_
    if team_runs > line:
        return ("W" if direction == "over" else "L"), True
    if team_runs < line:
        return ("L" if direction == "over" else "W"), True
    return "P", True


def _settle_f5_team_total(pk: str, h_inn: list[int], a_inn: list[int],
                          h: str, a: str) -> tuple[str, bool]:
    if min(len(h_inn), len(a_inn)) < 5:
        return "", False
    parts = pk.split()
    if len(parts) < 4:
        return "", False
    team, direction, line_s = parts[0], parts[2].lower(), parts[3]
    try:
        line = float(line_s)
    except ValueError:
        return "", False
    team_f5 = sum(h_inn[:5]) if team == h else sum(a_inn[:5])
    if team_f5 > line:
        return ("W" if direction == "over" else "L"), True
    if team_f5 < line:
        return ("L" if direction == "over" else "W"), True
    return "P", True


def _settle_inning_total(pk: str, h_inn: list[int], a_inn: list[int]) -> tuple[str, bool]:
    parts = pk.split()
    if len(parts) < 4:
        return "", False
    try:
        inning_n = int(parts[1])
        line = float(parts[3])
    except ValueError:
        return "", False
    direction = parts[2].lower()
    if inning_n < 1 or inning_n > min(len(h_inn), len(a_inn)):
        return "", False
    inn_total = h_inn[inning_n - 1] + a_inn[inning_n - 1]
    if inn_total > line:
        return ("W" if direction == "over" else "L"), True
    if inn_total < line:
        return ("L" if direction == "over" else "W"), True
    return "P", True


def _settle_inning_bts(pk: str, h_inn: list[int], a_inn: list[int]) -> tuple[str, bool]:
    parts = pk.split()
    if len(parts) < 4:
        return "", False
    try:
        inning_n = int(parts[1])
    except ValueError:
        return "", False
    direction = parts[3].lower()
    if inning_n < 1 or inning_n > min(len(h_inn), len(a_inn)):
        return "", False
    bts_yes = h_inn[inning_n - 1] > 0 and a_inn[inning_n - 1] > 0
    if direction == "yes":
        return ("W" if bts_yes else "L"), True
    return ("W" if not bts_yes else "L"), True


def _settle_1st_inn_winner(pk: str, h_inn: list[int], a_inn: list[int],
                           h: str, a: str) -> tuple[str, bool]:
    if min(len(h_inn), len(a_inn)) < 1:
        return "", False
    h1, a1 = h_inn[0], a_inn[0]
    parts = pk.split()
    if len(parts) < 3:
        return "", False
    label = parts[2]
    if label.lower() == "tie":
        return ("W" if h1 == a1 else "L"), True
    if label == h:
        return ("W" if h1 > a1 else "L"), True
    return ("W" if a1 > h1 else "L"), True


def _settle_f5_winner(pk: str, h_inn: list[int], a_inn: list[int],
                      h: str, a: str) -> tuple[str, bool]:
    if min(len(h_inn), len(a_inn)) < 5:
        return "", False
    f5_h = sum(h_inn[:5])
    f5_a = sum(a_inn[:5])
    parts = pk.split()
    if len(parts) < 2:
        return "", False
    label = parts[1]
    if label.lower() == "tie":
        return ("W" if f5_h == f5_a else "L"), True
    if label == h:
        return ("W" if f5_h > f5_a else "L"), True
    return ("W" if f5_a > f5_h else "L"), True


def _settle_total_oe(pk: str, total_runs: int) -> tuple[str, bool]:
    parts = pk.split()
    if len(parts) < 2:
        return "", False
    direction = parts[1].lower()
    is_odd = total_runs % 2 == 1
    if direction == "odd":
        return ("W" if is_odd else "L"), True
    return ("W" if not is_odd else "L"), True


def _settle_extra_innings(pk: str, h_inn: list[int], a_inn: list[int]) -> tuple[str, bool]:
    parts = pk.split()
    if len(parts) < 3:
        return "", False
    direction = parts[2].lower()
    went_extras = max(len(h_inn), len(a_inn)) > 9
    if direction == "yes":
        return ("W" if went_extras else "L"), True
    return ("W" if not went_extras else "L"), True


_SETTLE_FNS = {
    "Team Total":      lambda p, hs, as_, h_inn, a_inn, h, a: _settle_team_total(p, hs, as_, h, a),
    "F5 Team Total":   lambda p, hs, as_, h_inn, a_inn, h, a: _settle_f5_team_total(p, h_inn, a_inn, h, a),
    "Inning Total":    lambda p, hs, as_, h_inn, a_inn, h, a: _settle_inning_total(p, h_inn, a_inn),
    "Inning BTS":      lambda p, hs, as_, h_inn, a_inn, h, a: _settle_inning_bts(p, h_inn, a_inn),
    "1st Inn Winner":  lambda p, hs, as_, h_inn, a_inn, h, a: _settle_1st_inn_winner(p, h_inn, a_inn, h, a),
    "F5 Winner":       lambda p, hs, as_, h_inn, a_inn, h, a: _settle_f5_winner(p, h_inn, a_inn, h, a),
    "Total O/E":       lambda p, hs, as_, h_inn, a_inn, h, a: _settle_total_oe(p, hs + as_),
    "Extra Innings":   lambda p, hs, as_, h_inn, a_inn, h, a: _settle_extra_innings(p, h_inn, a_inn),
}


# ── Backtest runner ──────────────────────────────────────────

def run(min_edge: float = 0.0, market_filter: set[str] | None = None,
        limit: int | None = None, verbose: bool = False) -> dict:
    """Run the backtest and return per-bet-type stats.

    Args:
        min_edge: skip picks whose synthetic edge is below this %
                  (applied AFTER pick generation — measures the model's
                  "playable" subset, not raw every-pick).
        market_filter: only backtest these bet types (None = all 8).
        limit: cap the number of games iterated (handy for quick tests).
        verbose: per-game progress logging.
    """
    from .db import get_conn, get_team_by_id
    from .mlb_predict import predict_matchup
    from .mlb_derivative_picks import append_derivative_picks

    conn = get_conn()
    q = ("SELECT * FROM games WHERE status = 'final' "
         "AND home_linescore IS NOT NULL AND away_linescore IS NOT NULL "
         "ORDER BY date DESC")
    if limit:
        q += f" LIMIT {limit}"
    games = conn.execute(q).fetchall()
    logger.info("Running derivative backtest over %d MLB games", len(games))

    # Stats accumulator keyed by bet_type
    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"total": 0, "wins": 0, "losses": 0, "pushes": 0,
                 "profit": 0.0, "prob_sum": 0.0, "edge_sum": 0.0,
                 "buckets": defaultdict(lambda: {"n": 0, "wins": 0,
                                                  "prob_sum": 0.0})})

    for i, g in enumerate(games):
        g = dict(g)
        home_team = get_team_by_id(g.get("home_team_id"))
        away_team = get_team_by_id(g.get("away_team_id"))
        if not home_team or not away_team:
            continue
        h = home_team["abbreviation"]
        a = away_team["abbreviation"]
        hs = int(g.get("home_score") or 0)
        as_ = int(g.get("away_score") or 0)
        try:
            h_inn = json.loads(g.get("home_linescore") or "[]")
            a_inn = json.loads(g.get("away_linescore") or "[]")
        except Exception:
            continue

        # Factor-only pred for speed. MC/GBM would shift scalar probs
        # but rarely flip pick direction on derivatives.
        try:
            pred = predict_matchup(
                home_team_id=g["home_team_id"],
                away_team_id=g["away_team_id"],
                home_pitcher_id=g.get("home_pitcher_id"),
                away_pitcher_id=g.get("away_pitcher_id"),
                venue=g.get("venue"),
                backtest=True,
            )
        except Exception:
            continue
        if "error" in pred:
            continue

        # Synthetic lines = LEAGUE-TYPICAL totals (not the model's own
        # prediction). Setting line = model_prediction would zero out
        # edge by construction (the model would bet against itself);
        # league-typical lines give the model something real to disagree
        # with. Numbers from the adaptive baseline + the F5 depth split
        # (~58% of full-game runs land in the first 5).
        from .mlb_predict import MLB_AVG_RPG
        league_team_total = round(MLB_AVG_RPG, 1)        # ~4.85 → use 4.5 line
        league_f5_total = round(MLB_AVG_RPG * 0.58, 1)   # ~2.8 → use 2.5 line
        es = pred.get("expected_score") or {}
        f5 = pred.get("f5") or {}
        innings = pred.get("innings") or []

        def _round_half(x: float) -> float:
            return round(float(x) * 2) / 2

        odds = {
            "team_total_home": {
                "line": _round_half(league_team_total),
                "over_odds": -110, "under_odds": -110,
            },
            "team_total_away": {
                "line": _round_half(league_team_total),
                "over_odds": -110, "under_odds": -110,
            },
            "f5_team_total_home": {
                "line": _round_half(league_f5_total),
                "over_odds": -110, "under_odds": -110,
            },
            "f5_team_total_away": {
                "line": _round_half(league_f5_total),
                "over_odds": -110, "under_odds": -110,
            },
            "inning_totals": {
                str(inn_data["inning"]): {
                    "line": 0.5, "over_odds": 100, "under_odds": -130,
                }
                for inn_data in innings[:9]
            },
            "inning_bts": {
                str(inn_data["inning"]): {"yes_odds": 400, "no_odds": -500}
                for inn_data in innings[:9]
            },
            "inning_winner": {"home_ml": 200, "away_ml": 200, "tie_ml": -130},
            "f5_winner":     {"home_ml": 150, "away_ml": 150, "tie_ml": 300},
            "total_oe":      {"odd_odds": -110, "even_odds": -110},
            "extra_innings": {"yes_odds": 700, "no_odds": -1200},
        }

        picks: list[dict] = []
        try:
            append_derivative_picks(picks, pred, odds, h, a)
        except Exception:
            continue

        for p in picks:
            bt = p.get("type")
            if market_filter and bt not in market_filter:
                continue
            edge = p.get("edge") or 0
            if edge < min_edge:
                continue
            settler = _SETTLE_FNS.get(bt)
            if not settler:
                continue
            try:
                result, ok = settler(p.get("pick", ""), hs, as_,
                                      h_inn, a_inn, h, a)
            except Exception:
                continue
            if not ok:
                continue

            s = stats[bt]
            s["total"] += 1
            prob = float(p.get("prob") or 0)
            odds_used = int(p.get("odds") or -110)
            s["prob_sum"] += prob
            s["edge_sum"] += edge
            # Bucket by predicted prob for calibration check
            bkey = f"{int(prob * 10) * 10}-{int(prob * 10) * 10 + 10}%"
            b = s["buckets"][bkey]
            b["n"] += 1
            b["prob_sum"] += prob
            if result == "W":
                s["wins"] += 1
                s["profit"] += _payout(odds_used, True)
                b["wins"] += 1
            elif result == "L":
                s["losses"] += 1
                s["profit"] += _payout(odds_used, False)
            else:
                s["pushes"] += 1
                # push: 0 profit

        if verbose and (i + 1) % 500 == 0:
            logger.info("  processed %d / %d games", i + 1, len(games))

    # Derive WR / ROI / calibration from accumulators
    report: dict[str, Any] = {}
    for bt, s in stats.items():
        settled = s["wins"] + s["losses"]
        report[bt] = {
            "total": s["total"],
            "wins": s["wins"],
            "losses": s["losses"],
            "pushes": s["pushes"],
            "wr": round(s["wins"] / settled * 100, 1) if settled else 0,
            "roi": round(s["profit"] / s["total"], 2) if s["total"] else 0,
            "profit": round(s["profit"], 2),
            "avg_prob": round(s["prob_sum"] / s["total"], 4) if s["total"] else 0,
            "avg_edge": round(s["edge_sum"] / s["total"], 2) if s["total"] else 0,
            "buckets": {k: {"n": b["n"], "wins": b["wins"],
                            "avg_prob": round(b["prob_sum"] / b["n"], 4) if b["n"] else 0,
                            "actual_wr": round(b["wins"] / b["n"], 4) if b["n"] else 0}
                        for k, b in sorted(s["buckets"].items()) if b["n"] > 0},
        }
    return report


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--min-edge", type=float, default=0.0)
    ap.add_argument("--markets", help="comma-separated bet-type list")
    ap.add_argument("--limit", type=int, help="cap games iterated")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    markets = set(args.markets.split(",")) if args.markets else None
    report = run(min_edge=args.min_edge, market_filter=markets,
                 limit=args.limit, verbose=args.verbose)

    print()
    print("=" * 72)
    print(f"  MLB DERIVATIVE BACKTEST  "
          f"(min_edge={args.min_edge}%, synthetic -110 juice)")
    print("=" * 72)
    print(f"{'Market':<18} {'N':>5} {'W-L-P':>10} {'WR':>7} {'ROI':>7} {'avg prob':>9}  {'avg edge':>9}")
    print("-" * 72)
    for bt in sorted(report):
        r = report[bt]
        print(f"{bt:<18} {r['total']:>5} "
              f"{r['wins']:>3}-{r['losses']:>3}-{r['pushes']:>2}  "
              f"{r['wr']:>6.1f}% {r['roi']:>+6.2f}  "
              f"{r['avg_prob']*100:>7.1f}%   {r['avg_edge']:>+7.2f}%")
    print()
    print("=" * 72)
    print("  CALIBRATION (predicted prob bucket -> actual win rate)")
    print("=" * 72)
    for bt in sorted(report):
        r = report[bt]
        if not r["buckets"]:
            continue
        print(f"\n{bt}")
        for k, b in r["buckets"].items():
            diff = b["actual_wr"] - b["avg_prob"]
            arrow = " <- overconfident" if diff < -0.05 else " <- underconfident" if diff > 0.05 else ""
            print(f"  {k:<10} n={b['n']:>4}  predicted {b['avg_prob']*100:>5.1f}%  "
                  f"actual {b['actual_wr']*100:>5.1f}%  diff {diff*100:>+5.1f}%{arrow}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
