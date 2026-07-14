"""
NBA Q1 derivative market backtest.

Twin of engine.mlb/nhl_deriv_retrobt — replays every completed NBA
game with Q1 scoring data through the Q1 derivative pick generators
(Q1 Team Total, Q1 Total O/E), settles against nba_games.home_q1 /
away_q1, reports calibration + WR/ROI per bet_type.

Usage:
    python -m engine.nba_deriv_retrobt
    python -m engine.nba_deriv_retrobt --min-edge 4.0
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from typing import Any

logger = logging.getLogger(__name__)


def _payout(odds: int, won: bool) -> float:
    if not won:
        return -100.0
    if odds > 0:
        return float(odds)
    return 100.0 / abs(odds) * 100.0


def _settle_q1_team_total(pk: str, h_q1: int, a_q1: int, h: str, a: str) -> tuple[str, bool]:
    parts = pk.split()
    if len(parts) < 4:
        return "", False
    team, direction, line_s = parts[0], parts[2].lower(), parts[3]
    try:
        line = float(line_s)
    except ValueError:
        return "", False
    team_q1 = h_q1 if team == h else a_q1
    if team_q1 > line:
        return ("W" if direction == "over" else "L"), True
    if team_q1 < line:
        return ("L" if direction == "over" else "W"), True
    return "P", True


def _settle_q1_total_oe(pk: str, q1_total: int) -> tuple[str, bool]:
    parts = pk.split()
    if len(parts) < 3:
        return "", False
    direction = parts[2].lower()
    is_odd = q1_total % 2 == 1
    if direction == "odd":
        return ("W" if is_odd else "L"), True
    return ("W" if not is_odd else "L"), True


_SETTLE_FNS = {
    "Q1 Team Total": lambda p, h_q1, a_q1, h, a: _settle_q1_team_total(p, h_q1, a_q1, h, a),
    "Q1 Total O/E":  lambda p, h_q1, a_q1, h, a: _settle_q1_total_oe(p, h_q1 + a_q1),
}


def run(min_edge: float = 0.0, market_filter: set[str] | None = None,
        limit: int | None = None, verbose: bool = False) -> dict:
    from .db import get_conn as _nba_conn
    from .q1_predict import predict_q1
    from .derivative_picks import append_derivative_picks

    conn = _nba_conn()
    q = ("SELECT g.*, "
         "       (SELECT abbreviation FROM nba_teams WHERE id = g.home_team_id) AS home_abbr, "
         "       (SELECT abbreviation FROM nba_teams WHERE id = g.away_team_id) AS away_abbr "
         "FROM nba_games g "
         "WHERE home_q1 IS NOT NULL AND away_q1 IS NOT NULL "
         "ORDER BY date DESC")
    if limit:
        q += f" LIMIT {limit}"
    games = conn.execute(q).fetchall()
    logger.info("Running NBA derivative backtest over %d games", len(games))

    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"total": 0, "wins": 0, "losses": 0, "pushes": 0,
                 "profit": 0.0, "prob_sum": 0.0, "edge_sum": 0.0,
                 "buckets": defaultdict(lambda: {"n": 0, "wins": 0,
                                                  "prob_sum": 0.0})})

    for i, g in enumerate(games):
        g = dict(g)
        h_abbr = (g.get("home_abbr") or "").upper()
        a_abbr = (g.get("away_abbr") or "").upper()
        if not h_abbr or not a_abbr:
            continue
        h_q1 = int(g.get("home_q1") or 0)
        a_q1 = int(g.get("away_q1") or 0)
        season = g.get("season")

        try:
            pred = predict_q1(h_abbr, a_abbr, season=season, backtest=True)
        except Exception:
            continue
        if not pred:
            continue

        # Synthetic lines = league-typical Q1 totals (~29 home / ~28.5
        # away). Setting them to the model's own prediction would zero
        # out edge by construction; using a stable book-style line lets
        # the model genuinely bet against the consensus when its own
        # estimate diverges. League avg from the adaptive baseline
        # (LEAGUE_AVG_Q1_TOTAL ~58) split with a small home advantage.
        from .q1_predict import LEAGUE_AVG_Q1_TOTAL, HOME_Q1_BOOST
        league_home_q1 = (LEAGUE_AVG_Q1_TOTAL + HOME_Q1_BOOST) / 2
        league_away_q1 = (LEAGUE_AVG_Q1_TOTAL - HOME_Q1_BOOST) / 2

        def _round_half(x: float) -> float:
            return round(float(x) * 2) / 2

        odds = {
            "q1_team_total_home": {
                "line": _round_half(league_home_q1),
                "over_odds": -110, "under_odds": -110,
            },
            "q1_team_total_away": {
                "line": _round_half(league_away_q1),
                "over_odds": -110, "under_odds": -110,
            },
            "q1_total_oe": {"odd_odds": -110, "even_odds": -110},
        }

        picks: list[dict] = []
        try:
            append_derivative_picks(picks, pred, odds, h_abbr, a_abbr)
        except Exception:
            continue

        for p in picks:
            bt = p.get("type")
            if market_filter and bt not in market_filter:
                continue
            if (p.get("edge") or 0) < min_edge:
                continue
            settler = _SETTLE_FNS.get(bt)
            if not settler:
                continue
            try:
                result, ok = settler(p.get("pick", ""), h_q1, a_q1, h_abbr, a_abbr)
            except Exception:
                continue
            if not ok:
                continue

            s = stats[bt]
            s["total"] += 1
            prob = float(p.get("prob") or 0)
            odds_used = int(p.get("odds") or -110)
            s["prob_sum"] += prob
            s["edge_sum"] += p.get("edge") or 0
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

        if verbose and (i + 1) % 500 == 0:
            logger.info("  processed %d / %d games", i + 1, len(games))

    report: dict[str, Any] = {}
    for bt, s in stats.items():
        settled = s["wins"] + s["losses"]
        report[bt] = {
            "total": s["total"],
            "wins": s["wins"], "losses": s["losses"], "pushes": s["pushes"],
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
    ap.add_argument("--limit", type=int)
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
    print(f"  NBA Q1 DERIVATIVE BACKTEST  "
          f"(min_edge={args.min_edge}%, synthetic -110 juice)")
    print("=" * 72)
    print(f"{'Market':<16} {'N':>5} {'W-L-P':>10} {'WR':>7} {'ROI':>7} {'avg prob':>9}  {'avg edge':>9}")
    print("-" * 72)
    for bt in sorted(report):
        r = report[bt]
        print(f"{bt:<16} {r['total']:>5} "
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
