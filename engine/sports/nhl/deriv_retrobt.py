"""
NHL derivative market backtest.

Twin of engine.mlb_deriv_retrobt — replays every completed NHL game
through the derivative pick generators, settles against the DB's
per-period scoring columns (home_p1/p2/p3 + away_p1/p2/p3), reports
calibration + WR/ROI per bet_type.

Same synthetic-odds caveat as MLB: historical HR derivative odds
aren't stored yet, so we use -110/-110 for totals, +100/-130 for
period totals, etc. Edge numbers are measured against this synthetic
book, not a specific sportsbook.

OT detection: inferred from `home_score + away_score > regulation
period sum`. Shootout rows in the DB aggregate to final_score but
regulation sum stays at P1+P2+P3.

Usage:
    python -m engine.nhl_deriv_retrobt
    python -m engine.nhl_deriv_retrobt --min-edge 4.0
    python -m engine.nhl_deriv_retrobt --markets "Period Total,Team Total"
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from typing import Any

logger = logging.getLogger(__name__)


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


# ── Settlers — mirror engine.nhl_tracker.settle_picks exactly ────

def _settle_team_total(pk: str, hs: int, as_: int, h: str, a: str) -> tuple[str, bool]:
    parts = pk.split()
    if len(parts) < 3:
        return "", False
    team, direction = parts[0], parts[1].lower()
    try:
        line = float(parts[2])
    except ValueError:
        return "", False
    team_goals = hs if team == h else as_
    if team_goals > line:
        return ("W" if direction == "over" else "L"), True
    if team_goals < line:
        return ("L" if direction == "over" else "W"), True
    return "P", True


def _settle_period_total(pk: str, periods: list[int]) -> tuple[str, bool]:
    # periods = [p1_total, p2_total, p3_total]
    parts = pk.split()
    if len(parts) < 4 or not parts[0].startswith("P"):
        return "", False
    try:
        n = int(parts[0][1:])
        line = float(parts[3])
    except ValueError:
        return "", False
    direction = parts[2].lower()
    if n < 1 or n > len(periods):
        return "", False
    pt = periods[n - 1]
    if pt > line:
        return ("W" if direction == "over" else "L"), True
    if pt < line:
        return ("L" if direction == "over" else "W"), True
    return "P", True


def _settle_period_bts(pk: str, h_periods: list[int], a_periods: list[int]) -> tuple[str, bool]:
    parts = pk.split()
    if len(parts) < 4 or not parts[0].startswith("P"):
        return "", False
    try:
        n = int(parts[0][1:])
    except ValueError:
        return "", False
    direction = parts[3].lower()
    if n < 1 or n > min(len(h_periods), len(a_periods)):
        return "", False
    bts = h_periods[n - 1] > 0 and a_periods[n - 1] > 0
    if direction == "yes":
        return ("W" if bts else "L"), True
    return ("W" if not bts else "L"), True


def _settle_period_dnb(pk: str, h_periods: list[int], a_periods: list[int],
                       h: str, a: str) -> tuple[str, bool]:
    """Accepts both old "P{n} DNB {team}" and new "P{n} {team}" formats
    so backtests run consistently after the 2026-04-28 pick-text rename.
    """
    parts = pk.split()
    if len(parts) < 2 or not parts[0].startswith("P"):
        return "", False
    try:
        n = int(parts[0][1:])
    except ValueError:
        return "", False
    pick_team = next((t for t in reversed(parts[1:]) if t != "DNB"), "")
    if not pick_team:
        return "", False
    if n < 1 or n > min(len(h_periods), len(a_periods)):
        return "", False
    hp, ap = h_periods[n - 1], a_periods[n - 1]
    if hp == ap:
        return "P", True
    if pick_team == h:
        return ("W" if hp > ap else "L"), True
    return ("W" if ap > hp else "L"), True


def _settle_total_oe(pk: str, total: int) -> tuple[str, bool]:
    parts = pk.split()
    if len(parts) < 2:
        return "", False
    direction = parts[1].lower()
    is_odd = total % 2 == 1
    if direction == "odd":
        return ("W" if is_odd else "L"), True
    return ("W" if not is_odd else "L"), True


def _settle_overtime(pk: str, went_to_ot: bool) -> tuple[str, bool]:
    parts = pk.split()
    if len(parts) < 2:
        return "", False
    direction = parts[1].lower()
    if direction == "yes":
        return ("W" if went_to_ot else "L"), True
    return ("W" if not went_to_ot else "L"), True


def _settle_bts_full(pk: str, hs: int, as_: int) -> tuple[str, bool]:
    parts = pk.split()
    if len(parts) < 2:
        return "", False
    direction = parts[1].lower()
    bts = hs > 0 and as_ > 0
    if direction == "yes":
        return ("W" if bts else "L"), True
    return ("W" if not bts else "L"), True


_SETTLE_FNS = {
    "Team Total":   lambda p, hs, as_, hp, ap, h, a, ot: _settle_team_total(p, hs, as_, h, a),
    "Period Total": lambda p, hs, as_, hp, ap, h, a, ot: _settle_period_total(p, [hp[i] + ap[i] for i in range(len(hp))]),
    "Period BTS":   lambda p, hs, as_, hp, ap, h, a, ot: _settle_period_bts(p, hp, ap),
    "Period DNB":   lambda p, hs, as_, hp, ap, h, a, ot: _settle_period_dnb(p, hp, ap, h, a),
    "Total O/E":    lambda p, hs, as_, hp, ap, h, a, ot: _settle_total_oe(p, hs + as_),
    "Overtime":     lambda p, hs, as_, hp, ap, h, a, ot: _settle_overtime(p, ot),
    "BTS":          lambda p, hs, as_, hp, ap, h, a, ot: _settle_bts_full(p, hs, as_),
}


def run(min_edge: float = 0.0, market_filter: set[str] | None = None,
        limit: int | None = None, verbose: bool = False) -> dict:
    from .db import get_conn as _nhl_conn
    from .predict import predict_matchup
    from .derivative_picks import append_derivative_picks
    from ...data import list_teams, load_team

    # Build abbreviation -> team-key map. predict_matchup wants the
    # directory-style "team_key" (e.g., "boston_bruins"), not the
    # "BOS" abbreviation we have in nhl_games.
    _abbr_to_key: dict[str, str] = {}
    for t in list_teams("NHL"):
        team = load_team("NHL", t["key"])
        if team:
            abbr = team.get("abbreviation", "")
            if abbr:
                _abbr_to_key[abbr] = t["key"]

    conn = _nhl_conn()
    q = ("SELECT g.*, "
         "       (SELECT abbreviation FROM nhl_teams WHERE id = g.home_team_id) AS home_abbr, "
         "       (SELECT abbreviation FROM nhl_teams WHERE id = g.away_team_id) AS away_abbr "
         "FROM nhl_games g WHERE status = 'final' "
         "AND home_p1 IS NOT NULL AND away_p1 IS NOT NULL "
         "ORDER BY date DESC")
    if limit:
        q += f" LIMIT {limit}"
    games = conn.execute(q).fetchall()
    logger.info("Running NHL derivative backtest over %d games", len(games))

    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"total": 0, "wins": 0, "losses": 0, "pushes": 0,
                 "profit": 0.0, "prob_sum": 0.0, "edge_sum": 0.0,
                 "buckets": defaultdict(lambda: {"n": 0, "wins": 0,
                                                  "prob_sum": 0.0})})

    for i, g in enumerate(games):
        g = dict(g)
        h_abbr = g.get("home_abbr", "")
        a_abbr = g.get("away_abbr", "")
        if not h_abbr or not a_abbr:
            continue
        hs = int(g.get("home_score") or 0)
        as_ = int(g.get("away_score") or 0)
        hp = [int(g.get("home_p1") or 0), int(g.get("home_p2") or 0), int(g.get("home_p3") or 0)]
        ap = [int(g.get("away_p1") or 0), int(g.get("away_p2") or 0), int(g.get("away_p3") or 0)]
        reg_total = sum(hp) + sum(ap)
        went_to_ot = (hs + as_) > reg_total

        h_key = _abbr_to_key.get(h_abbr)
        a_key = _abbr_to_key.get(a_abbr)
        if not h_key or not a_key:
            continue
        try:
            pred = predict_matchup(h_key, a_key, backtest=True)
        except Exception:
            continue
        if not pred:
            continue

        # League-typical lines (not model-prediction-based) so the model
        # has something real to disagree with. NHL avg ~3.0 goals/team.
        es = pred.get("expected_score") or {}
        periods = pred.get("periods") or []
        league_team_total = 3.0

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
            "period_totals": {
                str(p.get("period", f"P{j+1}"))[-1]: {
                    "line": 1.5, "over_odds": 110, "under_odds": -140,
                }
                for j, p in enumerate(periods[:3])
            },
            "period_bts": {
                str(p.get("period", f"P{j+1}"))[-1]: {
                    "yes_odds": 250, "no_odds": -300,
                }
                for j, p in enumerate(periods[:3])
            },
            "period_dnb": {
                str(p.get("period", f"P{j+1}"))[-1]: {
                    "home_ml": -110, "away_ml": -110,
                }
                for j, p in enumerate(periods[:3])
            },
            "total_oe":  {"odd_odds": -110, "even_odds": -110},
            "overtime":  {"yes_odds": 280, "no_odds": -380},
            "bts_full":  {"yes_odds": -300, "no_odds": 240},
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
                result, ok = settler(p.get("pick", ""), hs, as_, hp, ap,
                                      h_abbr, a_abbr, went_to_ot)
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
    print(f"  NHL DERIVATIVE BACKTEST  "
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
