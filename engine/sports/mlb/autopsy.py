"""
MLB tracker autopsy — Phase 4 starting point.

Slices the settled picks history into (bet_type × direction × edge_bucket)
cells and reports W/L/profit/ROI per cell. The point isn't to suggest
fixes — it's to surface which specific cells are bleeding so the
calibration / shrinkage / weight-tuning workstreams downstream of this
have a concrete target list instead of guessing at universal knobs.

Run::

    python -m engine.mlb_autopsy
    python -m engine.mlb_autopsy --since 2025-10-01
    python -m engine.mlb_autopsy --bet-type "ALT O/U"
    python -m engine.mlb_autopsy --min-n 5

Each cell shows: N, W-L-P, WR%, profit, ROI%. Sorted by ROI ascending
(worst first) so the bleeders lead. Cells below `--min-n` are folded
into per-bet-type rollups.

Direction taxonomy:

  ML / RL / F5 ML / F5 RL / F5 Winner / ALT RL → home / away
      (decoded from pick text vs games.matchup home_abbr/away_abbr)
  O/U / ALT O/U / F5 O/U / Team Total          → over / under
  1st INN                                       → nrfi / yrfi
  F5 Team Total                                 → over / under (team)

Edge bucket boundaries (in %): 0, 2, 4, 6, 8, 10, 15, 20, 100. The
0-2% bucket should be empty by default (edge floor is 4%) but is
included in case overrides drop a sub-floor pick through.
"""

from __future__ import annotations
import argparse
import logging
import sqlite3
from collections import defaultdict
from typing import Iterable

from ...db import get_conn

logger = logging.getLogger(__name__)


# ── Edge bucket boundaries ──
_EDGE_BUCKETS = [(0, 2), (2, 4), (4, 6), (6, 8),
                 (8, 10), (10, 15), (15, 20), (20, 100)]


def _bucket_for(edge: float) -> str:
    """Return the bucket label for an edge value in %. Edge values are
    stored as percentages (0-100) in the picks table."""
    for lo, hi in _EDGE_BUCKETS:
        if lo <= edge < hi:
            return f"{lo}-{hi}%"
    return "20+%"


# ── Direction extraction ──

def _direction_for(bet_type: str, pick_text: str,
                   matchup: str) -> str:
    """Categorize a pick's direction. Returns "?" when the pick text
    doesn't clearly resolve — those land in an "unknown" bucket so we
    can flag them for cleanup separately."""
    bt = (bet_type or "").upper().strip()
    pk = (pick_text or "").strip()
    pk_lower = pk.lower()

    if bt == "1ST INN":
        if pk_lower.startswith("nrfi"):
            return "nrfi"
        if pk_lower.startswith("yrfi"):
            return "yrfi"
        return "?"

    if bt in ("O/U", "ALT O/U", "F5 O/U", "TEAM TOTAL", "F5 TEAM TOTAL"):
        if pk_lower.startswith("over"):
            return "over"
        if pk_lower.startswith("under"):
            return "under"
        return "?"

    # ML / RL / F5 ML / F5 RL / F5 WINNER / ALT RL — team-side picks.
    # Resolve by checking if pick's first token matches home or away
    # abbr from the matchup string.
    if " @ " in (matchup or ""):
        away_abbr, home_abbr = matchup.split(" @ ", 1)
        away_abbr = (away_abbr or "").strip().upper()
        home_abbr = (home_abbr or "").strip().upper()
        first = (pk.split()[0] if pk.split() else "").upper()
        # Pick text might be a full team name. Match on prefix.
        if first == home_abbr or pk.upper().startswith(home_abbr + " "):
            return "home"
        if first == away_abbr or pk.upper().startswith(away_abbr + " "):
            return "away"
        # Multi-word team name fallback. e.g. "Washington Nationals +1.5"
        # — strip trailing numeric token, see if anything matches.
        import re
        team_part = re.sub(r"\s*[+\-]?\d+\.?\d*\s*$", "", pk).strip()
        if team_part:
            # Try alias resolution via engine.abbr
            try:
                from ...abbr import aliases_for
                aliases_team = {a.upper() for a in aliases_for(team_part)}
                aliases_team.add(team_part.upper())
                if home_abbr in aliases_team:
                    return "home"
                if away_abbr in aliases_team:
                    return "away"
            except Exception:
                pass
    return "?"


# ── Aggregation ──

def _profit_for(result: str, american: int) -> float:
    """Standard $100-stake profit math. Mirrors the settler so this
    autopsy doesn't drift from how the tracker computed profits."""
    if result == "W":
        if american > 0:
            return float(american)
        if american < 0:
            return round(100.0 * 100.0 / abs(american), 2)
    if result == "L":
        return -100.0
    return 0.0


def _aggregate_cells(rows: Iterable[sqlite3.Row]) -> dict:
    """Group settled picks into (bet_type, direction, bucket) cells."""
    cells = defaultdict(lambda: {"n": 0, "w": 0, "l": 0, "p": 0,
                                  "profit": 0.0})
    for r in rows:
        bt = (r["bet_type"] or "")
        direction = _direction_for(bt, r["pick"] or "",
                                    r["matchup"] or "")
        edge = float(r["edge"] or 0)
        bucket = _bucket_for(edge)
        key = (bt, direction, bucket)
        c = cells[key]
        c["n"] += 1
        c["profit"] += float(r["profit"] or 0)
        if r["result"] == "W":
            c["w"] += 1
        elif r["result"] == "L":
            c["l"] += 1
        elif r["result"] == "P":
            c["p"] += 1
    return cells


# ── Reporting ──

def _format_row(label: str, c: dict) -> str:
    decided = c["w"] + c["l"]
    wr = (c["w"] / decided * 100) if decided else 0
    roi = (c["profit"] / (c["n"] * 100) * 100) if c["n"] else 0
    return (f"  {label:38s} n={c['n']:>4d} "
            f"{c['w']:>3d}-{c['l']:<3d}-{c['p']:<2d}  "
            f"WR={wr:>5.1f}%  "
            f"profit={c['profit']:>+9.2f}  ROI={roi:>+7.2f}%")


def report(cells: dict, min_n: int) -> str:
    out: list[str] = []
    out.append("=" * 90)
    out.append("MLB TRACKER AUTOPSY")
    out.append("=" * 90)

    # Per-bet-type rollup first
    by_bt = defaultdict(lambda: {"n": 0, "w": 0, "l": 0, "p": 0,
                                  "profit": 0.0})
    for (bt, d, b), c in cells.items():
        agg = by_bt[bt]
        agg["n"] += c["n"]
        agg["w"] += c["w"]
        agg["l"] += c["l"]
        agg["p"] += c["p"]
        agg["profit"] += c["profit"]

    out.append("\n# Per-bet-type rollup (sorted by ROI ascending)")
    sorted_bt = sorted(by_bt.items(),
                       key=lambda kv: (kv[1]["profit"] / max(1, kv[1]["n"])))
    for bt, c in sorted_bt:
        out.append(_format_row(bt, c))

    # Per-cell breakdown — only cells with >= min_n; everything else
    # is rolled up per (bet_type, direction).
    out.append(f"\n# Cells with n >= {min_n} (sorted by ROI ascending)")
    sorted_cells = [(k, v) for k, v in cells.items() if v["n"] >= min_n]
    sorted_cells.sort(key=lambda kv: (kv[1]["profit"] / max(1, kv[1]["n"])))
    for (bt, d, b), c in sorted_cells:
        label = f"{bt} · {d} · {b}"
        out.append(_format_row(label, c))

    # Per-(bet_type, direction) rollup — collapses bucket dimension so
    # cells under min_n still get visibility.
    out.append("\n# Per-(bet_type, direction) rollup")
    by_bd = defaultdict(lambda: {"n": 0, "w": 0, "l": 0, "p": 0,
                                  "profit": 0.0})
    for (bt, d, b), c in cells.items():
        agg = by_bd[(bt, d)]
        agg["n"] += c["n"]
        agg["w"] += c["w"]
        agg["l"] += c["l"]
        agg["p"] += c["p"]
        agg["profit"] += c["profit"]
    sorted_bd = sorted(by_bd.items(),
                       key=lambda kv: (kv[1]["profit"] / max(1, kv[1]["n"])))
    for (bt, d), c in sorted_bd:
        out.append(_format_row(f"{bt} · {d}", c))

    # Bottom line
    total_n = sum(c["n"] for c in cells.values())
    total_w = sum(c["w"] for c in cells.values())
    total_l = sum(c["l"] for c in cells.values())
    total_p = sum(c["p"] for c in cells.values())
    total_profit = sum(c["profit"] for c in cells.values())
    decided = total_w + total_l
    wr = (total_w / decided * 100) if decided else 0
    roi = (total_profit / (total_n * 100) * 100) if total_n else 0
    out.append("\n# Bottom line")
    out.append(f"  Total n={total_n}  W={total_w}  L={total_l}  P={total_p}  "
               f"WR={wr:.1f}%  profit={total_profit:+.2f}  ROI={roi:+.2f}%")

    return "\n".join(out)


def run_autopsy(since: str | None = None,
                bet_type_filter: str | None = None,
                min_n: int = 5) -> tuple[dict, str]:
    """Pull settled picks and aggregate. Returns (cells, report_str)."""
    conn = get_conn()
    where = ["result IN ('W','L','P')"]
    args: list = []
    if since:
        where.append("date >= ?")
        args.append(since)
    if bet_type_filter:
        where.append("bet_type = ?")
        args.append(bet_type_filter)
    sql = ("SELECT bet_type, pick, matchup, edge, result, profit, odds, date "
           f"FROM picks WHERE {' AND '.join(where)}")
    rows = conn.execute(sql, tuple(args)).fetchall()
    cells = _aggregate_cells(rows)
    return cells, report(cells, min_n)


def _cli() -> None:
    parser = argparse.ArgumentParser(description="MLB tracker autopsy")
    parser.add_argument("--since", type=str,
                        help="Only include picks dated >= YYYY-MM-DD")
    parser.add_argument("--bet-type", type=str,
                        help="Restrict to one bet_type")
    parser.add_argument("--min-n", type=int, default=5,
                        help="Minimum cell n to include in detail "
                             "section (default 5)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    _cells, report_str = run_autopsy(
        since=args.since,
        bet_type_filter=args.bet_type,
        min_n=args.min_n,
    )
    print(report_str)


if __name__ == "__main__":
    _cli()
