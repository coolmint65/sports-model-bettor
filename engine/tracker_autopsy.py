"""
Sport-agnostic tracker autopsy — Phase 4.

Generalises ``engine.mlb_autopsy`` to NHL and NBA. The MLB module
remains as a sport-locked alias for back-compat with existing scripts;
new callers should use ``run_autopsy('mlb' | 'nhl' | 'nba', ...)``.

Same output shape as the MLB version: per-bet-type rollup, per-cell
breakdown above min-n, per-(bet_type × direction) rollup, and a
bottom-line ROI. The point isn't to suggest fixes — it's to surface
which (sport × bet_type × direction × edge_bucket) cells are
bleeding so calibration / shrinkage / weight-tuning workstreams have
a concrete target list.

Run::

    python -m engine.tracker_autopsy nhl
    python -m engine.tracker_autopsy nba --since 2026-04-15
    python -m engine.tracker_autopsy mlb --bet-type "ALT O/U" --min-n 4
"""

from __future__ import annotations
import argparse
import logging
import sqlite3
from collections import defaultdict
from typing import Iterable

logger = logging.getLogger(__name__)


# ── DB router ──

def _get_conn(sport: str):
    sport = sport.lower()
    if sport == "mlb":
        from .db import get_conn
        return get_conn()
    if sport == "nhl":
        from .nhl_db import get_conn
        return get_conn()
    if sport == "nba":
        from .nba_db import get_conn
        return get_conn()
    raise ValueError(f"unsupported sport {sport!r}")


def _table_name(sport: str) -> str:
    return {"mlb": "picks", "nhl": "nhl_picks", "nba": "nba_picks"}[sport.lower()]


# ── Edge buckets ──

_EDGE_BUCKETS = [(0, 2), (2, 4), (4, 6), (6, 8),
                 (8, 10), (10, 15), (15, 20), (20, 100)]


def _bucket_for(edge: float) -> str:
    for lo, hi in _EDGE_BUCKETS:
        if lo <= edge < hi:
            return f"{lo}-{hi}%"
    return "20+%"


# ── Direction extraction (sport-aware) ──

# Bet types that resolve to over/under direction.
_OU_BET_TYPES = frozenset({
    # MLB
    "O/U", "ALT O/U", "F5 O/U", "TEAM TOTAL", "F5 TEAM TOTAL",
    # NHL
    # ("O/U" already covered above)
    # NBA
    "TOTAL", "ALT TOTAL", "Q1_TOTAL", "Q2_TOTAL", "Q3_TOTAL", "Q4_TOTAL",
    "1H TOTAL", "2H TOTAL",
})

# Bet types that resolve to home/away direction (team picks).
_HA_BET_TYPES = frozenset({
    # MLB
    "ML", "RL", "ALT RL", "F5 ML", "F5 RL", "F5 WINNER",
    # NHL
    "PL",  # ("ML" covered above)
    # NBA
    "SPREAD", "ALT SPREAD", "Q1_ML", "Q1_SPREAD", "Q2_ML", "Q2_SPREAD",
    "Q3_ML", "Q3_SPREAD", "Q4_ML", "Q4_SPREAD",
    "1H ML", "1H SPREAD", "2H ML", "2H SPREAD",
})

# 1st INN is MLB-specific NRFI/YRFI direction.
_FIRST_INNING_TYPES = frozenset({"1ST INN"})


def _direction_for(bet_type: str, pick_text: str, matchup: str,
                   sport: str = "mlb") -> str:
    """Categorise a pick's direction. Returns "?" when the pick
    text doesn't clearly resolve. `sport` drives team-name alias
    resolution (NHL TBL→TB, LAK→LA, etc.)."""
    bt = (bet_type or "").upper().strip()
    pk = (pick_text or "").strip()
    pk_lower = pk.lower()

    if bt in _FIRST_INNING_TYPES:
        if pk_lower.startswith("nrfi"):
            return "nrfi"
        if pk_lower.startswith("yrfi"):
            return "yrfi"
        return "?"

    if bt in _OU_BET_TYPES:
        if pk_lower.startswith("over"):
            return "over"
        if pk_lower.startswith("under"):
            return "under"
        return "?"

    if bt in _HA_BET_TYPES:
        return _team_direction(pk, matchup, sport)

    return "?"


def _team_direction(pick_text: str, matchup: str, sport: str = "mlb") -> str:
    """Resolve a team-side pick to home / away by matching the pick's
    team token against the matchup's "AWAY @ HOME" abbrs. Falls back
    to alias resolution for full-name picks like "Washington Nationals
    +1.5"."""
    if " @ " not in (matchup or ""):
        return "?"
    away_abbr, home_abbr = matchup.split(" @ ", 1)
    away_abbr = away_abbr.strip().upper()
    home_abbr = home_abbr.strip().upper()
    pk = (pick_text or "").strip()
    if not pk:
        return "?"

    first = pk.split()[0].upper() if pk.split() else ""
    if first == home_abbr or pk.upper().startswith(home_abbr + " "):
        return "home"
    if first == away_abbr or pk.upper().startswith(away_abbr + " "):
        return "away"

    # Fallback: strip trailing line value, try alias resolution. For
    # NHL we MUST pass sport='nhl' or aliases_for returns the MLB
    # alias set ("TB" maps to MLB Tampa Bay Rays' TBR, not the NHL
    # Lightning's TBL). Same fix applies generally — pick token
    # disambiguation goes through the sport-specific alias map.
    import re
    team_part = re.sub(r"\s*[+\-]?\d+\.?\d*\s*$", "", pk).strip()
    if team_part:
        try:
            from .abbr import aliases_for
            aliases_team = {a.upper() for a in aliases_for(team_part, sport=sport)}
            aliases_team.add(team_part.upper())
            if home_abbr in aliases_team:
                return "home"
            if away_abbr in aliases_team:
                return "away"
        except Exception:
            pass

    # Last-ditch: try matching the matchup abbrs through their own
    # alias sets — the pick token might be canonical and the matchup
    # uses an abbreviation alias.
    try:
        from .abbr import aliases_for
        for label, side in (("home", home_abbr), ("away", away_abbr)):
            aliases = {a.upper() for a in aliases_for(side, sport=sport)}
            if first in aliases:
                return label
    except Exception:
        pass

    return "?"


# ── Aggregation ──

def _aggregate_cells(rows: Iterable[sqlite3.Row], sport: str = "mlb") -> dict:
    cells = defaultdict(lambda: {"n": 0, "w": 0, "l": 0, "p": 0,
                                  "profit": 0.0,
                                  # Calibration accumulators
                                  "brier_sum": 0.0,
                                  "logloss_sum": 0.0,
                                  "calib_n": 0,
                                  # Reliability bins (10% wide each, 0-100)
                                  "bin_wins": [0] * 10,
                                  "bin_n": [0] * 10,
                                  # CLV accumulators — see _format_row
                                  "clv_sum": 0.0,
                                  "clv_n": 0})
    for r in rows:
        bt = r["bet_type"] or ""
        direction = _direction_for(bt, r["pick"] or "", r["matchup"] or "",
                                    sport=sport)
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
        # Calibration: only score W/L (push doesn't carry calibration
        # signal cleanly). Need model_prob from the row.
        try:
            mp = float(r["model_prob"]) if r["model_prob"] is not None else None
        except (TypeError, ValueError):
            mp = None
        if mp is not None and r["result"] in ("W", "L"):
            actual = 1.0 if r["result"] == "W" else 0.0
            mp_clip = min(0.999, max(0.001, mp))
            c["brier_sum"] += (mp - actual) ** 2
            import math
            c["logloss_sum"] += -(actual * math.log(mp_clip)
                                   + (1 - actual) * math.log(1 - mp_clip))
            c["calib_n"] += 1
            # Reliability bin (0-9 for 0%, 10%, ..., 90%)
            bin_idx = min(9, int(mp * 10))
            c["bin_n"][bin_idx] += 1
            if actual == 1.0:
                c["bin_wins"][bin_idx] += 1
        # CLV: implied-prob delta between pick odds and closing odds.
        # Positive CLV = we got a better price than the close (sharp).
        # Only counts settled rows where both odds are populated.
        try:
            o = int(r["odds"]) if r["odds"] is not None else None
            co = int(r["closing_odds"]) if r["closing_odds"] is not None else None
        except (TypeError, ValueError):
            o, co = None, None
        if o is not None and co is not None and r["result"] in ("W", "L", "P"):
            bet_imp = abs(o) / (abs(o) + 100) if o < 0 else 100 / (o + 100)
            cls_imp = abs(co) / (abs(co) + 100) if co < 0 else 100 / (co + 100)
            c["clv_sum"] += (cls_imp - bet_imp) * 100.0
            c["clv_n"] += 1
    return cells


# ── Reporting ──

def _format_row(label: str, c: dict) -> str:
    decided = c["w"] + c["l"]
    wr = (c["w"] / decided * 100) if decided else 0
    roi = (c["profit"] / (c["n"] * 100) * 100) if c["n"] else 0
    # CLV column — implied-prob delta avg across settled rows with
    # closing_odds populated. "-" when no CLV samples (the cell either
    # has no settled rows yet or its picks pre-date the per-pick CLV
    # capture wiring). CLV is the leading indicator vs ROI's lagging:
    # negative CLV in a market is the early signal that the model is
    # losing to the close even when ROI hasn't caught up yet.
    if c.get("clv_n"):
        clv_avg = c["clv_sum"] / c["clv_n"]
        clv_str = f"CLV={clv_avg:>+5.1f}pp(n={c['clv_n']:>3d})"
    else:
        clv_str = "CLV=    -          "
    return (f"  {label:38s} n={c['n']:>4d} "
            f"{c['w']:>3d}-{c['l']:<3d}-{c['p']:<2d}  "
            f"WR={wr:>5.1f}%  "
            f"profit={c['profit']:>+9.2f}  ROI={roi:>+7.2f}%  "
            f"{clv_str}")


def report(sport: str, cells: dict, min_n: int,
           cutoff_used: str | None = None,
           all_time: bool = False,
           since_explicit: str | None = None) -> str:
    out: list[str] = []
    out.append("=" * 90)
    out.append(f"{sport.upper()} TRACKER AUTOPSY")
    if cutoff_used:
        out.append(f"  scope: post-cutoff (>= {cutoff_used}) — current model only")
    elif since_explicit:
        out.append(f"  scope: custom since={since_explicit}")
    elif all_time:
        out.append(f"  scope: all-time (includes pre-cutoff picks from prior models)")
    out.append("=" * 90)

    by_bt = defaultdict(lambda: {"n": 0, "w": 0, "l": 0, "p": 0,
                                  "profit": 0.0,
                                  "clv_sum": 0.0, "clv_n": 0})
    for (bt, _d, _b), c in cells.items():
        agg = by_bt[bt]
        agg["n"] += c["n"]
        agg["w"] += c["w"]
        agg["l"] += c["l"]
        agg["p"] += c["p"]
        agg["profit"] += c["profit"]
        agg["clv_sum"] += c.get("clv_sum", 0.0)
        agg["clv_n"] += c.get("clv_n", 0)

    out.append("\n# Per-bet-type rollup (sorted by ROI ascending)")
    sorted_bt = sorted(by_bt.items(),
                       key=lambda kv: (kv[1]["profit"] / max(1, kv[1]["n"])))
    for bt, c in sorted_bt:
        out.append(_format_row(bt, c))

    out.append(f"\n# Cells with n >= {min_n} (sorted by ROI ascending)")
    sorted_cells = [(k, v) for k, v in cells.items() if v["n"] >= min_n]
    sorted_cells.sort(key=lambda kv: (kv[1]["profit"] / max(1, kv[1]["n"])))
    for (bt, d, b), c in sorted_cells:
        out.append(_format_row(f"{bt} . {d} . {b}", c))

    out.append("\n# Per-(bet_type, direction) rollup")
    by_bd = defaultdict(lambda: {"n": 0, "w": 0, "l": 0, "p": 0, "profit": 0.0})
    for (bt, d, _b), c in cells.items():
        agg = by_bd[(bt, d)]
        agg["n"] += c["n"]
        agg["w"] += c["w"]
        agg["l"] += c["l"]
        agg["p"] += c["p"]
        agg["profit"] += c["profit"]
    sorted_bd = sorted(by_bd.items(),
                       key=lambda kv: (kv[1]["profit"] / max(1, kv[1]["n"])))
    for (bt, d), c in sorted_bd:
        out.append(_format_row(f"{bt} . {d}", c))

    total_n = sum(c["n"] for c in cells.values())
    total_w = sum(c["w"] for c in cells.values())
    total_l = sum(c["l"] for c in cells.values())
    total_p = sum(c["p"] for c in cells.values())
    total_profit = sum(c["profit"] for c in cells.values())
    decided = total_w + total_l
    wr = (total_w / decided * 100) if decided else 0
    roi = (total_profit / (total_n * 100) * 100) if total_n else 0

    # ── Calibration block (be-right-first headline) ──
    # Aggregate Brier + log-loss across ALL cells. Per-bucket reliability
    # diagram pooled across cells too — caller sees how well the
    # probabilities the model emitted lined up with reality.
    total_brier_sum = sum(c["brier_sum"] for c in cells.values())
    total_logloss_sum = sum(c["logloss_sum"] for c in cells.values())
    total_calib_n = sum(c["calib_n"] for c in cells.values())
    pooled_bins_n = [0] * 10
    pooled_bins_w = [0] * 10
    for c in cells.values():
        for i in range(10):
            pooled_bins_n[i] += c["bin_n"][i]
            pooled_bins_w[i] += c["bin_wins"][i]

    out.append("\n# Calibration (be-right metric — how often does prob X actually win X% of the time)")
    if total_calib_n >= 30:
        brier = total_brier_sum / total_calib_n
        logloss = total_logloss_sum / total_calib_n
        out.append(f"  Brier score:  {brier:.4f}  "
                   f"(perfect=0.0, coin-flip=0.25, naive 60% prior=0.24)")
        out.append(f"  Log-loss:     {logloss:.4f}  "
                   f"(perfect=0.0, coin-flip=0.693)")
        out.append(f"  Sample n:     {total_calib_n}")
        out.append("\n  Reliability diagram (target = bucket midpoint):")
        out.append(f"  {'bucket':>10s}  {'n':>5s}  {'predicted':>10s}  "
                   f"{'observed':>9s}  {'gap':>7s}  flag")
        for i in range(10):
            n_i = pooled_bins_n[i]
            if n_i < 5:
                continue
            obs = pooled_bins_w[i] / n_i
            mid = (i + 0.5) / 10.0
            gap = obs - mid
            flag = ""
            if abs(gap) >= 0.10 and n_i >= 20:
                flag = "OFF" if gap < 0 else "OFF+"
            elif abs(gap) >= 0.05 and n_i >= 50:
                flag = "drift" if gap < 0 else "drift+"
            out.append(f"  {i*10}-{(i+1)*10}%      "
                       f"{n_i:>5d}  {mid:>9.1%}  "
                       f"{obs:>8.1%}  {gap:>+6.1%}  {flag}")
    else:
        out.append(f"  (insufficient sample — n={total_calib_n}, need 30+)")

    out.append("\n# Bottom line")
    out.append(f"  Total n={total_n}  W={total_w}  L={total_l}  P={total_p}  "
               f"WR={wr:.1f}%  profit={total_profit:+.2f}  ROI={roi:+.2f}%")

    return "\n".join(out)


def _query_cells(sport: str, since: str | None,
                  bet_type_filter: str | None) -> dict:
    conn = _get_conn(sport)
    tbl = _table_name(sport)
    where = ["result IN ('W','L','P')"]
    args: list = []
    if since:
        where.append("date >= ?")
        args.append(since)
    if bet_type_filter:
        where.append("bet_type = ?")
        args.append(bet_type_filter)
    sql = (f"SELECT bet_type, pick, matchup, edge, result, profit, odds, "
           f"closing_odds, date, model_prob "
           f"FROM {tbl} WHERE {' AND '.join(where)}")
    rows = conn.execute(sql, tuple(args)).fetchall()
    return _aggregate_cells(rows, sport=sport)


def _bottom_line(cells: dict) -> dict:
    total_n = sum(c["n"] for c in cells.values())
    total_w = sum(c["w"] for c in cells.values())
    total_l = sum(c["l"] for c in cells.values())
    total_p = sum(c["p"] for c in cells.values())
    total_profit = sum(c["profit"] for c in cells.values())
    decided = total_w + total_l
    wr = (total_w / decided * 100) if decided else 0.0
    roi = (total_profit / (total_n * 100) * 100) if total_n else 0.0
    return {"n": total_n, "w": total_w, "l": total_l, "p": total_p,
            "profit": total_profit, "wr": wr, "roi": roi}


def run_autopsy(sport: str, since: str | None = None,
                bet_type_filter: str | None = None,
                min_n: int = 5,
                all_time: bool = False) -> tuple[dict, str]:
    """Pull settled picks for `sport` and aggregate. Returns
    (cells, report_str).

    By default filters to picks at-or-after the per-sport model-stack
    cutoff (``engine.model_cutoffs``) so the report reflects the
    *current* model, not a mix of pre/post-MC eras. Pass
    ``all_time=True`` or an explicit ``since`` to override.

    When the cutoff is in effect AND there are pre-cutoff picks in
    the table, the report appends an "all-time vs post-cutoff" delta
    so the operator can see what the cutoff is hiding.
    """
    from .model_cutoffs import get_cutoff

    cutoff_used: str | None = None
    if since is not None:
        # Caller specified an explicit window; respect it verbatim.
        effective_since = since
    elif all_time:
        effective_since = None
    else:
        cutoff_used = get_cutoff(sport)
        effective_since = cutoff_used

    cells = _query_cells(sport, effective_since, bet_type_filter)

    # Companion all-time pull when the cutoff is in effect — only if
    # there's a chance pre-cutoff picks exist (would be wasted query
    # otherwise). Cheap second SELECT is fine; this runs nightly at
    # most.
    delta_block = ""
    if cutoff_used:
        all_cells = _query_cells(sport, None, bet_type_filter)
        post = _bottom_line(cells)
        full = _bottom_line(all_cells)
        pre_n = full["n"] - post["n"]
        if pre_n > 0:
            pre_profit = full["profit"] - post["profit"]
            pre_w = full["w"] - post["w"]
            pre_l = full["l"] - post["l"]
            pre_decided = pre_w + pre_l
            pre_wr = (pre_w / pre_decided * 100) if pre_decided else 0.0
            pre_roi = (pre_profit / (pre_n * 100) * 100) if pre_n else 0.0
            delta_block = (
                "\n# Pre-cutoff comparison (filtered out by default)\n"
                f"  pre-cutoff (< {cutoff_used}):  n={pre_n:>4d}  "
                f"WR={pre_wr:>5.1f}%  profit={pre_profit:>+9.2f}  "
                f"ROI={pre_roi:>+7.2f}%\n"
                f"  post-cutoff (>= {cutoff_used}): n={post['n']:>4d}  "
                f"WR={post['wr']:>5.1f}%  profit={post['profit']:>+9.2f}  "
                f"ROI={post['roi']:>+7.2f}%"
            )

    report_str = report(sport, cells, min_n,
                         cutoff_used=cutoff_used,
                         all_time=all_time and since is None,
                         since_explicit=since)
    if delta_block:
        report_str = report_str + "\n" + delta_block
    return cells, report_str


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Tracker autopsy across MLB / NHL / NBA")
    parser.add_argument("sport", choices=["mlb", "nhl", "nba"])
    parser.add_argument("--since", type=str,
                        help="Explicit YYYY-MM-DD lower bound. Overrides "
                             "the per-sport model-stack cutoff.")
    parser.add_argument("--all-time", action="store_true",
                        help="Disable the per-sport cutoff filter and "
                             "include picks generated by older models. "
                             "Use to see how much pre-cutoff data is "
                             "dragging the headline.")
    parser.add_argument("--bet-type", type=str)
    parser.add_argument("--min-n", type=int, default=5)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    _cells, report_str = run_autopsy(
        sport=args.sport,
        since=args.since,
        bet_type_filter=args.bet_type,
        min_n=args.min_n,
        all_time=args.all_time,
    )
    print(report_str)


if __name__ == "__main__":
    _cli()
