"""
Edge-bucket ROI analysis — find the sweet spot for edge thresholds.

Many models are genuinely predictive in the medium-edge range (roughly 3-7%)
but pick up garbage at extreme edges (10%+) because extreme edges usually
mean the model disagrees with the market by too much to be trusted. This
tool groups settled picks by edge and reports per-bucket W-L, WR, profit,
and ROI.

It also sweeps a range of minimum-edge cutoffs and reports what the total
record would have looked like if we'd filtered below that edge, so we can
pick the threshold that maximizes ROI across all tracked sports.

Usage:
    python -m engine.edge_report              # all sports + combined cutoff sweep
    python -m engine.edge_report --sport nhl  # single sport
    python -m engine.edge_report --sport mlb
"""

from __future__ import annotations

import argparse

from engine._analysis_common import (
    SPORTS,
    canon_result,
    fetch_settled,
    label,
    open_conn,
    pick_profit,
    resolve_sports,
)

BUCKETS: list[tuple[str, float, float]] = [
    ("<2%",    -999.0, 2.0),
    ("2-4%",   2.0,    4.0),
    ("4-6%",   4.0,    6.0),
    ("6-10%",  6.0,   10.0),
    ("10%+",  10.0,  9999.0),
]

CUTOFFS = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0]


def _bucket_for_edge(e: float) -> str | None:
    for name, lo, hi in BUCKETS:
        if lo <= e < hi:
            return name
    return None


def _aggregate(rows: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {
        name: {"w": 0, "l": 0, "p": 0, "profit": 0.0}
        for name, _, _ in BUCKETS
    }
    for r in rows:
        e = r.get("edge")
        if e is None:
            continue
        try:
            ef = float(e)
        except (TypeError, ValueError):
            continue
        name = _bucket_for_edge(ef)
        if not name:
            continue
        c = canon_result(r.get("result"))
        bucket = out[name]
        if c == "win":
            bucket["w"] += 1
        elif c == "loss":
            bucket["l"] += 1
        elif c == "push":
            bucket["p"] += 1
        else:
            continue
        bucket["profit"] += pick_profit(r)
    return out


def _print_buckets(header: str, agg: dict[str, dict]) -> None:
    print(f"\n{header}")
    any_rows = False
    for name, _, _ in BUCKETS:
        b = agg[name]
        n = b["w"] + b["l"] + b["p"]
        if n == 0:
            continue
        any_rows = True
        decided = b["w"] + b["l"]
        wr = 100.0 * b["w"] / decided if decided else 0.0
        roi = 100.0 * b["profit"] / (n * 100.0)  # each pick = $100 unit
        print(
            f"  {name:7s}: N={n:3d}  {b['w']:3d}-{b['l']:3d}-{b['p']}  "
            f"WR={wr:5.1f}%  profit=${b['profit']:+8.2f}  ROI={roi:+6.2f}%"
        )
    if not any_rows:
        print("  (no settled picks)")


def _cutoff_summary(rows: list[dict], cutoff: float) -> tuple[int, int, int, float]:
    """Return (wins, losses, pushes, profit) over picks with edge >= cutoff."""
    w = l = p = 0
    profit = 0.0
    for r in rows:
        try:
            e = float(r.get("edge") or 0)
        except (TypeError, ValueError):
            continue
        if e < cutoff:
            continue
        c = canon_result(r.get("result"))
        if c == "win":
            w += 1
        elif c == "loss":
            l += 1
        elif c == "push":
            p += 1
        else:
            continue
        profit += pick_profit(r)
    return w, l, p, profit


def report_sport(sport: str) -> list[dict]:
    conn = open_conn(sport)
    if conn is None:
        print(f"\nNo picks yet for {label(sport)} (DB missing).")
        return []
    try:
        rows = fetch_settled(conn, sport)
    finally:
        conn.close()

    if not rows:
        print(f"\nNo picks yet for {label(sport)}.")
        return []

    print(f"\n{label(sport)} edge buckets (N={len(rows)}):")
    agg = _aggregate(rows)
    _print_buckets(f"  Edge bucket breakdown:", agg)

    print(f"\n  Cutoff sweep ({label(sport)}):")
    for c in CUTOFFS:
        w, l, p, profit = _cutoff_summary(rows, c)
        n = w + l + p
        if n == 0:
            print(f"    >= {c:4.1f}%:  no picks")
            continue
        decided = w + l
        wr = 100.0 * w / decided if decided else 0.0
        roi = 100.0 * profit / (n * 100.0)
        print(
            f"    >= {c:4.1f}%:  N={n:3d}  {w:3d}-{l:3d}-{p}  "
            f"WR={wr:5.1f}%  profit=${profit:+8.2f}  ROI={roi:+6.2f}%"
        )

    return rows


def _print_combined_optimum(all_rows: list[dict]) -> None:
    if not all_rows:
        return
    print(f"\nCombined cutoff sweep (all sports, N={len(all_rows)}):")
    best = None  # (roi, cutoff, w, l, p, profit, n)
    for c in CUTOFFS:
        w, l, p, profit = _cutoff_summary(all_rows, c)
        n = w + l + p
        if n == 0:
            print(f"  >= {c:4.1f}%:  no picks")
            continue
        decided = w + l
        wr = 100.0 * w / decided if decided else 0.0
        roi = 100.0 * profit / (n * 100.0)
        print(
            f"  >= {c:4.1f}%:  N={n:3d}  {w:3d}-{l:3d}-{p}  "
            f"WR={wr:5.1f}%  profit=${profit:+8.2f}  ROI={roi:+6.2f}%"
        )
        # Require at least 10 picks to avoid over-fitting to a 3-pick "optimum"
        if n >= 10 and (best is None or roi > best[0]):
            best = (roi, c, w, l, p, profit, n)

    if best is not None:
        roi, c, w, l, p, profit, n = best
        print(
            f"\n  Optimal edge cutoff: {c:.1f}% (would have produced "
            f"{w}-{l}{'-'+str(p) if p else ''}, ${profit:+.2f}, {roi:+.2f}% ROI "
            f"on {n} picks across sports)"
        )
    else:
        print("\n  Optimal edge cutoff: insufficient settled sample (need N>=10 at a cutoff).")


def main() -> None:
    ap = argparse.ArgumentParser(description="Edge-bucket ROI analysis across tracked picks.")
    ap.add_argument("--sport", default="all",
                    help=f"One of: {', '.join(SPORTS)}, or 'all' (default).")
    args = ap.parse_args()

    sports = resolve_sports(args.sport)
    combined: list[dict] = []
    for sport in sports:
        rows = report_sport(sport)
        combined.extend(rows)

    if len(sports) > 1:
        _print_combined_optimum(combined)
    print()


if __name__ == "__main__":
    main()
