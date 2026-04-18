"""Empirical calibration check on the MLB picks tracker.

Buckets settled picks by predicted probability and compares to actual
win rate. If the 60% bucket actually wins ~50%, the model is 10pp
over-confident across the board and we need to tighten the soft
compression.

Usage:  python -m scripts.diag_calibration
"""
from collections import defaultdict
from engine.db import get_conn

BUCKETS = [(0.30, 0.40), (0.40, 0.50), (0.50, 0.55), (0.55, 0.60),
           (0.60, 0.65), (0.65, 0.70), (0.70, 0.80), (0.80, 1.01)]


def implied(odds: int) -> float:
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 100.0 / (odds + 100)


def bucket_for(p: float):
    for lo, hi in BUCKETS:
        if lo <= p < hi:
            return (lo, hi)
    return None


def main():
    conn = get_conn()
    rows = conn.execute(
        "SELECT bet_type, model_prob, edge, odds, result, profit "
        "FROM picks WHERE result IN ('W', 'L') AND model_prob IS NOT NULL "
        "ORDER BY date"
    ).fetchall()

    if not rows:
        print("No settled picks with model_prob -- nothing to calibrate.")
        return

    # Overall calibration
    print("=" * 72)
    print(f"OVERALL ({len(rows)} settled W/L picks)")
    print("=" * 72)
    by_bucket = defaultdict(lambda: {"n": 0, "w": 0, "stake": 0.0, "profit": 0.0})
    for r in rows:
        r = dict(r)
        b = bucket_for(r["model_prob"])
        if not b:
            continue
        d = by_bucket[b]
        d["n"] += 1
        d["w"] += 1 if r["result"] == "W" else 0
        d["stake"] += 100.0
        d["profit"] += r["profit"] or 0.0

    print(f"{'Bucket':<14} {'N':>5} {'Pred WR':>8} {'Real WR':>8} "
          f"{'Diff':>8} {'ROI':>8}")
    print("-" * 60)
    for lo, hi in BUCKETS:
        d = by_bucket.get((lo, hi))
        if not d or d["n"] == 0:
            continue
        pred_wr = (lo + hi) / 2
        real_wr = d["w"] / d["n"]
        diff = real_wr - pred_wr
        roi = d["profit"] / d["stake"] * 100 if d["stake"] else 0
        print(f"{lo:.2f}-{hi:.2f}    {d['n']:>5} {pred_wr*100:>7.1f}% "
              f"{real_wr*100:>7.1f}% {diff*100:>+7.1f}pp {roi:>+7.1f}%")

    # Per bet-type
    print("\n" + "=" * 72)
    print("BY BET TYPE")
    print("=" * 72)
    type_buckets = defaultdict(lambda: defaultdict(lambda: {"n": 0, "w": 0,
                                                              "profit": 0.0}))
    for r in rows:
        r = dict(r)
        bt = r["bet_type"]
        b = bucket_for(r["model_prob"])
        if not b:
            continue
        d = type_buckets[bt][b]
        d["n"] += 1
        d["w"] += 1 if r["result"] == "W" else 0
        d["profit"] += r["profit"] or 0.0

    for bt, buckets in sorted(type_buckets.items()):
        total_n = sum(b["n"] for b in buckets.values())
        total_w = sum(b["w"] for b in buckets.values())
        total_profit = sum(b["profit"] for b in buckets.values())
        wr = total_w / total_n if total_n else 0
        roi = total_profit / (total_n * 100) * 100 if total_n else 0
        print(f"\n{bt}: {total_w}-{total_n - total_w} ({wr*100:.1f}% WR, "
              f"{roi:+.1f}% ROI)")
        for lo, hi in BUCKETS:
            d = buckets.get((lo, hi))
            if not d or d["n"] == 0:
                continue
            pred_wr = (lo + hi) / 2
            real_wr = d["w"] / d["n"]
            print(f"   {lo:.2f}-{hi:.2f}: N={d['n']:>3}  "
                  f"pred={pred_wr*100:>5.1f}%  real={real_wr*100:>5.1f}%  "
                  f"diff={(real_wr - pred_wr)*100:+.1f}pp")

    # Edge calibration: do high-edge picks actually win at higher rates?
    print("\n" + "=" * 72)
    print("BY EDGE BUCKET (does displayed edge predict actual outperformance?)")
    print("=" * 72)
    edge_buckets = [(0, 3), (3, 6), (6, 10), (10, 15), (15, 25), (25, 100)]
    eb = defaultdict(lambda: {"n": 0, "w": 0, "profit": 0.0,
                               "implied_sum": 0.0, "model_sum": 0.0})
    for r in rows:
        r = dict(r)
        e = r["edge"]
        if e is None:
            continue
        for lo, hi in edge_buckets:
            if lo <= e < hi:
                d = eb[(lo, hi)]
                d["n"] += 1
                d["w"] += 1 if r["result"] == "W" else 0
                d["profit"] += r["profit"] or 0.0
                d["model_sum"] += r["model_prob"] or 0
                d["implied_sum"] += implied(r["odds"]) if r["odds"] else 0
                break

    print(f"{'Edge':<10} {'N':>5} {'Pred WR':>8} {'Real WR':>8} "
          f"{'Implied':>9} {'ROI':>8}")
    print("-" * 60)
    for lo, hi in edge_buckets:
        d = eb.get((lo, hi))
        if not d or d["n"] == 0:
            continue
        pred_wr = d["model_sum"] / d["n"]
        real_wr = d["w"] / d["n"]
        impl = d["implied_sum"] / d["n"]
        roi = d["profit"] / (d["n"] * 100) * 100
        print(f"{lo:>2}-{hi:<3}%    {d['n']:>5} {pred_wr*100:>7.1f}% "
              f"{real_wr*100:>7.1f}% {impl*100:>8.1f}% {roi:>+7.1f}%")


if __name__ == "__main__":
    main()
