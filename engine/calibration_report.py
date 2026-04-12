"""
Calibration report — compares model_prob predictions to actual hit rates.

If the model says 60%, do those picks actually hit 60% of the time? Bucket
settled picks by their stated probability and compare the actual win rate
to the expected (bucket midpoint) rate. A well-calibrated model has actual
~= expected across buckets; large systematic deltas flag miscalibration.

Usage:
    python -m engine.calibration_report              # all sports
    python -m engine.calibration_report --sport nhl  # single sport
    python -m engine.calibration_report --sport all

Buckets (model_prob, in percent):
    50-55, 55-60, 60-65, 65-70, 70-75, 75%+

Any bucket with >10pt delta AND N>=5 is flagged as a calibration issue.
Pushes are excluded from the hit-rate denominator (they don't cleanly
vote for calibration either way).
"""

from __future__ import annotations

import argparse

from engine._analysis_common import (
    SPORTS,
    canon_result,
    fetch_settled,
    label,
    open_conn,
    resolve_sports,
)

BUCKETS: list[tuple[str, float, float]] = [
    ("50-55%", 0.50, 0.55),
    ("55-60%", 0.55, 0.60),
    ("60-65%", 0.60, 0.65),
    ("65-70%", 0.65, 0.70),
    ("70-75%", 0.70, 0.75),
    ("75%+",   0.75, 1.01),  # upper bound inclusive-ish via < 1.01
]

FLAG_DELTA_PTS = 10.0
FLAG_MIN_N = 5


def _prob_to_float(v) -> float | None:
    """Accept 0.62 or 62 or '62%' — return fraction in [0,1] or None."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f > 1.5:  # stored as percent
        f = f / 100.0
    if f < 0 or f > 1:
        return None
    return f


def _bucket_for(p: float) -> tuple[str, float, float] | None:
    for name, lo, hi in BUCKETS:
        if lo <= p < hi:
            return name, lo, hi
    return None


def report_sport(sport: str) -> None:
    conn = open_conn(sport)
    if conn is None:
        print(f"No picks yet for {label(sport)} (DB missing).")
        return
    try:
        rows = fetch_settled(conn, sport)
    finally:
        conn.close()

    if not rows:
        print(f"No picks yet for {label(sport)}.")
        return

    # Aggregate per bucket: wins, losses (ignore pushes).
    agg: dict[str, dict] = {}
    for name, lo, hi in BUCKETS:
        agg[name] = {"wins": 0, "losses": 0, "lo": lo, "hi": hi, "midpoint": (lo + hi) / 2}
    # Clamp 75%+ midpoint to something reasonable (use 0.80 as expected)
    agg["75%+"]["midpoint"] = 0.80

    for r in rows:
        p = _prob_to_float(r.get("model_prob"))
        if p is None or p < 0.50:
            continue
        b = _bucket_for(p)
        if not b:
            continue
        name, _, _ = b
        c = canon_result(r.get("result"))
        if c == "win":
            agg[name]["wins"] += 1
        elif c == "loss":
            agg[name]["losses"] += 1
        # pushes skipped

    total_settled = sum(a["wins"] + a["losses"] for a in agg.values())
    print(f"\n{label(sport)} Calibration (N={total_settled}):")
    if total_settled == 0:
        print("  (no non-push settled picks with model_prob >= 50%)")
        return

    flags: list[str] = []
    for name, _, _ in BUCKETS:
        a = agg[name]
        n = a["wins"] + a["losses"]
        if n == 0:
            continue
        actual = 100.0 * a["wins"] / n
        expected = 100.0 * a["midpoint"]
        delta = actual - expected
        tag = ""
        if abs(delta) > FLAG_DELTA_PTS and n >= FLAG_MIN_N:
            tag = "  OVERCONFIDENT" if delta < 0 else "  (underconfident)"
            flags.append(
                f"{name}: {n} picks, actual {actual:.1f}% vs expected {expected:.1f}% "
                f"(delta {delta:+.1f}pt)"
            )
        print(
            f"  {name:7s}: {n:3d} picks, actual {actual:5.1f}% "
            f"(expected {expected:4.1f}%)  {delta:+5.1f}pts{tag}"
        )

    if flags:
        print(f"\n  Calibration issues (>{FLAG_DELTA_PTS:.0f}pt delta, N>={FLAG_MIN_N}):")
        for f in flags:
            print(f"    - {f}")
    else:
        print("\n  No buckets tripped the calibration threshold.")


def main() -> None:
    ap = argparse.ArgumentParser(description="Model calibration report across tracked picks.")
    ap.add_argument("--sport", default="all",
                    help=f"One of: {', '.join(SPORTS)}, or 'all' (default).")
    args = ap.parse_args()

    for sport in resolve_sports(args.sport):
        report_sport(sport)
    print()


if __name__ == "__main__":
    main()
