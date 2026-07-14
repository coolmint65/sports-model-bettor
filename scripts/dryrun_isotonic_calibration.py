"""Dry-run: compare current Beta-Binomial-per-bucket calibration against
an isotonic regression on the same historical data.

We're checking whether replacing the bucketed calibrator with an
isotonic mapping would produce:
  1. Smooth continuous output (vs. the current 12-value collapse)
  2. Monotonicity (higher raw prob → higher calibrated prob, always)
  3. Meaningful shift in which picks fire vs. get filtered by the
     0.50 belief gate

Reads the tennis_picks table for the target sport/bet_type, extracts
(raw_prob, realized) pairs, fits isotonic + Beta-Binomial for
comparison, and prints a side-by-side of what happens to each
historical pick under both mappings.

Does not touch any production tables. Safe to re-run.

Usage:
    python -m scripts.dryrun_isotonic_calibration --sport tennis --bet-type TOTAL_GAMES
    python -m scripts.dryrun_isotonic_calibration --sport tennis --bet-type ML
"""
from __future__ import annotations

import argparse
import statistics
import sys
from collections import Counter, defaultdict


def _load_pairs(sport: str, bet_type: str,
                 since: str | None = None) -> list[tuple[float, int]]:
    """(raw_prob, won) pairs for every settled pick in scope. `raw_prob`
    is the pre-calibration probability the picker computed; `won` is
    1 for W, 0 for L, 0.5 for P (push)."""
    if sport == "tennis":
        from engine.tennis_db import get_conn
    elif sport == "mlb":
        from engine.db import get_conn
    elif sport == "nhl":
        from engine.nhl_db import get_conn
    elif sport == "nba":
        from engine.nba_db import get_conn
    else:
        raise SystemExit(f"unsupported sport: {sport!r}")

    conn = get_conn()
    # Tennis stores post-calibration in `model_prob`; the raw pre-cal
    # doesn't exist per row. We reconstruct raw_prob from the currently-
    # cached calibrated value by walking backward through the bucket
    # midpoint mapping. For a proper isotonic replacement we'd want the
    # raw prob stored — falling back to the calibrated value here means
    # the dry-run compares "current calibrated" to "isotonic of current
    # calibrated", which still surfaces the bucketing behavior but
    # understates the raw-vs-current gap.
    table = "tennis_picks" if sport == "tennis" else f"{sport}_picks"
    prob_col = "model_prob"
    query = (
        f"SELECT {prob_col}, result FROM {table} "
        f"WHERE bet_type = ? AND result IN ('W', 'L', 'P')"
    )
    params: list = [bet_type]
    if since:
        query += " AND date >= ?"
        params.append(since)
    rows = conn.execute(query, params).fetchall()

    pairs: list[tuple[float, int]] = []
    for r in rows:
        p = r[0] if isinstance(r, tuple) else r[prob_col]
        result = r[1] if isinstance(r, tuple) else r["result"]
        if p is None:
            continue
        if result == "W":
            won = 1.0
        elif result == "L":
            won = 0.0
        else:
            won = 0.5
        pairs.append((float(p), won))
    return pairs


def _beta_binomial_per_bucket(pairs, buckets, prior_n0=10):
    """Current-system reproduction. For each bucket, posterior mean =
    (n0 * midpoint + w) / (n0 + n)."""
    by_bucket: dict[tuple[float, float], dict] = {
        b: {"n": 0, "w": 0.0} for b in buckets
    }
    for p, w in pairs:
        for lo, hi in buckets:
            if lo <= p < hi:
                by_bucket[(lo, hi)]["n"] += 1
                by_bucket[(lo, hi)]["w"] += w
                break
    out = {}
    for (lo, hi), c in by_bucket.items():
        mid = (lo + hi) / 2
        alpha = prior_n0 * mid + c["w"]
        beta = prior_n0 * (1.0 - mid) + (c["n"] - c["w"])
        out[(lo, hi)] = {
            "n": c["n"], "w": c["w"],
            "post": alpha / (alpha + beta),
        }
    return out


def _apply_bucketed(raw, buckets_out):
    for (lo, hi), meta in buckets_out.items():
        if lo <= raw < hi:
            return meta["post"]
    return raw


def _fit_isotonic(pairs):
    from sklearn.isotonic import IsotonicRegression
    xs = [p for p, _ in pairs]
    ys = [w for _, w in pairs]
    iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    iso.fit(xs, ys)
    return iso


def _apply_isotonic(iso, raw):
    return float(iso.predict([raw])[0])


def _implied_from_odds(odds):
    if odds is None or odds == 0:
        return 0.5
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 100 / (odds + 100)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="tennis")
    ap.add_argument("--bet-type", default="TOTAL_GAMES")
    ap.add_argument("--since", default=None)
    ap.add_argument("--min-samples", type=int, default=30)
    args = ap.parse_args()

    pairs = _load_pairs(args.sport, args.bet_type, since=args.since)
    if len(pairs) < args.min_samples:
        print(f"only {len(pairs)} settled pairs — need >= {args.min_samples}")
        return 1

    print(f"=== {args.sport}/{args.bet_type} — n={len(pairs)} settled pairs ===")
    print()

    # Distribution of raw probs
    raw_uniq = Counter(round(p, 4) for p, _ in pairs)
    print(f"distinct raw prob values: {len(raw_uniq)}")
    top = sorted(raw_uniq.items(), key=lambda kv: -kv[1])[:5]
    print(f"  top 5 by frequency: {top}")
    print()

    # Current bucketed calibration (reproduces production behavior)
    from engine.calibration_buckets import BUCKETS
    bucketed = _beta_binomial_per_bucket(pairs, BUCKETS)

    print("Current Beta-Binomial per-bucket (production):")
    print(f"  {'bucket':<14}{'n':>5}{'w':>7}{'wr':>7}{'post':>7}")
    for (lo, hi), c in bucketed.items():
        wr = c["w"] / c["n"] if c["n"] else 0
        print(f"  [{lo:.2f}, {hi:.2f}) {c['n']:>4} {c['w']:>6.1f}"
               f"  {wr:>6.3f} {c['post']:>6.3f}")
    print()

    # Isotonic fit
    iso = _fit_isotonic(pairs)

    # Diagnostic: sample the mapping at many raw values
    print("Isotonic mapping (sampled every 0.02):")
    for raw in [0.30 + i * 0.05 for i in range(15)]:
        b_val = _apply_bucketed(raw, bucketed)
        i_val = _apply_isotonic(iso, raw)
        print(f"  raw={raw:.2f}  bucketed={b_val:.3f}  isotonic={i_val:.3f}")
    print()

    # Monotonicity check on bucketed vs isotonic
    def _is_monotonic(f, step=0.01):
        prev = -1
        for i in range(30, 100):
            raw = i * step
            v = f(raw)
            if v < prev - 1e-9:
                return False
            prev = v
        return True

    b_mono = _is_monotonic(lambda r: _apply_bucketed(r, bucketed))
    i_mono = _is_monotonic(lambda r: _apply_isotonic(iso, r))
    print(f"monotonic? bucketed={b_mono}  isotonic={i_mono}")
    print()

    # Per-pick side-by-side. Show how the calibrated value differs and
    # whether the pick would still clear the 0.50 belief gate.
    print("Per-pick impact (sample of 25):")
    print(f"  {'raw':>6}{'realized':>10}{'bucketed':>10}{'isotonic':>10}"
           f"  {'passes_belief_now':>18}{'passes_belief_iso':>18}")
    for raw, won in pairs[:25]:
        b_cal = _apply_bucketed(raw, bucketed)
        i_cal = _apply_isotonic(iso, raw)
        result = "W" if won == 1 else "L" if won == 0 else "P"
        print(f"  {raw:>6.3f}{result:>10}{b_cal:>10.3f}{i_cal:>10.3f}"
               f"  {str(b_cal >= 0.50):>18}{str(i_cal >= 0.50):>18}")
    print()

    # Aggregate impact on belief gate
    n_pass_b = sum(1 for raw, _ in pairs
                    if _apply_bucketed(raw, bucketed) >= 0.50)
    n_pass_i = sum(1 for raw, _ in pairs
                    if _apply_isotonic(iso, raw) >= 0.50)
    print(f"Belief gate (>=0.50) pass rate:")
    print(f"  bucketed: {n_pass_b}/{len(pairs)} = {n_pass_b/len(pairs)*100:.1f}%")
    print(f"  isotonic: {n_pass_i}/{len(pairs)} = {n_pass_i/len(pairs)*100:.1f}%")
    print()

    # Overall calibration quality: Brier score for each mapping
    brier_b = sum((_apply_bucketed(raw, bucketed) - w) ** 2
                    for raw, w in pairs) / len(pairs)
    brier_i = sum((_apply_isotonic(iso, raw) - w) ** 2
                    for raw, w in pairs) / len(pairs)
    brier_raw = sum((raw - w) ** 2 for raw, w in pairs) / len(pairs)
    print(f"Brier score (lower = better):")
    print(f"  raw:      {brier_raw:.4f}")
    print(f"  bucketed: {brier_b:.4f}  (delta vs raw: {brier_b - brier_raw:+.4f})")
    print(f"  isotonic: {brier_i:.4f}  (delta vs raw: {brier_i - brier_raw:+.4f})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
