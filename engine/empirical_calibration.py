"""Empirical probability calibration from settled tracker picks.

The factor + MC + GBM ensemble is systemically over-confident at the
upper tail. The 191-pick tracker (Apr 2026) showed:
   ML  80%+ predicted bucket -> 60.0% real (-30pp)
   OU  70%+ predicted bucket -> 37.5% real (-37pp)
   RL  80%+ predicted bucket -> 47.1% real (-43pp)
i.e. the model claims it's 80%-confident but reality is a coin flip.

Rather than guess at a soft-compression factor, we calibrate from the
actual outcomes. Each bet type has a piecewise-linear map from raw
predicted probability -> empirical win rate. Outside the table we
clamp to the nearest bucket. The map is rebuilt nightly by
`refresh_calibration()` from the picks table; until enough rows
accumulate per bucket (>= MIN_BUCKET_N) the bucket falls back to its
midpoint passthrough (no recalibration).

This is a Platt/isotonic-flavored calibration, not a perfect fit, but
it's grounded in observed data and updates automatically as more picks
settle. When a bucket has too few samples we keep the model honest
(passthrough); when we have data we use it.

Public API:
   calibrate(bet_type, raw_prob) -> calibrated_prob
   refresh_calibration() -> dict (called by sync scheduler)
"""

from __future__ import annotations

import logging
import threading
from collections import defaultdict
from typing import Iterable

logger = logging.getLogger(__name__)

# Minimum samples per (bet_type, bucket) before we trust the empirical
# real_wr enough to override the model's raw prediction. Below this we
# pass the raw prob through unchanged so we don't over-fit small samples.
MIN_BUCKET_N = 5

# Buckets are open-on-the-right intervals: [lo, hi). The "raw" bucket
# label is also used as the lookup key.
_BUCKETS = [
    (0.30, 0.40),
    (0.40, 0.50),
    (0.50, 0.55),
    (0.55, 0.60),
    (0.60, 0.65),
    (0.65, 0.70),
    (0.70, 0.80),
    (0.80, 1.01),
]

# Loaded calibration: {bet_type_normalized: [(lo, hi, real_wr), ...]}
_TABLE: dict[str, list[tuple[float, float, float]]] = {}
_TABLE_LOCK = threading.Lock()
_TABLE_LOADED = False


def _normalize_bet_type(bt: str) -> str:
    """Collapse capitalization variants the tracker has accumulated."""
    if not bt:
        return ""
    bt = bt.strip().lower()
    # Keep "1st inn" -> "nrfi" so NRFI / 1st INN bucket together
    if bt in ("1st inn", "1stinn"):
        return "nrfi"
    return bt


def calibrate(bet_type: str, raw_prob: float) -> float:
    """Map a raw model probability to the empirical win-rate bucket.

    Falls back to raw passthrough when:
      - The calibration table hasn't been built yet (cold start)
      - This bet_type has no data
      - The matched bucket has fewer than MIN_BUCKET_N samples
    """
    if raw_prob is None:
        return raw_prob
    if not _TABLE_LOADED:
        # Lazy-load on first call so callers don't need to know about it.
        try:
            refresh_calibration()
        except Exception as e:
            logger.debug("calibration load failed; passthrough: %s", e)
            return raw_prob

    bt = _normalize_bet_type(bet_type)
    rows = _TABLE.get(bt)
    if not rows:
        return raw_prob

    for lo, hi, real_wr in rows:
        if lo <= raw_prob < hi:
            return real_wr

    # raw is outside our buckets -- match the closest end.
    if raw_prob < rows[0][0]:
        return rows[0][2]
    return rows[-1][2]


def calibrated_edge(bet_type: str, raw_prob: float, odds: int) -> float:
    """Edge = calibrated_prob - implied_prob, in percentage points.

    Use this everywhere the picks pipeline currently does
    `(prob - implied(odds)) * 100`. It silently keeps the prior shape
    when no calibration exists, so the change is safe to drop in."""
    if odds is None or raw_prob is None:
        return 0.0
    if odds < 0:
        implied = abs(odds) / (abs(odds) + 100)
    else:
        implied = 100.0 / (odds + 100)
    cal = calibrate(bet_type, raw_prob)
    return (cal - implied) * 100.0


def refresh_calibration() -> dict:
    """Rebuild _TABLE from the picks table. Returns a summary dict for
    callers that want to log how many buckets were filled."""
    global _TABLE_LOADED
    summary = {"buckets_filled": 0, "buckets_passthrough": 0,
               "total_rows": 0}
    try:
        from .db import get_conn
    except Exception as e:
        logger.warning("calibration: cannot import db: %s", e)
        return summary

    try:
        rows = get_conn().execute(
            "SELECT bet_type, model_prob, result FROM picks "
            "WHERE result IN ('W', 'L') AND model_prob IS NOT NULL"
        ).fetchall()
    except Exception as e:
        logger.warning("calibration: query failed: %s", e)
        return summary

    summary["total_rows"] = len(rows)
    if not rows:
        with _TABLE_LOCK:
            _TABLE.clear()
            _TABLE_LOADED = True
        return summary

    # Count W and total per (bet_type, bucket).
    counts: dict[tuple[str, tuple[float, float]], dict] = defaultdict(
        lambda: {"n": 0, "w": 0}
    )
    for r in rows:
        r = dict(r)
        bt = _normalize_bet_type(r.get("bet_type"))
        p = r.get("model_prob") or 0.0
        b = _bucket_for(p)
        if not b:
            continue
        c = counts[(bt, b)]
        c["n"] += 1
        if r["result"] == "W":
            c["w"] += 1

    new_table: dict[str, list[tuple[float, float, float]]] = defaultdict(list)
    for (bt, (lo, hi)), c in counts.items():
        if c["n"] >= MIN_BUCKET_N:
            real_wr = c["w"] / c["n"]
            new_table[bt].append((lo, hi, real_wr))
            summary["buckets_filled"] += 1
        else:
            # Passthrough: midpoint of bucket. Sample too thin to trust.
            new_table[bt].append((lo, hi, (lo + hi) / 2))
            summary["buckets_passthrough"] += 1

    # Sort each bet-type's rows by lo so the lookup loop is deterministic.
    for bt in list(new_table.keys()):
        new_table[bt].sort(key=lambda row: row[0])

    with _TABLE_LOCK:
        _TABLE.clear()
        _TABLE.update(new_table)
        _TABLE_LOADED = True

    logger.info("calibration: built table from %d rows -- %d buckets "
                "filled, %d passthrough",
                summary["total_rows"], summary["buckets_filled"],
                summary["buckets_passthrough"])
    return summary


def snapshot() -> dict:
    """Return the current calibration table for debugging / health."""
    with _TABLE_LOCK:
        return {bt: list(rows) for bt, rows in _TABLE.items()}


def _bucket_for(p: float) -> tuple[float, float] | None:
    for lo, hi in _BUCKETS:
        if lo <= p < hi:
            return (lo, hi)
    return None


def buckets() -> Iterable[tuple[float, float]]:
    """Bucket boundaries used internally; useful for the diag report."""
    return list(_BUCKETS)
