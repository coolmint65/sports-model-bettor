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
# Set to 10 because at N=5-9 a single bad streak can make a bucket look
# like 15% real WR when underlying truth is closer to 50%; that level of
# noise actively misleads the picks engine. 10 trades cold-start coverage
# for fewer false-positive recalibrations.
MIN_BUCKET_N = 10

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

# Per-sport calibration tables: {sport: {bet_type: [(lo, hi, real_wr)]}}
_TABLE: dict[str, dict[str, list[tuple[float, float, float]]]] = {}
_TABLE_LOCK = threading.Lock()
_TABLE_LOADED: dict[str, bool] = {}

# Per-sport pick-table location. Each sport has its own SQLite DB and
# its own picks-table name. Adding a sport = one entry here.
_SPORT_SOURCES = {
    "mlb": ("engine.db",     "picks"),
    "nhl": ("engine.nhl_db", "nhl_picks"),
    "nba": ("engine.nba_db", "nba_picks"),
}


def _normalize_bet_type(bt: str) -> str:
    """Collapse capitalization variants the tracker has accumulated."""
    if not bt:
        return ""
    bt = bt.strip().lower()
    # Keep "1st inn" -> "nrfi" so NRFI / 1st INN bucket together
    if bt in ("1st inn", "1stinn"):
        return "nrfi"
    return bt


def calibrate(bet_type: str, raw_prob: float, sport: str = "mlb") -> float:
    """Map a raw model probability to the empirical win-rate bucket
    for `sport`.

    Falls back to raw passthrough when:
      - The sport's calibration table hasn't been built yet (cold start)
      - This bet_type has no data for the sport
      - The matched bucket has fewer than MIN_BUCKET_N samples
    """
    if raw_prob is None:
        return raw_prob
    if not _TABLE_LOADED.get(sport):
        # Lazy-load on first call so callers don't need to know about it.
        try:
            refresh_calibration(sport)
        except Exception as e:
            logger.debug("calibration load failed (%s): passthrough: %s",
                         sport, e)
            return raw_prob

    bt = _normalize_bet_type(bet_type)
    sport_table = _TABLE.get(sport, {})
    rows = sport_table.get(bt)
    if not rows:
        return raw_prob

    for lo, hi, real_wr in rows:
        if lo <= raw_prob < hi:
            return real_wr

    # raw is outside our buckets -- match the closest end.
    if raw_prob < rows[0][0]:
        return rows[0][2]
    return rows[-1][2]


def calibrated_edge(bet_type: str, raw_prob: float, odds: int,
                    sport: str = "mlb") -> float:
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
    cal = calibrate(bet_type, raw_prob, sport=sport)
    return (cal - implied) * 100.0


def refresh_calibration(sport: str = "mlb") -> dict:
    """Rebuild the calibration table for `sport` from its picks table.
    Returns a summary dict for callers that want to log progress."""
    summary = {"sport": sport, "buckets_filled": 0,
               "buckets_passthrough": 0, "total_rows": 0}
    src = _SPORT_SOURCES.get(sport)
    if not src:
        logger.warning("calibration: unknown sport %r", sport)
        return summary
    db_module_name, table_name = src

    try:
        import importlib
        db_module = importlib.import_module(db_module_name)
        get_conn = db_module.get_conn
    except Exception as e:
        logger.warning("calibration: cannot import %s: %s",
                       db_module_name, e)
        return summary

    try:
        rows = get_conn().execute(
            f"SELECT bet_type, model_prob, result FROM {table_name} "
            "WHERE result IN ('W', 'L') AND model_prob IS NOT NULL"
        ).fetchall()
    except Exception as e:
        logger.warning("calibration: query on %s failed: %s",
                       table_name, e)
        return summary

    summary["total_rows"] = len(rows)
    if not rows:
        with _TABLE_LOCK:
            _TABLE[sport] = {}
            _TABLE_LOADED[sport] = True
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
        _TABLE[sport] = dict(new_table)
        _TABLE_LOADED[sport] = True

    logger.info("calibration[%s]: built from %d rows -- %d buckets "
                "filled, %d passthrough",
                sport, summary["total_rows"], summary["buckets_filled"],
                summary["buckets_passthrough"])
    return summary


def refresh_all_sports() -> dict:
    """Refresh every sport in _SPORT_SOURCES. Returns per-sport summaries."""
    return {sport: refresh_calibration(sport) for sport in _SPORT_SOURCES}


def snapshot(sport: str | None = None) -> dict:
    """Return the calibration table for `sport`, or all sports when None."""
    with _TABLE_LOCK:
        if sport is None:
            return {s: {bt: list(rows) for bt, rows in tbl.items()}
                    for s, tbl in _TABLE.items()}
        return {bt: list(rows) for bt, rows in _TABLE.get(sport, {}).items()}


def _bucket_for(p: float) -> tuple[float, float] | None:
    for lo, hi in _BUCKETS:
        if lo <= p < hi:
            return (lo, hi)
    return None


def buckets() -> Iterable[tuple[float, float]]:
    """Bucket boundaries used internally; useful for the diag report."""
    return list(_BUCKETS)
