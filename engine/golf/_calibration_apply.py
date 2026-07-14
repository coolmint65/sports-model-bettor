"""Golf calibration applicator.

Reads per-tour ``data/golf/<tour>_calibration.json`` produced by
``engine.golf._calibrate`` and exposes ``calibrate(tour, market,
raw_prob)`` for the picks engine. Returns a Bayesian-shrunk realized
hit-rate based on the bucket the raw prob falls in.

Golf can't use the shared ``engine.framework_calibration`` adapter
because its bucket grid is wrong-sized for golf markets — WINNER
probs live in 0.5–15% range, MAKE_CUT in 55–95%, and the shared 8
buckets starting at 0.30 would dump every WINNER pick into the
catch-all. Calibrator uses per-market grids; this applicator mirrors
them.

Cache: per-tour JSON read once + held in memory, refreshed when the
file mtime advances (so a fresh weekly recalibration takes effect
without a restart).
"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

_BASE = Path(__file__).resolve().parent.parent.parent / "data" / "golf"

_CACHE: dict[str, dict | None] = {}
_CACHE_MTIMES: dict[str, float] = {}
_LOCK = threading.Lock()


# Same shrinkage constant as engine.framework_calibration so calibration
# behaviour matches the rest of the codebase. n=6 going 6/6 at avg_pred=0.05
# without shrinkage would return 1.0; with n0=10 it shrinks to ~0.40, a
# more responsible point estimate for a thin bucket.
_PRIOR_N0 = 10.0


def _path(tour: str) -> Path:
    return _BASE / f"{tour}_calibration.json"


def _load(tour: str) -> dict | None:
    """Read + memoize the calibration JSON for ``tour``. Refreshes when
    the file's mtime advances (i.e. a fresh walk-forward refit just
    landed) so long-running processes don't keep using the stale table."""
    with _LOCK:
        path = _path(tour)
        if not path.exists():
            _CACHE[tour] = None
            _CACHE_MTIMES[tour] = 0.0
            return None
        cur_mtime = path.stat().st_mtime
        if tour in _CACHE and _CACHE_MTIMES.get(tour) == cur_mtime:
            return _CACHE[tour]
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("[golf:%s] calibration load failed: %s", tour, exc)
            _CACHE[tour] = None
            _CACHE_MTIMES[tour] = cur_mtime
            return None
        if not (data.get("buckets") or {}):
            logger.warning("[golf:%s] calibration JSON has no buckets — "
                           "treating as cold-start", tour)
            _CACHE[tour] = None
            _CACHE_MTIMES[tour] = cur_mtime
            return None
        _CACHE[tour] = data
        _CACHE_MTIMES[tour] = cur_mtime
        return data


def invalidate(tour: str | None = None) -> None:
    """Drop cached table for ``tour`` (or all when None)."""
    with _LOCK:
        if tour is None:
            _CACHE.clear()
            _CACHE_MTIMES.clear()
        else:
            _CACHE.pop(tour, None)
            _CACHE_MTIMES.pop(tour, None)


def is_available(tour: str) -> bool:
    """True iff a calibration JSON exists for ``tour``."""
    return _load(tour) is not None


def calibrate(tour: str, market: str, raw_prob: float) -> float:
    """Map raw ``raw_prob`` to a Bayesian-shrunk realized hit rate
    using the (tour, market, bucket) cell from the walk-forward table.

    Returns raw_prob when no calibration JSON exists for the tour
    (cold-start passthrough) or when the bucket is missing / empty.
    """
    if raw_prob is None:
        return raw_prob
    data = _load(tour)
    if not data:
        return raw_prob
    buckets = (data.get("buckets") or {}).get(market) or []
    if not buckets:
        return raw_prob

    # Find the bucket the raw prob falls into.
    cell: dict | None = None
    for entry in buckets:
        bk = entry.get("bucket") or [None, None]
        lo, hi = bk[0], bk[1]
        if lo is None or hi is None:
            continue
        if lo <= raw_prob < hi:
            cell = entry
            break
    if cell is None:
        return raw_prob

    n = int(cell.get("n") or 0)
    realized = cell.get("realized_wr")
    avg_pred = cell.get("avg_pred")
    if realized is None or n <= 0:
        return raw_prob

    if avg_pred is None:
        # Malformed cell — fall back to bucket midpoint as the
        # shrinkage center. Warn so a re-seed is triggered.
        bk = cell.get("bucket") or [None, None]
        lo, hi = bk[0], bk[1]
        if lo is None or hi is None:
            return raw_prob
        logger.warning("[golf:%s] %s bucket [%.4f, %.4f) has n=%d "
                       "realized=%.3f but avg_pred missing", tour, market,
                       lo, hi, n, float(realized))
        center = (lo + hi) / 2.0
    else:
        center = float(avg_pred)

    calibrated = (n * float(realized) + _PRIOR_N0 * center) / (n + _PRIOR_N0)
    return calibrated
