"""Apply per-league calibration tables to raw model probabilities.

Reads ``data/basketball/<league>_calibration.json`` (output of B4
walk-forward seeding) and exposes a ``calibrate(league, bet_type, raw_prob)``
function the picks pipeline calls before scoring.

For each (bet_type, prob_bucket) we use the realized hit rate from the
walk-forward window. Sparse buckets (n < 5) fall through to the raw prob
to avoid noisy adjustments. Missing tables fall through to identity.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "basketball"
_TABLES: dict[str, dict] = {}
_MIN_BUCKET_N = 5  # below this, prefer raw over the noisy bucket realized
# Empirical-Bayes prior strength. A bucket with n=6 going 6/6 yields
# realized_wr=1.0 — mapping every prob in that bucket to 100% is the
# kind of overfit the user reported (NZ NBL: raw 52.5% → calibrated
# 100% → bumped to 90.5%, picking the wrong side of a near-coinflip).
# n0=10 pulls toward the bucket's avg_pred so a 6-sample fit only moves
# the prob ~37% of the way to the realized rate.
_PRIOR_N0 = 10.0


def _load(league: str) -> dict | None:
    if league in _TABLES:
        return _TABLES[league]
    path = _DIR / f"{league}_calibration.json"
    if not path.exists():
        _TABLES[league] = {}
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        _TABLES[league] = data.get("buckets") or {}
        return _TABLES[league]
    except Exception as e:
        logger.warning("[%s] calibration table load failed: %s", league, e)
        _TABLES[league] = {}
        return None


def calibrate(league: str, bet_type: str, raw_prob: float) -> float:
    """Return calibrated probability for ``bet_type`` at ``raw_prob``.

    Falls through to raw_prob when:
      - league has no calibration table
      - bet_type isn't in the table
      - the matching bucket has fewer than _MIN_BUCKET_N samples
    """
    table = _load(league)
    if not table:
        return raw_prob
    bt_buckets = table.get(bet_type)
    if not bt_buckets:
        return raw_prob
    for entry in bt_buckets:
        bucket = entry.get("bucket") or [None, None]
        if bucket[0] is None or bucket[1] is None:
            continue
        if bucket[0] <= raw_prob < bucket[1]:
            n = entry.get("n", 0)
            realized = entry.get("realized_wr")
            if n >= _MIN_BUCKET_N and realized is not None:
                # Beta-binomial shrinkage toward the bucket's avg_pred
                # (or the bucket midpoint when avg_pred is missing). For
                # a small-sample bucket the realized rate carries less
                # weight; as n grows it dominates.
                avg_pred = entry.get("avg_pred")
                center = (avg_pred if avg_pred is not None
                           else (bucket[0] + bucket[1]) / 2.0)
                return float((n * realized + _PRIOR_N0 * center)
                              / (n + _PRIOR_N0))
            return raw_prob
    return raw_prob


def reset_cache() -> None:
    _TABLES.clear()
