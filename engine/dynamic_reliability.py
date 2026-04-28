"""
Auto-tuned per-(sport, bet_type) reliability multiplier.

Why: the static MLB/NHL/NBA_BET_RELIABILITY dicts in engine.config were
hand-tuned once and only change on source edits. They drift out of sync
with what the live tracker actually shows — F5 Team Total tested at
+25.51 ROI in the leak-free retro backtest but stayed at 0.50 reliability
because nobody bumped it. This module reads settled picks from each
sport's `picks` + `derivative_picks` table, computes ROI per pick by
bet_type, and maps that to a reliability multiplier on the same scale
the static dicts use (0.5 ≈ neutral, 1.0 ≈ proven, 1.5 ≈ exceptional,
0.1 ≈ demote heavily).

Cold-start guard: bet types with fewer than MIN_SETTLED settled picks
fall back to the static dict value so a 5-pick streak can't swing the
multiplier wildly. Between MIN_SETTLED and STABLE_N we blend the
computed value with the static one, weighting toward computed as
volume grows.

Calibration-bucket bonus: when the calibration table has high-n buckets
for this bet_type, we add a small confidence bonus so well-calibrated
markets outrank thinly-sampled ones at the same raw edge — addresses
the "POTD just picks the noisiest market" failure mode.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# Minimum settled picks before any auto-tuning kicks in. Below this
# threshold the static dict value is returned unchanged.
MIN_SETTLED = 30
# Volume at which the auto-tuned value is fully trusted. Between
# MIN_SETTLED and this we blend with the static value, weighting toward
# computed as n grows.
STABLE_N = 100

# Reliability bounds. The static dicts use 0.30–1.00; we widen the
# upper bound to 1.50 so genuinely exceptional bet types (proven by
# real ROI) can outrank the historical best.
RELIABILITY_FLOOR = 0.10
RELIABILITY_CEIL = 1.50

# How aggressively ROI maps to reliability. roi_per_pick of 0 (break-even)
# → 0.50; +25 → 1.50; -25 → -0.50 (clamped to floor). 25 picked because
# the post-leak-fix backtest showed F5 Team Total at +25 ROI as the best
# market — anchoring the ceiling there keeps the scale honest.
ROI_SCALE = 25.0
NEUTRAL_RELIABILITY = 0.50

# Calibration-bucket bonus: a bet type with multiple high-n buckets in
# the calibration table is more trustworthy than one with sparse data.
# Multiplier ranges 0.85 (no usable buckets) to 1.10 (>=3 buckets each
# with n >= 200). Small effect by design — calibration is already
# applied to `prob`, this just nudges POTD selection toward markets
# whose calibration we have high confidence in.
CALIBRATION_BONUS_FLOOR = 0.85
CALIBRATION_BONUS_CEIL = 1.10
CALIBRATION_BUCKET_HIGH_N = 200

_CACHE_TTL_S = 1800  # 30 minutes
_cache: dict[tuple[str, str], tuple[float, float]] = {}  # (sport, bt) -> (ts, value)


def _conn(sport: str):
    if sport == "mlb":
        from .db import get_conn
    elif sport == "nhl":
        from .nhl_db import get_conn
    elif sport == "nba":
        from .nba_db import get_conn
    else:
        raise ValueError(f"unknown sport {sport}")
    return get_conn()


def _settled_stats(sport: str, bet_type: str) -> tuple[int, float]:
    """Return (n_settled, roi_per_pick) across both core picks + derivative
    picks tables for this (sport, bet_type)."""
    conn = _conn(sport)
    main_table = {"mlb": "picks", "nhl": "nhl_picks", "nba": "nba_picks"}[sport]
    deriv_table = {"mlb": "derivative_picks",
                   "nhl": "nhl_derivative_picks",
                   "nba": "nba_derivative_picks"}[sport]

    n = 0
    profit_sum = 0.0
    for table in (main_table, deriv_table):
        try:
            row = conn.execute(
                f"SELECT COUNT(*) AS n, COALESCE(SUM(profit), 0) AS p "
                f"FROM {table} WHERE bet_type = ? AND result IN ('W','L','P')",
                (bet_type,),
            ).fetchone()
            if row:
                n += int(row["n"] or 0)
                profit_sum += float(row["p"] or 0)
        except Exception as e:
            # Table missing (NBA derivative_picks may not exist on first
            # run) — log once and continue. Don't let a missing table
            # bring down the whole reliability lookup.
            logger.debug("reliability stats: %s table query failed: %s",
                         table, e)
    if n == 0:
        return (0, 0.0)
    return (n, profit_sum / n)


def _calibration_bonus(sport: str, bet_type: str) -> float:
    """Confidence multiplier from calibration-bucket sample sizes.

    Returns CALIBRATION_BONUS_FLOOR..CALIBRATION_BONUS_CEIL based on how
    many high-n buckets exist for this bet_type. A bet type with 3+
    buckets each at n≥200 is well-calibrated; one with no usable buckets
    is shrunk to FLOOR.
    """
    try:
        from .empirical_calibration import (
            _TABLE, _TABLE_LOADED, _normalize_bet_type, refresh_calibration,
        )
    except ImportError:
        return 1.0

    # Lazy-load the calibration table on first call. Without this, the
    # first reliability lookup of a process sees an empty _TABLE and
    # short-circuits to FLOOR, which crushes well-calibrated bet types
    # back to the static dict's value.
    if not _TABLE_LOADED.get(sport):
        try:
            refresh_calibration(sport)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(
                "calibration refresh for %s failed (%s); reliability "
                "lookup falling back to floor — picks will weight by "
                "static dict only until next refresh", sport, e,
            )
            return CALIBRATION_BONUS_FLOOR

    sport_table = _TABLE.get(sport, {})
    if not sport_table:
        return CALIBRATION_BONUS_FLOOR

    bt_norm = _normalize_bet_type(bet_type)
    high_n_buckets = 0
    for key, bucket in sport_table.items():
        # Match the legacy (bt, prob_bucket) key form — counts coarse
        # buckets only, which is the form most reliably populated.
        if not isinstance(key, tuple) or len(key) != 2:
            continue
        if key[0] != bt_norm:
            continue
        if (bucket.get("n") or 0) >= CALIBRATION_BUCKET_HIGH_N:
            high_n_buckets += 1

    # Linear ramp from FLOOR (0 buckets) to CEIL (>=3 buckets).
    ramp = min(1.0, high_n_buckets / 3.0)
    return CALIBRATION_BONUS_FLOOR + ramp * (CALIBRATION_BONUS_CEIL - CALIBRATION_BONUS_FLOOR)


def _static_fallback(sport: str, bet_type: str) -> float:
    """Original hand-tuned dict value, used as cold-start fallback."""
    from .config import (
        MLB_BET_RELIABILITY, NHL_BET_RELIABILITY, NBA_BET_RELIABILITY,
    )
    table = {"mlb": MLB_BET_RELIABILITY,
             "nhl": NHL_BET_RELIABILITY,
             "nba": NBA_BET_RELIABILITY}.get(sport, {})
    return float(table.get(bet_type, NEUTRAL_RELIABILITY))


def get_reliability(sport: str, bet_type: str) -> float:
    """Auto-tuned reliability multiplier in roughly the same 0.1–1.5
    range the static dicts used. Cached for 30 minutes per (sport, bt).
    """
    key = (sport, bet_type)
    now = time.time()
    cached = _cache.get(key)
    if cached and (now - cached[0]) < _CACHE_TTL_S:
        return cached[1]

    static = _static_fallback(sport, bet_type)

    try:
        n, roi = _settled_stats(sport, bet_type)
    except Exception as e:
        logger.debug("reliability lookup failed for %s/%s: %s",
                     sport, bet_type, e)
        _cache[key] = (now, static)
        return static

    if n < MIN_SETTLED:
        # Cold start — trust the hand-tuned value. Apply only the
        # calibration-bucket bonus so well-calibrated markets still
        # get a small lift over uncalibrated ones.
        result = static * _calibration_bonus(sport, bet_type)
        result = max(RELIABILITY_FLOOR, min(RELIABILITY_CEIL, result))
        _cache[key] = (now, result)
        return result

    # Map ROI -> reliability on the documented scale.
    computed = NEUTRAL_RELIABILITY + (roi / ROI_SCALE)
    computed = max(RELIABILITY_FLOOR, min(RELIABILITY_CEIL, computed))

    # Blend computed with static between MIN_SETTLED and STABLE_N so a
    # 31-pick sample doesn't override a hand-tuned value wholesale.
    if n >= STABLE_N:
        blend_w = 1.0
    else:
        blend_w = (n - MIN_SETTLED) / (STABLE_N - MIN_SETTLED)
    result = blend_w * computed + (1.0 - blend_w) * static

    # Apply calibration-bucket bonus on top.
    result *= _calibration_bonus(sport, bet_type)
    result = max(RELIABILITY_FLOOR, min(RELIABILITY_CEIL, result))

    _cache[key] = (now, result)
    return result


def clear_cache() -> None:
    """Force a recompute on the next get_reliability call. Useful after
    the settler runs a batch and the tracker has materially new data.
    """
    _cache.clear()


def snapshot(sport: str) -> dict:
    """Return the full reliability table for a sport, computed fresh.
    Mostly for diagnostics — `python -m engine.dynamic_reliability mlb`."""
    from .config import (
        MLB_BET_RELIABILITY, NHL_BET_RELIABILITY, NBA_BET_RELIABILITY,
    )
    table = {"mlb": MLB_BET_RELIABILITY,
             "nhl": NHL_BET_RELIABILITY,
             "nba": NBA_BET_RELIABILITY}.get(sport, {})
    out = {}
    clear_cache()
    for bt in table:
        n, roi = _settled_stats(sport, bt)
        out[bt] = {
            "static": _static_fallback(sport, bt),
            "settled_n": n,
            "roi_per_pick": round(roi, 2),
            "calibration_bonus": round(_calibration_bonus(sport, bt), 3),
            "computed": round(get_reliability(sport, bt), 3),
        }
    return out


if __name__ == "__main__":
    import sys, json
    sport = sys.argv[1] if len(sys.argv) > 1 else "mlb"
    print(json.dumps(snapshot(sport), indent=2))
