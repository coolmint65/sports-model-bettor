"""Shared probability-bucket grid for calibration modules.

Both ``empirical_calibration`` and ``framework_calibration`` (and the
walk-forward seeders that produce their input data) bucket predictions
on the same favored-side probability grid. Earlier each module defined
its own ``_BUCKETS`` literal — if anyone ever updated one without the
other, the same raw_prob would land in different buckets in different
modules and calibration drifts apart silently.

Importing from here is the only sanctioned way to add or modify a
bucket boundary.
"""
from __future__ import annotations

# Bucketed on favored-side prob (always >= 0.5). Tighter spacing in
# the 0.5-0.7 range where the bulk of edge picks live; wider tails
# because realized data is too thin to support fine bucketing there.
BUCKETS: list[tuple[float, float]] = [
    (0.30, 0.40),
    (0.40, 0.50),
    (0.50, 0.55),
    (0.55, 0.60),
    (0.60, 0.65),
    (0.65, 0.70),
    (0.70, 0.80),
    (0.80, 1.01),
]


def bucket_for(p: float) -> tuple[float, float] | None:
    """Return the (lo, hi) tuple containing ``p``, or None if out of
    range. Half-open like the producers' ``lo <= p < hi`` convention."""
    for lo, hi in BUCKETS:
        if lo <= p < hi:
            return (lo, hi)
    return None
