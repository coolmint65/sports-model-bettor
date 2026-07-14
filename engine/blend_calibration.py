"""
Final-stage probability calibration layer.

Sits AFTER ``empirical_calibration`` in the picks pipeline. Where
``empirical_calibration`` does bucketed shrinkage per (sport,
bet_type, direction, edge_tier, juice_tier, prob_bucket), this layer
fits a single sport-wide isotonic regression on settled tracker
outcomes — catches systemic over/under-confidence the bucketed step
missed.

Why we need both
----------------
``empirical_calibration`` is direction-aware and tier-aware but each
bucket has thin samples; high-confidence buckets (80-90%, 90-100%)
get only 30-50 settled picks per sport, and small-sample fits go
back to passthrough. Net effect: high-confidence predictions don't
get pulled down enough.

This layer pools across all bet_types within a sport so the 80-100%
region has hundreds of samples, fits an isotonic curve over the
pooled data, and maps the post-empirical prob → calibrated prob.
The isotonic preserves rank order (a higher raw prob always emits a
higher calibrated prob) but bends the curve to match observed reality.

QA on 2026-05-01 showed MLB Brier 0.29 (worse than coin-flip 0.25)
with 95%-bucket reality at 38.5%. After this layer ships, those
high-confidence predictions snap to where reality actually lives.

Public API::

    fit_sport(sport)          — refresh the calibrator for `sport`
    calibrate(sport, raw_prob) — final-stage mapped prob

Persisted to data/blend_calibration.json so loads are cheap and the
calibrator survives restarts.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "blend_calibration.json",
)
_LOCK = threading.Lock()
_CACHE: dict[str, dict] = {}
_CACHE_LOADED = False

# Minimum settled picks before we fit. Below this we passthrough —
# fitting on thin data overfits noise and makes calibration WORSE.
#
# Raised from 80 → 200 on 2026-05-02 after NHL tracker data showed
# the failure mode: 95 settled picks during a -13.82% ROI bleeding
# period produced a Platt sigmoid that crushed every raw prob by
# ~15pp regardless of input. Net effect: every NHL pick post-blend
# went negative-edge, the slate emptied, no bets reached the card.
# The textbook "100 samples is enough for Platt" assumes the sample
# is unbiased; tracker data during a losing streak is biased and
# 95 samples isn't enough to wash that out. Identity passthrough
# until we have isotonic-grade volume is safer.
MIN_SAMPLES = 500

# Minimum decided picks per sport for sklearn isotonic; below this we
# fall back to identity passthrough (no Platt intermediate stage —
# see MIN_SAMPLES note above).
#
# Bumped 200 → 500 on 2026-05-03 after provenance log surfaced the same
# pathology on MLB (n=285): isotonic learned a step function mapping
# raw 0-0.49 → 0.001, 0.5-0.69 → 0.51, 0.7+ → 0.54. Net effect: every
# MLB underdog pick crushed to ~zero edge, every favorite pick capped
# at 54%. Sparse-bucket isotonic on biased pick samples is the issue.
# 500-pick floor means MLB stays passthrough until samples are dense
# enough that the curve is reasonable.
ISOTONIC_FLOOR = 500


# ── Sport sources ─────────────────────────────────────────────

_SOURCES = {
    "mlb": ("engine.db",         "picks"),
    "nhl": ("engine.nhl_db",     "nhl_picks"),
    "nba": ("engine.nba_db",     "nba_picks"),
    "tennis": ("engine.tennis_db", "tennis_picks"),
}


def _load_settled(sport: str, since: str | None = None) -> list[tuple[float, int]]:
    """Pull (model_prob, won) pairs from the settled tracker. Returns
    only W/L rows; pushes are excluded because they don't carry a
    calibration target."""
    src = _SOURCES.get(sport)
    if not src:
        return []
    db_module_name, table = src
    try:
        import importlib
        mod = importlib.import_module(db_module_name)
        conn = mod.get_conn()
    except Exception as e:
        logger.warning("blend_cal: cannot open %s: %s", db_module_name, e)
        return []
    where = ["result IN ('W', 'L')", "model_prob IS NOT NULL"]
    params: list = []
    if since:
        where.append("date >= ?")
        params.append(since)
    sql = f"SELECT model_prob, result FROM {table} WHERE {' AND '.join(where)}"
    try:
        rows = conn.execute(sql, params).fetchall()
    except Exception as e:
        logger.warning("blend_cal: query on %s failed: %s", table, e)
        return []
    out = []
    for r in rows:
        try:
            mp = float(r["model_prob"])
        except (TypeError, ValueError):
            continue
        if 0.0 < mp < 1.0:
            out.append((mp, 1 if r["result"] == "W" else 0))
    return out


# ── Fit primitives ────────────────────────────────────────────

def _fit_isotonic(samples: list[tuple[float, int]]) -> dict | None:
    """Fit an isotonic regression. Returns serializable {x, y} arrays
    representing the fitted step function — we don't pickle sklearn
    objects so the JSON file is portable."""
    try:
        from sklearn.isotonic import IsotonicRegression
        import numpy as np
    except ImportError:
        return None
    if len(samples) < ISOTONIC_FLOOR:
        return None
    X = np.array([s[0] for s in samples], dtype=float)
    y = np.array([s[1] for s in samples], dtype=float)
    iso = IsotonicRegression(y_min=0.001, y_max=0.999, out_of_bounds="clip")
    iso.fit(X, y)
    # Sample the curve at 101 points so applying it later is just a
    # piecewise-linear lookup — no sklearn dep at inference time.
    xs = np.linspace(0.0, 1.0, 101)
    ys = iso.predict(xs)
    return {
        "method": "isotonic",
        "n": len(samples),
        "x": [round(float(v), 4) for v in xs],
        "y": [round(float(v), 4) for v in ys],
    }


def _fit_platt(samples: list[tuple[float, int]]) -> dict | None:
    """Fit a sigmoid (Platt scaling): calibrated = 1/(1 + exp(A*x + B))
    where x is the model prob. Robust for small samples (≥100). Solves
    for A, B by Newton-Raphson on logistic regression NLL.
    """
    if len(samples) < MIN_SAMPLES:
        return None
    try:
        import numpy as np
        from scipy.optimize import minimize
    except ImportError:
        return None
    X = np.array([s[0] for s in samples], dtype=float)
    y = np.array([s[1] for s in samples], dtype=float)
    # logit transform — Platt fits in logit space so the sigmoid maps
    # back from raw prob through a learned slope/intercept
    logit_x = np.log(np.clip(X, 1e-3, 1 - 1e-3) /
                      np.clip(1 - X, 1e-3, 1 - 1e-3))

    def _nll(params: tuple) -> float:
        a, b = params
        z = a * logit_x + b
        # log(1 + exp(z)) — clipped for numerical stability
        z_clip = np.clip(z, -30, 30)
        nll = np.mean(np.log1p(np.exp(z_clip)) - y * z_clip)
        return float(nll)

    res = minimize(_nll, [1.0, 0.0], method="Nelder-Mead",
                    options={"xatol": 1e-4, "fatol": 1e-5})
    a, b = float(res.x[0]), float(res.x[1])
    return {"method": "platt", "n": len(samples), "a": a, "b": b}


def _identity_curve() -> dict:
    """Passthrough — used when sample size is too thin to fit."""
    return {"method": "identity", "n": 0}


# ── Public API ────────────────────────────────────────────────

def fit_sport(sport: str, since: str | None = None) -> dict:
    """Fit and persist the calibrator for ``sport``. Returns a summary."""
    samples = _load_settled(sport, since=since)
    summary = {"sport": sport, "n": len(samples)}
    # MIN_SAMPLES == ISOTONIC_FLOOR == 500 today, so the elif/else
    # below collapses to a single branch in practice. Restructured for
    # clarity and to make the dead-code "Platt-only" branch obvious if
    # the constants ever diverge.
    if len(samples) < MIN_SAMPLES:
        curve = _identity_curve()
        summary["fit"] = "passthrough"
    elif len(samples) >= ISOTONIC_FLOOR:
        curve = _fit_isotonic(samples) or _fit_platt(samples) or _identity_curve()
        summary["fit"] = curve["method"]
    else:
        # Reached only if MIN_SAMPLES < ISOTONIC_FLOOR (currently equal).
        curve = _fit_platt(samples) or _identity_curve()
        summary["fit"] = curve["method"]
    curve["fit_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    _persist(sport, curve)
    summary["curve"] = curve
    logger.info("blend_cal[%s]: %s on n=%d", sport, summary["fit"], len(samples))
    return summary


def fit_all_sports(since: str | None = None) -> dict:
    return {s: fit_sport(s, since=since) for s in _SOURCES}


def calibrate(sport: str, raw_prob: float) -> float:
    """Map ``raw_prob`` (post-empirical_calibration output) through
    the fitted calibrator. Returns the raw value when no calibrator
    is loaded for the sport.

    Contract: monotonic on raw_prob (higher in → higher out).
    """
    if raw_prob is None:
        return raw_prob
    p = max(0.0, min(1.0, float(raw_prob)))
    curve = _curve_for(sport)
    if not curve:
        return p
    method = curve.get("method")
    if method == "identity":
        return p
    if method == "isotonic":
        # piecewise-linear lookup against the sampled (x, y) arrays
        xs = curve.get("x") or []
        ys = curve.get("y") or []
        if not xs or not ys or len(xs) != len(ys):
            return p
        # Binary search would be faster but the array is small (101)
        # so a linear scan is fine.
        if p <= xs[0]:
            return ys[0]
        if p >= xs[-1]:
            return ys[-1]
        for i in range(1, len(xs)):
            if p <= xs[i]:
                # Interpolate between (xs[i-1], ys[i-1]) and (xs[i], ys[i])
                x0, x1 = xs[i - 1], xs[i]
                y0, y1 = ys[i - 1], ys[i]
                if x1 == x0:
                    return y0
                t = (p - x0) / (x1 - x0)
                return y0 + t * (y1 - y0)
        return ys[-1]
    if method == "platt":
        import math
        a = curve.get("a", 1.0)
        b = curve.get("b", 0.0)
        # logit(p) = log(p / (1-p))
        p_c = max(1e-3, min(1 - 1e-3, p))
        logit = math.log(p_c / (1 - p_c))
        z = a * logit + b
        # Sigmoid → calibrated
        if z >= 0:
            return 1.0 / (1.0 + math.exp(-z))
        return math.exp(z) / (1.0 + math.exp(z))
    return p


# ── Persistence ───────────────────────────────────────────────

def _persist(sport: str, curve: dict) -> None:
    with _LOCK:
        _ensure_loaded()
        _CACHE[sport] = curve
        os.makedirs(os.path.dirname(_PATH), exist_ok=True)
        with open(_PATH, "w", encoding="utf-8") as f:
            json.dump(_CACHE, f, indent=2, sort_keys=True)


def _curve_for(sport: str) -> dict | None:
    _ensure_loaded()
    return _CACHE.get(sport)


def _ensure_loaded() -> None:
    global _CACHE_LOADED
    if _CACHE_LOADED:
        return
    _CACHE_LOADED = True
    if not os.path.exists(_PATH):
        return
    try:
        with open(_PATH, "r", encoding="utf-8") as f:
            blob = json.load(f) or {}
    except Exception as e:
        logger.warning("blend_cal: load failed: %s", e)
        return
    if isinstance(blob, dict):
        _CACHE.update(blob)


def reload() -> None:
    global _CACHE_LOADED
    _CACHE.clear()
    _CACHE_LOADED = False
    _ensure_loaded()


# ── CLI ───────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    import argparse
    import sys
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(prog="engine.blend_calibration")
    ap.add_argument("--sport", choices=tuple(_SOURCES.keys()) + ("all",),
                    default="all")
    ap.add_argument("--since", type=str, default=None,
                    help="Lower-bound on settled date (YYYY-MM-DD).")
    args = ap.parse_args(argv)
    if args.sport == "all":
        summary = fit_all_sports(since=args.since)
    else:
        summary = {args.sport: fit_sport(args.sport, since=args.since)}
    print()
    for sport, s in summary.items():
        print(f"  {sport}: fit={s['fit']:<12s}  n={s['n']}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())


__all__ = [
    "fit_sport", "fit_all_sports", "calibrate", "reload",
    "MIN_SAMPLES", "ISOTONIC_FLOOR",
]
