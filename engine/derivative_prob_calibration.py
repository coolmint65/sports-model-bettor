"""Post-hoc calibration for DERIVATIVE / QUARTER / PLAYER-PROP probabilities
(2026-09-16, MintyBot).

Why this exists
---------------
``engine/final_calibration.py`` de-biases the MAIN moneyline / spread /
total ``prob`` per league. The derivative markets (MLB F5 / team-total /
inning; NBA Q1; NHL period; every player-prop) are scored on DIFFERENT
paths and were never touched by it -- ``picks_core.score_pick`` only ever
applied the main per-league calibrator, keyed on ``sport``, which is the
WRONG curve for a Q1 total or a strikeout prop. On the settled history
those markets run badly OVERCONFIDENT (e.g. MLB Pitcher-Ks O/U predicted
60% vs 32% realized = +28pp; Batter-Hits O/U 81% vs 44% = +37pp; MLB
team-total +7.5pp). This module is the derivative twin of
final_calibration: ONE monotonic correction fit post-hoc on the settled
DERIVATIVE/PROP pick history, applied right before edge is derived.

Keying
------
Calibrators are keyed ``"<sport>:<bet_type>"`` -- the exact string built
by the derivative/prop pickers from the runtime ``sport`` + the pick's
``bet_type`` (which is what lands in the settled tables the fit reads),
e.g. ``"mlb:Pitcher Ks O/U"``, ``"mlb:Team Total"``, ``"nba:Q1 Team
Total"``, ``"nhl:Period Total"``. Resolution falls back exact key ->
per-sport pool ``"<sport>:_global"`` -> global pool ``"_global"`` ->
passthrough, so a thin / unseen derivative market still gets the pooled
de-overconfidence correction rather than the wrong main-market curve.

Storage
-------
Fitted calibrators live in ``data/derivative_prob_calibration.json``
(produced by ``fit_derivative_prob_calibration.py`` at repo root). Per
key: isotonic ``{"method":"isotonic","x":[...],"y":[...]}`` (n>=60) or
Platt ``{"method":"platt","A":..,"B":..}`` (30<=n<60); pooled
``"<sport>:_global"`` + ``"_global"`` isotonic fallbacks; a ``meta``
block records the fit date and per-key n.

Public API
----------
    apply(market_key, prob) -> float
        Map ``prob`` (0-1, or 0-100 -> normalized) through the calibrator
        for ``market_key`` (with the fallback chain above). Passes
        ``prob`` through unchanged when the JSON is absent, no calibrator
        (not even a pool) matches, or anything goes wrong. Pure-python
        (no numpy); the linear-interpolation path matches
        ``sklearn.isotonic.IsotonicRegression(out_of_bounds='clip')``.

Gated in picks_core / the prop scorers by the ``ENABLE_DERIV_CALIBRATION``
config flag (default TRUE). PAPER model (no live betting); the change is
reversible by flipping that flag or deleting the JSON.
"""

from __future__ import annotations

import json
import math
import os
import logging

logger = logging.getLogger(__name__)

_JSON_PATH = os.path.join(os.path.dirname(__file__), "..", "data",
                          "derivative_prob_calibration.json")

# Module-level cache. ``_CACHE`` is the loaded dict; ``_LOADED`` guards
# the one-shot load (so a missing file is not re-stat'd on every pick).
_CACHE: dict | None = None
_LOADED: bool = False


def _load() -> dict | None:
    global _CACHE, _LOADED
    if _LOADED:
        return _CACHE
    _LOADED = True
    try:
        with open(_JSON_PATH, "r", encoding="utf-8") as f:
            _CACHE = json.load(f)
    except Exception as exc:  # file absent / unreadable / bad JSON
        logger.debug("derivative_prob_calibration: no calibrators (%s)", exc)
        _CACHE = None
    return _CACHE


def reload() -> None:
    """Drop the cache so the next apply() re-reads the JSON. For tooling
    that regenerates the JSON in a live process."""
    global _CACHE, _LOADED
    _CACHE = None
    _LOADED = False


def _norm(p: float) -> float:
    """Normalize to 0-1 (the fit normalized DB probs the same way)."""
    p = float(p)
    return p / 100.0 if p > 1.0 else p


def _clip01(x: float) -> float:
    return 0.0 if x < 0.0 else (1.0 if x > 1.0 else x)


def _apply_isotonic(x: list, y: list, p: float) -> float:
    """Linear interpolation over the isotonic knots with endpoint
    clamping (equivalent to IsotonicRegression(out_of_bounds='clip'))."""
    if not x or not y:
        return p
    if p <= x[0]:
        return _clip01(y[0])
    if p >= x[-1]:
        return _clip01(y[-1])
    lo, hi = 0, len(x) - 1
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if x[mid] <= p:
            lo = mid
        else:
            hi = mid
    x0, x1, y0, y1 = x[lo], x[hi], y[lo], y[hi]
    if x1 == x0:
        return _clip01(y1)
    t = (p - x0) / (x1 - x0)
    return _clip01(y0 + t * (y1 - y0))


def _logit(p: float) -> float:
    p = min(max(p, 1e-6), 1 - 1e-6)
    return math.log(p / (1 - p))


def _sigmoid(z: float) -> float:
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


def _apply_platt(A: float, B: float, p: float) -> float:
    return _clip01(_sigmoid(A * _logit(p) + B))


def _apply_cal(cal: dict, p: float) -> float | None:
    """Apply one calibrator dict to a normalized prob, or None if the
    calibrator is malformed / an unknown method."""
    method = cal.get("method")
    if method == "identity":
        # Deliberate passthrough: this market is already well-calibrated
        # (or the fitted curve did not improve the holdout), so it is
        # shipped as a no-op to keep it from being pulled by the pooled
        # fallback. See fit_derivative_prob_calibration.py.
        return p
    if method == "isotonic":
        return _apply_isotonic(cal.get("x") or [], cal.get("y") or [], p)
    if method == "platt":
        return _apply_platt(float(cal["A"]), float(cal["B"]), p)
    return None


def apply(market_key: str, prob: float) -> float:
    """Return the post-hoc calibrated probability for ``prob`` under the
    calibrator keyed by ``market_key`` ("<sport>:<bet_type>").

    Fallback chain: exact key -> per-sport pool "<sport>:_global" ->
    global pool "_global" -> passthrough (``prob`` unchanged). Also
    passes through when the JSON is absent or on any error. Never raises.
    """
    try:
        blob = _load()
        if not blob:
            return prob
        p = _norm(prob)
        key = str(market_key or "")
        # 1. exact market key
        cal = blob.get(key)
        # 2. per-sport pooled fallback ("mlb:_global")
        if cal is None and ":" in key:
            cal = blob.get(key.split(":", 1)[0] + ":_global")
        # 3. global pooled fallback
        if cal is None:
            cal = blob.get("_global")
        if cal is None:
            return prob
        out = _apply_cal(cal, p)
        return prob if out is None else out
    except Exception as exc:
        logger.debug("derivative_prob_calibration.apply passthrough (%s)", exc)
        return prob


__all__ = ["apply", "reload"]
