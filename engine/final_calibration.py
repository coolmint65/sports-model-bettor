"""Final post-hoc per-league calibration correction (2026-09-15, MintyBot).

Why this exists
---------------
The model's internal calibration chain (empirical_calibration ->
blend_calibration -> closing_line_prior in ``engine/picks_core.py``)
leaves the final ``prob`` badly OVERCONFIDENT: on settled picks the
average predicted win% ran +13 to +38pp above the realized win% in
every league (cfb 74/36, nfl 62/29, atp 63/43, wnba 72/53, wta 62/46,
mlb 66/51, nhl 60/47, nba 68/55). Rather than re-open the (fragile)
internal stages, this module applies ONE more monotonic correction,
fit post-hoc on the settled-pick history, right before ``edge`` is
derived from the calibrated prob.

Keying
------
Calibrators are keyed on the exact string ``picks_core.score_pick``
receives as its ``sport`` argument -- which IS the runtime league
identifier the per-sport pickers pass:
    tennis_picks.py   -> "tennis"            (atp + wta pooled)
    picks.py (mlb)    -> "mlb"
    sports/nba        -> "nba"
    sports/nhl        -> "nhl"
    football/_picks   -> "football_<league>" (football_cfb, football_nfl)
    baseball/_picks   -> "baseball_<league>" (baseball_college)
    basketball/_picks -> "<league>"          (wnba, puerto_rico_bsn, ...)
    soccer/_picks     -> "<league>"          (mls, esp_laliga, ...)
    hockey/_picks     -> "<league>"          (aihl, ahl, pwhl, nzihl)
So ``apply(sport, cal)`` in picks_core needs no key munging -- it passes
``sport`` straight through and it matches a fitted key (or falls to the
pooled ``_global`` calibrator, or passes through unchanged).

Storage
-------
Fitted calibrators live in ``data/final_calibration.json`` (produced by
the offline fit; see MEMORY / the calibration build notes). Per key:
isotonic ``{"method":"isotonic","x":[...],"y":[...]}`` knot arrays, or
Platt ``{"method":"platt","A":..,"B":..}``. A ``_global`` key (isotonic,
all leagues pooled) is the fallback for unknown / thin keys, and a
``meta`` block records the fit date and per-key n.

n-aware shrinkage (2026-10-10)
------------------------------
Every key above also carries its own fit ``n`` (settled-pick count). A
thin fit -- football_nfl n=31, afl/usl_championship/conmebol_libertadores
n=58/50/33 -- is a noisy correction; trusting it as much as tennis's
n=1499 fit is overfit, not calibration (concretely: football_nfl picks
went to 0, football_cfb's 3-knot isotonic flatlines every raw>=0.575 to
~0.36). ``apply()`` now credibility-weights the fitted correction by its
own n before the final clamp: ``w = n/(n+FC_SHRINK_N0)``, blending
``calibrated`` with the INPUT prob -- ``shrunk = w*calibrated +
(1-w)*prob``. Thin n -> w small -> output ~= input (this stage goes
nearly inert, deferring to the already-calibrated empirical/blend/
closing-line chain upstream); n >> FC_SHRINK_N0 -> w ~= 1 -> trust the
fit as before. See ``FC_SHRINK_N0`` below.

Public API
----------
    apply(key, prob) -> float
        Map ``prob`` (0-1, or 0-100 -> normalized) through the calibrator
        for ``key``. Falls back to ``_global`` when the key is missing;
        passes ``prob`` through unchanged if the file is absent/unreadable
        or anything goes wrong. Pure-python (no numpy dependency); the
        linear-interpolation path was verified to match
        ``sklearn.isotonic.IsotonicRegression.predict`` to 0.0 abs error.

This stage is gated in picks_core by the ``ENABLE_FINAL_CALIBRATION``
config flag (default TRUE). The model is PAPER (no live betting); the
change is reversible by flipping that flag or deleting the JSON.
"""

from __future__ import annotations

import json
import math
import os
import logging

logger = logging.getLogger(__name__)

_JSON_PATH = os.path.join(os.path.dirname(__file__), "..", "data",
                          "final_calibration.json")

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
        logger.debug("final_calibration: no calibrators loaded (%s)", exc)
        _CACHE = None
    return _CACHE


def reload() -> None:
    """Drop the cache so the next apply() re-reads the JSON. For tooling
    that regenerates ``data/final_calibration.json`` in a live process."""
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


_FC_Y_LO, _FC_Y_HI, _FC_Y_SAT = 0.05, 0.95, 0.98


def _guard_iso(x, y):
    """De-saturate a degenerate isotonic tail + guard-band (Austin 2026-10-07).
    An isotonic fit on a thin all-win END bin yields a y=1.0 knot, which maps every
    high-confidence prob to a literal 100% -> fake "+35% edge" POTDs (the empirical layer
    already guards this; the final layer did not). DROP a saturated TOP tail so the curve
    clamps to the last RELIABLE knot (the plateau) rather than 1.0, then guard-band all y
    (which floors a degenerate 0.0 bottom WITHOUT inflating genuinely-low probs)."""
    if not x or not y or len(x) != len(y):
        return x, y
    xs, ys = list(x), list(y)
    while len(ys) > 1 and ys[-1] >= _FC_Y_SAT:
        xs.pop(); ys.pop()
    ys = [min(max(v, _FC_Y_LO), _FC_Y_HI) for v in ys]
    return xs, ys


# n-aware shrinkage pseudo-count (Austin/MintyBot 2026-10-10). ``w =
# n / (n + FC_SHRINK_N0)`` is the standard Bayesian-shrinkage shape (same
# form as a Beta/Normal conjugate posterior weight, with FC_SHRINK_N0
# "pseudo-observations" of no-correction as the prior): n == FC_SHRINK_N0
# means the fit gets half-credibility (w=0.5). Validated at {50,100,200}
# against current candidates (NFL/CFB/thin-Platt leagues back to sensible
# picks vs. healthy large-n sports [mlb/nhl/tennis] staying ~unchanged,
# w>=0.9 at their n) -- see MEMORY calibration-shrinkage validation notes
# for the sweep table. 100 below is a provisional middle-of-sweep value;
# bump to whichever N0 Austin confirms from that sweep. Changing it is a
# one-line, no-restart-required edit (next apply() call re-reads the
# module constant, no cache to invalidate).
FC_SHRINK_N0 = 100


def apply(key: str, prob: float) -> float:
    """Return the post-hoc calibrated probability for ``prob`` under the
    calibrator keyed by ``key`` (the score_pick ``sport`` argument),
    n-aware-shrunk toward the input ``prob`` (see FC_SHRINK_N0 above and
    the module docstring's "n-aware shrinkage" section).

    Passthrough (returns ``prob`` unchanged) when the JSON is absent, the
    key has no calibrator AND there is no ``_global`` fallback, or on any
    error. Never raises.
    """
    try:
        blob = _load()
        if not blob:
            return prob
        p = _norm(prob)
        cal = blob.get(key)
        if cal is None:
            cal = blob.get("_global")
        if cal is None:
            return prob
        method = cal.get("method")
        if method == "isotonic":
            _gx, _gy = _guard_iso(cal.get("x") or [], cal.get("y") or [])
            calibrated = _apply_isotonic(_gx, _gy, p)
        elif method == "platt":
            calibrated = _apply_platt(float(cal["A"]), float(cal["B"]), p)
        else:
            return prob
        # Credibility-weight the correction by the fit's own n (the
        # `_global` entry's n, on the fallback path -- `cal` is already
        # resolved to whichever blob entry actually fired above). Missing
        # n -> w=1 (no shrinkage -- conservative: unchanged pre-shrinkage
        # behavior). n is a plain settled-pick count, always >= 0, and
        # FC_SHRINK_N0 is a positive module constant, so n + FC_SHRINK_N0
        # is never <= 0 here.
        n = cal.get("n")
        w = 1.0 if n is None else float(n) / (float(n) + FC_SHRINK_N0)
        shrunk = w * calibrated + (1.0 - w) * p
        return min(max(shrunk, _FC_Y_LO), _FC_Y_HI)
    except Exception as exc:
        logger.debug("final_calibration.apply passthrough (%s)", exc)
        return prob


__all__ = ["apply", "reload"]
