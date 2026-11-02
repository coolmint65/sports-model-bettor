# -*- coding: utf-8 -*-
"""Derivative calibration layer — honest probabilities for derivative picks.

Diagnosed 2026-08-05: every derivative market's model_prob is overconfident
by 12-32pp (point sims assume independence; state MC overrates conviction).
Coefficients are fitted by fit_derivative_calibration.py (repo root) into
data/derivative_calibration.json, per (sport, tour, bet_type):

  mode 'calibrated' -> cal_p = sigmoid(a + b*logit(model_prob))
  mode 'no_signal'  -> within-market ranking has no predictive value;
                       cal_p = the market's base win rate (constant)
  unfitted          -> None (too little data; layer stays out of the way)

Currently wired in SHADOW MODE: bet_queue attaches {cal_p, cal_edge, mode}
to queue rows for logging/UI, changing nothing. Flipping it to gate real
money is a strategy decision (Austin's call).
"""
from __future__ import annotations
import json
import math
import os
import time
from pathlib import Path

_PATH = Path(__file__).resolve().parent.parent / "data" / "derivative_calibration.json"
_cache = {"ts": 0.0, "data": None}
_TTL_S = 300.0


def _load() -> dict:
    now = time.time()
    if _cache["data"] is not None and now - _cache["ts"] < _TTL_S:
        return _cache["data"]
    try:
        with open(_PATH) as f:
            _cache["data"] = json.load(f)
    except Exception:
        _cache["data"] = {"markets": {}}
    _cache["ts"] = now
    return _cache["data"]


def _logit(p: float) -> float:
    p = min(max(p, 1e-6), 1 - 1e-6)
    return math.log(p / (1 - p))


def _sigmoid(z: float) -> float:
    return 1.0 / (1.0 + math.exp(-z))


def _implied(odds) -> float | None:
    """American (or decimal) odds -> implied probability."""
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    if o >= 100 or o <= -100:
        d = 1 + (o / 100.0 if o > 0 else 100.0 / (-o))
    elif 1.0 < o < 30:
        d = o
    else:
        return None
    return 1.0 / d


def shadow(sport: str, tour: str | None, bet_type: str,
           model_prob, odds) -> dict | None:
    """Calibrated view of a pick, or None when unfitted.

    Returns {"cal_p": float, "cal_edge": float|None, "mode": str}.
    cal_edge = cal_p - implied(odds) (None when odds unusable).
    Never raises — this must not be able to break the queue.
    """
    try:
        p = float(model_prob)
        if not (0.0 < p < 1.0):
            return None
        key = "%s:%s:%s" % ((sport or "").lower(), (tour or "").lower(), bet_type or "")
        m = _load().get("markets", {}).get(key)
        if not m:
            return None
        if m.get("mode") == "no_signal":
            cal_p = float(m.get("base", p))
        else:
            cal_p = _sigmoid(float(m["a"]) + float(m["b"]) * _logit(p))
        imp = _implied(odds)
        return {"cal_p": round(cal_p, 4),
                "cal_edge": round(cal_p - imp, 4) if imp is not None else None,
                "mode": m.get("mode", "calibrated")}
    except Exception:
        return None
