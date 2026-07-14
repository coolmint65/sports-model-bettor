"""Isotonic regression calibration (A4).

Replacement for the bucket-mean Bayesian beta-binomial calibration in
``engine.empirical_calibration``. Three structural improvements:

  1. **Smooth monotonic mapping** — isotonic regression produces a
     monotonic raw→calibrated curve with no bucket-edge discontinuities
     (the 0.499 vs 0.501 jump that the bucket-mean approach has).
  2. **Rolling 90-day window** — refits continuously as picks settle,
     giving fast-changing markets (post-rule-change, post-roster-shift)
     a way to adapt without lagging months. Older data falls off
     naturally instead of being shrunk by a Bayesian prior.
  3. **Versioned shadow ship** — registers as a candidate via A2's
     ``model_versions`` registry. Live calibration stays bucket-mean
     until the candidate beats it on Brier/ROI over enough samples.

Reads decisions + settles from the events log (A1). No legacy table
dependency; the projection is the source of truth.

Public API:
  fit(sport, *, bet_type=None, window_days=90,
      min_samples=200, sport=None) -> dict
      Fits one isotonic curve per (sport, bet_type) cell. Returns the
      per-cell knots (sorted x + y arrays) the calibrator interpolates
      against at decision time.

  calibrate(sport, bet_type, raw_prob,
            curves=None) -> float
      Apply a fitted curve to a raw prob. Falls through to raw_prob
      when no curve fitted yet (cold-start safe).

  register_as_candidate(...) -> int
      Register the freshly-fit curves as an A2 candidate version
      under component='empirical_calibration_<sport>'. Picks code can
      shadow-evaluate against this version.
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Iterable

from . import events as _events

logger = logging.getLogger(__name__)


# ── Isotonic regression (PAV — Pool Adjacent Violators) ──────

def _pav(xs: list[float], ys: list[float]) -> tuple[list[float], list[float]]:
    """Pool-Adjacent-Violators isotonic regression.

    Standard non-parametric monotonic fit. Given (x, y) pairs sorted by
    x, returns (x, y_iso) where y_iso is the unique non-decreasing
    sequence minimizing sum of squared errors. Operates in O(n) — pure
    Python, no scikit-learn dependency.

    Returns the same xs back + the smoothed ys.
    """
    if not xs:
        return [], []
    n = len(xs)
    # Each block tracks (sum, count) of the values pooled into it.
    # When we add a new value and the running monotonic sequence violates,
    # we merge backward until the constraint holds.
    sums = list(ys)
    counts = [1] * n
    means = list(ys)
    # Stack of indices; for each new index, merge backward while the
    # previous block's mean exceeds the new one's.
    stack: list[int] = []
    for i in range(n):
        stack.append(i)
        while len(stack) >= 2:
            j = stack[-2]
            k = stack[-1]
            if means[j] <= means[k]:
                break
            # Merge k into j
            sums[j] += sums[k]
            counts[j] += counts[k]
            means[j] = sums[j] / counts[j]
            stack.pop()
    # Walk the stack and fill block-equal means out
    smoothed = [0.0] * n
    cursor = 0
    for idx_in_stack, block_start in enumerate(stack):
        block_end = (stack[idx_in_stack + 1]
                     if idx_in_stack + 1 < len(stack) else n)
        for i in range(block_start, block_end):
            smoothed[i] = means[block_start]
    return list(xs), smoothed


def _interpolate(curve: dict, raw_prob: float) -> float:
    """Linear-interpolate ``raw_prob`` against the curve's knots.

    Outside the fitted range, clamp to the nearest knot."""
    xs = curve["xs"]
    ys = curve["ys"]
    if not xs:
        return raw_prob
    if raw_prob <= xs[0]:
        return ys[0]
    if raw_prob >= xs[-1]:
        return ys[-1]
    # Binary search would be O(log n) but n ≤ 1000 typically; linear is
    # fine and avoids importing bisect.
    for i in range(len(xs) - 1):
        x0, x1 = xs[i], xs[i + 1]
        if x0 <= raw_prob <= x1:
            if x1 == x0:
                return ys[i]
            t = (raw_prob - x0) / (x1 - x0)
            return ys[i] + t * (ys[i + 1] - ys[i])
    return raw_prob


# ── Fitting from event log ───────────────────────────────────

def fit(
    *,
    sport: str | None = None,
    window_days: int = 90,
    min_samples: int = 30,
) -> dict:
    """Walk decision+settle pairs in the recent window, group by
    (sport, bet_type[+direction]), fit one isotonic curve per cell.

    Returns ``{sport: {key: {xs, ys, n, fit_at}}}`` where key is
    ``bet_type`` or ``bet_type|direction``."""
    cutoff = (datetime.now(timezone.utc)
                - timedelta(days=window_days)).isoformat()
    pairs = _pull_pairs(sport=sport, since=cutoff)
    # Group by (sport, key)
    by_cell: dict[tuple[str, str], list[tuple[float, int]]] = defaultdict(list)
    for p in pairs:
        key = p["bet_type"]
        if p.get("direction"):
            key = f"{key}|{p['direction']}"
        by_cell[(p["sport"], key)].append((p["prob"], p["outcome"]))

    out: dict[str, dict[str, dict]] = {}
    fit_at = datetime.now(timezone.utc).isoformat()
    for (sp, key), samples in by_cell.items():
        if len(samples) < min_samples:
            continue
        # Sort by predicted prob; PAV needs sorted x.
        samples.sort(key=lambda t: t[0])
        xs = [t[0] for t in samples]
        ys = [float(t[1]) for t in samples]
        xs_iso, ys_iso = _pav(xs, ys)
        # De-duplicate: collapse runs of equal x to a single knot whose
        # y is the run's mean. Keeps curves compact + fast to interpolate.
        knots_x: list[float] = []
        knots_y: list[float] = []
        i = 0
        while i < len(xs_iso):
            j = i
            while j + 1 < len(xs_iso) and xs_iso[j + 1] == xs_iso[i]:
                j += 1
            knots_x.append(xs_iso[i])
            avg_y = sum(ys_iso[i:j + 1]) / (j - i + 1)
            knots_y.append(avg_y)
            i = j + 1
        out.setdefault(sp, {})[key] = {
            "xs": knots_x,
            "ys": knots_y,
            "n": len(samples),
            "fit_at": fit_at,
            "window_days": window_days,
        }
    return out


def _pull_pairs(*, sport: str | None = None,
                  since: str) -> list[dict]:
    """Extract (prob, outcome) pairs from the event log within the
    rolling window. Mirrors drift_detector._settled_pairs but exposed
    via this module's contract."""
    conn = _events._get_conn()
    where = ["event_type = 'decision'", "ts >= ?", "pick_id IS NOT NULL"]
    params: list = [since]
    if sport:
        where.append("sport = ?")
        params.append(sport)
    decisions = conn.execute(
        f"SELECT id, sport, ts, pick_id, bet_type, pick_text, payload "
        f"FROM events WHERE {' AND '.join(where)} "
        f"ORDER BY ts ASC, id ASC",
        params,
    ).fetchall()
    settle_where = "WHERE event_type = 'settle' AND ts >= ?"
    settle_params: list = [since]
    if sport:
        settle_where += " AND sport = ?"
        settle_params.append(sport)
    settles = conn.execute(
        f"SELECT pick_id, sport, payload FROM events {settle_where}",
        settle_params,
    ).fetchall()
    settle_by = {(s["sport"], int(s["pick_id"])): s["payload"]
                  for s in settles if s["pick_id"] is not None}

    out = []
    for d in decisions:
        try:
            payload = json.loads(d["payload"]) if d["payload"] else {}
        except json.JSONDecodeError:
            continue
        if not payload.get("accepted"):
            continue
        s_raw = settle_by.get((d["sport"], int(d["pick_id"])))
        if not s_raw:
            continue
        try:
            s = json.loads(s_raw)
        except json.JSONDecodeError:
            continue
        result = s.get("result")
        if result not in ("W", "L"):
            continue
        prob = payload.get("calibrated_prob") or payload.get("raw_prob")
        if prob is None:
            continue
        bt = (d["bet_type"] or "").upper()
        from .events_materialize import _direction_label
        direction = _direction_label(bt, d["pick_text"] or "")
        out.append({
            "sport": d["sport"], "ts": d["ts"],
            "bet_type": bt, "direction": direction,
            "prob": float(prob), "outcome": 1 if result == "W" else 0,
        })
    return out


# ── Calibration application ──────────────────────────────────

def calibrate(sport: str, bet_type: str, raw_prob: float,
                *, direction: str | None = None,
                curves: dict | None = None) -> float:
    """Apply the fitted isotonic curve. Falls through to raw_prob when
    no curve exists for the (sport, bet_type[+direction]) cell.

    ``curves`` is the dict returned by ``fit()``. Caller passes it to
    avoid re-fitting on every prediction; production wraps fit() once
    per refit cycle and reuses the result."""
    if not curves:
        return raw_prob
    sport_curves = curves.get(sport) or {}
    key = bet_type if not direction else f"{bet_type}|{direction}"
    curve = sport_curves.get(key) or sport_curves.get(bet_type)
    if not curve:
        return raw_prob
    return _interpolate(curve, raw_prob)


# ── A2 integration: register as candidate ────────────────────

def register_as_candidate(
    sport: str,
    *,
    window_days: int = 90,
    min_samples: int = 30,
    notes: str | None = None,
) -> dict:
    """Fit + register a fresh isotonic candidate for the sport. Doesn't
    promote — just registers under the A2 model_versions table so picks
    code can shadow-evaluate. Promotion happens via
    ``model_versions.promote_if_better`` after enough samples."""
    from . import model_versions
    curves = fit(sport=sport, window_days=window_days,
                  min_samples=min_samples)
    sport_curves = curves.get(sport, {})
    component = f"empirical_calibration_{sport}"
    version_id = model_versions.register_version(
        component=component,
        config={
            "method": "isotonic_regression",
            "window_days": window_days,
            "min_samples": min_samples,
            "fit_at": datetime.now(timezone.utc).isoformat(),
            "curves": sport_curves,
            "n_cells_fit": len(sport_curves),
        },
        status="candidate",
        notes=notes or f"Isotonic candidate fit on {window_days}d window",
    )
    return {
        "version_id": version_id,
        "component": component,
        "n_cells_fit": len(sport_curves),
        "cells": sorted(sport_curves.keys()),
    }


# ── CLI ──────────────────────────────────────────────────────

def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.isotonic_calibration")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_fit = sub.add_parser("fit")
    p_fit.add_argument("--sport", default=None)
    p_fit.add_argument("--window-days", type=int, default=90)
    p_fit.add_argument("--min-samples", type=int, default=30)
    p_reg = sub.add_parser("register-candidate")
    p_reg.add_argument("sport")
    p_reg.add_argument("--window-days", type=int, default=90)
    p_reg.add_argument("--min-samples", type=int, default=30)
    p_reg.add_argument("--notes", default=None)
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    if args.cmd == "fit":
        out = fit(sport=args.sport, window_days=args.window_days,
                   min_samples=args.min_samples)
        # Compact summary
        for sp, cells in out.items():
            print(f"== {sp} ==  {len(cells)} cells fit")
            for key, c in cells.items():
                print(f"  {key:18s} n={c['n']:>4d}  knots={len(c['xs'])}")
    elif args.cmd == "register-candidate":
        out = register_as_candidate(
            args.sport,
            window_days=args.window_days,
            min_samples=args.min_samples,
            notes=args.notes,
        )
        print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
