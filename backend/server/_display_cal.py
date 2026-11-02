"""Display-time win-probability calibration (2026-09-16, MintyBot).

The dashboard's win-probability BARS were showing the model's RAW,
overconfident probabilities. The pick DECISION is already run through the
post-hoc per-league calibrator inside ``engine.picks_core.score_pick``
(the ``final_calibration`` stage), but the game/prediction-level win
probabilities that feed the frontend bars are RAW model output that never
passed through that stage:

    win_prob{home, away}          nba / nhl / mlb best-bets
    prediction.ml_home/ml_away    basketball / hockey /today
    prediction.p_home/p_away      football / baseball /today
    prediction.p1/p2_win_prob     tennis predict / preview / scheduled
    prediction.p_home/draw/away   soccer 1X2 /today (3-way)
    spread_cover_prob / over_prob  basketball secondary market bars

This module applies the SAME calibrator (``engine.final_calibration.apply``)
to those DISPLAY values, in ONE place, so every read route stays
consistent. It never touches the pick objects (their ``prob`` is already
calibrated by score_pick) — calibrating those again would double-count.

Contract
--------
* Scale-preserving: an input > 1.0 is treated as a percentage (0-100) and
  returned on the 0-100 scale; an input in [0, 1] is returned as a
  fraction. (Mirrors ``final_calibration._norm``.) Rounded to 4 dp.
* DEFENSIVE: every public function wraps the calibrator in try/except and
  returns the ORIGINAL value unchanged on ANY error. Never raises — a read
  route must never 500 because of display calibration.
* The ``engine.final_calibration`` import is LAZY (inside the call) so
  importing this module can never affect app startup or route import.
* Keys match ``final_calibration.apply``: ``'nba'``, ``'wnba'``, ``'mlb'``,
  ``'nhl'``, ``'tennis'``, ``'football_nfl'``/``'football_cfb'``, and the
  bare league string otherwise. ``_global`` is the built-in fallback;
  unknown keys pass through unchanged (handled inside ``apply``).

Public API
----------
    cal_prob(key, p)                 -> calibrated scalar (scale kept)
    cal_winprob(key, {home, away})   -> new 2-way dict, second = complement
    cal_pair(key, obj, a, b)         -> in-place 2-way on sibling fields
    cal_multiway(key, obj, fields)   -> in-place n-way, renormalized
"""
from __future__ import annotations

from typing import Any, Iterable


import json as _json
import os as _os
import threading as _threading

_MAPS = None
_MAPS_MTIME = None
_MAPS_LOCK = _threading.Lock()


def _maps_path() -> str:
    here = _os.path.dirname(_os.path.abspath(__file__))
    return _os.path.join(here, "..", "..", "data", "display_calibration.json")


def _load_maps() -> dict:
    """Load + cache the prediction-log display-calibration maps from
    data/display_calibration.json. Keys accepted in both 'sport/league' and
    'sport_league' forms. Reloads automatically when the file's mtime changes
    so a nightly refit is picked up live (no backend restart). Returns {}
    (universal passthrough) if the file is missing/unreadable -- display
    calibration must never break a route."""
    global _MAPS, _MAPS_MTIME
    path = _maps_path()
    try:
        mt = _os.path.getmtime(path)
    except Exception:
        mt = None
    if _MAPS is not None and mt == _MAPS_MTIME:
        return _MAPS
    with _MAPS_LOCK:
        if _MAPS is not None and mt == _MAPS_MTIME:
            return _MAPS
        m = {}
        try:
            with open(path, encoding="utf-8") as fh:
                raw = _json.load(fh)
            for k, v in raw.items():
                if isinstance(v, dict) and v.get("grid") and v.get("mapped"):
                    m[k] = v
                    m[k.replace("/", "_")] = v
        except Exception:
            m = {}
        _MAPS = m
        _MAPS_MTIME = mt
        return _MAPS


def _interp(grid, mapped, p):
    """Piecewise-linear lookup of p (0-1) on the fitted isotonic knots."""
    if p <= grid[0]:
        return mapped[0]
    if p >= grid[-1]:
        return mapped[-1]
    for k in range(1, len(grid)):
        if p <= grid[k]:
            t = (p - grid[k - 1]) / (grid[k] - grid[k - 1])
            return mapped[k - 1] + t * (mapped[k] - mapped[k - 1])
    return mapped[-1]


def _apply(key: str, value: float) -> float:
    """Map a RAW display win-prob through the fitted prediction-log
    calibrator for ``key``; return a 0-1 fraction. Passthrough (raw) for any
    key without a fitted map, so NFL/UFL and every un-fitted market stay
    unchanged. A NO-FLIP CLAMP keeps the result on the same side of 0.5 as
    the raw value, so a favorite can never render as a dog (isotonic is
    already monotonic; the clamp guards a noisy tail knot)."""
    v = float(value)
    frac = v / 100.0 if v > 1.0 else v
    entry = _load_maps().get(key)
    if not entry:
        return frac
    c = _interp(entry["grid"], entry["mapped"], frac)
    if frac >= 0.5 and c < 0.5:
        c = 0.5
    elif frac < 0.5 and c > 0.5:
        c = 0.5
    return float(c)


def _num(x: Any):
    """Return float(x), or None if x is not a finite real number."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if v != v or v in (float("inf"), float("-inf")):  # NaN / inf guard
        return None
    return v


def _cal_two(key: str, obj: dict, a_field: str, b_field: str) -> None:
    """Core 2-way, in place: obj[a_field] -> calibrated, obj[b_field] ->
    its complement on the same scale (only if b_field is currently a
    number). No-op if a_field is missing / non-numeric."""
    va = _num(obj.get(a_field))
    if va is None:
        return
    is_pct = va > 1.0
    ca = _apply(key, va)  # 0-1
    if is_pct:
        obj[a_field] = round(ca * 100.0, 4)
        if _num(obj.get(b_field)) is not None:
            obj[b_field] = round(100.0 - ca * 100.0, 4)
    else:
        obj[a_field] = round(ca, 4)
        if _num(obj.get(b_field)) is not None:
            obj[b_field] = round(1.0 - ca, 4)


def cal_prob(key: str, p: Any) -> Any:
    """Calibrate a single display probability, preserving its 0-1 / 0-100
    scale. Returns ``p`` unchanged on any error / non-numeric input."""
    try:
        v = _num(p)
        if v is None:
            return p
        cal = _apply(key, v)  # 0-1
        return round(cal * 100.0, 4) if v > 1.0 else round(cal, 4)
    except Exception:
        return p


def cal_winprob(key: str, wp: Any) -> Any:
    """Calibrate a 2-way win-probability mapping and return a NEW dict.
    Recognises {home, away} or {p1, p2}: the first side is calibrated and
    the second becomes its complement on the same scale, so the pair stays
    consistent. Other keys are preserved. Returns ``wp`` unchanged on any
    error, if it is not a dict, or if the recognised pair is absent."""
    try:
        if not isinstance(wp, dict):
            return wp
        if "home" in wp and "away" in wp:
            a, b = "home", "away"
        elif "p1" in wp and "p2" in wp:
            a, b = "p1", "p2"
        else:
            return wp
        out = dict(wp)
        _cal_two(key, out, a, b)
        return out
    except Exception:
        return wp


def cal_pair(key: str, obj: Any, a_field: str, b_field: str) -> Any:
    """In-place 2-way calibration of two sibling scalar fields on ``obj``
    (a dict): calibrate ``obj[a_field]``, set ``obj[b_field]`` to its
    complement on the same scale. Returns ``obj`` (mutated). No-op on any
    error, if ``obj`` is not a dict, or if ``a_field`` is missing /
    non-numeric (so it is safe to call for market pairs that may be
    absent, e.g. cover/over when no line was priced)."""
    try:
        if isinstance(obj, dict):
            _cal_two(key, obj, a_field, b_field)
    except Exception:
        pass
    return obj


def cal_multiway(key: str, obj: Any, fields: Iterable[str]) -> Any:
    """In-place n-way calibration for a mutually-exclusive outcome set
    (e.g. soccer 1X2 home/draw/away). Each field is calibrated
    independently, then the group is renormalized to preserve its original
    sum-scale (1.0 for fractions, 100.0 for percentages) so the bars stay
    a valid distribution. Returns ``obj`` (mutated). No-op on any error or
    if fewer than two usable numeric fields are present."""
    try:
        if not isinstance(obj, dict):
            return obj
        fs = [f for f in fields if _num(obj.get(f)) is not None]
        if len(fs) < 2:
            return obj
        vals = [_num(obj.get(f)) for f in fs]
        unit = 100.0 if max(vals) > 1.0 else 1.0
        cal = [_apply(key, v) for v in vals]  # apply normalizes; each 0-1
        s = sum(cal)
        if s <= 0:
            return obj  # degenerate — leave the originals untouched
        for f, c in zip(fs, cal):
            obj[f] = round(c / s * unit, 4)
        return obj
    except Exception:
        return obj


__all__ = ["cal_prob", "cal_winprob", "cal_pair", "cal_multiway"]


# ── Market-blended card percentages (MintyBot 2026-10-26) ─────────────────────
# Austin: "Any percentages for any card in any sport, should be that way" -- the card shows what the Pick
# Authority bets with, W * model + (1 - W) * de-vigged market price, W = edge_proven.market_weight for that
# market (0.50 once the cell has PROVEN edge, 0.15 until then). Our models are mostly less accurate than the
# line (CFB 2026 leak-free Brier: market .180 vs model ~.21), so the raw model number alone overstated
# disagreements; it stays on the card as the 'model' detail. Model-only when there is no usable price.

def _pa_weight(sport: str, league: str, bet_type: str, odds) -> float:
    try:
        from engine.selection.edge_proven import market_weight
        return float(market_weight(sport, league, bet_type, "", odds))
    except Exception:
        return 0.15


def market_card_wp(sport: str, league: str, p_home: Any, home_odds: Any, away_odds: Any,
                   bet_type: str = "ML", w_unproven: float | None = None) -> dict | None:
    """2-way card win probability {home, away, model_home, model_away, market_home, w, source}."""
    try:
        v = _num(p_home)
        if v is None:
            return None
        p = v / 100.0 if v > 1.0 else v
        model = {"model_home": round(p, 4), "model_away": round(1.0 - p, 4)}
        from engine.market_blend import devig_prob
        dv = devig_prob(home_odds, away_odds)
        if dv is None:
            return {"home": round(p, 4), "away": round(1.0 - p, 4), **model, "source": "model"}
        w = _pa_weight(sport, league, bet_type, home_odds if p >= 0.5 else away_odds)
        if w_unproven is not None and w < 0.5:   # caller's segment weight; a PROVEN cell keeps its 0.50
            w = float(w_unproven)
        b = w * p + (1.0 - w) * dv
        return {"home": round(b, 4), "away": round(1.0 - b, 4), **model,
                "market_home": round(dv, 4), "w": round(w, 3), "source": "blend"}
    except Exception:
        return None


def market_card_wp3(sport: str, league: str, p_home: Any, p_draw: Any, p_away: Any,
                    home_odds: Any, draw_odds: Any, away_odds: Any, bet_type: str = "1X2") -> dict | None:
    """3-way (soccer 1X2) version: {home, draw, away, model_*, market_*, w, source}."""
    try:
        ps = [_num(p_home), _num(p_draw), _num(p_away)]
        if any(x is None for x in ps):
            return None
        ps = [x / 100.0 if x > 1.0 else x for x in ps]
        s = sum(ps) or 1.0
        ps = [x / s for x in ps]
        model = {"model_home": round(ps[0], 4), "model_draw": round(ps[1], 4), "model_away": round(ps[2], 4)}
        from engine.market_blend import devig_multiway
        dv = devig_multiway([home_odds, draw_odds, away_odds])
        if not dv:
            return {"home": model["model_home"], "draw": model["model_draw"], "away": model["model_away"],
                    **model, "source": "model"}
        fav = max(range(3), key=lambda i: ps[i])
        w = _pa_weight(sport, league, bet_type, [home_odds, draw_odds, away_odds][fav])
        b = [w * ps[i] + (1.0 - w) * dv[i] for i in range(3)]
        t = sum(b) or 1.0
        b = [x / t for x in b]
        return {"home": round(b[0], 4), "draw": round(b[1], 4), "away": round(b[2], 4), **model,
                "market_home": round(dv[0], 4), "market_draw": round(dv[1], 4), "market_away": round(dv[2], 4),
                "w": round(w, 3), "source": "blend"}
    except Exception:
        return None


def blend_card_wp(sport: str, league: str, wp: Any, home_odds: Any, away_odds: Any,
                  bet_type: str = "ML") -> Any:
    """{home, away, ...} -> the market-blended card dict (other keys kept). Returns ``wp`` unchanged when
    it can't blend (no model number) -- a read route must never break on display math."""
    try:
        if not isinstance(wp, dict) or wp.get("home") is None:
            return wp
        out = market_card_wp(sport, league, wp["home"], home_odds, away_odds, bet_type)
        if out is None:
            return wp
        extra = {k: v for k, v in wp.items() if k not in ("home", "away")}
        return {**extra, **out}
    except Exception:
        return wp
