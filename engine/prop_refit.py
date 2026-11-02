"""Weekly market-blend refit for player props, shared by NFL and NHL (MintyBot 2026-10-26,
Austin "Yes to weekly refit" / "Go for it" for NHL).

The prop pickers anchor the model's probability to the de-vigged Hard Rock line with a
weight w (the model's share; 0.35 = the NHL props rebuild's guess). Each sport logs EVERY
main line it prices -- raw model prob with the price at three moments: first seen, PICK
TIME (game day, when the picker locks its card) and the last pre-game price (~the close)
-- and grades those lines from its box scores. fit() picks, per stat, the w minimising
log-loss of

    p = w * p_model + (1 - w) * p_market      (both at pick time; first seen if missing)

against the outcomes, shrinks it toward DEFAULT_W by N0 pseudo-lines and clamps it to
[0, W_MAX]; a stat keeps DEFAULT_W until it has MIN_N graded lines. Fitting against the
pick-time price, not the close, is deliberate: the picker blends with the price it can
actually bet, and by the close the market has already absorbed much of what the model
knows (on synthetic data an informative model looked worthless against the close).
w -> 0 means the model adds nothing over Hard Rock (its picker then finds no edge and goes
quiet). Each fit also records the closing-line signal: when the model disagreed with the
pick-time price by >= CLV_DISAGREE, how far (pp) the line moved toward it by the close
(same line only) -- the beat-the-close read, and far less noisy than outcomes.

Sport wrappers: engine.sports.nfl.prop_refit, engine.sports.nhl.prop_eval,
engine.sports.nba.prop_eval. A sport may pass its own per-stat starting weights
(``default_w``) -- NBA starts each stat from its DraftKings backtest instead of 0.35.
"""
from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_W = 0.35
MIN_N = 150          # graded lines per stat before its weight moves off the default
N0 = 300.0           # shrinkage toward DEFAULT_W, in pseudo-lines (outcome fits are noisy)
W_MAX = 0.60         # the model never gets more than 60% say from a fit
REFIT_DAYS = 7
CLV_DISAGREE = 0.10  # model vs opening de-vig gap that counts as a disagreement
_GRID = [i / 100.0 for i in range(0, 101, 5)]
_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"
_CACHE: dict = {}    # str(path) -> (mtime, {bet_type: w})


def _imp(o):
    if o is None:
        return None
    o = float(o)
    if o == 0:
        return None
    return 100.0 / (o + 100.0) if o > 0 else -o / (-o + 100.0)


def devig(over, under):
    """De-vigged P(over) from a two-way price, or None."""
    a, b = _imp(over), _imp(under)
    if a is None or b is None or a + b <= 0:
        return None
    return a / (a + b)


def _ll(p: float, y: float) -> float:
    p = min(max(p, 1e-4), 1 - 1e-4)
    return -(y * math.log(p) + (1 - y) * math.log(1 - p))


def _w0(default_w, bet_type) -> float:
    if isinstance(default_w, dict):
        return float(default_w.get(bet_type, DEFAULT_W))
    return DEFAULT_W if default_w is None else float(default_w)


def fit(rows, markets, default_w=None) -> dict:
    """``rows``: graded eval-log rows (mappings with bet_type, ref_line, ref_over, ref_under,
    ref_p_over -- the pick-time snapshot, else first seen -- last_line, last_over,
    last_under, actual). ``default_w``: the starting / shrink-target weight, a number or a
    {bet_type: w} map (None = DEFAULT_W). Returns the weights document (per-market n, w,
    status + diagnostics)."""
    out = {"fit_ts": datetime.now(timezone.utc).strftime(_TS_FMT),
           "default_w": default_w if default_w is not None else DEFAULT_W,
           "min_n": MIN_N, "n0": N0, "w_max": W_MAX, "markets": {}}
    for bt in markets:
        w0 = _w0(default_w, bt)
        data, clv = [], []
        for r in rows:
            if r["bet_type"] != bt or r["ref_p_over"] is None or r["ref_line"] is None:
                continue
            pm = devig(r["ref_over"], r["ref_under"])
            if pm is None or r["actual"] is None or r["actual"] == r["ref_line"]:
                continue                                    # unpriced / ungraded / push
            y = 1.0 if r["actual"] > r["ref_line"] else 0.0
            data.append((float(r["ref_p_over"]), pm, y))
            pm_close = devig(r["last_over"], r["last_under"])
            if pm_close is not None and r["last_line"] == r["ref_line"]:
                gap = float(r["ref_p_over"]) - pm
                if abs(gap) >= CLV_DISAGREE:
                    clv.append((pm_close - pm) * (1.0 if gap > 0 else -1.0))
        n = len(data)
        e = {"n": n, "w": w0, "status": "default"}
        if n:
            ll_model = sum(_ll(p, y) for p, _, y in data) / n
            ll_mkt = sum(_ll(m, y) for _, m, y in data) / n
            ll_fit, w_fit = min((sum(_ll(w * p + (1 - w) * m, y) for p, m, y in data) / n, w)
                                for w in _GRID)
            e.update(hit_rate=round(sum(y for _, _, y in data) / n, 4),
                     mean_p_model=round(sum(p for p, _, _ in data) / n, 4),
                     mean_p_market=round(sum(m for _, m, _ in data) / n, 4),
                     logloss_model=round(ll_model, 4), logloss_market=round(ll_mkt, 4),
                     logloss_fit=round(ll_fit, 4), w_fit=w_fit)
            if n >= MIN_N:
                w = (n * w_fit + N0 * w0) / (n + N0)
                e.update(w=round(min(W_MAX, max(0.0, w)), 3), status="fitted")
        if clv:
            e.update(clv_n=len(clv), clv_move_pp=round(100.0 * sum(clv) / len(clv), 2))
        out["markets"][bt] = e
    return out


def write(path: Path, doc: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(doc, indent=1))
    os.replace(tmp, path)


def due(path: Path) -> bool:
    """True when the weights file is missing/unreadable or REFIT_DAYS old."""
    try:
        ts = datetime.strptime(json.loads(path.read_text())["fit_ts"], _TS_FMT)
    except Exception:
        return True
    age = datetime.now(timezone.utc).replace(tzinfo=None) - ts
    return age.total_seconds() >= REFIT_DAYS * 86400


def weight_for(path: Path, bet_type: str, default: float | None = None) -> float:
    """Fitted model weight for ``bet_type`` from ``path`` (``default``, else DEFAULT_W, when
    absent); mtime-cached."""
    d0 = DEFAULT_W if default is None else float(default)
    try:
        m = path.stat().st_mtime
    except OSError:
        return d0
    hit = _CACHE.get(str(path))
    if hit is None or hit[0] != m:
        try:
            d = json.loads(path.read_text())
            hit = (m, {bt: float(e.get("w", d0)) for bt, e in (d.get("markets") or {}).items()})
        except Exception:
            return d0
        _CACHE[str(path)] = hit
    return hit[1].get(bet_type, d0)


__all__ = ["DEFAULT_W", "MIN_N", "REFIT_DAYS", "devig", "fit", "write", "due", "weight_for"]
