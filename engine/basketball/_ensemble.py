"""Ensemble blender for the basketball framework.

Combines three signals per game:
  - Factor model (pace × ratings × home boost × injuries × recent form)
  - GBM (XGBoost trained on point-in-time features)
  - MC sims (correlated joint distribution from factor expected scores)

Per-target weights live in
``data/basketball/<league>_gbm/ensemble_weights.json`` and are seeded
from holdout RMSE in B4 (the calibration step). Until then we use a
balanced default that's been validated empirically on NBA: factor 0.34,
GBM 0.33, MC 0.33.

Public:
    blend(league, factor_pred, features, *, spread=None, total=None) -> dict
"""
from __future__ import annotations

import json
import logging
import math
from pathlib import Path

from ._gbm import predict as gbm_predict
from ._mc import simulate, market_probs

logger = logging.getLogger(__name__)


_WEIGHTS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "basketball"


# Default weights when no fitted weights exist yet. Mirrors NBA's
# 0.34/0.33/0.33 split — balanced, no signal dominates.
# Keys match _blend_target() — `ml_home` not `home_win` so the lookup
# in each per-signal dict matches.
_DEFAULT_WEIGHTS = {
    "ml_home": {"factor": 0.34, "gbm": 0.33, "mc": 0.33},
    "margin":  {"factor": 0.34, "gbm": 0.33, "mc": 0.33},
    "total":   {"factor": 0.34, "gbm": 0.33, "mc": 0.33},
}


# One-time-per-league flag so the "still on defaults" warning fires
# once per process instead of every prediction.
_DEFAULT_WEIGHTS_WARNED: set[str] = set()


def _load_weights(league: str) -> dict:
    path = _WEIGHTS_DIR / f"{league}_gbm" / "ensemble_weights.json"
    if not path.exists():
        if league not in _DEFAULT_WEIGHTS_WARNED:
            logger.info(
                "[%s] no ensemble_weights.json — using NBA-tuned defaults "
                "(0.34/0.33/0.33 factor/gbm/mc). Weights will stay coarse "
                "until B4 walk-forward calibration fits per-league values.",
                league,
            )
            _DEFAULT_WEIGHTS_WARNED.add(league)
        return _DEFAULT_WEIGHTS
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("[%s] ensemble weights load failed: %s", league, e)
        return _DEFAULT_WEIGHTS


def _norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def blend(league: str, factor_pred: dict, features: dict,
           home_id: int, away_id: int,
           *, spread: float | None = None,
           total: float | None = None,
           n_sims: int = 10_000,
           seed: int | None = None) -> dict:
    """Blend factor + GBM + MC signals for one game.

    Args:
        league:        registry key
        factor_pred:   output of ``_predict.predict_full`` (factor-only)
        features:      output of ``_features.extract_features`` (for GBM)
        home_id, away_id: team ids (for MC's per-team variance lookup)
        spread, total: posted lines (used to derive cover/over probs)

    Returns:
        Dict with blended predictions + per-signal breakdown for
        transparency. Shape::

            {
              "ensemble": {
                "margin": float, "total": float,
                "ml_home": float,
                "spread_cover_prob": float|None, "over_prob": float|None,
              },
              "signals": {"factor": {...}, "gbm": {...}, "mc": {...}},
              "weights": {target: {factor, gbm, mc}},
            }
    """
    weights = _load_weights(league)

    # ── Factor signal ───────────────────────────────────────
    factor = {
        "margin": factor_pred.get("predicted_margin"),
        "total": factor_pred.get("predicted_total"),
        "ml_home": factor_pred.get("ml_home"),
    }

    # ── GBM signal ──────────────────────────────────────────
    gbm = {
        "margin": gbm_predict(league, "margin", features),
        "total": gbm_predict(league, "total_points", features),
        "ml_home": gbm_predict(league, "home_win", features),
    }

    # ── MC signal ───────────────────────────────────────────
    sims = simulate(
        league, home_id, away_id,
        home_expected=factor_pred.get("home_expected") or 0.0,
        away_expected=factor_pred.get("away_expected") or 0.0,
        n_sims=n_sims, seed=seed,
    )
    mc = {
        "margin": sims["mean_margin"],
        "total": sims["mean_total"],
        "ml_home": sims["p_home_win"],
    }
    mc_probs = market_probs(sims, spread=spread, total=total)

    # ── Blend ───────────────────────────────────────────────
    def _w(target):
        return weights.get(target, _DEFAULT_WEIGHTS[target])

    def _blend_target(name):
        w = _w(name)
        f = factor.get(name)
        g = gbm.get(name)
        m = mc.get(name)
        # Drop None signals + renormalize remaining weights
        active = [(k, v, w[k]) for k, v in (("factor", f), ("gbm", g), ("mc", m))
                   if v is not None]
        if not active:
            return None
        total_w = sum(wi for _, _, wi in active)
        if total_w <= 0:
            return None
        return sum(v * (wi / total_w) for _, v, wi in active)

    margin_blend = _blend_target("margin")
    total_blend = _blend_target("total")
    ml_home_blend = _blend_target("ml_home")

    # Spread cover prob — derive analytically from blended margin +
    # factor's margin sigma (MC already gave us a sample-derived value;
    # blend the two for the final).
    sigma_margin = (factor_pred.get("factors") or {}).get("constants", {}).get(
        "margin_std") or 16.0
    sigma_total = (factor_pred.get("factors") or {}).get("constants", {}).get(
        "total_std") or 21.0
    spread_cover = None
    if spread is not None and margin_blend is not None and sigma_margin > 0:
        z = (margin_blend - (-spread)) / sigma_margin
        analytical = _norm_cdf(z)
        mc_p = mc_probs.get("p_home_cover")
        spread_cover = (
            (analytical + mc_p) / 2 if mc_p is not None else analytical
        )
    over = None
    if total is not None and total_blend is not None and sigma_total > 0:
        z = (total_blend - total) / sigma_total
        analytical = _norm_cdf(z)
        mc_p = mc_probs.get("p_over")
        over = (analytical + mc_p) / 2 if mc_p is not None else analytical

    return {
        "ensemble": {
            "margin": margin_blend,
            "total": total_blend,
            "ml_home": ml_home_blend,
            "spread_cover_prob": spread_cover,
            "over_prob": over,
        },
        "signals": {"factor": factor, "gbm": gbm, "mc": mc},
        "weights": weights,
        "mc_probs": mc_probs,
    }
