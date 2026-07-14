"""Baseball ensemble: factor + MC + GBM blend. Mirror of
engine.football._ensemble. Default weights tilt toward factor on
small samples (sample-efficient closed-form) then rebalance as the
GBM gains data."""
from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_WEIGHTS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "baseball"

_DEFAULT_WEIGHTS = {
    "ml":     {"factor": 0.5, "mc": 0.5, "gbm": 0.0},
    "spread": {"factor": 0.5, "mc": 0.5, "gbm": 0.0},
    "total":  {"factor": 0.5, "mc": 0.5, "gbm": 0.0},
}

_DEFAULT_WEIGHTS_WITH_GBM = {
    "ml":     {"factor": 0.4, "mc": 0.3, "gbm": 0.3},
    "spread": {"factor": 0.4, "mc": 0.3, "gbm": 0.3},
    "total":  {"factor": 0.4, "mc": 0.3, "gbm": 0.3},
}


def _load_weights(league: str, gbm_trained: bool) -> dict:
    path = _WEIGHTS_DIR / f"{league}_gbm" / "ensemble_weights.json"
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("[baseball:%s] weights load failed: %s — default",
                            league, e)
    return _DEFAULT_WEIGHTS_WITH_GBM if gbm_trained else _DEFAULT_WEIGHTS


def _blend_one(factor, mc, gbm, weights):
    legs = []
    if factor is not None:
        legs.append((factor, weights.get("factor", 0.0)))
    if mc is not None:
        legs.append((mc, weights.get("mc", 0.0)))
    if gbm is not None:
        legs.append((gbm, weights.get("gbm", 0.0)))
    if not legs:
        return None
    total_w = sum(w for _, w in legs)
    if total_w <= 0:
        return sum(p for p, _ in legs) / len(legs)
    return sum(p * w for p, w in legs) / total_w


def blend(league: str, pred: dict) -> None:
    gbm_trained = bool(pred.get("gbm_trained"))
    weights = _load_weights(league, gbm_trained)
    pred["_signal_factor"] = {
        "p_home":       pred.get("p_home"),
        "p_away":       pred.get("p_away"),
        "p_home_cover": pred.get("p_home_cover"),
        "p_away_cover": pred.get("p_away_cover"),
        "p_over":       pred.get("p_over"),
        "p_under":      pred.get("p_under"),
    }
    pred["_signal_mc"] = {
        "p_home":       pred.get("mc_p_home_decided") or pred.get("mc_p_home"),
        "p_away":       pred.get("mc_p_away"),
        "p_home_cover": pred.get("mc_p_home_cover"),
        "p_away_cover": pred.get("mc_p_away_cover"),
        "p_over":       pred.get("mc_p_over"),
        "p_under":      pred.get("mc_p_under"),
    }
    pred["_signal_gbm"] = {
        "p_home":       pred.get("gbm_p_home"),
        "p_away":       pred.get("gbm_p_away"),
        "p_home_cover": pred.get("gbm_p_home_cover"),
        "p_away_cover": pred.get("gbm_p_away_cover"),
        "p_over":       pred.get("gbm_p_over"),
        "p_under":      pred.get("gbm_p_under"),
    }
    pred["_ensemble_weights"] = weights
    pred["_signal_gbm_trained"] = gbm_trained

    w_ml = weights.get("ml", {})
    b = _blend_one(pred["_signal_factor"]["p_home"],
                    pred["_signal_mc"]["p_home"],
                    pred["_signal_gbm"]["p_home"], w_ml)
    if b is not None:
        pred["p_home"] = round(b, 4)
        pred["p_away"] = round(1.0 - b, 4)

    w_sp = weights.get("spread", {})
    b = _blend_one(pred["_signal_factor"]["p_home_cover"],
                    pred["_signal_mc"]["p_home_cover"],
                    pred["_signal_gbm"]["p_home_cover"], w_sp)
    if b is not None:
        pred["p_home_cover"] = round(b, 4)
        pred["p_away_cover"] = round(1.0 - b, 4)

    w_tot = weights.get("total", {})
    b = _blend_one(pred["_signal_factor"]["p_over"],
                    pred["_signal_mc"]["p_over"],
                    pred["_signal_gbm"]["p_over"], w_tot)
    if b is not None:
        pred["p_over"] = round(b, 4)
        pred["p_under"] = round(1.0 - b, 4)
