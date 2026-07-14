"""Football ensemble: factor + MC + GBM → final probability.

Mirrors ``engine.basketball._ensemble``. Weights load from per-league
JSON (``data/football/{league}_gbm/ensemble_weights.json``) and fall
back to a sport-wide default that excludes the GBM leg when no model
is trained yet.

The blender's job is small but load-bearing: take the (factor, mc,
gbm) probability triplet per market and produce one number the picker
uses to compute edges. When the GBM leg is None (gated by training-
data volume), the remaining factor+MC weights are renormalized to
sum to 1.0 so the blended prob doesn't drift toward 0.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_WEIGHTS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "football"

# Sport-wide defaults. Used when no per-league fit has been promoted.
# Factor carries the majority weight on small samples because the
# closed-form Elo+Normal is the most sample-efficient. Once UFL has
# enough games to train a GBM, _ensemble_fit will rewrite these
# per-league based on walk-forward MSE.
_DEFAULT_WEIGHTS = {
    "ml":     {"factor": 0.5, "mc": 0.5, "gbm": 0.0},
    "spread": {"factor": 0.5, "mc": 0.5, "gbm": 0.0},
    "total":  {"factor": 0.5, "mc": 0.5, "gbm": 0.0},
}

# Once a GBM is trained for the league, swap to a 0.4/0.3/0.3 split so
# the GBM gets a meaningful voice without dominating a small sample.
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
            logger.warning("[football:%s] weights load failed: %s — using default",
                            league, e)
    return _DEFAULT_WEIGHTS_WITH_GBM if gbm_trained else _DEFAULT_WEIGHTS


def _blend_one(factor: float | None, mc: float | None,
                gbm: float | None, weights: dict) -> float | None:
    """Weighted average of available legs. Drops any None leg and
    renormalizes the remaining weights."""
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
        # Degenerate weights — return the simple average so callers
        # don't get a nonsense divide.
        return sum(p for p, _ in legs) / len(legs)
    return sum(p * w for p, w in legs) / total_w


def blend(league: str, pred: dict) -> None:
    """Mutate ``pred`` in place — replace the factor-only probability
    keys with ensemble-blended ones, and stash the per-leg sub-probs
    under ``_signal_*`` so V3.2 explain endpoints can introspect.
    """
    gbm_trained = bool(pred.get("gbm_trained"))
    weights = _load_weights(league, gbm_trained)

    # Snapshot factor-only legs before we overwrite them.
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

    # Blend per market — overwrite the factor-only keys so the picker
    # (which reads p_home / p_home_cover / p_over) gets the ensemble
    # value without code change.
    w_ml = weights.get("ml", {})
    blended_home = _blend_one(
        pred["_signal_factor"]["p_home"],
        pred["_signal_mc"]["p_home"],
        pred["_signal_gbm"]["p_home"],
        w_ml,
    )
    if blended_home is not None:
        pred["p_home"] = round(blended_home, 4)
        pred["p_away"] = round(1.0 - blended_home, 4)

    w_sp = weights.get("spread", {})
    blended_cover = _blend_one(
        pred["_signal_factor"]["p_home_cover"],
        pred["_signal_mc"]["p_home_cover"],
        pred["_signal_gbm"]["p_home_cover"],
        w_sp,
    )
    if blended_cover is not None:
        pred["p_home_cover"] = round(blended_cover, 4)
        pred["p_away_cover"] = round(1.0 - blended_cover, 4)

    w_tot = weights.get("total", {})
    blended_over = _blend_one(
        pred["_signal_factor"]["p_over"],
        pred["_signal_mc"]["p_over"],
        pred["_signal_gbm"]["p_over"],
        w_tot,
    )
    if blended_over is not None:
        pred["p_over"] = round(blended_over, 4)
        pred["p_under"] = round(1.0 - blended_over, 4)
