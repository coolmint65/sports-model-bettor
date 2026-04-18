"""
Ensemble of factor model + Monte Carlo + GBM.

Three independent prediction signals are available at prediction time:
  1. Factor model (engine.mlb_predict / engine.nhl_predict / etc.)
  2. Monte Carlo (engine.mc_mlb / mc_nhl / mc_nba)
  3. GBM (engine.gbm.predict)

Each signal is wrong in different ways. A weighted blend averages out
uncorrelated errors and usually outperforms any single model. The
blend weights per market are stored in model_overrides so the ops
layer can retune them from backtest data without a code change.

Default weights are 0.34 / 0.33 / 0.33 for markets where all three
signals exist, or reduced to pairwise/single-source when one or more
is absent. No attempt to learn weights inside this module -- that's
the tuning script's job. Here we apply whatever weights exist.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


# Default weights. Keyed by (sport, market). Ops can override via
# ensemble_weights in the model_overrides table; key format
# "ENSEMBLE_<sport>_<market>" -> JSON dict of component weights.
_DEFAULT_WEIGHTS = {
    # MLB markets
    ("mlb", "home_win"):    {"factor": 0.34, "mc": 0.33, "gbm": 0.33},
    ("mlb", "total"):       {"factor": 0.34, "mc": 0.33, "gbm": 0.33},
    ("mlb", "nrfi"):        {"factor": 0.10, "mc": 0.55, "gbm": 0.35},
    ("mlb", "f5_home_win"): {"factor": 0.10, "mc": 0.55, "gbm": 0.35},
    ("mlb", "f5_total"):    {"factor": 0.10, "mc": 0.55, "gbm": 0.35},
    # NHL: three-way blend once GBM artifacts ship. When GBM is missing,
    # blend() redistributes its weight across the remaining signals.
    ("nhl", "home_win"):       {"factor": 0.40, "mc": 0.30, "gbm": 0.30},
    ("nhl", "total"):          {"factor": 0.40, "mc": 0.30, "gbm": 0.30},
    ("nhl", "p1_home_win"):    {"factor": 0.30, "mc": 0.40, "gbm": 0.30},
    ("nhl", "p1_total"):       {"factor": 0.30, "mc": 0.40, "gbm": 0.30},
    # NBA: same three-way default for each market; Q1 markets lean a
    # bit more on MC because the factor model is Q1-specific.
    ("nba", "q1_home_win"):    {"factor": 0.30, "mc": 0.40, "gbm": 0.30},
    ("nba", "q1_total"):       {"factor": 0.30, "mc": 0.40, "gbm": 0.30},
    ("nba", "q1_margin"):      {"factor": 0.30, "mc": 0.40, "gbm": 0.30},
    ("nba", "home_win"):       {"factor": 0.40, "mc": 0.30, "gbm": 0.30},
}


def weights_for(sport: str, market: str) -> dict[str, float]:
    """Return the blend weights for a (sport, market), consulting overrides."""
    try:
        from .model_overrides import get_override
        ov = get_override(sport, f"ENSEMBLE_{market}")
        if isinstance(ov, str):
            try:
                parsed = json.loads(ov)
                if isinstance(parsed, dict):
                    return _normalize(parsed)
            except Exception:
                pass
    except Exception:
        pass
    return _DEFAULT_WEIGHTS.get((sport, market), {"factor": 1.0})


def _normalize(w: dict[str, float]) -> dict[str, float]:
    """Drop components with non-positive weight and re-normalize to sum 1."""
    w2 = {k: float(v) for k, v in w.items() if float(v) > 0}
    s = sum(w2.values())
    if s <= 0:
        return {"factor": 1.0}
    return {k: v / s for k, v in w2.items()}


def blend(values: dict[str, float], weights: dict[str, float]) -> float | None:
    """Weighted average over the components that produced a value.

    If some components returned None (missing data, untrained model),
    their weight gets redistributed across the available ones. Returns
    None only when every component is missing.
    """
    available = {k: (v, weights.get(k, 0.0)) for k, v in values.items()
                 if v is not None and weights.get(k, 0.0) > 0}
    if not available:
        return None
    total_w = sum(w for _, w in available.values())
    if total_w <= 0:
        return None
    return sum(v * (w / total_w) for v, w in available.values())


# ── Sport-specific glue ────────────────────────────────────────

def ensemble_mlb(pred: dict) -> dict:
    """Produce ensemble probabilities for the MLB markets we trade.

    Reads the factor-model output already on pred, pred["mc"], and
    pred["gbm"] if present. Leaves pred untouched and returns a new
    "ensemble" dict with fields:
      home_win, total_expected, nrfi, f5_home_win, f5_total, weights_used
    """
    mc = (pred.get("mc") or {})
    gbm = (pred.get("gbm") or {})
    if "error" in mc: mc = {}
    if "error" in gbm: gbm = {}

    out: dict[str, Any] = {"weights_used": {}}

    # Home win probability
    factor_home_wp = (pred.get("win_prob") or {}).get("home")
    mc_home_wp = (mc.get("win_prob") or {}).get("home")
    gbm_home_wp = gbm.get("home_win")
    w = weights_for("mlb", "home_win")
    home_wp = blend(
        {"factor": factor_home_wp, "mc": mc_home_wp, "gbm": gbm_home_wp},
        w,
    )
    if home_wp is not None:
        out["home_win"] = round(home_wp, 4)
        out["weights_used"]["home_win"] = w

    # Total runs (expected value)
    factor_total = pred.get("total")
    mc_total = (mc.get("expected_runs") or {}).get("total")
    gbm_total = gbm.get("total_runs")
    w = weights_for("mlb", "total")
    total = blend(
        {"factor": factor_total, "mc": mc_total, "gbm": gbm_total},
        w,
    )
    if total is not None:
        out["total_expected"] = round(total, 3)
        out["weights_used"]["total"] = w

    # NRFI probability
    factor_nrfi = (pred.get("first_inning") or {}).get("nrfi")
    mc_nrfi = (mc.get("nrfi") or {}).get("nrfi")
    gbm_nrfi = gbm.get("nrfi_hit")
    w = weights_for("mlb", "nrfi")
    nrfi = blend(
        {"factor": factor_nrfi, "mc": mc_nrfi, "gbm": gbm_nrfi},
        w,
    )
    if nrfi is not None:
        out["nrfi"] = round(nrfi, 4)
        out["weights_used"]["nrfi"] = w

    # F5 home win probability
    factor_f5_wp = ((pred.get("f5") or {}).get("win_prob") or {}).get("home")
    mc_f5_wp = ((mc.get("f5") or {}).get("win_prob") or {}).get("home")
    gbm_f5_wp = gbm.get("f5_home_win")
    w = weights_for("mlb", "f5_home_win")
    f5_wp = blend(
        {"factor": factor_f5_wp, "mc": mc_f5_wp, "gbm": gbm_f5_wp},
        w,
    )
    if f5_wp is not None:
        out["f5_home_win"] = round(f5_wp, 4)
        out["weights_used"]["f5_home_win"] = w

    # F5 total expected value
    factor_f5_total = (pred.get("f5") or {}).get("total")
    mc_f5_total = ((mc.get("f5") or {}).get("expected_runs") or {}).get("total")
    gbm_f5_total = gbm.get("f5_total")
    w = weights_for("mlb", "f5_total")
    f5_total = blend(
        {"factor": factor_f5_total, "mc": mc_f5_total, "gbm": gbm_f5_total},
        w,
    )
    if f5_total is not None:
        out["f5_total_expected"] = round(f5_total, 3)
        out["weights_used"]["f5_total"] = w

    return out


def ensemble_nhl(pred: dict) -> dict:
    """NHL ensemble: factor + MC + GBM. Missing components (e.g. GBM not
    yet trained) have their weight redistributed by blend()."""
    mc = (pred.get("mc") or {})
    gbm = (pred.get("gbm") or {})
    if "error" in mc: mc = {}
    if "error" in gbm: gbm = {}
    out: dict[str, Any] = {"weights_used": {}}

    factor_wp = (pred.get("win_prob") or {}).get("home")
    mc_wp = (mc.get("win_prob") or {}).get("home")
    gbm_wp = gbm.get("home_win")
    w = weights_for("nhl", "home_win")
    wp = blend({"factor": factor_wp, "mc": mc_wp, "gbm": gbm_wp}, w)
    if wp is not None:
        out["home_win"] = round(wp, 4)
        out["weights_used"]["home_win"] = w

    factor_total = pred.get("total")
    mc_total = (mc.get("expected_goals") or {}).get("total")
    gbm_total = gbm.get("total_goals")
    w = weights_for("nhl", "total")
    tot = blend({"factor": factor_total, "mc": mc_total, "gbm": gbm_total}, w)
    if tot is not None:
        out["total_expected"] = round(tot, 3)
        out["weights_used"]["total"] = w

    # Period-1 markets. Both the factor model (engine.nhl_predict) and
    # the MC simulator emit a `first_period` block, not `p1` -- prior
    # revision hit the wrong key and silently blended nothing from
    # factor + MC, so only GBM was contributing to p1 ensemble rows.
    factor_fp = pred.get("first_period") or {}
    mc_fp = mc.get("first_period") or {}

    # p1 home-win probability: factor doesn't emit a scalar, only the
    # expected_home/away split. MC + GBM carry the blend; factor weight
    # redistributes through blend() when None.
    mc_p1_wp = mc_fp.get("home_win")
    gbm_p1_wp = gbm.get("p1_home_win")
    w = weights_for("nhl", "p1_home_win")
    p1_wp = blend({"factor": None, "mc": mc_p1_wp, "gbm": gbm_p1_wp}, w)
    if p1_wp is not None:
        out["p1_home_win"] = round(p1_wp, 4)
        out["weights_used"]["p1_home_win"] = w

    # p1 total: all three signals present (factor = expected_home +
    # expected_away, MC = expected_total, GBM = p1_total_goals).
    factor_p1_tot = factor_fp.get("expected_total")
    mc_p1_tot = mc_fp.get("expected_total")
    gbm_p1_tot = gbm.get("p1_total_goals")
    w = weights_for("nhl", "p1_total")
    p1_tot = blend({"factor": factor_p1_tot, "mc": mc_p1_tot, "gbm": gbm_p1_tot}, w)
    if p1_tot is not None:
        out["p1_total_expected"] = round(p1_tot, 3)
        out["weights_used"]["p1_total"] = w

    return out


def ensemble_nba(pred: dict) -> dict:
    """NBA Q1 ensemble: factor + MC + GBM."""
    mc = (pred.get("mc") or {})
    gbm = (pred.get("gbm") or {})
    if "error" in mc: mc = {}
    if "error" in gbm: gbm = {}
    out: dict[str, Any] = {"weights_used": {}}

    # Q1 ML
    factor_wp = pred.get("q1_ml_home") or (pred.get("win_prob") or {}).get("home")
    mc_wp = (mc.get("win_prob") or {}).get("home")
    gbm_wp = gbm.get("q1_home_win")
    w = weights_for("nba", "q1_home_win")
    wp = blend({"factor": factor_wp, "mc": mc_wp, "gbm": gbm_wp}, w)
    if wp is not None:
        out["q1_home_win"] = round(wp, 4)
        out["weights_used"]["q1_home_win"] = w

    # Q1 total
    factor_total = pred.get("predicted_total") or pred.get("q1_total")
    mc_total = (mc.get("expected_points") or {}).get("total")
    gbm_total = gbm.get("q1_total_points")
    w = weights_for("nba", "q1_total")
    tot = blend({"factor": factor_total, "mc": mc_total, "gbm": gbm_total}, w)
    if tot is not None:
        out["q1_total_expected"] = round(tot, 3)
        out["weights_used"]["q1_total"] = w

    # Q1 margin (home minus away) -- regression, no factor/MC signal today
    # so GBM stands alone here. Exposed for completeness so callers can
    # reason about Q1 spread value.
    gbm_margin = gbm.get("q1_margin")
    if gbm_margin is not None:
        w = weights_for("nba", "q1_margin")
        margin = blend({"gbm": gbm_margin}, w)
        if margin is not None:
            out["q1_margin_expected"] = round(margin, 3)
            out["weights_used"]["q1_margin"] = w

    # Full-game ML -- GBM-only for now; factor model is Q1-focused.
    gbm_full_wp = gbm.get("home_win")
    if gbm_full_wp is not None:
        w = weights_for("nba", "home_win")
        full_wp = blend({"gbm": gbm_full_wp}, w)
        if full_wp is not None:
            out["home_win"] = round(full_wp, 4)
            out["weights_used"]["home_win"] = w

    return out
