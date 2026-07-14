"""
Feature-importance audit for prematch GBMs.

Walks every saved GBM artifact (mlb / nhl / nba × all targets) and
reports per-feature gain. Identifies features that are dead weight
under the ``factors=noise`` rule:

  - **Zero-importance**: feature was never used as a split. Strong
    candidate for removal.
  - **Trivial-importance**: gain is below ``LOW_GAIN_THRESHOLD`` of
    the model's total gain. Carrying it adds variance for marginal
    contribution.

Why this matters
----------------
The auto-tune output 2026-05-01 showed factor weight = 0.00 across
all 5 MLB markets — meaning the GBM is doing all the predictive
work. If the GBM has 49 features but only 25 actually pull weight,
we're paying compute + carrying 24 features of noise that hurt
calibration.

This module doesn't auto-prune (factors=noise rule applies but the
removal decision is operator's). It surfaces the candidates.

CLI::

    python -m engine.gbm.feature_audit [--sport mlb|nhl|nba|all]
                                         [--target home_win|...]
                                         [--threshold 0.005]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


_MODELS_DIR = (Path(__file__).resolve().parent.parent.parent
                / "data" / "models")

# Features below this fraction of total gain are flagged as
# trivial-importance candidates. 0.5% is conservative — anything
# under that contributes < 1/200th of the model's predictive power.
LOW_GAIN_THRESHOLD = 0.005

# Targets per sport, mirrors engine.gbm.train.SPORT_TARGETS for the
# prematch models (excluding tennis_atp/wta and *_live which have
# their own audit needs).
_PREMATCH_TARGETS = {
    "mlb": ("home_win", "nrfi_hit", "f5_home_win",
            "total_runs", "f5_total"),
    "nhl": ("home_win", "total_goals", "p1_home_win",
            "p1_total_goals"),
    "nba": ("home_win", "total_points", "margin",
            "q1_home_win", "q1_total_points", "q1_margin"),
}


# ── Audit primitives ──────────────────────────────────────────

def _load_model(sport: str, target: str):
    """Load the latest GBM artifact for (sport, target). Returns
    (booster, feature_names) or (None, None) on failure."""
    art_path = _MODELS_DIR / f"{sport}_gbm_{target}_latest.json"
    meta_path = _MODELS_DIR / f"{sport}_gbm_{target}_latest.meta.json"
    if not art_path.exists() or not meta_path.exists():
        return None, None
    try:
        import xgboost as xgb
    except ImportError:
        return None, None
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        feature_names = meta.get("feature_names") or []
        # Loading via XGBClassifier first; if that fails (regression
        # target), retry with XGBRegressor.
        try:
            m = xgb.XGBClassifier()
            m.load_model(str(art_path))
        except Exception:
            m = xgb.XGBRegressor()
            m.load_model(str(art_path))
        return m.get_booster(), feature_names
    except Exception as e:
        logger.warning("load failed for %s/%s: %s", sport, target, e)
        return None, None


def audit(sport: str, target: str,
          threshold: float = LOW_GAIN_THRESHOLD) -> dict | None:
    """Audit one (sport, target) GBM. Returns a dict with per-feature
    gain + categorisation, or None when the artifact can't be loaded."""
    booster, feature_names = _load_model(sport, target)
    if booster is None:
        return None
    # XGBoost's get_score uses the feature names registered at training
    # time. When trained from a pandas DataFrame these are real names;
    # otherwise they're 'f0', 'f1', ...
    raw_scores = booster.get_score(importance_type="gain")
    total_gain = sum(raw_scores.values()) or 1.0

    by_feature: list[dict] = []
    used_features = set()
    for fname, gain in raw_scores.items():
        # XGBoost may emit either real names or 'fNN' depending on how
        # the booster was loaded. Re-map 'fNN' to feature_names[NN]
        # when applicable.
        display_name = fname
        if fname.startswith("f") and fname[1:].isdigit():
            idx = int(fname[1:])
            if 0 <= idx < len(feature_names):
                display_name = feature_names[idx]
        used_features.add(display_name)
        by_feature.append({
            "name": display_name,
            "gain": float(gain),
            "share": float(gain) / total_gain,
        })
    by_feature.sort(key=lambda r: -r["gain"])

    zero_imp = [f for f in feature_names if f not in used_features]
    trivial_imp = [f for f in by_feature if f["share"] < threshold]

    return {
        "sport": sport,
        "target": target,
        "total_features": len(feature_names),
        "used_features": len(used_features),
        "total_gain": round(total_gain, 2),
        "by_feature": by_feature,
        "zero_importance": sorted(zero_imp),
        "trivial_importance": [f["name"] for f in trivial_imp],
        "threshold": threshold,
    }


def audit_sport(sport: str, threshold: float = LOW_GAIN_THRESHOLD) -> dict:
    """Run audit for every prematch target of ``sport``."""
    targets = _PREMATCH_TARGETS.get(sport, ())
    out = {}
    for target in targets:
        result = audit(sport, target, threshold=threshold)
        if result is not None:
            out[target] = result
    return out


def cross_target_summary(audit_results: dict) -> dict:
    """Pool feature gains across targets to find features that are
    dead weight in EVERY model — strongest removal candidates."""
    by_feature: dict[str, dict] = defaultdict(lambda: {
        "total_gain": 0.0, "models_used_in": 0, "models_total": 0
    })
    for target, info in audit_results.items():
        used_set = {f["name"] for f in info["by_feature"]}
        # Count this target as a denominator for every feature in
        # the model
        for fname in info["zero_importance"]:
            by_feature[fname]["models_total"] += 1
        for fname in used_set:
            by_feature[fname]["total_gain"] += sum(
                f["gain"] for f in info["by_feature"]
                if f["name"] == fname
            )
            by_feature[fname]["models_used_in"] += 1
            by_feature[fname]["models_total"] += 1
    # Flag features that were dead in every model
    dead_in_every = [fname for fname, agg in by_feature.items()
                      if agg["models_total"] >= 2
                      and agg["models_used_in"] == 0]
    return {
        "by_feature": dict(by_feature),
        "dead_in_every_model": sorted(dead_in_every),
    }


# ── Reporting ─────────────────────────────────────────────────

def _print_audit(info: dict, top_n: int = 10) -> None:
    print(f"\n  {info['sport']}.{info['target']}: "
          f"{info['used_features']}/{info['total_features']} features used, "
          f"total_gain={info['total_gain']}")
    if info["by_feature"]:
        print(f"    Top {top_n} by gain:")
        for f in info["by_feature"][:top_n]:
            share_pct = f["share"] * 100
            print(f"      {f['name']:<32s} "
                  f"gain={f['gain']:>8.2f}  ({share_pct:>5.2f}%)")
    if info["zero_importance"]:
        print(f"    Zero importance ({len(info['zero_importance'])}): "
              f"{', '.join(info['zero_importance'][:6])}"
              f"{'...' if len(info['zero_importance']) > 6 else ''}")
    if info["trivial_importance"]:
        thr_pct = info["threshold"] * 100
        print(f"    Trivial (< {thr_pct:.1f}% of gain) "
              f"({len(info['trivial_importance'])}): "
              f"{', '.join(info['trivial_importance'][:6])}"
              f"{'...' if len(info['trivial_importance']) > 6 else ''}")


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(prog="engine.gbm.feature_audit")
    ap.add_argument("--sport",
                    choices=("mlb", "nhl", "nba", "all"),
                    default="all")
    ap.add_argument("--target", default=None,
                    help="Limit to a single target name.")
    ap.add_argument("--threshold", type=float,
                    default=LOW_GAIN_THRESHOLD,
                    help="Trivial-importance threshold (fraction of total gain).")
    args = ap.parse_args(argv)

    sports = (args.sport,) if args.sport != "all" else ("mlb", "nhl", "nba")
    summaries: dict = {}

    for sport in sports:
        if args.target:
            info = audit(sport, args.target, threshold=args.threshold)
            if info is None:
                print(f"  {sport}.{args.target}: artifact missing")
                continue
            _print_audit(info)
            summaries[sport] = {args.target: info}
        else:
            results = audit_sport(sport, threshold=args.threshold)
            print(f"\n== {sport.upper()} feature audit ==")
            for target in sorted(results):
                _print_audit(results[target])
            cross = cross_target_summary(results)
            if cross["dead_in_every_model"]:
                print(f"\n  *** Dead in EVERY {sport.upper()} model "
                      f"({len(cross['dead_in_every_model'])}): ***")
                for fname in cross["dead_in_every_model"]:
                    print(f"      {fname}")
            summaries[sport] = results
    return 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["audit", "audit_sport", "cross_target_summary",
            "LOW_GAIN_THRESHOLD", "_PREMATCH_TARGETS"]
