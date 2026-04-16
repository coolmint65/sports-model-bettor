"""
Ensemble blend-weight tuner.

Given historical predictions from all three signals (factor model, MC,
GBM) and the actual outcomes, search a small grid of blend weights and
pick the combination with the lowest Brier score / RMSE per market.

The tuned weights are written to the model_overrides table under keys
ENSEMBLE_<market> so engine.ensemble.weights_for picks them up on the
next prediction without a redeploy.

Prerequisites:
  1. Run engine.mc_backtest to get MC predictions on stored games.
  2. Train a GBM via engine.gbm.train so artifacts exist.
  3. Run this tuner: python -m engine.ensemble_tune mlb --days 60

This module does NOT produce MC/GBM predictions on the fly -- it
assumes you have historical model outputs saved. A future iteration
will orchestrate the three signals + outcomes in one pass, but for
now the simpler path is to run the three backtest CLIs and feed their
outputs in.
"""

from __future__ import annotations

import argparse
import json
import logging
from itertools import product
from typing import Iterable

logger = logging.getLogger(__name__)


# Weight grid: only integer percentages summing to 100 in 5% steps.
# For three signals that's 231 combinations; for two signals it's 21.
_GRID_STEP = 5   # percent


def weight_grid(n_components: int) -> Iterable[tuple[float, ...]]:
    """Yield every tuple of n non-negative floats summing to 1.0 in _GRID_STEP%."""
    if n_components == 1:
        yield (1.0,)
        return
    steps = list(range(0, 101, _GRID_STEP))
    if n_components == 2:
        for a in steps:
            yield (a / 100.0, (100 - a) / 100.0)
    elif n_components == 3:
        for a in steps:
            for b in steps:
                if a + b <= 100:
                    yield (a / 100.0, b / 100.0, (100 - a - b) / 100.0)
    else:
        raise ValueError("Only 1, 2, or 3 components supported")


def brier(preds: list[float], actuals: list[int]) -> float:
    """Brier score for probabilistic classification; lower is better."""
    if not preds:
        return 1.0
    return sum((p - a) ** 2 for p, a in zip(preds, actuals)) / len(preds)


def rmse(preds: list[float], actuals: list[float]) -> float:
    """RMSE for regression outputs; lower is better."""
    if not preds:
        return 1e9
    return (sum((p - a) ** 2 for p, a in zip(preds, actuals)) / len(preds)) ** 0.5


def best_weights(component_preds: list[list[float]],
                  actuals: list[float],
                  is_classification: bool = True) -> tuple[tuple[float, ...], float]:
    """Brute-force the grid and return (best_weights, best_score).

    component_preds is a list of N parallel prediction arrays -- one per
    signal. Arrays must be the same length. actuals is the observed
    outcome for each game (0/1 for classification, numeric for regression).
    """
    if not component_preds or not actuals:
        return ((1.0,), 0.0)
    n_components = len(component_preds)
    n_games = len(actuals)
    best_score = float("inf")
    best_w: tuple[float, ...] = tuple([1.0 / n_components] * n_components)
    metric = brier if is_classification else rmse

    for w in weight_grid(n_components):
        blended = [
            sum(w[i] * component_preds[i][j] for i in range(n_components))
            for j in range(n_games)
        ]
        s = metric(blended, actuals)
        if s < best_score:
            best_score = s
            best_w = tuple(round(x, 2) for x in w)
    return best_w, round(best_score, 4)


def apply_tuned_weights(sport: str, market: str,
                         component_names: list[str],
                         weights: tuple[float, ...]) -> None:
    """Persist a tuned weight vector to the model_overrides table."""
    from .model_overrides import apply_override
    payload = dict(zip(component_names, weights))
    apply_override(
        sport, f"ENSEMBLE_{market}", json.dumps(payload),
        reason="ensemble tuner",
        n_samples=None,
        p_value=None,
        expiry_days=90,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sport", choices=["mlb"])
    parser.add_argument(
        "--preds-file",
        required=True,
        help=("JSON file mapping market -> {components: {...}, actuals: [...]} "
              "for the tuning window. Produce this from engine.mc_backtest + "
              "engine.gbm.predict on historical games."),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(levelname)-7s %(message)s")

    data = json.loads(open(args.preds_file).read())
    for market, m in data.items():
        comps = m["components"]   # {name: [preds]}
        actuals = m["actuals"]
        is_class = all(a in (0, 1) for a in actuals)
        names = sorted(comps.keys())
        arrays = [comps[n] for n in names]
        w, score = best_weights(arrays, actuals, is_classification=is_class)
        print(f"{args.sport}/{market}: best_weights = "
              f"{dict(zip(names, w))}, score = {score}")
        if not args.dry_run:
            apply_tuned_weights(args.sport, market, names, w)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
