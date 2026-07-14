"""Per-league ensemble weight fitter.

Walks each league's chronological holdout (same window the GBM tested on,
read from ``<league>_gbm/summary.json``), runs the full factor+GBM+MC
blend per game, and fits weights to minimize Brier (for ml_home) or MSE
(for margin/total) on the realized outcomes.

Output:
  data/basketball/<league>_gbm/ensemble_weights.json::

    {
      "ml_home": {"factor": 0.45, "gbm": 0.30, "mc": 0.25, "brier": ...},
      "margin":  {"factor": 0.40, "gbm": 0.20, "mc": 0.40, "mse": ...},
      "total":   {"factor": 0.20, "gbm": 0.30, "mc": 0.50, "mse": ...},
      "n_holdout": N,
      "fitted_at": "..."
    }

Picked up automatically by ``engine.basketball._ensemble._load_weights``
at the next predict call — no other wiring needed.

CLI::

    python -m engine.basketball._ensemble_fit <league>
    python -m engine.basketball._ensemble_fit --all
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_OUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "basketball"

# Grid resolution for the constrained simplex search. 0.05 → 231 points
# on the 2-simplex (cheap, ~ms per target).
_GRID_STEP = 0.05

# Minimum per-signal weight. Without this, the grid found corner
# solutions (1.0 on a single signal) on small-holdout leagues —
# Latvia n=23 dropped to 1.0 MC for ml_home which is pure noise.
# The floor regularizes thin samples; the in-sample loss penalty on
# large-holdout leagues (NCAAM n=4997) is negligible since their
# fitted weights barely move.
_MIN_WEIGHT = 0.10


def _grid_simplex(step: float = _GRID_STEP, min_w: float = _MIN_WEIGHT):
    """Yield (w_factor, w_gbm, w_mc) triples summing to ~1 within ``step``,
    each weight >= ``min_w``. The min-weight floor prevents the grid
    optimum from landing on a single-signal corner — important for
    thin-holdout leagues where 1.0 weight is grid overfit, not real."""
    n = int(round(1.0 / step))
    min_steps = int(round(min_w / step))
    for i in range(min_steps, n + 1 - 2 * min_steps):
        for j in range(min_steps, n + 1 - i - min_steps):
            k = n - i - j
            if k < min_steps:
                continue
            yield (i / n, j / n, k / n)


def _signals_for_holdout(league: str, *, n_sims: int = 1000) -> dict:
    """Walk the league's holdout games, collect (factor, gbm, mc, realized)
    per target. Returns a dict of lists keyed by target name."""
    from ._db import get_conn
    from ._features import extract_features
    from ._ensemble import blend
    from ._predict import _team_full_ppg, _resolve_constants
    from ._gbm import _model_path

    summary_path = _OUT_DIR / f"{league}_gbm" / "summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(
            f"GBM summary missing — run engine.basketball._gbm {league} first"
        )
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    test_start = summary.get("test_start_date")
    if not test_start:
        raise ValueError("GBM summary missing test_start_date")

    for target in ("home_win", "margin", "total_points"):
        if not _model_path(league, target).exists():
            raise FileNotFoundError(
                f"GBM model missing for {league}/{target}"
            )

    conn = get_conn(league)
    rows = conn.execute(
        "SELECT * FROM games WHERE status='final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "  AND date >= ? ORDER BY date",
        (test_start,),
    ).fetchall()

    constants, _ = _resolve_constants(league)
    league_avg_ppg = constants["league_avg_ppg"]

    signals = {
        "ml_home": {"factor": [], "gbm": [], "mc": [], "y": []},
        "margin":  {"factor": [], "gbm": [], "mc": [], "y": []},
        "total":   {"factor": [], "gbm": [], "mc": [], "y": []},
    }
    n_processed = 0
    n_skipped = 0
    t0 = time.time()
    for i, r in enumerate(rows):
        if i and i % 100 == 0:
            logger.info("[%s] %d/%d (skipped=%d, %.0fs)",
                        league, i, len(rows), n_skipped, time.time() - t0)
        g = dict(r)
        feats = extract_features(league, g)
        if not feats:
            n_skipped += 1
            continue
        home_full = _team_full_ppg(league, g["home_team_id"], g["season"],
                                    league_avg_ppg)
        away_full = _team_full_ppg(league, g["away_team_id"], g["season"],
                                    league_avg_ppg)
        factor_view = {
            "predicted_margin": home_full["margin"] - away_full["margin"],
            "predicted_total":  home_full["ppg"] + away_full["ppg"],
            "ml_home": 0.5,
            "home_expected": home_full["ppg"],
            "away_expected": away_full["ppg"],
            "factors": {"constants": constants},
        }
        try:
            ens = blend(league, factor_view, feats,
                         home_id=g["home_team_id"],
                         away_id=g["away_team_id"],
                         n_sims=n_sims)
        except Exception as exc:
            logger.debug("[%s] blend failed game=%s: %s",
                         league, g.get("game_id"), exc)
            n_skipped += 1
            continue
        sig = ens.get("signals") or {}
        f = sig.get("factor") or {}
        gb = sig.get("gbm") or {}
        m = sig.get("mc") or {}

        y_margin = float(g["home_score"]) - float(g["away_score"])
        y_total = float(g["home_score"]) + float(g["away_score"])
        y_ml = 1.0 if y_margin > 0 else 0.0

        for tname, ykey, yval in (
            ("ml_home", "ml_home", y_ml),
            ("margin",  "margin",  y_margin),
            ("total",   "total",   y_total),
        ):
            fv = f.get(ykey); gv = gb.get(ykey); mv = m.get(ykey)
            if fv is None or gv is None or mv is None:
                continue
            signals[tname]["factor"].append(float(fv))
            signals[tname]["gbm"].append(float(gv))
            signals[tname]["mc"].append(float(mv))
            signals[tname]["y"].append(yval)
        n_processed += 1
    logger.info("[%s] collect done: processed=%d skipped=%d (%.0fs)",
                league, n_processed, n_skipped, time.time() - t0)
    return {"signals": signals, "n_processed": n_processed,
            "test_start_date": test_start}


def _fit_target(t_signals: dict, loss: str = "mse") -> dict:
    """Grid-search weights minimizing ``loss`` over the signals lists."""
    n = len(t_signals["y"])
    if n == 0:
        return {"factor": 1/3, "gbm": 1/3, "mc": 1/3,
                "loss": None, "n": 0, "method": "default"}
    fv = t_signals["factor"]
    gv = t_signals["gbm"]
    mv = t_signals["mc"]
    yv = t_signals["y"]
    best = None
    for wf, wg, wm in _grid_simplex():
        s = 0.0
        for i in range(n):
            pred = wf * fv[i] + wg * gv[i] + wm * mv[i]
            err = pred - yv[i]
            s += err * err
        mean_err = s / n
        if best is None or mean_err < best[0]:
            best = (mean_err, wf, wg, wm)
    return {
        "factor": round(best[1], 4),
        "gbm":    round(best[2], 4),
        "mc":     round(best[3], 4),
        "loss":   round(best[0], 6),
        "n":      n,
        "method": f"grid_simplex_step{_GRID_STEP}",
    }


def fit_weights(league: str, *, n_sims: int = 1000) -> dict:
    """Run holdout walk + per-target weight fit. Persists weights JSON
    to ``<league>_gbm/ensemble_weights.json``. Returns the dict."""
    data = _signals_for_holdout(league, n_sims=n_sims)
    signals = data["signals"]
    out = {
        "ml_home": _fit_target(signals["ml_home"]),
        "margin":  _fit_target(signals["margin"]),
        "total":   _fit_target(signals["total"]),
        "n_holdout": data["n_processed"],
        "test_start_date": data["test_start_date"],
        "fitted_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    out_path = _OUT_DIR / f"{league}_gbm" / "ensemble_weights.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    logger.info("[%s] ensemble weights persisted → %s",
                league, out_path.name)
    return out


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.basketball._ensemble_fit")
    ap.add_argument("league", nargs="?")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--n-sims", type=int, default=1000)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    if args.all:
        from ._config import LEAGUE_REGISTRY
        leagues = [k for k in LEAGUE_REGISTRY if k != "nba"]
    elif args.league:
        leagues = [args.league]
    else:
        ap.error("specify --all or a league name")
        return 1
    for lg in leagues:
        try:
            res = fit_weights(lg, n_sims=args.n_sims)
            print(f"[{lg}] ml_home: {res['ml_home']}")
            print(f"[{lg}] margin:  {res['margin']}")
            print(f"[{lg}] total:   {res['total']}")
        except Exception as e:
            print(f"[{lg}] FAILED: {e}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
