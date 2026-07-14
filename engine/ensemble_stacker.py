"""Stacking meta-learner for the prematch ensemble.

The current ensemble (engine/ensemble.py) blends factor / MC / GBM with
fixed (or auto-tuned) weights — same coefficients across every game.
A stacker learns regime-dependent weights: trust GBM more when GBM
and MC agree, trust factor when they diverge, etc.

For binary markets (home_win, nrfi, q1_home_win, etc.) we fit a
logistic regression on the base components' logits::

    logit(post) = b0 + b1 * logit(factor) + b2 * logit(mc) + b3 * logit(gbm)

For regression markets (total_runs, total_goals, total_points) we fit
a plain linear regression::

    post = b0 + b1 * factor + b2 * mc + b3 * gbm

Per (sport, market) one model. Trained on historical (factor, mc, gbm)
predictions logged by ``engine/ensemble_log.py`` joined to the
realized outcome from the per-sport games table.

Storage
-------
data/stacker_weights.json::

    {
      "mlb": {
        "home_win": {"kind": "logit",
                      "coef": [b0, b_factor, b_mc, b_gbm],
                      "n": 1234, "brier": 0.234},
        "total":    {"kind": "linear",
                      "coef": [b0, b_factor, b_mc, b_gbm],
                      "n": 1102, "rmse": 1.42},
        ...
      },
      "nhl": {...},
      "nba": {...}
    }

Lookup at ensemble time:
- if (sport, market) has a fitted model → stacker_predict
- else fall back to the existing weighted blend (no behavior change)

Requires no per-sport changes to ensemble.py until fits exist.

CLI::

    python -m engine.ensemble_stacker --fit                # all sports
    python -m engine.ensemble_stacker --fit mlb            # one sport
    python -m engine.ensemble_stacker --report
"""

from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)


_WEIGHTS_PATH = Path(__file__).resolve().parent.parent / "data" / "stacker_weights.json"
_MIN_FIT_SAMPLES = 100   # per (sport, market) before we trust a fit


# ── Math ────────────────────────────────────────────────────────

def _clip(p: float, lo: float = 1e-6, hi: float = 1 - 1e-6) -> float:
    return max(lo, min(hi, p))


def _logit(p: float) -> float:
    p = _clip(p)
    return math.log(p / (1.0 - p))


def _sigmoid(z: float) -> float:
    if z >= 0:
        ez = math.exp(-z)
        return 1.0 / (1.0 + ez)
    ez = math.exp(z)
    return ez / (1.0 + ez)


def _solve_normal_equations(X: list[list[float]], y: list[float]) -> list[float]:
    """Plain OLS via normal equations: beta = (X'X)^-1 X'y. No external
    dependency. n_features small (4) so this is fine."""
    n_rows = len(X)
    n_cols = len(X[0]) if X else 0
    if n_rows == 0:
        return [0.0] * n_cols
    # Compute X'X (n_cols x n_cols) and X'y (n_cols)
    XtX = [[0.0] * n_cols for _ in range(n_cols)]
    Xty = [0.0] * n_cols
    for i in range(n_rows):
        row = X[i]
        yi = y[i]
        for a in range(n_cols):
            Xty[a] += row[a] * yi
            for b in range(n_cols):
                XtX[a][b] += row[a] * row[b]
    return _gauss_jordan_solve(XtX, Xty)


def _gauss_jordan_solve(A: list[list[float]], b: list[float]) -> list[float]:
    """Solve A x = b by Gauss-Jordan elimination on the augmented matrix."""
    n = len(A)
    M = [row[:] + [b[i]] for i, row in enumerate(A)]
    for col in range(n):
        # Pivot — largest absolute in column at or below current row.
        pivot_row = max(range(col, n), key=lambda r: abs(M[r][col]))
        if abs(M[pivot_row][col]) < 1e-12:
            # Singular; return zeros + intercept-only fallback.
            return [0.0] * n
        M[col], M[pivot_row] = M[pivot_row], M[col]
        pivot = M[col][col]
        for j in range(col, n + 1):
            M[col][j] /= pivot
        for r in range(n):
            if r != col and abs(M[r][col]) > 1e-12:
                factor = M[r][col]
                for j in range(col, n + 1):
                    M[r][j] -= factor * M[col][j]
    return [M[i][n] for i in range(n)]


def _fit_logit(rows: list[tuple[float | None, ...]]) -> dict | None:
    """Logistic regression via IRLS (Newton-Raphson). rows: list of
    (factor_p, mc_p, gbm_p, outcome 0/1). None components are skipped.
    Returns coef list [b0, b_factor, b_mc, b_gbm] or None if not enough.
    """
    # Build design matrix using only rows where ALL three components present.
    design = []
    targets = []
    for r in rows:
        f, m, g, y = r
        if f is None or m is None or g is None or y is None:
            continue
        design.append([1.0, _logit(float(f)), _logit(float(m)), _logit(float(g))])
        targets.append(float(y))
    n = len(design)
    if n < _MIN_FIT_SAMPLES:
        return None
    # IRLS — 25 iterations max, converges fast for 4 params.
    beta = [0.0, 0.5, 0.5, 0.5]
    for _ in range(25):
        # Compute predicted probabilities and weights.
        XtWX = [[0.0] * 4 for _ in range(4)]
        XtWz = [0.0] * 4
        max_step = 0.0
        for i in range(n):
            row = design[i]
            z = sum(beta[k] * row[k] for k in range(4))
            p = _sigmoid(z)
            w = p * (1.0 - p)
            if w < 1e-12:
                continue
            # Working response: z + (y - p)/w
            yi = targets[i]
            zi = z + (yi - p) / w
            for a in range(4):
                XtWz[a] += w * row[a] * zi
                for b in range(4):
                    XtWX[a][b] += w * row[a] * row[b]
        new_beta = _gauss_jordan_solve(XtWX, XtWz)
        max_step = max(abs(new_beta[k] - beta[k]) for k in range(4))
        beta = new_beta
        if max_step < 1e-6:
            break
    # Compute Brier on the same data (training fit, will overfit slightly
    # — operator should compare against held-out separately).
    brier = 0.0
    for i in range(n):
        z = sum(beta[k] * design[i][k] for k in range(4))
        brier += (_sigmoid(z) - targets[i]) ** 2
    brier /= n
    return {"kind": "logit", "coef": [round(c, 6) for c in beta],
            "n": n, "brier": round(brier, 4)}


def _fit_linear(rows: list[tuple[float | None, ...]]) -> dict | None:
    """OLS for regression markets. Returns coef + RMSE."""
    design = []
    targets = []
    for r in rows:
        f, m, g, y = r
        if f is None or m is None or g is None or y is None:
            continue
        design.append([1.0, float(f), float(m), float(g)])
        targets.append(float(y))
    n = len(design)
    if n < _MIN_FIT_SAMPLES:
        return None
    beta = _solve_normal_equations(design, targets)
    sse = 0.0
    for i in range(n):
        pred = sum(beta[k] * design[i][k] for k in range(4))
        sse += (pred - targets[i]) ** 2
    rmse = math.sqrt(sse / n)
    return {"kind": "linear", "coef": [round(c, 6) for c in beta],
            "n": n, "rmse": round(rmse, 4)}


# ── Predict (used by ensemble.py at runtime) ───────────────────

def stacker_predict(sport: str, market: str,
                    components: dict[str, float | None]) -> float | None:
    """Stacker prediction for (sport, market). Returns None if no
    fitted model exists or if any required component is missing.

    components: {"factor": p, "mc": p, "gbm": p}. Missing or None
    components disable the stacker for this call (caller falls back
    to the weighted blend).
    """
    data = _load()
    model = ((data.get(sport.lower()) or {}).get(market))
    if not model:
        return None
    f = components.get("factor")
    m = components.get("mc")
    g = components.get("gbm")
    if f is None or m is None or g is None:
        # All three required for a clean stack. Fall back to blend.
        return None
    coef = model["coef"]
    if model["kind"] == "logit":
        z = (coef[0] + coef[1] * _logit(float(f))
                     + coef[2] * _logit(float(m))
                     + coef[3] * _logit(float(g)))
        return _sigmoid(z)
    if model["kind"] == "linear":
        return coef[0] + coef[1] * float(f) + coef[2] * float(m) + coef[3] * float(g)
    return None


def has_model(sport: str, market: str) -> bool:
    """Cheap check the ensemble can use to decide stack vs blend."""
    data = _load()
    return bool((data.get(sport.lower()) or {}).get(market))


# ── Weights persistence ────────────────────────────────────────

_CACHE: dict | None = None


def _load() -> dict:
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    if not _WEIGHTS_PATH.exists():
        _CACHE = {}
        return _CACHE
    try:
        _CACHE = json.loads(_WEIGHTS_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("ensemble_stacker: load failed (%s)", exc)
        _CACHE = {}
    return _CACHE


def invalidate() -> None:
    global _CACHE
    _CACHE = None


def _save(data: dict) -> None:
    _WEIGHTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    _WEIGHTS_PATH.write_text(json.dumps(data, indent=2, sort_keys=True),
                              encoding="utf-8")
    invalidate()


# ── Fitting (reads from ensemble_log table, joined to outcomes) ─

# Per-(sport, market) target-outcome resolver. Each entry returns
# (column_in_games_table, mapper) so we can compose true outcomes
# from the per-sport games tables.
# game_id in ensemble_log is composite "{date}_{home}_{away}" — see
# the call sites in __init__.py / routes_nba.py / routes_nhl.py. Keys
# match home/away IDs (MLB/NBA) or abbreviations (NHL/tennis), as
# whatever the predict-call carried. Outcome rows here build the same
# composite key so the stacker fit can JOIN.
_TARGET_RESOLVERS = {
    ("mlb", "home_win"):    {"task": "binary",     "outcome_sql":
        "SELECT (date || '_' || home_team_id || '_' || away_team_id) AS gid, "
        "CASE WHEN home_score > away_score THEN 1.0 "
        "     WHEN home_score < away_score THEN 0.0 ELSE NULL END AS y "
        "FROM games WHERE status='final'"},
    ("mlb", "total"):       {"task": "regression", "outcome_sql":
        "SELECT (date || '_' || home_team_id || '_' || away_team_id) AS gid, "
        "(home_score + away_score) AS y FROM games WHERE status='final'"},
    ("nhl", "home_win"):    {"task": "binary",     "outcome_sql":
        "SELECT (date || '_' || home_team_id || '_' || away_team_id) AS gid, "
        "CASE WHEN home_score > away_score THEN 1.0 "
        "     WHEN home_score < away_score THEN 0.0 ELSE NULL END AS y "
        "FROM nhl_games WHERE status='final'"},
    ("nhl", "total"):       {"task": "regression", "outcome_sql":
        "SELECT (date || '_' || home_team_id || '_' || away_team_id) AS gid, "
        "(home_score + away_score) AS y FROM nhl_games WHERE status='final'"},
    ("nba", "q1_home_win"): {"task": "binary",     "outcome_sql":
        "SELECT (date || '_' || ht.abbreviation || '_' || at.abbreviation) AS gid, "
        "CASE WHEN home_q1 > away_q1 THEN 1.0 "
        "     WHEN home_q1 < away_q1 THEN 0.0 ELSE NULL END AS y "
        "FROM nba_games g "
        "LEFT JOIN nba_teams ht ON g.home_team_id = ht.id "
        "LEFT JOIN nba_teams at ON g.away_team_id = at.id "
        "WHERE status='final' AND home_q1 IS NOT NULL AND away_q1 IS NOT NULL"},
}


def _outcome_map(sport: str, market: str) -> dict:
    """Build {game_id → outcome} from the per-sport games table.
    None if (sport, market) has no resolver yet."""
    spec = _TARGET_RESOLVERS.get((sport, market))
    if not spec:
        return {}
    try:
        if sport == "mlb":
            from .db import get_conn
        elif sport == "nhl":
            from .nhl_db import get_conn
        elif sport == "nba":
            from .nba_db import get_conn
        else:
            return {}
        rows = get_conn().execute(spec["outcome_sql"]).fetchall()
    except Exception as exc:
        logger.warning("ensemble_stacker: outcome fetch failed for %s.%s (%s)",
                       sport, market, exc)
        return {}
    out = {}
    for r in rows:
        try:
            out[str(r["gid"])] = float(r["y"]) if r["y"] is not None else None
        except (TypeError, ValueError):
            continue
    return out


def fit_sport(sport: str) -> dict:
    """Fit per-market stackers for one sport from logged predictions.

    Reads from the ensemble_log table (populated by ensemble_log.record)
    joined to the per-sport games table for true outcomes.
    """
    from .ensemble_log import load_predictions
    rows_by_market = load_predictions(sport)
    fitted = {}
    diagnostics = {}
    for market, rows in rows_by_market.items():
        spec = _TARGET_RESOLVERS.get((sport, market))
        if not spec:
            diagnostics[market] = {"skipped": "no_resolver"}
            continue
        outcomes = _outcome_map(sport, market)
        # Join logged components to outcomes by game_id.
        joined = []
        for game_id, factor_p, mc_p, gbm_p in rows:
            y = outcomes.get(str(game_id))
            if y is None:
                continue
            joined.append((factor_p, mc_p, gbm_p, y))
        if spec["task"] == "binary":
            model = _fit_logit(joined)
        else:
            model = _fit_linear(joined)
        if not model:
            diagnostics[market] = {"n_joined": len(joined),
                                    "skipped": f"under_{_MIN_FIT_SAMPLES}"}
            continue
        fitted[market] = model
        diagnostics[market] = {"n_joined": len(joined), **{
            k: v for k, v in model.items() if k != "coef"
        }}
    data = _load()
    data[sport.lower()] = fitted
    _save(data)
    return {"fitted": fitted, "diagnostics": diagnostics}


def fit_all_sports() -> dict[str, dict]:
    return {sport: fit_sport(sport) for sport in ("mlb", "nhl", "nba")}


# ── CLI ────────────────────────────────────────────────────────

def _report() -> str:
    data = _load()
    if not data:
        return "(no fitted stackers — run --fit after enough predictions log)"
    lines = []
    for sport in sorted(data):
        block = data[sport]
        lines.append(f"== {sport.upper()} ==")
        for market in sorted(block):
            m = block[market]
            metric = ("brier" if m["kind"] == "logit" else "rmse")
            lines.append(f"  {market:18s} kind={m['kind']:6s} n={m['n']:>5d} "
                         f"{metric}={m[metric]:.4f}  coef={m['coef']}")
    return "\n".join(lines)


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="Ensemble stacker admin")
    p.add_argument("--fit", nargs="?", const="all",
                   help="Fit stackers — pass a sport, or omit for all")
    p.add_argument("--report", action="store_true",
                   help="Print currently-stored stackers")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    if args.fit:
        sports = ("mlb", "nhl", "nba") if args.fit == "all" else (args.fit,)
        for sport in sports:
            result = fit_sport(sport)
            print(f"\n=== {sport.upper()} ===")
            for market, diag in result["diagnostics"].items():
                if "skipped" in diag:
                    n = diag.get("n_joined", 0)
                    print(f"  {market:18s} n={n} (skipped: {diag['skipped']})")
                else:
                    metric_name = "brier" if "brier" in diag else "rmse"
                    print(f"  {market:18s} n={diag['n']:>5d} kind={diag['kind']:6s} "
                          f"{metric_name}={diag[metric_name]:.4f}")
        print()
    if args.report:
        print(_report())


if __name__ == "__main__":
    main()
