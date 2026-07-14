"""Closing-line Bayesian prior — final-stage probability adjustment.

Treats the market's implied probability as informed prior and the
model's calibrated output as likelihood. Posterior is a log-odds linear
pool of the two, with the model's weight learned per (sport, bet_type)
by minimizing held-out Brier on settled picks.

Why this exists
---------------
Books aggregate every sharp's information into one number. Anchoring
on the market and letting the model push only when it has strong
evidence is mathematically the right move under "predict reality
accurately" — which is the project's stated goal (see
feedback_be_right_first.md).

Tradeoff
--------
Shrinking toward market by construction shrinks "edge". Marginal-edge
picks (model 55% / market 51%) collapse closer to market. Only picks
where the model strongly disagrees with the market survive the edge
floor. That's the intended behavior under accuracy-first.

Math
----
For binary outcomes, log-odds linear pool is the well-behaved
combiner — preserves coherent calibration when both inputs are
calibrated::

    logit(post) = w * logit(model) + (1 - w) * logit(market)

w = 1 → pure model (no prior).
w = 0 → pure market (model ignored).
w = 0.5 → equal weight.

Per (sport, bet_type) the optimal w is found by grid search over
historical settled picks: minimize Brier of posterior vs actual
outcome.

Storage
-------
data/closing_line_weights.json::

    {
      "mlb": {"_default": 0.65, "ML": 0.7, "RL": 0.5, "O/U": 0.6},
      "nhl": {"_default": 0.55, ...},
      ...
    }

Fall back to ``_default`` per sport when no per-bet-type weight
exists. Fall back to 1.0 (pure model) when even the default is
missing — preserves old behavior so the prior can ship dark.

CLI::

    python -m engine.closing_line_prior --fit         # fit all sports
    python -m engine.closing_line_prior --fit mlb     # one sport
    python -m engine.closing_line_prior --report      # show fitted weights
"""

from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)


_WEIGHTS_PATH = Path(__file__).resolve().parent.parent / "data" / "closing_line_weights.json"
_MIN_FIT_SAMPLES = 50  # per (sport, bet_type) before we trust a fitted weight
_DEFAULT_WEIGHT = 1.0  # cold-start: pure model, prior dormant
# Floor on fitted weights — never below 0.5. Otherwise a single
# bad-fit batch can set w=0 ("follow market") and permanently silence
# the model: every subsequent pick collapses to implied prob, edge
# becomes 0, no new picks fire, no new data to re-fit on.
_MIN_WEIGHT = 0.5


# ── Math ────────────────────────────────────────────────────────

def _clip(p: float, lo: float = 1e-6, hi: float = 1 - 1e-6) -> float:
    """Clamp to (0,1) open interval to keep logits finite."""
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


def _implied_from_american(odds: int | float) -> float:
    """American → implied probability. No-vig adjustment is the
    market's job, not ours; we want raw market belief here."""
    o = int(odds)
    if o < 0:
        return abs(o) / (abs(o) + 100)
    return 100.0 / (o + 100)


def posterior_prob(model_prob: float, market_implied: float,
                   weight: float) -> float:
    """Log-odds linear pool. ``weight`` is the model's contribution;
    market gets (1 - weight). Returns posterior probability."""
    weight = max(0.0, min(1.0, float(weight)))
    if weight >= 0.999:
        return _clip(model_prob)
    if weight <= 0.001:
        return _clip(market_implied)
    z = weight * _logit(model_prob) + (1.0 - weight) * _logit(market_implied)
    return _sigmoid(z)


# ── Weights persistence ────────────────────────────────────────

_CACHE: dict | None = None
_CACHE_MTIME: float = 0.0


def _load() -> dict:
    """Load weights JSON and refresh on mtime change. Earlier this only
    loaded once per process, so a fit() that wrote a fresh file
    required an explicit invalidate() call. Now invalidation happens
    automatically on the next call after the file changes."""
    global _CACHE, _CACHE_MTIME
    if not _WEIGHTS_PATH.exists():
        _CACHE = {}
        _CACHE_MTIME = 0.0
        return _CACHE
    cur_mtime = _WEIGHTS_PATH.stat().st_mtime
    if _CACHE is not None and cur_mtime == _CACHE_MTIME:
        return _CACHE
    try:
        _CACHE = json.loads(_WEIGHTS_PATH.read_text(encoding="utf-8"))
        _CACHE_MTIME = cur_mtime
    except Exception as exc:
        logger.warning("closing_line_prior: load failed (%s)", exc)
        _CACHE = {}
        _CACHE_MTIME = cur_mtime
    return _CACHE


def invalidate() -> None:
    """Drop in-memory cache; next call re-reads from disk. Used after
    a fit so a running backend picks up new weights without restart."""
    global _CACHE
    _CACHE = None


def learned_weight(sport: str, bet_type: str) -> float:
    """Return the model's blend weight for (sport, bet_type).
    Lookup order: per-bet-type → per-sport ``_default`` → global
    ``_DEFAULT_WEIGHT``."""
    data = _load()
    sport_block = data.get(sport.lower()) or {}
    if bet_type in sport_block:
        return float(sport_block[bet_type])
    if "_default" in sport_block:
        return float(sport_block["_default"])
    return _DEFAULT_WEIGHT


def _save(data: dict) -> None:
    _WEIGHTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    _WEIGHTS_PATH.write_text(json.dumps(data, indent=2, sort_keys=True),
                              encoding="utf-8")
    invalidate()


# ── Fitting ────────────────────────────────────────────────────

def _brier(samples: Iterable[tuple[float, int]]) -> float:
    """Mean (prob - outcome)^2."""
    n = 0
    s = 0.0
    for p, y in samples:
        s += (p - y) ** 2
        n += 1
    return s / n if n else float("inf")


def _fit_weight(rows: list[tuple[float, float, int]]) -> tuple[float, float]:
    """Grid-search the optimal model weight w∈[_MIN_WEIGHT, 1] in 0.05
    steps that minimizes Brier of posterior vs actual outcome.

    Floored at 0.5 so the fit can never fully silence the model — a
    weight of 0 means "ignore model, follow market", which collapses
    every pick's edge to 0 and stops new picks from being made (which
    in turn means no new data, so the model can never recover from
    a bad fit). The 0.5 floor lets the prior shrink toward market by
    up to 50% but always preserves at least half the model's signal.
    User flagged 2026-05-25: MLB/NBA/tennis all silently shut down
    after a fit that returned w=0 for every sport.

    rows: list of (model_prob, market_implied, outcome 0/1).
    Returns (best_w, best_brier).
    """
    best_w = 0.75
    best_brier = float("inf")
    for step in range(11):  # 10 steps: 0.50, 0.55, ..., 1.00
        w = _MIN_WEIGHT + step * 0.05
        if w > 1.0:
            break
        samples = ((posterior_prob(mp, mi, w), y) for mp, mi, y in rows)
        b = _brier(samples)
        if b < best_brier:
            best_brier = b
            best_w = w
    return best_w, best_brier


def _fetch_settled_rows(sport: str) -> dict[str, list[tuple[float, float, int]]]:
    """Pull settled picks with both model_prob and closing_odds, grouped
    by bet_type. outcome = 1 for W, 0 for L; pushes excluded.
    Reads the per-sport tracker DB; tennis stored separately."""
    sport = sport.lower()
    rows_by_bt: dict[str, list[tuple[float, float, int]]] = {}
    try:
        if sport == "mlb":
            from .db import get_conn
            conn = get_conn()
            table = "picks"
        elif sport == "nhl":
            from .nhl_tracker._helpers import _get_nhl_db
            conn = _get_nhl_db()
            table = "nhl_picks"
        elif sport == "nba":
            from .nba_db import get_conn
            conn = get_conn()
            table = "nba_picks"
        elif sport == "tennis":
            from .tennis_db import get_conn
            conn = get_conn()
            table = "tennis_picks"
        elif sport == "nba_live":
            from .nba_db import get_conn
            conn = get_conn()
            table = "live_picks_nba"
        elif sport == "nhl_live":
            from .nhl_db import get_conn
            conn = get_conn()
            table = "live_picks_nhl"
        else:
            return rows_by_bt
        sql = (f"SELECT bet_type, model_prob, closing_odds, result "
               f"FROM {table} "
               f"WHERE result IN ('W','L') "
               f"  AND model_prob IS NOT NULL "
               f"  AND closing_odds IS NOT NULL")
        for r in conn.execute(sql).fetchall():
            bt = r["bet_type"]
            mp = r["model_prob"]
            try:
                ci = _implied_from_american(int(r["closing_odds"]))
            except (TypeError, ValueError):
                continue
            if mp is None:
                continue
            y = 1 if r["result"] == "W" else 0
            rows_by_bt.setdefault(bt, []).append((float(mp), ci, y))
    except Exception as exc:
        logger.warning("closing_line_prior: fetch %s failed (%s)", sport, exc)
    return rows_by_bt


def fit_sport(sport: str) -> dict:
    """Fit per-bet-type weights for one sport. Bet types with fewer
    than ``_MIN_FIT_SAMPLES`` are skipped — falls back to the per-sport
    ``_default`` (also fitted from the union of all bet types).

    Returns the fitted-block dict (also persisted)."""
    rows_by_bt = _fetch_settled_rows(sport)
    fitted: dict[str, float] = {}
    diagnostics: dict[str, dict] = {}
    union: list = []
    for bt, rows in rows_by_bt.items():
        union.extend(rows)
        if len(rows) < _MIN_FIT_SAMPLES:
            diagnostics[bt] = {"n": len(rows), "skipped": True}
            continue
        w, brier_post = _fit_weight(rows)
        brier_model = _brier(((mp, y) for mp, _, y in rows))
        brier_market = _brier(((mi, y) for _, mi, y in rows))
        fitted[bt] = round(w, 3)
        diagnostics[bt] = {
            "n": len(rows),
            "w": round(w, 3),
            "brier_model": round(brier_model, 4),
            "brier_market": round(brier_market, 4),
            "brier_posterior": round(brier_post, 4),
            "improvement_vs_model": round(brier_model - brier_post, 4),
        }
    if union:
        w_default, brier_default = _fit_weight(union)
        brier_model_all = _brier(((mp, y) for mp, _, y in union))
        fitted["_default"] = round(w_default, 3)
        diagnostics["_default"] = {
            "n": len(union),
            "w": round(w_default, 3),
            "brier_model": round(brier_model_all, 4),
            "brier_posterior": round(brier_default, 4),
            "improvement_vs_model": round(brier_model_all - brier_default, 4),
        }

    data = _load()
    data[sport.lower()] = fitted
    _save(data)
    return {"fitted": fitted, "diagnostics": diagnostics}


def fit_all_sports() -> dict[str, dict]:
    return {sport: fit_sport(sport)
            for sport in ("mlb", "nhl", "nba", "tennis",
                          "nba_live", "nhl_live")}


# ── CLI ────────────────────────────────────────────────────────

def _report() -> str:
    data = _load()
    if not data:
        return "(no fitted weights — run --fit)"
    lines = []
    for sport in sorted(data):
        block = data[sport]
        lines.append(f"== {sport.upper()} ==")
        for bt in sorted(block):
            lines.append(f"  {bt:25s} w={block[bt]:.3f}")
    return "\n".join(lines)


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="Closing-line Bayesian prior admin")
    p.add_argument("--fit", nargs="?", const="all",
                   help="Fit weights — pass a sport, or omit for all")
    p.add_argument("--report", action="store_true",
                   help="Print currently-stored weights")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    if args.fit:
        sports = (("mlb", "nhl", "nba", "tennis", "nba_live", "nhl_live")
                   if args.fit == "all" else (args.fit,))
        for sport in sports:
            result = fit_sport(sport)
            print(f"\n=== {sport.upper()} ===")
            for bt, diag in result["diagnostics"].items():
                if diag.get("skipped"):
                    print(f"  {bt:25s} n={diag['n']} (skipped - under {_MIN_FIT_SAMPLES})")
                else:
                    delta = diag.get("improvement_vs_model", 0.0)
                    tag = "BETTER" if delta > 0 else ("WORSE" if delta < -0.001 else "neutral")
                    print(f"  {bt:25s} n={diag['n']:>4d} w={diag['w']:.3f} "
                          f"brier_model={diag['brier_model']:.4f} "
                          f"brier_post={diag['brier_posterior']:.4f} "
                          f"delta={delta:+.4f}  {tag}")
        print()
    if args.report:
        print(_report())


if __name__ == "__main__":
    main()
