"""F1 predictor calibration.

Walk-forward backtest: for each completed race, run ``predict_race``
using only data prior to that race's date, then log per-driver
(predicted_prob, actual_outcome) pairs. Bin into prob buckets, compute
empirical hit rate per bucket with Bayesian beta-binomial shrinkage
toward the bucket midpoint, and persist the calibration map to
``model_config``.

Why beta-binomial shrinkage: same pattern the team-sports calibration
already uses (memory: project_bayesian_calibration). Empirical rates
on small buckets are noisy; shrinking toward the bucket midpoint with
PRIOR_N0=10 always returns a reasonable calibrated value, never
passthrough or NaN.

Apply: ``calibrate(series, market, raw_prob)`` maps a raw predictor
output through the persisted map. Used by ``_picks.generate_picks``
to gate on calibrated probability rather than raw model output.
"""
from __future__ import annotations

import json
import logging
from typing import Iterable

from ._db import get_conn

logger = logging.getLogger(__name__)


# Beta-binomial prior: PRIOR_N0 effective observations centered on the
# bucket midpoint. Higher = more shrinkage toward the prior, lower =
# trust empirical hit rate more. 10 matches the team-sports calibrators.
PRIOR_N0 = 10

# Markets that get calibrated. WINNER outcome = finish_pos == 1,
# PODIUM outcome = finish_pos in (1, 2, 3).
MARKETS = ("WINNER", "PODIUM")

# Per-market bucket grids. Equal-width [0,1] buckets dumped 80% of
# WINNER observations into one cell (F1 win probs cluster at the low
# end — most drivers are backmarkers with ~0.5% chance). These grids
# match the actual prob distribution per market so every bucket gets
# 40+ samples for meaningful aggregation. Same approach as
# engine.golf._calibrate.
_BUCKET_GRIDS: dict[str, list[tuple[float, float]]] = {
    "WINNER": [
        (0.000, 0.005),
        (0.005, 0.010),
        (0.010, 0.020),
        (0.020, 0.040),
        (0.040, 0.070),
        (0.070, 0.120),
        (0.120, 0.180),
        (0.180, 0.280),
        (0.280, 1.001),
    ],
    "PODIUM": [
        (0.000, 0.030),
        (0.030, 0.080),
        (0.080, 0.150),
        (0.150, 0.250),
        (0.250, 0.350),
        (0.350, 0.450),
        (0.450, 0.550),
        (0.550, 0.700),
        (0.700, 0.850),
        (0.850, 1.001),
    ],
}

_CONFIG_KEY = "calibration_v1"


# ── Walk-forward backtest ────────────────────────────────────

def backfill(series: str) -> dict:
    """Walk every complete race chronologically, generate predictions
    using only data from before that race, and log per-driver
    (predicted_prob, actual_outcome) pairs to ``backtest_predictions``.

    Returns counts. Idempotent: re-running clears the table first so
    earlier predictor versions don't bias the calibration."""
    from ._predict import predict_race
    conn = get_conn(series)
    _ensure_backtest_table(conn)
    conn.execute("DELETE FROM backtest_predictions")
    conn.commit()

    races = conn.execute("""
        SELECT race_id, race_date FROM races
        WHERE status = 'complete' ORDER BY race_date
    """).fetchall()

    n_races = 0
    n_rows = 0
    for race in races:
        race_id = race["race_id"]
        # Need at least one prior race to have any signal — first race
        # in the dataset gets predicted on a cold start. predict_race
        # handles this by falling back to neutral priors.
        # We do still want to log it because the first-race rookie
        # baseline is informative for the calibrator.
        try:
            # apply_calibration=False so we log RAW model probabilities.
            # Logging calibrated probs would create a feedback loop
            # where the next fit shrinks an already-shrunk distribution.
            pred = predict_race(series, race_id, apply_calibration=False)
        except Exception as e:
            logger.warning("backfill predict %s failed: %s", race_id, e)
            continue
        # Pull actual finishes for this race
        results = {
            r["driver_id"]: r["finish_pos"]
            for r in conn.execute(
                "SELECT driver_id, finish_pos FROM race_results "
                "WHERE race_id = ?", (race_id,),
            ).fetchall()
        }
        for d in pred.get("drivers") or []:
            did = d["driver_id"]
            actual_finish = results.get(did)
            if actual_finish is None:
                # Driver predicted but didn't show on the entry list —
                # skip rather than logging a phantom 0/0 row.
                continue
            won = 1 if actual_finish == 1 else 0
            podium = 1 if actual_finish <= 3 else 0
            conn.execute("""
                INSERT INTO backtest_predictions
                    (race_id, driver_id, p_win, p_podium, won, podium)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (race_id, did, d["p_win"], d["p_podium"], won, podium))
            n_rows += 1
        n_races += 1
    conn.commit()
    logger.info("[%s] backfill: %d races, %d rows logged",
                series, n_races, n_rows)
    return {"races": n_races, "rows": n_rows}


def _ensure_backtest_table(conn) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS backtest_predictions (
        race_id    TEXT NOT NULL,
        driver_id  INTEGER NOT NULL,
        p_win      REAL,
        p_podium   REAL,
        won        INTEGER,
        podium     INTEGER,
        PRIMARY KEY (race_id, driver_id)
    );
    """)


# ── Fit calibration map ─────────────────────────────────────

def fit(series: str) -> dict:
    """Bucket the backtest table by predicted prob and compute
    Bayesian-shrunk empirical hit rate per bucket. Persists to
    model_config under _CONFIG_KEY. Returns the fitted map for the
    caller to inspect / log."""
    conn = get_conn(series)
    rows = conn.execute(
        "SELECT p_win, p_podium, won, podium FROM backtest_predictions"
    ).fetchall()
    if not rows:
        return {}

    pairs_win = [(r["p_win"], r["won"]) for r in rows if r["p_win"] is not None]
    pairs_pod = [(r["p_podium"], r["podium"]) for r in rows if r["p_podium"] is not None]

    cal = {
        "WINNER": _fit_market(pairs_win, "WINNER"),
        "PODIUM": _fit_market(pairs_pod, "PODIUM"),
        "n_observations": len(rows),
    }
    conn.execute("""
        INSERT INTO model_config (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, (_CONFIG_KEY, json.dumps(cal)))
    conn.commit()
    return cal


def _fit_market(pairs: list[tuple[float, int]], market: str) -> dict:
    """For one market's (raw_prob, outcome) pairs, bucket and fit using
    the market-specific grid in ``_BUCKET_GRIDS``."""
    grid = _BUCKET_GRIDS[market]
    buckets: dict[int, dict] = {b: {"n": 0, "k": 0, "sum_p": 0.0}
                                 for b in range(len(grid))}
    for raw, outcome in pairs:
        b = _bucket_idx_for(market, raw)
        buckets[b]["n"] += 1
        buckets[b]["k"] += outcome
        buckets[b]["sum_p"] += raw

    out: dict[str, dict] = {}
    for b, info in buckets.items():
        lo, hi = grid[b]
        # Bayesian shrinkage: posterior = (k + alpha) / (n + alpha + beta)
        # with alpha + beta = PRIOR_N0 and alpha/(alpha+beta) = midpoint.
        # Cap hi at 1.0 for midpoint computation so the open-ended top
        # bucket [0.28, 1.001) doesn't anchor to 0.64.
        eff_hi = min(hi, 1.0)
        midpoint = (lo + eff_hi) / 2.0
        n = info["n"]
        k = info["k"]
        alpha = PRIOR_N0 * midpoint
        beta = PRIOR_N0 * (1.0 - midpoint)
        if n > 0:
            calibrated = (k + alpha) / (n + alpha + beta)
            avg_raw = info["sum_p"] / n
        else:
            calibrated = midpoint
            avg_raw = midpoint
        out[str(b)] = {
            "n": n,
            "wins": k,
            "bucket": [lo, hi],
            "midpoint": midpoint,
            "avg_raw": avg_raw,
            "calibrated": calibrated,
        }
    return out


def _bucket_idx_for(market: str, raw_prob: float) -> int:
    """Resolve which bucket index ``raw_prob`` falls into for the
    given ``market``. Clamps below 0 → 0; above 1 → last bucket."""
    grid = _BUCKET_GRIDS.get(market)
    if not grid:
        return 0
    if raw_prob is None or raw_prob <= 0:
        return 0
    for i, (lo, hi) in enumerate(grid):
        if lo <= raw_prob < hi:
            return i
    return len(grid) - 1


# ── Apply calibration at inference ──────────────────────────

# Per-process cache so we don't re-deserialize the JSON map on every
# pick generation. Keyed by series; cleared explicitly via clear_cache.
_CACHE: dict[str, dict] = {}


def _load(series: str) -> dict:
    if series in _CACHE:
        return _CACHE[series]
    conn = get_conn(series)
    row = conn.execute(
        "SELECT value FROM model_config WHERE key = ?", (_CONFIG_KEY,),
    ).fetchone()
    if not row:
        _CACHE[series] = {}
        return {}
    try:
        cal = json.loads(row["value"])
    except (json.JSONDecodeError, TypeError):
        _CACHE[series] = {}
        return {}
    _CACHE[series] = cal
    return cal


def calibrate(series: str, market: str, raw_prob: float) -> float:
    """Map a raw predictor probability through the calibration table
    for ``market`` (WINNER | PODIUM). Returns ``raw_prob`` unchanged
    when no calibration is fitted yet."""
    if raw_prob is None:
        return raw_prob
    cal = _load(series)
    market_map = cal.get(market) if cal else None
    if not market_map:
        return raw_prob
    bucket = _bucket_idx_for(market, raw_prob)
    info = market_map.get(str(bucket))
    if not info:
        return raw_prob
    return float(info["calibrated"])


def clear_cache() -> None:
    _CACHE.clear()


def refresh(series: str) -> dict:
    """One-shot: backfill predictions then fit calibration. Returns the
    fitted map. Cron / manual operators call this after a new race
    settles to keep the calibrator current."""
    backfill_out = backfill(series)
    cal = fit(series)
    clear_cache()
    return {"backfill": backfill_out, "calibration": cal,
            "diagnostics": diagnostics(series)}


def diagnostics(series: str) -> dict:
    """Brier score + per-bucket calibration table for inspection.
    Useful for the Calibration tab when picks history is still empty
    but the predictor has run a backtest. Series that haven't run
    ``fit_from_history`` yet (NASCAR / IndyCar during onboarding)
    return an empty-diagnostics stub rather than crashing the panel."""
    conn = get_conn(series)
    _ensure_backtest_table(conn)
    rows = conn.execute(
        "SELECT p_win, p_podium, won, podium FROM backtest_predictions"
    ).fetchall()
    if not rows:
        return {"brier_win": None, "brier_podium": None, "n": 0,
                "calibration": _load(series)}
    n = 0
    sw = 0.0
    sp = 0.0
    for r in rows:
        if r["p_win"] is not None:
            sw += (r["p_win"] - r["won"]) ** 2
        if r["p_podium"] is not None:
            sp += (r["p_podium"] - r["podium"]) ** 2
        n += 1
    return {
        "n": n,
        "brier_win": sw / n if n else None,
        "brier_podium": sp / n if n else None,
        "calibration": _load(series),
    }


if __name__ == "__main__":
    import sys
    series = sys.argv[1] if len(sys.argv) > 1 else "f1"
    out = refresh(series)
    diag = out["diagnostics"]
    print(f"[{series}] backfill: {out['backfill']}")
    print(f"[{series}] n={diag['n']}  brier_win={diag['brier_win']:.4f}  "
          f"brier_podium={diag['brier_podium']:.4f}")
