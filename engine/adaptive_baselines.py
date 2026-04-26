"""
Adaptive baselines — auto-tunes numeric model constants from observed
game outcomes.

The factor model has a handful of league-average constants
(LEAGUE_AVG_Q1_TOTAL, LEAGUE_AVG_PACE, MLB_AVG_RPG, NHL_AVG_GOALS, etc.)
that drift over a season — pace slows in playoffs, pitching dominates
in cold weather, etc. Hardcoded values become stale and the engine.train
report flags drift like "Q1 totals trending lower: avg 57.4 (model
baseline 58.8, off by -1.4)".

This module computes those baselines from completed games on a rolling
window and writes them via engine.model_overrides so the predict path
picks them up via get_flag(). Two-tier baselines (long + short) detect
streaks: when the short window diverges significantly from the long
window we partially blend toward the short, capturing regime shifts
without thrashing on noise.

Currently wired baselines:
    LEAGUE_AVG_Q1_TOTAL (NBA) — used by nba_q1_predict.predict_q1

To add another constant:
    1. Define a Spec in BASELINES below
    2. Implement compute_actual(spec, conn) returning a list of values
       from completed games in the lookback window
    3. Replace the hardcoded constant's read site with
       get_flag(spec.flag_name, default_value, sport=spec.sport)

Design notes (for future extension):
    - All-completed-games (not bet-on subset) — kills selection bias
    - Long window for stability, short window for streak detection
    - Z-score on (short - long) gates the blend ratio
    - Hard min/max clamps + per-update step caps prevent corruption
    - Override TTL keeps old streaks from sticking around
    - Audit trail in model_override_log
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from . import model_overrides as _ov

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BaselineSpec:
    """Configuration for one tunable baseline."""
    sport: str
    flag_name: str
    default: float
    # Hard bounds — any computed value outside this range is treated
    # as data corruption and the override is skipped.
    min_value: float
    max_value: float
    # Maximum step per update (absolute). Prevents single-day data
    # quirks from yanking the baseline far in one shot.
    max_step: float
    # Lookback windows in games. Long for stability, short for streaks.
    long_window: int
    short_window: int
    # Min sample size in the long window to bother updating at all.
    min_n: int
    # When |z(short - long)| exceeds this, blend toward short.
    regime_z_threshold: float
    # Override TTL in days; refreshed on every successful update.
    expiry_days: int
    # Function that returns a list of float observations from a DB
    # connection, sorted newest-first. Take `(conn, n)` where n is
    # max games to fetch.
    compute_actual: Callable[[object, int], list[float]]


# ── Per-baseline observation extractors ───────────────────────────

def _nba_q1_totals(conn, n: int) -> list[float]:
    """Return the last `n` completed NBA games' total Q1 points."""
    rows = conn.execute(
        "SELECT home_q1, away_q1 FROM nba_games "
        "WHERE status = 'final' AND home_q1 IS NOT NULL AND away_q1 IS NOT NULL "
        "ORDER BY date DESC LIMIT ?",
        (n,),
    ).fetchall()
    return [float(r["home_q1"]) + float(r["away_q1"]) for r in rows]


def _mlb_team_runs(conn, n: int) -> list[float]:
    """Return per-team runs from the last completed MLB games.

    Two team-rows per game (home and away) so the sample is twice the
    game count. Pulls the most recent ``n`` rows by date.
    """
    rows = conn.execute(
        "SELECT r FROM ("
        "  SELECT home_score AS r, date FROM games "
        "  WHERE status='final' AND home_score IS NOT NULL "
        "  UNION ALL "
        "  SELECT away_score AS r, date FROM games "
        "  WHERE status='final' AND away_score IS NOT NULL"
        ") ORDER BY date DESC LIMIT ?",
        (n,),
    ).fetchall()
    return [float(r["r"]) for r in rows]


# ── Spec registry ──────────────────────────────────────────────────

BASELINES: list[BaselineSpec] = [
    BaselineSpec(
        sport="nba",
        flag_name="LEAGUE_AVG_Q1_TOTAL",
        default=58.8,
        min_value=54.0,    # NBA Q1 totals never realistically below 54
        max_value=63.0,    # or above 63 — clamp guards against bad data
        max_step=1.5,      # at most ±1.5pt per update; multi-day to drift far
        long_window=200,
        short_window=30,
        min_n=60,          # need ≥60 games in long window before tuning
        regime_z_threshold=2.0,
        expiry_days=14,
        compute_actual=_nba_q1_totals,
    ),
    BaselineSpec(
        sport="mlb",
        flag_name="MLB_AVG_RPG",
        # Per-team runs/game. Source-code default in mlb_scoring.py is
        # 4.85 (held high to bias the model toward Overs while we hunt
        # the Under-WR regression); adaptive tuning will pull it back
        # toward observed reality (~4.6 last 90d) once min_n samples
        # accumulate. Cold-start before tracker has 200 team-rows: the
        # source-code default stands.
        default=4.85,
        min_value=3.5,     # league avg below 3.5 means broken data
        max_value=5.5,     # 5.5+ means a juiced-ball era; flag, don't auto-set
        max_step=0.10,     # never move more than 0.10 r/g per refresh
        long_window=400,   # ~200 games (each game contributes 2 team-rows)
        short_window=60,   # ~30 games for streak detection
        min_n=200,
        regime_z_threshold=2.0,
        expiry_days=14,
        compute_actual=_mlb_team_runs,
    ),
]


# ── Computation ────────────────────────────────────────────────────

def _stats(values: list[float]) -> tuple[float, float, int]:
    """Return (mean, stderr_of_mean, n)."""
    n = len(values)
    if n == 0:
        return (0.0, 0.0, 0)
    mean = sum(values) / n
    if n < 2:
        return (mean, 0.0, n)
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    sem = (var / n) ** 0.5
    return (mean, sem, n)


def _two_tier_baseline(long_values: list[float],
                       short_values: list[float],
                       z_threshold: float) -> tuple[float, str, float]:
    """Combine long + short into a single baseline.

    Returns (value, regime_state, z_score).
        - "stable"  (|z| < z_threshold * 0.75): use long only
        - "drift"   (z_threshold * 0.75 ≤ |z| < z_threshold): blend 0.7 long + 0.3 short
        - "shift"   (|z| ≥ z_threshold): blend 0.4 long + 0.6 short
    """
    long_mean, long_se, n_long = _stats(long_values)
    short_mean, short_se, n_short = _stats(short_values)
    if n_long == 0 or n_short == 0:
        return (long_mean if n_long else short_mean, "insufficient", 0.0)
    pooled_se = (long_se ** 2 + short_se ** 2) ** 0.5 or 1e-9
    z = (short_mean - long_mean) / pooled_se
    abs_z = abs(z)
    soft_threshold = z_threshold * 0.75
    if abs_z >= z_threshold:
        return (0.4 * long_mean + 0.6 * short_mean, "shift", z)
    if abs_z >= soft_threshold:
        return (0.7 * long_mean + 0.3 * short_mean, "drift", z)
    return (long_mean, "stable", z)


def update_baseline(spec: BaselineSpec) -> dict:
    """Compute the new baseline for one spec and write the override
    (if it cleared all guard rails). Returns a dict with the decision
    and supporting numbers — useful for logging + train-report output."""
    conn = _ov._conn(spec.sport)
    long_values = spec.compute_actual(conn, spec.long_window)
    short_values = spec.compute_actual(conn, spec.short_window)

    n_long = len(long_values)
    if n_long < spec.min_n:
        return {
            "flag": spec.flag_name, "action": "skip-low-n",
            "n_long": n_long, "min_n": spec.min_n,
        }

    new_value, regime, z = _two_tier_baseline(
        long_values, short_values, spec.regime_z_threshold,
    )

    # Hard bounds — anything outside is corruption.
    if not (spec.min_value <= new_value <= spec.max_value):
        return {
            "flag": spec.flag_name, "action": "skip-out-of-bounds",
            "new_value": new_value,
            "bounds": (spec.min_value, spec.max_value),
        }

    # Step cap — never move more than max_step from current effective value
    # (which may be the existing override or the source-code default).
    current = _ov.get_override(spec.sport, spec.flag_name)
    if current is None:
        current = spec.default
    delta = new_value - float(current)
    if abs(delta) > spec.max_step:
        new_value = float(current) + (spec.max_step if delta > 0 else -spec.max_step)
        regime = f"{regime}-stepcap"

    # If the change is tiny vs current (less than ~0.1 of the long SE),
    # don't bother writing — saves the audit-log churn.
    long_mean, long_se, _ = _stats(long_values)
    if abs(new_value - float(current)) < max(0.05, long_se * 0.1):
        return {
            "flag": spec.flag_name, "action": "no-change",
            "current": float(current), "computed": new_value,
            "long_mean": long_mean, "regime": regime, "z": round(z, 2),
        }

    reason = (
        f"long={round(long_mean, 2)} (n={n_long}), "
        f"short={round(_stats(short_values)[0], 2)} (n={len(short_values)}), "
        f"z={round(z, 2)} -> {regime}"
    )
    _ov.apply_override(
        spec.sport, spec.flag_name, round(new_value, 2),
        reason=reason, n_samples=n_long,
        expiry_days=spec.expiry_days,
    )
    return {
        "flag": spec.flag_name, "action": "applied",
        "new_value": round(new_value, 2),
        "previous": float(current),
        "long_mean": round(long_mean, 2),
        "short_mean": round(_stats(short_values)[0], 2),
        "n_long": n_long, "n_short": len(short_values),
        "z": round(z, 2), "regime": regime,
    }


def update_all() -> list[dict]:
    """Run all registered baselines. Each returns its decision dict.
    Failures on one spec don't stop the others."""
    out = []
    for spec in BASELINES:
        try:
            out.append(update_baseline(spec))
        except Exception as e:
            logger.warning("Adaptive baseline %s failed: %s",
                           spec.flag_name, e, exc_info=True)
            out.append({"flag": spec.flag_name, "action": "error",
                        "error": str(e)})
    return out


if __name__ == "__main__":
    import json as _json
    print(_json.dumps(update_all(), indent=2, default=str))
