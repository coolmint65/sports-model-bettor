"""
Replay calibration for player-prop MC (Phase 2j-prep).

For every (player, game) row in ``player_game_logs``, replays the
picker's question — "what model_prob would the MC have given for
Over/Under line L on this player+stat going into this game?" — using
ONLY the player's data from BEFORE that game (no future-data leak).
Then compares to the actual observed value.

Buckets predicted probability into 5%-wide bins (50–55, 55–60, …,
80–85, 85+) and reports observed hit rate per bin per stat. A
well-calibrated model has observed ≈ predicted in every bucket. A
consistent gap (e.g. "predicted 70% but observed 55% in NegBin
stats") flags miscalibration the picker should account for.

Without this, we can't distinguish a real +15% edge pick from one
where the model is just systematically overconfident at high
predictions. The derivative backtest caught two losing markets
exactly this way (MLB Inning BTS, Inning Total — both posted heavy
negative ROI despite "edge" projections).

Use as a one-shot diagnostic, not a runtime gate. Run results inform
which bet_types deserve a per-bet-type shrinkage factor in the
picker before money goes live.
"""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import numpy as np

from .player_props_db import _conn_for
from .distribution_fit import get_distribution
from .mlb_player_mc import _sample, prob_over
from ._tz import et_today_str

logger = logging.getLogger(__name__)


# Stat lists per sport — must match the picker's bet-type stat keys.
_SPORT_STATS = {
    "mlb": ["k_p", "bb_p", "outs", "er", "h_allowed",
            "hr", "h", "tb", "rbi", "r", "sb", "bb_b", "k_b"],
    "nba": ["pts", "reb", "ast", "tpm", "ftm", "to", "stl", "blk"],
    "nhl": ["g", "a", "sog", "hits", "blocks", "saves", "shots_against", "ga"],
    # WNBA shares NBA's prop stat set — same MC modules underneath.
    "wnba": ["pts", "reb", "ast", "tpm", "ftm", "to", "stl", "blk"],
}

# Required role gate per stat (mirrors the per-sport MC modules so
# replay observations match what the picker would actually use).
_ROLE_GATE = {
    "mlb": {
        "pitcher": {"k_p", "bb_p", "outs", "er", "h_allowed"},
        "batter":  {"hr", "h", "tb", "rbi", "r", "sb", "bb_b", "k_b"},
    },
}


def _is_present_mlb(stats: dict, stat_key: str) -> bool:
    if stat_key not in stats:
        return False
    if stat_key in _ROLE_GATE["mlb"]["pitcher"]:
        return stats.get("outs", 0) > 0
    if stat_key in _ROLE_GATE["mlb"]["batter"]:
        return stats.get("pa", 0) > 0
    return True


def _is_present_nba(stats: dict, stat_key: str) -> bool:
    return stat_key in stats and stats.get("min", 0) > 0


def _is_present_nhl(stats: dict, stat_key: str) -> bool:
    return stat_key in stats and stats.get("toi_min", 0) > 0


_PRESENCE = {
    "mlb": _is_present_mlb,
    "nba": _is_present_nba,
    "nhl": _is_present_nhl,
    # WNBA presence rule = NBA's (minutes-gated). Same player-game shape.
    "wnba": _is_present_nba,
}


def _player_history(sport: str, player_id: int, stat_key: str,
                    before_date: str, lookback_days: int = 60) -> np.ndarray:
    """Pull a player's observations for a stat from rows STRICTLY
    before ``before_date``. Equivalent to the rolling window the
    live MC would have at pick-time on date D-1."""
    conn = _conn_for(sport)
    since = (datetime.strptime(before_date, "%Y-%m-%d") -
             timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT stats_json FROM player_game_logs "
        "WHERE player_id = ? AND date >= ? AND date < ? "
        "ORDER BY date",
        (int(player_id), since, before_date),
    ).fetchall()
    presence = _PRESENCE[sport]
    out = []
    for r in rows:
        try:
            stats = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        if not presence(stats, stat_key):
            continue
        try:
            out.append(float(stats[stat_key]))
        except (TypeError, ValueError):
            continue
    return np.asarray(out, dtype=float)


def _natural_lines(observations: np.ndarray) -> list[float]:
    """Return a small set of natural HR-style half-point lines around
    where the player's distribution actually lives. Bracketing the
    median ±2 captures the lines an actual prop bet would target
    without enumerating every theoretical threshold."""
    if len(observations) == 0:
        return []
    median = float(np.median(observations))
    base = math.floor(median)
    # Half-point lines so over/under semantics never push.
    return [base + 0.5 + d for d in (-2, -1, 0, 1, 2) if base + 0.5 + d >= 0.5]


def calibrate_sport(sport: str, *,
                    days: int = 30,
                    n_sims: int = 5_000,
                    min_history: int = 5,
                    min_samples_per_stat: int = 100) -> dict:
    """Replay calibration over the last ``days`` of ``sport`` game
    logs. Returns ``{stat: {bucket: {n, observed_rate, mean_predicted}}}``
    plus aggregate stats per bet_type.

    Bucket layout: 50-55, 55-60, 60-65, 65-70, 70-75, 75-80, 80-85,
    85+. Below 50% gets bucketed as Under-side (the picker would have
    taken Under, so we test under hit rate symmetrically).
    """
    if sport not in _SPORT_STATS:
        raise ValueError(f"unknown sport {sport}")
    today = et_today_str()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    conn = _conn_for(sport)
    presence = _PRESENCE[sport]

    out: dict[str, dict] = {}
    rng = np.random.default_rng(20260426)

    for stat_key in _SPORT_STATS[sport]:
        dist = get_distribution(sport, stat_key)
        if dist is None:
            continue
        rows = conn.execute(
            "SELECT player_id, date, stats_json FROM player_game_logs "
            "WHERE date >= ? AND date <= ? "
            "ORDER BY date",
            (cutoff, today),
        ).fetchall()
        # Bucket: prob_bin -> [list of bool outcomes]
        buckets: dict[str, list[tuple[float, bool]]] = defaultdict(list)
        for r in rows:
            try:
                stats = json.loads(r["stats_json"] or "{}")
            except (TypeError, ValueError):
                continue
            if not presence(stats, stat_key):
                continue
            try:
                actual = float(stats[stat_key])
            except (TypeError, ValueError):
                continue
            history = _player_history(sport, int(r["player_id"]),
                                       stat_key, r["date"])
            if len(history) < min_history:
                continue
            samples = np.sort(_sample(history, dist, n_sims, rng))
            for line in _natural_lines(history):
                p_over = prob_over(samples, line)
                # Picker would take whichever side has prob > 50%.
                # Test that side's actual hit rate.
                if p_over >= 0.5:
                    pred = p_over
                    hit = actual > line
                else:
                    pred = 1.0 - p_over
                    hit = actual < line
                bucket = _bucket_for(pred)
                buckets[bucket].append((pred, hit))
        # Summarize buckets.
        summary = {}
        total_n = 0
        for bucket, entries in sorted(buckets.items()):
            if not entries:
                continue
            n = len(entries)
            mean_pred = float(np.mean([p for p, _ in entries]))
            obs_rate = float(np.mean([1.0 if h else 0.0 for _, h in entries]))
            summary[bucket] = {
                "n": n,
                "mean_predicted": round(mean_pred, 3),
                "observed_rate": round(obs_rate, 3),
                "delta": round(obs_rate - mean_pred, 3),
            }
            total_n += n
        if total_n < min_samples_per_stat:
            summary["_warning"] = f"insufficient sample (n={total_n})"
        out[stat_key] = summary
    return out


def _bucket_for(p: float) -> str:
    """Bucket a probability into a 5%-wide band starting at 50%."""
    if p < 0.50:
        return "<50"  # shouldn't happen post-flip but defensive
    if p >= 0.85:
        return "85+"
    lo = int(p * 100) // 5 * 5
    return f"{lo}-{lo+5}"


def shrinkage_recommendation(calibration: dict) -> dict:
    """For each stat, compute a recommended multiplier on (model_prob
    - 0.5) that would close the average overconfidence gap.

    If observed_rate consistently lags mean_predicted by 8pp at high
    buckets, the recommended shrink is 0.85 (model_prob shifted 15%
    toward 50%). When observed matches predicted, shrink stays 1.0.
    """
    recs: dict[str, dict] = {}
    for stat, buckets in calibration.items():
        # Use weighted average delta across buckets >= 60%.
        deltas = []
        weights = []
        for bucket, info in buckets.items():
            if not isinstance(info, dict) or "delta" not in info:
                continue
            try:
                lo = int(bucket.split("-")[0]) if "-" in bucket else 85
            except ValueError:
                continue
            if lo < 60:
                continue
            deltas.append(info["delta"])
            weights.append(info["n"])
        if not deltas:
            recs[stat] = {"shrink": 1.0, "note": "insufficient data"}
            continue
        avg_delta = float(np.average(deltas, weights=weights))
        # Negative avg_delta means model overpredicts. Shrink toward
        # 0.5 by an amount proportional to the gap.
        if avg_delta < -0.03:
            shrink = max(0.5, 1.0 + avg_delta * 2)  # clamp
            note = f"overconfident by {-avg_delta*100:.1f}pp on avg → shrink {shrink:.2f}"
        elif avg_delta > 0.03:
            shrink = 1.0  # underconfident is fine, no shrink needed
            note = f"underconfident by {avg_delta*100:.1f}pp (no adjustment)"
        else:
            shrink = 1.0
            note = "well-calibrated"
        recs[stat] = {"shrink": round(shrink, 3),
                      "avg_delta": round(avg_delta, 3),
                      "note": note}
    return recs


def refresh_shrinkage(sport: str, *, days: int = 365,
                      n_sims: int = 5_000,
                      min_history: int = 5,
                      min_samples_per_stat: int = 100) -> dict:
    """Rebuild the persisted shrinkage table for ``sport`` from recent
    log data.

    Pipeline: ``calibrate_sport`` (replay) → ``shrinkage_recommendation``
    (per-stat avg_delta → multiplier) → ``save_learned_shrinkage``
    (write to ``data/prop_shrinkage.json``). The final write triggers
    a reload on the in-process shrinkage cache so the live picker
    picks up the new multipliers without a server restart.

    Returns ``{sport, days, recs, path}`` for diagnostic logging.

    Default ``days=365`` mirrors the long-window calibration the
    hardcoded ``_PROB_SHRINK`` table was last tuned against — short
    windows (30d) over-shrink stats whose recent variance was a streak,
    not a structural bias. Override when you specifically want a recent
    snapshot for a slate-by-slate experiment.

    Model-cutoff note: the replay walks raw observations from
    ``player_game_logs`` and runs them through the *current* MC code,
    so a longer ``days`` window doesn't drag in old-picker behaviour
    — only old game stats. Stats like batter HR or NBA points don't
    structurally shift across the picker's lifetime, so a 365-day
    window is the right tradeoff for sample size vs recency.
    """
    from .distribution_fit import save_learned_shrinkage
    cal = calibrate_sport(sport, days=days, n_sims=n_sims,
                           min_history=min_history,
                           min_samples_per_stat=min_samples_per_stat)
    recs = shrinkage_recommendation(cal)
    path = save_learned_shrinkage(sport, recs)
    logger.info("refresh_shrinkage[%s]: %d stat recommendations written → %s",
                sport, sum(1 for _ in recs), path)
    return {"sport": sport, "days": days, "recs": recs, "path": path}


__all__ = ["calibrate_sport", "shrinkage_recommendation",
           "refresh_shrinkage"]


def _cli(argv: list[str] | None = None) -> int:
    """Lightweight CLI for cron / sync — runs ``refresh_shrinkage`` and
    prints a one-line summary per sport. Bigger diagnostic tables live
    in ``engine.props_backtest``; this entry point exists so the sync
    scripts have a single command that just refreshes the persisted
    multipliers without the surrounding report verbosity."""
    import argparse
    ap = argparse.ArgumentParser(prog="engine.player_props_calibration")
    ap.add_argument("--sport", choices=_SPORT_STATS.keys(),
                    help="Limit to one sport. Default: all three.")
    ap.add_argument("--days", type=int, default=365,
                    help="Calibration window in days (default 365).")
    ap.add_argument("--n-sims", type=int, default=5000,
                    help="MC simulations per (player, game) replay.")
    args = ap.parse_args(argv)
    sports = (args.sport,) if args.sport else tuple(_SPORT_STATS.keys())
    rc = 0
    for sport in sports:
        try:
            res = refresh_shrinkage(sport, days=args.days, n_sims=args.n_sims)
            adjusted = sum(1 for v in res["recs"].values()
                           if isinstance(v, dict) and v.get("shrink", 1.0) < 1.0)
            print(f"[{sport}] {adjusted} stat(s) shrunk -> {res['path']}")
        except Exception as e:
            print(f"[{sport}] FAILED: {e}")
            rc = 1
    return rc


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
