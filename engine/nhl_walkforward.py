"""Walk-forward calibration seeding for the NHL.

Replays the Poisson predictor against every historical final game in
date order, using only data available BEFORE that game. Aggregates
predicted-bucket → realized hit rate per (bet_type, bucket) cell so
``framework_calibration.calibrate("nhl", ...)`` can ship a leakage-
clean Bayesian shrinkage on top of (or instead of) the live-picks
empirical calibration.

Why a walk-forward when empirical_calibration already trains on
settled picks: empirical's sample only contains picks that fired
(cleared edge floor + juice wall). That's a selection-biased subset
of the model's prediction distribution. Walk-forward replays every
prediction the model would have produced, so the bucket maps reflect
the actual calibration of the factor signal at every conviction level.

Mirrors ``engine.hockey._calibrate.seed_calibration`` but uses the
NHL DB (``engine.nhl_db.get_conn()``) and the ``nhl_games`` schema
(``home_p1`` not ``home_q1``, ``season`` integer, etc).

Markets seeded (all leakage-clean from historical scores):
  ML     — Poisson grid P(home wins) vs realized winner
  SPREAD — Skellam tail at puck-line offsets vs realized margin
  TOTAL  — Poisson sum tail at line offsets vs realized total

Output: ``data/nhl_calibration.json`` in Format A
(``buckets[bet_type]`` keyed) — ``picks_core`` reads bet-type-aware
shrinkage. Backward-compatible ``ml`` dict is also written.

Usage::

    python -m engine.nhl_walkforward
"""
from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_OUT = Path(__file__).resolve().parent.parent / "data" / "nhl_calibration.json"

# Bucket grid matches engine.calibration_buckets / hockey framework.
_BUCKETS = [
    (0.30, 0.40),
    (0.40, 0.50),
    (0.50, 0.55),
    (0.55, 0.60),
    (0.60, 0.65),
    (0.65, 0.70),
    (0.70, 0.80),
    (0.80, 1.01),
]

# League constants. NHL averages 3.05 goals/team/game and home ice is
# worth ~0.15 goals — same numbers the live predictor uses.
_LEAGUE_AVG_GPG = 3.05
_HOME_BOOST = 0.15
_MAX_GOALS = 12

# Spread / total probe offsets. NHL puck lines are virtually always
# ±1.5 so probing at the canonical lines covers the live distribution.
_SPREAD_LINES = (-2.5, -1.5, -0.5, 0.5, 1.5, 2.5)
_TOTAL_OFFSETS = (-2.5, -1.5, -0.5, 0.5, 1.5, 2.5)


def _bucket_for(p: float) -> tuple[float, float] | None:
    for lo, hi in _BUCKETS:
        if lo <= p < hi:
            return lo, hi
    return None


def _poisson_pmf(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def _ml_home_prob(home_xg: float, away_xg: float) -> float:
    """P(home wins) splitting tie mass 50/50 — same convention the live
    NHL predictor uses for OT/SO games (no per-team tiebreaker model)."""
    p_home = p_away = p_tie = 0.0
    for h in range(_MAX_GOALS + 1):
        ph = _poisson_pmf(home_xg, h)
        for a in range(_MAX_GOALS + 1):
            joint = ph * _poisson_pmf(away_xg, a)
            if h > a:
                p_home += joint
            elif a > h:
                p_away += joint
            else:
                p_tie += joint
    if p_tie > 0:
        total = p_home + p_away
        if total > 0:
            p_home += p_tie * (p_home / total)
        else:
            p_home += p_tie / 2
    return p_home


def _spread_home_cover_prob(home_xg: float, away_xg: float,
                              line: float) -> float:
    p = 0.0
    for h in range(_MAX_GOALS + 1):
        ph = _poisson_pmf(home_xg, h)
        for a in range(_MAX_GOALS + 1):
            if (h - a) > line:
                p += ph * _poisson_pmf(away_xg, a)
    return p


def _total_over_prob(home_xg: float, away_xg: float, line: float) -> float:
    total_lam = home_xg + away_xg
    if total_lam <= 0:
        return 0.0
    p = 0.0
    for k in range(int(line) + 1, _MAX_GOALS * 2 + 1):
        p += _poisson_pmf(total_lam, k)
    return p


def _team_rates_as_of(conn, team_id: int, as_of_date: str,
                       season: int, min_games: int = 5
                       ) -> tuple[float, float, int]:
    """Compute (off_rate, def_rate, n_games) using only games this team
    played in ``season`` BEFORE ``as_of_date``. Returns league averages
    when sample < min_games."""
    rows = conn.execute(
        "SELECT home_team_id, away_team_id, home_score, away_score "
        "FROM nhl_games WHERE season = ? AND status = 'final' "
        "  AND date < ? "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "  AND (home_team_id = ? OR away_team_id = ?)",
        (season, as_of_date, team_id, team_id),
    ).fetchall()
    if len(rows) < min_games:
        return _LEAGUE_AVG_GPG, _LEAGUE_AVG_GPG, len(rows)
    scored = []; allowed = []
    for r in rows:
        if r["home_team_id"] == team_id:
            scored.append(r["home_score"]); allowed.append(r["away_score"])
        else:
            scored.append(r["away_score"]); allowed.append(r["home_score"])
    return (sum(scored) / len(scored),
             sum(allowed) / len(allowed),
             len(rows))


def seed_calibration() -> dict:
    """Walk every final NHL game in date order. For each: compute
    predicted ML/SPREAD/TOTAL using only prior-game data, bucket the
    predictions, persist + return the calibration dict."""
    from .nhl_db import get_conn
    conn = get_conn()

    games = conn.execute(
        "SELECT game_id, date, season, home_team_id, away_team_id, "
        "       home_score, away_score "
        "FROM nhl_games WHERE status = 'final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date ASC, game_id ASC"
    ).fetchall()

    buckets_ml = {b: {"n": 0, "wins": 0, "sum_p": 0.0,
                       "sum_brier": 0.0} for b in _BUCKETS}
    buckets_sp = {b: {"n": 0, "wins": 0, "sum_p": 0.0} for b in _BUCKETS}
    buckets_to = {b: {"n": 0, "wins": 0, "sum_p": 0.0} for b in _BUCKETS}

    total = 0
    skipped_thin = 0
    for g in games:
        h_off, h_def, h_n = _team_rates_as_of(
            conn, g["home_team_id"], g["date"], g["season"])
        a_off, a_def, a_n = _team_rates_as_of(
            conn, g["away_team_id"], g["date"], g["season"])
        if h_n < 5 or a_n < 5:
            skipped_thin += 1
            continue

        home_xg = (h_off * (a_def / _LEAGUE_AVG_GPG)) + _HOME_BOOST
        away_xg = a_off * (h_def / _LEAGUE_AVG_GPG)
        actual_margin = int(g["home_score"]) - int(g["away_score"])
        actual_total = int(g["home_score"]) + int(g["away_score"])

        # ── ML ──
        p_home = _ml_home_prob(home_xg, away_xg)
        outcome = 1 if actual_margin > 0 else 0
        prob = p_home if p_home >= 0.5 else (1 - p_home)
        side_outcome = outcome if p_home >= 0.5 else (1 - outcome)
        b = _bucket_for(prob)
        if b is not None:
            buckets_ml[b]["n"] += 1
            buckets_ml[b]["wins"] += side_outcome
            buckets_ml[b]["sum_p"] += prob
            buckets_ml[b]["sum_brier"] += (prob - side_outcome) ** 2

        # ── SPREAD ──
        for line in _SPREAD_LINES:
            p_home_cover = _spread_home_cover_prob(home_xg, away_xg, line)
            home_won_cover = 1 if actual_margin > line else 0
            if p_home_cover >= 0.5:
                sp, sw = p_home_cover, home_won_cover
            else:
                sp, sw = 1.0 - p_home_cover, 1 - home_won_cover
            bk = _bucket_for(sp)
            if bk is not None:
                buckets_sp[bk]["n"] += 1
                buckets_sp[bk]["wins"] += sw
                buckets_sp[bk]["sum_p"] += sp

        # ── TOTAL ──
        predicted_total = home_xg + away_xg
        center = round(predicted_total)
        for off in _TOTAL_OFFSETS:
            line = center + off
            if line < 0.5:
                continue
            p_over = _total_over_prob(home_xg, away_xg, line)
            over_won = 1 if actual_total > line else 0
            if p_over >= 0.5:
                sp, sw = p_over, over_won
            else:
                sp, sw = 1.0 - p_over, 1 - over_won
            bk = _bucket_for(sp)
            if bk is not None:
                buckets_to[bk]["n"] += 1
                buckets_to[bk]["wins"] += sw
                buckets_to[bk]["sum_p"] += sp
        total += 1

    def _fmt(buckets_dict, include_brier: bool = False) -> list[dict]:
        rows = []
        for lo, hi in _BUCKETS:
            v = buckets_dict[(lo, hi)]
            n = v["n"]
            entry = {
                "bucket": [lo, hi],
                "n": n,
                "avg_pred": round(v["sum_p"] / n, 4) if n else None,
                "realized_wr": round(v["wins"] / n, 4) if n else None,
            }
            if include_brier:
                entry["brier"] = (round(v["sum_brier"] / n, 4)
                                  if n else None)
            rows.append(entry)
        return rows

    out: dict = {
        "league": "nhl",
        "n_games": total,
        "skipped_thin": skipped_thin,
        "fitted_at": datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"),
        "buckets": {
            "ML":     _fmt(buckets_ml, include_brier=True),
            "SPREAD": _fmt(buckets_sp),
            "TOTAL":  _fmt(buckets_to),
        },
        # Format B kept for legacy callers.
        "ml": {
            f"{lo:.2f}-{hi:.2f}": {
                "n": v["n"],
                "wins": v["wins"],
                "hit_rate": round(v["wins"] / v["n"], 4) if v["n"] else None,
                "avg_pred": round(v["sum_p"] / v["n"], 4) if v["n"] else None,
                "brier": round(v["sum_brier"] / v["n"], 4) if v["n"] else None,
            }
            for (lo, hi), v in buckets_ml.items()
        },
    }

    _OUT.parent.mkdir(parents=True, exist_ok=True)
    _OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
    logger.info("[nhl] walk-forward calibration: %d games scored, "
                "%d skipped (thin), wrote %s",
                total, skipped_thin, _OUT)
    return out


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.nhl_walkforward")
    ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    res = seed_calibration()
    print(f"\n=== NHL walk-forward calibration ===")
    print(f"Games scored: {res['n_games']}, skipped (thin): "
          f"{res['skipped_thin']}")
    print(f"\nBucket   |  n  | hit_rate | avg_pred | Brier")
    print("-" * 50)
    for bk, v in res["ml"].items():
        if v["n"] == 0:
            continue
        print(f"{bk:<10} {v['n']:>4}  {v['hit_rate']!s:<8}  "
              f"{v['avg_pred']!s:<8}  {v['brier']}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
