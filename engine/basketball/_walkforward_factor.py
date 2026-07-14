"""Factor-only walk-forward calibration for basketball-framework leagues.

Mirrors `engine.hockey._calibrate.seed_calibration` for basketball.
Doesn't require a trained GBM — uses the league's calibrated factor
constants (home_boost, margin_std, total_std, league_avg_total) to
predict ml_home for each historical game using only data BEFORE that
game, then aggregates predicted-bucket → realized hit rate.

Existing `engine.basketball._walkforward.seed_calibration` requires a
trained GBM (B2). This function is the GBM-free fallback so leagues
without GBM (NCAAM, Euroleague, FIBA Champions, etc) still get a
seeded empirical-calibration table.

Output: ``data/basketball/<league>_calibration.json`` — same shape as
the GBM-based seeder so downstream readers don't branch.
"""
from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone

from ._config import LEAGUE_REGISTRY
from ._db import get_conn, games_table

logger = logging.getLogger(__name__)

_OUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "basketball"

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


def _bucket_for(p: float) -> tuple[float, float] | None:
    for lo, hi in _BUCKETS:
        if lo <= p < hi:
            return lo, hi
    return None


def _team_rates_as_of(conn, g_tbl: str, team_id: int, as_of_date: str,
                       season: int, league_avg_ppg: float,
                       min_games: int = 8) -> tuple[float, float, int]:
    """Per-team off/def points-per-game using only games this team
    played in the season BEFORE ``as_of_date``."""
    rows = conn.execute(
        f"SELECT home_team_id, away_team_id, home_score, away_score "
        f"FROM {g_tbl} WHERE season = ? AND status = 'final' "
        f"  AND date < ? "
        f"  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        f"  AND (home_team_id = ? OR away_team_id = ?)",
        (season, as_of_date, team_id, team_id),
    ).fetchall()
    if len(rows) < min_games:
        return league_avg_ppg, league_avg_ppg, len(rows)
    scored = []; allowed = []
    for r in rows:
        if r["home_team_id"] == team_id:
            scored.append(r["home_score"]); allowed.append(r["away_score"])
        else:
            scored.append(r["away_score"]); allowed.append(r["home_score"])
    return (sum(scored) / len(scored),
             sum(allowed) / len(allowed),
             len(rows))


def _ml_home_prob_normal(margin_mu: float, margin_std: float) -> float:
    """Closed-form P(home margin > 0) under a Normal margin model.
    Faster + more accurate than discrete grid sums for basketball where
    scores are integer-resolution but the margin distribution is
    well-approximated by a Normal."""
    if margin_std <= 0:
        return 1.0 if margin_mu > 0 else (0.0 if margin_mu < 0 else 0.5)
    z = margin_mu / margin_std
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def seed_calibration(league: str) -> dict:
    """Walk every final game in date order. Each prediction uses only
    games strictly before its date. Persist + return calibration dict."""
    if league not in LEAGUE_REGISTRY:
        raise KeyError(f"Unknown league {league!r}")
    if league == "nba":
        raise ValueError("NBA uses its own /api/nba/* calibration path")

    cfg = LEAGUE_REGISTRY[league]
    league_avg_total = cfg.get("league_avg_total") or 160.0
    league_avg_ppg = league_avg_total / 2
    home_boost = cfg.get("home_boost") or 2.5
    margin_std = cfg.get("margin_std") or 12.0

    conn = get_conn(league)
    g_tbl = games_table(league)
    games = conn.execute(
        f"SELECT date, season, home_team_id, away_team_id, "
        f"       home_score, away_score "
        f"FROM {g_tbl} WHERE status = 'final' "
        f"  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        f"ORDER BY date ASC"
    ).fetchall()

    buckets: dict[tuple[float, float], dict] = {b: {"n": 0, "wins": 0,
                                                       "sum_p": 0.0,
                                                       "sum_brier": 0.0}
                                                    for b in _BUCKETS}
    skipped_thin = 0
    total = 0
    for g in games:
        h_off, h_def, h_n = _team_rates_as_of(
            conn, g_tbl, g["home_team_id"], g["date"], g["season"],
            league_avg_ppg,
        )
        a_off, a_def, a_n = _team_rates_as_of(
            conn, g_tbl, g["away_team_id"], g["date"], g["season"],
            league_avg_ppg,
        )
        if h_n < 8 or a_n < 8:
            skipped_thin += 1
            continue
        # Expected points: blend offense vs opponent defense around mean.
        home_xp = (h_off + a_def) / 2 + home_boost / 2
        away_xp = (a_off + h_def) / 2 - home_boost / 2
        margin_mu = home_xp - away_xp
        p_home = _ml_home_prob_normal(margin_mu, margin_std)
        outcome = 1 if g["home_score"] > g["away_score"] else 0
        prob = p_home if p_home >= 0.5 else (1 - p_home)
        side_outcome = outcome if p_home >= 0.5 else (1 - outcome)
        b = _bucket_for(prob)
        if b is None:
            continue
        buckets[b]["n"] += 1
        buckets[b]["wins"] += side_outcome
        buckets[b]["sum_p"] += prob
        buckets[b]["sum_brier"] += (prob - side_outcome) ** 2
        total += 1

    out: dict = {
        "league": league,
        "method": "factor_walk_forward",
        "n_games": total,
        "skipped_thin": skipped_thin,
        "fitted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ml": {
            f"{lo:.2f}-{hi:.2f}": {
                "n": v["n"], "wins": v["wins"],
                "hit_rate": round(v["wins"] / v["n"], 4) if v["n"] else None,
                "avg_pred": round(v["sum_p"] / v["n"], 4) if v["n"] else None,
                "brier": round(v["sum_brier"] / v["n"], 4) if v["n"] else None,
            }
            for (lo, hi), v in buckets.items()
        },
    }

    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _OUT_DIR / f"{league}_calibration.json"
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    logger.info(
        "[%s] factor walk-forward calibration: %d games scored, "
        "%d skipped (thin), wrote %s", league, total, skipped_thin, out_path,
    )
    return out


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.basketball._walkforward_factor")
    ap.add_argument("league")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    res = seed_calibration(args.league)
    print(f"\n=== {args.league.upper()} walk-forward calibration ===")
    print(f"Games scored: {res['n_games']}, skipped (thin): {res['skipped_thin']}")
    print(f"\nBucket     |   n   | hit_rate | avg_pred | Brier")
    print("-" * 55)
    for bk, v in res["ml"].items():
        if v["n"] == 0: continue
        print(f"{bk:<11} {v['n']:>5}    {v['hit_rate']!s:<8}  "
              f"{v['avg_pred']!s:<8}  {v['brier']}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
