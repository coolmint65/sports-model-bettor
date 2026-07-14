"""MLB team-level player composer (Stage 0 of #171).

Goal: predict team runs and total runs by aggregating per-player
recent contributions, instead of using team-level aggregates as the
factor model does. Tests whether per-player resolution actually beats
team-level for MLB.

Stage 0 minimal-viable architecture:
1. **Batting**: pull last-30-day game logs for each team's recent
   batters; estimate per-batter runs/PA × expected PAs/game.
2. **Pitching**: starter's last-30-day ER/IP × ~6 IP per start;
   bullpen filled with aggregate ERA × ~3 IP (per locked decision).
3. **Compose**: team_runs = batter_contribution + opponent_pitching_drag.

Markets predicted:
- ``home_win`` (sigmoid on margin)
- ``total``   (expected total runs)

PIT correctness: every aggregate uses ``date < cutoff_date``. No leak.

This is intentionally simple — the question Stage 0 answers is
"does per-player aggregation beat team aggregation enough to justify
the build?" If the simple composer loses, more sophistication won't
save it; if it wins, Stage 1 fleshes out PA-level sampling, lineup
resolution, foul-trouble-equivalent edge cases, etc.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timedelta
from typing import Any
from ..._tz import et_today_str

logger = logging.getLogger(__name__)


# League-average reference points (from Sackmann era; recompute if
# regimes shift). Used as cold-start priors when a player's sample
# is too thin.
LEAGUE_AVG_RUNS_PER_GAME = 4.5
LEAGUE_AVG_ERA = 4.40
TYPICAL_STARTER_IP = 5.5
TYPICAL_BULLPEN_IP = 3.5

# Variance constants (for sigmoid on margin → home_win prob).
# Empirical sigma of MLB game margin is ~3.6 runs.
MARGIN_STD = 3.6


# ── Per-team aggregates ────────────────────────────────────────

def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _team_recent_batter_runs(conn, team_id: int, cutoff_date: str,
                              lookback_days: int = 30) -> float:
    """Sum of (mean runs/game per batter) across the team's 9 most
    recent active batters. Uses each batter's last-N-day mean — so a
    cold batter contributes less than a hot one even at the same
    season-aggregate."""
    since = (datetime.fromisoformat(cutoff_date) -
             timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT player_id, stats_json FROM player_game_logs "
        "WHERE team_id = ? AND date >= ? AND date < ? ",
        (team_id, since, cutoff_date),
    ).fetchall()
    runs_by_player: dict[int, list[float]] = {}
    pa_by_player: dict[int, int] = {}
    for r in rows:
        try:
            stats = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        pa = stats.get("pa", 0)
        if not isinstance(pa, (int, float)) or pa <= 0:
            continue
        runs = stats.get("r", 0)
        runs_by_player.setdefault(r["player_id"], []).append(float(runs or 0))
        pa_by_player[r["player_id"]] = pa_by_player.get(r["player_id"], 0) + int(pa)

    if not runs_by_player:
        return LEAGUE_AVG_RUNS_PER_GAME

    # Top-9 by PA volume — proxy for "regular" lineup
    ranked = sorted(pa_by_player.items(), key=lambda kv: -kv[1])
    top9 = [pid for pid, _ in ranked[:9]]
    total = 0.0
    for pid in top9:
        rs = runs_by_player.get(pid, [])
        if rs:
            total += sum(rs) / len(rs)
    # If fewer than 9 unique active batters, scale to 9
    if len(top9) < 9 and len(top9) > 0:
        total = total * (9.0 / len(top9))
    return total


def _starter_recent_era(conn, pitcher_id: int | None,
                         cutoff_date: str,
                         lookback_days: int = 60) -> float:
    """Starting pitcher's recent ER/IP rate. Returns league avg ERA
    when pitcher_id is missing or pitcher has no recent starts."""
    if pitcher_id is None:
        return LEAGUE_AVG_ERA
    since = (datetime.fromisoformat(cutoff_date) -
             timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT stats_json FROM player_game_logs "
        "WHERE player_id = ? AND date >= ? AND date < ? ",
        (int(pitcher_id), since, cutoff_date),
    ).fetchall()
    total_outs = 0
    total_er = 0.0
    for r in rows:
        try:
            stats = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        outs = stats.get("outs", 0)
        if not outs:
            continue
        er = stats.get("er", 0) or 0
        total_outs += int(outs)
        total_er += float(er)
    if total_outs < 30:
        return LEAGUE_AVG_ERA
    ip = total_outs / 3.0
    return (total_er / ip) * 9.0


# ── Public API ────────────────────────────────────────────────

def predict_team_composer(home_team_id: int, away_team_id: int,
                          home_pitcher_id: int | None,
                          away_pitcher_id: int | None,
                          cutoff_date: str | None = None) -> dict:
    """Compose team-runs prediction from per-batter and per-starter
    aggregates. Returns home_win prob + expected total."""
    from ...db import get_conn
    conn = get_conn()
    cutoff = cutoff_date or et_today_str()

    # Per-team batter contribution (independent of opponent for
    # this minimal composer — Stage 1 would couple batter quality
    # with opposing pitcher).
    home_offense = _team_recent_batter_runs(conn, home_team_id, cutoff)
    away_offense = _team_recent_batter_runs(conn, away_team_id, cutoff)

    # Per-team starter ERA — quality of opposing pitcher reduces the
    # offense's expected output. Linear adjustment: every 1.0 ERA
    # above league mean reduces opposing offense by ~0.4 runs.
    home_starter_era = _starter_recent_era(conn, home_pitcher_id, cutoff)
    away_starter_era = _starter_recent_era(conn, away_pitcher_id, cutoff)

    # Composer formula. Offense-vs-pitching balance:
    home_runs_exp = home_offense - 0.4 * (away_starter_era - LEAGUE_AVG_ERA)
    away_runs_exp = away_offense - 0.4 * (home_starter_era - LEAGUE_AVG_ERA)
    # Home-field advantage is small in MLB (~0.1 runs)
    home_runs_exp += 0.1

    # Clamp to realistic range (1-12 runs/team is normal)
    home_runs_exp = max(1.5, min(11.0, home_runs_exp))
    away_runs_exp = max(1.5, min(11.0, away_runs_exp))

    margin = home_runs_exp - away_runs_exp
    home_win = _normal_cdf(margin / MARGIN_STD)

    return {
        "home_win":          home_win,
        "total":             home_runs_exp + away_runs_exp,
        "home_runs_expected": round(home_runs_exp, 2),
        "away_runs_expected": round(away_runs_exp, 2),
        "factors": {
            "home_offense":     round(home_offense, 2),
            "away_offense":     round(away_offense, 2),
            "home_starter_era": round(home_starter_era, 2),
            "away_starter_era": round(away_starter_era, 2),
        },
    }


__all__ = ["predict_team_composer"]
