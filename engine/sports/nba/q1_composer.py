"""NBA Q1 player composer (Stage 0 of #172).

Predicts Q1 markets (home_win, total_points) from per-starter recent
scoring instead of team-level Q1 aggregates.

Why Q1 specifically: only the top 5 starters play full Q1 (no rotation,
no foul-out, no garbage time). That makes it the cleanest player-driven
market in the codebase. If player aggregation EVER beats team aggregation,
it should be here.

Architecture (Stage 0 minimal):
1. Identify each team's 5 likely starters from recent games where
   `starter=1` flag is set in stats_json.
2. For each starter, mean total points over last 10 games (PIT-correct).
3. Q1 contribution per starter = total_pts × Q1_SHARE (~0.27 = 12 / 45 typical NBA min split).
4. Team Q1 = sum of 5 starter contributions.
5. Q1 total = home + away. Q1 margin → sigmoid → home_win prob.

Data limitations called out:
- No per-quarter stats in player_game_logs (only full-game pts) → Q1
  share is a single multiplier, not modeled per player. A player who
  starts hot in Q1 vs starts cold isn't differentiated.
- Pace adjustment ignored — high-pace teams have higher Q1 totals
  systematically. Stage 0 uses league-mean pace.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timedelta
from ..._tz import et_today_str

logger = logging.getLogger(__name__)


# Empirical fraction of total points scored in Q1 (~12 min / 48 min,
# adjusted for warm-up effect — actual is closer to 0.25).
Q1_SHARE = 0.255

# Sigma on Q1 margin (used for sigmoid → win prob). Empirical ~8.6.
Q1_MARGIN_STD = 8.6

# League-average Q1 total (if no player data available).
LEAGUE_AVG_Q1_TOTAL = 58.0


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _team_starter_q1_contribution(conn, team_id: int, cutoff_date: str,
                                   lookback_days: int = 30) -> float:
    """Sum of (mean total pts × Q1_SHARE) across the team's 5 most
    recent starters. Returns expected Q1 team points."""
    since = (datetime.fromisoformat(cutoff_date) -
             timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT player_id, stats_json FROM player_game_logs "
        "WHERE team_id = ? AND date >= ? AND date < ?",
        (team_id, since, cutoff_date),
    ).fetchall()
    pts_by_player: dict[int, list[float]] = {}
    starts_by_player: dict[int, int] = {}
    for r in rows:
        try:
            stats = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        pts = stats.get("pts")
        if pts is None:
            continue
        pts_by_player.setdefault(r["player_id"], []).append(float(pts))
        if stats.get("starter"):
            starts_by_player[r["player_id"]] = starts_by_player.get(r["player_id"], 0) + 1

    if not pts_by_player:
        return LEAGUE_AVG_Q1_TOTAL / 2.0  # split 50/50

    # Top 5 by start count — identifies the actual starting lineup.
    # If fewer than 5 confirmed starters, fall back to top-5 by games played.
    ranked = sorted(starts_by_player.items(), key=lambda kv: -kv[1])
    starters = [pid for pid, _ in ranked[:5]]
    if len(starters) < 5:
        # Fill with top-played-by-games
        played = sorted(pts_by_player.items(), key=lambda kv: -len(kv[1]))
        for pid, _ in played:
            if pid not in starters:
                starters.append(pid)
            if len(starters) >= 5:
                break

    total = 0.0
    for pid in starters[:5]:
        ps = pts_by_player.get(pid, [])
        if ps:
            total += (sum(ps) / len(ps)) * Q1_SHARE
    if len(starters) < 5 and starters:
        total = total * (5.0 / len(starters))
    return total


def predict_q1_composer(home_team_id: int, away_team_id: int,
                         cutoff_date: str | None = None) -> dict:
    """Compose Q1 prediction from per-starter recent contributions."""
    from .db import get_conn
    conn = get_conn()
    cutoff = cutoff_date or et_today_str()

    home_q1 = _team_starter_q1_contribution(conn, home_team_id, cutoff)
    away_q1 = _team_starter_q1_contribution(conn, away_team_id, cutoff)

    # Small home-court Q1 boost (~+0.7 pts empirically — see calibration)
    home_q1 += 0.35
    away_q1 -= 0.35

    margin = home_q1 - away_q1
    home_win = _normal_cdf(margin / Q1_MARGIN_STD)

    return {
        "q1_home_win":    home_win,
        "q1_total_points": home_q1 + away_q1,
        "home_q1_expected": round(home_q1, 2),
        "away_q1_expected": round(away_q1, 2),
    }


__all__ = ["predict_q1_composer"]
