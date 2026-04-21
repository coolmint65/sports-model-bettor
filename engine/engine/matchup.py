"""
Team-vs-team matchup analysis and contextual interaction model.

Two components:

1. H2H Historical: How have these two teams performed against each
   other historically? Win rates, scoring patterns, trends.

2. Contextual Interaction: Given TODAY's specific conditions for both
   teams, how do their strengths/weaknesses interact? This isn't just
   stacking independent factors — it identifies specific exploits:
   - RH-heavy lineup vs LHP who struggles against righties
   - High-K pitcher vs lineup that strikes out a lot (double effect)
   - Fast team vs slow pitcher (pace mismatch)
   - Good bullpen vs bad late-inning offense (compounds late)
"""

import json
import logging
import math
from datetime import datetime, timedelta

from .db import get_conn
from .pit_stats import compute_team_stats_at_date, compute_pitcher_stats_at_date
from .team_calibration import get_team_adjustment

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# PART 1: H2H Historical
# ═══════════════════════════════════════════════════════════════

def get_h2h_history(team_a_id: int, team_b_id: int,
                     seasons: int = 3) -> dict:
    """
    Get historical head-to-head record between two teams.
    Looks back N seasons.
    """
    conn = get_conn()
    yr = datetime.now().year
    start_year = yr - seasons

    games = conn.execute("""
        SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.mlb_id
        JOIN teams at ON g.away_team_id = at.mlb_id
        WHERE ((g.home_team_id = ? AND g.away_team_id = ?)
            OR (g.home_team_id = ? AND g.away_team_id = ?))
          AND g.status = 'final' AND g.season >= ?
          AND g.home_score IS NOT NULL
        ORDER BY g.date DESC
    """, (team_a_id, team_b_id, team_b_id, team_a_id, start_year)).fetchall()

    if not games:
        return {"games": 0}

    a_wins = 0
    b_wins = 0
    a_runs = 0
    b_runs = 0
    a_home_wins = 0
    a_home_games = 0
    recent = []  # Last 10

    for g in games:
        g = dict(g)
        a_is_home = g["home_team_id"] == team_a_id
        a_score = g["home_score"] if a_is_home else g["away_score"]
        b_score = g["away_score"] if a_is_home else g["home_score"]

        a_runs += a_score
        b_runs += b_score

        if a_score > b_score:
            a_wins += 1
        else:
            b_wins += 1

        if a_is_home:
            a_home_games += 1
            if a_score > b_score:
                a_home_wins += 1

        if len(recent) < 10:
            recent.append({
                "date": g["date"],
                "a_score": a_score,
                "b_score": b_score,
                "a_won": a_score > b_score,
                "venue": g.get("venue", ""),
            })

    n = len(games)
    return {
        "games": n,
        "a_wins": a_wins,
        "b_wins": b_wins,
        "a_win_pct": round(a_wins / n, 3) if n > 0 else 0.5,
        "a_runs_pg": round(a_runs / n, 2) if n > 0 else 0,
        "b_runs_pg": round(b_runs / n, 2) if n > 0 else 0,
        "a_home_wins": a_home_wins,
        "a_home_games": a_home_games,
        "recent": recent,
        "seasons_covered": seasons,
    }


def h2h_adjustment(team_a_id: int, team_b_id: int) -> float:
    """
    H2H adjustment factor for team A's scoring.
    Returns multiplier: >1 if A historically dominates B, <1 if not.
    Weighted by sample size.
    """
    h2h = get_h2h_history(team_a_id, team_b_id, seasons=2)

    if h2h["games"] < 4:
        return 1.0  # Not enough data

    # How does A score against B vs league average?
    a_rpg_vs_b = h2h["a_runs_pg"]
    # Compare to ~4.5 league avg
    ratio = a_rpg_vs_b / 4.5 if a_rpg_vs_b > 0 else 1.0

    # Weight by sample size (caps at 15 games)
    confidence = min(h2h["games"] / 15, 1.0)

    # Blend with neutral (1.0) based on confidence
    adjustment = 1.0 + (ratio - 1.0) * confidence * 0.3  # 30% max influence

    return max(0.85, min(1.20, adjustment))


# ═══════════════════════════════════════════════════════════════
# PART 2: Contextual Matchup Interaction
# ═══════════════════════════════════════════════════════════════

def compute_matchup_interaction(
    home_team_id: int, away_team_id: int,
    home_pitcher_id: int | None, away_pitcher_id: int | None,
    home_pit: dict | None, away_pit: dict | None,
    home_sp_pit: dict | None, away_sp_pit: dict | None,
    home_adj: dict | None, away_adj: dict | None,
    venue: str | None = None,
) -> dict:
    """
    Compute interaction effects between two specific teams TODAY.

    Instead of independent factors, this identifies compound effects:
    - When Team A's strength exploits Team B's weakness = amplified
    - When Team A's weakness meets Team B's strength = amplified
    - When both teams are similar = neutral

    Returns {
        home_interaction: float (multiplier on home runs),
        away_interaction: float (multiplier on away runs),
        insights: [str, ...] (human-readable matchup notes),
    }
    """
    conn = get_conn()
    season = datetime.now().year
    insights = []

    home_mult = 1.0
    away_mult = 1.0

    # ── 1. Pitching vs Hitting style interaction ──
    # High-K pitcher vs high-K lineup = compound effect (more Ks, fewer runs)
    # Low-K pitcher vs patient lineup = more balls in play
    if home_sp_pit and away_pit:
        sp_k_rate = home_sp_pit.get("first_inning_runs_per_start")  # proxy for dominance
        sp_era = home_sp_pit.get("era")
        team_rpg = away_pit.get("runs_pg", 4.5)

        if sp_era and team_rpg:
            # Pitcher quality vs offense quality interaction
            # Elite pitcher vs weak offense = runs suppressed MORE than sum of parts
            pitcher_quality = 4.1 / sp_era if sp_era > 0 else 1.0  # >1 = good pitcher
            offense_quality = team_rpg / 4.5  # >1 = good offense

            if pitcher_quality > 1.15 and offense_quality < 0.90:
                # Ace vs weak lineup — compound suppression
                interaction = 0.95
                away_mult *= interaction
                insights.append(f"Home SP dominates weak {_abbr(away_team_id)} lineup")
            elif pitcher_quality < 0.85 and offense_quality > 1.10:
                # Bad pitcher vs good lineup — compound boost
                interaction = 1.05
                away_mult *= interaction
                insights.append(f"{_abbr(away_team_id)} offense exploits weak home pitching")

    if away_sp_pit and home_pit:
        sp_era = away_sp_pit.get("era")
        team_rpg = home_pit.get("runs_pg", 4.5)

        if sp_era and team_rpg:
            pitcher_quality = 4.1 / sp_era if sp_era > 0 else 1.0
            offense_quality = team_rpg / 4.5

            if pitcher_quality > 1.15 and offense_quality < 0.90:
                interaction = 0.95
                home_mult *= interaction
                insights.append(f"Away SP dominates weak {_abbr(home_team_id)} lineup")
            elif pitcher_quality < 0.85 and offense_quality > 1.10:
                interaction = 1.05
                home_mult *= interaction
                insights.append(f"{_abbr(home_team_id)} offense exploits weak away pitching")

    # ── 2. Bullpen vs late-inning offense interaction ──
    if home_adj and away_adj:
        home_bp = home_adj.get("bullpen_factor", 1.0)
        away_bp = away_adj.get("bullpen_factor", 1.0)
        home_off = home_adj.get("offense_factor", 1.0)
        away_off = away_adj.get("offense_factor", 1.0)

        # Bad bullpen vs good offense = extra late runs
        if home_bp > 1.15 and away_off > 1.05:
            away_mult *= 1.03
            insights.append(f"Weak {_abbr(home_team_id)} bullpen vs strong {_abbr(away_team_id)} offense")
        if away_bp > 1.15 and home_off > 1.05:
            home_mult *= 1.03
            insights.append(f"Weak {_abbr(away_team_id)} bullpen vs strong {_abbr(home_team_id)} offense")

        # Elite bullpen vs weak offense = extra suppression
        if home_bp < 0.85 and away_off < 0.95:
            away_mult *= 0.97
            insights.append(f"Elite {_abbr(home_team_id)} bullpen shuts down {_abbr(away_team_id)}")
        if away_bp < 0.85 and home_off < 0.95:
            home_mult *= 0.97
            insights.append(f"Elite {_abbr(away_team_id)} bullpen shuts down {_abbr(home_team_id)}")

    # ── 3. Home/away strength vs opponent's road/home weakness ──
    if home_adj and away_adj:
        home_home_str = home_adj.get("home_factor", 1.0)
        away_away_str = away_adj.get("away_factor", 1.0)

        if home_home_str > 1.10 and away_away_str < 0.95:
            home_mult *= 1.03
            insights.append(f"{_abbr(home_team_id)} dominant at home vs {_abbr(away_team_id)} poor on road")
        elif away_away_str > 1.10 and home_home_str < 0.95:
            away_mult *= 1.03
            insights.append(f"{_abbr(away_team_id)} strong on road vs {_abbr(home_team_id)} weak at home")

    # ── 4. First inning interaction ──
    if home_adj and away_adj:
        home_fi = home_adj.get("first_inn_factor", 1.0)
        away_fi = away_adj.get("first_inn_factor", 1.0)

        if home_fi > 1.5 and away_fi > 1.5:
            insights.append("Both teams score early — YRFI likely")
        elif home_fi < 0.6 and away_fi < 0.6:
            insights.append("Both teams quiet in 1st — NRFI likely")

    # ── 5. H2H historical factor ──
    h2h_home = h2h_adjustment(home_team_id, away_team_id)
    h2h_away = h2h_adjustment(away_team_id, home_team_id)

    if h2h_home != 1.0:
        home_mult *= h2h_home
        if h2h_home > 1.05:
            insights.append(f"{_abbr(home_team_id)} historically dominates this matchup")
        elif h2h_home < 0.95:
            insights.append(f"{_abbr(home_team_id)} historically struggles in this matchup")

    if h2h_away != 1.0:
        away_mult *= h2h_away

    # Clamp
    home_mult = max(0.85, min(1.20, home_mult))
    away_mult = max(0.85, min(1.20, away_mult))

    return {
        "home_interaction": round(home_mult, 4),
        "away_interaction": round(away_mult, 4),
        "insights": insights,
    }


def _abbr(team_id: int) -> str:
    """Get team abbreviation from ID."""
    conn = get_conn()
    row = conn.execute("SELECT abbreviation FROM teams WHERE mlb_id = ?",
                      (team_id,)).fetchone()
    return row["abbreviation"] if row else str(team_id)
