"""
NBA Q1 Monte Carlo runner -- DB glue.

Loads Q1 stats from nba_db.get_team_q1_stats, builds NBATeamProfile
objects, and invokes the simulator. Playoff detection defers to
engine.nba_q1_predict._is_nba_playoffs so the same window logic
governs both the factor model and MC.
"""

from __future__ import annotations

import logging
from datetime import datetime

from .mc_nba import (
    NBATeamProfile, simulate_q1, aggregate_nba_q1,
    LEAGUE_Q1_PPP_MEAN, LEAGUE_Q1_PACE,
)

logger = logging.getLogger(__name__)


def run_nba_q1_mc(home_abbr: str, away_abbr: str,
                   n_sims: int = 50_000,
                   is_playoff: bool | None = None,
                   seed: int | None = None) -> dict:
    """Load Q1 stats from the NBA DB and run the Q1 MC simulator."""
    home = _load_q1_profile(home_abbr)
    away = _load_q1_profile(away_abbr)

    if is_playoff is None:
        try:
            from .nba_q1_predict import _is_nba_playoffs
            is_playoff = _is_nba_playoffs()
        except Exception:
            is_playoff = False

    raw = simulate_q1(home, away, n_sims=n_sims,
                      is_playoff=is_playoff, seed=seed)
    agg = aggregate_nba_q1(raw)
    agg["meta"] = {
        "home": home.name,
        "away": away.name,
        "is_playoff": is_playoff,
        "n_sims": n_sims,
    }
    return agg


def _load_q1_profile(abbr: str) -> NBATeamProfile:
    """Read Q1 stats for a team from the NBA DB."""
    try:
        from .nba_db import get_conn, get_nba_team_by_abbr
    except Exception as e:
        logger.debug("nba_db unavailable: %s", e)
        return NBATeamProfile(name=abbr)

    team = get_nba_team_by_abbr(abbr)
    if not team:
        logger.debug("NBA team not found: %s", abbr)
        return NBATeamProfile(name=abbr)

    conn = get_conn()
    season = datetime.now().year
    row = None
    for season_try in (season, season - 1):
        row = conn.execute(
            "SELECT * FROM nba_q1_stats WHERE team_id = ? AND season = ?",
            (team["id"], season_try),
        ).fetchone()
        if row:
            break

    lg = LEAGUE_Q1_PPP_MEAN * LEAGUE_Q1_PACE
    if not row:
        return NBATeamProfile(name=team.get("abbreviation", abbr))

    r = dict(row)
    return NBATeamProfile(
        q1_ppg=_safe(r.get("q1_ppg"), lg, 15, 40),
        q1_opp_ppg=_safe(r.get("q1_opp_ppg"), lg, 15, 40),
        pace=_safe(r.get("pace"), 99.0, 88, 110),
        name=team.get("abbreviation", abbr),
    )


def _safe(v, default: float, lo: float, hi: float) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    if x != x or x <= 0:
        return default
    return max(lo, min(hi, x))
