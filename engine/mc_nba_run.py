"""
NBA Monte Carlo runner -- DB glue for both Q1 and full-game sims.

Loads team stats from nba_db, builds the appropriate profile object,
and invokes the simulator. Playoff detection defers to
engine.nba_q1_predict._is_nba_playoffs so the same window logic
governs every NBA layer.
"""

from __future__ import annotations

import logging
from datetime import datetime

from .mc_nba import (
    NBATeamProfile, simulate_q1, aggregate_nba_q1,
    NBAFullProfile, simulate_full, aggregate_nba_full,
)
from . import mc_constants as _mc

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

    lg = _mc.nba_q1_ppp_mean() * _mc.nba_q1_pace()
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


# ── Full-game runner ─────────────────────────────────────────────

def run_nba_full_mc(home_abbr: str, away_abbr: str,
                    n_sims: int = 50_000,
                    is_playoff: bool | None = None,
                    seed: int | None = None) -> dict:
    """Load full-game stats from the NBA DB and run the full-game MC.

    Mirrors run_nba_q1_mc but builds an NBAFullProfile from the team's
    actual full-game PPG/opp-PPG (computed by
    engine.nba_predict._compute_team_full_ppg) plus the pace/efficiency
    ratings from nba_q1_stats.
    """
    home = _load_full_profile(home_abbr)
    away = _load_full_profile(away_abbr)

    if is_playoff is None:
        try:
            from .nba_q1_predict import _is_nba_playoffs
            is_playoff = _is_nba_playoffs()
        except Exception:
            is_playoff = False

    raw = simulate_full(home, away, n_sims=n_sims,
                        is_playoff=is_playoff, seed=seed)
    agg = aggregate_nba_full(raw)
    agg["meta"] = {
        "home": home.name,
        "away": away.name,
        "is_playoff": is_playoff,
        "n_sims": n_sims,
    }
    return agg


def _load_full_profile(abbr: str) -> NBAFullProfile:
    """Read full-game stats for a team. Falls back to league averages
    when no row exists (cold start / preseason)."""
    from .nba_predict import _compute_team_full_ppg
    try:
        from .nba_db import get_nba_team_by_abbr, get_team_q1_stats
    except Exception as e:
        logger.debug("nba_db unavailable: %s", e)
        return NBAFullProfile(name=abbr)

    team = get_nba_team_by_abbr(abbr)
    if not team:
        logger.debug("NBA team not found: %s", abbr)
        return NBAFullProfile(name=abbr)

    season = datetime.now().year if datetime.now().month >= 9 else datetime.now().year - 1
    full = _compute_team_full_ppg(team["id"], season)
    q1 = get_team_q1_stats(team["id"], season) or {}

    lg = _mc.nba_full_ppp_mean() * _mc.nba_full_pace()  # ~113.95 fitted
    return NBAFullProfile(
        ppg=_safe(full.get("ppg"), lg, 80, 140),
        opp_ppg=_safe(full.get("opp_ppg"), lg, 80, 140),
        pace=_safe(q1.get("pace"), 99.0, 88, 110),
        name=team.get("abbreviation", abbr),
    )
