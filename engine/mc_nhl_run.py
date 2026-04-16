"""
NHL Monte Carlo runner -- DB glue between engine.mc_nhl and the rest
of the app. Builds NHLTeamProfile objects from team stats + goalie
data and invokes the simulator.
"""

from __future__ import annotations

import logging
from datetime import datetime

from .mc_nhl import (
    NHLTeamProfile, simulate_games, aggregate_nhl,
    LEAGUE_GF_PER_GAME, LEAGUE_GA_PER_GAME, LEAGUE_SAVE_PCT,
    LEAGUE_PP_GOALS_PER_GAME,
)

logger = logging.getLogger(__name__)


def run_nhl_mc(home_key: str, away_key: str,
                home_goalie_save_pct: float | None = None,
                away_goalie_save_pct: float | None = None,
                is_playoff: bool | None = None,
                n_sims: int = 50_000,
                seed: int | None = None) -> dict:
    """Build profiles from the NHL DB, run the simulator, return markets.

    Args:
        home_key / away_key: team abbreviations (e.g. "NYR").
        home_goalie_save_pct / away_goalie_save_pct: override the team
          save% when a confirmed starter is known for tonight. Falls back
          to the team's season save% when None.
        is_playoff: override the season-window check; usually leave None
          and let engine.nhl_predict._is_playoff_window() decide.
    """
    home = _load_team_profile(home_key, home_goalie_save_pct)
    away = _load_team_profile(away_key, away_goalie_save_pct)

    if is_playoff is None:
        try:
            from .nhl_predict import _is_playoff_window
            is_playoff = _is_playoff_window()
        except Exception:
            is_playoff = False

    raw = simulate_games(home, away, n_sims=n_sims,
                         is_playoff=is_playoff, seed=seed)
    agg = aggregate_nhl(raw)
    agg["meta"] = {
        "home": home.name,
        "away": away.name,
        "is_playoff": is_playoff,
        "n_sims": n_sims,
    }
    return agg


def _load_team_profile(team_key: str,
                        goalie_save_pct: float | None) -> NHLTeamProfile:
    """Read NHL team season stats from the DB and build a profile."""
    try:
        from .nhl_db import get_conn, get_nhl_team_by_abbr
    except Exception as e:
        logger.debug("nhl_db unavailable: %s", e)
        return NHLTeamProfile(name=team_key)

    team = get_nhl_team_by_abbr(team_key)
    if not team:
        logger.debug("NHL team not found: %s", team_key)
        return NHLTeamProfile(name=team_key)

    conn = get_conn()
    season = datetime.now().year
    # Try current season, fall back to most recent
    row = None
    for season_try in (season, season - 1):
        row = conn.execute(
            "SELECT * FROM nhl_team_stats WHERE team_id = ? AND season = ?",
            (team["id"], season_try),
        ).fetchone()
        if row:
            break

    if not row:
        return NHLTeamProfile(name=team.get("abbreviation", team_key))

    r = dict(row)
    gf = _safe(r.get("goals_for_avg"), LEAGUE_GF_PER_GAME, lo=1.5, hi=5.0)
    ga = _safe(r.get("goals_against_avg"), LEAGUE_GA_PER_GAME, lo=1.5, hi=5.0)
    # Save % from goalies; if caller gave us a confirmed starter, use that.
    if goalie_save_pct and 0.80 < goalie_save_pct < 0.96:
        save_pct = goalie_save_pct
    else:
        save_pct = _safe(r.get("save_pct"), LEAGUE_SAVE_PCT, lo=0.850, hi=0.940)
    pp_goals = _safe(r.get("pp_goals_per_game"), LEAGUE_PP_GOALS_PER_GAME,
                     lo=0.1, hi=1.5)

    return NHLTeamProfile(
        gf_per_game=gf,
        ga_per_game=ga,
        pp_goals_per_game=pp_goals,
        save_pct=save_pct,
        name=team.get("abbreviation", team_key),
    )


def _safe(v, default: float, lo: float, hi: float) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    if x != x or x <= 0:
        return default
    return max(lo, min(hi, x))
