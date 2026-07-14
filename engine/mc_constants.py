"""Data-fit MC simulation constants with hardcoded fallbacks.

Centralizes the variance / boost / league-average constants the MC simulators
depend on. Each accessor:

  1. Looks up a calibrated value from the per-sport ``*_model_config`` table
     (populated by the calibration scripts in ``engine/{sport}_calibration.py``).
  2. Falls back to a hardcoded ``DEFAULT_*`` if no calibrated row exists.
  3. Caches the result for the life of the process to avoid hammering SQLite.

Why this exists (#158): pre-2026-05-02 the MC sims read module-level constants
that drifted from reality as the season progressed. The calibration scripts
already wrote freshly-fit values to the model_config tables, but the sims
never read them — so a calibration run "succeeded" without changing anything.
This module closes that loop.

Refresh the cache after running calibration: ``invalidate()``.
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ── Hardcoded fallback baselines ──
# These are the values the MC sims used historically when no calibrated row
# exists yet. Kept here so a fresh database (no calibration run) still
# produces reasonable picks. Real numbers replace these once the calibration
# script runs.

DEFAULT_NBA_Q1_MARGIN_STD = 8.63
DEFAULT_NBA_Q1_TOTAL_STD = 8.46     # 0.98 x Q1_STD_DEV (historical relationship)
DEFAULT_NBA_Q1_HOME_BOOST = 0.69
DEFAULT_NBA_Q1_AVG_TOTAL = 58.8
DEFAULT_NBA_Q1_PACE = 24.5          # possessions/team in Q1 at pace 99
DEFAULT_NBA_Q1_PPP_MEAN = 1.12      # points-per-possession in Q1
DEFAULT_NBA_Q1_PPP_STD = 0.28       # std-dev across possessions

DEFAULT_NBA_FULL_MARGIN_STD = 16.06
DEFAULT_NBA_FULL_TOTAL_STD = 21.25
DEFAULT_NBA_FULL_HOME_BOOST = 2.14
DEFAULT_NBA_FULL_AVG_TOTAL = 227.89
DEFAULT_NBA_FULL_PACE = 99.0
DEFAULT_NBA_FULL_PPP_MEAN = 1.151   # 113.95 / 99.0
DEFAULT_NBA_FULL_TEAM_STD = 11.36
DEFAULT_NBA_FULL_GAME_SHOCK_STD = 6.96

# NHL — only home_edge is currently persisted to nhl_model_config
# (engine/sports/nhl/calibration.py: clamped to [0.05, 0.30] goals).
# The other constants are seasonal averages we haven't wired into a
# calibration writer yet, so the defaults below are the source of
# truth for now. Adding writers later just means populating the
# corresponding keys; this module already reads them.
DEFAULT_NHL_GF_PER_GAME = 3.10
DEFAULT_NHL_GA_PER_GAME = 3.10
DEFAULT_NHL_PP_GOALS_PER_GAME = 0.65
DEFAULT_NHL_SAVE_PCT = 0.905
DEFAULT_NHL_HOME_EDGE_GOALS = 0.20
DEFAULT_NHL_OT_3V3_GF_RATE = 2.0


# ── Cache ──
_CACHE: dict[str, Optional[float]] = {}


def invalidate() -> None:
    """Drop the in-memory cache. Call after a calibration run."""
    _CACHE.clear()


def _read(sport: str, key: str) -> Optional[float]:
    """Read one row from ``{sport}_model_config``. Returns None if missing
    or if the table doesn't exist yet (fresh DB before first calibration)."""
    cache_key = f"{sport}:{key}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    try:
        if sport == "nba":
            from .nba_db import get_conn
        elif sport == "nhl":
            from .nhl_db import get_conn
        else:
            from .db import get_conn
        conn = get_conn()
        row = conn.execute(
            f"SELECT value FROM {sport}_model_config WHERE key = ?",
            (key,),
        ).fetchone()
    except Exception as exc:
        # Fresh DB / missing table — fall through to default.
        logger.debug("mc_constants %s.%s lookup failed: %s", sport, key, exc)
        _CACHE[cache_key] = None
        return None

    value = row["value"] if row else None
    _CACHE[cache_key] = value
    return value


def _resolve(sport: str, key: str, default: float) -> float:
    """Read a calibrated value or fall back to default."""
    fitted = _read(sport, key)
    if fitted is None or fitted <= 0:
        return default
    return float(fitted)


# ── NBA Q1 ──

def nba_q1_margin_std() -> float:
    return _resolve("nba", "q1_margin_std_dev", DEFAULT_NBA_Q1_MARGIN_STD)


def nba_q1_total_std() -> float:
    return _resolve("nba", "q1_total_std_dev", DEFAULT_NBA_Q1_TOTAL_STD)


def nba_q1_home_boost() -> float:
    # Boost is signed — fall back to default rather than discarding negatives.
    fitted = _read("nba", "q1_home_boost")
    if fitted is None:
        return DEFAULT_NBA_Q1_HOME_BOOST
    return float(fitted)


def nba_q1_avg_total() -> float:
    return _resolve("nba", "q1_avg_total", DEFAULT_NBA_Q1_AVG_TOTAL)


# ── NBA Q1 (possession-level constants) ──

def nba_q1_pace() -> float:
    return _resolve("nba", "q1_pace", DEFAULT_NBA_Q1_PACE)


def nba_q1_ppp_mean() -> float:
    return _resolve("nba", "q1_ppp_mean", DEFAULT_NBA_Q1_PPP_MEAN)


def nba_q1_ppp_std() -> float:
    return _resolve("nba", "q1_ppp_std", DEFAULT_NBA_Q1_PPP_STD)


# ── NBA full game ──

def nba_full_margin_std() -> float:
    return _resolve("nba", "full_margin_std_dev", DEFAULT_NBA_FULL_MARGIN_STD)


def nba_full_total_std() -> float:
    return _resolve("nba", "full_total_std_dev", DEFAULT_NBA_FULL_TOTAL_STD)


def nba_full_home_boost() -> float:
    fitted = _read("nba", "full_home_boost")
    if fitted is None:
        return DEFAULT_NBA_FULL_HOME_BOOST
    return float(fitted)


def nba_full_avg_total() -> float:
    return _resolve("nba", "full_avg_total", DEFAULT_NBA_FULL_AVG_TOTAL)


def nba_full_pace() -> float:
    return _resolve("nba", "full_pace", DEFAULT_NBA_FULL_PACE)


def nba_full_ppp_mean() -> float:
    return _resolve("nba", "full_ppp_mean", DEFAULT_NBA_FULL_PPP_MEAN)


def nba_full_team_std() -> float:
    return _resolve("nba", "full_team_std", DEFAULT_NBA_FULL_TEAM_STD)


def nba_full_game_shock_std() -> float:
    return _resolve("nba", "full_game_shock_std", DEFAULT_NBA_FULL_GAME_SHOCK_STD)


# ── NHL ──

def nhl_gf_per_game() -> float:
    return _resolve("nhl", "gf_per_game", DEFAULT_NHL_GF_PER_GAME)


def nhl_ga_per_game() -> float:
    return _resolve("nhl", "ga_per_game", DEFAULT_NHL_GA_PER_GAME)


def nhl_pp_goals_per_game() -> float:
    return _resolve("nhl", "pp_goals_per_game", DEFAULT_NHL_PP_GOALS_PER_GAME)


def nhl_save_pct() -> float:
    return _resolve("nhl", "save_pct", DEFAULT_NHL_SAVE_PCT)


def nhl_home_edge_goals() -> float:
    # NHL calibration persists this as ``home_edge`` (goals).
    fitted = _read("nhl", "home_edge")
    if fitted is None:
        return DEFAULT_NHL_HOME_EDGE_GOALS
    return float(fitted)


def nhl_ot_3v3_gf_rate() -> float:
    return _resolve("nhl", "ot_3v3_gf_rate", DEFAULT_NHL_OT_3V3_GF_RATE)
