"""Rolling team-level pace + offensive/defensive rating computations.

Reads from ``game_team_stats`` (populated by ``_boxscore_ingest``) and
provides per-team rolling averages the predictor uses to replace the
basketball-wide prior values for pace / ORtg / DRtg.

Two surfaces:

    team_rates(league, team_id, *, recent_n=None) -> dict
        Returns ``{pace, ortg, drtg, n}`` for a team. ``recent_n=None``
        averages over every game on file; passing an int restricts to
        the most recent N games (used for "recent form" blending).

    matchup_pace(league, home_id, away_id) -> float
        Standard convention: matchup pace = average of the two teams'
        pace. Falls back to the league prior when neither team has
        enough games.

Per-process cache keyed on (league, team_id) so a slate refresh doesn't
re-aggregate on every game lookup. TTL 5 min — pace shifts slowly.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from ._config import get_league_config
from ._db import get_conn

logger = logging.getLogger(__name__)


_CACHE: dict[tuple[str, int, Optional[int]], tuple[float, dict]] = {}
_CACHE_TTL_S = 300

# Minimum games before we trust the per-team rate over the league prior.
# Below this threshold the predictor blends 50/50 to avoid early-season
# noise dominating a team's rating.
_MIN_GAMES_FOR_TRUST = 5


def _league_priors(league: str) -> dict:
    cfg = get_league_config(league)
    return {
        "pace": cfg.get("league_avg_pace") or 95.0,
        # ORtg/DRtg priors derive from league_avg_ppg / pace such that
        # a league-average team scores league_avg_ppg per game.
        "ortg": (cfg.get("league_avg_ppg") or 100.0)
                * 100.0 / (cfg.get("league_avg_pace") or 95.0),
        "drtg": (cfg.get("league_avg_ppg") or 100.0)
                * 100.0 / (cfg.get("league_avg_pace") or 95.0),
    }


def team_rates(league: str, team_id: int, *,
                recent_n: int | None = None) -> dict:
    """Return rolling pace / ORtg / DRtg for a team.

    Blends with the league prior when the sample is below
    ``_MIN_GAMES_FOR_TRUST``. The returned dict carries ``n`` so the
    caller can decide whether to weight the team rate or fall back."""
    key = (league, int(team_id), recent_n)
    cached = _CACHE.get(key)
    now = time.time()
    if cached and (now - cached[0]) < _CACHE_TTL_S:
        return cached[1]

    conn = get_conn(league)
    if recent_n:
        rows = conn.execute(
            "SELECT s.pace, s.ortg, s.drtg "
            "FROM game_team_stats s "
            "JOIN games g ON s.game_id = g.game_id "
            "WHERE s.team_id = ? "
            "  AND s.pace IS NOT NULL "
            "  AND s.ortg IS NOT NULL "
            "  AND s.drtg IS NOT NULL "
            "ORDER BY g.date DESC LIMIT ?",
            (team_id, int(recent_n)),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT pace, ortg, drtg FROM game_team_stats "
            "WHERE team_id = ? "
            "  AND pace IS NOT NULL "
            "  AND ortg IS NOT NULL "
            "  AND drtg IS NOT NULL",
            (team_id,),
        ).fetchall()

    n = len(rows)
    priors = _league_priors(league)
    if n == 0:
        out = {"pace": priors["pace"], "ortg": priors["ortg"],
               "drtg": priors["drtg"], "n": 0, "source": "prior"}
    else:
        avg_pace = sum(r["pace"] for r in rows) / n
        avg_ortg = sum(r["ortg"] for r in rows) / n
        avg_drtg = sum(r["drtg"] for r in rows) / n
        if n < _MIN_GAMES_FOR_TRUST:
            # Linear blend toward priors based on sample size:
            # n=0 → 100% prior, n=_MIN → 100% computed.
            w = n / _MIN_GAMES_FOR_TRUST
            out = {
                "pace": w * avg_pace + (1 - w) * priors["pace"],
                "ortg": w * avg_ortg + (1 - w) * priors["ortg"],
                "drtg": w * avg_drtg + (1 - w) * priors["drtg"],
                "n": n,
                "source": "blend",
            }
        else:
            out = {"pace": avg_pace, "ortg": avg_ortg, "drtg": avg_drtg,
                   "n": n, "source": "rates"}
    _CACHE[key] = (now, out)
    return out


def matchup_pace(league: str, home_id: int, away_id: int) -> float:
    """Standard convention: average the two teams' pace ratings.

    Both teams play the same number of possessions in any given game,
    so the matchup's expected pace is the average of their season
    paces (with prior shrinkage when samples are thin)."""
    h = team_rates(league, home_id)
    a = team_rates(league, away_id)
    return (h["pace"] + a["pace"]) / 2.0


def reset_cache() -> None:
    _CACHE.clear()
