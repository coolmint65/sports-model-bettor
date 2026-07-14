"""Football Elo rating system.

Simpler than soccer's: no goal-margin adjustment up front, just a
standard Elo update with a margin-of-victory multiplier (FiveThirty-
Eight-style). K-factor and HFA tuned via walk-forward on the league's
own history.
"""
from __future__ import annotations

import math
import sqlite3

from . import get_league_config
from ._db import get_conn


INIT_ELO = 1500.0
DEFAULT_K = 20.0
# Home-field advantage in Elo points. NFL ~55, college ~65, UFL is
# spring-league with small crowds so 25 is a more conservative starting
# point. Refit per-league via _calibrate.
DEFAULT_HFA = 25.0


def expected_score(r_home: float, r_away: float, hfa: float = DEFAULT_HFA
                    ) -> float:
    """Home expected score under standard logistic Elo with HFA."""
    return 1.0 / (1.0 + 10 ** ((r_away - r_home - hfa) / 400.0))


def _margin_multiplier(margin: int, elo_diff: float) -> float:
    """FiveThirtyEight-style multiplier: amplify K when margin is
    larger, dampen when the win was against a stronger opponent (so
    Elo doesn't run away on heavy upsets)."""
    if margin <= 0:
        return 1.0
    return math.log(margin + 1) * (2.2 / (elo_diff * 0.001 + 2.2))


def update(r_home: float, r_away: float, *, home_score: int,
            away_score: int, hfa: float = DEFAULT_HFA,
            k: float = DEFAULT_K) -> tuple[float, float]:
    """Return new (r_home, r_away) after one game."""
    expected = expected_score(r_home, r_away, hfa)
    actual = 0.5 if home_score == away_score else (
        1.0 if home_score > away_score else 0.0)
    margin = abs(home_score - away_score)
    elo_diff = r_home - r_away + (hfa if actual == 1.0 else -hfa)
    mult = _margin_multiplier(margin, elo_diff)
    delta = k * mult * (actual - expected)
    return r_home + delta, r_away - delta


def replay(league: str, conn: sqlite3.Connection | None = None,
            *, k: float = DEFAULT_K, hfa: float | None = None
            ) -> dict[int, float]:
    """Walk every finalized game in chronological order and return the
    final per-team Elo state. Cold-start ratings = 1500.

    Used by both the predictor (for "current" Elo state) and the
    walk-forward backtest (the backtest replays to a cutoff date and
    holds state from there)."""
    if conn is None:
        conn = get_conn(league)
    if hfa is None:
        cfg = get_league_config(league)
        hfa = cfg.get("home_advantage") or DEFAULT_HFA
    rows = conn.execute(
        "SELECT home_team_id, away_team_id, home_score, away_score, date "
        "FROM games "
        "WHERE status = 'final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date ASC, game_id ASC"
    ).fetchall()
    ratings: dict[int, float] = {}
    for r in rows:
        h_id = int(r["home_team_id"])
        a_id = int(r["away_team_id"])
        r_h = ratings.get(h_id, INIT_ELO)
        r_a = ratings.get(a_id, INIT_ELO)
        n_h, n_a = update(
            r_h, r_a,
            home_score=int(r["home_score"]),
            away_score=int(r["away_score"]),
            hfa=hfa, k=k,
        )
        ratings[h_id] = n_h
        ratings[a_id] = n_a
    return ratings


def get_rating(league: str, team_id: int) -> float:
    """Convenience accessor — re-replays history each call. For a hot
    serving loop this should be cached; for picks-per-slate frequency
    it's fine."""
    ratings = replay(league)
    return ratings.get(int(team_id), INIT_ELO)
