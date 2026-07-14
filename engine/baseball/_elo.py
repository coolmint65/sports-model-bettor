"""Baseball Elo. Lighter than football's because run differential
isn't as informative — most baseball games swing on a single inning.
Standard 538-style K with a softer margin multiplier."""
from __future__ import annotations

import math
import sqlite3

from . import get_league_config
from ._db import get_conn


INIT_ELO = 1500.0
DEFAULT_K = 8.0
# HFA in baseball is small (~0.15 runs ≈ 4% win rate). Translated to
# Elo: ~30 points (NFL 538 uses 55 for football, NBA ~85 for hoops).
DEFAULT_HFA = 30.0


def expected_score(r_home: float, r_away: float, hfa: float = DEFAULT_HFA
                    ) -> float:
    return 1.0 / (1.0 + 10 ** ((r_away - r_home - hfa) / 400.0))


def _margin_multiplier(margin: int, elo_diff: float) -> float:
    if margin <= 0:
        return 1.0
    # Softer than football: log scale dampens blowout overrating
    # because blowouts in baseball are often pitcher-driven (one ace
    # vs a depleted bullpen) rather than systemic team strength.
    return math.log(margin + 1) * (1.8 / (elo_diff * 0.001 + 1.8))


def update(r_home: float, r_away: float, *, home_score: int,
            away_score: int, hfa: float = DEFAULT_HFA,
            k: float = DEFAULT_K) -> tuple[float, float]:
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
    if conn is None:
        conn = get_conn(league)
    if hfa is None:
        cfg = get_league_config(league)
        hfa = cfg.get("home_advantage_elo") or DEFAULT_HFA
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
    return replay(league).get(int(team_id), INIT_ELO)
