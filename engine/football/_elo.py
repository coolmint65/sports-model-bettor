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
_REPLAY_CACHE: dict = {}  # (league, "elo"|"pts") -> (n_final_games, ratings) — serving-path cache
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


# ── Points-based ratings (CFB, MintyBot 2026-10-25, Austin "fix it all up") ──────
# The win/loss Elo above (K=40 + 538 MOV multiplier, no season carry-over, every team
# born at 1500) had zero information beyond HR's CFB line and ran -30% on 139 dog
# spreads. Walk-forward re-tune on 6,064 CFB finals (2023-26; every prediction from
# PRE-game ratings, Elo->pts slope fit on the PRIOR season): move ratings by the
# MARGIN error, regress 20% toward the team's tier at each season start, start non-FBS
# teams at 800, double the step for a team's first games of a season. Out-of-sample
# margin RMSE 2025 18.9 -> 16.5, 2026 21.0 -> 17.4; vs HR's pre-kickoff line on 644
# games 17.3 -> 15.6 (HR 14.7); win-prob log-loss 2026 .587 -> .508. Still no proven
# edge over the line (incremental slope .02 -> .08, inside the noise) -- it stops the
# false big-dog leans, it doesn't promise sides. Enabled by cfg["elo_update"] == "pts".

def _season_of(date_str: str) -> int:
    y, m = int(date_str[:4]), int(date_str[5:7])
    return y if m >= 7 else y - 1


def _tier_prior(cfg: dict, team_id: int, fbs: frozenset) -> float:
    if not fbs or team_id in fbs:
        return INIT_ELO
    return float(cfg.get("elo_init_other") or INIT_ELO)


def init_rating(league: str, team_id: int) -> float:
    """Rating for a team replay() has never seen (predict-time default)."""
    cfg = get_league_config(league)
    if cfg.get("elo_update") != "pts":
        return INIT_ELO
    from ._segments import fbs_team_ids
    return _tier_prior(cfg, int(team_id), fbs_team_ids())


def serving_params(cfg: dict) -> tuple[float, float]:
    """(hfa_elo, elo_per_point) that match replay()'s SERVING ratings for the league. The
    points-based ratings were tuned with their own HFA + slope; legacy Elo (and the GBM,
    whose features were trained on legacy Elo) keep home_advantage / elo_per_point."""
    if cfg.get("elo_update") == "pts":
        return (float(cfg.get("elo_pts_hfa") or DEFAULT_HFA),
                float(cfg.get("elo_pts_per_point") or 25.0))
    return (float(cfg.get("home_advantage") or DEFAULT_HFA),
            float(cfg.get("elo_per_point") or 25.0))


def _replay_pts(cfg: dict, rows, on_game=None) -> dict[int, float]:
    """``on_game(row, r_home_pre, r_away_pre)``, if given, sees every game's PRE-game ratings
    (walk_forward's hook); the returned ratings are unaffected."""
    from ._segments import fbs_team_ids
    fbs = fbs_team_ids()
    hfa = float(cfg.get("elo_pts_hfa") or DEFAULT_HFA)
    kp = float(cfg.get("elo_pts_k") or 2.2)              # Elo moved per point of margin error
    cap = float(cfg.get("elo_pts_cap") or 50.0)          # margin-error clamp (garbage-time blowouts)
    early = float(cfg.get("elo_pts_early") or 0.0)       # extra step size over a team's first 4 games
    err_epp = float(cfg.get("elo_pts_err_epp") or 22.0)  # Elo->pts inside the update (kp absorbs scale)
    regress = float(cfg.get("elo_season_regress") or 0.0)
    ratings: dict[int, float] = {}
    played: dict[int, int] = {}
    season = None
    for r in rows:
        s = _season_of(str(r["date"]))
        if s != season:
            season = s
            played = {}
            if regress > 0:
                for t in ratings:
                    mu = _tier_prior(cfg, t, fbs)
                    ratings[t] = mu + (1.0 - regress) * (ratings[t] - mu)
        h_id = int(r["home_team_id"]); a_id = int(r["away_team_id"])
        r_h = ratings.get(h_id, _tier_prior(cfg, h_id, fbs))
        r_a = ratings.get(a_id, _tier_prior(cfg, a_id, fbs))
        if on_game is not None:
            on_game(r, r_h, r_a)
        margin = int(r["home_score"]) - int(r["away_score"])
        err = max(-cap, min(cap, margin - (r_h - r_a + hfa) / err_epp))
        boost = 1.0 + early * max(0, 4 - min(played.get(h_id, 0), played.get(a_id, 0))) / 4.0
        d = kp * boost * err
        ratings[h_id] = r_h + d
        ratings[a_id] = r_a - d
        played[h_id] = played.get(h_id, 0) + 1
        played[a_id] = played.get(a_id, 0) + 1
    return ratings


def replay(league: str, conn: sqlite3.Connection | None = None,
            *, k: float | None = None, hfa: float | None = None,
            legacy: bool = False) -> dict[int, float]:
    """Walk every finalized game in chronological order and return the
    final per-team Elo state. Cold-start ratings = 1500.

    Used by both the predictor (for "current" Elo state) and the
    walk-forward backtest (the backtest replays to a cutoff date and
    holds state from there). ``legacy=True`` forces the win/loss Elo even
    for a points-based league (the GBM's features were trained on it)."""
    _default = (k is None and hfa is None)  # serving/predict path (vs backtest)
    if conn is None:
        conn = get_conn(league)
    cfg = get_league_config(league)
    _pts = _default and not legacy and cfg.get("elo_update") == "pts"
    _ck = (league, "pts" if _pts else "elo")
    if hfa is None:
        hfa = cfg.get("home_advantage") or DEFAULT_HFA
    if k is None:
        k = cfg.get("elo_k") or DEFAULT_K      # per-league K (cfb fit 40 on 2026-09-03)
    # Cache the default replay (Austin 2026-09-19 perf): predict_gbm's
    # _build_features re-replays the FULL Elo history per prediction (~3x/game x
    # slate ~= 38s). Key on the finalized-game count — cheap check, invalidates
    # the instant a new game finalizes (the only thing that changes Elo).
    _n = None
    if _default:
        try:
            _n = conn.execute(
                "SELECT COUNT(*) FROM games WHERE status = 'final' "
                "  AND home_score IS NOT NULL AND away_score IS NOT NULL"
            ).fetchone()[0]
        except Exception:
            _n = None
        _c = _REPLAY_CACHE.get(_ck)
        if _c is not None and _n is not None and _c[0] == _n:
            return _c[1]
    rows = conn.execute(
        "SELECT home_team_id, away_team_id, home_score, away_score, date "
        "FROM games "
        "WHERE status = 'final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date ASC, game_id ASC"
    ).fetchall()
    if _pts:
        ratings = _replay_pts(cfg, rows)
        if _n is not None:
            _REPLAY_CACHE[_ck] = (_n, ratings)
        return ratings
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
    if _default and _n is not None:
        _REPLAY_CACHE[_ck] = (_n, ratings)
    return ratings


def walk_forward(league: str, conn: sqlite3.Connection | None = None) -> list:
    """[(row, r_home_pre, r_away_pre)] for every finalized game in replay() order, from the SAME
    update the serving replay() uses (points-based when elo_update == "pts", else the win/loss
    Elo with the league's HFA/K) -- the pre-game ratings predict_match would have had. For
    fitters that must score the SERVED number out of sample (display-bar calibration,
    MintyBot 2026-10-25). Rows carry game_id, date, team ids and scores."""
    if conn is None:
        conn = get_conn(league)
    cfg = get_league_config(league)
    rows = conn.execute(
        "SELECT game_id, home_team_id, away_team_id, home_score, away_score, date "
        "FROM games "
        "WHERE status = 'final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date ASC, game_id ASC"
    ).fetchall()
    out: list = []
    if cfg.get("elo_update") == "pts":
        _replay_pts(cfg, rows, on_game=lambda r, rh, ra: out.append((r, rh, ra)))
        return out
    hfa = cfg.get("home_advantage") or DEFAULT_HFA
    k = cfg.get("elo_k") or DEFAULT_K
    ratings: dict[int, float] = {}
    for r in rows:
        h_id = int(r["home_team_id"])
        a_id = int(r["away_team_id"])
        r_h = ratings.get(h_id, INIT_ELO)
        r_a = ratings.get(a_id, INIT_ELO)
        out.append((r, r_h, r_a))
        ratings[h_id], ratings[a_id] = update(
            r_h, r_a, home_score=int(r["home_score"]),
            away_score=int(r["away_score"]), hfa=hfa, k=k)
    return out


def get_rating(league: str, team_id: int) -> float:
    """Convenience accessor — re-replays history each call. For a hot
    serving loop this should be cached; for picks-per-slate frequency
    it's fine."""
    ratings = replay(league)
    return ratings.get(int(team_id), INIT_ELO)
