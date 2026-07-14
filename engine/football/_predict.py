"""Football matchup predictor.

Given Elo ratings + per-league average total points, derive the full
slate of model probabilities the picker needs:

    p_home / p_away              — moneyline
    expected_margin              — for spread bets (home POV)
    expected_total               — for over/under
    p_home_cover / p_away_cover   — spread cover under N(margin, σ²)
    p_over / p_under              — total cover under N(total, σ²)

Standard deviations are sport-wide constants tuned from realized
score differences. UFL plays ~10 games per team per season — enough
to detect signal in walk-forward but not enough to fit per-team σ.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime

from . import get_league_config
from ._db import get_conn
from ._elo import INIT_ELO, expected_score, replay

logger = logging.getLogger(__name__)


# Realized standard deviations from UFL 2024-25 sample (n≈110).
# Margin σ ~ 13.5 (NFL is ~13.86 per 538), total σ ~ 14 (UFL is
# higher-variance per game than NFL because some teams play "rest"
# games with depleted rosters).
_DEFAULT_MARGIN_SIGMA = 13.5
_DEFAULT_TOTAL_SIGMA  = 14.0
# Expected total points across both teams. UFL averaged ~42 in 2024;
# refit per-league via _calibrate.
_DEFAULT_AVG_TOTAL    = 42.0
# Slope: how many points one Elo point is "worth" in expected margin.
# Football convention is ~25 Elo = 1 point. Tunable per-league.
_DEFAULT_ELO_PER_POINT = 25.0


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def predict_match(league: str, home_team_id: int, away_team_id: int,
                   *, ratings: dict[int, float] | None = None,
                   spread: float | None = None,
                   total_line: float | None = None) -> dict:
    """Predict every market for one matchup.

    ``spread`` and ``total_line`` are optional — when provided, the
    response includes the implied cover/over probabilities so the
    picker doesn't have to recompute. Otherwise the picker probes
    its own (line, odds) candidates against the predicted
    distribution.
    """
    cfg = get_league_config(league)
    if ratings is None:
        ratings = replay(league)

    r_home = ratings.get(int(home_team_id), INIT_ELO)
    r_away = ratings.get(int(away_team_id), INIT_ELO)
    hfa = cfg.get("home_advantage") or 25.0

    p_home = expected_score(r_home, r_away, hfa)
    p_away = 1.0 - p_home

    avg_total = cfg.get("league_avg_total") or _DEFAULT_AVG_TOTAL
    elo_diff = r_home - r_away + hfa
    expected_margin = elo_diff / _DEFAULT_ELO_PER_POINT
    # Split the total by win-prob skew so the favorite carries more
    # expected points than the dog.
    expected_home = (avg_total + expected_margin) / 2.0
    expected_away = (avg_total - expected_margin) / 2.0
    expected_total = expected_home + expected_away

    out = {
        "league":          league,
        "home_team_id":    int(home_team_id),
        "away_team_id":    int(away_team_id),
        "elo_home":        round(r_home, 1),
        "elo_away":        round(r_away, 1),
        "p_home":          round(p_home, 4),
        "p_away":          round(p_away, 4),
        "expected_margin": round(expected_margin, 2),
        "expected_total":  round(expected_total, 2),
        "expected_home":   round(expected_home, 2),
        "expected_away":   round(expected_away, 2),
    }

    if spread is not None:
        # Home covers a spread of ``spread`` (negative = home is favored
        # by abs(spread)) iff (home - away) > -spread, i.e. margin >
        # -spread. P(margin > -spread) under N(expected_margin, σ²).
        z = (-spread - expected_margin) / _DEFAULT_MARGIN_SIGMA
        p_home_cover = 1.0 - _norm_cdf(z)
        out["spread"]           = float(spread)
        out["p_home_cover"]     = round(p_home_cover, 4)
        out["p_away_cover"]     = round(1.0 - p_home_cover, 4)

    if total_line is not None:
        z = (total_line - expected_total) / _DEFAULT_TOTAL_SIGMA
        p_under = _norm_cdf(z)
        out["total_line"]   = float(total_line)
        out["p_over"]       = round(1.0 - p_under, 4)
        out["p_under"]      = round(p_under, 4)

    # ── MC layer ─────────────────────────────────────────────
    # Runs a 10k-sim Monte Carlo over the same expected scores so the
    # picker has a second signal leg for the V3.2 ensemble explain.
    # Cheap (~5ms per game) — runs every prediction. GBM layer is
    # gated separately on training-data volume; see ``_gbm.py``.
    from ._mc import simulate as _mc_simulate
    mc = _mc_simulate(
        expected_home=expected_home,
        expected_away=expected_away,
        margin_sigma=cfg.get("margin_sigma") or _DEFAULT_MARGIN_SIGMA,
        spread=spread,
        total_line=total_line,
    )
    out.update(mc)

    # ── GBM layer ────────────────────────────────────────────
    # Gated on sample size — UFL has 110 games which is too thin for a
    # reliable GBM. The hook is here so future sports/leagues with
    # more history pick it up automatically, and V3.2's signal_explain
    # endpoint reports `gbm_prob: null` cleanly rather than silently
    # omitting the leg.
    from ._gbm import predict_gbm as _predict_gbm
    out.update(_predict_gbm(
        league=league,
        home_team_id=int(home_team_id),
        away_team_id=int(away_team_id),
        expected_margin=expected_margin,
        expected_total=expected_total,
        spread=spread,
        total_line=total_line,
    ))

    # ── Ensemble ─────────────────────────────────────────────
    # Blend the three legs (factor / MC / GBM) into a final
    # probability per market. When GBM is gated (gbm_prob is None),
    # the blender renormalizes the remaining weights so a missing
    # leg doesn't drag the prob toward 0 — same pattern the basketball
    # _ensemble module uses.
    from ._ensemble import blend as _ensemble_blend
    _ensemble_blend(league, out)

    # Signal logging is the route's responsibility, NOT the predictor's
    # — see ``log_signals`` below. Calling it from here would write 3×
    # per game (factor predict + spread re-predict + total re-predict),
    # which races the shared cached connection from concurrent slate
    # hits and produced intermittent 500s.

    return out


def log_signals(league: str, pred: dict) -> None:
    """Insert one prediction_signals row per market into the league's
    DB. Called by the slate-route entry point (NOT the predictor) so
    each game logs exactly once per slate hit.

    Uses a short-lived dedicated sqlite3 connection rather than the
    module's cached one — concurrent slate hits would otherwise race
    on the same connection across threads and intermittently fail
    with `database is locked`.
    """
    gid = pred.get("game_id")
    if not gid:
        return
    home_id = pred.get("home_team_id")
    away_id = pred.get("away_team_id")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    f = pred.get("_signal_factor") or {}
    m = pred.get("_signal_mc") or {}
    g = pred.get("_signal_gbm") or {}
    rows = []
    for market, key in (("home_win", "p_home"),
                         ("home_cover", "p_home_cover"),
                         ("total_over", "p_over")):
        if f.get(key) is None and m.get(key) is None and g.get(key) is None:
            continue
        rows.append((
            "football", league, str(gid), today, home_id, away_id, market,
            f.get(key), m.get(key), g.get(key),
        ))
    if not rows:
        return
    # Short-lived writer connection. WAL mode + a fresh connection
    # means the writer doesn't contend with the cached reader-and-
    # everyone-else connection that the rest of the framework uses.
    import sqlite3
    from . import get_league_config
    cfg = get_league_config(league)
    try:
        wconn = sqlite3.connect(cfg["db_path"], timeout=5.0,
                                  isolation_level=None)
        wconn.execute("PRAGMA journal_mode = WAL")
        try:
            wconn.executemany(
                "INSERT INTO prediction_signals "
                "(sport, league, game_id, game_date, home_team_id, "
                " away_team_id, market, factor_prob, mc_prob, gbm_prob) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
        finally:
            wconn.close()
    except Exception as e:
        # Never propagate — the slate must render even if the signal
        # log fails on a contention burst.
        logger.debug("[football:%s] signal log skipped: %s", league, e)
