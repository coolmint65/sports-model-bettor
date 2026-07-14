"""Baseball matchup predictor — Elo + Pythagorean style scoring,
plus the MC + GBM legs that complete the factor/MC/GBM ensemble.

Output dict shape matches engine.football._predict so the picker +
V3.2 signal-explain layer plug in unchanged.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime

from . import get_league_config
from ._db import get_conn
from ._elo import INIT_ELO, expected_score, replay

logger = logging.getLogger(__name__)


# Realized constants from college baseball ~2024-25 sample. Defaults
# below get overlaid by ``_calibrate.fit`` once the league has
# history.
_DEFAULT_MARGIN_SIGMA = 4.5
_DEFAULT_TOTAL_SIGMA  = 4.8
_DEFAULT_AVG_TOTAL    = 12.5
# Elo points → runs slope. 538-style baseball Elo: 25 Elo ≈ 0.13 runs.
# We use a slightly tighter slope for college because the strength
# spread between programs is wider than MLB.
_DEFAULT_ELO_PER_RUN  = 100.0


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def predict_match(league: str, home_team_id: int, away_team_id: int,
                   *, ratings: dict[int, float] | None = None,
                   spread: float | None = None,
                   total_line: float | None = None,
                   game_date: str | None = None) -> dict:
    cfg = get_league_config(league)
    if ratings is None:
        ratings = replay(league)

    r_home = ratings.get(int(home_team_id), INIT_ELO)
    r_away = ratings.get(int(away_team_id), INIT_ELO)
    hfa = cfg.get("home_advantage_elo") or 30.0

    p_home = expected_score(r_home, r_away, hfa)
    p_away = 1.0 - p_home

    avg_total = cfg.get("league_avg_total") or _DEFAULT_AVG_TOTAL
    margin_sigma = cfg.get("margin_sigma") or _DEFAULT_MARGIN_SIGMA
    total_sigma = cfg.get("total_sigma") or _DEFAULT_TOTAL_SIGMA
    elo_diff = r_home - r_away + hfa
    expected_margin = elo_diff / _DEFAULT_ELO_PER_RUN
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
        z = (-spread - expected_margin) / margin_sigma
        p_home_cover = 1.0 - _norm_cdf(z)
        out["spread"]           = float(spread)
        out["p_home_cover"]     = round(p_home_cover, 4)
        out["p_away_cover"]     = round(1.0 - p_home_cover, 4)

    if total_line is not None:
        z = (total_line - expected_total) / total_sigma
        p_under = _norm_cdf(z)
        out["total_line"]   = float(total_line)
        out["p_over"]       = round(1.0 - p_under, 4)
        out["p_under"]      = round(p_under, 4)

    # MC layer
    from ._mc import simulate as _mc_simulate
    mc = _mc_simulate(
        expected_home=expected_home,
        expected_away=expected_away,
        margin_sigma=margin_sigma,
        spread=spread,
        total_line=total_line,
    )
    out.update(mc)

    # GBM layer (gated on training data)
    from ._gbm import predict_gbm as _predict_gbm
    out.update(_predict_gbm(
        league=league,
        home_team_id=int(home_team_id),
        away_team_id=int(away_team_id),
        expected_margin=expected_margin,
        expected_total=expected_total,
        spread=spread,
        total_line=total_line,
        as_of_date=game_date,
    ))

    # Ensemble blend
    from ._ensemble import blend as _blend
    _blend(league, out)

    return out


def log_signals(league: str, pred: dict) -> None:
    """Insert one prediction_signals row per market via a short-lived
    writer connection. Same fire-and-forget pattern as the football
    framework — never propagate write failures into the slate
    response."""
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
            "baseball", league, str(gid), today, home_id, away_id, market,
            f.get(key), m.get(key), g.get(key),
        ))
    if not rows:
        return
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
        logger.debug("[baseball:%s] signal log skipped: %s", league, e)
