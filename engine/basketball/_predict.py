"""Generic basketball predictor.

Factors out the core math from engine.nba_predict (opponent-adjusted
attack/defense + pace + home boost + recent form) into a league-agnostic
function that reads constants from LEAGUE_REGISTRY.

Intentionally NOT included (NBA keeps these in nba_predict.py until a
cross-league pattern emerges):
  - Playoff series context (per-league playoff structure varies)
  - B2B detection (needs per-league schedule API)
  - Injury feeds (no clean source for most leagues)
  - Roster availability gating

Per-league fallback: when a constant is None in the registry (a fresh
``pending_calibration`` league with no backfill yet), the predictor uses
a basketball-wide prior so the league still produces directionally-
correct picks while we collect data to fit per-league constants. The
result dict carries ``constants_source`` so the picker / tracker can
gate on whether the league is fitted yet.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime

from ._config import get_league_config, season_label
from ._db import get_conn, games_table, teams_table
from .._tz import et_today_str

logger = logging.getLogger(__name__)


# ── Basketball-wide priors ─────────────────────────────────────
# Used for leagues whose registry constants are still None. These are
# coarse — appropriate for paper-bet only. Live picks should require
# fitted per-league constants. Numbers are NBA-derived because that's
# our deepest backfill; international leagues will diverge but this is
# better than nothing for cold-start.

PRIOR_HOME_BOOST = 2.5
PRIOR_B2B_PENALTY = -2.5
PRIOR_MARGIN_STD = 16.0
PRIOR_TOTAL_STD = 21.0
PRIOR_LEAGUE_AVG_TOTAL = 200.0   # midway between college (~145) and NBA (~228)
PRIOR_LEAGUE_AVG_PPG = 100.0
PRIOR_LEAGUE_AVG_PACE = 95.0


def _norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _resolve_constants(league: str) -> tuple[dict, str]:
    """Return (constants, source_tag). Source is 'fitted' when the
    load-bearing constants (home_boost, margin_std, total_std,
    league_avg_total, league_avg_ppg) are all populated from a backfill.
    b2b_penalty and league_avg_pace fall back to priors per-key
    independently — they need data the games table doesn't yet store
    (schedule density, possessions) and shouldn't gate the league's
    fitted status.

    Reads the on-disk ``{league}_constants.json`` directly and overlays
    over the registry. The registry-overlay path in ``_config`` only
    runs at import time, so a freshly-fit league wouldn't pick up
    until restart; this in-line read closes that gap."""
    cfg = dict(get_league_config(league))
    try:
        from . import _calibrate as _cal
        fitted = _cal.load(league)
    except Exception:
        fitted = None
    if fitted:
        for k in ("home_boost", "b2b_penalty", "margin_std", "total_std",
                   "league_avg_total", "league_avg_ppg", "league_avg_pace"):
            if fitted.get(k) is not None:
                cfg[k] = fitted[k]
    load_bearing = ("home_boost", "margin_std", "total_std",
                     "league_avg_total", "league_avg_ppg")
    is_fitted = all(cfg.get(k) is not None for k in load_bearing)
    return ({
        "home_boost": cfg.get("home_boost") or PRIOR_HOME_BOOST,
        "b2b_penalty": cfg.get("b2b_penalty") or PRIOR_B2B_PENALTY,
        "margin_std": cfg.get("margin_std") or PRIOR_MARGIN_STD,
        "total_std": cfg.get("total_std") or PRIOR_TOTAL_STD,
        "league_avg_total": cfg.get("league_avg_total") or PRIOR_LEAGUE_AVG_TOTAL,
        "league_avg_ppg": cfg.get("league_avg_ppg") or PRIOR_LEAGUE_AVG_PPG,
        "league_avg_pace": cfg.get("league_avg_pace") or PRIOR_LEAGUE_AVG_PACE,
    }, "fitted" if is_fitted else "prior")


# ── Team data loading ─────────────────────────────────────────

def _team_id_for(league: str, abbr: str) -> int | None:
    conn = get_conn(league)
    tbl = teams_table(league)
    row = conn.execute(
        f"SELECT id FROM {tbl} WHERE abbreviation = ? LIMIT 1",
        (abbr,),
    ).fetchone()
    return int(row["id"]) if row else None


def _team_full_ppg(league: str, team_id: int, season: int,
                    league_avg_ppg: float) -> dict:
    """Compute season scoring averages for a team. Mirrors the NBA
    helper but reads from the framework's games table (or nba_games for
    NBA via the table-name shim)."""
    conn = get_conn(league)
    tbl = games_table(league)
    rows = conn.execute(
        f"SELECT home_team_id, away_team_id, home_score, away_score "
        f"FROM {tbl} "
        f"WHERE season = ? AND status = 'final' "
        f"  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        f"  AND (home_team_id = ? OR away_team_id = ?)",
        (season, team_id, team_id),
    ).fetchall()
    if not rows:
        return {"games": 0, "ppg": league_avg_ppg, "opp_ppg": league_avg_ppg,
                "margin": 0.0, "home_ppg": None, "home_opp_ppg": None,
                "away_ppg": None, "away_opp_ppg": None}
    scored, allowed, h_s, h_a, a_s, a_a = [], [], [], [], [], []
    for r in rows:
        if r["home_team_id"] == team_id:
            scored.append(r["home_score"]); allowed.append(r["away_score"])
            h_s.append(r["home_score"]);   h_a.append(r["away_score"])
        else:
            scored.append(r["away_score"]); allowed.append(r["home_score"])
            a_s.append(r["away_score"]);   a_a.append(r["home_score"])
    n = len(scored)
    return {
        "games": n,
        "ppg": round(sum(scored) / n, 2),
        "opp_ppg": round(sum(allowed) / n, 2),
        "margin": round(sum(scored) / n - sum(allowed) / n, 2),
        "home_ppg": round(sum(h_s) / len(h_s), 2) if h_s else None,
        "home_opp_ppg": round(sum(h_a) / len(h_a), 2) if h_a else None,
        "away_ppg": round(sum(a_s) / len(a_s), 2) if a_s else None,
        "away_opp_ppg": round(sum(a_a) / len(a_a), 2) if a_a else None,
    }


def _recent_form(league: str, team_id: int, n: int = 10) -> dict:
    """Last-N game scoring form. Generic; reads framework games table."""
    conn = get_conn(league)
    tbl = games_table(league)
    rows = conn.execute(
        f"SELECT home_team_id, away_team_id, home_score, away_score "
        f"FROM {tbl} "
        f"WHERE status = 'final' "
        f"  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        f"  AND (home_team_id = ? OR away_team_id = ?) "
        f"ORDER BY date DESC LIMIT ?",
        (team_id, team_id, n),
    ).fetchall()
    if not rows:
        return {"recent_scored": None, "recent_allowed": None,
                "recent_margin": None, "recent_games": 0}
    scored, allowed = [], []
    for r in rows:
        is_home = r["home_team_id"] == team_id
        scored.append(r["home_score"] if is_home else r["away_score"])
        allowed.append(r["away_score"] if is_home else r["home_score"])
    return {
        "recent_scored": round(sum(scored) / len(scored), 2),
        "recent_allowed": round(sum(allowed) / len(allowed), 2),
        "recent_margin": round((sum(scored) - sum(allowed)) / len(scored), 2),
        "recent_games": len(scored),
    }


# ── College path (registry "model": "ratings") ───────────────

def _predict_ratings(league: str, home_abbr: str, away_abbr: str, *,
                     spread: float | None = None, total: float | None = None,
                     home_id=None, away_id=None, game_date: str | None = None,
                     neutral=None, conf_game=None, game_id=None, **_ignored) -> dict:
    """NCAAM / NCAAW: engine.basketball._ratings (opponent-adjusted O/D ratings) is both the
    factor leg and the decision. The GBM leg is still computed and logged to
    prediction_signals so it can be evaluated walk-forward, but carries no weight -- it lost
    to the ratings on 2025-26 (MintyBot 2026-10-26). Pass team ids: college abbreviations
    collide (UNI = Northern Iowa AND Union (TN), VAL, WIN, ...)."""
    from . import _ratings
    constants, _src = _resolve_constants(league)
    if home_id is None:
        home_id = _team_id_for(league, home_abbr)
    if away_id is None:
        away_id = _team_id_for(league, away_abbr)
    if home_id is None or away_id is None:
        return {"league": league, "constants_source": "ratings",
                "error": f"team not found (home={home_abbr}, away={away_abbr})",
                "home_expected": None, "away_expected": None}
    home_id, away_id = int(home_id), int(away_id)
    game_date = (game_date or et_today_str())[:10]
    r = _ratings.predict(league, home_id, away_id, game_date=game_date,
                         neutral=bool(neutral), conf=bool(conf_game))
    margin, total_pred = r["margin"], r["total"]
    sd_m, sd_t = r["margin_std"], r["total_std"]
    constants = dict(constants, margin_std=sd_m, total_std=sd_t, home_boost=r["home_court"])
    cover = _ratings.cover_prob(margin, sd_m, spread) if spread is not None else None
    over = _ratings.over_prob(total_pred, sd_t, total) if total is not None else None
    hr_, ar_ = r["home_rating"], r["away_rating"]
    reasoning = [
        f"ratings (net, pts vs avg D1): {home_abbr} {hr_['net']:+.1f}, {away_abbr} {ar_['net']:+.1f}"
        f" ({hr_['games']}/{ar_['games']} games this season)",
        "neutral site -- no home edge" if r["neutral"] else
        f"{home_abbr} home court +{r['home_court']:.1f} ({'conference' if r['conf_game'] else 'non-conference'})",
    ]
    signals = {"factor": {"margin": margin, "total": total_pred, "ml_home": r["ml_home"]},
               "gbm": {}, "mc": {}}
    try:
        from ._features import extract_features
        from ._gbm import predict as _gbm_predict
        feats = extract_features(league, {"home_team_id": home_id, "away_team_id": away_id,
                                          "date": game_date, "season": r["season"]})
        if feats:
            signals["gbm"] = {"margin": _gbm_predict(league, "margin", feats),
                              "total": _gbm_predict(league, "total_points", feats),
                              "ml_home": _gbm_predict(league, "home_win", feats)}
    except Exception as e:
        logger.debug("[%s] GBM side leg skipped: %s", league, e)
    try:
        _f, _g = signals["factor"], signals["gbm"]
        _rows = [("basketball", league, str(game_id) if game_id else None, game_date, home_id,
                  away_id, _mk, _f.get(_mk), None, _g.get(_mk))
                 for _mk in ("ml_home", "margin", "total")]
        _cn = get_conn(league)
        _cn.executemany(
            "INSERT INTO prediction_signals (sport, league, game_id, game_date, "
            "home_team_id, away_team_id, market, factor_prob, mc_prob, gbm_prob) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)", _rows)
        _cn.commit()
    except Exception as e:
        logger.debug("[%s] prediction_signals log skipped: %s", league, e)
    one = {"factor": 1.0, "gbm": 0.0, "mc": 0.0}
    return {
        "league": league,
        "constants_source": "ratings",
        "home_abbr": home_abbr,
        "away_abbr": away_abbr,
        "home_expected": round(r["home_expected"], 2),
        "away_expected": round(r["away_expected"], 2),
        "predicted_margin": round(margin, 2),
        "predicted_total": round(total_pred, 2),
        "ml_home": round(r["ml_home"], 4),
        "ml_away": round(1.0 - r["ml_home"], 4),
        "spread_cover_prob": round(cover, 4) if cover is not None else None,
        "over_prob": round(over, 4) if over is not None else None,
        "ensemble": {
            "ensemble": {"margin": margin, "total": total_pred, "ml_home": r["ml_home"],
                         "spread_cover_prob": cover, "over_prob": over},
            "signals": signals,
            "weights": {"ml_home": one, "margin": one, "total": one, "method": "ratings_only"},
        },
        "period": r["period"],
        "factors": {
            "home_rating": hr_, "away_rating": ar_,
            "home_court": r["home_court"], "neutral": r["neutral"], "conf_game": r["conf_game"],
            "season": r["season"], "phase": r["phase"], "fit_finals": r["fit_finals"],
            "home_recent": _recent_form(league, home_id),
            "away_recent": _recent_form(league, away_id),
            "constants": constants,
        },
        "reasoning": reasoning,
    }


# ── Core prediction ───────────────────────────────────────────

def predict_full(league: str, home_abbr: str, away_abbr: str,
                  spread: float | None = None,
                  total: float | None = None,
                  season: int | None = None,
                  **kwargs) -> dict:
    """Predict full-game outcome for ``league``.

    Args:
        league:    Key from LEAGUE_REGISTRY ('nba' / 'wnba' / 'euroleague' / ...)
        home_abbr: Home team abbreviation (matches teams.abbreviation)
        away_abbr: Away team abbreviation
        spread:    Posted home spread (negative = home favored). Used for
                   spread cover prob; doesn't anchor the model unless the
                   caller asks for a market-blended output.
        total:     Posted O/U total for over/under prob.
        season:    Season start year (defaults to current calendar logic).
        **kwargs:  Forwarded to per-league module when dispatching (e.g.
                   NBA accepts ``backtest=True``).

    Returns:
        Dict with home_expected, away_expected, predicted_margin,
        predicted_total, ml_home/away, spread_cover_prob, over_prob,
        constants_source ('fitted' or 'prior'), reasoning, factors.

    NBA dispatch: NBA has Q1 + playoff series + B2B + injury layers the
    generic predictor doesn't model. We delegate to the existing
    ``engine.nba_predict.predict_full`` so NBA's behavior is preserved
    bit-for-bit while non-NBA leagues use the framework's generic path.
    The dispatch is contained here; callers see one unified API.
    """
    if league == "nba":
        from ..nba_predict import predict_full as _nba_predict
        result = _nba_predict(home_abbr, away_abbr,
                               spread=spread, total=total,
                               season=season, **kwargs)
        # Annotate with the framework's standard fields so downstream
        # code can read them uniformly across leagues. NBA's existing
        # output already has every key we need; just tag the source.
        if isinstance(result, dict):
            result.setdefault("league", "nba")
            result.setdefault("constants_source", "fitted")
            result.setdefault("home_abbr", home_abbr)
            result.setdefault("away_abbr", away_abbr)
        return result

    if get_league_config(league).get("model") == "ratings":
        return _predict_ratings(league, home_abbr, away_abbr, spread=spread, total=total,
                                **kwargs)

    constants, source = _resolve_constants(league)
    if season is None:
        # The league's DB season label for today (college labels by END year).
        season = season_label(league)

    home_id = _team_id_for(league, home_abbr)
    away_id = _team_id_for(league, away_abbr)
    league_avg_ppg = constants["league_avg_ppg"]

    if home_id is None or away_id is None:
        return {
            "league": league,
            "constants_source": source,
            "error": f"team not found (home={home_abbr}, away={away_abbr})",
            "home_expected": None, "away_expected": None,
        }

    home = _team_full_ppg(league, home_id, season, league_avg_ppg)
    away = _team_full_ppg(league, away_id, season, league_avg_ppg)
    home_recent = _recent_form(league, home_id)
    away_recent = _recent_form(league, away_id)

    # ── Per-team pace + ratings (B1 — supersedes the PPG-only path
    # when box-score data is available). Falls back to the PPG path
    # only when the team has zero box-scored games on file. ──
    from ._team_stats import team_rates
    home_rates = team_rates(league, home_id)
    away_rates = team_rates(league, away_id)

    reasoning: list[str] = []
    if source == "prior":
        reasoning.append(
            f"{league} constants not yet fitted — using basketball-wide priors"
        )

    # 1) Opponent-adjusted base scoring.
    if home_rates["n"] > 0 and away_rates["n"] > 0:
        # Box-score path: pace × (off_rtg × opp_def_rtg / league_rtg) / 100
        # — the standard adjusted-rating model. Captures pace explicitly
        # rather than burying it in PPG.
        m_pace = (home_rates["pace"] + away_rates["pace"]) / 2.0
        league_rtg = constants["league_avg_ppg"] * 100.0 / max(
            constants["league_avg_pace"] or 95.0, 1.0,
        )
        home_expected = m_pace * (home_rates["ortg"] * away_rates["drtg"]
                                   / league_rtg) / 100.0
        away_expected = m_pace * (away_rates["ortg"] * home_rates["drtg"]
                                   / league_rtg) / 100.0
        reasoning.append(
            f"matchup pace {m_pace:.1f} possessions; "
            f"home ORtg {home_rates['ortg']:.1f}, "
            f"away DRtg {away_rates['drtg']:.1f}"
        )
    else:
        # Fallback PPG path (cold-start or no box-scores yet).
        home_off = home.get("home_ppg") or home["ppg"]
        home_def = home.get("home_opp_ppg") or home["opp_ppg"]
        away_off = away.get("away_ppg") or away["ppg"]
        away_def = away.get("away_opp_ppg") or away["opp_ppg"]
        league_team_avg = constants["league_avg_total"] / 2.0
        if league_team_avg > 0:
            home_expected = (home_off * away_def) / league_team_avg
            away_expected = (away_off * home_def) / league_team_avg
        else:
            home_expected, away_expected = home_off, away_off

    # 2) Home court boost (split half-half across the margin).
    hb = constants["home_boost"]
    home_expected += hb / 2
    away_expected -= hb / 2
    if hb >= 1.0:
        reasoning.append(f"{home_abbr} home-court boost applied (+{hb:.1f})")

    # 2.5) Roster availability — subtract OUT players' expected scoring.
    # LIVE uses the flat per-head penalty; the player-WEIGHTED penalty
    # (each OUT player's actual PPG, replacement-shrunk — a star costs
    # ~0.65*PPG, a benchwarmer ~nothing) is computed and shadow-logged
    # alongside so the flip-to-live gate can prove weighting beats flat
    # before it moves money (2026-08-09, Austin "lets upgrade").
    from ._injuries import (
        out_count_for_team, margin_penalty_for_team,
        weighted_penalty_for_team, log_avail_shadow, USE_WEIGHTED_AVAIL,
    )
    home_out = out_count_for_team(league, home_id)
    away_out = out_count_for_team(league, away_id)
    flat_home = margin_penalty_for_team(league, home_id) if home_out else 0.0
    flat_away = margin_penalty_for_team(league, away_id) if away_out else 0.0
    wtd_home, det_home = weighted_penalty_for_team(league, home_id)
    wtd_away, det_away = weighted_penalty_for_team(league, away_id)
    try:
        log_avail_shadow(league, home_abbr, away_abbr, home_id, away_id,
                         et_today_str(), flat_home, flat_away,
                         wtd_home, wtd_away, det_home, det_away)
    except Exception:
        pass
    pen_home = wtd_home if USE_WEIGHTED_AVAIL else flat_home
    pen_away = wtd_away if USE_WEIGHTED_AVAIL else flat_away
    if pen_home:
        home_expected -= pen_home
        reasoning.append(f"{home_abbr} missing {home_out} (-{pen_home:.1f} pts)")
    if pen_away:
        away_expected -= pen_away
        reasoning.append(f"{away_abbr} missing {away_out} (-{pen_away:.1f} pts)")

    # 3) Recent form blend (70/30 with season).
    if home_recent["recent_games"] >= 3 and home_recent["recent_scored"]:
        home_expected = 0.30 * home_expected + 0.70 * home_recent["recent_scored"]
    if away_recent["recent_games"] >= 3 and away_recent["recent_scored"]:
        away_expected = 0.30 * away_expected + 0.70 * away_recent["recent_scored"]

    # 4) Quality / margin signal — blend recent margin into the gap.
    if home_recent.get("recent_margin") is not None and away_recent.get("recent_margin") is not None:
        rm_diff = (home_recent["recent_margin"] - away_recent["recent_margin"])
        # Half-weight pull toward recent quality differential.
        home_expected += rm_diff / 4
        away_expected -= rm_diff / 4

    # ── Probability outputs ──────────────────────────────────
    margin = home_expected - away_expected
    total_pred = home_expected + away_expected

    sigma_margin = constants["margin_std"]
    sigma_total = constants["total_std"]

    # Defensive: a calibration write that produced sigma <= 0 (or a
    # broken registry override) would previously make ml_home/spread/
    # total collapse to 0.5 / 0.0 / None — disguised as a real
    # prediction and impossible to spot downstream. Fall back to the
    # league-agnostic priors instead and warn so it's visible in logs.
    if not sigma_margin or sigma_margin <= 0:
        logger.warning(
            "[%s] margin_std=%s invalid; falling back to PRIOR_MARGIN_STD",
            league, sigma_margin,
        )
        sigma_margin = PRIOR_MARGIN_STD
    if not sigma_total or sigma_total <= 0:
        logger.warning(
            "[%s] total_std=%s invalid; falling back to PRIOR_TOTAL_STD",
            league, sigma_total,
        )
        sigma_total = PRIOR_TOTAL_STD

    # ML home: P(margin > 0)
    ml_home = _norm_cdf(margin / sigma_margin)
    ml_away = 1.0 - ml_home

    spread_cover_prob = None
    if spread is not None:
        # Convention: home spread is negative when home is favored.
        # P(home wins by more than -spread) = P(margin > -spread)
        # = P((margin - (-spread)) / sigma > 0) = 1 - cdf((-spread - margin)/sigma)
        z = (margin - (-spread)) / sigma_margin
        spread_cover_prob = _norm_cdf(z)

    over_prob = None
    if total is not None:
        z = (total_pred - total) / sigma_total
        over_prob = _norm_cdf(z)

    # ── Ensemble blend (B3) — factor + GBM + MC. Available when GBM
    # has been trained for this league; else returns None so callers
    # know the league is factor-only.
    ensemble_block = None
    try:
        from ._features import extract_features
        from ._ensemble import blend
        # Synthesize a "game" dict for feature extraction. We use today
        # as the date — this is a pre-game prediction, not a backtest.
        game_for_feat = {
            "home_team_id": home_id, "away_team_id": away_id,
            "date": et_today_str(),
            "season": season,
        }
        features = extract_features(league, game_for_feat)
        if not features:
            # Surface the cold-start case so ops knows ensemble is
            # silently degrading to factor-only for this league. The
            # outer try/except only catches actual exceptions, not
            # this logical None — without this log, an entire league
            # could skip ensemble for weeks unnoticed.
            logger.info(
                "[%s] feature extraction returned None for "
                "home=%s away=%s — falling back to factor-only",
                league, home_abbr, away_abbr,
            )
        if features:
            factor_view = {
                "predicted_margin": margin,
                "predicted_total": total_pred,
                "ml_home": ml_home,
                "home_expected": home_expected,
                "away_expected": away_expected,
                "factors": {"constants": constants},
            }
            ensemble_block = blend(
                league, factor_view, features,
                home_id=home_id, away_id=away_id,
                spread=spread, total=total,
            )
            # Log the 3 component predictions (factor/MC/GBM) per market for the
            # self-improvement loop. Fail-safe: a logging error can NEVER touch the bet. (2026-08-26)
            try:
                from ._db import get_conn as _gc
                from datetime import datetime as _dt
                _sig = (ensemble_block or {}).get("signals") or {}
                _f = _sig.get("factor") or {}; _g = _sig.get("gbm") or {}; _m = _sig.get("mc") or {}
                _gd = _dt.utcnow().strftime("%Y-%m-%d")
                _rows = [("basketball", league, None, _gd, home_id, away_id, _mk,
                          _f.get(_mk), _m.get(_mk), _g.get(_mk))
                         for _mk in ("ml_home", "margin", "total")
                         if any(_x.get(_mk) is not None for _x in (_f, _m, _g))]
                if _rows:
                    _cn = _gc(league)
                    _cn.executemany(
                        "INSERT INTO prediction_signals (sport, league, game_id, game_date, "
                        "home_team_id, away_team_id, market, factor_prob, mc_prob, gbm_prob) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?)", _rows)
                    _cn.commit()
            except Exception as _se:
                logger.debug("[%s] prediction_signals log skipped: %s", league, _se)
    except Exception as e:
        logger.debug("[%s] ensemble blend failed: %s", league, e)

    return {
        "league": league,
        "constants_source": source,
        "home_abbr": home_abbr,
        "away_abbr": away_abbr,
        "home_expected": round(home_expected, 2),
        "away_expected": round(away_expected, 2),
        "predicted_margin": round(margin, 2),
        "predicted_total": round(total_pred, 2),
        "ml_home": round(ml_home, 4),
        "ml_away": round(ml_away, 4),
        "ensemble": ensemble_block,
        "spread_cover_prob": (round(spread_cover_prob, 4)
                                if spread_cover_prob is not None else None),
        "over_prob": (round(over_prob, 4) if over_prob is not None else None),
        "factors": {
            "home_ppg": home["ppg"], "away_ppg": away["ppg"],
            "home_opp_ppg": home["opp_ppg"], "away_opp_ppg": away["opp_ppg"],
            "home_recent": home_recent, "away_recent": away_recent,
            "home_rates": home_rates, "away_rates": away_rates,
            "constants": constants,
        },
        "reasoning": reasoning,
    }
