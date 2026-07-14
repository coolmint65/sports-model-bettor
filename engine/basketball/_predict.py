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

from ._config import get_league_config
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

    constants, source = _resolve_constants(league)
    if season is None:
        # Use league's primary season-start month (first entry of season_months)
        cfg = get_league_config(league)
        months = cfg.get("season_months") or (10,)
        primary_start = months[0]
        now = datetime.now()
        season = now.year if now.month >= primary_start else now.year - 1

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

    # 2.5) Roster availability — subtract OUT players' expected scoring
    # contribution. Per-league penalty calibrated for roster size; the
    # B2 GBM features will refine this with per-player VORP.
    from ._injuries import (
        out_count_for_team, margin_penalty_for_team,
    )
    home_out = out_count_for_team(league, home_id)
    away_out = out_count_for_team(league, away_id)
    if home_out:
        pen = margin_penalty_for_team(league, home_id)
        home_expected -= pen
        reasoning.append(f"{home_abbr} missing {home_out} (-{pen:.1f} pts)")
    if away_out:
        pen = margin_penalty_for_team(league, away_id)
        away_expected -= pen
        reasoning.append(f"{away_abbr} missing {away_out} (-{pen:.1f} pts)")

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
