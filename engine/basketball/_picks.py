"""Generic basketball picks generator.

Converts ``predict_full`` output into ML / SPREAD / TOTAL pick candidates
and runs them through the universal ``engine.picks_core.score_pick``
pipeline (juice wall + odds cap + belief gate + calibration + edge
ceiling + edge floor — see ``project_picks_core.md``).

Sport string passed to picks_core is the league key itself ('wnba',
'ncaam', 'euroleague', ...). picks_core's per-sport lookup tables fall
through to defaults for unknown sports — appropriate cold-start
behavior. As leagues accumulate settled picks, calibration / edge
floors / odds caps will train per-league through the normal calibration
refit cadence.

NBA's existing engine.nba_picks stays the source of truth for NBA
itself — this module is for the framework-managed leagues. NBA refactor
is a separate task (#209).
"""
from __future__ import annotations

import logging
from typing import Iterable

from ._config import get_league_config
from ._db import get_conn, games_table

logger = logging.getLogger(__name__)


# ── Juice wall ────────────────────────────────────────────────
# Default basketball juice wall mirrors NBA's -200. Per-league override
# possible later if a small league shows different juice patterns.
DEFAULT_JUICE_WALL = -200


def _juice_wall_for(league: str) -> int:
    cfg = get_league_config(league)
    return int(cfg.get("juice_wall") or DEFAULT_JUICE_WALL)


def _odds_dict_to_int(odds: int | float | str | None) -> int | None:
    """Coerce HR's odds (American int or sometimes a string) to int.
    Returns None on bad input — pick is skipped silently."""
    if odds is None:
        return None
    try:
        return int(odds)
    except (TypeError, ValueError):
        return None


# ── Pick emit ────────────────────────────────────────────────

def _emit(candidates: list[dict], league: str) -> list[dict]:
    """Score every candidate through picks_core.score_pick and return
    the ones that pass every gate.

    Calibration is applied INSIDE picks_core via framework_calibration
    (which reads the league's walk-forward bucket data and shrinks toward
    avg_pred via beta-binomial). Earlier this module pre-calibrated via
    ``_calibration_apply`` and picks_core re-calibrated via
    ``framework_calibration`` — double shrinkage that pushed every
    high-conviction pick to the same bucket-midpoint cap (e.g. NZ NBL's
    NEL @ FRA flipping sides nightly because both teams' raw probs sat
    on the 0.50 boundary)."""
    from ..picks_core import score_pick
    juice_wall = _juice_wall_for(league)
    out: list[dict] = []
    for c in candidates:
        try:
            scored = score_pick(c, sport=league, juice_wall=juice_wall)
        except Exception as e:
            logger.warning("basketball %s score_pick failed for %s/%s: %s",
                           league, c.get("type"), c.get("pick"), e)
            continue
        if scored is not None:
            out.append(scored)
    return out


def generate_picks(league: str, prediction: dict, odds: dict | None,
                    *, game_id: str | None = None) -> list[dict]:
    """Produce ML / SPREAD / TOTAL picks for one game.

    Args:
        league:     LEAGUE_REGISTRY key
        prediction: Output of ``predict_full(league, ...)``
        odds:       Sportsbook odds dict in the conventional shape:
                        {ml: {home: int, away: int},
                         spread: {home_pt: float, home_odds: int,
                                  away_pt: float, away_odds: int},
                         total: {line: float, over_odds: int, under_odds: int}}
                    None or missing markets → those picks are skipped.
        game_id:    Carried through to the pick dict for tracker joins.

    Returns:
        List of pick dicts (only the ones that cleared every gate).

    NBA dispatch: NBA has Q1, ALT spreads/totals, derivative markets,
    and ROI-tuned reliability adjustments the framework doesn't replicate.
    For NBA we delegate to ``engine.nba_picks.generate_full_picks`` so
    NBA's full-game output is preserved bit-for-bit.
    """
    if league == "nba":
        from ..nba_picks import generate_full_picks as _nba_full
        # NBA's signature is (home_abbr, away_abbr, odds, ..., pred=).
        # We pass the prediction back through ``pred=`` so NBA doesn't
        # redo the predictor work, then it runs its native ensemble +
        # ALT pipeline. game_id stays implicit (NBA wires it from the
        # backend's bet payload, not this call).
        home = prediction.get("home_abbr")
        away = prediction.get("away_abbr")
        if not home or not away:
            return []
        return _nba_full(home, away, odds=odds, pred=prediction)

    if not prediction or prediction.get("error"):
        return []
    odds = odds or {}
    candidates: list[dict] = []
    home = prediction.get("home_abbr") or "HOME"
    away = prediction.get("away_abbr") or "AWAY"

    # Prefer the ensemble blend (factor + GBM + MC) when it ran;
    # otherwise fall back to factor-only. Critical for calibration to
    # match: the walk-forward in _walkforward.seed_calibration uses
    # the blended ml_home/margin/total to build the bucket table.
    # Live picks used to read factor-only `prediction["ml_home"]`, so
    # the calibration shrinkage was being applied to a distribution
    # that didn't match what fit it — fixed 2026-05-17.
    ensemble = prediction.get("ensemble") or {}
    pred_ml_home = (ensemble.get("ml_home")
                    if ensemble.get("ml_home") is not None
                    else prediction.get("ml_home"))
    pred_margin = (ensemble.get("margin")
                   if ensemble.get("margin") is not None
                   else prediction.get("predicted_margin"))
    pred_total = (ensemble.get("total")
                  if ensemble.get("total") is not None
                  else prediction.get("predicted_total"))

    # ── Moneyline ───────────────────────────────────────────
    ml_odds = odds.get("ml") or {}
    ml_home_p = pred_ml_home
    if ml_home_p is not None:
        h_o = _odds_dict_to_int(ml_odds.get("home"))
        if h_o:
            candidates.append({
                "type": "ML", "pick": home,
                "raw_prob": ml_home_p, "odds": h_o,
                "game_id": game_id,
            })
        a_o = _odds_dict_to_int(ml_odds.get("away"))
        if a_o:
            candidates.append({
                "type": "ML", "pick": away,
                "raw_prob": 1.0 - ml_home_p, "odds": a_o,
                "game_id": game_id,
            })

    # ── Spread ──────────────────────────────────────────────
    sp = odds.get("spread") or {}
    if sp.get("home_pt") is not None:
        # Re-call predictor for the spread cover prob? No — the
        # prediction dict already carries one if the predictor was
        # called with `spread=`. If not present, derive a fresh
        # cover prob using the predicted margin and the line.
        # `pred_margin` is the ensemble blend when available; factor
        # otherwise (see the moneyline section above for rationale).
        margin = pred_margin
        constants = (prediction.get("factors") or {}).get("constants") or {}
        sigma_m = constants.get("margin_std") or 16.0
        if margin is not None and sigma_m > 0:
            import math
            def _ncdf(x): return 0.5 * (1 + math.erf(x / math.sqrt(2)))
            home_pt = float(sp["home_pt"])
            # P(home covers) = P(margin > -home_pt)
            z = (margin - (-home_pt)) / sigma_m
            p_home_cover = _ncdf(z)
            h_o = _odds_dict_to_int(sp.get("home_odds"))
            if h_o:
                candidates.append({
                    "type": "SPREAD",
                    "pick": f"{home} {home_pt:+g}",
                    "raw_prob": p_home_cover, "odds": h_o,
                    "game_id": game_id,
                })
            away_pt = float(sp.get("away_pt") or -home_pt)
            a_o = _odds_dict_to_int(sp.get("away_odds"))
            if a_o:
                candidates.append({
                    "type": "SPREAD",
                    "pick": f"{away} {away_pt:+g}",
                    "raw_prob": 1.0 - p_home_cover, "odds": a_o,
                    "game_id": game_id,
                })

    # ── Total ───────────────────────────────────────────────
    tot = odds.get("total") or {}
    if tot.get("line") is not None:
        # `pred_total` is the ensemble blend when available; factor
        # otherwise.
        total_pred = pred_total
        constants = (prediction.get("factors") or {}).get("constants") or {}
        sigma_t = constants.get("total_std") or 21.0
        if total_pred is not None and sigma_t > 0:
            import math
            def _ncdf(x): return 0.5 * (1 + math.erf(x / math.sqrt(2)))
            line = float(tot["line"])
            p_over = _ncdf((total_pred - line) / sigma_t)
            o_o = _odds_dict_to_int(tot.get("over_odds"))
            if o_o:
                candidates.append({
                    "type": "TOTAL",
                    "pick": f"Over {line}",
                    "raw_prob": p_over, "odds": o_o,
                    "direction": "over",
                    "game_id": game_id,
                })
            u_o = _odds_dict_to_int(tot.get("under_odds"))
            if u_o:
                candidates.append({
                    "type": "TOTAL",
                    "pick": f"Under {line}",
                    "raw_prob": 1.0 - p_over, "odds": u_o,
                    "direction": "under",
                    "game_id": game_id,
                })

    # ── Q1 markets ──────────────────────────────────────────
    # WNBA + any future league with fitted Q1 constants emits Q1_ML /
    # Q1_SPREAD / Q1_TOTAL candidates. Skipped for leagues without Q1
    # constants in LEAGUE_REGISTRY (the fitted-overrides loader populates
    # them at import time from the per-league JSON).
    q1 = odds.get("q1") or {}
    cfg = get_league_config(league)
    q1_margin_std = cfg.get("q1_margin_std")
    q1_total_std = cfg.get("q1_total_std")
    q1_home_boost = cfg.get("q1_home_boost")
    q1_avg_total = cfg.get("q1_avg_total")
    if q1 and q1_margin_std and q1_total_std and q1_avg_total is not None:
        import math
        def _ncdf(x): return 0.5 * (1 + math.erf(x / math.sqrt(2)))
        # Derive Q1 predicted margin + total from the full-game numbers.
        # Q1 is 1/4 of the game in minutes (10 of 40 for WNBA) so the
        # scaling factor approximates 0.25 — but historical WNBA shows
        # Q1 total averages 41.86 vs full 163.38 → 25.6% share. Use the
        # share to scale, and the fitted q1_home_boost as the home edge.
        full_margin = prediction.get("predicted_margin")
        full_total = prediction.get("predicted_total")
        full_avg_total = cfg.get("league_avg_total") or 163.0
        q1_share = q1_avg_total / float(full_avg_total) if full_avg_total else 0.256
        q1_margin = ((full_margin or 0.0) * q1_share) if full_margin is not None else None
        q1_total = ((full_total or full_avg_total) * q1_share
                    if full_total is not None else q1_avg_total)
        # Home-side Q1 advantage: a fraction of the fitted boost moves
        # with the matchup quality (favored team gets a smaller bump
        # since the full-margin already prices that in).
        if q1_margin is not None:
            q1_margin = q1_margin + q1_home_boost
        # Q1 ML — Draw-No-Bet refunds ties; we treat tie prob as ~3%
        # and compute side-win probs against margin > 0.
        q1_ml = q1.get("ml") or {}
        if q1_ml and q1_margin is not None:
            p_home_win_q1 = _ncdf(q1_margin / q1_margin_std)
            h_o = _odds_dict_to_int(q1_ml.get("home"))
            if h_o:
                candidates.append({
                    "type": "Q1_ML", "pick": f"{home} Q1",
                    "raw_prob": p_home_win_q1, "odds": h_o,
                    "game_id": game_id,
                })
            a_o = _odds_dict_to_int(q1_ml.get("away"))
            if a_o:
                candidates.append({
                    "type": "Q1_ML", "pick": f"{away} Q1",
                    "raw_prob": 1.0 - p_home_win_q1, "odds": a_o,
                    "game_id": game_id,
                })
        # Q1 spread
        q1_sp = q1.get("spread") or {}
        if q1_sp.get("home_pt") is not None and q1_margin is not None:
            home_pt = float(q1_sp["home_pt"])
            z = (q1_margin - (-home_pt)) / q1_margin_std
            p_home_cover = _ncdf(z)
            h_o = _odds_dict_to_int(q1_sp.get("home_odds"))
            if h_o:
                candidates.append({
                    "type": "Q1_SPREAD",
                    "pick": f"{home} {home_pt:+g} Q1",
                    "raw_prob": p_home_cover, "odds": h_o,
                    "game_id": game_id,
                })
            away_pt = float(q1_sp.get("away_pt") or -home_pt)
            a_o = _odds_dict_to_int(q1_sp.get("away_odds"))
            if a_o:
                candidates.append({
                    "type": "Q1_SPREAD",
                    "pick": f"{away} {away_pt:+g} Q1",
                    "raw_prob": 1.0 - p_home_cover, "odds": a_o,
                    "game_id": game_id,
                })
        # Q1 total
        q1_tot = q1.get("total") or {}
        if q1_tot.get("line") is not None and q1_total is not None:
            line = float(q1_tot["line"])
            p_over = _ncdf((q1_total - line) / q1_total_std)
            o_o = _odds_dict_to_int(q1_tot.get("over_odds"))
            if o_o:
                candidates.append({
                    "type": "Q1_TOTAL",
                    "pick": f"Over {line} Q1",
                    "raw_prob": p_over, "odds": o_o,
                    "direction": "over",
                    "game_id": game_id,
                })
            u_o = _odds_dict_to_int(q1_tot.get("under_odds"))
            if u_o:
                candidates.append({
                    "type": "Q1_TOTAL",
                    "pick": f"Under {line} Q1",
                    "raw_prob": 1.0 - p_over, "odds": u_o,
                    "direction": "under",
                    "game_id": game_id,
                })

    return _emit(candidates, league)
