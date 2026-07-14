"""AIHL predictor — Poisson scoring model.

Per-team off/def goals-per-game (from this season's games), regressed
toward league mean. Score grid (16×16) → P(home_win) + expected
totals. No GBM blend (sample too thin to train one); status sits at
``pending_calibration`` until ROI proven live.
"""
from __future__ import annotations

import json
import logging
import math
from pathlib import Path

from ...predictor import GameContext, Prediction, register
from .db import get_conn, get_team_by_name
from . import GAMES_TABLE

logger = logging.getLogger(__name__)


# Defaults — overlaid by data/hockey/aihl_constants.json at import
# time. The values below are pre-calibration cold-start priors; the
# JSON values (fitted from the SofaScore 5-season backfill 2026-05-14)
# replace them once the file exists.
LEAGUE_AVG_GPG: float = 4.55
HOME_BOOST: float = 0.50
LEAGUE_AVG_TOTAL: float = 9.10
MAX_GOALS: int = 12
MIN_GAMES_FULL_TRUST: int = 10

_CONSTANTS_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "hockey" / "aihl_constants.json"
)


def _load_constants() -> None:
    """Overlay disk-persisted constants onto module globals so the
    calibrator can refresh them without a code edit."""
    global LEAGUE_AVG_GPG, HOME_BOOST, LEAGUE_AVG_TOTAL
    if not _CONSTANTS_PATH.exists():
        return
    try:
        cfg = json.loads(_CONSTANTS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return
    if cfg.get("league_avg_gpg") is not None:
        LEAGUE_AVG_GPG = float(cfg["league_avg_gpg"])
    if cfg.get("home_boost") is not None:
        HOME_BOOST = float(cfg["home_boost"])
    if cfg.get("league_avg_total") is not None:
        LEAGUE_AVG_TOTAL = float(cfg["league_avg_total"])


_load_constants()


def _team_rates(team_id: int) -> tuple[float, float, int]:
    """Return ``(off_rate, def_rate, games_played)`` from this season's
    finalized games. Walks both home and away rows so the rate isn't
    skewed by an unbalanced split."""
    conn = get_conn()
    rows = conn.execute(
        f"SELECT home_team_id, away_team_id, home_score, away_score "
        f"FROM {GAMES_TABLE} "
        f"WHERE status = 'final' AND home_score IS NOT NULL "
        f"  AND (home_team_id = ? OR away_team_id = ?)",
        (team_id, team_id),
    ).fetchall()
    if not rows:
        return LEAGUE_AVG_GPG, LEAGUE_AVG_GPG, 0
    gf = ga = 0
    for r in rows:
        if r["home_team_id"] == team_id:
            gf += r["home_score"] or 0
            ga += r["away_score"] or 0
        else:
            gf += r["away_score"] or 0
            ga += r["home_score"] or 0
    n = len(rows)
    return gf / n, ga / n, n


def _shrink(rate: float, n: int, prior: float) -> float:
    """Sample-size shrinkage toward the league prior."""
    if n >= MIN_GAMES_FULL_TRUST:
        return rate
    w = n / MIN_GAMES_FULL_TRUST
    return w * rate + (1 - w) * prior


def _poisson_pmf(lam: float, k: int) -> float:
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def predict_matchup(home_key: str, away_key: str,
                    season: int | None = None) -> dict | None:
    """Predict one AIHL fixture by team name. Returns the same dict
    shape as ``ahl.predict.predict_matchup`` so the picks engine and
    consumer protocols are sport-agnostic."""
    home = get_team_by_name(home_key)
    away = get_team_by_name(away_key)
    if not home or not away:
        return None
    h_off, h_def, h_n = _team_rates(home["id"])
    a_off, a_def, a_n = _team_rates(away["id"])
    h_off = _shrink(h_off, h_n, LEAGUE_AVG_GPG)
    h_def = _shrink(h_def, h_n, LEAGUE_AVG_GPG)
    a_off = _shrink(a_off, a_n, LEAGUE_AVG_GPG)
    a_def = _shrink(a_def, a_n, LEAGUE_AVG_GPG)

    home_xg = 0.5 * (h_off + a_def) + HOME_BOOST
    away_xg = 0.5 * (a_off + h_def)
    home_xg = max(home_xg, 0.4)
    away_xg = max(away_xg, 0.4)

    # 16×16 score grid → win probs.
    p_h_win = p_a_win = p_tie = 0.0
    for hg in range(MAX_GOALS):
        ph = _poisson_pmf(home_xg, hg)
        for ag in range(MAX_GOALS):
            cell = ph * _poisson_pmf(away_xg, ag)
            if hg > ag:   p_h_win += cell
            elif hg < ag: p_a_win += cell
            else:         p_tie  += cell
    # Ties resolve ~50/50 OT/SO in low-volume leagues — split evenly.
    p_h_ml = p_h_win + 0.5 * p_tie
    p_a_ml = p_a_win + 0.5 * p_tie
    total = home_xg + away_xg

    return {
        "home": {"name": home.get("full_name") or home_key,
                  "abbreviation": home.get("abbreviation") or "",
                  "key": home_key},
        "away": {"name": away.get("full_name") or away_key,
                  "abbreviation": away.get("abbreviation") or "",
                  "key": away_key},
        "expected_score": {"home": round(home_xg, 2),
                            "away": round(away_xg, 2)},
        "total": round(total, 2),
        "spread": round(away_xg - home_xg, 2),
        "win_prob": {"home": round(p_h_ml, 4), "away": round(p_a_ml, 4)},
        "regulation_draw_prob": round(p_tie, 4),
        "samples": {"home_games": h_n, "away_games": a_n},
        "reasoning": [
            f"home off/def {h_off:.2f}/{h_def:.2f} (n={h_n})",
            f"away off/def {a_off:.2f}/{a_def:.2f} (n={a_n})",
        ],
    }


class AIHLPredictor:
    name = "aihl_factor"

    def predict(self, game: GameContext) -> Prediction:
        try:
            raw = predict_matchup(game.home, game.away, season=game.season)
        except Exception as e:
            logger.warning("AIHLPredictor failed for %s @ %s: %s",
                            game.away, game.home, e)
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, error=str(e),
                                source="factor")
        if not raw:
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away,
                                error="team lookup failed",
                                source="factor")
        es = raw["expected_score"]
        wp = raw["win_prob"]
        return Prediction(
            sport=game.sport, home=game.home, away=game.away,
            home_expected=es["home"], away_expected=es["away"],
            margin=raw["spread"], total=raw["total"],
            ml_home=wp["home"], ml_away=wp["away"],
            signals={"samples": raw["samples"], "factor": {
                "ml_home": wp["home"], "margin": raw["spread"],
                "total": raw["total"]}},
            reasoning=raw["reasoning"], source="factor",
        )


register("aihl", AIHLPredictor())
