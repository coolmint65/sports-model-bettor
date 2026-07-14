"""AHL prediction engine.

Two-stage Poisson model. For each team we compute:
    off_rate = goals scored per game (this season)
    def_rate = goals allowed per game (this season)

Expected goals per side blends offense vs opponent's defense, regressed
toward league mean by sample size — stops 5-game cold-spells from
producing wild predictions early in the season.

Win prob comes from a 16x16 score matrix (Poisson home goals * Poisson
away goals); ties get distributed proportionally to the OT/SO winner
distribution observed in the league (roughly 50/50). Calibrated home
boost + sigma constants are persisted to data/hockey/ahl_constants.json
by the calibrator and overlaid at predict time.

The output mirrors NHL's predict_matchup() shape so the same Predictor
adapter pattern + downstream consumers work without per-league branching.
"""
from __future__ import annotations

import json
import logging
import math
from datetime import datetime
from pathlib import Path

from ...predictor import GameContext, Prediction, register
from .db import get_conn, get_team_by_name
from . import GAMES_TABLE
from ..._tz import et_today_str

logger = logging.getLogger(__name__)

# Defaults — overridden by the on-disk constants JSON when calibration runs.
LEAGUE_AVG_GPG = 3.20         # goals per team per game (typical AHL ~3.0-3.4)
HOME_BOOST = 0.15             # extra goals scored at home
LEAGUE_AVG_TOTAL = 6.40       # goals both sides combined
MAX_GOALS = 12                # truncation for the score matrix
MIN_GAMES_FULL_TRUST = 30     # sample-size shrinkage threshold

_CONSTANTS_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "hockey" / "ahl_constants.json"
)


def _load_constants() -> None:
    """Overlay disk-persisted constants onto module globals."""
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


def _team_rates(team_id: int, season: int) -> tuple[float, float, int]:
    """(off_rate, def_rate, games) for a team in a season — straight
    league-table averages over completed games."""
    rows = get_conn().execute(
        f"SELECT home_team_id, away_team_id, home_score, away_score "
        f"FROM {GAMES_TABLE} WHERE season = ? AND status = 'final' "
        f"  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        f"  AND (home_team_id = ? OR away_team_id = ?)",
        (season, team_id, team_id),
    ).fetchall()
    if not rows:
        return LEAGUE_AVG_GPG, LEAGUE_AVG_GPG, 0
    scored, allowed = [], []
    for r in rows:
        if r["home_team_id"] == team_id:
            scored.append(r["home_score"]); allowed.append(r["away_score"])
        else:
            scored.append(r["away_score"]); allowed.append(r["home_score"])
    return (sum(scored) / len(scored), sum(allowed) / len(allowed), len(scored))


def _shrunk(rate: float, n_games: int) -> float:
    """Bayes-style shrinkage: pull toward league mean when sample is thin."""
    if n_games <= 0:
        return LEAGUE_AVG_GPG
    w = min(1.0, n_games / MIN_GAMES_FULL_TRUST)
    return w * rate + (1 - w) * LEAGUE_AVG_GPG


def _poisson(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def _score_matrix(home_xg: float, away_xg: float) -> list[list[float]]:
    return [[_poisson(home_xg, h) * _poisson(away_xg, a)
              for a in range(MAX_GOALS + 1)]
             for h in range(MAX_GOALS + 1)]


def _win_probs(matrix: list[list[float]]) -> tuple[float, float]:
    p_home = p_away = p_tie = 0.0
    n = len(matrix)
    for h in range(n):
        for a in range(n):
            if h > a:
                p_home += matrix[h][a]
            elif a > h:
                p_away += matrix[h][a]
            else:
                p_tie += matrix[h][a]
    # AHL games can't tie — split the tie mass proportionally to
    # regulation win probability (good enough proxy for OT/SO outcomes).
    if p_tie > 0:
        total = p_home + p_away
        if total > 0:
            p_home += p_tie * (p_home / total)
            p_away += p_tie * (p_away / total)
        else:
            p_home += p_tie / 2; p_away += p_tie / 2
    return p_home, p_away


def predict_matchup(home_name: str, away_name: str,
                     season: int | None = None) -> dict | None:
    """Generate a single-matchup prediction. Returns the same shape as
    engine.sports.nhl.predict.predict_matchup so downstream consumers
    don't branch by league."""
    season = season or _current_season()
    home = get_team_by_name(home_name)
    away = get_team_by_name(away_name)
    if not (home and away):
        return None
    h_off, h_def, h_n = _team_rates(home["id"], season)
    a_off, a_def, a_n = _team_rates(away["id"], season)
    # Regress thin samples toward league mean so a 5-game start doesn't
    # produce 80% predictions.
    h_off, h_def = _shrunk(h_off, h_n), _shrunk(h_def, h_n)
    a_off, a_def = _shrunk(a_off, a_n), _shrunk(a_def, a_n)
    # Expected goals: blend each team's offense with opponent defense
    # against league mean (so 1.0 def_rate = league-average defense).
    home_xg = (h_off * (a_def / LEAGUE_AVG_GPG)) + HOME_BOOST
    away_xg = a_off * (h_def / LEAGUE_AVG_GPG)
    matrix = _score_matrix(home_xg, away_xg)
    p_home, p_away = _win_probs(matrix)
    total = home_xg + away_xg
    return {
        "home": {"id": home["id"], "name": home["full_name"]},
        "away": {"id": away["id"], "name": away["full_name"]},
        "expected_score": {"home": round(home_xg, 2),
                             "away": round(away_xg, 2)},
        "total": round(total, 2),
        "spread": round(away_xg - home_xg, 2),
        "win_prob": {"home": round(p_home, 4), "away": round(p_away, 4)},
        "season": season,
        "samples": {"home_games": h_n, "away_games": a_n},
        "reasoning": [
            f"{home['short_name'] or home['full_name']} {h_off:.2f} GF/g, "
            f"{h_def:.2f} GA/g over {h_n} games",
            f"{away['short_name'] or away['full_name']} {a_off:.2f} GF/g, "
            f"{a_def:.2f} GA/g over {a_n} games",
            f"home boost {HOME_BOOST:+.2f} (calibrated)",
        ],
    }


def _current_season() -> int:
    n = datetime.now()
    return n.year if n.month >= 9 else n.year - 1


# ── Predictor protocol adapter ───────────────────────────────

class AHLPredictor:
    """Predictor protocol implementation for AHL. Mirrors NHLFactorPredictor's
    shape so the harness + event log treat AHL like any other sport."""
    name = "ahl_factor"

    def predict(self, game: GameContext) -> Prediction:
        try:
            raw = predict_matchup(game.home, game.away, season=game.season)
        except Exception as e:
            logger.warning("AHLPredictor failed for %s @ %s: %s",
                            game.away, game.home, e)
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, error=str(e),
                                source="factor")
        if not raw:
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away,
                                error=f"team lookup failed ({game.home}/{game.away})",
                                source="factor")
        es = raw["expected_score"]
        wp = raw["win_prob"]

        # Ensemble blend with the trained hockey GBM when available.
        # Factor stays the cold-start signal; once the GBM is on disk
        # (data/hockey/ahl_gbm/), we blend its home_win + margin +
        # total_goals at equal weights with the factor model. Skips
        # silently when the GBM hasn't been trained for this league.
        signals = {"samples": raw["samples"], "factor": {
            "ml_home": wp["home"], "margin": raw["spread"],
            "total": raw["total"]}}
        ml_home = wp["home"]
        margin = raw["spread"]
        total = raw["total"]
        try:
            from ...hockey._gbm import is_available as _gbm_avail, predict as _gbm_predict
            from ...hockey._features import extract_features
            from datetime import datetime
            league = game.sport
            if _gbm_avail(league):
                home_id, away_id = self._team_ids(game)
                if home_id and away_id:
                    feats = extract_features(league, {
                        "home_team_id": home_id, "away_team_id": away_id,
                        "date": et_today_str(),
                        "season": game.season,
                    })
                    if feats:
                        gbm_ml = _gbm_predict(league, "home_win", feats)
                        gbm_margin = _gbm_predict(league, "margin", feats)
                        gbm_total = _gbm_predict(league, "total_goals", feats)
                        if gbm_ml is not None:
                            ml_home = 0.5 * wp["home"] + 0.5 * gbm_ml
                        if gbm_margin is not None:
                            margin = 0.5 * raw["spread"] + 0.5 * gbm_margin
                        if gbm_total is not None:
                            total = 0.5 * raw["total"] + 0.5 * gbm_total
                        signals["gbm"] = {"ml_home": gbm_ml,
                                            "margin": gbm_margin,
                                            "total": gbm_total}
                        signals["ensemble_weights"] = {"factor": 0.5, "gbm": 0.5}
        except Exception as e:
            logger.debug("AHL GBM blend skipped: %s", e)

        return Prediction(
            sport=game.sport, home=game.home, away=game.away,
            home_expected=es["home"], away_expected=es["away"],
            margin=margin, total=total,
            ml_home=ml_home, ml_away=1.0 - ml_home,
            signals=signals,
            reasoning=raw["reasoning"], source="ensemble" if "gbm" in signals else "factor",
        )

    def _team_ids(self, game: GameContext) -> tuple[int | None, int | None]:
        """Resolve home/away team_id from the per-league teams table."""
        try:
            from .db import get_team_by_name
            h = get_team_by_name(game.home)
            a = get_team_by_name(game.away)
            return (h["id"] if h else None, a["id"] if a else None)
        except Exception:
            return (None, None)


register("ahl", AHLPredictor())
