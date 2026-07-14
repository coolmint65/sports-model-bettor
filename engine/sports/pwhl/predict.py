"""PWHL prediction engine — same Poisson model as AHL.

Constants tuned separately because PWHL games run a bit lower-scoring
and tighter than AHL (~5.5 total vs AHL ~6.4). Calibration writes to
data/hockey/pwhl_constants.json.
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

logger = logging.getLogger(__name__)

# Defaults — calibration overrides via the on-disk constants JSON.
LEAGUE_AVG_GPG = 2.75
HOME_BOOST = 0.10
LEAGUE_AVG_TOTAL = 5.50
MAX_GOALS = 12
MIN_GAMES_FULL_TRUST = 20  # PWHL seasons are short; trust earlier

_CONSTANTS_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "hockey" / "pwhl_constants.json"
)


def _load_constants() -> None:
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
            if h > a: p_home += matrix[h][a]
            elif a > h: p_away += matrix[h][a]
            else: p_tie += matrix[h][a]
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
    season = season or _current_season()
    home = get_team_by_name(home_name)
    away = get_team_by_name(away_name)
    if not (home and away):
        return None
    h_off, h_def, h_n = _team_rates(home["id"], season)
    a_off, a_def, a_n = _team_rates(away["id"], season)
    h_off, h_def = _shrunk(h_off, h_n), _shrunk(h_def, h_n)
    a_off, a_def = _shrunk(a_off, a_n), _shrunk(a_def, a_n)
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
    }


def _current_season() -> int:
    n = datetime.now()
    return n.year if n.month >= 9 else n.year - 1


class PWHLPredictor:
    name = "pwhl_factor"

    def predict(self, game: GameContext) -> Prediction:
        try:
            raw = predict_matchup(game.home, game.away, season=game.season)
        except Exception as e:
            logger.warning("PWHLPredictor failed for %s @ %s: %s",
                            game.away, game.home, e)
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, error=str(e),
                                source="factor")
        if not raw:
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away,
                                error=f"team lookup failed ({game.home}/{game.away})",
                                source="factor")
        es, wp = raw["expected_score"], raw["win_prob"]
        return Prediction(
            sport=game.sport, home=game.home, away=game.away,
            home_expected=es["home"], away_expected=es["away"],
            margin=raw["spread"], total=raw["total"],
            ml_home=wp["home"], ml_away=wp["away"],
            signals={"samples": raw["samples"]},
            source="factor",
        )


register("pwhl", PWHLPredictor())
