"""Shared helpers + constants for the NHL backtest harness."""

from __future__ import annotations
import math
from datetime import datetime

SEASON = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1

# Synthetic average NHL odds (used as FALLBACK when real historical odds are unavailable)
AVG_FAV_ODDS = -150
AVG_DOG_ODDS = 130
PL_ODDS = -110
OU_ODDS = -110

MAX_GOALS = 10


def _poisson(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def _score_matrix(home_xg: float, away_xg: float) -> list[list[float]]:
    matrix = []
    for h in range(MAX_GOALS + 1):
        row = []
        for a in range(MAX_GOALS + 1):
            row.append(_poisson(home_xg, h) * _poisson(away_xg, a))
        matrix.append(row)
    return matrix


def _implied(ml: int) -> float:
    if ml < 0:
        return abs(ml) / (abs(ml) + 100)
    return 100 / (ml + 100)


def _payout(odds: int) -> float:
    """Return profit on a $100 bet at the given odds."""
    if odds > 0:
        return odds
    return (100 / abs(odds)) * 100


def _prob_to_american(prob: float) -> int:
    """Convert a raw probability to American odds with standard vig."""
    if prob <= 0 or prob >= 1:
        return -110
    vigged = min(0.95, prob + 0.025)
    if vigged >= 0.5:
        return int(-vigged / (1 - vigged) * 100)
    else:
        return int((1 - vigged) / vigged * 100)


def _empty_cat():
    return {"wins": 0, "losses": 0, "pushes": 0, "profit": 0.0}


def _record_bet(cat, won, odds):
    if won:
        cat["wins"] += 1
        cat["profit"] += _payout(odds)
    else:
        cat["losses"] += 1
        cat["profit"] -= 100


def _summarize(cat):
    total = cat["wins"] + cat["losses"]
    cat["total_bets"] = total
    cat["win_pct"] = round(cat["wins"] / total * 100, 1) if total > 0 else 0
    cat["roi"] = round(cat["profit"] / (total * 100) * 100, 1) if total > 0 else 0
    cat["profit"] = round(cat["profit"], 2)


def _abbr_to_team_key(abbr: str) -> str | None:
    """Map a team abbreviation to the JSON file key used by load_team."""
    if not hasattr(_abbr_to_team_key, "_cache"):
        _abbr_to_team_key._cache = {}

    if abbr in _abbr_to_team_key._cache:
        return _abbr_to_team_key._cache[abbr]

    try:
        from ..data import list_teams, load_team
        for t in list_teams("NHL"):
            team = load_team("NHL", t["key"])
            if team and team.get("abbreviation", "").upper() == abbr.upper():
                _abbr_to_team_key._cache[abbr] = t["key"]
                return t["key"]
    except Exception:
        pass

    _abbr_to_team_key._cache[abbr] = None
    return None
