"""Market-derived features for the MLB GBM. Parallel of the NBA + NHL
versions — joins to scoresandodds historical closing odds
(data/mlb/historical_odds.db) and returns the V3.1 feature set.

MLB ``games.home_team_id`` uses MLB Stats API IDs (e.g. 139 = NY Yankees),
NOT the internal ``teams.id`` row index. The join key is therefore
``teams.mlb_id``, not ``teams.id`` — caller's team_id is mlb_id.
"""
from __future__ import annotations

import logging
import sqlite3
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

_HIST_DB = (Path(__file__).resolve().parent.parent.parent
            / "data" / "mlb" / "historical_odds.db")


# Hand-curated nickname → mlb_team segment. MLB has cleaner compound
# names than NHL but a couple are worth noting (Blue Jays, Red Sox,
# White Sox, D-backs). Substring match against teams.name keeps the
# map robust to franchise rebrands (Athletics' Sacramento → Las Vegas).
_NICK_TO_NAME = {
    "Angels":       "Angels",
    "Astros":       "Astros",
    "Athletics":    "Athletics",
    "Blue Jays":    "Blue Jays",
    "Braves":       "Braves",
    "Brewers":      "Brewers",
    "Cardinals":    "Cardinals",
    "Cubs":         "Cubs",
    "Diamondbacks": "Diamondbacks",
    "D-backs":      "Diamondbacks",
    "Dodgers":      "Dodgers",
    "Giants":       "Giants",
    "Guardians":    "Guardians",
    "Mariners":     "Mariners",
    "Marlins":      "Marlins",
    "Mets":         "Mets",
    "Nationals":    "Nationals",
    "Orioles":      "Orioles",
    "Padres":       "Padres",
    "Phillies":     "Phillies",
    "Pirates":      "Pirates",
    "Rangers":      "Rangers",
    "Rays":         "Rays",
    "Red Sox":      "Red Sox",
    "Reds":         "Reds",
    "Rockies":      "Rockies",
    "Royals":       "Royals",
    "Tigers":       "Tigers",
    "Twins":        "Twins",
    "White Sox":    "White Sox",
    "Yankees":      "Yankees",
}


@lru_cache(maxsize=1)
def _nick_to_team_id() -> dict[str, int]:
    """Build nickname → MLB Stats API team_id by matching name segment
    against ``teams.name``. Returns mlb_id (the key the games table
    uses on home_team_id / away_team_id), not the internal id."""
    from ..db import get_conn
    conn = get_conn()
    rows = conn.execute("SELECT mlb_id, name FROM teams").fetchall()
    out: dict[str, int] = {}
    for nick, segment in _NICK_TO_NAME.items():
        seg_lower = segment.lower()
        for r in rows:
            if seg_lower in str(r["name"]).lower():
                out[nick] = int(r["mlb_id"])
                break
    return out


def _implied(american: int) -> float:
    if american < 0:
        return abs(american) / (abs(american) + 100)
    return 100.0 / (american + 100)


def _devig_moneyline(home_ml: int | None,
                      away_ml: int | None) -> float | None:
    if home_ml is None or away_ml is None:
        return None
    h_imp = _implied(int(home_ml))
    a_imp = _implied(int(away_ml))
    s = h_imp + a_imp
    if s <= 0:
        return None
    return h_imp / s


@lru_cache(maxsize=1)
def _hist_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_HIST_DB), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


FEATURE_KEYS = (
    "has_market_data",
    "market_home_implied",
    "market_total_line",
    "market_spread_line",
    "market_spread_move",
    "market_total_move",
)


def _empty() -> dict[str, float]:
    return {k: 0.0 for k in FEATURE_KEYS}


def extract_market_features(home_team_id: int, away_team_id: int,
                              game_date: str) -> dict[str, float]:
    if not _HIST_DB.exists():
        return _empty()
    try:
        nick_to_id = _nick_to_team_id()
    except Exception as e:
        logger.debug("mlb nickname map unavailable: %s", e)
        return _empty()
    id_to_nicks: dict[int, list[str]] = {}
    for nick, tid in nick_to_id.items():
        id_to_nicks.setdefault(int(tid), []).append(nick)
    home_nicks = id_to_nicks.get(int(home_team_id), [])
    away_nicks = id_to_nicks.get(int(away_team_id), [])
    if not home_nicks or not away_nicks:
        return _empty()

    placeholders_h = ",".join(["?"] * len(home_nicks))
    placeholders_a = ",".join(["?"] * len(away_nicks))
    row = _hist_conn().execute(f"""
        SELECT
          home_ml, away_ml, total_line, spread_home_line,
          open_spread_line, open_total_line
        FROM mlb_historical_odds
        WHERE date = ?
          AND home_abbr IN ({placeholders_h})
          AND away_abbr IN ({placeholders_a})
        LIMIT 1
    """, [game_date, *home_nicks, *away_nicks]).fetchone()
    if not row:
        return _empty()

    out = _empty()
    out["has_market_data"] = 1.0
    home_imp = _devig_moneyline(row["home_ml"], row["away_ml"])
    if home_imp is not None:
        out["market_home_implied"] = float(home_imp)
    if row["total_line"] is not None:
        out["market_total_line"] = float(row["total_line"])
    if row["spread_home_line"] is not None:
        out["market_spread_line"] = float(row["spread_home_line"])
    if (row["spread_home_line"] is not None
            and row["open_spread_line"] is not None):
        out["market_spread_move"] = (
            float(row["spread_home_line"]) - float(row["open_spread_line"])
        )
    if (row["total_line"] is not None
            and row["open_total_line"] is not None):
        out["market_total_move"] = (
            float(row["total_line"]) - float(row["open_total_line"])
        )
    return out
