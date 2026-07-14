"""Market-derived features for the NHL GBM. Parallel of
``market_features_nba`` — joins to scoresandodds NHL closing-odds
backfill (data/nhl/historical_odds.db) and returns the V3.1 feature
set the trainer consumes.

NHL nickname → team_id mapping is a hand map rather than last-word
because the league has compound nicknames (Blue Jackets, Maple Leafs,
Golden Knights, Utah's "Hockey Club", etc.) that a naive last-token
split would mangle.
"""
from __future__ import annotations

import logging
import sqlite3
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

_HIST_DB = (Path(__file__).resolve().parent.parent.parent
            / "data" / "nhl" / "historical_odds.db")


# NHL teams have multi-word nicknames; can't just take the last token.
# Map each scoresandodds nickname to the canonical name segment we'll
# match against nhl_teams.name. ESPN team_id is the join key.
_NICK_TO_NAME = {
    "Bruins":         "Bruins",
    "Sabres":         "Sabres",
    "Red Wings":      "Red Wings",
    "Panthers":       "Panthers",
    "Canadiens":      "Canadiens",
    "Senators":       "Senators",
    "Lightning":      "Lightning",
    "Maple Leafs":    "Maple Leafs",
    "Hurricanes":     "Hurricanes",
    "Blue Jackets":   "Blue Jackets",
    "Devils":         "Devils",
    "Islanders":      "Islanders",
    "Rangers":        "Rangers",
    "Flyers":         "Flyers",
    "Penguins":       "Penguins",
    "Capitals":       "Capitals",
    "Blackhawks":     "Blackhawks",
    "Avalanche":      "Avalanche",
    "Stars":          "Stars",
    "Wild":           "Wild",
    "Predators":      "Predators",
    "Blues":          "Blues",
    "Jets":           "Jets",
    "Ducks":          "Ducks",
    "Flames":         "Flames",
    "Oilers":         "Oilers",
    "Kings":          "Kings",
    "Sharks":         "Sharks",
    "Kraken":         "Kraken",
    "Canucks":        "Canucks",
    "Golden Knights": "Golden Knights",
    # Utah franchise rebranded over the last two seasons:
    "Hockey Club":    "Utah",   # 2024-25 transition name (Utah Hockey Club)
    "Mammoth":        "Mammoth",  # 2025-26 final name
    "Coyotes":        "Coyotes",
}


@lru_cache(maxsize=1)
def _nick_to_team_id() -> dict[str, int]:
    """Build nickname → team_id by matching the curated name segment
    above against nhl_teams.name (substring match). Cached for the
    process lifetime."""
    from ..nhl_db import get_conn
    conn = get_conn()
    rows = conn.execute("SELECT id, name FROM nhl_teams").fetchall()
    out: dict[str, int] = {}
    for nick, segment in _NICK_TO_NAME.items():
        seg_lower = segment.lower()
        for r in rows:
            if seg_lower in str(r["name"]).lower():
                out[nick] = int(r["id"])
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
        logger.debug("nhl nickname map unavailable: %s", e)
        return _empty()
    # Reverse map: id → [all nicknames]. Utah's franchise has two
    # nicknames in the historical DB (2024-25 "Hockey Club", 2025-26
    # "Mammoth") that both resolve to the same team_id. Lookup tries
    # every variant so games during the rebrand transition match.
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
        FROM nhl_historical_odds
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
