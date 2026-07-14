"""Market-derived features for the NBA GBM.

Joins the V3.1 historical-odds backfill (data/nba/historical_odds.db,
scraped from scoresandodds.com) to (date, home_team, away_team) and
returns the closing-line implied probabilities + opening→closing line
movement as features the model can learn from.

The intuition (V3.1 thesis): the market — even one book's closing line
— is the single strongest predictor of true outcome in sports. By
adding it as a feature, the model learns to be a *delta on top of the
market* rather than a parallel signal. Brier improvement of 20-40% is
typical in the academic literature when this feature lands cleanly.

Returned feature names:
    has_market_data        1.0 when we have odds for this game, 0.0 otherwise
    market_home_implied    Devigged home win probability (closing ML)
    market_total_line      Closing total line (points)
    market_spread_line     Closing spread (home perspective, negative = home favored)
    market_spread_move     Closing - opening spread (positive = line drifted to away)
    market_total_move      Closing - opening total

All features default to NaN / 0 when no historical odds exist for the
game; ``has_market_data`` lets the tree learn to fall back to the
non-market features when the flag is 0.
"""
from __future__ import annotations

import logging
import sqlite3
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

_HIST_DB = (Path(__file__).resolve().parent.parent.parent
            / "data" / "nba" / "historical_odds.db")


@lru_cache(maxsize=1)
def _team_nickname_to_id() -> dict[str, int]:
    """Build the nickname → ESPN team_id map from nba_teams.

    scoresandodds uses team nicknames (Knicks, Trail Blazers, 76ers).
    nba_teams stores full names with city + nickname. Last-token match
    covers every team except Trail Blazers (which has a 2-word nick);
    we special-case that one explicitly."""
    from ..nba_db import get_conn
    conn = get_conn()
    rows = conn.execute("SELECT id, name FROM nba_teams").fetchall()
    out: dict[str, int] = {}
    for r in rows:
        tid, name = int(r["id"]), str(r["name"])
        # Last word usually IS the nickname
        toks = name.split()
        if toks:
            out[toks[-1]] = tid
        # Multi-word nickname special cases
        if "Trail Blazers" in name:
            out["Trail Blazers"] = tid
    return out


def _devig_moneyline(home_ml: int | None,
                      away_ml: int | None) -> float | None:
    """Two-way moneyline → devigged home win probability. Removes the
    book's overround by normalizing implied probs to sum to 1."""
    if home_ml is None or away_ml is None:
        return None
    h_imp = _implied(int(home_ml))
    a_imp = _implied(int(away_ml))
    s = h_imp + a_imp
    if s <= 0:
        return None
    return h_imp / s


def _implied(american: int) -> float:
    if american < 0:
        return abs(american) / (abs(american) + 100)
    return 100.0 / (american + 100)


@lru_cache(maxsize=1)
def _hist_conn() -> sqlite3.Connection:
    """Read-only-style connection to the historical odds DB. The
    scraper is the only writer so concurrent access is safe."""
    conn = sqlite3.connect(str(_HIST_DB), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


_FEATURE_KEYS = (
    "has_market_data",
    "market_home_implied",
    "market_total_line",
    "market_spread_line",
    "market_spread_move",
    "market_total_move",
)


def _empty() -> dict[str, float]:
    """Default feature dict — emitted when no historical odds exist
    for this game. ``has_market_data=0`` lets the tree learn to fall
    back to the non-market path; other fields are 0 placeholders so
    the column stays numeric and well-defined."""
    return {k: 0.0 for k in _FEATURE_KEYS}


def extract_market_features(home_team_id: int, away_team_id: int,
                              game_date: str) -> dict[str, float]:
    """Look up the historical closing odds for this game and return
    the V3.1 market features. Empty dict when no match."""
    if not _HIST_DB.exists():
        return _empty()
    try:
        nick_to_id = _team_nickname_to_id()
    except Exception as e:
        logger.debug("nba team nickname map unavailable: %s", e)
        return _empty()
    # Reverse the map for the lookup we need (id → nickname)
    id_to_nick = {tid: nick for nick, tid in nick_to_id.items()}
    home_nick = id_to_nick.get(int(home_team_id))
    away_nick = id_to_nick.get(int(away_team_id))
    if not home_nick or not away_nick:
        return _empty()

    row = _hist_conn().execute("""
        SELECT
          home_ml, away_ml, total_line, spread_home_line,
          open_spread_line, open_total_line
        FROM nba_historical_odds
        WHERE date = ? AND home_abbr = ? AND away_abbr = ?
        LIMIT 1
    """, (game_date, home_nick, away_nick)).fetchone()
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
    # Line movement = closing - opening. Positive spread_move = line
    # moved against the home side (home line drifted higher in the
    # away direction).
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


FEATURE_KEYS = _FEATURE_KEYS  # public re-export for the trainer
