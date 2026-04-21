"""
Historical odds storage -- saves DK odds for each game to enable
honest backtesting against real market prices.

Stores in the nhl.db database in the nhl_odds table.
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def store_nhl_odds(games_with_odds: list[dict]) -> int:
    """Store odds for today's NHL games.

    Called during the scoreboard enrichment after fetching from Odds API.
    Each game dict should have:
    - game_date (or date)
    - home_abbr, away_abbr
    - home_ml, away_ml
    - over_under, over_odds, under_odds
    - home_spread_point, home_spread_odds, away_spread_point, away_spread_odds

    Returns the number of rows upserted.
    """
    try:
        from .nhl_db import get_conn
    except Exception:
        logger.warning("Cannot store odds: nhl_db unavailable")
        return 0

    conn = get_conn()
    stored = 0

    for g in games_with_odds:
        game_date = g.get("game_date") or g.get("date")
        home_abbr = g.get("home_abbr", "")
        away_abbr = g.get("away_abbr", "")

        if not game_date or not home_abbr or not away_abbr:
            continue

        odds = g.get("odds", g)  # Allow flat dict or nested odds key

        try:
            conn.execute("""
                INSERT INTO nhl_odds
                    (game_date, home_abbr, away_abbr,
                     home_ml, away_ml,
                     over_under, over_odds, under_odds,
                     home_spread_point, home_spread_odds,
                     away_spread_point, away_spread_odds,
                     provider)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(game_date, home_abbr, away_abbr) DO UPDATE SET
                    home_ml = excluded.home_ml,
                    away_ml = excluded.away_ml,
                    over_under = excluded.over_under,
                    over_odds = excluded.over_odds,
                    under_odds = excluded.under_odds,
                    home_spread_point = excluded.home_spread_point,
                    home_spread_odds = excluded.home_spread_odds,
                    away_spread_point = excluded.away_spread_point,
                    away_spread_odds = excluded.away_spread_odds,
                    provider = excluded.provider,
                    captured_at = datetime('now')
            """, (
                game_date, home_abbr, away_abbr,
                odds.get("home_ml"),
                odds.get("away_ml"),
                odds.get("over_under"),
                odds.get("over_odds"),
                odds.get("under_odds"),
                odds.get("home_spread_point"),
                odds.get("home_spread_odds"),
                odds.get("away_spread_point"),
                odds.get("away_spread_odds"),
                odds.get("provider", "DraftKings"),
            ))
            stored += 1
        except Exception as e:
            logger.warning("Failed to store odds for %s @ %s on %s: %s",
                           away_abbr, home_abbr, game_date, e)

    if stored:
        conn.commit()
        logger.info("Stored %d NHL odds snapshots", stored)

    return stored


def get_historical_odds(date: str = None, team_abbr: str = None) -> list[dict]:
    """Retrieve stored odds for backtesting.

    Args:
        date: Filter by game_date (YYYY-MM-DD). If None, return all.
        team_abbr: Filter by team abbreviation (home or away). If None, no filter.

    Returns list of dicts with all odds columns.
    """
    try:
        from .nhl_db import get_conn
    except Exception:
        return []

    conn = get_conn()
    query = "SELECT * FROM nhl_odds WHERE 1=1"
    params: list = []

    if date:
        query += " AND game_date = ?"
        params.append(date)

    if team_abbr:
        query += " AND (home_abbr = ? OR away_abbr = ?)"
        params.append(team_abbr.upper())
        params.append(team_abbr.upper())

    query += " ORDER BY game_date DESC, home_abbr"

    try:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning("Failed to query historical odds: %s", e)
        return []
