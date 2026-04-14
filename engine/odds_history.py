"""
Historical odds storage -- saves DK odds for each game to enable
honest backtesting against real market prices.

NHL odds → nhl.db / nhl_odds table.
MLB odds → mlb.db / odds table (including NRFI + F5 markets).
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def store_mlb_odds(games_with_odds: list[dict]) -> int:
    """Store MLB odds (including NRFI + F5 markets) for today's games.

    Called during scoreboard enrichment after fetching from Odds API.
    Each game dict should include an ``odds`` sub-dict with the standard
    market fields (home_ml, over_under, etc.) plus nrfi_* and f5_*.

    The MLB game_id is resolved by looking up the ``games`` table by
    date + home/away team IDs. Games not yet in the ``games`` table
    (e.g., if daily sync hasn't run) are skipped.

    Returns number of rows upserted.
    """
    try:
        from .db import get_conn
    except Exception:
        logger.warning("Cannot store MLB odds: engine.db unavailable")
        return 0

    conn = get_conn()
    stored = 0

    for g in games_with_odds:
        odds = g.get("odds") or {}
        if not odds.get("home_ml"):
            continue

        # Prefer an explicit mlb_game_id; otherwise resolve via the games table
        game_id = g.get("mlb_game_id")
        if not game_id:
            game_id = _resolve_mlb_game_id(conn, g)
        if not game_id:
            continue

        try:
            conn.execute("""
                INSERT INTO odds (
                    game_id, source,
                    home_ml, away_ml,
                    spread, home_spread_odds, away_spread_odds,
                    total, over_odds, under_odds,
                    nrfi_line, nrfi_over_odds, nrfi_under_odds,
                    f5_home_ml, f5_away_ml,
                    f5_total, f5_over_odds, f5_under_odds,
                    f5_spread,
                    f5_home_spread_point, f5_home_spread_odds,
                    f5_away_spread_point, f5_away_spread_odds,
                    updated_at
                )
                VALUES (?, ?,  ?, ?,  ?, ?, ?,  ?, ?, ?,
                        ?, ?, ?,  ?, ?,  ?, ?, ?,
                        ?,  ?, ?, ?, ?,  datetime('now'))
                ON CONFLICT(game_id, source) DO UPDATE SET
                    home_ml = excluded.home_ml,
                    away_ml = excluded.away_ml,
                    spread = excluded.spread,
                    home_spread_odds = excluded.home_spread_odds,
                    away_spread_odds = excluded.away_spread_odds,
                    total = excluded.total,
                    over_odds = excluded.over_odds,
                    under_odds = excluded.under_odds,
                    nrfi_line = excluded.nrfi_line,
                    nrfi_over_odds = excluded.nrfi_over_odds,
                    nrfi_under_odds = excluded.nrfi_under_odds,
                    f5_home_ml = excluded.f5_home_ml,
                    f5_away_ml = excluded.f5_away_ml,
                    f5_total = excluded.f5_total,
                    f5_over_odds = excluded.f5_over_odds,
                    f5_under_odds = excluded.f5_under_odds,
                    f5_spread = excluded.f5_spread,
                    f5_home_spread_point = excluded.f5_home_spread_point,
                    f5_home_spread_odds = excluded.f5_home_spread_odds,
                    f5_away_spread_point = excluded.f5_away_spread_point,
                    f5_away_spread_odds = excluded.f5_away_spread_odds,
                    updated_at = datetime('now')
            """, (
                game_id,
                odds.get("provider", "DraftKings"),
                odds.get("home_ml"),
                odds.get("away_ml"),
                odds.get("spread"),
                odds.get("home_spread_odds"),
                odds.get("away_spread_odds"),
                odds.get("over_under"),
                odds.get("over_odds"),
                odds.get("under_odds"),
                odds.get("nrfi_line"),
                odds.get("nrfi_over_odds"),
                odds.get("nrfi_under_odds"),
                odds.get("f5_home_ml"),
                odds.get("f5_away_ml"),
                odds.get("f5_total"),
                odds.get("f5_over_odds"),
                odds.get("f5_under_odds"),
                odds.get("f5_spread"),
                odds.get("f5_home_spread_point"),
                odds.get("f5_home_spread_odds"),
                odds.get("f5_away_spread_point"),
                odds.get("f5_away_spread_odds"),
            ))
            stored += 1
        except Exception as e:
            logger.warning("Failed to store MLB odds for game_id=%s: %s", game_id, e)

    if stored:
        conn.commit()
        logger.info("Stored %d MLB odds snapshots", stored)

    return stored


def _resolve_mlb_game_id(conn, game: dict) -> int | None:
    """Look up mlb_game_id from the games table by date + team IDs."""
    home_id = (game.get("home") or {}).get("team_id")
    away_id = (game.get("away") or {}).get("team_id")
    date = (game.get("date", "") or "")[:10]
    if not (home_id and away_id and date):
        return None
    try:
        row = conn.execute("""
            SELECT mlb_game_id FROM games
            WHERE date = ? AND home_team_id = ? AND away_team_id = ?
            LIMIT 1
        """, (date, home_id, away_id)).fetchone()
        return row["mlb_game_id"] if row else None
    except Exception as e:
        logger.debug("mlb_game_id resolution failed: %s", e)
        return None


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
