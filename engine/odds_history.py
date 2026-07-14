"""
Historical odds storage -- saves real sportsbook odds for each game
to enable honest backtesting against real market prices.

NHL odds → nhl.db / nhl_odds table.
MLB odds → mlb.db / odds table (including NRFI + F5 markets).

When a game isn't yet in the games table (early season, fresh DB,
ingestion lag), MLB odds are queued in pending_odds and drained on
the next ingestion run by drain_pending_mlb_odds().
"""

import json
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

    queued = 0

    for g in games_with_odds:
        odds = g.get("odds") or {}
        if not odds.get("home_ml"):
            continue

        # Prefer an explicit mlb_game_id; otherwise resolve via the games table
        game_id = g.get("mlb_game_id")
        if not game_id:
            game_id = _resolve_mlb_game_id(conn, g)
        if not game_id:
            # Queue for the next sync to reconcile. This covers early-season
            # / fresh-DB cases where ESPN has the game but the MLB Stats API
            # ingestion hasn't created the games row yet.
            if _enqueue_pending(conn, g, odds):
                queued += 1
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
                odds.get("provider", "HardRock"),
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

    if stored or queued:
        conn.commit()
        logger.info(
            "MLB odds: stored %d, queued %d (drain via drain_pending_mlb_odds)",
            stored, queued,
        )

    return stored


def _enqueue_pending(conn, game: dict, odds: dict) -> bool:
    """Park an unresolvable odds payload in pending_odds for later reconciliation."""
    home = game.get("home") or {}
    away = game.get("away") or {}
    date = (game.get("date", "") or "")[:10]
    if not date:
        return False
    try:
        conn.execute("""
            INSERT INTO pending_odds (
                game_date, home_team_id, away_team_id,
                home_abbr, away_abbr, payload, captured_at
            )
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(game_date, home_team_id, away_team_id) DO UPDATE SET
                payload = excluded.payload,
                captured_at = excluded.captured_at
        """, (
            date,
            home.get("team_id"),
            away.get("team_id"),
            home.get("abbreviation"),
            away.get("abbreviation"),
            json.dumps(odds, default=str),
        ))
        return True
    except Exception as e:
        logger.warning("Failed to enqueue pending odds for %s: %s", date, e)
        return False


def drain_pending_mlb_odds() -> dict:
    """Re-attempt storage of any pending odds whose games table row now exists.

    Called after MLB Stats API ingestion (sync_mlb.bat). Walks pending_odds,
    re-resolves mlb_game_id by date + team IDs, and on success forwards the
    payload through store_mlb_odds(). Anything still unresolvable stays
    queued; rows older than the cutoff are dropped so a permanently-broken
    matchup doesn't accumulate forever.

    Returns a summary dict {drained, dropped, still_pending}.
    """
    try:
        from .db import get_conn
    except Exception:
        return {"drained": 0, "dropped": 0, "still_pending": 0}

    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, game_date, home_team_id, away_team_id, "
            "       home_abbr, away_abbr, payload, captured_at "
            "FROM pending_odds"
        ).fetchall()
    except Exception as e:
        logger.warning("drain_pending_mlb_odds: cannot read queue: %s", e)
        return {"drained": 0, "dropped": 0, "still_pending": 0}

    drained = 0
    dropped = 0
    still_pending = 0
    cutoff_age_days = 14   # drop entries this old; the game is unlikely to materialize

    for r in rows:
        try:
            captured = datetime.fromisoformat(
                r["captured_at"].replace(" ", "T")
            )
            age_days = (datetime.now() - captured).total_seconds() / 86400
        except Exception:
            age_days = 0

        try:
            payload = json.loads(r["payload"])
        except Exception:
            payload = None

        # Try to resolve the game row now.
        game_id = None
        if r["home_team_id"] and r["away_team_id"]:
            try:
                grow = conn.execute(
                    "SELECT mlb_game_id FROM games "
                    "WHERE date = ? AND home_team_id = ? AND away_team_id = ? LIMIT 1",
                    (r["game_date"], r["home_team_id"], r["away_team_id"]),
                ).fetchone()
                game_id = grow["mlb_game_id"] if grow else None
            except Exception:
                game_id = None

        if game_id and payload:
            stub_game = {
                "mlb_game_id": game_id,
                "date": r["game_date"],
                "home": {"team_id": r["home_team_id"], "abbreviation": r["home_abbr"]},
                "away": {"team_id": r["away_team_id"], "abbreviation": r["away_abbr"]},
                "odds": payload,
            }
            store_mlb_odds([stub_game])
            conn.execute("DELETE FROM pending_odds WHERE id = ?", (r["id"],))
            drained += 1
        elif age_days > cutoff_age_days:
            conn.execute("DELETE FROM pending_odds WHERE id = ?", (r["id"],))
            dropped += 1
        else:
            still_pending += 1

    if drained or dropped:
        conn.commit()
        logger.info(
            "drain_pending_mlb_odds: drained=%d dropped=%d still_pending=%d",
            drained, dropped, still_pending,
        )

    return {"drained": drained, "dropped": dropped, "still_pending": still_pending}


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
                odds.get("provider", "HardRock"),
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
