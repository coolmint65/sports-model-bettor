"""
NBA per-player minutes projector (Phase 2k-ii prep).

Projects how many minutes a player will play tonight, layering the
cheapest signals first. Goal: replace the implicit "season MPG is
fine" assumption in build_player_mc with a same-game-aware estimate
so prop GBMs (pts, reb, ast) get a real-time minutes feature.

Layers (in order, each one optional):
  1. Base: rolling 14-day MPG (from player_game_logs)
  2. Adjustment for OUT teammates today (each OUT player frees
     up minutes; the lift per OUT teammate is empirical from the
     player's own history when computable, else a flat default)
  3. Back-to-back fatigue (small drop)

Tonight's version uses a flat 1.6 min/lift per OUT teammate as
the constant. Future iteration: replace with per-teammate
historical lift computed from player_game_logs.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from ..._tz import et_today_str

logger = logging.getLogger(__name__)


# Average minutes a non-OUT player gains per OUT teammate. Conservative
# starting point — empirical refinement is the next step once the
# baseline GBM pass tells us if minutes-as-feature helps at all.
DEFAULT_LIFT_PER_OUT_MIN = 1.6

# Back-to-back fatigue factor — historical NBA wisdom is ~7% MPG drop
# on the second night of a B2B for high-minute players.
B2B_FACTOR = 0.93


def _player_props_conn():
    from ...player_props_db import _conn_for
    return _conn_for("nba")


def _games_conn():
    from .db import get_conn
    return get_conn()


def _rolling_mpg(player_id: int, before_date: str, days: int = 14) -> float | None:
    """Mean minutes per game over the trailing window. Excludes DNPs
    (min=0) so a player who's been benched for one game doesn't drag
    the projection."""
    conn = _player_props_conn()
    cutoff = (datetime.strptime(before_date, "%Y-%m-%d") -
              timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT stats_json FROM player_game_logs "
        "WHERE player_id = ? AND date >= ? AND date < ? "
        "  AND json_extract(stats_json, '$.min') > 0 "
        "ORDER BY date",
        (int(player_id), cutoff, before_date),
    ).fetchall()
    mins = []
    for r in rows:
        try:
            stats = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        m = stats.get("min")
        if m is None:
            continue
        try:
            mins.append(float(m))
        except (TypeError, ValueError):
            continue
    if not mins:
        return None
    return sum(mins) / len(mins)


def _was_back_to_back(player_id: int, game_date: str) -> bool:
    """True if the player played within 1 day prior."""
    conn = _player_props_conn()
    yesterday = (datetime.strptime(game_date, "%Y-%m-%d") -
                 timedelta(days=1)).strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT 1 FROM player_game_logs "
        "WHERE player_id = ? AND date = ? "
        "  AND json_extract(stats_json, '$.min') > 0 LIMIT 1",
        (int(player_id), yesterday),
    ).fetchone()
    return row is not None


def _out_teammates_count(team_id: int, game_date: str) -> int:
    """Count of currently-OUT teammates per the live nba_injuries
    table. We only have today's snapshot — historical OUT counts
    for older games are inferable from player_game_logs (any
    rostered player with no row in the team's games that day) but
    that's a heavier lookup; flat-count is fine for v1."""
    conn = _games_conn()
    today_str = et_today_str()
    if game_date != today_str:
        # Live injuries snapshot only meaningful for today; for past
        # games we'd need historical injury reports we don't store.
        return 0
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM nba_injuries "
        "WHERE team_id = ? AND status IN ('Out', 'OUT', 'out')",
        (int(team_id),),
    ).fetchone()
    return int(row["n"]) if row else 0


def project_minutes(player_id: int, game_id: str | None = None,
                    *, team_id: int | None = None,
                    game_date: str | None = None) -> float | None:
    """Returns projected minutes for the player in the given game.

    ``team_id`` and ``game_date`` are optional — derived from the
    games table when not provided. Returns None if the player has
    no recent minutes history (rookie, season-long injury, etc.)."""
    if game_date is None and game_id is not None:
        conn = _games_conn()
        row = conn.execute(
            "SELECT date FROM nba_games WHERE game_id = ? LIMIT 1",
            (str(game_id),),
        ).fetchone()
        if row:
            game_date = row["date"]
    if game_date is None:
        game_date = et_today_str()

    base = _rolling_mpg(player_id, game_date, days=14)
    if base is None:
        return None

    # OUT-teammate lift
    if team_id is None and game_id is not None:
        pgl = _player_props_conn().execute(
            "SELECT team_id FROM player_game_logs "
            "WHERE player_id = ? AND game_id = ? LIMIT 1",
            (int(player_id), str(game_id)),
        ).fetchone()
        if pgl:
            team_id = pgl["team_id"]
    out_n = _out_teammates_count(team_id, game_date) if team_id else 0
    base += DEFAULT_LIFT_PER_OUT_MIN * out_n

    # B2B fatigue
    if _was_back_to_back(player_id, game_date):
        base *= B2B_FACTOR

    return max(0.0, min(48.0, base))


__all__ = ["project_minutes", "DEFAULT_LIFT_PER_OUT_MIN", "B2B_FACTOR"]
