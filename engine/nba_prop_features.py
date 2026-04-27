"""
NBA per-stat feature extraction for prop GBM training.

Same shape as engine.mlb_prop_features_v2 but pulls NBA-specific
context: opponent defensive rating, pace, opponent's own production
(more passes = more steals/turnovers given up), and the player's
season minutes-per-game from nba_players.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta


_UNIVERSAL = ["rolling_30d", "rolling_l5", "rest_days", "is_home", "season_mpg"]

_PER_STAT = {
    "pts": ["opp_def_rating", "opp_pace", "opp_q1_opp_ppg"],
    "reb": ["opp_pace", "opp_reb_rate"],
    "ast": ["opp_def_rating", "opp_pace"],
    "tpm": ["opp_def_rating", "opp_three_pct"],
    "ftm": ["opp_pace"],
    "to":  ["opp_pace"],
    "stl": ["opp_pace"],
    "blk": ["opp_pace"],
}


def feature_cols(stat_key: str) -> list[str]:
    return _UNIVERSAL + _PER_STAT.get(stat_key, [])


@dataclass
class TrainingRow:
    player_id: int
    game_id: str
    date: str
    target: float
    features: dict


def _conn():
    from .nba_db import get_conn
    return get_conn()


def _player_props_conn():
    from .player_props_db import _conn_for
    return _conn_for("nba")


def _rolling(conn, player_id: int, stat_key: str, before_date: str,
              days: int = 30, max_n: int | None = None) -> float | None:
    cutoff = (datetime.strptime(before_date, "%Y-%m-%d") -
              timedelta(days=days)).strftime("%Y-%m-%d")
    sql = (
        "SELECT stats_json FROM player_game_logs "
        "WHERE player_id = ? AND date >= ? AND date < ? "
        "  AND json_extract(stats_json, '$.min') > 0 "
        "ORDER BY date DESC"
    )
    rows = conn.execute(sql, (player_id, cutoff, before_date)).fetchall()
    if max_n is not None:
        rows = rows[:max_n]
    vals = []
    for r in rows:
        try:
            stats = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        v = stats.get(stat_key)
        if v is not None:
            try:
                vals.append(float(v))
            except (TypeError, ValueError):
                continue
    return sum(vals) / len(vals) if vals else None


def _rest_days(conn, player_id: int, before_date: str) -> int | None:
    row = conn.execute(
        "SELECT MAX(date) AS last FROM player_game_logs "
        "WHERE player_id = ? AND date < ? "
        "  AND json_extract(stats_json, '$.min') > 0",
        (player_id, before_date),
    ).fetchone()
    if not row or not row["last"]:
        return None
    try:
        return (datetime.strptime(before_date, "%Y-%m-%d") -
                datetime.strptime(row["last"], "%Y-%m-%d")).days
    except (ValueError, TypeError):
        return None


def _opp_q1_stat(games_conn, opp_team_id: int, season: int, col: str) -> float | None:
    if not opp_team_id:
        return None
    row = games_conn.execute(
        f"SELECT {col} FROM nba_q1_stats WHERE team_id = ? AND season = ? LIMIT 1",
        (opp_team_id, season),
    ).fetchone()
    if not row or row[col] is None:
        return None
    return float(row[col])


def _player_season_mpg(games_conn, player_id: int, season: int) -> float | None:
    row = games_conn.execute(
        "SELECT minutes_per_game FROM nba_players WHERE player_id = ? AND season = ? LIMIT 1",
        (player_id, season),
    ).fetchone()
    if not row or row["minutes_per_game"] is None:
        return None
    return float(row["minutes_per_game"])


def extract_features(player_id: int, game_id: str, stat_key: str) -> dict | None:
    pgl_conn = _player_props_conn()
    games_conn = _conn()
    g = games_conn.execute("SELECT * FROM nba_games WHERE game_id = ?", (str(game_id),)).fetchone()
    if not g:
        return None
    g = dict(g)
    season = int(g.get("season") or datetime.strptime(g["date"], "%Y-%m-%d").year)
    pgl = pgl_conn.execute(
        "SELECT team_id, opp_team_id, is_home FROM player_game_logs "
        "WHERE player_id = ? AND game_id = ?",
        (player_id, str(game_id)),
    ).fetchone()
    if not pgl:
        return None
    opp_team_id = pgl["opp_team_id"]
    is_home = bool(pgl["is_home"])

    rolling_30d = _rolling(pgl_conn, player_id, stat_key, g["date"], days=30)
    if rolling_30d is None:
        return None
    rolling_l5 = _rolling(pgl_conn, player_id, stat_key, g["date"], days=90, max_n=5)
    rest = _rest_days(pgl_conn, player_id, g["date"])
    season_mpg = _player_season_mpg(games_conn, player_id, season)

    feats = {
        "rolling_30d": rolling_30d,
        "rolling_l5":  rolling_l5,
        "rest_days":   rest,
        "is_home":     1 if is_home else 0,
        "season_mpg":  season_mpg,
    }
    extras = _PER_STAT.get(stat_key, [])
    for col in extras:
        if col == "opp_def_rating":
            feats[col] = _opp_q1_stat(games_conn, opp_team_id, season, "def_rating")
        elif col == "opp_pace":
            feats[col] = _opp_q1_stat(games_conn, opp_team_id, season, "pace")
        elif col == "opp_q1_opp_ppg":
            feats[col] = _opp_q1_stat(games_conn, opp_team_id, season, "q1_opp_ppg")
        elif col == "opp_reb_rate":
            feats[col] = _opp_q1_stat(games_conn, opp_team_id, season, "reb_rate")
        elif col == "opp_three_pct":
            feats[col] = _opp_q1_stat(games_conn, opp_team_id, season, "three_pct")
        else:
            feats[col] = None
    return feats


def build_training_set(stat_key: str, *, lookback_days: int = 180) -> list[TrainingRow]:
    conn = _player_props_conn()
    today = datetime.now().strftime("%Y-%m-%d")
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT player_id, game_id, date, stats_json "
        "FROM player_game_logs "
        "WHERE date >= ? AND date <= ? AND json_extract(stats_json, '$.min') > 0",
        (cutoff, today),
    ).fetchall()
    out: list[TrainingRow] = []
    for r in rows:
        try:
            stats = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        target = stats.get(stat_key)
        if target is None:
            continue
        feats = extract_features(r["player_id"], str(r["game_id"]), stat_key)
        if not feats:
            continue
        out.append(TrainingRow(
            player_id=int(r["player_id"]),
            game_id=str(r["game_id"]),
            date=r["date"],
            target=float(target),
            features=feats,
        ))
    return out


__all__ = ["feature_cols", "build_training_set", "extract_features", "TrainingRow"]
