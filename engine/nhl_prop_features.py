"""
NHL per-stat feature extraction for prop GBM training.

Skater stats lean on opp_team_gaa / opp_goalie_save_pct (more
generous goalie = more goals + assists allowed). Goalie stats lean
on opp_team_shots_per_game (the offense rate they'll face).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta


_UNIVERSAL = ["rolling_30d", "rolling_l5", "rest_days", "is_home"]

_PER_STAT = {
    # Skaters
    "g":      ["opp_gaa", "opp_goalie_save_pct", "opp_pp_pct"],
    "a":      ["opp_gaa", "opp_pp_pct"],
    "sog":    ["opp_shots_against_per_game"],
    "hits":   ["opp_team_hits"],
    "blocks": ["opp_shots_against_per_game"],
    # Goalies
    "saves":         ["opp_shots_per_game"],
    "shots_against": ["opp_shots_per_game"],
    "ga":            ["opp_goals_per_game"],
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
    from .nhl_db import get_conn
    return get_conn()


def _player_props_conn():
    from .player_props_db import _conn_for
    return _conn_for("nhl")


def _rolling(conn, player_id: int, stat_key: str, before_date: str,
              days: int = 30, max_n: int | None = None) -> float | None:
    cutoff = (datetime.strptime(before_date, "%Y-%m-%d") -
              timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT stats_json FROM player_game_logs "
        "WHERE player_id = ? AND date >= ? AND date < ? "
        "  AND json_extract(stats_json, '$.toi_min') > 0 "
        "ORDER BY date DESC",
        (player_id, cutoff, before_date),
    ).fetchall()
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
        "  AND json_extract(stats_json, '$.toi_min') > 0",
        (player_id, before_date),
    ).fetchone()
    if not row or not row["last"]:
        return None
    try:
        return (datetime.strptime(before_date, "%Y-%m-%d") -
                datetime.strptime(row["last"], "%Y-%m-%d")).days
    except (ValueError, TypeError):
        return None


def _opp_team_stat(games_conn, opp_team_id: int, season: int, col: str) -> float | None:
    if not opp_team_id:
        return None
    row = games_conn.execute(
        f"SELECT {col} FROM nhl_team_stats WHERE team_id = ? AND season = ? LIMIT 1",
        (opp_team_id, season),
    ).fetchone()
    if not row or row[col] is None:
        return None
    return float(row[col])


def _opp_goalie_save_pct(games_conn, opp_team_id: int, season: int) -> float | None:
    """Approximate by averaging save_pct across goalies on opp team
    weighted by games. Real-time starting-goalie pull would be
    better but we don't have that snapshot at training time."""
    if not opp_team_id:
        return None
    rows = games_conn.execute(
        "SELECT g.save_pct, g.games FROM goalie_stats g "
        "JOIN nhl_players p ON p.id = g.player_id "
        "WHERE p.team_id = ? AND g.season = ? AND g.games > 0 "
        "  AND g.save_pct IS NOT NULL",
        (opp_team_id, season),
    ).fetchall()
    if not rows:
        return None
    total_g = sum(r["games"] for r in rows if r["save_pct"] is not None)
    if total_g == 0:
        return None
    return sum(float(r["save_pct"]) * r["games"]
               for r in rows if r["save_pct"] is not None) / total_g


def extract_features(player_id: int, game_id: str, stat_key: str) -> dict | None:
    pgl_conn = _player_props_conn()
    games_conn = _conn()
    g = games_conn.execute("SELECT * FROM nhl_games WHERE game_id = ?", (str(game_id),)).fetchone()
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

    feats = {
        "rolling_30d": rolling_30d,
        "rolling_l5":  rolling_l5,
        "rest_days":   rest,
        "is_home":     1 if is_home else 0,
    }
    extras = _PER_STAT.get(stat_key, [])
    for col in extras:
        if col == "opp_gaa":
            ga = _opp_team_stat(games_conn, opp_team_id, season, "goals_against")
            wins = _opp_team_stat(games_conn, opp_team_id, season, "wins")
            losses = _opp_team_stat(games_conn, opp_team_id, season, "losses")
            ot = _opp_team_stat(games_conn, opp_team_id, season, "ot_losses")
            games = (wins or 0) + (losses or 0) + (ot or 0)
            feats[col] = (ga / games) if (ga is not None and games > 0) else None
        elif col == "opp_goals_per_game":
            gf = _opp_team_stat(games_conn, opp_team_id, season, "goals_for")
            wins = _opp_team_stat(games_conn, opp_team_id, season, "wins")
            losses = _opp_team_stat(games_conn, opp_team_id, season, "losses")
            ot = _opp_team_stat(games_conn, opp_team_id, season, "ot_losses")
            games = (wins or 0) + (losses or 0) + (ot or 0)
            feats[col] = (gf / games) if (gf is not None and games > 0) else None
        elif col == "opp_pp_pct":
            feats[col] = _opp_team_stat(games_conn, opp_team_id, season, "pp_pct")
        elif col == "opp_shots_per_game":
            feats[col] = _opp_team_stat(games_conn, opp_team_id, season, "shots_per_game")
        elif col == "opp_shots_against_per_game":
            feats[col] = _opp_team_stat(games_conn, opp_team_id, season, "shots_against_per_game")
        elif col == "opp_goalie_save_pct":
            feats[col] = _opp_goalie_save_pct(games_conn, opp_team_id, season)
        elif col == "opp_team_hits":
            # Team hits not stored on team_stats; use season avg from
            # games table instead — approximate by scanning games.
            feats[col] = None  # not available; XGBoost handles NaN
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
        "WHERE date >= ? AND date <= ? AND json_extract(stats_json, '$.toi_min') > 0",
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
