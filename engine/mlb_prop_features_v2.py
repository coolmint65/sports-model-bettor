"""
Stat-aware MLB feature extraction for player-prop GBM training.

One feature builder per stat — universal features (rolling_30d,
rolling_l5, rest_days, is_home, weather_temp_f) are shared, plus a
small stat-specific block that pulls the most theoretically-relevant
opponent / context columns.

Pitcher stats (k_p, bb_p, outs, er, h_allowed): opponent's TEAM
stats matter (opp_team_k_pct for K props, opp_team_obp for hits-
allowed, etc.). Plus pitcher's own season-to-date numbers.

Batter stats (hr, h, tb, rbi, r, sb, bb_b, k_b): opposing PITCHER's
season-to-date stats matter (the batter is facing one pitcher, not
a team). Plus park HR/H factor depending on the stat.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# Universal features for every stat.
_UNIVERSAL_COLS = [
    "rolling_30d", "rolling_l5",
    "rest_days", "is_home",
    "weather_temp_f",
]

# Per-stat extra columns. Order matters — features are stacked in
# the same order across rows so the column index is stable.
_PER_STAT_EXTRA = {
    # ── Pitcher stats: opp team batting metrics + own season ──
    "k_p":       ["opp_team_k_pct", "season_k_per_9", "ump_run_factor", "park_h_factor"],
    "bb_p":     ["opp_team_bb_pct", "season_bb_per_9", "ump_run_factor"],
    "outs":     ["opp_team_runs_pg", "season_innings_per_start", "park_run_factor"],
    "er":       ["opp_team_runs_pg", "season_era", "park_run_factor", "ump_run_factor"],
    "h_allowed":["opp_team_avg", "season_whip", "park_h_factor"],

    # ── Batter stats: opp SP season stats + park ──
    "hr":       ["opp_sp_hr_per_9", "park_hr_factor"],
    "h":        ["opp_sp_whip", "park_h_factor"],
    "tb":       ["opp_sp_hr_per_9", "opp_sp_whip", "park_hr_factor"],
    "rbi":      ["opp_sp_era", "park_run_factor"],
    "r":        ["opp_sp_era", "park_run_factor"],
    "sb":       [],  # rolling rate dominates; no extra signal worth pulling
    "bb_b":     ["opp_sp_bb_per_9"],
    "k_b":      ["opp_sp_k_per_9"],
}

# Stat → role classification (filters game-log rows + chooses which
# pitcher feature path to use for opposing-pitcher lookups).
_PITCHER_STATS = {"k_p", "bb_p", "outs", "er", "h_allowed"}
_BATTER_STATS = {"hr", "h", "tb", "rbi", "r", "sb", "bb_b", "k_b"}


def feature_cols(stat_key: str) -> list[str]:
    return _UNIVERSAL_COLS + _PER_STAT_EXTRA.get(stat_key, [])


@dataclass
class TrainingRow:
    player_id: int
    game_id: str
    date: str
    target: float
    features: dict


def _conn():
    from .db import get_conn
    return get_conn()


# ── Universal helpers ────────────────────────────────────────

def _rolling(conn, player_id: int, stat_key: str, before_date: str,
              days: int = 30, max_n: int | None = None) -> float | None:
    cutoff = (datetime.strptime(before_date, "%Y-%m-%d") -
              timedelta(days=days)).strftime("%Y-%m-%d")
    presence_filter = ("json_extract(stats_json, '$.outs') > 0"
                       if stat_key in _PITCHER_STATS
                       else "json_extract(stats_json, '$.pa') > 0")
    sql = (
        f"SELECT stats_json FROM player_game_logs "
        f"WHERE player_id = ? AND date >= ? AND date < ? "
        f"  AND {presence_filter} "
        f"ORDER BY date DESC"
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
        if v is None:
            continue
        try:
            vals.append(float(v))
        except (TypeError, ValueError):
            continue
    return sum(vals) / len(vals) if vals else None


def _rest_days(conn, player_id: int, stat_key: str, before_date: str) -> int | None:
    presence = ("$.outs" if stat_key in _PITCHER_STATS else "$.pa")
    row = conn.execute(
        f"SELECT MAX(date) AS last FROM player_game_logs "
        f"WHERE player_id = ? AND date < ? "
        f"  AND json_extract(stats_json, '{presence}') > 0",
        (player_id, before_date),
    ).fetchone()
    if not row or not row["last"]:
        return None
    try:
        last = datetime.strptime(row["last"], "%Y-%m-%d")
        cur = datetime.strptime(before_date, "%Y-%m-%d")
        return (cur - last).days
    except (ValueError, TypeError):
        return None


# ── Per-stat extra extractors ────────────────────────────────

def _opp_team_stat(conn, opp_team_id: int, season: int, col: str) -> float | None:
    if not opp_team_id:
        return None
    row = conn.execute(
        f"SELECT {col} FROM team_stats WHERE team_id = ? AND season = ? LIMIT 1",
        (opp_team_id, season),
    ).fetchone()
    if not row or row[col] is None:
        return None
    return float(row[col])


def _own_pitcher_stat(conn, pitcher_id: int, season: int, col: str) -> float | None:
    row = conn.execute(
        f"SELECT {col} FROM pitcher_stats WHERE player_id = ? AND season = ? LIMIT 1",
        (pitcher_id, season),
    ).fetchone()
    if not row or row[col] is None:
        return None
    return float(row[col])


def _opp_sp_id(g: dict, batter_team_id: int) -> int | None:
    """For a batter's game, return the opposing starting pitcher's id."""
    if g.get("home_team_id") == batter_team_id:
        return g.get("away_pitcher_id")
    if g.get("away_team_id") == batter_team_id:
        return g.get("home_pitcher_id")
    return None


def _park_factor(conn, venue: str, season: int, col: str) -> float | None:
    if not venue:
        return None
    row = conn.execute(
        f"SELECT {col} FROM park_factors WHERE venue = ? AND season = ? LIMIT 1",
        (venue, season),
    ).fetchone()
    if not row or row[col] is None:
        return None
    return float(row[col])


def _ump_factor(conn, ump: str, season: int) -> float | None:
    if not ump:
        return None
    row = conn.execute(
        "SELECT run_factor FROM umpire_season_stats "
        "WHERE name = ? AND season <= ? "
        "ORDER BY season DESC LIMIT 1",
        (ump, season),
    ).fetchone()
    if not row or row["run_factor"] is None:
        return None
    return float(row["run_factor"])


def _season_innings_per_start(conn, pitcher_id: int, season: int) -> float | None:
    row = conn.execute(
        "SELECT innings, games_started FROM pitcher_stats "
        "WHERE player_id = ? AND season = ?", (pitcher_id, season),
    ).fetchone()
    if not row or not row["games_started"] or not row["innings"]:
        return None
    try:
        return float(row["innings"]) / float(row["games_started"])
    except (TypeError, ValueError, ZeroDivisionError):
        return None


# ── Per-stat extractor router ─────────────────────────────────

def _extra_for_stat(conn, stat_key: str, *, game: dict, player_id: int,
                    player_team_id: int, opp_team_id: int,
                    season: int) -> dict:
    """Returns a dict of {col_name: value or None} for the stat's
    extra features."""
    out: dict[str, float | None] = {}
    extras = _PER_STAT_EXTRA.get(stat_key, [])
    for col in extras:
        if col == "opp_team_k_pct":
            out[col] = _opp_team_stat(conn, opp_team_id, season, "k_pct")
        elif col == "opp_team_bb_pct":
            out[col] = _opp_team_stat(conn, opp_team_id, season, "bb_pct")
        elif col == "opp_team_runs_pg":
            out[col] = _opp_team_stat(conn, opp_team_id, season, "runs_pg")
        elif col == "opp_team_avg":
            out[col] = _opp_team_stat(conn, opp_team_id, season, "avg")
        elif col == "season_k_per_9":
            out[col] = _own_pitcher_stat(conn, player_id, season, "k_per_9")
        elif col == "season_bb_per_9":
            out[col] = _own_pitcher_stat(conn, player_id, season, "bb_per_9")
        elif col == "season_era":
            out[col] = _own_pitcher_stat(conn, player_id, season, "era")
        elif col == "season_whip":
            out[col] = _own_pitcher_stat(conn, player_id, season, "whip")
        elif col == "season_innings_per_start":
            out[col] = _season_innings_per_start(conn, player_id, season)
        elif col == "park_h_factor":
            out[col] = _park_factor(conn, game.get("venue", ""), season, "h_factor")
        elif col == "park_hr_factor":
            out[col] = _park_factor(conn, game.get("venue", ""), season, "hr_factor")
        elif col == "park_run_factor":
            out[col] = _park_factor(conn, game.get("venue", ""), season, "run_factor")
        elif col == "ump_run_factor":
            out[col] = _ump_factor(conn, game.get("umpire", ""), season)
        # Batter-side opposing-pitcher features
        elif col.startswith("opp_sp_"):
            stat_col = col.replace("opp_sp_", "")
            sp_id = _opp_sp_id(game, player_team_id)
            out[col] = _own_pitcher_stat(conn, sp_id, season, stat_col) if sp_id else None
        else:
            out[col] = None
    return out


def extract_features(player_id: int, game_pk: str, stat_key: str) -> dict | None:
    """Build the full feature dict for one (player, game, stat).
    Returns None if the player doesn't have rolling history (the
    one feature we always need)."""
    conn = _conn()
    g = conn.execute("SELECT * FROM games WHERE mlb_game_id = ?", (str(game_pk),)).fetchone()
    if not g:
        return None
    g = dict(g)
    season = int(g.get("season") or datetime.strptime(g["date"], "%Y-%m-%d").year)

    # Find player's team + opp team for this game.
    pgl = conn.execute(
        "SELECT team_id, opp_team_id, is_home FROM player_game_logs "
        "WHERE player_id = ? AND game_id = ?",
        (player_id, str(game_pk)),
    ).fetchone()
    if pgl:
        player_team_id = pgl["team_id"]
        opp_team_id = pgl["opp_team_id"]
        is_home = bool(pgl["is_home"])
    else:
        return None

    rolling_30d = _rolling(conn, player_id, stat_key, g["date"], days=30)
    if rolling_30d is None:
        return None
    rolling_l5 = _rolling(conn, player_id, stat_key, g["date"], days=90, max_n=5)
    rest = _rest_days(conn, player_id, stat_key, g["date"])
    temp = g.get("weather_temp")
    try:
        temp_f = float(temp) if temp is not None else None
    except (TypeError, ValueError):
        temp_f = None

    feats = {
        "rolling_30d":  rolling_30d,
        "rolling_l5":   rolling_l5,
        "rest_days":    rest,
        "is_home":      1 if is_home else 0,
        "weather_temp_f": temp_f,
    }
    feats.update(_extra_for_stat(
        conn, stat_key, game=g, player_id=player_id,
        player_team_id=player_team_id, opp_team_id=opp_team_id,
        season=season,
    ))
    return feats


def build_training_set(stat_key: str, *,
                        lookback_days: int = 180) -> list[TrainingRow]:
    """Build training data for one stat. Filters game-log rows by
    role (pitcher stats need outs>0; batter stats need pa>0) and
    requires the target value to be present."""
    conn = _conn()
    today = datetime.now().strftime("%Y-%m-%d")
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    if stat_key in _PITCHER_STATS:
        presence = "json_extract(stats_json, '$.outs') > 0"
    elif stat_key in _BATTER_STATS:
        presence = "json_extract(stats_json, '$.pa') > 0"
    else:
        return []
    rows = conn.execute(
        f"SELECT player_id, game_id, date, stats_json "
        f"FROM player_game_logs "
        f"WHERE date >= ? AND date <= ? AND {presence}",
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
