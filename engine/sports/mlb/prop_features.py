"""
MLB pitcher-Ks feature extraction (Phase 2k-i / training prep).

Pulls the per-(pitcher, game) features the GBM will train on. Strict
no-leak rule: every feature is computed using ONLY data that would
have been available BEFORE the target game, so the training split
mirrors what the live picker sees on game day.

Features extracted (only ones with a clear theoretical reason to
move pitcher Ks — keeps the candidate set tight so the per-feature
ablation in training has fewer terms to chase):

    rolling_k_30d     pitcher's mean Ks over last 30 days
    rolling_k_l5      same but last 5 starts (recent form)
    season_k_per_9    pitcher's season K/9 from pitcher_stats
    season_k_pct      pitcher's season K% (when populated)
    opp_team_k_pct    opposing team's season strikeout rate
    park_h_factor     venue h_factor (proxy — no K factor in DB)
    ump_run_factor    umpire's season run factor (low = pitcher-friendly)
    rest_days         days since pitcher's last start (0 if first)
    is_home           1 if pitching at home, else 0
    weather_temp_f    game-time temperature (None → median imputed)

The actual target is the pitcher's K count in this game.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from ..._tz import et_today_str

logger = logging.getLogger(__name__)

FEATURE_COLS = [
    "rolling_k_30d", "rolling_k_l5",
    "season_k_per_9", "season_k_pct",
    "opp_team_k_pct",
    "park_h_factor", "ump_run_factor",
    "rest_days", "is_home",
    "weather_temp_f",
]


@dataclass
class TrainingRow:
    pitcher_id: int
    game_id: str
    date: str
    target_k: float
    features: dict


def _conn():
    from ...db import get_conn
    return get_conn()


def _opp_team_id_for(game_row: dict, pitcher_team_id: int) -> int | None:
    """Returns the opposing team_id given the games-table row and the
    pitcher's team."""
    if game_row["home_team_id"] == pitcher_team_id:
        return game_row["away_team_id"]
    if game_row["away_team_id"] == pitcher_team_id:
        return game_row["home_team_id"]
    return None


def _rolling_k(conn, pitcher_id: int, before_date: str,
               days: int = 30, max_starts: int | None = None) -> float | None:
    """Mean Ks per start over the trailing window. ``max_starts``
    caps to a fixed N regardless of date span (used for last-5)."""
    cutoff = (datetime.strptime(before_date, "%Y-%m-%d") -
              timedelta(days=days)).strftime("%Y-%m-%d")
    sql = (
        "SELECT stats_json FROM player_game_logs "
        "WHERE player_id = ? AND date >= ? AND date < ? "
        "  AND json_extract(stats_json, '$.outs') > 0 "
        "ORDER BY date DESC"
    )
    rows = conn.execute(sql, (pitcher_id, cutoff, before_date)).fetchall()
    if max_starts is not None:
        rows = rows[:max_starts]
    ks = []
    for r in rows:
        try:
            stats = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        v = stats.get("k_p")
        if v is not None:
            try:
                ks.append(float(v))
            except (TypeError, ValueError):
                continue
    if not ks:
        return None
    return sum(ks) / len(ks)


def _season_pitcher_stats(conn, pitcher_id: int, season: int) -> tuple[float | None, float | None]:
    """Returns (k_per_9, k_pct) for the pitcher's season-to-date.
    Both can be None — early-season rows often miss k_pct."""
    row = conn.execute(
        "SELECT k_per_9, k_pct FROM pitcher_stats "
        "WHERE player_id = ? AND season = ? LIMIT 1",
        (pitcher_id, season),
    ).fetchone()
    if not row:
        return None, None
    k9 = row["k_per_9"]
    kp = row["k_pct"]
    return (float(k9) if k9 is not None else None,
            float(kp) if kp is not None else None)


def _opp_team_k_pct(conn, opp_team_id: int, season: int) -> float | None:
    row = conn.execute(
        "SELECT k_pct FROM team_stats WHERE team_id = ? AND season = ? LIMIT 1",
        (opp_team_id, season),
    ).fetchone()
    if not row or row["k_pct"] is None:
        return None
    return float(row["k_pct"])


def _park_h_factor(conn, venue: str, season: int) -> float | None:
    if not venue:
        return None
    row = conn.execute(
        "SELECT h_factor FROM park_factors WHERE venue = ? AND season = ? LIMIT 1",
        (venue, season),
    ).fetchone()
    if not row or row["h_factor"] is None:
        return None
    return float(row["h_factor"])


def _ump_run_factor(conn, ump_name: str, season: int) -> float | None:
    if not ump_name:
        return None
    row = conn.execute(
        "SELECT run_factor FROM umpire_season_stats "
        "WHERE name = ? AND season <= ? "
        "ORDER BY season DESC LIMIT 1",
        (ump_name, season),
    ).fetchone()
    if not row or row["run_factor"] is None:
        return None
    return float(row["run_factor"])


def _rest_days(conn, pitcher_id: int, before_date: str) -> int | None:
    row = conn.execute(
        "SELECT MAX(date) AS last_date FROM player_game_logs "
        "WHERE player_id = ? AND date < ? "
        "  AND json_extract(stats_json, '$.outs') > 0",
        (pitcher_id, before_date),
    ).fetchone()
    if not row or not row["last_date"]:
        return None
    try:
        last = datetime.strptime(row["last_date"], "%Y-%m-%d")
        cur = datetime.strptime(before_date, "%Y-%m-%d")
        return (cur - last).days
    except (ValueError, TypeError):
        return None


def extract_features(pitcher_id: int, game_pk: str) -> dict | None:
    """Extract the full feature dict for a single (pitcher, game).
    Returns None if the game can't be resolved or critical features
    can't be computed (rolling_k_30d is required — without it the
    pitcher has no recent history to build from)."""
    conn = _conn()
    g = conn.execute(
        "SELECT * FROM games WHERE mlb_game_id = ?", (str(game_pk),),
    ).fetchone()
    if not g:
        return None
    g = dict(g)
    season = int(g.get("season") or datetime.strptime(g["date"], "%Y-%m-%d").year)

    # Determine pitcher's team — look up player_game_logs for this game.
    pgl = conn.execute(
        "SELECT team_id, opp_team_id, is_home FROM player_game_logs "
        "WHERE player_id = ? AND game_id = ?",
        (pitcher_id, str(game_pk)),
    ).fetchone()
    if pgl:
        pitcher_team_id = pgl["team_id"]
        opp_team_id = pgl["opp_team_id"]
        is_home = bool(pgl["is_home"])
    else:
        # Fall back to deriving from games row when player_game_logs
        # hasn't been written yet (live game day).
        if g.get("home_pitcher_id") == pitcher_id:
            pitcher_team_id = g.get("home_team_id")
            opp_team_id = g.get("away_team_id")
            is_home = True
        elif g.get("away_pitcher_id") == pitcher_id:
            pitcher_team_id = g.get("away_team_id")
            opp_team_id = g.get("home_team_id")
            is_home = False
        else:
            return None

    rolling_30d = _rolling_k(conn, pitcher_id, g["date"], days=30)
    if rolling_30d is None:
        return None  # No recent history — bail rather than build a fake row.

    rolling_l5 = _rolling_k(conn, pitcher_id, g["date"], days=90, max_starts=5)
    season_k9, season_kpct = _season_pitcher_stats(conn, pitcher_id, season)
    opp_kpct = _opp_team_k_pct(conn, opp_team_id, season) if opp_team_id else None
    park_h = _park_h_factor(conn, g.get("venue", ""), season)
    ump_rf = _ump_run_factor(conn, g.get("umpire", ""), season)
    rest = _rest_days(conn, pitcher_id, g["date"])
    temp = g.get("weather_temp")
    try:
        temp_f = float(temp) if temp is not None else None
    except (TypeError, ValueError):
        temp_f = None

    return {
        "rolling_k_30d":   rolling_30d,
        "rolling_k_l5":    rolling_l5,
        "season_k_per_9":  season_k9,
        "season_k_pct":    season_kpct,
        "opp_team_k_pct":  opp_kpct,
        "park_h_factor":   park_h,
        "ump_run_factor":  ump_rf,
        "rest_days":       rest,
        "is_home":         1 if is_home else 0,
        "weather_temp_f":  temp_f,
    }


def build_training_set(*, lookback_days: int = 30) -> list[TrainingRow]:
    """Walk every (pitcher, game) row from the last ``lookback_days``
    where the pitcher started AND we know their K count. Returns
    one TrainingRow per qualifying row."""
    conn = _conn()
    today = et_today_str()
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT player_id, game_id, date, stats_json "
        "FROM player_game_logs "
        "WHERE date >= ? AND date <= ? "
        "  AND json_extract(stats_json, '$.is_starter') = 1",
        (cutoff, today),
    ).fetchall()
    out: list[TrainingRow] = []
    for r in rows:
        try:
            stats = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        target = stats.get("k_p")
        if target is None:
            continue
        feats = extract_features(r["player_id"], str(r["game_id"]))
        if not feats:
            continue
        out.append(TrainingRow(
            pitcher_id=int(r["player_id"]),
            game_id=str(r["game_id"]),
            date=r["date"],
            target_k=float(target),
            features=feats,
        ))
    return out


__all__ = [
    "FEATURE_COLS", "TrainingRow",
    "extract_features", "build_training_set",
]
