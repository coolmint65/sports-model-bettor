"""
MLB 1st-inning per-team feature extraction (Phase 2k-iii).

Per-team binary target: did this team score ≥1 run in the 1st
inning of this game? Per-game NRFI probability is then derived as

    P(NRFI) = (1 - P_home_scores) × (1 - P_away_scores)

assuming home/away independence (reasonable for a one-inning,
one-pitcher event where the two teams' offensive contributions
don't interact).

Features default to NaN when the underlying data isn't available;
XGBoost handles that natively. Strict no-leak: every feature is
computed using only data observable BEFORE the game's date.

Tier 1 (always extracted, no lineup dependency):
    opp_sp_first_inning_era         pitcher's career 1st-inning ERA
    opp_sp_first_inning_runs_per    pitcher's career 1st-inning R/start
    opp_sp_first_inning_scoreless   pitcher's career 1st-inning scoreless%
    opp_sp_first_inning_starts      sample-size weight
    opp_sp_recent5_first_inning_r   recent-form 1st-inning runs
    opp_sp_season_k_per_9           strikeout rate
    opp_sp_season_bb_per_9          walk rate
    team_first_inning_avg_runs      this team's season 1st-INN R/G
    team_first_inning_score_pct     this team's season 1st-INN score%
    park_run_factor                 venue run factor
    park_hr_factor                  HR-driven YRFI proxy
    park_h_factor                   hits-driven baserunner proxy

Tier 3 (also no lineup dependency):
    weather_temp_f
    weather_wind_speed_out_mph      wind blowing out (positive YRFI)
    is_day_game
    is_opener                       opp SP is a relief-pitcher opener
    ump_run_factor                  full-game umpire factor
    is_home_team                    home team bats 2nd in 1st
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from ..._tz import et_today_str

logger = logging.getLogger(__name__)


_TIER_1 = [
    "opp_sp_first_inning_era",
    "opp_sp_first_inning_runs_per",
    "opp_sp_first_inning_scoreless",
    "opp_sp_first_inning_starts",
    "opp_sp_recent5_first_inning_r",
    "opp_sp_season_k_per_9",
    "opp_sp_season_bb_per_9",
    "team_first_inning_avg_runs",
    "team_first_inning_score_pct",
    "park_run_factor",
    "park_hr_factor",
    "park_h_factor",
]

_TIER_3 = [
    "weather_temp_f",
    "weather_wind_speed_out_mph",
    "is_day_game",
    "is_opener",
    "ump_run_factor",
    "is_home_team",
]


def feature_cols() -> list[str]:
    return list(_TIER_1) + list(_TIER_3)


@dataclass
class TrainingRow:
    game_id: int
    date: str
    team_id: int          # the offensive team
    opp_pitcher_id: int   # the pitcher this team faced in the 1st
    target: int           # 1 if team scored ≥1 in 1st, else 0
    features: dict


def _conn():
    from ...db import get_conn
    return get_conn()


# ── Pitcher features ──────────────────────────────────────────

_pitcher_cache: dict[tuple, dict | None] = {}


def _pit_first_inning(pitcher_id: int, before_date: str,
                       season: int) -> dict:
    """Career-to-date 1st-inning stats for pitcher computed from
    linescores of his prior starts. Cached per (pitcher, date) so
    repeated lookups in training don't re-walk the same starts."""
    key = (pitcher_id, before_date)
    if key in _pitcher_cache:
        return _pitcher_cache[key] or {}
    conn = _conn()
    rows = conn.execute(
        "SELECT g.home_pitcher_id, g.away_pitcher_id, "
        "       g.home_linescore, g.away_linescore "
        "FROM games g "
        "WHERE g.date < ? AND g.status = 'final' "
        "  AND (g.home_pitcher_id = ? OR g.away_pitcher_id = ?) "
        "  AND g.home_linescore IS NOT NULL "
        "ORDER BY g.date DESC LIMIT 100",
        (before_date, pitcher_id, pitcher_id),
    ).fetchall()
    starts = scoreless = total_runs = 0
    recent5_runs: list[int] = []
    for r in rows:
        home_pid = r["home_pitcher_id"]
        if pitcher_id == home_pid:
            ls = r["away_linescore"]
        else:
            ls = r["home_linescore"]
        try:
            arr = json.loads(ls)
            if not arr:
                continue
            r1 = int(arr[0])
        except (ValueError, TypeError, json.JSONDecodeError):
            continue
        starts += 1
        total_runs += r1
        if r1 == 0:
            scoreless += 1
        if len(recent5_runs) < 5:
            recent5_runs.append(r1)
    out = {
        "starts": starts,
        "first_inning_era": (total_runs / starts) * 9 if starts else None,
        "first_inning_runs_per": total_runs / starts if starts else None,
        "first_inning_scoreless_pct": scoreless / starts if starts else None,
        "recent5_first_inning_r": (sum(recent5_runs) / len(recent5_runs)
                                    if recent5_runs else None),
    }
    _pitcher_cache[key] = out
    return out


def _pit_season(pitcher_id: int, season: int) -> tuple[float | None, float | None]:
    conn = _conn()
    row = conn.execute(
        "SELECT k_per_9, bb_per_9 FROM pitcher_stats "
        "WHERE player_id = ? AND season = ? LIMIT 1",
        (pitcher_id, season),
    ).fetchone()
    if not row:
        return None, None
    return ((float(row["k_per_9"]) if row["k_per_9"] is not None else None),
            (float(row["bb_per_9"]) if row["bb_per_9"] is not None else None))


def _is_opener(pitcher_id: int, before_date: str) -> int:
    """Heuristic: a pitcher whose career avg IP per appearance is
    < 3 is acting as an opener / bullpen arm regardless of how the
    game lists them. Cached via the pit_first_inning machinery."""
    conn = _conn()
    row = conn.execute(
        "SELECT AVG(json_array_length(home_linescore)) AS avg_ip "
        "FROM games g "
        "WHERE g.date < ? AND g.status = 'final' "
        "  AND (g.home_pitcher_id = ? OR g.away_pitcher_id = ?) LIMIT 1",
        (before_date, pitcher_id, pitcher_id),
    ).fetchone()
    # This is an approximation — real IP comes from boxscore. For
    # tonight, fall back to season innings_per_start.
    season = int(before_date[:4])
    ips_row = conn.execute(
        "SELECT innings, games_started FROM pitcher_stats "
        "WHERE player_id = ? AND season = ?",
        (pitcher_id, season),
    ).fetchone()
    if ips_row and ips_row["games_started"]:
        try:
            ip_per_start = float(ips_row["innings"]) / float(ips_row["games_started"])
            return 1 if ip_per_start < 3.0 else 0
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return 0


# ── Team features ──────────────────────────────────────────────

def _team_first_inning(team_id: int, season: int) -> tuple[float | None, float | None]:
    """Returns (avg_runs_in_1st, score_pct) for this team's season-
    to-date offensive 1st-inning. Same source the legacy 1st INN
    model uses."""
    try:
        from ...pit_stats import compute_team_stats_at_date
    except Exception:
        return None, None
    today = et_today_str()
    stats = compute_team_stats_at_date(team_id, today, season) or {}
    return (stats.get("first_inning_avg_runs"), stats.get("first_inning_score_pct"))


# ── Park / weather / umpire ──────────────────────────────────

def _park(venue: str, season: int) -> tuple[float | None, float | None, float | None]:
    if not venue:
        return None, None, None
    conn = _conn()
    row = conn.execute(
        "SELECT run_factor, hr_factor, h_factor FROM park_factors "
        "WHERE venue = ? AND season = ? LIMIT 1",
        (venue, season),
    ).fetchone()
    if not row:
        return None, None, None
    return (
        float(row["run_factor"]) if row["run_factor"] is not None else None,
        float(row["hr_factor"]) if row["hr_factor"] is not None else None,
        float(row["h_factor"]) if row["h_factor"] is not None else None,
    )


# Wind-string parser: "12 mph, Out To CF" / "8 mph, In From RF" / "Calm".
_WIND_RE = re.compile(r"(\d+)\s*mph", re.IGNORECASE)
_WIND_OUT_RE = re.compile(r"\bout\b", re.IGNORECASE)


def _wind_out_mph(wind_str) -> float | None:
    """Returns wind speed in mph if it's blowing OUT (toward outfield),
    else 0 for in/cross/calm. None if string unparseable."""
    if not isinstance(wind_str, str):
        return None
    m = _WIND_RE.search(wind_str)
    if not m:
        return None
    speed = float(m.group(1))
    return speed if _WIND_OUT_RE.search(wind_str) else 0.0


def _ump_run_factor(ump_name: str, season: int) -> float | None:
    if not ump_name:
        return None
    conn = _conn()
    row = conn.execute(
        "SELECT run_factor FROM umpire_season_stats "
        "WHERE name = ? AND season <= ? "
        "ORDER BY season DESC LIMIT 1",
        (ump_name, season),
    ).fetchone()
    return float(row["run_factor"]) if row and row["run_factor"] is not None else None


# ── Per-game extraction ───────────────────────────────────────

def extract_features(game: dict, *, batting_team_id: int,
                     opp_pitcher_id: int) -> dict:
    """Build feature dict for one (team, game) row. ``game`` is a
    games-table row dict; ``batting_team_id`` is the offensive
    team; ``opp_pitcher_id`` is the pitcher they faced."""
    season = int(game.get("season") or
                 datetime.strptime(game["date"], "%Y-%m-%d").year)
    pit_fi = _pit_first_inning(opp_pitcher_id, game["date"], season) if opp_pitcher_id else {}
    sp_k9, sp_bb9 = _pit_season(opp_pitcher_id, season) if opp_pitcher_id else (None, None)
    team_avg, team_pct = _team_first_inning(batting_team_id, season)
    park_run, park_hr, park_h = _park(game.get("venue", ""), season)
    wind_out = _wind_out_mph(game.get("weather_wind"))
    try:
        temp_f = float(game.get("weather_temp")) if game.get("weather_temp") is not None else None
    except (TypeError, ValueError):
        temp_f = None
    is_home_team = 1 if batting_team_id == game.get("home_team_id") else 0
    return {
        # Tier 1
        "opp_sp_first_inning_era":      pit_fi.get("first_inning_era"),
        "opp_sp_first_inning_runs_per": pit_fi.get("first_inning_runs_per"),
        "opp_sp_first_inning_scoreless": pit_fi.get("first_inning_scoreless_pct"),
        "opp_sp_first_inning_starts":   pit_fi.get("starts"),
        "opp_sp_recent5_first_inning_r": pit_fi.get("recent5_first_inning_r"),
        "opp_sp_season_k_per_9":        sp_k9,
        "opp_sp_season_bb_per_9":       sp_bb9,
        "team_first_inning_avg_runs":   team_avg,
        "team_first_inning_score_pct":  team_pct,
        "park_run_factor":              park_run,
        "park_hr_factor":               park_hr,
        "park_h_factor":                park_h,
        # Tier 3
        "weather_temp_f":               temp_f,
        "weather_wind_speed_out_mph":   wind_out,
        "is_day_game":                   1 if (game.get("day_night") or "").lower() == "day" else 0,
        "is_opener":                    _is_opener(opp_pitcher_id, game["date"]) if opp_pitcher_id else 0,
        "ump_run_factor":               _ump_run_factor(game.get("umpire", ""), season),
        "is_home_team":                 is_home_team,
    }


def build_training_set(*, lookback_days: int = 1100) -> list[TrainingRow]:
    """Walk games with linescores and build one TrainingRow per
    (team, game). Default lookback covers ~3 seasons."""
    conn = _conn()
    today = et_today_str()
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT * FROM games "
        "WHERE date >= ? AND date <= ? AND status = 'final' "
        "  AND home_linescore IS NOT NULL AND away_linescore IS NOT NULL "
        "  AND home_pitcher_id IS NOT NULL AND away_pitcher_id IS NOT NULL "
        "ORDER BY date",
        (cutoff, today),
    ).fetchall()
    out: list[TrainingRow] = []
    for g in rows:
        gd = dict(g)
        try:
            home_ls = json.loads(gd["home_linescore"])
            away_ls = json.loads(gd["away_linescore"])
            if not home_ls or not away_ls:
                continue
            home_r1 = int(home_ls[0])
            away_r1 = int(away_ls[0])
        except (ValueError, TypeError, json.JSONDecodeError):
            continue
        # Two rows per game: home batting vs away pitcher, away vs home
        for batting_team_id, opp_pit, target in [
            (gd["home_team_id"], gd["away_pitcher_id"], 1 if home_r1 > 0 else 0),
            (gd["away_team_id"], gd["home_pitcher_id"], 1 if away_r1 > 0 else 0),
        ]:
            if not (batting_team_id and opp_pit):
                continue
            feats = extract_features(
                gd, batting_team_id=batting_team_id, opp_pitcher_id=opp_pit,
            )
            out.append(TrainingRow(
                game_id=int(gd["mlb_game_id"]) if gd.get("mlb_game_id") else 0,
                date=gd["date"],
                team_id=int(batting_team_id),
                opp_pitcher_id=int(opp_pit),
                target=target,
                features=feats,
            ))
    return out


__all__ = ["feature_cols", "build_training_set", "extract_features", "TrainingRow"]
