"""Feature extraction for the basketball GBM.

For each game, build a point-in-time feature row from:
  - Rolling (last-10) team pace, ORtg, DRtg, PPG, win pct
  - Season-to-date team pace, ORtg, DRtg, PPG, win pct, games played
  - Schedule density (rest days, B2B flag, 3-in-4)
  - Cross-team deltas (home - away on each load-bearing rate)

Reads from the per-league ``games`` + ``game_team_stats`` tables.
``game_team_stats`` carries pace/ORtg/DRtg per team-game; missing rows
fall back to PPG-only features so games without a box-score don't
break the dataset.

No leakage: every feature uses only games dated strictly before the
target game's date.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from ._db import get_conn

logger = logging.getLogger(__name__)


# Defaults applied when a team has zero prior games this season — keeps
# the feature row dense so XGBoost doesn't need missing-value handling.
# Hardcoded baselines per league. Can be overridden by an on-disk file
# at data/basketball/{league}_feature_defaults.json so calibration jobs
# can refresh them from prior-season averages without code edits.
_DEFAULTS_BY_LEAGUE = {
    "wnba": {
        "pace": 81.0, "ortg": 100.0, "drtg": 100.0,
        "ppg": 81.0, "opp_ppg": 81.0,
    },
    "ncaam": {
        "pace": 67.0, "ortg": 105.0, "drtg": 105.0,
        "ppg": 70.0, "opp_ppg": 70.0,
    },
    "ncaaw": {
        "pace": 73.0, "ortg": 95.0, "drtg": 95.0,
        "ppg": 70.0, "opp_ppg": 70.0,
    },
    "euroleague": {
        "pace": 70.0, "ortg": 110.0, "drtg": 110.0,
        "ppg": 80.0, "opp_ppg": 80.0,
    },
}


def _load_league_defaults(league: str) -> dict:
    """Read on-disk override if present, else fall back to the hardcoded
    _DEFAULTS_BY_LEAGUE entry. Override path follows the same per-league
    conventions as _constants.json / _calibration.json."""
    from pathlib import Path
    import json
    p = Path(__file__).resolve().parent.parent.parent / "data" / "basketball" / f"{league}_feature_defaults.json"
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("[%s] feature-defaults override load failed: %s",
                           league, e)
    return _DEFAULTS_BY_LEAGUE.get(league) or {}


def feature_columns() -> list[str]:
    """Stable feature column order. Must match between train + predict."""
    cols = []
    for prefix in ("home", "away"):
        for w in ("l10", "season"):
            for k in ("pace", "ortg", "drtg", "ppg", "opp_ppg",
                       "win_pct"):
                cols.append(f"{prefix}_{w}_{k}")
            cols.append(f"{prefix}_{w}_games")
        cols.append(f"{prefix}_rest_days")
        cols.append(f"{prefix}_b2b")
        cols.append(f"{prefix}_3in4")
    for k in ("pace", "ortg", "drtg", "ppg", "win_pct"):
        cols.append(f"l10_{k}_delta")
    cols.append("rest_delta")
    cols.append("b2b_delta")
    return cols


def extract_features(league: str, game: dict) -> dict[str, float] | None:
    """Build the feature dict for one game, point-in-time.

    Returns None when required identifiers are missing."""
    home_id = game.get("home_team_id")
    away_id = game.get("away_team_id")
    game_date = game.get("date")
    if not (home_id and away_id and game_date):
        return None
    season = game.get("season") or _season_from_date(league, game_date)

    conn = get_conn(league)
    feats: dict[str, float] = {}
    home_stats = _team_window_stats(conn, league, home_id, game_date, season)
    away_stats = _team_window_stats(conn, league, away_id, game_date, season)
    for k, v in home_stats.items():
        feats[f"home_{k}"] = v
    for k, v in away_stats.items():
        feats[f"away_{k}"] = v
    feats["home_rest_days"] = _rest_days(conn, league, home_id, game_date)
    feats["away_rest_days"] = _rest_days(conn, league, away_id, game_date)
    h_density = _schedule_density(conn, league, home_id, game_date)
    a_density = _schedule_density(conn, league, away_id, game_date)
    feats["home_b2b"] = h_density["b2b"]
    feats["away_b2b"] = a_density["b2b"]
    feats["home_3in4"] = h_density["three_in_four"]
    feats["away_3in4"] = a_density["three_in_four"]
    # Deltas (home - away)
    for k in ("pace", "ortg", "drtg", "ppg", "win_pct"):
        feats[f"l10_{k}_delta"] = (feats.get(f"home_l10_{k}", 0)
                                    - feats.get(f"away_l10_{k}", 0))
    feats["rest_delta"] = feats["home_rest_days"] - feats["away_rest_days"]
    feats["b2b_delta"] = feats["away_b2b"] - feats["home_b2b"]
    return feats


def extract_targets(game: dict) -> dict[str, float] | None:
    """Targets we train against: home_win (0/1), margin, total_points.
    Returns None for non-final games (no labels yet)."""
    if game.get("status") != "final":
        return None
    hs, as_ = game.get("home_score"), game.get("away_score")
    if hs is None or as_ is None:
        return None
    return {
        "home_win": 1 if hs > as_ else 0,
        "margin": hs - as_,
        "total_points": hs + as_,
    }


# ── Helpers ──────────────────────────────────────────────────

def _team_window_stats(conn, league: str, team_id: int,
                        cutoff_date: str, season: int) -> dict[str, float]:
    """Last-10 + season-to-date stats for a team, all games before cutoff."""
    defaults = _load_league_defaults(league) or _DEFAULTS_BY_LEAGUE["wnba"]
    out: dict[str, float] = {}

    # All prior games this season — for season-to-date
    season_rows = conn.execute(
        "SELECT g.game_id, g.date, g.home_team_id, g.away_team_id, "
        "       g.home_score, g.away_score, "
        "       s.pace AS pace, s.ortg AS ortg, s.drtg AS drtg "
        "FROM games g "
        "LEFT JOIN game_team_stats s "
        "  ON s.game_id = g.game_id AND s.team_id = ? "
        "WHERE g.status = 'final' "
        "  AND g.season = ? "
        "  AND g.date < ? "
        "  AND (g.home_team_id = ? OR g.away_team_id = ?) "
        "  AND g.home_score IS NOT NULL "
        "ORDER BY g.date DESC",
        (team_id, season, cutoff_date, team_id, team_id),
    ).fetchall()
    season_n = len(season_rows)

    def _aggregate(rows):
        if not rows:
            return None
        pace_vals = [r["pace"] for r in rows if r["pace"] is not None]
        ortg_vals = [r["ortg"] for r in rows if r["ortg"] is not None]
        drtg_vals = [r["drtg"] for r in rows if r["drtg"] is not None]
        ppg, opp_ppg, wins = [], [], 0
        for r in rows:
            is_home = r["home_team_id"] == team_id
            if is_home:
                ppg.append(r["home_score"])
                opp_ppg.append(r["away_score"])
                wins += 1 if r["home_score"] > r["away_score"] else 0
            else:
                ppg.append(r["away_score"])
                opp_ppg.append(r["home_score"])
                wins += 1 if r["away_score"] > r["home_score"] else 0
        return {
            "pace": (sum(pace_vals) / len(pace_vals)) if pace_vals else defaults["pace"],
            "ortg": (sum(ortg_vals) / len(ortg_vals)) if ortg_vals else defaults["ortg"],
            "drtg": (sum(drtg_vals) / len(drtg_vals)) if drtg_vals else defaults["drtg"],
            "ppg": sum(ppg) / len(ppg),
            "opp_ppg": sum(opp_ppg) / len(opp_ppg),
            "win_pct": wins / len(rows),
            "games": len(rows),
        }

    season_agg = _aggregate(season_rows)
    if season_agg:
        for k, v in season_agg.items():
            out[f"season_{k}"] = v
    else:
        out.update({
            "season_pace": defaults["pace"], "season_ortg": defaults["ortg"],
            "season_drtg": defaults["drtg"], "season_ppg": defaults["ppg"],
            "season_opp_ppg": defaults["opp_ppg"], "season_win_pct": 0.5,
            "season_games": 0,
        })

    l10 = season_rows[:10]
    l10_agg = _aggregate(l10)
    if l10_agg:
        for k, v in l10_agg.items():
            out[f"l10_{k}"] = v
    else:
        out.update({
            "l10_pace": defaults["pace"], "l10_ortg": defaults["ortg"],
            "l10_drtg": defaults["drtg"], "l10_ppg": defaults["ppg"],
            "l10_opp_ppg": defaults["opp_ppg"], "l10_win_pct": 0.5,
            "l10_games": 0,
        })
    return out


def _rest_days(conn, league: str, team_id: int, cutoff_date: str) -> int:
    """Days since the team's last game. Capped at 14 (anything beyond
    is treated as fully rested)."""
    last = conn.execute(
        "SELECT MAX(date) AS last FROM games "
        "WHERE status = 'final' "
        "  AND date < ? "
        "  AND (home_team_id = ? OR away_team_id = ?)",
        (cutoff_date, team_id, team_id),
    ).fetchone()
    if not last or not last["last"]:
        return 14
    try:
        a = datetime.strptime(cutoff_date, "%Y-%m-%d")
        b = datetime.strptime(last["last"], "%Y-%m-%d")
    except ValueError:
        return 14
    return min(14, (a - b).days)


def _schedule_density(conn, league: str, team_id: int,
                       cutoff_date: str) -> dict:
    """B2B / 3-in-4 flags for a team going into ``cutoff_date``."""
    try:
        cutoff = datetime.strptime(cutoff_date, "%Y-%m-%d")
    except ValueError:
        return {"b2b": 0, "three_in_four": 0}
    look_back = (cutoff - timedelta(days=4)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT date FROM games "
        "WHERE status = 'final' "
        "  AND date < ? AND date >= ? "
        "  AND (home_team_id = ? OR away_team_id = ?)",
        (cutoff_date, look_back, team_id, team_id),
    ).fetchall()
    b2b = 0
    three_in_four = 0
    yesterday = (cutoff - timedelta(days=1)).strftime("%Y-%m-%d")
    for r in rows:
        if r["date"] == yesterday:
            b2b = 1
    if len(rows) >= 2:
        three_in_four = 1  # 2 games in last 4 days, plus today = 3
    return {"b2b": b2b, "three_in_four": three_in_four}


def _season_from_date(league: str, game_date: str) -> int | None:
    """Best-guess season-start year from a game date. Each league has
    its own season-start month; defaults to October if unspecified."""
    from ._config import get_league_config
    cfg = get_league_config(league)
    months = cfg.get("season_months") or (10,)
    primary = months[0]
    try:
        d = datetime.strptime(game_date, "%Y-%m-%d")
    except ValueError:
        return None
    return d.year if d.month >= primary else d.year - 1
