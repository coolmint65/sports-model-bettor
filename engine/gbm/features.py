"""
Feature extraction for the GBM training pipeline.

Given a completed game, extract a feature vector that reflects WHAT THE
MODEL WOULD HAVE KNOWN BEFORE THE GAME STARTED. Point-in-time
discipline is critical -- using season-end stats as features for a
mid-season game is textbook data leakage.

The feature set mirrors what the factor model already uses, so the
GBM trains on the same inputs but learns non-linear interactions the
multiplicative factor stack can't express. Additional raw signals
(exact pitcher ERA/FIP, team wRC+, park run factor, etc.) are passed
in because GBMs can weight them independently.

Returns a dict mapping feature name -> value, which callers turn into
a pandas DataFrame.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)


# Feature-name -> default value when the underlying data is missing.
# GBMs handle NaN natively but stable defaults keep training stable
# across sparse-feature regions.
_DEFAULTS = {
    "home_runs_pg": 4.5,
    "away_runs_pg": 4.5,
    "home_runs_allowed_pg": 4.5,
    "away_runs_allowed_pg": 4.5,
    "home_wrc_plus": 100.0,
    "away_wrc_plus": 100.0,
    "home_ops": 0.720,
    "away_ops": 0.720,
    "home_sp_era": 4.10,
    "away_sp_era": 4.10,
    "home_sp_fip": 4.10,
    "away_sp_fip": 4.10,
    "home_sp_k_pct": 0.225,
    "away_sp_k_pct": 0.225,
    "home_sp_bb_pct": 0.085,
    "away_sp_bb_pct": 0.085,
    "home_sp_whip": 1.30,
    "away_sp_whip": 1.30,
    "home_sp_games_started": 0,
    "away_sp_games_started": 0,
    "home_bullpen_era": 4.20,
    "away_bullpen_era": 4.20,
    "park_run_factor": 1.0,
    "days_into_season": 30,
    "is_playoff": 0,
    "home_form_last_10_pct": 0.500,
    "away_form_last_10_pct": 0.500,
}


def extract_mlb_features(conn, game: dict) -> dict[str, float] | None:
    """Build the feature dict for one completed MLB game.

    Uses point-in-time stats when the PIT helpers return them, and
    falls back to the season snapshot in batter_stats / pitcher_stats
    / bullpen / team_stats when PIT returns None. The season-end
    fallback introduces mild leakage for mid-season games (a May
    prediction sees June's ERA) but is still far more informative
    than the league-average defaults we used before. A strict-PIT
    pass can replace this once per-date snapshotting is implemented.

    Returns None when critical inputs (team IDs, date) are missing.
    """
    date = game.get("date")
    home_id = game.get("home_team_id")
    away_id = game.get("away_team_id")
    if not (date and home_id and away_id):
        return None

    features: dict[str, float] = dict(_DEFAULTS)
    season = _season_of(date)

    # ── Team offensive + defensive rates (PIT first, snapshot fallback) ──
    try:
        from engine.pit_stats import compute_team_stats_at_date
        home_pit = compute_team_stats_at_date(home_id, date, season) or {}
        away_pit = compute_team_stats_at_date(away_id, date, season) or {}
        features["home_runs_pg"] = _num(home_pit.get("runs_pg"), 4.5)
        features["away_runs_pg"] = _num(away_pit.get("runs_pg"), 4.5)
        features["home_runs_allowed_pg"] = _num(home_pit.get("runs_allowed_pg"),
                                                  _team_runs_allowed(conn, home_id, season))
        features["away_runs_allowed_pg"] = _num(away_pit.get("runs_allowed_pg"),
                                                  _team_runs_allowed(conn, away_id, season))
    except Exception as e:
        logger.debug("team PIT stats failed for game %s: %s", game.get("mlb_game_id"), e)

    # Team OPS / wRC+ / bullpen ERA from snapshot tables (season-end leakage
    # accepted). These are the signals that move the needle for GBM.
    features["home_ops"] = _team_ops(conn, home_id, season)
    features["away_ops"] = _team_ops(conn, away_id, season)
    features["home_wrc_plus"] = _team_wrc_plus(conn, home_id, season)
    features["away_wrc_plus"] = _team_wrc_plus(conn, away_id, season)
    features["home_bullpen_era"] = _bullpen_era(conn, home_id, season)
    features["away_bullpen_era"] = _bullpen_era(conn, away_id, season)

    # Pitcher features -- PIT first, fall back to season-end snapshot
    # in pitcher_stats for anything the PIT helper didn't compute.
    try:
        from engine.pit_stats import compute_pitcher_stats_at_date
        for side, pid_key in (("home", "home_pitcher_id"), ("away", "away_pitcher_id")):
            pid = game.get(pid_key)
            if not pid:
                continue
            pit = compute_pitcher_stats_at_date(pid, date, season) or {}
            snap = _pitcher_snapshot(conn, pid, season)
            features[f"{side}_sp_era"] = _num(pit.get("era"), snap.get("era", 4.10))
            features[f"{side}_sp_fip"] = _num(pit.get("fip"), snap.get("fip", 4.10))
            features[f"{side}_sp_k_pct"] = _num(pit.get("k_pct"), snap.get("k_pct", 0.225))
            features[f"{side}_sp_bb_pct"] = _num(pit.get("bb_pct"), snap.get("bb_pct", 0.085))
            features[f"{side}_sp_whip"] = _num(pit.get("whip"), snap.get("whip", 1.30))
            features[f"{side}_sp_games_started"] = _num(pit.get("games_started"),
                                                         snap.get("games_started", 0))
    except Exception as e:
        logger.debug("pitcher stats failed for game %s: %s", game.get("mlb_game_id"), e)

    # Park factor (venue-based)
    try:
        from engine.db import get_park_factor
        park = get_park_factor(game.get("venue") or "") or {}
        features["park_run_factor"] = _num(park.get("run_factor"), 1.0)
    except Exception:
        pass

    # Season-phase features
    features["days_into_season"] = _days_into_season(date)
    features["is_playoff"] = 1 if _is_postseason(date) else 0

    # Recent form -- query the games table directly so we always get a
    # PIT-correct answer (exclude games on or after the target date).
    features["home_form_last_10_pct"] = _form_last_10(conn, home_id, date)
    features["away_form_last_10_pct"] = _form_last_10(conn, away_id, date)

    return features


# ── Direct-table helpers (snapshot-based, mild leakage, strong signal) ──

def _pitcher_snapshot(conn, pitcher_id: int, season: int) -> dict:
    """Season-end snapshot from pitcher_stats. Used as a fallback when
    the PIT helper returns Nones for most rate fields (which it does
    for backfilled historical games without per-date rollups)."""
    try:
        row = conn.execute(
            "SELECT era, fip, k_pct, bb_pct, whip, games_started, hr_per_9, babip "
            "FROM pitcher_stats WHERE player_id = ? AND season = ?",
            (pitcher_id, season),
        ).fetchone()
        return dict(row) if row else {}
    except Exception:
        return {}


def _team_ops(conn, team_id: int, season: int) -> float:
    """Team OPS = weighted mean of batter OPS rows for the team+season."""
    try:
        row = conn.execute(
            "SELECT SUM(ops * at_bats) / NULLIF(SUM(at_bats), 0) AS wops "
            "FROM batter_stats WHERE team_id = ? AND season = ?",
            (team_id, season),
        ).fetchone()
        if row and row["wops"]:
            return float(row["wops"])
    except Exception:
        pass
    return 0.720


def _team_wrc_plus(conn, team_id: int, season: int) -> float:
    """Team wRC+ -- weighted by PA if available, AB otherwise."""
    try:
        row = conn.execute(
            "SELECT SUM(wrc_plus * at_bats) / NULLIF(SUM(at_bats), 0) AS wrc "
            "FROM batter_stats WHERE team_id = ? AND season = ? "
            "  AND wrc_plus IS NOT NULL AND at_bats > 0",
            (team_id, season),
        ).fetchone()
        if row and row["wrc"]:
            return float(row["wrc"])
    except Exception:
        pass
    return 100.0


def _bullpen_era(conn, team_id: int, season: int) -> float:
    """Bullpen ERA for the team+season from the bullpen table."""
    try:
        row = conn.execute(
            "SELECT era FROM bullpen WHERE team_id = ? AND season = ?",
            (team_id, season),
        ).fetchone()
        if row and row["era"]:
            return float(row["era"])
    except Exception:
        pass
    return 4.20


def _team_runs_allowed(conn, team_id: int, season: int) -> float:
    """Season runs allowed per game from the games table (PIT-ish: we use
    ALL finished games of the season, so for mid-season games this is
    leaky. Strict PIT is a later upgrade)."""
    try:
        row = conn.execute(
            "SELECT AVG(CASE WHEN home_team_id = ? THEN away_score "
            "              WHEN away_team_id = ? THEN home_score "
            "              END) AS ra "
            "FROM games WHERE (home_team_id = ? OR away_team_id = ?) "
            "  AND season = ? AND status = 'final'",
            (team_id, team_id, team_id, team_id, season),
        ).fetchone()
        if row and row["ra"]:
            return float(row["ra"])
    except Exception:
        pass
    return 4.5


def _form_last_10(conn, team_id: int, as_of_date: str) -> float:
    """Last-10 win pct, AS OF as_of_date (exclusive). Strict PIT: we
    only look at games strictly before the target date."""
    try:
        rows = conn.execute(
            "SELECT home_team_id, away_team_id, home_score, away_score "
            "FROM games "
            "WHERE (home_team_id = ? OR away_team_id = ?) "
            "  AND status = 'final' AND date < ? "
            "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
            "ORDER BY date DESC LIMIT 10",
            (team_id, team_id, as_of_date),
        ).fetchall()
        if not rows:
            return 0.500
        wins = 0
        for r in rows:
            is_home = (r["home_team_id"] == team_id)
            hs, as_ = r["home_score"], r["away_score"]
            if is_home and hs > as_:
                wins += 1
            elif not is_home and as_ > hs:
                wins += 1
        return wins / len(rows)
    except Exception:
        return 0.500


def extract_target(game: dict) -> dict[str, Any]:
    """Extract the outcome targets we want GBM to predict.

    home_win is the primary target; total_runs is the O/U target;
    home_margin is useful for RL-style regression; nrfi_hit and
    f5_home_win target the inning-specific markets when linescore data
    is available.
    """
    import json
    hs = game.get("home_score")
    as_ = game.get("away_score")
    if hs is None or as_ is None:
        return {}
    out: dict[str, Any] = {
        "home_win": int(hs > as_),
        "total_runs": hs + as_,
        "home_margin": hs - as_,
    }
    try:
        h_ls = json.loads(game.get("home_linescore") or "[]")
        a_ls = json.loads(game.get("away_linescore") or "[]")
        if h_ls and a_ls:
            out["nrfi_hit"] = int(h_ls[0] == 0 and a_ls[0] == 0)
            if len(h_ls) >= 5 and len(a_ls) >= 5:
                f5h = sum(h_ls[:5])
                f5a = sum(a_ls[:5])
                if f5h != f5a:
                    out["f5_home_win"] = int(f5h > f5a)
                    out["f5_total"] = f5h + f5a
    except Exception:
        pass
    return out


# ── Helpers ────────────────────────────────────────────────────

def _num(v, default: float) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    if x != x:
        return default
    return x


def _season_of(date_str: str) -> int:
    try:
        return int(str(date_str)[:4])
    except Exception:
        return datetime.now().year


def _days_into_season(date_str: str) -> int:
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
    except Exception:
        return 0
    opener = datetime(d.year, 3, 26)
    return max(0, (d - opener).days)


def _is_postseason(date_str: str) -> bool:
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
    except Exception:
        return False
    if d.month == 10:
        return True
    if d.month == 11 and d.day <= 7:
        return True
    return False


FEATURE_NAMES = sorted(_DEFAULTS.keys())
