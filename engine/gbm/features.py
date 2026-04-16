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
    # Games-table-derived features (always available for all backfilled
    # seasons because they query the games table, not player stat tables).
    "home_season_win_pct": 0.500,
    "away_season_win_pct": 0.500,
    "home_run_diff_pg": 0.0,
    "away_run_diff_pg": 0.0,
    "home_scoring_std": 2.5,
    "away_scoring_std": 2.5,
    "home_home_win_pct": 0.540,
    "away_away_win_pct": 0.460,
    "sp_era_diff": 0.0,              # home SP ERA minus away SP ERA
    "home_sp_era_last_3": 4.10,
    "away_sp_era_last_3": 4.10,
    "home_rest_days": 1,
    "away_rest_days": 1,
    # Weather (from games.weather_temp / weather_wind, when populated)
    "weather_temp_f": 70.0,
    "weather_wind_mph": 5.0,
    # HP umpire (from umpires table, looked up by games.umpire name)
    "umpire_run_factor": 1.0,
    "umpire_k_pct": 0.22,
    "umpire_over_pct": 0.50,
    # Derived / interaction features. GBM can find these automatically
    # once enough base features have signal, but computing them directly
    # gives the model a head start on the most obvious patterns.
    "sp_era_last3_diff": 0.0,       # home vs away SP ERA last-3
    "run_diff_delta": 0.0,          # home run_diff_pg - away run_diff_pg
    "win_pct_diff": 0.0,            # home season_win_pct - away season_win_pct
    "offense_vs_pitching_home": 0.0,  # home offense quality - away SP quality
    "offense_vs_pitching_away": 0.0,  # away offense quality - home SP quality
    "park_adj_total_offense": 0.0,  # (home_runs_pg + away_runs_pg) * park_factor
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

    # ── Games-table-derived features (always available) ──
    # These query the games table which is fully backfilled for all
    # historical seasons, so they never fall back to defaults.
    h_season = _team_season_stats(conn, home_id, season, date)
    a_season = _team_season_stats(conn, away_id, season, date)
    features["home_season_win_pct"] = h_season.get("win_pct", 0.500)
    features["away_season_win_pct"] = a_season.get("win_pct", 0.500)
    features["home_run_diff_pg"] = h_season.get("run_diff_pg", 0.0)
    features["away_run_diff_pg"] = a_season.get("run_diff_pg", 0.0)
    features["home_scoring_std"] = h_season.get("scoring_std", 2.5)
    features["away_scoring_std"] = a_season.get("scoring_std", 2.5)
    features["home_home_win_pct"] = h_season.get("home_win_pct", 0.540)
    features["away_away_win_pct"] = a_season.get("away_win_pct", 0.460)

    # SP ERA differential (strong signal: positive = away SP is better)
    features["sp_era_diff"] = features["home_sp_era"] - features["away_sp_era"]

    # SP ERA last 3 starts (games-table-derived, PIT-correct)
    if game.get("home_pitcher_id"):
        features["home_sp_era_last_3"] = _sp_era_last_n(
            conn, game["home_pitcher_id"], date, n=3)
    if game.get("away_pitcher_id"):
        features["away_sp_era_last_3"] = _sp_era_last_n(
            conn, game["away_pitcher_id"], date, n=3)

    # Rest days since last game
    features["home_rest_days"] = _rest_days(conn, home_id, date)
    features["away_rest_days"] = _rest_days(conn, away_id, date)

    # ── Weather (games.weather_temp / weather_wind) ──
    # Games the scrapers ingest AFTER 2024 carry populated weather fields
    # most of the time. Earlier games fall back to the 70F / 5mph default
    # which is close enough to league-average to not hurt the model.
    wtemp = game.get("weather_temp")
    wwind = game.get("weather_wind")
    if wtemp is not None:
        try:
            features["weather_temp_f"] = float(wtemp)
        except (TypeError, ValueError):
            pass
    if wwind is not None:
        features["weather_wind_mph"] = _parse_wind(wwind)

    # ── Umpire (from the umpires table, joined by name) ──
    ump_name = game.get("umpire") or ""
    if ump_name:
        ump = _umpire_stats(conn, ump_name)
        if ump.get("run_factor") is not None:
            features["umpire_run_factor"] = float(ump["run_factor"])
        if ump.get("k_pct") is not None:
            features["umpire_k_pct"] = float(ump["k_pct"])
        if ump.get("over_pct") is not None:
            features["umpire_over_pct"] = float(ump["over_pct"])

    # ── Derived / interaction features ──
    # GBM can find these from the base features, but giving them to it
    # directly as single columns makes it easier for shallow trees to
    # find the splits. Cheap to compute, strictly additive.
    features["sp_era_last3_diff"] = round(
        features["home_sp_era_last_3"] - features["away_sp_era_last_3"], 3,
    )
    features["run_diff_delta"] = round(
        features["home_run_diff_pg"] - features["away_run_diff_pg"], 3,
    )
    features["win_pct_diff"] = round(
        features["home_season_win_pct"] - features["away_season_win_pct"], 3,
    )
    # Offense vs. opposing SP: positive = home offense faces weak SP
    features["offense_vs_pitching_home"] = round(
        features["home_runs_pg"] - (LEAGUE_AVG_RPG_PER_SP_ERA * features["away_sp_era"]), 3,
    )
    features["offense_vs_pitching_away"] = round(
        features["away_runs_pg"] - (LEAGUE_AVG_RPG_PER_SP_ERA * features["home_sp_era"]), 3,
    )
    # Combined run environment -- team totals scaled by the park
    features["park_adj_total_offense"] = round(
        (features["home_runs_pg"] + features["away_runs_pg"])
        * features["park_run_factor"], 3,
    )

    return features


# Calibration constant for offense-vs-pitching interaction. ~0.50 means
# every point of SP ERA above 4.10 corresponds to ~0.5 extra runs in
# that game. Empirical league regression.
LEAGUE_AVG_RPG_PER_SP_ERA = 0.50


def _parse_wind(val) -> float:
    """Turn a wind string like '8 mph Out to CF' into a numeric mph."""
    try:
        return float(val)
    except (TypeError, ValueError):
        pass
    s = str(val or "").strip()
    if not s:
        return 5.0
    # Leading numeric token
    import re as _re
    m = _re.match(r"(\d+(?:\.\d+)?)", s)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return 5.0
    return 5.0


def _umpire_stats(conn, umpire_name: str) -> dict:
    """Lookup umpire tendency stats. Names in games.umpire are the
    MLB Stats API format (e.g. 'Angel Hernandez'), which should match
    the umpires table's `name` column. Returns empty dict on miss."""
    try:
        row = conn.execute(
            "SELECT run_factor, k_pct, bb_pct, over_pct "
            "FROM umpires WHERE name = ?",
            (umpire_name,),
        ).fetchone()
        return dict(row) if row else {}
    except Exception:
        return {}


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
    """Team wRC+ -- weighted by PA if available, AB otherwise.

    When the batter_stats.wrc_plus column is populated, use it directly.
    Otherwise approximate from team OPS by scaling against a fixed
    league baseline (OPS 0.720 -> wRC+ 100). Imperfect but gives the
    feature real variance instead of a flat 100.
    """
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
    # Fallback: approximate from team OPS. Rule of thumb: +/-0.010 OPS
    # around league ~= +/-5 wRC+. Empirical league regression over the
    # last 3 seasons.
    team_ops = _team_ops(conn, team_id, season)
    return round(100.0 + (team_ops - 0.720) * 500, 1)


def _bullpen_era(conn, team_id: int, season: int) -> float:
    """Bullpen ERA for the team+season.

    First tries the dedicated bullpen table (which only has current-
    season rows in practice). Falls back to computing from pitcher_stats
    -- aggregating ERA for all pitchers on the team with games_started
    < 5 (our definition of "bullpen arm"). This gives historical seasons
    a real bullpen signal instead of the flat 4.20 default.
    """
    try:
        row = conn.execute(
            "SELECT era FROM bullpen WHERE team_id = ? AND season = ?",
            (team_id, season),
        ).fetchone()
        if row and row["era"]:
            return float(row["era"])
    except Exception:
        pass
    # Fallback: weighted ERA of all relievers on the team for that season
    try:
        row = conn.execute(
            "SELECT SUM(earned_runs) * 9.0 / NULLIF(SUM(innings), 0) AS era "
            "FROM pitcher_stats "
            "WHERE team_id = ? AND season = ? "
            "  AND COALESCE(games_started, 0) < 5 "
            "  AND innings > 0",
            (team_id, season),
        ).fetchone()
        if row and row["era"] and row["era"] > 0:
            return round(float(row["era"]), 2)
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


def _team_season_stats(conn, team_id: int, season: int,
                        as_of_date: str) -> dict:
    """Compute team aggregate stats from the games table, PIT-correct.

    Returns win_pct, run_diff_pg, scoring_std, home_win_pct, away_win_pct.
    Only counts games strictly before as_of_date.
    """
    out = {}
    try:
        rows = conn.execute(
            "SELECT home_team_id, away_team_id, home_score, away_score "
            "FROM games "
            "WHERE (home_team_id = ? OR away_team_id = ?) "
            "  AND season = ? AND status = 'final' AND date < ? "
            "  AND home_score IS NOT NULL AND away_score IS NOT NULL",
            (team_id, team_id, season, as_of_date),
        ).fetchall()
        if not rows:
            return out
        wins = 0
        home_wins = 0
        home_games = 0
        away_wins = 0
        away_games = 0
        runs_scored = []
        runs_allowed = []
        for r in rows:
            is_home = (r["home_team_id"] == team_id)
            hs, as_ = r["home_score"], r["away_score"]
            if is_home:
                home_games += 1
                runs_scored.append(hs)
                runs_allowed.append(as_)
                if hs > as_:
                    wins += 1
                    home_wins += 1
            else:
                away_games += 1
                runs_scored.append(as_)
                runs_allowed.append(hs)
                if as_ > hs:
                    wins += 1
                    away_wins += 1
        n = len(rows)
        out["win_pct"] = round(wins / n, 3)
        rs_mean = sum(runs_scored) / n
        ra_mean = sum(runs_allowed) / n
        out["run_diff_pg"] = round(rs_mean - ra_mean, 3)
        if n > 1:
            from statistics import stdev as _std
            out["scoring_std"] = round(_std(runs_scored), 3)
        else:
            out["scoring_std"] = 2.5
        out["home_win_pct"] = round(home_wins / max(home_games, 1), 3)
        out["away_win_pct"] = round(away_wins / max(away_games, 1), 3)
    except Exception as e:
        logger.debug("_team_season_stats failed: %s", e)
    return out


def _sp_era_last_n(conn, pitcher_id: int, as_of_date: str,
                    n: int = 3) -> float:
    """Compute a pitcher's ERA over their last N starts, from the games table.

    PIT-correct: only uses games before as_of_date.
    Returns 4.10 if insufficient data.
    """
    try:
        rows = conn.execute(
            "SELECT home_team_id, away_team_id, home_score, away_score, "
            "       home_pitcher_id, away_pitcher_id "
            "FROM games "
            "WHERE (home_pitcher_id = ? OR away_pitcher_id = ?) "
            "  AND status = 'final' AND date < ? "
            "  AND home_score IS NOT NULL "
            "ORDER BY date DESC LIMIT ?",
            (pitcher_id, pitcher_id, as_of_date, n),
        ).fetchall()
        if not rows:
            return 4.10
        # Approximate ERA from runs allowed in starts
        # (runs allowed per start / ~6 innings per start * 9)
        total_ra = 0
        for r in rows:
            if r["home_pitcher_id"] == pitcher_id:
                total_ra += r["away_score"]
            else:
                total_ra += r["home_score"]
        avg_ra = total_ra / len(rows)
        # Convert to ERA: assume ~5.5 IP per start
        era_approx = avg_ra * 9 / 5.5
        return round(max(0.0, min(15.0, era_approx)), 2)
    except Exception:
        return 4.10


def _rest_days(conn, team_id: int, as_of_date: str) -> int:
    """Days since the team's last game before as_of_date."""
    try:
        row = conn.execute(
            "SELECT date FROM games "
            "WHERE (home_team_id = ? OR away_team_id = ?) "
            "  AND status = 'final' AND date < ? "
            "ORDER BY date DESC LIMIT 1",
            (team_id, team_id, as_of_date),
        ).fetchone()
        if row and row["date"]:
            last = datetime.strptime(row["date"][:10], "%Y-%m-%d")
            today = datetime.strptime(as_of_date[:10], "%Y-%m-%d")
            return max(0, (today - last).days)
    except Exception:
        pass
    return 1


FEATURE_NAMES = sorted(_DEFAULTS.keys())
