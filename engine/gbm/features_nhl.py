"""
NHL feature extraction for the GBM training + inference pipeline.

Rolling point-in-time aggregation: for each game we compute team stats
from `nhl_games` rows where date < target_date AND season = target_season.
No season-end leak; the trade-off is 2 extra queries per feature-extract
call (one per team) vs a precomputed snapshot table. For a typical
season's ~1200 games that's roughly 40ms/game overhead -- tolerable.

Targets mirror the factor model's horizons:
  - home_win          (classification)   full-game
  - total_goals       (regression)       full-game O/U
  - p1_home_win       (classification)   period-1 market
  - p1_total_goals    (regression)       period-1 totals
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# League-average fallbacks. These are used when a team has no prior
# games in the season (e.g. opening week) so the model sees a neutral
# prior instead of a NaN or 0.
_NHL_DEFAULTS = {
    "home_l10_win_pct": 0.500,
    "away_l10_win_pct": 0.500,
    "home_l10_goals_for_pg": 3.10,
    "away_l10_goals_for_pg": 3.10,
    "home_l10_goals_against_pg": 3.10,
    "away_l10_goals_against_pg": 3.10,
    "home_l10_shots_for_pg": 30.0,
    "away_l10_shots_for_pg": 30.0,
    "home_l10_shots_against_pg": 30.0,
    "away_l10_shots_against_pg": 30.0,
    "home_l10_pp_pct": 0.20,
    "away_l10_pp_pct": 0.20,
    "home_l10_pk_pct": 0.80,
    "away_l10_pk_pct": 0.80,
    "home_l10_faceoff_pct": 0.50,
    "away_l10_faceoff_pct": 0.50,
    "home_l10_p1_gf_pg": 1.00,
    "away_l10_p1_gf_pg": 1.00,
    "home_l10_p1_ga_pg": 1.00,
    "away_l10_p1_ga_pg": 1.00,
    "home_season_win_pct": 0.500,
    "away_season_win_pct": 0.500,
    "home_season_goals_for_pg": 3.10,
    "away_season_goals_for_pg": 3.10,
    "home_season_goals_against_pg": 3.10,
    "away_season_goals_against_pg": 3.10,
    "home_games_played": 0,
    "away_games_played": 0,
    "home_rest_days": 1,
    "away_rest_days": 1,
    "is_playoff": 0,
    # Derived / interaction features.
    "goals_for_delta": 0.0,
    "goals_against_delta": 0.0,
    "win_pct_delta": 0.0,
    "shots_delta": 0.0,
    "pp_minus_pk_home": 0.0,
    "pp_minus_pk_away": 0.0,
    "p1_gf_delta": 0.0,
}


def extract_nhl_features(conn, game: dict) -> dict[str, float] | None:
    """Build the feature dict for one NHL game.

    Point-in-time: all team-stat aggregates are computed from games
    with date < game_date in the same season. Games with no prior
    history fall back to league-average defaults rather than returning
    None so early-season games still contribute to training.
    """
    home_id = game.get("home_team_id")
    away_id = game.get("away_team_id")
    game_date = game.get("date")
    season = game.get("season") or _season_from_date(game_date)

    if not home_id or not away_id or not game_date or not season:
        return None

    features = dict(_NHL_DEFAULTS)

    home_stats = _rolling_team_stats(conn, home_id, game_date, season)
    away_stats = _rolling_team_stats(conn, away_id, game_date, season)

    for k, v in home_stats.items():
        features[f"home_{k}"] = v
    for k, v in away_stats.items():
        features[f"away_{k}"] = v

    features["home_rest_days"] = _rest_days(conn, home_id, game_date)
    features["away_rest_days"] = _rest_days(conn, away_id, game_date)
    features["is_playoff"] = 1 if (game.get("game_type") or 2) == 3 else 0

    features["goals_for_delta"] = (
        features["home_l10_goals_for_pg"] - features["away_l10_goals_for_pg"]
    )
    features["goals_against_delta"] = (
        features["home_l10_goals_against_pg"] - features["away_l10_goals_against_pg"]
    )
    features["win_pct_delta"] = (
        features["home_season_win_pct"] - features["away_season_win_pct"]
    )
    features["shots_delta"] = (
        features["home_l10_shots_for_pg"] - features["away_l10_shots_for_pg"]
    )
    features["pp_minus_pk_home"] = (
        features["home_l10_pp_pct"] - (1.0 - features["away_l10_pk_pct"])
    )
    features["pp_minus_pk_away"] = (
        features["away_l10_pp_pct"] - (1.0 - features["home_l10_pk_pct"])
    )
    features["p1_gf_delta"] = (
        features["home_l10_p1_gf_pg"] - features["away_l10_p1_gf_pg"]
    )

    return features


def _rolling_team_stats(conn, team_id: int, cutoff_date: str,
                        season: int, last_n: int = 10) -> dict:
    """Compute last-N and season-to-date aggregates for one team.

    Does TWO queries: one bounded to the most recent `last_n` games
    (for form-sensitive features like l10 goals/game), one over all
    season-to-date games (for stability features like season win%).
    """
    l10 = conn.execute("""
        SELECT home_team_id, away_team_id, home_score, away_score,
               home_shots, away_shots,
               home_pp_goals, home_pp_opps, away_pp_goals, away_pp_opps,
               home_faceoff_pct, away_faceoff_pct,
               home_p1, away_p1
        FROM nhl_games
        WHERE (home_team_id = ? OR away_team_id = ?)
          AND date < ? AND season = ? AND status = 'final'
          AND home_score IS NOT NULL AND away_score IS NOT NULL
        ORDER BY date DESC
        LIMIT ?
    """, (team_id, team_id, cutoff_date, season, last_n)).fetchall()

    # Season-to-date aggregates: no LIMIT so we can compute stable win%.
    season_rows = conn.execute("""
        SELECT home_team_id, home_score, away_score
        FROM nhl_games
        WHERE (home_team_id = ? OR away_team_id = ?)
          AND date < ? AND season = ? AND status = 'final'
          AND home_score IS NOT NULL AND away_score IS NOT NULL
    """, (team_id, team_id, cutoff_date, season)).fetchall()

    stats: dict[str, Any] = {}

    if l10:
        gf = ga = sf = sa = 0.0
        pp_g = pp_o = pk_g = pk_o = 0  # pp = team's power play; pk = opp PP stopped
        fo_sum = fo_n = 0
        p1_gf_sum = p1_ga_sum = p1_n = 0
        for r in l10:
            is_home = r["home_team_id"] == team_id
            tgf = r["home_score"] if is_home else r["away_score"]
            tga = r["away_score"] if is_home else r["home_score"]
            gf += tgf
            ga += tga
            if r["home_shots"] is not None and r["away_shots"] is not None:
                sf += r["home_shots"] if is_home else r["away_shots"]
                sa += r["away_shots"] if is_home else r["home_shots"]
            if is_home and r["home_pp_opps"]:
                pp_g += r["home_pp_goals"] or 0
                pp_o += r["home_pp_opps"]
            if not is_home and r["away_pp_opps"]:
                pp_g += r["away_pp_goals"] or 0
                pp_o += r["away_pp_opps"]
            # PK: opponent PP opportunities we successfully killed.
            opp_pp_o = (r["away_pp_opps"] if is_home else r["home_pp_opps"]) or 0
            opp_pp_g = (r["away_pp_goals"] if is_home else r["home_pp_goals"]) or 0
            pk_g += opp_pp_g
            pk_o += opp_pp_o
            fo = r["home_faceoff_pct"] if is_home else r["away_faceoff_pct"]
            if fo is not None:
                fo_sum += fo
                fo_n += 1
            if r["home_p1"] is not None and r["away_p1"] is not None:
                p1_gf_sum += r["home_p1"] if is_home else r["away_p1"]
                p1_ga_sum += r["away_p1"] if is_home else r["home_p1"]
                p1_n += 1

        n = len(l10)
        stats["l10_goals_for_pg"] = gf / n
        stats["l10_goals_against_pg"] = ga / n
        # Shots may be missing for older ingested games; guard denominator.
        shots_n = sum(1 for r in l10 if r["home_shots"] is not None)
        stats["l10_shots_for_pg"] = (sf / shots_n) if shots_n else _NHL_DEFAULTS["home_l10_shots_for_pg"]
        stats["l10_shots_against_pg"] = (sa / shots_n) if shots_n else _NHL_DEFAULTS["home_l10_shots_against_pg"]
        stats["l10_pp_pct"] = (pp_g / pp_o) if pp_o else _NHL_DEFAULTS["home_l10_pp_pct"]
        stats["l10_pk_pct"] = (1.0 - pk_g / pk_o) if pk_o else _NHL_DEFAULTS["home_l10_pk_pct"]
        stats["l10_faceoff_pct"] = (fo_sum / fo_n / 100.0 if fo_sum > 5 else fo_sum / fo_n) if fo_n else _NHL_DEFAULTS["home_l10_faceoff_pct"]
        if p1_n:
            stats["l10_p1_gf_pg"] = p1_gf_sum / p1_n
            stats["l10_p1_ga_pg"] = p1_ga_sum / p1_n

    if season_rows:
        wins = 0
        sgf = sga = 0
        for r in season_rows:
            is_home = r["home_team_id"] == team_id
            tgf = r["home_score"] if is_home else r["away_score"]
            tga = r["away_score"] if is_home else r["home_score"]
            sgf += tgf
            sga += tga
            if tgf > tga:
                wins += 1
        n = len(season_rows)
        stats["season_win_pct"] = wins / n
        stats["season_goals_for_pg"] = sgf / n
        stats["season_goals_against_pg"] = sga / n
        stats["games_played"] = n

    return stats


def _rest_days(conn, team_id: int, cutoff_date: str) -> int:
    """Days since the team's last final game. Returns 7 (no-signal
    sentinel) when there's no prior game -- typical of season openers."""
    row = conn.execute("""
        SELECT MAX(date) AS last_date FROM nhl_games
        WHERE (home_team_id = ? OR away_team_id = ?)
          AND date < ? AND status = 'final'
    """, (team_id, team_id, cutoff_date)).fetchone()
    if not row or not row["last_date"]:
        return 7
    from datetime import date as _date
    try:
        d1 = _date.fromisoformat(cutoff_date[:10])
        d2 = _date.fromisoformat(row["last_date"][:10])
        return max(1, (d1 - d2).days)
    except ValueError:
        return 7


def _season_from_date(game_date: str | None) -> int | None:
    """Extract season from YYYY-MM-DD. NHL seasons straddle two years,
    labeled by the ending year (2024-25 season = season 2025)."""
    if not game_date or len(game_date) < 7:
        return None
    try:
        year = int(game_date[:4])
        month = int(game_date[5:7])
    except ValueError:
        return None
    # Oct-Dec belongs to the season that ends in year+1.
    return year + 1 if month >= 8 else year


def extract_nhl_target(game: dict) -> dict[str, Any]:
    """Outcome targets we train on: full-game win + total + P1 split."""
    hs = game.get("home_score")
    as_ = game.get("away_score")
    if hs is None or as_ is None:
        return {}
    out: dict[str, Any] = {
        "home_win": int(hs > as_),
        "total_goals": int(hs + as_),
    }
    h_p1 = game.get("home_p1")
    a_p1 = game.get("away_p1")
    if h_p1 is not None and a_p1 is not None:
        out["p1_home_win"] = int(h_p1 > a_p1)
        out["p1_total_goals"] = int(h_p1 + a_p1)
    return out


NHL_FEATURE_NAMES = sorted(_NHL_DEFAULTS.keys())
