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
    # L10 win pct, PP%, PK%, faceoff% all came back zero-gain across
    # every NHL target in the 2026-04-30 feature audit (#108). Removed
    # in #116 to drop training noise.
    "home_l10_goals_for_pg": 3.10,
    "away_l10_goals_for_pg": 3.10,
    "home_l10_goals_against_pg": 3.10,
    "away_l10_goals_against_pg": 3.10,
    "home_l10_shots_for_pg": 30.0,
    "away_l10_shots_for_pg": 30.0,
    "home_l10_shots_against_pg": 30.0,
    "away_l10_shots_against_pg": 30.0,
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
    # ── #159: factor-model multiplicative adjustments lifted into the
    # GBM feature set. Tree learns the magnitudes from data instead of
    # inheriting the hand-tuned penalties the factor model applies.
    "home_b2b": 0,                  # 1 = home played yesterday
    "away_b2b": 0,                  # 1 = away played yesterday
    "home_3in4": 0,
    "away_3in4": 0,
    "home_4in6": 0,
    "away_4in6": 0,
    # Derived / interaction features.
    "goals_for_delta": 0.0,
    "goals_against_delta": 0.0,
    "win_pct_delta": 0.0,
    "shots_delta": 0.0,
    "p1_gf_delta": 0.0,
    "rest_delta": 0.0,
    "b2b_delta": 0,                 # away_b2b - home_b2b
    # V3.1 — market-as-feature. Closing-line implied prob + line
    # movement from the scoresandodds NHL backfill. Defaults to 0 with
    # has_market_data=0 for games predating coverage (pre-2024-07-15).
    "has_market_data":     0.0,
    "market_home_implied": 0.0,
    "market_total_line":   0.0,
    "market_spread_line":  0.0,
    "market_spread_move":  0.0,
    "market_total_move":   0.0,
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

    home_density = _schedule_density(conn, home_id, game_date)
    away_density = _schedule_density(conn, away_id, game_date)
    features["home_b2b"] = home_density["b2b"]
    features["away_b2b"] = away_density["b2b"]
    features["home_3in4"] = home_density["three_in_four"]
    features["away_3in4"] = away_density["three_in_four"]
    features["home_4in6"] = home_density["four_in_six"]
    features["away_4in6"] = away_density["four_in_six"]
    features["rest_delta"] = features["home_rest_days"] - features["away_rest_days"]
    features["b2b_delta"] = features["away_b2b"] - features["home_b2b"]

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
    features["p1_gf_delta"] = (
        features["home_l10_p1_gf_pg"] - features["away_l10_p1_gf_pg"]
    )

    # V3.1 — market-as-feature. Same shape as NBA: closing-line
    # implied prob + line movement; falls through to has_market_data=0
    # for games predating the scoresandodds NHL backfill window.
    try:
        from .market_features_nhl import extract_market_features
        market = extract_market_features(home_id, away_id, game_date)
        features.update(market)
    except Exception as e:
        logger.debug("nhl market features skipped for %s: %s",
                      game.get("game_id"), e)

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


def _schedule_density(conn, team_id: int, cutoff_date: str) -> dict:
    """Mirror of features_nba._schedule_density on nhl_games. NHL
    densities are typically lighter than NBA but B2B is still a known
    fatigue signal — especially for the back end of a road back-to-back."""
    out = {"b2b": 0, "three_in_four": 0, "four_in_six": 0}
    from datetime import date as _date
    try:
        cutoff = _date.fromisoformat(cutoff_date[:10])
    except ValueError:
        return out
    rows = conn.execute("""
        SELECT date FROM nhl_games
        WHERE (home_team_id = ? OR away_team_id = ?)
          AND date < ? AND status = 'final'
        ORDER BY date DESC
        LIMIT 6
    """, (team_id, team_id, cutoff_date)).fetchall()
    if not rows:
        return out
    dates = []
    for r in rows:
        try:
            dates.append(_date.fromisoformat(r["date"][:10]))
        except (ValueError, TypeError, KeyError):
            continue
    if not dates:
        return out
    if (cutoff - dates[0]).days <= 1:
        out["b2b"] = 1
    if sum(1 for d in dates if (cutoff - d).days <= 3) >= 2:
        out["three_in_four"] = 1
    if sum(1 for d in dates if (cutoff - d).days <= 5) >= 3:
        out["four_in_six"] = 1
    return out


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
        # Puck line — home covers -1.5 = wins by 2 or more goals.
        # Settles at the final score, OT/SO included (consistent with
        # how books grade NHL puck lines).
        "home_pl_cover": int(hs - as_ >= 2),
    }
    h_p1 = game.get("home_p1")
    a_p1 = game.get("away_p1")
    if h_p1 is not None and a_p1 is not None:
        out["p1_home_win"] = int(h_p1 > a_p1)
        out["p1_total_goals"] = int(h_p1 + a_p1)
    return out


NHL_FEATURE_NAMES = sorted(_NHL_DEFAULTS.keys())
