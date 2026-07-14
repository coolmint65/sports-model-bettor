"""
NBA feature extraction for the GBM training + inference pipeline.

Point-in-time rolling aggregates pulled from `nba_games`. For each game
we walk prior same-season games for each team and compute last-10 +
season-to-date averages. No season-end snapshot leak.

Targets span both full-game and Q1-specific markets because the NBA
book's best-value props live in the early-quarter space:
  - home_win         (classification)   full-game ML
  - q1_home_win      (classification)   Q1 ML
  - q1_total_points  (regression)       Q1 O/U
  - q1_margin        (regression)       Q1 spread
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


_NBA_DEFAULTS = {
    # Last-10 full-game
    "home_l10_win_pct": 0.500,
    "away_l10_win_pct": 0.500,
    "home_l10_ppg": 112.0,
    "away_l10_ppg": 112.0,
    "home_l10_opp_ppg": 112.0,
    "away_l10_opp_ppg": 112.0,
    # Last-10 Q1
    "home_l10_q1_ppg": 28.0,
    "away_l10_q1_ppg": 28.0,
    "home_l10_q1_opp_ppg": 28.0,
    "away_l10_q1_opp_ppg": 28.0,
    "home_l10_q1_margin": 0.0,
    "away_l10_q1_margin": 0.0,
    "home_l10_q1_over_pct": 0.50,   # Q1 total > 56 (rough league avg)
    "away_l10_q1_over_pct": 0.50,
    # NB: l10_pace dropped in #116 — zero-gain across all 6 NBA targets
    # in the 2026-04-30 audit. nba_games.home_pace/away_pace are sparse
    # and the rolling mean rarely diverges enough to drive a tree split.
    # Season-to-date full-game
    "home_season_win_pct": 0.500,
    "away_season_win_pct": 0.500,
    "home_season_ppg": 112.0,
    "away_season_ppg": 112.0,
    "home_season_opp_ppg": 112.0,
    "away_season_opp_ppg": 112.0,
    "home_season_q1_ppg": 28.0,
    "away_season_q1_ppg": 28.0,
    "home_games_played": 0,
    "away_games_played": 0,
    # Schedule factors
    "home_rest_days": 1,
    "away_rest_days": 1,
    # ── #159: lift the factor model's multiplicative adjustments into
    # the GBM feature set so the tree learns the magnitudes from data
    # rather than inheriting our hand-tuned penalties.
    "home_b2b": 0,                  # 1 = home played yesterday
    "away_b2b": 0,                  # 1 = away played yesterday
    "home_3in4": 0,                 # 1 = home played 3 games in last 4 days
    "away_3in4": 0,                 # 1 = away played 3 games in last 4 days
    "home_4in6": 0,                 # 1 = home played 4 games in last 6 days
    "away_4in6": 0,                 # 1 = away played 4 games in last 6 days
    # Derived
    "q1_ppg_delta": 0.0,
    "q1_opp_ppg_delta": 0.0,
    "ppg_delta": 0.0,
    "win_pct_delta": 0.0,
    "q1_margin_delta": 0.0,
    "rest_delta": 0.0,
    "b2b_delta": 0,                 # away_b2b - home_b2b (positive = home advantage)
    # V3.1 — market-as-feature. Closing-line implied prob + line
    # movement from the scoresandodds backfill. Defaults to 0 with
    # has_market_data=0 for games predating coverage (pre-2024-07-15).
    "has_market_data":     0.0,
    "market_home_implied": 0.0,
    "market_total_line":   0.0,
    "market_spread_line":  0.0,
    "market_spread_move":  0.0,
    "market_total_move":   0.0,
}


def extract_nba_features(conn, game: dict) -> dict[str, float] | None:
    """Build the feature dict for one NBA game, rolling-window PIT."""
    home_id = game.get("home_team_id")
    away_id = game.get("away_team_id")
    game_date = game.get("date")
    season = game.get("season") or _season_from_date(game_date)

    if not home_id or not away_id or not game_date or not season:
        return None

    features = dict(_NBA_DEFAULTS)

    home_stats = _rolling_team_stats(conn, home_id, game_date, season)
    away_stats = _rolling_team_stats(conn, away_id, game_date, season)

    for k, v in home_stats.items():
        features[f"home_{k}"] = v
    for k, v in away_stats.items():
        features[f"away_{k}"] = v

    features["home_rest_days"] = _rest_days(conn, home_id, game_date)
    features["away_rest_days"] = _rest_days(conn, away_id, game_date)

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

    features["q1_ppg_delta"] = (
        features["home_l10_q1_ppg"] - features["away_l10_q1_ppg"]
    )
    features["q1_opp_ppg_delta"] = (
        features["home_l10_q1_opp_ppg"] - features["away_l10_q1_opp_ppg"]
    )
    features["ppg_delta"] = features["home_l10_ppg"] - features["away_l10_ppg"]
    features["win_pct_delta"] = (
        features["home_season_win_pct"] - features["away_season_win_pct"]
    )
    features["q1_margin_delta"] = (
        features["home_l10_q1_margin"] - features["away_l10_q1_margin"]
    )

    # V3.1 — market-as-feature. Closing-line implied probability + line
    # movement from the scoresandodds historical backfill. The model
    # learns to be a delta on top of the market rather than a parallel
    # signal — biggest single accuracy win available in the literature.
    # Falls back to 0 / has_market_data=0 for games predating the
    # backfill window (2024-07-15 onward only).
    try:
        from .market_features_nba import extract_market_features
        market = extract_market_features(home_id, away_id, game_date)
        features.update(market)
    except Exception as e:
        logger.debug("market features skipped for game=%s: %s",
                      game.get("game_id"), e)

    return features


def _rolling_team_stats(conn, team_id: int, cutoff_date: str,
                        season: int, last_n: int = 10) -> dict:
    l10 = conn.execute("""
        SELECT home_team_id, away_team_id, home_score, away_score,
               home_q1, away_q1
        FROM nba_games
        WHERE (home_team_id = ? OR away_team_id = ?)
          AND date < ? AND season = ? AND status = 'final'
          AND home_score IS NOT NULL AND away_score IS NOT NULL
        ORDER BY date DESC
        LIMIT ?
    """, (team_id, team_id, cutoff_date, season, last_n)).fetchall()

    season_rows = conn.execute("""
        SELECT home_team_id, home_score, away_score, home_q1, away_q1
        FROM nba_games
        WHERE (home_team_id = ? OR away_team_id = ?)
          AND date < ? AND season = ? AND status = 'final'
          AND home_score IS NOT NULL AND away_score IS NOT NULL
    """, (team_id, team_id, cutoff_date, season)).fetchall()

    stats: dict[str, Any] = {}

    if l10:
        wins = 0
        pts = opp_pts = 0.0
        q1_f = q1_a = 0.0
        q1_n = 0
        q1_over_n = 0
        for r in l10:
            is_home = r["home_team_id"] == team_id
            tpts = r["home_score"] if is_home else r["away_score"]
            opts = r["away_score"] if is_home else r["home_score"]
            pts += tpts
            opp_pts += opts
            if tpts > opts:
                wins += 1
            if r["home_q1"] is not None and r["away_q1"] is not None:
                tq1 = r["home_q1"] if is_home else r["away_q1"]
                oq1 = r["away_q1"] if is_home else r["home_q1"]
                q1_f += tq1
                q1_a += oq1
                q1_n += 1
                if (r["home_q1"] + r["away_q1"]) > 56:
                    q1_over_n += 1

        n = len(l10)
        stats["l10_win_pct"] = wins / n
        stats["l10_ppg"] = pts / n
        stats["l10_opp_ppg"] = opp_pts / n
        if q1_n:
            stats["l10_q1_ppg"] = q1_f / q1_n
            stats["l10_q1_opp_ppg"] = q1_a / q1_n
            stats["l10_q1_margin"] = (q1_f - q1_a) / q1_n
            stats["l10_q1_over_pct"] = q1_over_n / q1_n

    if season_rows:
        wins = 0
        pts = opp_pts = 0.0
        q1_f = q1_n = 0
        for r in season_rows:
            is_home = r["home_team_id"] == team_id
            tpts = r["home_score"] if is_home else r["away_score"]
            opts = r["away_score"] if is_home else r["home_score"]
            pts += tpts
            opp_pts += opts
            if tpts > opts:
                wins += 1
            if r["home_q1"] is not None and r["away_q1"] is not None:
                q1_f += r["home_q1"] if is_home else r["away_q1"]
                q1_n += 1
        n = len(season_rows)
        stats["season_win_pct"] = wins / n
        stats["season_ppg"] = pts / n
        stats["season_opp_ppg"] = opp_pts / n
        if q1_n:
            stats["season_q1_ppg"] = q1_f / q1_n
        stats["games_played"] = n

    return stats


def _rest_days(conn, team_id: int, cutoff_date: str) -> int:
    row = conn.execute("""
        SELECT MAX(date) AS last_date FROM nba_games
        WHERE (home_team_id = ? OR away_team_id = ?)
          AND date < ? AND status = 'final'
    """, (team_id, team_id, cutoff_date)).fetchone()
    if not row or not row["last_date"]:
        return 3
    from datetime import date as _date
    try:
        d1 = _date.fromisoformat(cutoff_date[:10])
        d2 = _date.fromisoformat(row["last_date"][:10])
        return max(1, (d1 - d2).days)
    except ValueError:
        return 3


def _schedule_density(conn, team_id: int, cutoff_date: str) -> dict:
    """Count prior finalized games inside short rolling windows.

    Mirrors the multiplicative schedule-density adjustments the factor
    model applies (B2B penalty, 4-in-6 fatigue). The GBM gets the raw
    flags so the tree can learn the correct magnitude per market.
    """
    out = {"b2b": 0, "three_in_four": 0, "four_in_six": 0}
    from datetime import date as _date, timedelta as _td
    try:
        cutoff = _date.fromisoformat(cutoff_date[:10])
    except ValueError:
        return out

    rows = conn.execute("""
        SELECT date FROM nba_games
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

    # B2B: prior game was within 1 day of cutoff.
    if (cutoff - dates[0]).days <= 1:
        out["b2b"] = 1
    # 3-in-4: at least 2 prior games in the trailing 4-day window
    # (counting cutoff makes it 3 games in 4 days inclusive).
    if sum(1 for d in dates if (cutoff - d).days <= 3) >= 2:
        out["three_in_four"] = 1
    # 4-in-6: at least 3 prior games in trailing 6-day window.
    if sum(1 for d in dates if (cutoff - d).days <= 5) >= 3:
        out["four_in_six"] = 1
    return out


def _season_from_date(game_date: str | None) -> int | None:
    """NBA season spans Oct YYYY -> Jun YYYY+1, labeled by ending year."""
    if not game_date or len(game_date) < 7:
        return None
    try:
        year = int(game_date[:4])
        month = int(game_date[5:7])
    except ValueError:
        return None
    return year + 1 if month >= 9 else year


def extract_nba_target(game: dict) -> dict[str, Any]:
    """Outcome targets: full-game win/total/margin + Q1 splits."""
    hs = game.get("home_score")
    as_ = game.get("away_score")
    if hs is None or as_ is None:
        return {}
    out: dict[str, Any] = {
        "home_win":     int(hs > as_),
        "total_points": int(hs + as_),
        "margin":       int(hs - as_),
    }
    hq1 = game.get("home_q1")
    aq1 = game.get("away_q1")
    if hq1 is not None and aq1 is not None:
        out["q1_home_win"] = int(hq1 > aq1)
        out["q1_total_points"] = int(hq1 + aq1)
        out["q1_margin"] = int(hq1 - aq1)
    return out


NBA_FEATURE_NAMES = sorted(_NBA_DEFAULTS.keys())
