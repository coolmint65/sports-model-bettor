"""Feature extraction for hockey-framework GBM (AHL/PWHL/etc).

Mirrors `engine.basketball._features` shape so the same train/predict
loop pattern works for hockey. Hockey doesn't have pace/ortg/drtg
stats per game (theScore doesn't ship advanced stats for AHL/PWHL),
so features are simpler — derived from scores + schedule context.

Per-game features (from data BEFORE the game's date):
  - home/away last-10 goals_for_pg, goals_against_pg, win_pct, games
  - home/away season-to-date same set
  - home/away rest_days, b2b flag, 3in4 flag
  - delta versions of every above pair

Targets:
  - home_win (classification)
  - margin (regression, home_score - away_score)
  - total_goals (regression, home_score + away_score)

All stats are point-in-time: only games strictly BEFORE the cutoff
date count, so train/predict can't peek at the outcome.
"""
from __future__ import annotations

from datetime import datetime, timedelta


def feature_columns() -> list[str]:
    """Stable column order shared between train + predict."""
    cols = []
    for prefix in ("home", "away"):
        for w in ("l10", "season"):
            for k in ("gpg", "opp_gpg", "win_pct"):
                cols.append(f"{prefix}_{w}_{k}")
            cols.append(f"{prefix}_{w}_games")
        cols.append(f"{prefix}_rest_days")
        cols.append(f"{prefix}_b2b")
        cols.append(f"{prefix}_3in4")
    for k in ("gpg", "opp_gpg", "win_pct"):
        cols.append(f"l10_{k}_delta")
    cols.append("rest_delta")
    cols.append("b2b_delta")
    return cols


def extract_features(league: str, game: dict) -> dict[str, float] | None:
    """Build the feature dict for one game, point-in-time. Returns None
    when required identifiers are missing."""
    home_id = game.get("home_team_id")
    away_id = game.get("away_team_id")
    game_date = game.get("date")
    if not (home_id and away_id and game_date):
        return None
    season = game.get("season") or _season_from_date(game_date)
    conn = _conn(league)
    feats: dict[str, float] = {}
    home_stats = _team_window_stats(conn, home_id, game_date, season, league)
    away_stats = _team_window_stats(conn, away_id, game_date, season, league)
    for k, v in home_stats.items():
        feats[f"home_{k}"] = v
    for k, v in away_stats.items():
        feats[f"away_{k}"] = v
    feats["home_rest_days"] = _rest_days(conn, home_id, game_date)
    feats["away_rest_days"] = _rest_days(conn, away_id, game_date)
    h_density = _schedule_density(conn, home_id, game_date)
    a_density = _schedule_density(conn, away_id, game_date)
    feats["home_b2b"] = h_density["b2b"]
    feats["away_b2b"] = a_density["b2b"]
    feats["home_3in4"] = h_density["three_in_four"]
    feats["away_3in4"] = a_density["three_in_four"]
    for k in ("gpg", "opp_gpg", "win_pct"):
        feats[f"l10_{k}_delta"] = (feats.get(f"home_l10_{k}", 0)
                                    - feats.get(f"away_l10_{k}", 0))
    feats["rest_delta"] = feats["home_rest_days"] - feats["away_rest_days"]
    feats["b2b_delta"] = feats["away_b2b"] - feats["home_b2b"]
    return feats


def extract_targets(game: dict) -> dict[str, float] | None:
    if game.get("status") != "final":
        return None
    hs, as_ = game.get("home_score"), game.get("away_score")
    if hs is None or as_ is None:
        return None
    return {
        "home_win": 1 if hs > as_ else 0,
        "margin": hs - as_,
        "total_goals": hs + as_,
    }


# ── Helpers ──────────────────────────────────────────────────

# Reasonable defaults so a brand-new team / very thin sample still
# produces non-degenerate feature values. Tuned to AHL averages
# (~3.0 GPG / ~6.0 total).
_DEFAULTS = {
    "l10_gpg": 3.0, "l10_opp_gpg": 3.0, "l10_win_pct": 0.5, "l10_games": 0,
    "season_gpg": 3.0, "season_opp_gpg": 3.0, "season_win_pct": 0.5,
    "season_games": 0,
}


def _conn(league: str):
    mod = __import__(f"engine.sports.{league}.db", fromlist=["get_conn"])
    return mod.get_conn()


def _season_from_date(date_iso: str) -> int:
    y, m, _ = date_iso.split("-")
    y = int(y)
    return y if int(m) >= 9 else y - 1


def _team_window_stats(conn, team_id: int, cutoff_date: str,
                        season: int, league: str) -> dict[str, float]:
    """l10 + season-to-date stats from games BEFORE cutoff_date."""
    out: dict[str, float] = dict(_DEFAULTS)
    season_rows = conn.execute(
        "SELECT date, home_team_id, away_team_id, home_score, away_score "
        "FROM games WHERE status = 'final' AND season = ? "
        "  AND date < ? "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "  AND (home_team_id = ? OR away_team_id = ?) "
        "ORDER BY date DESC",
        (season, cutoff_date, team_id, team_id),
    ).fetchall()
    if not season_rows:
        return out
    season_gf, season_ga, season_w = [], [], 0
    for r in season_rows:
        is_home = r["home_team_id"] == team_id
        gf = r["home_score"] if is_home else r["away_score"]
        ga = r["away_score"] if is_home else r["home_score"]
        season_gf.append(gf); season_ga.append(ga)
        if gf > ga:
            season_w += 1
    n = len(season_rows)
    out["season_gpg"] = sum(season_gf) / n
    out["season_opp_gpg"] = sum(season_ga) / n
    out["season_win_pct"] = season_w / n
    out["season_games"] = n
    # Last 10
    last10 = season_rows[:10]
    if last10:
        l10_gf, l10_ga, l10_w = [], [], 0
        for r in last10:
            is_home = r["home_team_id"] == team_id
            gf = r["home_score"] if is_home else r["away_score"]
            ga = r["away_score"] if is_home else r["home_score"]
            l10_gf.append(gf); l10_ga.append(ga)
            if gf > ga:
                l10_w += 1
        n10 = len(last10)
        out["l10_gpg"] = sum(l10_gf) / n10
        out["l10_opp_gpg"] = sum(l10_ga) / n10
        out["l10_win_pct"] = l10_w / n10
        out["l10_games"] = n10
    return out


def _rest_days(conn, team_id: int, cutoff_date: str) -> float:
    row = conn.execute(
        "SELECT MAX(date) FROM games WHERE status = 'final' "
        "  AND date < ? "
        "  AND (home_team_id = ? OR away_team_id = ?)",
        (cutoff_date, team_id, team_id),
    ).fetchone()
    if not row or not row[0]:
        return 7.0  # plenty of rest, default
    last = datetime.fromisoformat(row[0])
    cur = datetime.fromisoformat(cutoff_date)
    return float((cur - last).days)


def _schedule_density(conn, team_id: int, cutoff_date: str) -> dict:
    """B2B (yesterday) + 3-in-4 (3 games in last 4 days)."""
    cur = datetime.fromisoformat(cutoff_date)
    rows = conn.execute(
        "SELECT date FROM games WHERE status = 'final' "
        "  AND date < ? AND date >= ? "
        "  AND (home_team_id = ? OR away_team_id = ?)",
        (cutoff_date, (cur - timedelta(days=4)).strftime("%Y-%m-%d"),
         team_id, team_id),
    ).fetchall()
    dates = [datetime.fromisoformat(r[0]) for r in rows]
    b2b = any((cur - d).days == 1 for d in dates)
    three_in_four = len(dates) >= 2  # 2 prior + this one = 3 in 4
    return {"b2b": 1.0 if b2b else 0.0,
             "three_in_four": 1.0 if three_in_four else 0.0}
