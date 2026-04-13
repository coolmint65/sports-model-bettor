"""
MLB per-matchup factor ablation dump.

For a SPECIFIC game, runs predict_matchup with all factors ON, then
ablates each individual factor and prints the predicted-runs delta.
Any factor whose removal dramatically shifts the prediction is either
legitimately load-bearing (rare) or contains a sign/magnitude bug.

Usage:
    python -m engine.mlb_game_diagnose HOU SEA
    python -m engine.mlb_game_diagnose HOU SEA --home-pitcher 684152 --away-pitcher 694819

(Different from engine.mlb_diagnose, which does settled-pick analysis
across the whole tracker history rather than per-matchup ablation.)
"""

import argparse
import logging

from . import config as cfg


def _find_team_id(abbr: str) -> int | None:
    from .db import get_conn
    conn = get_conn()
    for sql in (
        "SELECT mlb_id FROM teams WHERE abbreviation = ? LIMIT 1",
        "SELECT id FROM teams WHERE abbreviation = ? LIMIT 1",
    ):
        try:
            row = conn.execute(sql, (abbr.upper(),)).fetchone()
            if row:
                return row[0]
        except Exception:
            continue
    return None


def _today_starters(home_team_id: int, away_team_id: int) -> tuple[int | None, int | None]:
    from .db import get_conn
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT home_starter_id, away_starter_id
            FROM games
            WHERE home_team_id = ? AND away_team_id = ? AND date >= ?
            ORDER BY date ASC LIMIT 1
        """, (home_team_id, away_team_id, today)).fetchone()
        if row:
            return row["home_starter_id"], row["away_starter_id"]
    except Exception:
        pass
    return None, None


def _pretty_pred(p: dict, home_abbr: str, away_abbr: str) -> str:
    if not p or "error" in p:
        return f"ERROR: {p.get('error') if p else 'no prediction'}"
    home_runs = p.get("home_expected_runs") or 0
    away_runs = p.get("away_expected_runs") or 0
    wp = p.get("win_prob") or {}
    return (f"{home_abbr} {home_runs:.2f} - {away_abbr} {away_runs:.2f} "
            f"(home wp {wp.get('home', 0):.1%})")


def run(home_abbr: str, away_abbr: str,
        home_pitcher: int | None, away_pitcher: int | None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    from .mlb_predict import predict_matchup

    home_id = _find_team_id(home_abbr)
    away_id = _find_team_id(away_abbr)
    if not home_id or not away_id:
        print(f"Team not found: {home_abbr if not home_id else away_abbr}")
        return

    if home_pitcher is None or away_pitcher is None:
        hp, ap = _today_starters(home_id, away_id)
        home_pitcher = home_pitcher or hp
        away_pitcher = away_pitcher or ap

    print(f"\n{'=' * 72}")
    print(f"  MLB Factor Diagnostic: {away_abbr} @ {home_abbr}")
    print(f"{'=' * 72}")
    print(f"  Home team_id: {home_id}  Away team_id: {away_id}")
    print(f"  Home pitcher: {home_pitcher}  Away pitcher: {away_pitcher}")

    baseline = predict_matchup(
        home_team_id=home_id, away_team_id=away_id,
        home_pitcher_id=home_pitcher, away_pitcher_id=away_pitcher,
    )
    print(f"\n  BASELINE (all factors ON):")
    print(f"    {_pretty_pred(baseline, home_abbr, away_abbr)}")
    if not baseline or "error" in baseline:
        return

    base_home = baseline.get("home_expected_runs", 0) or 0
    base_away = baseline.get("away_expected_runs", 0) or 0
    base_home_wp = (baseline.get("win_prob") or {}).get("home", 0)

    factors = [
        "MLB_ENABLE_BULLPEN_FATIGUE",
        "MLB_ENABLE_SITUATIONAL_AGG",
        "MLB_ENABLE_UMPIRE_FACTOR",
        "MLB_ENABLE_WEATHER_ADJ",
        "MLB_ENABLE_TRAVEL_FATIGUE",
        "MLB_ENABLE_MATCHUP_INTERACTION",
        "MLB_ENABLE_COORS_BOOST",
        "MLB_ENABLE_PLATOON_DUP",
        "MLB_ENABLE_H2H_VS_PITCHER",
        "MLB_ENABLE_LINEUP_STRENGTH",
        "MLB_ENABLE_SITUATIONAL_FACTORS",
    ]

    print(f"\n  ABLATION (factor OFF -> delta vs baseline):")
    print(f"    {'Factor':<36} {'Home Δ':>8} {'Away Δ':>8} {'Home WP Δ':>11}")
    print(f"    {'-' * 68}")
    for factor in factors:
        if not hasattr(cfg, factor):
            continue
        original = getattr(cfg, factor)
        setattr(cfg, factor, False)
        try:
            p = predict_matchup(
                home_team_id=home_id, away_team_id=away_id,
                home_pitcher_id=home_pitcher, away_pitcher_id=away_pitcher,
            )
        except Exception as e:
            p = None
            print(f"    {factor:<36}  CRASHED: {type(e).__name__}: {e}")
        finally:
            setattr(cfg, factor, original)

        if p and "error" not in p:
            h = p.get("home_expected_runs", 0) or 0
            a = p.get("away_expected_runs", 0) or 0
            hw = (p.get("win_prob") or {}).get("home", 0)
            short = factor.replace("MLB_ENABLE_", "")
            marker = ""
            if abs(h - base_home) > 0.3 or abs(a - base_away) > 0.3:
                marker = "  <-- LOAD-BEARING"
            print(f"    {short:<36} {h - base_home:>+7.2f}  {a - base_away:>+7.2f}  "
                  f"{(hw - base_home_wp) * 100:>+9.1f}pp{marker}")

    print()
    print("  Interpretation:")
    print("    If removing a factor IMPROVES the prediction (moves it")
    print("    toward what intuition suggests), that factor is likely")
    print("    inverted or miscalibrated. Use engine.factor_backtest to")
    print("    confirm on multiple historical games before flipping it off.")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("home", help="Home team abbreviation (e.g. SEA)")
    parser.add_argument("away", help="Away team abbreviation (e.g. HOU)")
    parser.add_argument("--home-pitcher", type=int, default=None)
    parser.add_argument("--away-pitcher", type=int, default=None)
    args = parser.parse_args()
    run(args.home, args.away, args.home_pitcher, args.away_pitcher)
