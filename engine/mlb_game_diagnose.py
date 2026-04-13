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


def _extract_runs(p: dict) -> tuple[float, float]:
    """Pull (home_runs, away_runs) from the prediction dict."""
    if not p:
        return 0.0, 0.0
    es = p.get("expected_score") or {}
    home = es.get("home") or p.get("home_expected_runs") or 0
    away = es.get("away") or p.get("away_expected_runs") or 0
    return float(home), float(away)


def _pretty_pred(p: dict, home_abbr: str, away_abbr: str) -> str:
    if not p or "error" in p:
        return f"ERROR: {p.get('error') if p else 'no prediction'}"
    home_runs, away_runs = _extract_runs(p)
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

    base_home, base_away = _extract_runs(baseline)
    base_home_wp = (baseline.get("win_prob") or {}).get("home", 0)

    # Print the top-level factor values so we can spot obvious bugs
    # (symmetric factors, inverted signs, etc.) without running ablation.
    print(f"\n  BASELINE FACTOR VALUES:")
    print(f"    park_factor    : {baseline.get('park_factor')}")
    print(f"    umpire.factor  : {(baseline.get('umpire') or {}).get('factor')}")
    print(f"    weather_adj    : {baseline.get('weather_adj')}")
    print(f"    travel         : {baseline.get('travel')}")
    print(f"    platoon_adj    : {baseline.get('platoon_adj')}")
    sit = baseline.get("situational") or {}
    print(f"    situational.home_multiplier: {sit.get('home_multiplier')}")
    print(f"    situational.away_multiplier: {sit.get('away_multiplier')}")

    # ── Raw source data: team stats + team_cal factors + pitcher/injury ──
    # These are NOT toggleable via config (yet) but they're the source of
    # the prediction. If team_cal has learned weird values OR team stats
    # are inverted, the ablation above shows nothing because all the
    # toggleable factors are neutral.
    print(f"\n  RAW SOURCE DATA (not toggleable - inspect for source-of-truth bugs):")
    try:
        from .db import get_team_record
        from .pit_stats import compute_team_stats_at_date
        from datetime import datetime as _dt
        import importlib

        SEASON = _dt.now().year
        today = _dt.now().strftime("%Y-%m-%d")

        home_rec = get_team_record(home_id, SEASON) or {}
        away_rec = get_team_record(away_id, SEASON) or {}
        print(f"    HOME ({home_abbr}) record: runs_pg={home_rec.get('runs_pg')} "
              f"ops={home_rec.get('ops')} wrc+={home_rec.get('wrc_plus')} "
              f"W-L={home_rec.get('wins')}-{home_rec.get('losses')}")
        print(f"    AWAY ({away_abbr}) record: runs_pg={away_rec.get('runs_pg')} "
              f"ops={away_rec.get('ops')} wrc+={away_rec.get('wrc_plus')} "
              f"W-L={away_rec.get('wins')}-{away_rec.get('losses')}")

        # Point-in-time stats (what predict_matchup actually uses)
        home_pit = compute_team_stats_at_date(home_id, today, SEASON) or {}
        away_pit = compute_team_stats_at_date(away_id, today, SEASON) or {}
        print(f"    HOME ({home_abbr}) PIT  : runs_pg={home_pit.get('runs_pg')} "
              f"ops={home_pit.get('ops')} games={home_pit.get('games_played')}")
        print(f"    AWAY ({away_abbr}) PIT  : runs_pg={away_pit.get('runs_pg')} "
              f"ops={away_pit.get('ops')} games={away_pit.get('games_played')}")

        # team_cal learned factors - these multiply the base offense rating
        try:
            tc = importlib.import_module("engine.team_calibration")
            # Try common function names
            load_fn = (getattr(tc, "load_team_adjustment", None)
                       or getattr(tc, "get_team_adjustment", None)
                       or getattr(tc, "get_adjustment", None))
            if load_fn:
                home_adj = load_fn(home_id) or {}
                away_adj = load_fn(away_id) or {}
                print(f"    HOME team_cal: offense={home_adj.get('offense_factor')} "
                      f"defense={home_adj.get('defense_factor')} "
                      f"home={home_adj.get('home_factor')} "
                      f"games={home_adj.get('games_analyzed')}")
                print(f"    AWAY team_cal: offense={away_adj.get('offense_factor')} "
                      f"defense={away_adj.get('defense_factor')} "
                      f"away={away_adj.get('away_factor')} "
                      f"games={away_adj.get('games_analyzed')}")
            else:
                print(f"    team_cal: no loader fn found (tried load/get/get_adjustment)")
                # Dump module attributes to help find the right name
                print(f"    team_calibration public attrs: "
                      f"{[a for a in dir(tc) if not a.startswith('_')][:15]}")
        except Exception as e:
            print(f"    team_cal load failed: {type(e).__name__}: {e}")
    except Exception as e:
        print(f"    Raw stats dump failed: {type(e).__name__}: {e}")

    # Injury impact (already logged but repeated here for one-shot view)
    inj = baseline.get("injuries") or {}
    print(f"    Injuries applied: home={len(inj.get('home') or [])} "
          f"away={len(inj.get('away') or [])} players")

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
            h, a = _extract_runs(p)
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
