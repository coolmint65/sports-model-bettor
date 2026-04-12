"""
Explain a single NHL prediction in full: every factor, every xG modifier,
and the final lean. Use this to spot a sign-flipped factor.

Usage:
    python -m engine.nhl_explain BOS TOR           # by abbreviation
    python -m engine.nhl_explain bruins maple_leafs # by JSON file stem

If a team has way better stats than its opponent but the model leans the
other way, that points to which factor is inverted.

Output focuses on directional sanity checks:
  - Raw team stats (goals for/against, PP%, PK%, save%)
  - Final xG after all factors
  - Which team the model picks and with what probability
  - The bet-type picks ranked by adjusted EV
"""

import sys
import logging


def _find_key(abbr: str) -> str | None:
    """Map abbreviation to team JSON key."""
    from engine.data import list_teams, load_team
    teams = list_teams("NHL")
    for t in teams:
        try:
            data = load_team("NHL", t["key"])
            if data and data.get("abbreviation", "").upper() == abbr.upper():
                return t["key"]
        except Exception:
            continue
    return None


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: python -m engine.nhl_explain HOME AWAY")
        print("  HOME/AWAY can be abbreviation (BOS) or JSON key (bruins)")
        sys.exit(1)

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

    home_arg = sys.argv[1]
    away_arg = sys.argv[2]

    home_key = home_arg if len(home_arg) > 3 else _find_key(home_arg) or home_arg.lower()
    away_key = away_arg if len(away_arg) > 3 else _find_key(away_arg) or away_arg.lower()

    from engine.nhl_predict import predict_matchup, _get_live_team_stats

    print(f"\n{'='*70}")
    print(f"  NHL Prediction Explainer: {away_arg} @ {home_arg}")
    print(f"{'='*70}")

    # Show raw live stats first so we know what the model is reading
    live = _get_live_team_stats()
    print(f"\n── Live stats for both teams ──")
    for label, arg in [("HOME", home_arg), ("AWAY", away_arg)]:
        abbr = arg.upper() if len(arg) <= 4 else None
        if abbr and abbr in live:
            s = live[abbr]
            print(f"\n  {label} ({abbr}):")
            print(f"    GF/gm: {s.get('goals_for_avg', '?')}   "
                  f"GA/gm: {s.get('goals_against_avg', '?')}")
            print(f"    PP%:   {s.get('pp_pct', '?')}   "
                  f"PK%:   {s.get('pk_pct', '?')}")
            print(f"    SV%:   {s.get('save_pct', '?')}   "
                  f"Shots: {s.get('shots_per_game', '?')}")
            print(f"    FO%:   {s.get('faceoff_pct', '?')}")
            print(f"    Home pts%: {s.get('home_pts_pct', '?')}   "
                  f"Road pts%: {s.get('road_pts_pct', '?')}")
            print(f"    Home GF/A: {s.get('home_gf_avg', '?')}/{s.get('home_ga_avg', '?')}")
            print(f"    Road GF/A: {s.get('road_gf_avg', '?')}/{s.get('road_ga_avg', '?')}")

    # Run prediction
    print(f"\n── Running prediction for {home_key} vs {away_key} ──")
    pred = predict_matchup(home_key, away_key)
    if not pred:
        print(f"  ERROR: could not load prediction for {home_key} / {away_key}")
        return

    h = pred.get("home", {})
    a = pred.get("away", {})
    es = pred.get("expected_score", {})
    wp = pred.get("win_prob", {})
    pl = pred.get("puck_line", {})
    factors = pred.get("factors", {})

    print(f"\n{'='*70}")
    print(f"  Final expected score: {h.get('abbreviation')} {es.get('home', 0):.2f}  "
          f"{a.get('abbreviation')} {es.get('away', 0):.2f}")
    print(f"  Win probability: {h.get('abbreviation')} {100*wp.get('home', 0):.1f}%  "
          f"{a.get('abbreviation')} {100*wp.get('away', 0):.1f}%")
    print(f"  Regulation draw: {100*pred.get('regulation_draw_prob', 0):.1f}%")
    print(f"  Projected total: {pred.get('total', 0):.1f}")

    print(f"\n  Puck line:")
    print(f"    {h.get('abbreviation')} -1.5: {100*pl.get('home_minus_1_5', 0):.1f}%")
    print(f"    {a.get('abbreviation')} +1.5: {100*pl.get('away_plus_1_5', 0):.1f}%")
    print(f"    {a.get('abbreviation')} -1.5: {100*pl.get('away_minus_1_5', 0):.1f}%")
    print(f"    {h.get('abbreviation')} +1.5: {100*pl.get('home_plus_1_5', 0):.1f}%")

    print(f"\n  O/U (model lines):")
    for line, probs in sorted((pred.get("over_under") or {}).items()):
        if probs.get("over", 0) > 0.50:
            print(f"    {line}: OVER {100*probs['over']:.1f}%")
        elif probs.get("under", 0) > 0.50:
            print(f"    {line}: UNDER {100*probs['under']:.1f}%")

    # Sanity checks: does the pick align with the stats?
    print(f"\n── Sanity checks ──")
    h_live = live.get(h.get("abbreviation"), {})
    a_live = live.get(a.get("abbreviation"), {})
    h_pts = h_live.get("home_pts_pct") or 0.5
    a_pts = a_live.get("road_pts_pct") or 0.5
    better_team = h.get("abbreviation") if h_pts > a_pts else a.get("abbreviation")
    model_pick = h.get("abbreviation") if wp.get("home", 0) > wp.get("away", 0) else a.get("abbreviation")
    print(f"  Better pts% team:  {better_team} (home pts={h_pts:.3f} vs road pts={a_pts:.3f})")
    print(f"  Model favors:      {model_pick}")
    if better_team != model_pick:
        print(f"  >> Model is picking AGAINST the better-rated team. This may be")
        print(f"     correct (form, injuries, etc.) or it may indicate a flip.")

    h_gf = h_live.get("goals_for_avg") or 3.0
    a_gf = a_live.get("goals_for_avg") or 3.0
    h_ga = h_live.get("goals_against_avg") or 3.0
    a_ga = a_live.get("goals_against_avg") or 3.0
    print(f"\n  {h.get('abbreviation')} profile: scores {h_gf:.2f}/gm, allows {h_ga:.2f}/gm")
    print(f"  {a.get('abbreviation')} profile: scores {a_gf:.2f}/gm, allows {a_ga:.2f}/gm")
    print(f"  Final xG:     {h.get('abbreviation')} {es.get('home', 0):.2f}   "
          f"{a.get('abbreviation')} {es.get('away', 0):.2f}")
    expected_dir = "home" if (h_gf - h_ga) > (a_gf - a_ga) else "away"
    actual_dir = "home" if es.get("home", 0) > es.get("away", 0) else "away"
    print(f"  xG direction aligns with team-quality direction: "
          f"{'YES' if expected_dir == actual_dir else 'NO — possible sign flip'}")

    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    main()
