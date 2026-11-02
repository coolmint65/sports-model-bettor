import sys
sys.path.insert(0, r'C:\sports-model-bettor')
try:
    from engine.picks import fetch_real_odds_for_games, match_odds
    o = fetch_real_odds_for_games()
    print("n_odds:", len(o))
    print("keys:", list(o.keys())[:30])
    for k, v in list(o.items())[:30]:
        if isinstance(v, dict):
            print("  KEY", repr(k), "| odds_home:", v.get("odds_home"), "odds_away:", v.get("odds_away"), "home_ml:", v.get("home_ml"), "away_ml:", v.get("away_ml"))
    print("match MIL/TB:", bool(match_odds("MIL", "TB", o)))
    print("match TB/MIL:", bool(match_odds("TB", "MIL", o)))
except Exception as e:
    import traceback; traceback.print_exc()
