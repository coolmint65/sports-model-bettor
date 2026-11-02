import sys; sys.path.insert(0, r'C:\sports-model-bettor')
import scrapers.hardrock_odds as hr
hr._event_already_started = lambda ev: False   # force PREMATCH path
data, err = hr._fetch_with_markets("mlb")
parsed = hr._parse_response("mlb", data)
print("with_markets err:", repr(err))
print("PARSED games (forced prematch):", len(parsed), "| keys:", list(parsed.keys()))
for k, v in parsed.items():
    print("  ", k, "| odds_home:", v.get("odds_home"), "odds_away:", v.get("odds_away"),
          "| home_ml:", v.get("home_ml"), "away_ml:", v.get("away_ml"), "| O/U:", v.get("over_under"))
