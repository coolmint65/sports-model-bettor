import sys, json; sys.path.insert(0, r'C:\sports-model-bettor')
from scrapers.hardrock_odds import _fetch_events_for_comp, _extract_teams, _team_abbr
evs, _ = _fetch_events_for_comp('2899138833758814469')
ev = evs[0]
print("event name:", ev.get("name"))
away, home = _extract_teams(ev)
print("extracted away/home:", repr(away), repr(home), "-> abbr:", _team_abbr("mlb",away), _team_abbr("mlb",home))
mkts = ev.get("markets") or ev.get("marketNodes") or []
print("n_markets:", len(mkts))
from collections import Counter
types = Counter()
for m in mkts:
    t = m.get("type") or m.get("marketType") or m.get("code") or m.get("key")
    types[str(t)] += 1
print("DISTINCT market types (top 25):")
for t, n in types.most_common(25):
    print(f"   {t}  x{n}")
# Does ANY match the whitelist?
from scrapers.hardrock_odds import _GAME_MARKET_TYPES
print("whitelist keys:", list(_GAME_MARKET_TYPES.keys()))
hit = [t for t in types if any(t.endswith(sfx) or sfx in t for sfx in _GAME_MARKET_TYPES)]
print("types matching whitelist:", hit[:10])
