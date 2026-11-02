import sys, json; sys.path.insert(0, r'C:\sports-model-bettor')
from scrapers.hardrock_odds import _fetch_with_markets, _parse_response, _fetch_events_for_comp
# raw events for MLB comp
evs, e_err = _fetch_events_for_comp('2899138833758814469')
print("events err:", repr(e_err), "| n_events:", len(evs) if evs else 0)
for ev in (evs or [])[:3]:
    print("\n--- RAW EVENT ---")
    for key in ("name","eventNames","participants","competitors","teams","home","away","startTime","startEventDate"):
        if key in ev: print(f"  {key}: {json.dumps(ev[key])[:200]}")
    print("  n_markets:", len(ev.get("markets") or ev.get("marketNodes") or []))
data, werr = _fetch_with_markets("mlb")
parsed = _parse_response("mlb", data)
print("\nPARSED n_games:", len(parsed), "| keys:", list(parsed.keys())[:10])
