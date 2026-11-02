import sys; sys.path.insert(0, r'C:\sports-model-bettor')
from scrapers.hardrock_odds import _fetch_sports_tree, _resolve_comp_ids, _fetch_with_markets
tree, err = _fetch_sports_tree()
print("TREE err:", repr(err))
if isinstance(tree, dict):
    sports = ((tree.get("data") or {}).get("betSync") or {}).get("sports") or []
    print("TREE n_sports:", len(sports))
    for sp in sports:
        code = sp.get("code") or sp.get("name")
        if code and ("BASE" in str(code).upper() or "BALL" in str(code).upper()):
            comps = []
            for cat in (sp.get("categories") or []):
                for c in (cat.get("competitions") or []):
                    comps.append((c.get("id"), c.get("name")))
            print("  SPORT", code, "comps:", comps[:12])
try:
    print("mlb comp_ids:", _resolve_comp_ids("mlb", tree))
except Exception as e:
    import traceback; traceback.print_exc()
data, werr = _fetch_with_markets("mlb")
print("WITH_MARKETS err:", repr(werr))
