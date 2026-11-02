import sys, os, time, json
sys.path.insert(0, r"C:\sports-model-bettor")
os.chdir(r"C:\sports-model-bettor")
t0 = time.time()
from engine.selection import edge_proven as ep
m = ep._map()
print("edge_proven cells", len(m), "eligible", [k for k, v in m.items() if v["eligible"]], "%.2fs" % (time.time() - t0))
for k in sorted(m):
    if k[0] in ("nba", "nhl") or (k[0] == "basketball" and k[1] in ("china_cba", "australia_nbl", "ncaam", "ncaaw")) or (k[0]=="soccer" and k[1]=="eng_premier") or k[0]=="football":
        v = m[k]
        print("  ", k, v["n"], round(v["win"], 1), round(v["roi"], 1), round(v["conf"], 3), v["eligible"])
t0 = time.time()
from engine.sports.nhl import card_gate as cg
s = cg.summary()
print("NHL card gate", s["rule"], "proven", s["proven_cells"], "shadow_pending", s["shadow_pending"], "%.2fs" % (time.time() - t0))
for c in s["cells"]:
    print("  ", c)
t0 = time.time()
from engine.sports.nfl import prop_picks as npp
g = npp.card_gate()
print("NFL card gate", g["rule"], "proven", g["proven"], "%.2fs" % (time.time() - t0))
for k, v in g["cells"].items():
    print("  ", k, v)
from engine import player_props_potd as ppp
for sp in ("nhl", "nba", "nfl"):
    t0 = time.time()
    recs = ppp.cell_records(sp)
    print("POTD cells", sp, ppp.model_since(sp), "%.2fs" % (time.time() - t0))
    for k, v in recs.items():
        print("  ", k, {x: v.get(x) for x in ("n", "wins", "losses", "pushes", "win_pct", "be_pct", "roi", "margin_pp", "se_pp", "proven")})
