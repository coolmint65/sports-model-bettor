# -*- coding: utf-8 -*-
"""READ-ONLY: why does soccer/eng_premier have no proven market? Dump cells."""
import sys, sqlite3, os
sys.path.insert(0, r"C:\sports-model-bettor")
from engine.selection import edge_proven

m = edge_proven._map()  # full map incl non-eligible
print("=== ALL soccer cells in edge_proven map (sport=soccer) ===")
soc = {k: v for k, v in m.items() if k[0] == "soccer"}
if not soc:
    print("  (no soccer cells at all in picks_unified settled record)")
for k, v in sorted(soc.items(), key=lambda kv: (kv[0][1], kv[0][2], kv[0][3])):
    print("  %-8s %-12s %-8s %-6s | n=%-4d win=%4.1f roi=%6.1f conf=%.3f elig=%s" % (
        k[0], k[1], k[2], k[3] or "(none)",
        v["n"], v["win"], v["roi"], v["conf"], v["eligible"]))

print("\n=== thresholds ===")
print("MIN_ROI=%s STD_N=%s FAST_N=%s FAST_CONF=%s MIN_CONF(floor)=%s" % (
    edge_proven.MIN_ROI, edge_proven.STD_N, edge_proven.FAST_N,
    edge_proven.FAST_CONF, edge_proven.MIN_CONF))

# Raw blended (pre-condition) ML/DC record straight from picks_unified, to show
# what the numbers looked like BEFORE condition-segmentation split them.
DB = os.path.join(r"C:\sports-model-bettor", "data", "picks_unified.db")
con = sqlite3.connect("file:" + DB + "?mode=ro", uri=True)
print("\n=== picks_unified raw soccer settled counts by (league, bet_type) [blended, no condition] ===")
for lg, bt, n, w in con.execute(
        "SELECT league, UPPER(bet_type), COUNT(*), SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) "
        "FROM picks WHERE sport='soccer' AND result IN ('W','L') GROUP BY league, UPPER(bet_type) "
        "ORDER BY league, COUNT(*) DESC"):
    print("  %-12s %-8s n=%-4d W=%-4d win%%=%.1f" % (lg, bt, n, w, 100.0*w/n if n else 0))
con.close()
