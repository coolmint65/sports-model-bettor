import sqlite3, os, sys
ROOT = r"C:\sports-model-bettor\data"
def ro(path):
    c = sqlite3.connect("file:" + path + "?mode=ro", uri=True); c.row_factory = sqlite3.Row; return c
u = ro(os.path.join(ROOT, "picks_unified.db"))
for sp in ("football", "basketball", "soccer", "nhl"):
    print("==", sp)
    seen = set()
    for r in u.execute("SELECT league, bet_type, pick_text, line, result FROM picks WHERE sport=? AND pick_date>='2026-10-20' ORDER BY id DESC", (sp,)):
        k = (r["league"], r["bet_type"])
        if k in seen: continue
        seen.add(k); print("  ", tuple(r))
