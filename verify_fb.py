import os, sys, sqlite3, glob
sys.path.insert(0, r"C:\sports-model-bettor")
from engine.selection import edge_proven
m = edge_proven._compute()
print("=== PROVEN football cells ===")
for k,v in sorted(m.items()):
    if k[0]=="football" and v.get("eligible"):
        print("  ELIGIBLE:", k, {kk:(round(vv,3) if isinstance(vv,float) else vv) for kk,vv in v.items() if kk in ('n','roi','conf','eligible')})
# football DBs
for base in [r"C:\sports-model-bettor\data\football", r"C:\sports-model-bettor\data"]:
    for p in glob.glob(os.path.join(base,"*.db")):
        n=os.path.basename(p)
        if not any(x in n for x in ("nfl","cfb","ufl","football")): continue
        try:
            c=sqlite3.connect(p); 
            tabs=[r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")]
            if "picks" not in tabs: 
                c.close(); continue
            s0=c.execute("SELECT COUNT(*),COUNT(DISTINCT game_id),MAX(date) FROM picks WHERE COALESCE(is_shadow,0)=0 AND (result IS NULL OR result!='V')").fetchone()
            # rows per game distribution
            mx=c.execute("SELECT MAX(cnt) FROM (SELECT game_id,COUNT(*) cnt FROM picks WHERE COALESCE(is_shadow,0)=0 AND (result IS NULL OR result!='V') GROUP BY game_id)").fetchone()[0]
            print(f"  {p}: s0 rows={s0[0]} games={s0[1]} maxdate={s0[2]} max_rows_per_game={mx}")
            c.close()
        except Exception as e:
            print("  ",p,"ERR",e)
