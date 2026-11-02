import os, sys, sqlite3, glob
sys.path.insert(0, r"C:\sports-model-bettor")
from engine.selection import select_pick as _authority_select
from engine.selection import edge_proven
BB = r"C:\sports-model-bettor\data\basketball"
Q1 = {"Q1_ML","Q1_SPREAD","Q1_TOTAL"}
def _to_pick(r):
    return {"type": r["bet_type"], "pick": r["pick"], "model_prob": r["model_prob"],
            "edge": r["edge"], "odds": r["odds"], "locked": True}
def auth(league, rows):
    rs = sorted(rows, key=lambda r: (r["edge"] if r["edge"] is not None else -1e9), reverse=True)
    f = next((_to_pick(r) for r in rs if (r["bet_type"] or "") not in Q1), None)
    q = next((_to_pick(r) for r in rs if (r["bet_type"] or "") in Q1), None)
    return _authority_select("basketball", league, [p for p in (f,q) if p])

print("=== PROVEN basketball cells (edge_proven map) ===")
try:
    m = edge_proven._compute()
    for k,v in sorted(m.items()):
        if k[0]=="basketball" and v.get("eligible"):
            print("  ELIGIBLE:", k, {kk:round(vv,3) if isinstance(vv,float) else vv for kk,vv in v.items() if kk in ('n','roi','conf','eligible')})
except Exception as e:
    print("  compute err:", e)

print("\n=== per-league: authority(non-None) over recorded is_shadow=0 rows ===")
print("league | games_s0 | auth_nonNone | ungated_bestfull_nonNull | s1_games")
for p in sorted(glob.glob(os.path.join(BB,"*.db"))):
    lg = os.path.basename(p)[:-3]
    c = sqlite3.connect(p); c.row_factory = sqlite3.Row
    try:
        gids = [r[0] for r in c.execute("SELECT DISTINCT game_id FROM picks WHERE COALESCE(is_shadow,0)=0 AND (result IS NULL OR result!='V')")]
    except Exception:
        c.close(); continue
    if not gids:
        c.close(); continue
    nonnone=0; ungated=0
    for gid in gids:
        rows = c.execute("SELECT bet_type,pick,edge,model_prob,odds FROM picks WHERE game_id=? AND COALESCE(is_shadow,0)=0 AND (result IS NULL OR result!='V')",(str(gid),)).fetchall()
        if not rows: continue
        a = auth(lg, rows)
        if a: nonnone+=1
        # ungated best full-game (any row) exists -> would populate under ungated rule
        if any((r["edge"] or 0)>0 for r in rows): ungated+=1
    s1g = c.execute("SELECT COUNT(DISTINCT game_id) FROM picks WHERE COALESCE(is_shadow,0)=1").fetchone()[0]
    print(f"{lg:<22} | {len(gids):>4} | {nonnone:>4} | {ungated:>4} | {s1g:>4}")
    c.close()
