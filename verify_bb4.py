import os, sys, sqlite3
sys.path.insert(0, r"C:\sports-model-bettor")
from engine.selection import select_pick as _authority_select
Q1 = {"Q1_ML","Q1_SPREAD","Q1_TOTAL"}
def _to_pick(r):
    return {"type": r["bet_type"], "pick": r["pick"], "model_prob": r["model_prob"],
            "edge": r["edge"], "odds": r["odds"], "locked": True}
def gated_auth(league, rows):
    rs = sorted(rows, key=lambda r:(r["edge"] if r["edge"] is not None else -1e9), reverse=True)
    f = next((_to_pick(r) for r in rs if (r["bet_type"] or "") not in Q1), None)
    q = next((_to_pick(r) for r in rs if (r["bet_type"] or "") in Q1), None)
    return _authority_select("basketball", league, [p for p in (f,q) if p])
def ungated_best(rows):
    cands=[r for r in rows if (r["edge"] or 0) is not None]
    staked=[r for r in rows if (r["stake_units"] or 0)>0]
    pool=staked if staked else list(rows)
    return max(pool, key=lambda r:(float(r["edge"] or 0))*float(r["model_prob"] or 0))
p=os.path.join(r"C:\sports-model-bettor\data\basketball","wnba.db")
c=sqlite3.connect(p); c.row_factory=sqlite3.Row
# games where gated authority is non-None
gids=[r[0] for r in c.execute("SELECT game_id, MAX(created_at) mc FROM picks WHERE COALESCE(is_shadow,0)=0 AND (result IS NULL OR result!='V') GROUP BY game_id ORDER BY mc DESC")]
shown=0
for gid in gids:
    rows=c.execute("SELECT bet_type,pick,edge,model_prob,odds,result,created_at,stake_units FROM picks WHERE game_id=? AND COALESCE(is_shadow,0)=0 AND (result IS NULL OR result!='V') ORDER BY created_at ASC",(str(gid),)).fetchall()
    a=gated_auth("wnba",rows)
    if not a: continue
    mu=c.execute("SELECT matchup,date FROM picks WHERE game_id=? AND COALESCE(is_shadow,0)=0 LIMIT 1",(str(gid),)).fetchone()
    latest=c.execute("SELECT bet_type,pick FROM picks WHERE game_id=? AND COALESCE(is_shadow,0)=0 AND (result IS NULL OR result!='V') ORDER BY created_at DESC LIMIT 1",(str(gid),)).fetchone()
    ub=ungated_best(rows)
    print(f"\n--- wnba {gid} {mu['matchup']} {mu['date']} (rows={len(rows)}) ---")
    for r in rows: print(f"    s0 bt={r['bet_type']:<9} pick={str(r['pick'])[:22]:<22} edge={r['edge']} prob={r['model_prob']} res={r['result']} su={r['stake_units']} ca={r['created_at']}")
    print(f"    CARD best_pick (gated select_pick) = bt={a['type']} pick={a['pick']}")
    print(f"    NAIVE latest-row                    = bt={latest['bet_type']} pick={latest['pick']}")
    print(f"    UNGATED edge*prob best              = bt={ub['bet_type']} pick={ub['pick']}")
    shown+=1
    if shown>=3: break
c.close()
