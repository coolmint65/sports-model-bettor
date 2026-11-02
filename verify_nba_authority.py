# -*- coding: utf-8 -*-
"""READ-ONLY verification: compare OLD tracker selection (_nba_merged_recorded_best
re-derivation across nba_paper_picks + nba_derivative_picks, cal_edge + gate) vs
the CARD's authority best = max(Full-view, Q1-view) by edge*model_prob, where the
view headlines are the recorded authority rows in nba_paper_picks.
Does NOT write anything."""
import sys, sqlite3
sys.path.insert(0, r"C:\sports-model-bettor")

DB = r"C:\sports-model-bettor\data\nba.db"
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# --- replicate scope bucketing (routes_nba._nba_market_scope) ---
def scope(bt):
    bt = (bt or "").strip().upper()
    if bt in ("ML","SPREAD","TOTAL","ALT SPREAD","ALT TOTAL","ALT_SPREAD","ALT_TOTAL"):
        return "Full Game"
    if bt in ("Q1_ML","Q1_SPREAD","Q1_TOTAL"):
        return "Q1"
    return "Q1 Team Total"

# --- OLD tracker best (replicate _nba_merged_recorded_best) ---
from engine.calibration_gate import cal_edge as _ce, headline_eligible as _he

def _extra_ev_map():
    m = {}
    for row in conn.execute(
        "SELECT bet_type, COUNT(*) n, MIN(date) mn, MAX(date) mx FROM ("
        "  SELECT bet_type,date,result FROM nba_paper_picks"
        "  UNION ALL SELECT bet_type,date,result FROM nba_derivative_picks"
        ") WHERE result IS NOT NULL AND result != 'V' GROUP BY bet_type"):
        n = row["n"] or 0
        days = 0
        if row["mn"] and row["mx"]:
            try:
                days = int((conn.execute("SELECT julianday(?)-julianday(?)",(row["mx"],row["mn"])).fetchone()[0]) or 0)
            except Exception:
                days = 0
        m[row["bet_type"]] = (n, days)
    return m
EVM = _extra_ev_map()

def old_best(gid):
    cands = []
    for r in conn.execute(
        "SELECT bet_type,pick,odds,edge,model_prob,result FROM nba_paper_picks "
        "WHERE game_id=? AND (result IS NULL OR result!='V') AND ABS(COALESCE(edge,0))<=20.0",(str(gid),)):
        d = dict(r); d["_scope"]=scope(d["bet_type"]); d["_extra"]=(d["_scope"]!="Full Game"); cands.append(d)
    for r in conn.execute(
        "SELECT bet_type,pick,odds,edge,model_prob,result FROM nba_derivative_picks "
        "WHERE game_id=? AND (result IS NULL OR result!='V')",(str(gid),)):
        d = dict(r); d["_scope"]=scope(d["bet_type"]); d["_extra"]=True; cands.append(d)
    if not cands: return None
    scored=[]
    for p in cands:
        ce=_ce("nba",p["bet_type"],p["model_prob"],p["odds"],pick_text=p["pick"],raw_edge=p["edge"])
        n=days=None
        if p["_extra"]: n,days=EVM.get(p["bet_type"],(0,0))
        elig=_he(ce,p["_extra"],n,days)
        scored.append((elig, ce if ce is not None else -999.0, p))
    pool=[s for s in scored if s[0]]
    if not pool: return None
    b=max(pool,key=lambda s:s[1])[2]
    return b

# --- NEW card-authority best = max(best_full, best_q1) by edge*model_prob ---
def _epr(p):
    return float(p["edge"] or 0) * float(p["model_prob"] or 0)

def new_best(gid):
    # Faithful to card line 670: NO edge guard (card applies none before `best`).
    rows=[dict(r) for r in conn.execute(
        "SELECT bet_type,pick,odds,edge,model_prob,result FROM nba_paper_picks "
        "WHERE game_id=? AND (result IS NULL OR result!='V')",(str(gid),))]
    for r in rows: r["_scope"]=scope(r["bet_type"])
    full=[r for r in rows if r["_scope"]=="Full Game"]
    q1p =[r for r in rows if r["_scope"]=="Q1"]
    q1d =[r for r in rows if r["_scope"]=="Q1 Team Total"]
    cand_full = max(full, key=_epr) if full else None
    cand_q1 = (max(q1p, key=_epr) if q1p else (max(q1d, key=_epr) if q1d else None))
    mains=[x for x in (cand_full,cand_q1) if x]
    if not mains: return None
    return max(mains, key=_epr)

def fmt(p):
    if not p: return "None"
    return f"{p['bet_type']}/{p['pick']} edge={p['edge']} prob={p['model_prob']} odds={p['odds']} res={p['result']}"

# --- recent games (most recently created in paper table) ---
gids=[r[0] for r in conn.execute(
    "SELECT game_id, MAX(created_at) c FROM nba_paper_picks "
    "WHERE result IS NOT NULL AND result!='V' GROUP BY game_id ORDER BY c DESC LIMIT 40")]
print("recent settled game_ids:", gids, "\n")
diffs=0
for gid in gids:
    meta=conn.execute("SELECT matchup,date FROM nba_paper_picks WHERE game_id=? LIMIT 1",(str(gid),)).fetchone()
    print(f"=== game {gid}  {meta['matchup'] if meta else '?'}  {meta['date'] if meta else '?'} ===")
    allrows=[dict(r) for r in conn.execute(
        "SELECT bet_type,pick,edge,model_prob,odds,result FROM nba_paper_picks WHERE game_id=? ORDER BY edge DESC",(str(gid),))]
    for r in allrows:
        print(f"   paper[{scope(r['bet_type'])}]: {fmt(r)}")
    for r in conn.execute("SELECT bet_type,pick,edge,model_prob,odds,result FROM nba_derivative_picks WHERE game_id=? ORDER BY edge DESC",(str(gid),)):
        print(f"   deriv[{scope(r['bet_type'])}]: {fmt(dict(r))}")
    o=old_best(gid); n=new_best(gid)
    if o is None and n is None:
        same = True
    else:
        same = bool(o and n and o["bet_type"]==n["bet_type"] and o["pick"]==n["pick"])
    if not same: diffs+=1
    print(f"   OLD tracker  -> {fmt(o)}")
    print(f"   NEW card-auth-> {fmt(n)}")
    print(f"   {'MATCH' if same else '*** DIFFERS ***'}\n")
print(f"TOTAL games={len(gids)} differ={diffs}")
conn.close()
