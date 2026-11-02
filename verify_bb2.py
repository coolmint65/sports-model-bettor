import os, sys, sqlite3
sys.path.insert(0, r"C:\sports-model-bettor")
from engine.selection import select_pick as _authority_select
BB = r"C:\sports-model-bettor\data\basketball"
Q1 = {"Q1_ML","Q1_SPREAD","Q1_TOTAL"}

def _to_pick(r):
    return {"type": r["bet_type"], "pick": r["pick"], "model_prob": r["model_prob"],
            "edge": r["edge"], "odds": r["odds"], "locked": True}

def card_authority_over_recorded(league, rows):
    # Mirror _lock_basketball_live_pick: full_locked=first non-Q1 by edge desc, q1_locked=first Q1
    rs = sorted(rows, key=lambda r: (r["edge"] if r["edge"] is not None else -1e9), reverse=True)
    full_locked = next((_to_pick(r) for r in rs if (r["bet_type"] or "") not in Q1), None)
    q1_locked   = next((_to_pick(r) for r in rs if (r["bet_type"] or "") in Q1), None)
    cands = [p for p in (full_locked, q1_locked) if p]
    try:
        return _authority_select("basketball", league, cands)
    except Exception as e:
        return {"_err": str(e)}

for league in ["israel_super","czech_nbl","china_cba","wnba","germany_bbl"]:
    p = os.path.join(BB, league + ".db")
    c = sqlite3.connect(p); c.row_factory = sqlite3.Row
    gids = [r[0] for r in c.execute(
        "SELECT game_id, MAX(created_at) mc FROM picks WHERE COALESCE(is_shadow,0)=0 AND (result IS NULL OR result!='V') "
        "GROUP BY game_id ORDER BY mc DESC LIMIT 3")]
    print("\n================= LEAGUE:", league, "=================")
    for gid in gids:
        rows = c.execute("SELECT bet_type,pick,edge,model_prob,odds,result,created_at,stake_units FROM picks "
                         "WHERE game_id=? AND COALESCE(is_shadow,0)=0 AND (result IS NULL OR result!='V') "
                         "ORDER BY created_at ASC", (str(gid),)).fetchall()
        mu = c.execute("SELECT matchup,date FROM picks WHERE game_id=? AND COALESCE(is_shadow,0)=0 LIMIT 1",(str(gid),)).fetchone()
        print(f"\n--- game {gid}  {mu['matchup'] if mu else '?'}  {mu['date'] if mu else ''}  (s0 rows={len(rows)}) ---")
        for r in rows:
            print(f"    s0: bt={r['bet_type']:<9} pick={str(r['pick'])[:28]:<28} edge={r['edge']} prob={r['model_prob']} odds={r['odds']} res={r['result']} su={r['stake_units']} ca={r['created_at']}")
        # latest is_shadow=0 row (the naive football pattern)
        latest = c.execute("SELECT bet_type,pick,result FROM picks WHERE game_id=? AND COALESCE(is_shadow,0)=0 AND (result IS NULL OR result!='V') ORDER BY created_at DESC LIMIT 1",(str(gid),)).fetchone()
        # card authority reproduced over recorded rows
        auth = card_authority_over_recorded(league, rows)
        a_bt = (auth or {}).get("type") if isinstance(auth,dict) else None
        a_pk = (auth or {}).get("pick") if isinstance(auth,dict) else None
        print(f"    => LATEST-row(football-naive): bt={latest['bet_type']} pick={latest['pick']}")
        print(f"    => CARD-authority(select_pick): bt={a_bt} pick={a_pk}  {'' if not (isinstance(auth,dict) and auth.get('_err')) else 'ERR='+auth['_err']}")
        match = (latest['bet_type']==a_bt and str(latest['pick'])==str(a_pk))
        print(f"    => MATCH(latest==authority)? {match}")
    c.close()
