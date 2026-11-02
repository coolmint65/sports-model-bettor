import sys, os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
os.chdir(r'C:\sports-model-bettor')
sys.path.insert(0, r'C:\sports-model-bettor')
import sqlite3
from engine.db import get_conn
conn = get_conn(); conn.row_factory = sqlite3.Row

# --- Try to load the REAL patched code (best-case verification) ---
REAL_TRACKER = REAL_GRP = None
import_err = None
try:
    from backend.server.routes_mlb import _mlb_recorded_authority_pick as REAL_TRACKER
    from backend.server import _get_recorded_pick as REAL_GRP
    print("IMPORT: backend.server.routes_mlb OK — testing REAL patched functions\n")
except Exception as e:
    import_err = repr(e)
    print("IMPORT: could not import server package:", import_err)
    print("        -> falling back to replicated SQL (equivalent).\n")

def card_selection(matchup, date, include_settled):
    # EXACT _get_recorded_pick selection (backend/server/__init__.py:1319-1324),
    # optionally with the `result IS NULL` filter removed so we can see the row the
    # card DID / WOULD headline for an already-settled game.
    rf = "AND result IS NULL " if not include_settled else "AND (result IS NULL OR result!='V') "
    return conn.execute(
        "SELECT bet_type,pick,model_prob,edge,odds,result FROM picks "
        "WHERE date=? AND matchup=? " + rf + "ORDER BY edge DESC LIMIT 1",
        (date, matchup)).fetchone()

def tracker_pick(game_id):
    if REAL_TRACKER:
        return REAL_TRACKER(game_id, conn=conn)
    # Replicated fallback (same SQL as the patched helper)
    r = conn.execute(
        "SELECT matchup,date,bet_type,pick,odds,edge,model_prob,result "
        "FROM picks WHERE game_id=? AND (result IS NULL OR result!='V') "
        "ORDER BY edge DESC LIMIT 1", (str(game_id),)).fetchone()
    if not r: return None
    return {"type": r["bet_type"], "pick": r["pick"], "edge": r["edge"],
            "odds": r["odds"], "result": r["result"], "prob": r["model_prob"]}

def eq(a, b):
    return (a is not None and b is not None
            and a["bet_type"] == b["type"] and a["pick"] == b["pick"]
            and (a["edge"] or 0) == (b["edge"] or 0) and a["odds"] == b["odds"])

# --- Sample set: all multi-row games (where max-edge selection is non-trivial)
#     + a few recent single-row games. ---
multi = [r[0] for r in conn.execute(
    "SELECT game_id FROM picks WHERE game_id IS NOT NULL GROUP BY game_id "
    "HAVING COUNT(*)>1 ORDER BY MAX(created_at) DESC")]
recent = [r[0] for r in conn.execute(
    "SELECT game_id FROM picks WHERE game_id IS NOT NULL AND (result IS NULL OR result!='V') "
    "GROUP BY game_id ORDER BY MAX(created_at) DESC LIMIT 4")]
sample = []
for g in multi + recent:
    if g not in sample: sample.append(g)

print(f"=== {len(multi)} multi-row games, {len(recent)} recent singles; verifying {len(sample)} games ===\n")

mismatches = 0
for gid in sample:
    # all rows for context
    allrows = conn.execute(
        "SELECT bet_type,pick,edge,odds,result FROM picks WHERE game_id=? ORDER BY edge DESC",
        (str(gid),)).fetchall()
    meta = conn.execute("SELECT matchup,date FROM picks WHERE game_id=? LIMIT 1", (str(gid),)).fetchone()
    mu, dt = meta["matchup"], meta["date"]
    is_multi = len(allrows) > 1
    tag = "MULTI" if is_multi else "single"
    print(f"--- game {gid}  {mu}  {dt}  [{tag}, {len(allrows)} recorded row(s)] ---")
    for rr in allrows:
        print(f"      row: bt={rr['bet_type']!r:14} pick={rr['pick']!r:16} edge={rr['edge']} odds={rr['odds']} result={rr['result']}")
    card_asrec = card_selection(mu, dt, include_settled=True)   # card authority (pre-settle logic)
    card_live  = card_selection(mu, dt, include_settled=False)  # live _get_recorded_pick SQL
    trk        = tracker_pick(gid)
    # Also exercise the REAL _get_recorded_pick if importable
    grp_live = REAL_GRP("mlb", mu, dt) if REAL_GRP else "(not imported)"
    print(f"   CARD authority (edge DESC, settled-incl): bt={card_asrec['bet_type']!r} pick={card_asrec['pick']!r} edge={card_asrec['edge']} odds={card_asrec['odds']} result={card_asrec['result']}")
    print(f"   TRACKER (_mlb_recorded_authority_pick)   : bt={trk['type']!r} pick={trk['pick']!r} edge={trk['edge']} odds={trk['odds']} result={trk['result']}")
    print(f"   live _get_recorded_pick (result IS NULL) : {('None (no pending row)' if (card_live is None) else dict(card_live))}  | REAL call -> {grp_live if not hasattr(grp_live,'get') else {k:grp_live.get(k) for k in ('type','pick','edge','odds')}}")
    ok = eq(card_asrec, trk)
    if not ok: mismatches += 1
    print(f"   MATCH (bet_type,pick,edge,odds): {'YES' if ok else '*** NO ***'}\n")

print(f"=== RESULT: {len(sample)-mismatches}/{len(sample)} games match; mismatches={mismatches} ===")

# --- Also reconstruct the endpoint's row list + summary (no auto-settle side effect) ---
gids = [r[0] for r in conn.execute(
    "SELECT game_id, MAX(created_at) c FROM picks "
    "WHERE game_id IS NOT NULL AND (result IS NULL OR result != 'V') "
    "GROUP BY game_id ORDER BY c DESC LIMIT 300")]
rows = []
for gid in gids:
    b = tracker_pick(gid)
    if b: rows.append(b)
w = sum(1 for r in rows if r["result"]=="W"); l = sum(1 for r in rows if r["result"]=="L")
print(f"\nendpoint reconstruction: {len(rows)} rows (games), W={w} L={l} "
      f"win%={round(100*w/(w+l),1) if (w+l) else 0}")
