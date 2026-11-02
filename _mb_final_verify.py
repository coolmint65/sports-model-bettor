import sys, os, sqlite3
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
os.chdir(r'C:\sports-model-bettor'); sys.path.insert(0, r'C:\sports-model-bettor')
from engine.db import get_conn
conn = get_conn(); conn.row_factory = sqlite3.Row
from backend.server.routes_mlb import _mlb_recorded_authority_pick as TRK
from backend.server import _get_recorded_pick as GRP
print("imported REAL patched _mlb_recorded_authority_pick + _get_recorded_pick\n")

def card_sel(matchup, date):
    # _get_recorded_pick's EXACT selection, result-filter removed to see the
    # settled row the card headlined for that (matchup,date).
    return conn.execute(
        "SELECT bet_type,pick,model_prob,edge,odds,result FROM picks "
        "WHERE date=? AND matchup=? AND (result IS NULL OR result!='V') "
        "ORDER BY edge DESC LIMIT 1", (date, matchup)).fetchone()

# Endpoint enumeration (date-aware), same as patched code:
grp = [(r[0], r[1]) for r in conn.execute(
    "SELECT game_id, date, MAX(created_at) c FROM picks "
    "WHERE game_id IS NOT NULL AND (result IS NULL OR result != 'V') "
    "GROUP BY game_id, date ORDER BY c DESC LIMIT 300")]
print(f"endpoint groups (game_id,date): {len(grp)}")

# Sample: all multi-row game_ids (incl BOTH instances of the postponed game) + recent
multi = [r[0] for r in conn.execute("SELECT game_id FROM picks WHERE game_id IS NOT NULL GROUP BY game_id HAVING COUNT(*)>1")]
sample = [(g,d) for (g,d) in grp if g in multi][:14] + grp[:4]
seen=set(); sample=[x for x in sample if not (x in seen or seen.add(x))]

mm=0
for gid, gdate in sample:
    trk = TRK(gid, conn=conn, date=gdate)
    meta = conn.execute("SELECT matchup FROM picks WHERE game_id=? AND date=? LIMIT 1",(str(gid),gdate)).fetchone()
    mu = meta["matchup"] if meta else None
    card = card_sel(mu, gdate)
    grp_live = GRP("mlb", mu, gdate)  # real card call (None now: no pending rows)
    ok = (trk and card and trk["type"]==card["bet_type"] and trk["pick"]==card["pick"]
          and (trk["edge"] or 0)==(card["edge"] or 0) and trk["odds"]==card["odds"]
          and trk["result"]==card["result"])
    if not ok: mm+=1
    note = "  <-- postponed game instance" if gid==824362 else ""
    print(f"  {mu:12} {gdate}  card=({card['bet_type']!r},{card['pick']!r},e{card['edge']},{card['odds']},{card['result']})  "
          f"tracker=({trk['type']!r},{trk['pick']!r},e{trk['edge']},{trk['odds']},{trk['result']})  "
          f"liveGRP={'None' if grp_live is None else 'dict'}  MATCH={'YES' if ok else 'NO'}{note}")

print(f"\n=== {len(sample)-mm}/{len(sample)} match (date-aware); mismatches={mm} ===")

# Full endpoint reconstruction + confirm postponed game appears as its 2 instances
rows=[]
for gid,gdate in grp:
    b=TRK(gid, conn=conn, date=gdate)
    if b: rows.append((gid,gdate,b))
pg=[ (g,d,b["result"],b["edge"]) for (g,d,b) in rows if g==824362]
w=sum(1 for _,_,b in rows if b["result"]=="W"); l=sum(1 for _,_,b in rows if b["result"]=="L")
print(f"endpoint rows={len(rows)}  W={w} L={l} win%={round(100*w/(w+l),1) if (w+l) else 0}")
print(f"postponed game 824362 instances in output: {pg}")
