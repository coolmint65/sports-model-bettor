import os, sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
os.chdir(r'C:\sports-model-bettor'); sys.path.insert(0, r'C:\sports-model-bettor')
from engine.db import get_conn
c = get_conn(); c.row_factory = sqlite3.Row
from backend.server.routes_mlb import _mlb_recorded_authority_pick as T

print("Postponed game 824362 (NYM @ COL) — date-aware tracker vs card per date:")
for d in ('2026-05-05', '2026-05-07'):
    b = T(824362, conn=c, date=d)
    card = c.execute("SELECT bet_type,pick,edge,odds,result FROM picks "
                     "WHERE date=? AND matchup='NYM @ COL' AND (result IS NULL OR result!='V') "
                     "ORDER BY edge DESC LIMIT 1", (d,)).fetchone()
    trk = (b['type'], b['pick'], b['edge'], b['odds'], b['result'], b['_scope'])
    crd = (card['bet_type'], card['pick'], card['edge'], card['odds'], card['result'])
    ok = trk[:5] == crd
    print(f"  {d}: tracker={trk}  card={crd}  MATCH={'YES' if ok else 'NO'}")
