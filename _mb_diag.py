import sys, os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
os.chdir(r'C:\sports-model-bettor'); sys.path.insert(0, r'C:\sports-model-bettor')
import sqlite3
from engine.db import get_conn
conn = get_conn(); conn.row_factory = sqlite3.Row

print("== game 824362 full rows (repr) ==")
for r in conn.execute("SELECT id,game_id,date,matchup,bet_type,pick,edge,odds,result,created_at FROM picks WHERE game_id=824362 ORDER BY edge DESC"):
    print(f"  id={r['id']} gid={r['game_id']} date={r['date']!r} matchup={r['matchup']!r} bt={r['bet_type']!r} pick={r['pick']!r} edge={r['edge']} odds={r['odds']} result={r['result']} created={r['created_at']!r}")

print("\n== games whose single game_id spans >1 DISTINCT matchup string (duplicate-spelling) ==")
rows = conn.execute("""
  SELECT game_id, COUNT(DISTINCT matchup) nd, GROUP_CONCAT(DISTINCT matchup) mus
  FROM picks WHERE game_id IS NOT NULL GROUP BY game_id HAVING nd>1 ORDER BY game_id
""").fetchall()
print(f"  count = {len(rows)} games")
for r in rows:
    print(f"    gid={r['game_id']}  matchups={r['mus']!r}")

print("\n== does the UNIQUE index uq_picks_family exist, and any true dup keys? ==")
for r in conn.execute("SELECT name,sql FROM sqlite_master WHERE type='index' AND name LIKE 'uq_picks%'"):
    print("  index:", r['name'])
dups = conn.execute("""
  SELECT date,game_id,bet_type,pick,COUNT(*) c FROM picks
  GROUP BY date,game_id,bet_type,pick HAVING c>1 ORDER BY c DESC LIMIT 10
""").fetchall()
print(f"  exact (date,game_id,bet_type,pick) dup groups: {len(dups)}")
for d in dups:
    print(f"    {d['date']} gid={d['game_id']} bt={d['bet_type']!r} pick={d['pick']!r} x{d['c']}")

print("\n== for each duplicate-spelling game: does the PICK (not snapshot) agree between")
print("   game_id-max-edge (tracker) and each matchup's max-edge (card view)? ==")
for r in rows:
    gid = r['game_id']
    trk = conn.execute("SELECT bet_type,pick,edge FROM picks WHERE game_id=? AND (result IS NULL OR result!='V') ORDER BY edge DESC LIMIT 1",(str(gid),)).fetchone()
    mus = conn.execute("SELECT DISTINCT matchup,date FROM picks WHERE game_id=?",(str(gid),)).fetchall()
    cardpicks=set()
    for m in mus:
        c = conn.execute("SELECT bet_type,pick FROM picks WHERE date=? AND matchup=? AND (result IS NULL OR result!='V') ORDER BY edge DESC LIMIT 1",(m['date'],m['matchup'])).fetchone()
        if c: cardpicks.add((c['bet_type'],c['pick']))
    agree = (trk['bet_type'],trk['pick']) in cardpicks
    print(f"  gid={gid} tracker=({trk['bet_type']!r},{trk['pick']!r}) card-variants={cardpicks} pick-agrees={agree}")
