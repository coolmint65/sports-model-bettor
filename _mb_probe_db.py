import sys, os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
os.chdir(r'C:\sports-model-bettor')
sys.path.insert(0, r'C:\sports-model-bettor')
from engine.db import get_conn
import sqlite3
conn = get_conn(); conn.row_factory = sqlite3.Row

print("== DB FILE ==")
try:
    print([r[2] for r in conn.execute("PRAGMA database_list")])
except Exception as e:
    print("dbpath err", e)

print("\n== distinct bet_type (counts) ==")
for r in conn.execute("SELECT bet_type, COUNT(*) n, SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) pending FROM picks GROUP BY bet_type ORDER BY n DESC"):
    print(f"  {r['bet_type']!r:20} n={r['n']:5}  pending={r['pending']}")

print("\n== NULL game_id rows ==")
print("  null game_id:", conn.execute("SELECT COUNT(*) FROM picks WHERE game_id IS NULL").fetchone()[0])
print("  total rows  :", conn.execute("SELECT COUNT(*) FROM picks").fetchone()[0])

print("\n== rows-per-game distribution (how many games have >1 recorded pick) ==")
for r in conn.execute("SELECT c, COUNT(*) games FROM (SELECT game_id, COUNT(*) c FROM picks WHERE game_id IS NOT NULL GROUP BY game_id) GROUP BY c ORDER BY c"):
    print(f"  {r['c']} rows/game -> {r['games']} games")

print("\n== how often is the max-EDGE authority row a non-core (1st INN) bet_type? ==")
# for each game, the authority = max edge row; show its bet_type distribution
q = """
SELECT bet_type, COUNT(*) n FROM (
  SELECT game_id,
    (SELECT p2.bet_type FROM picks p2 WHERE p2.game_id=p.game_id AND (p2.result IS NULL OR p2.result!='V')
     ORDER BY p2.edge DESC LIMIT 1) bet_type
  FROM picks p WHERE game_id IS NOT NULL GROUP BY game_id
) GROUP BY bet_type ORDER BY n DESC
"""
for r in conn.execute(q):
    print(f"  authority bet_type {r['bet_type']!r:20} -> {r['n']} games")

print("\n== recent 6 games (by MAX created_at): ALL recorded rows ==")
gids = [r[0] for r in conn.execute(
    "SELECT game_id, MAX(created_at) c FROM picks WHERE game_id IS NOT NULL AND (result IS NULL OR result!='V') "
    "GROUP BY game_id ORDER BY c DESC LIMIT 6")]
for gid in gids:
    print(f"\n  --- game_id={gid} ---")
    for r in conn.execute("SELECT date,matchup,bet_type,pick,edge,odds,model_prob,result,profit,stake_units,created_at FROM picks WHERE game_id=? ORDER BY edge DESC", (gid,)):
        print(f"    date={r['date']} mu={r['matchup']!r} bt={r['bet_type']!r} pick={r['pick']!r} edge={r['edge']} odds={r['odds']} prob={r['model_prob']} result={r['result']} profit={r['profit']} su={r['stake_units']}")
