# -*- coding: utf-8 -*-
"""POST-PATCH verify (read-only): import the REAL patched functions and call the
actual tracker endpoint. Confirm new authority helper != old merged for the
flagged games, and that api_nba_tracker_unified runs + returns the card picks."""
import sys, sqlite3
sys.path.insert(0, r"C:\sports-model-bettor")

from backend.server.routes_nba import (
    _nba_recorded_authority_best as NEW,
    _nba_merged_recorded_best as OLD,
    api_nba_tracker_unified,
)
from engine.nba_db import get_conn

conn = get_conn(); conn.row_factory = sqlite3.Row
gids = [r[0] for r in conn.execute(
    "SELECT game_id, MAX(created_at) c FROM nba_paper_picks "
    "WHERE result IS NOT NULL AND result!='V' GROUP BY game_id ORDER BY c DESC LIMIT 40")]

def fmt(p):
    return "None" if not p else f"{p['type']}/{p['pick']} edge={p['edge']} res={p['result']} prof={p.get('profit')}"

diffs = []
for gid in gids:
    n = NEW(gid, conn=conn)
    evm = None
    o = OLD(gid, conn=conn)
    same = (o is None and n is None) or (o and n and o['type']==n['type'] and o['pick']==n['pick'])
    if not same:
        meta = conn.execute("SELECT matchup,date FROM nba_paper_picks WHERE game_id=? LIMIT 1",(str(gid),)).fetchone()
        diffs.append((gid, meta['matchup'] if meta else '?', fmt(o), fmt(n)))

print(f"games checked: {len(gids)}  behavior-changed: {len(diffs)}")
for gid, mu, o, n in diffs:
    print(f"  {gid} {mu}\n     OLD(merged re-deriv) -> {o}\n     NEW(card authority)  -> {n}")

print("\n--- api_nba_tracker_unified() end-to-end ---")
res = api_nba_tracker_unified(limit=40)
print("rows:", len(res['rows']), "| summary.all:", res['summary']['all'])
print("by_market:", {m: v['n'] for m, v in res['by_market'].items()})
# show the two flagged games as they now appear in the endpoint output
for r in res['rows']:
    if r['game_id'] in ('401909088','401909090'):
        print(f"  ENDPOINT {r['game_id']} {r['matchup']}: {r['bet_type']}/{r['pick']} "
              f"market={r['market']} edge={r['edge']} result={r['result']} profit={r['profit']}")
conn.close()
