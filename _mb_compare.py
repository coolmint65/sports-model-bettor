# -*- coding: utf-8 -*-
"""READ-ONLY: compare soccer card best_pick vs unified tracker vs select_pick(recorded_rows)."""
import json, sys, urllib.request, sqlite3
sys.path.insert(0, r"C:\sports-model-bettor")

LEAGUE = "eng_premier"
BASE = "http://localhost:8000"

def get(path):
    with urllib.request.urlopen(BASE + path, timeout=60) as r:
        return json.loads(r.read())

def pk(p):
    if not p:
        return "None (NO-PLAY)"
    return "%s / %s | edge=%s prob=%s odds=%s result=%s" % (
        p.get("type") or p.get("bet_type"), p.get("pick"),
        p.get("edge"), p.get("model_prob") or p.get("prob"),
        p.get("odds"), p.get("result"))

print("====== CARD endpoint /today ======")
card = get("/api/soccer/%s/today" % LEAGUE)
card_matches = (card.get("matches") or []) + (card.get("upcoming") or [])
card_best = {}
for m in card_matches:
    mid = str(m.get("match_id"))
    bp = m.get("best_pick")
    card_best[mid] = bp
    print("match_id=%s %s status=%s" % (mid, m.get("matchup"), m.get("status")))
    print("   CARD best_pick: %s" % pk(bp))
    print("   #candidate picks on card: %d" % len(m.get("picks") or []))

print("\n====== TRACKER endpoint /tracker/unified ======")
trk = get("/api/soccer/%s/tracker/unified" % LEAGUE)
trk_by_mid = {}
for row in trk.get("rows") or []:
    trk_by_mid.setdefault(str(row.get("game_id")), []).append(row)
print("tracker total rows:", len(trk.get("rows") or []))
for mid in list(card_best.keys()):
    rws = trk_by_mid.get(mid) or []
    print("match_id=%s -> %d tracker row(s)" % (mid, len(rws)))
    for row in rws:
        print("   TRACKER: %s" % pk(row))

print("\n====== select_pick(recorded_rows) for the SAME matches ======")
from engine.soccer._db import get_conn
from engine.selection import select_pick
conn = get_conn(LEAGUE); conn.row_factory = sqlite3.Row
for mid in list(card_best.keys()):
    try:
        recorded = [dict(r) for r in conn.execute(
            "SELECT bet_type,pick,side,line,model_prob,edge,odds,stake_units,result "
            "FROM picks WHERE match_id=?", (int(mid),)).fetchall()]
    except Exception as e:
        print("match_id=%s recorded-read ERR %s" % (mid, e)); continue
    # open (result IS NULL) subset = what _lock_live_picks would surface
    open_rows = [r for r in recorded if r.get("result") is None]
    sp_all = select_pick("soccer", LEAGUE, recorded)
    sp_open = select_pick("soccer", LEAGUE, open_rows)
    print("match_id=%s recorded=%d open=%d" % (mid, len(recorded), len(open_rows)))
    print("   select_pick(ALL recorded):  %s" % pk(sp_all))
    print("   select_pick(OPEN recorded): %s" % pk(sp_open))
    print("   CARD best_pick:             %s" % pk(card_best[mid]))
    # show the recorded leans
    for r in recorded:
        print("      lean: %s/%s edge=%s prob=%s odds=%s result=%s" % (
            r.get("bet_type"), r.get("pick"), r.get("edge"),
            r.get("model_prob"), r.get("odds"), r.get("result")))
