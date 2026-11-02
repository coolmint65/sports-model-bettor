# -*- coding: utf-8 -*-
"""READ-ONLY: for the tracker's own matches, compare the tracker's shadow-pool
best (_soccer_merged_recorded_best) vs select_pick(recorded_rows) == card logic."""
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
    return "%s/%s edge=%s prob=%s odds=%s result=%s" % (
        p.get("type") or p.get("bet_type") or p.get("market"), p.get("pick"),
        p.get("edge"), p.get("model_prob") or p.get("prob"),
        p.get("odds"), p.get("result"))

card = get("/api/soccer/%s/today" % LEAGUE)
print("CARD keys:", sorted(card.keys()))
print("CARD date:", card.get("date"), "loading:", card.get("loading"),
      "fell_forward:", card.get("fell_forward"),
      "#matches:", len(card.get("matches") or []),
      "#upcoming:", len(card.get("upcoming") or []))
for m in (card.get("matches") or []):
    print("  CARD match", m.get("match_id"), m.get("matchup"), m.get("status"),
          "best_pick=", pk(m.get("best_pick")))

trk = get("/api/soccer/%s/tracker/unified" % LEAGUE)
rows = trk.get("rows") or []
print("\nTRACKER rows:", len(rows))

from engine.soccer._db import get_conn
from engine.selection import select_pick
from engine.selection import edge_proven
conn = get_conn(LEAGUE); conn.row_factory = sqlite3.Row

# show which soccer markets are currently proven/eligible
print("\n--- edge_proven eligible cells for soccer/%s ---" % LEAGUE)
em = edge_proven.eligible_markets()
soc = {k: v for k, v in em.items() if k[0] == "soccer"}
for k, v in sorted(soc.items()):
    print("   ", k, "n=%d roi=%.1f conf=%.2f" % (v["n"], v["roi"], v["conf"]))
if not soc:
    print("    (NONE proven for soccer at all)")

print("\n%-10s %-26s | %-34s | %-34s | match?" % ("game_id","matchup","TRACKER shows","select_pick(recorded)==CARD"))
print("-"*140)
agree = disagree = 0
for row in rows:
    mid = str(row.get("game_id"))
    recorded = [dict(r) for r in conn.execute(
        "SELECT bet_type,pick,side,line,model_prob,edge,odds,stake_units,result "
        "FROM picks WHERE match_id=?", (int(mid),)).fetchall()]
    sp = select_pick("soccer", LEAGUE, recorded)
    trk_desc = pk(row)
    sp_desc = pk(sp)
    # compare by (bet_type, pick)
    tbt = (row.get("bet_type") or "").upper(); tpk = (row.get("pick") or "")
    sbt = ((sp or {}).get("bet_type") or (sp or {}).get("type") or "").upper() if sp else ""
    spk = (sp or {}).get("pick") if sp else None
    same = (sp is not None) and (tbt == sbt) and (tpk == spk)
    if same: agree += 1
    else: disagree += 1
    print("%-10s %-26s | %-34s | %-34s | %s" % (
        mid, (row.get("matchup") or "")[:26], trk_desc[:34], sp_desc[:34],
        "SAME" if same else "DIFF"))

print("\nSUMMARY: tracker rows=%d  agree-with-card-logic=%d  DIFFER=%d" % (len(rows), agree, disagree))
