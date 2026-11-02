import json, sqlite3
d = json.load(open(r"C:\sports-model-bettor\data\card_lean_lock.json"))
c = sqlite3.connect(r"file:C:\sports-model-bettor\data\nba.db?mode=ro", uri=True)
for g in ("401909903","401909904","401909905","401909906"):
    e = d.get("nba|" + g) or {}
    l = e.get("lean") or {}
    print(g, "LOCK", e.get("at"), l.get("type"), l.get("pick"), l.get("odds"), l.get("edge"), l.get("prob"), l.get("model_prob"))
    for r in c.execute("SELECT id,bet_type,pick,odds,edge,model_prob,created_at FROM nba_paper_picks WHERE game_id=? ORDER BY id LIMIT 1", (g,)):
        print("   DB ", r)
