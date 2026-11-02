import sqlite3
P = r"C:\sports-model-bettor\data\nba.db"
src = sqlite3.connect(P); bak = sqlite3.connect(P + ".bak-20261101-fpw"); src.backup(bak); bak.close()
# entry prices, corroborated by card_lean_lock.json (first sight) AND pick_events 'appeared' rows
fix = {9388: ("401909905", "H2_SPREAD", "GS -2.5 2H", 120, 10.2, 0.5564),
       9061: ("401909906", "H1_TOTAL", "Over 111.5 1H", -105, 6.8, 0.5799)}
for rid, (g, bt, pk, o, e, p) in fix.items():
    n = src.execute("UPDATE nba_paper_picks SET odds=?, edge=?, model_prob=? WHERE id=? AND game_id=? AND bet_type=? "
                    "AND pick=? AND result IS NULL", (o, e, p, rid, g, bt, pk)).rowcount
    print(rid, "restored" if n == 1 else "NOT FOUND", src.execute("SELECT id,bet_type,pick,odds,edge,model_prob FROM nba_paper_picks WHERE id=?", (rid,)).fetchone())
src.commit(); src.close()
