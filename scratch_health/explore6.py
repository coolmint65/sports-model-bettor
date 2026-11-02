import sqlite3, os, sys
ROOT = r"C:\sports-model-bettor\data"
def ro(path):
    c = sqlite3.connect("file:" + path + "?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c
u = ro(os.path.join(ROOT, "picks_unified.db"))
n = ro(os.path.join(ROOT, "nba.db"))
def imp(o):
    o = float(o)
    return abs(o)/(abs(o)+100) if o < 0 else 100/(o+100)
print("== NBA card rows vs shadow twin (same game, bet_type, pick)")
for r in n.execute("SELECT * FROM nba_paper_picks WHERE date>='2026-10-26' ORDER BY id"):
    gk = "nba:nba:%s" % r["game_id"]
    tw = u.execute("SELECT bet_type, pick_text, odds, prob, edge_pct, is_shadow, result, created_at FROM picks WHERE game_key=? AND (bet_type=? OR REPLACE(UPPER(bet_type),' ','_')=REPLACE(UPPER(?),' ','_'))", (gk, r["bet_type"], r["bet_type"])).fetchall()
    print("CARD", r["date"], r["matchup"], r["bet_type"], r["pick"], r["odds"], round(r["model_prob"], 4), "imp", round(imp(r["odds"]), 4), r["result"])
    for t in tw:
        print("     twin", tuple(t))
print("== picks_unified NBA rows pending (any date)")
for r in u.execute("SELECT pick_date, matchup, bet_type, pick_text, odds, is_shadow, created_at FROM picks WHERE sport='nba' AND result IS NULL ORDER BY pick_date"):
    print(tuple(r))
print("== picks_unified pending older than 2 days, by sport")
for r in u.execute("SELECT sport, league, is_shadow, COUNT(*), MIN(pick_date), MAX(pick_date) FROM picks WHERE result IS NULL AND pick_date <= date('now','-2 day') GROUP BY 1,2,3"):
    print(tuple(r))
sys.stdout.flush()
