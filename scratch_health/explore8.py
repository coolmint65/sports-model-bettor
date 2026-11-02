import sqlite3, os, sys
ROOT = r"C:\sports-model-bettor\data"
def ro(path):
    c = sqlite3.connect("file:" + path + "?mode=ro", uri=True); c.row_factory = sqlite3.Row; return c
n = ro(os.path.join(ROOT, "nba.db"))
for gid in ("401909904", "401909903", "401909906", "401909905"):
    r = n.execute("SELECT * FROM nba_games WHERE game_id=?", (gid,)).fetchone()
    d = dict(r)
    h1 = (d["home_q1"] or 0)+(d["home_q2"] or 0)+(d["away_q1"] or 0)+(d["away_q2"] or 0)
    print(gid, d["date"], d["status"], d["start_time"], "q1", d["home_q1"], d["away_q1"], "q2", d["home_q2"], d["away_q2"], "final", d["home_score"], d["away_score"], "1H total", h1)
    for p in n.execute("SELECT id, bet_type, pick, odds, result, profit, settled_at FROM nba_paper_picks WHERE game_id=?", (gid,)):
        print("   card", tuple(p))
u = ro(os.path.join(ROOT, "picks_unified.db"))
for r in u.execute("SELECT id, bet_type, pick_text, odds, result, profit, settled_at, created_at FROM picks WHERE game_key='nba:nba:401909904' AND bet_type LIKE 'H1%'"):
    print("   unified", tuple(r))
h = ro(os.path.join(ROOT, "nhl.db"))
print([r[0] for r in h.execute("SELECT name FROM sqlite_master WHERE type='table'")])
