import sqlite3, os, sys, json
ROOT = r"C:\sports-model-bettor\data"
def ro(path):
    c = sqlite3.connect("file:" + path + "?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c
u = ro(os.path.join(ROOT, "picks_unified.db"))
print("== variants")
for r in u.execute("SELECT sport, variant, is_shadow, COUNT(*) FROM picks WHERE pick_date>='2026-10-20' GROUP BY 1,2,3"):
    print(tuple(r))
print("== dup groups (same game_key,bet_type,pick_text) since 10-20")
for r in u.execute("""SELECT sport, COUNT(*) g, SUM(c) rows FROM (SELECT sport, game_key, bet_type, pick_text, COUNT(*) c FROM picks WHERE pick_date>='2026-10-20' GROUP BY 1,2,3,4 HAVING c>1) GROUP BY 1"""):
    print(tuple(r))
print("== NBA card table nba_paper_picks since 10-26")
n = ro(os.path.join(ROOT, "nba.db"))
for r in n.execute("SELECT date, game_id, matchup, bet_type, pick, odds, edge, model_prob, result, profit, stake_units, closing_odds, created_at FROM nba_paper_picks WHERE date>='2026-10-26' ORDER BY id"):
    print(dict(r))
print("== nba_derivative_picks since 10-26")
for r in n.execute("SELECT date, game_id, bet_type, pick, odds, result, stake_units FROM nba_derivative_picks WHERE date>='2026-10-26' ORDER BY id"):
    print(tuple(r))
print("== NHL nhl_picks since 10-26")
h = ro(os.path.join(ROOT, "nhl.db"))
for r in h.execute("SELECT date, game_id, matchup, bet_type, pick, odds, edge, result, profit, stake_units, is_shadow, created_at FROM nhl_picks WHERE date>='2026-10-26' ORDER BY id"):
    print(dict(r))
print("== nhl_derivative_picks / p1 since 10-26")
for t in ("nhl_derivative_picks", "nhl_p1_paper_picks"):
    for r in h.execute(f"SELECT '{t}', date, game_id, bet_type, pick, odds, result, stake_units FROM {t} WHERE date>='2026-10-26' ORDER BY id"):
        print(tuple(r))
sys.stdout.flush()
