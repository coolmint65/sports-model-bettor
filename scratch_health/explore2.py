import sqlite3, os, sys
ROOT = r"C:\sports-model-bettor\data"
def ro(path):
    c = sqlite3.connect("file:" + path + "?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c

c = ro(os.path.join(ROOT, "picks_unified.db"))
print("== by sport/league/is_shadow (since 2026-10-01)")
for r in c.execute("""SELECT sport, league, is_shadow, COUNT(*) n,
       SUM(result IN ('W','L','P','V')) settled, SUM(result IS NULL OR result='' OR result='pending') pend,
       MIN(pick_date) mn, MAX(pick_date) mx, SUM(stake_units IS NULL) st_null, SUM(stake_units=0) st0
       FROM picks WHERE pick_date >= '2026-10-01' GROUP BY 1,2,3 ORDER BY 1,2,3"""):
    print(dict(r))
print("== distinct results")
for r in c.execute("SELECT result, COUNT(*) FROM picks GROUP BY 1"):
    print(tuple(r))
print("== model_version / pipeline_version (since 10-20)")
for r in c.execute("SELECT sport, model_version, pipeline_version, COUNT(*) FROM picks WHERE pick_date>='2026-10-20' GROUP BY 1,2,3 ORDER BY 1"):
    print(tuple(r))
print("== bet types since 10-20 by sport/league")
for r in c.execute("SELECT sport, league, bet_type, scope, is_shadow, COUNT(*) n, SUM(result IN ('W','L')) wl FROM picks WHERE pick_date>='2026-10-20' GROUP BY 1,2,3,4,5 ORDER BY 1,2,3"):
    print(tuple(r))
print("== sample rows")
for r in c.execute("SELECT * FROM picks WHERE pick_date>='2026-10-30' ORDER BY id DESC LIMIT 6"):
    print(dict(r))
sys.stdout.flush()
