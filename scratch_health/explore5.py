import sqlite3, os, sys, gzip, json
ROOT = r"C:\sports-model-bettor\data"
def ro(path):
    c = sqlite3.connect("file:" + path + "?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c
g = ro(os.path.join(ROOT, "game_odds.db"))
print("== game_odds last_seen per sport/league")
for r in g.execute("SELECT sport, league, COUNT(*) n, MAX(last_seen) ls, MAX(capture_date) cd, MAX(start_time) st FROM game_odds GROUP BY 1,2 ORDER BY ls DESC"):
    print(tuple(r))
print("== game_derivative_odds last_seen per sport/league")
for r in g.execute("SELECT sport, league, COUNT(*) n, MAX(last_seen) ls FROM game_derivative_odds GROUP BY 1,2 ORDER BY ls DESC"):
    print(tuple(r))
print("== sample game_odds rows")
for r in g.execute("SELECT * FROM game_odds ORDER BY last_seen DESC LIMIT 3"):
    print(dict(r))
p = ro(os.path.join(ROOT, "pick_provenance.db"))
print("== provenance per sport (last 2 days)")
for r in p.execute("SELECT sport, accepted, COUNT(*), MAX(created_at) FROM pick_provenance WHERE created_at >= datetime('now','-2 day') GROUP BY 1,2"):
    print(tuple(r))
print("== indexes")
for r in p.execute("SELECT name, sql FROM sqlite_master WHERE type='index'"):
    print(tuple(r))
print("== sample accepted trace")
r = p.execute("SELECT * FROM pick_provenance WHERE accepted=1 AND sport='nba' ORDER BY created_at DESC LIMIT 1").fetchone()
d = dict(r); tg = d.pop("trace_gz")
print(d)
try:
    print(json.dumps(json.loads(gzip.decompress(tg)), indent=0)[:3000])
except Exception as e:
    print("decode err", e, type(tg), tg[:50] if tg else None)
sys.stdout.flush()
