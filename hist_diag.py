import sys, sqlite3; sys.path.insert(0, r'C:\sports-model-bettor')
# what does the recorder's get_conn point to?
try:
    from engine.db import get_conn
    c = get_conn()
    import os
    dbpath = [r[2] for r in c.execute("PRAGMA database_list")][0]
    print("recorder get_conn DB path:", dbpath)
except Exception as e:
    print("get_conn err:", e); c = sqlite3.connect(r'C:\sports-model-bettor\data\mlb.db')
print("\n-- picks table history --")
tot = c.execute("SELECT COUNT(*) FROM picks").fetchone()[0]
mm = c.execute("SELECT MIN(date), MAX(date) FROM picks").fetchone()
print(f"total picks: {tot} | date range: {mm}")
recent = c.execute("SELECT date, COUNT(*) FROM picks GROUP BY date ORDER BY date DESC LIMIT 12").fetchall()
print("most recent dates with picks:")
for r in recent: print("   ", tuple(r))
# games table — are postseason games even in the schedule?
try:
    g = c.execute("SELECT date, COUNT(*) FROM games WHERE date >= date('now','-14 day') GROUP BY date ORDER BY date DESC LIMIT 14").fetchall()
    print("\ngames in schedule (last 14d):", [tuple(x) for x in g])
except Exception as e: print("games q err:", e)
