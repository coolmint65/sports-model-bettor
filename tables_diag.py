import sqlite3
c = sqlite3.connect(r'C:\sports-model-bettor\data\mlb.db')
tabs = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")]
print("mlb.db tables:", tabs)
for t in tabs:
    try:
        cols = [r[1] for r in c.execute(f"PRAGMA table_info({t})")]
        if 'date' in cols:
            mx = c.execute(f"SELECT MAX(date), COUNT(*) FROM {t}").fetchone()
            print(f"  {t}: max_date={mx[0]} rows={mx[1]}")
    except Exception as e:
        print(f"  {t}: err {e}")
# also check if there's a separate tracker/unified db
import glob
for db in glob.glob(r'C:\sports-model-bettor\data\*.db'):
    nm = db.split(chr(92))[-1]
    if nm == 'mlb.db': continue
    try:
        cc = sqlite3.connect(db)
        for t in [r[0] for r in cc.execute("SELECT name FROM sqlite_master WHERE type='table'")]:
            cols = [r[1] for r in cc.execute(f"PRAGMA table_info({t})")]
            if 'date' in cols and ('pick' in t.lower() or 'track' in t.lower() or 'bet' in t.lower()):
                mx = cc.execute(f"SELECT MAX(date), COUNT(*) FROM {t}").fetchone()
                if mx[0] and mx[0] >= '2026-10-01': print(f"  [{nm}] {t}: max_date={mx[0]} rows={mx[1]}")
        cc.close()
    except Exception: pass
