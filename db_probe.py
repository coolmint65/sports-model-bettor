import glob, sqlite3
for db in glob.glob(r'C:\sports-model-bettor\data\*.db'):
    try:
        c = sqlite3.connect(db)
        t = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='picks'")]
        if t:
            rows = c.execute("SELECT date, COUNT(*) FROM picks WHERE date >= date('now','-12 day') GROUP BY date ORDER BY date DESC LIMIT 14").fetchall()
            print(f"{db.split(chr(92))[-1]}: picks table -> {rows}")
        c.close()
    except Exception as e:
        pass
