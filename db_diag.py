import sys, glob, os, sqlite3
sys.path.insert(0, r'C:\sports-model-bettor')
# find the tracker DB via the recorder's own get_conn
try:
    from engine.tracker._helpers import get_conn
    conn = get_conn()
    print("get_conn OK")
except Exception as e:
    print("get_conn import failed:", e)
    # fallback: find .db files
    dbs = glob.glob(r'C:\sports-model-bettor\**\*.db', recursive=True)
    print("db files:", dbs[:10])
    conn = None
if conn:
    try:
        rows = conn.execute("""
            SELECT date, COUNT(*) n FROM picks
            WHERE date >= date('now','-10 day')
            GROUP BY date ORDER BY date DESC LIMIT 15
        """).fetchall()
        print("picks by date (last 10d):")
        for r in rows: print("  ", tuple(r))
        # MLB-specific: matchups look like 'XX @ YY'
        mlb = conn.execute("""
            SELECT date, COUNT(*) n FROM picks
            WHERE bet_type IN ('ML','RL','TOTAL','1st INN','F5 ML') AND date >= date('now','-10 day')
            GROUP BY date ORDER BY date DESC LIMIT 15
        """).fetchall()
        print("MLB-ish picks by date:", [tuple(r) for r in mlb])
    except Exception as e:
        import traceback; traceback.print_exc()
