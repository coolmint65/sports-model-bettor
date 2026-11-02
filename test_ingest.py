import traceback
# 1) does the ESPN ingest work for MLB, or 403/fail?
try:
    from engine.baseball._espn_ingest import ingest_today
    r = ingest_today('mlb')
    print('ingest RESULT:', r)
except Exception as e:
    print('ingest ERROR:', type(e).__name__, str(e)[:180])
    traceback.print_exc()

# 2) what's in the games DB for today/soon?
try:
    from engine.baseball._db import get_conn
    conn = get_conn('mlb')
    cols = [d[1] for d in conn.execute('pragma table_info(games)')]
    print('games cols:', cols[:14])
    # find a date-ish column
    datecol = next((c for c in cols if 'date' in c.lower() or c.lower() in ('day', 'game_day')), None)
    timecol = next((c for c in cols if 'time' in c.lower() or 'start' in c.lower()), None)
    print('date col:', datecol, '| time col:', timecol)
    n = conn.execute('select count(*) from games').fetchone()[0]
    print('total games rows:', n)
    if datecol:
        recent = conn.execute('select %s, count(*) from games group by %s order by %s desc limit 6' % (datecol, datecol, datecol)).fetchall()
        print('recent dates:', recent)
except Exception as e:
    print('DB check err:', type(e).__name__, str(e)[:150])
