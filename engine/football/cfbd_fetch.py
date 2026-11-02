"""CollegeFootballData.com raw pull -> data/football/cfbd.db (Austin 2026-10-31, free key, 1,000 calls/month).
Every response is cached by its exact path+query and never fetched twice, so the monthly budget goes to new data only.
  python -m engine.football.cfbd_fetch backfill        (2024-2026: games, lines, returning, talent, player game stats)
"""
import json, os, sqlite3, sys, time, urllib.request, urllib.parse, urllib.error

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB = os.path.join(ROOT, 'data', 'football', 'cfbd.db')
KEY = open(os.path.join(ROOT, 'secrets', 'cfbd_api_key.txt')).read().strip()
BASE = 'https://api.collegefootballdata.com'

def conn():
    c = sqlite3.connect(DB, timeout=60)
    c.execute('CREATE TABLE IF NOT EXISTS raw (path TEXT PRIMARY KEY, fetched_at INTEGER, n INTEGER, body TEXT)')
    return c

def get(c, path, params, refresh=False):
    q = path + '?' + urllib.parse.urlencode(sorted(params.items()))
    if not refresh:
        r = c.execute('SELECT body FROM raw WHERE path=?', (q,)).fetchone()
        if r: return json.loads(r[0])
    req = urllib.request.Request(BASE + q, headers={'Authorization': f'Bearer {KEY}', 'Accept': 'application/json'})
    for i in range(5):
        try:
            body = urllib.request.urlopen(req, timeout=120).read().decode(); break
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and i < 4:
                time.sleep(10 * (i + 1)); continue
            raise
    d = json.loads(body)
    c.execute('INSERT OR REPLACE INTO raw VALUES (?,?,?,?)', (q, int(time.time()), len(d) if isinstance(d, list) else 1, body)); c.commit()
    print('  fetched', q, len(d) if isinstance(d, list) else '-', flush=True)
    time.sleep(0.5)
    return d

def remaining():
    req = urllib.request.Request(BASE + '/info', headers={'Authorization': f'Bearer {KEY}'})
    return json.loads(urllib.request.urlopen(req, timeout=30).read())

def backfill(years=(2024, 2025, 2026), last_week_2026=10):
    c = conn()
    for y in years:
        get(c, '/games', {'year': y, 'seasonType': 'both'})
        get(c, '/lines', {'year': y, 'seasonType': 'both'})
        get(c, '/player/returning', {'year': y})
        get(c, '/talent', {'year': y})
        weeks = range(1, (last_week_2026 if y == 2026 else 16) + 1)
        for w in weeks:
            get(c, '/games/players', {'year': y, 'week': w, 'seasonType': 'regular'})
        if y != 2026:
            get(c, '/games/players', {'year': y, 'week': 1, 'seasonType': 'postseason'})
    print(remaining())

if __name__ == '__main__':
    if sys.argv[1:2] == ['info']: print(remaining())
    else: backfill()
