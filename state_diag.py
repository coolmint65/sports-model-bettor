import sys, json; sys.path.insert(0, r'C:\sports-model-bettor')
import scrapers.hardrock_odds as hr
from datetime import datetime, timezone
evs, _ = hr._fetch_events_for_comp('2899138833758814469')
ev = evs[0]
print("name:", ev.get("name"))
for f in ("inplay","trackerStatus","startTime","eventTime","startEventDate","status","state"):
    if f in ev: print(f"  {f}: {ev[f]!r}")
print("  _event_already_started():", hr._event_already_started(ev))
print("  NOW utc:", datetime.now(timezone.utc).isoformat())
# recorder DB game row for date 2026-10-24
try:
    from engine.tracker._db import get_conn
except Exception:
    from backend.db import get_conn
import sqlite3
conn = get_conn()
rows = conn.execute("SELECT date, mlb_game_id, home_team_id, away_team_id, game_time, status FROM games WHERE date='2026-10-24'").fetchall()
print("DB games 2026-10-24:", [dict(r) if not isinstance(r,tuple) else r for r in rows])
